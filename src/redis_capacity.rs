use std::fmt;

use redis::{aio::MultiplexedConnection, ErrorKind};
use serde::Serialize;

use crate::formation::CoordinationRole;

pub const ROLE_MEMORY_LIMIT_NAME: &str = "max-bytes";
const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const MIN_FORMATION_RESERVE_BYTES: u64 = 8 * MIB;
const FORMATION_RESERVE_DIVISOR: u64 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisRoleCapacityMeasurement {
    pub protocol_records_bytes: u64,
    pub pending_delivery_metadata_bytes: u64,
    pub script_overhead_bytes: u64,
    pub aof_progress_headroom_bytes: u64,
    pub restart_reconstruction_headroom_bytes: u64,
}

impl RedisRoleCapacityMeasurement {
    pub const fn total_bytes(self) -> u64 {
        self.protocol_records_bytes
            + self.pending_delivery_metadata_bytes
            + self.script_overhead_bytes
            + self.aof_progress_headroom_bytes
            + self.restart_reconstruction_headroom_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisRoleCapacityCalibration {
    pub role: CoordinationRole,
    pub protocol_identity: &'static str,
    pub accounted_objects: &'static str,
    pub cleanup_boundary: RedisQuotaCleanupBoundary,
    pub measurement: RedisRoleCapacityMeasurement,
    pub minimum_bytes: u64,
    pub default_bytes: u64,
    pub maximum_bytes: u64,
}

pub const CALIBRATED_ROLE_CAPACITIES: [RedisRoleCapacityCalibration; 13] = [
    calibration(
        CoordinationRole::CommandBus,
        "tickr.command-bus.redis-request-reply/1",
        "request, correlation, reply, delivery, expiry",
        RedisQuotaCleanupBoundary::ReplyDeliveredOrExpired,
        measured(10, 1, 128, 1, 1),
        16,
    ),
    calibration(
        CoordinationRole::TaskDispatch,
        "tickr.task-dispatch.redis-stream/1",
        "dispatch, pending delivery, pickup generation, owner claim, staged event",
        RedisQuotaCleanupBoundary::DispatchTerminal,
        measured(56, 4, 256, 2, 2),
        80,
    ),
    calibration(
        CoordinationRole::TaskEvents,
        "tickr.task-events.redis-stream/1",
        "event, pending delivery, completion evidence",
        RedisQuotaCleanupBoundary::TaskEventRelayed,
        measured(56, 4, 256, 2, 2),
        80,
    ),
    calibration(
        CoordinationRole::TaskCancellation,
        "tickr.task-cancellation.redis-fence/1",
        "request, generation fence, acknowledgement, reconciliation evidence",
        RedisQuotaCleanupBoundary::CancellationTerminal,
        measured(28, 2, 128, 1, 1),
        40,
    ),
    calibration(
        CoordinationRole::CompactionStaging,
        "tickr.compaction-staging.redis-stream/1",
        "envelope, pending delivery, seal reference, archive commit",
        RedisQuotaCleanupBoundary::ArchiveCommitted,
        measured(256, 16, 256, 8, 8),
        320,
    ),
    calibration(
        CoordinationRole::LifecycleWork,
        "tickr.lifecycle-work.redis-advisory-notification/1",
        "coalesced hint, expiry index, delivery metadata",
        RedisQuotaCleanupBoundary::LifecycleSettled,
        measured_kib(512, 256, 128, 512, 512),
        4,
    ),
    calibration(
        CoordinationRole::LogStaging,
        "tickr.log-staging.redis-accepted-stream/1",
        "accepted record, gap, frontier, terminal, seal, archive commit",
        RedisQuotaCleanupBoundary::LogArchiveCommitted,
        measured(256, 16, 256, 8, 8),
        320,
    ),
    calibration(
        CoordinationRole::ScopeStore,
        "tickr.scope-store.redis-opaque-snapshot/1",
        "scope value, owner binding, stable claim, snapshot, archive commit",
        RedisQuotaCleanupBoundary::WorkflowArchived,
        measured(256, 16, 256, 8, 8),
        320,
    ),
    calibration(
        CoordinationRole::IngressIdempotencyStore,
        "tickr.ingress-idempotency.redis-lease/1",
        "reservation, effect, result, relay intent, rejection",
        RedisQuotaCleanupBoundary::IngressTerminal,
        measured(56, 4, 256, 2, 2),
        80,
    ),
    calibration(
        CoordinationRole::LivenessWatchdog,
        "tickr.liveness-watchdog.redis-deadline-election/1",
        "generation record, owner deadline, deadline index, elected verdict",
        RedisQuotaCleanupBoundary::LivenessTerminal,
        measured(32, 2, 256, 1, 1),
        48,
    ),
    calibration(
        CoordinationRole::SignalAppliedNotifier,
        "tickr.signal-applied.redis-pubsub/1",
        "coalesced hint, expiry index, reconciliation metadata",
        RedisQuotaCleanupBoundary::NotificationReconciled,
        measured_kib(512, 256, 128, 512, 512),
        4,
    ),
    calibration(
        CoordinationRole::ExecutorFleetStatus,
        "tickr.executor-fleet-status.redis-expiring-observation/1",
        "observation, reporter incarnation, expiry index",
        RedisQuotaCleanupBoundary::ObservationExpired,
        measured_kib(2048, 512, 128, 512, 512),
        8,
    ),
    calibration(
        CoordinationRole::EventIngress,
        "tickr.event-ingress.redis-stream/1",
        "delivery, pending metadata, terminal disposition",
        RedisQuotaCleanupBoundary::EventIngressTerminal,
        measured(56, 4, 256, 2, 2),
        80,
    ),
];

const fn measured(
    protocol_records_mib: u64,
    pending_delivery_metadata_mib: u64,
    script_overhead_kib: u64,
    aof_progress_headroom_mib: u64,
    restart_reconstruction_headroom_mib: u64,
) -> RedisRoleCapacityMeasurement {
    RedisRoleCapacityMeasurement {
        protocol_records_bytes: protocol_records_mib * MIB,
        pending_delivery_metadata_bytes: pending_delivery_metadata_mib * MIB,
        script_overhead_bytes: script_overhead_kib * KIB,
        aof_progress_headroom_bytes: aof_progress_headroom_mib * MIB,
        restart_reconstruction_headroom_bytes: restart_reconstruction_headroom_mib * MIB,
    }
}

const fn measured_kib(
    protocol_records_kib: u64,
    pending_delivery_metadata_kib: u64,
    script_overhead_kib: u64,
    aof_progress_headroom_kib: u64,
    restart_reconstruction_headroom_kib: u64,
) -> RedisRoleCapacityMeasurement {
    RedisRoleCapacityMeasurement {
        protocol_records_bytes: protocol_records_kib * KIB,
        pending_delivery_metadata_bytes: pending_delivery_metadata_kib * KIB,
        script_overhead_bytes: script_overhead_kib * KIB,
        aof_progress_headroom_bytes: aof_progress_headroom_kib * KIB,
        restart_reconstruction_headroom_bytes: restart_reconstruction_headroom_kib * KIB,
    }
}

const fn calibration(
    role: CoordinationRole,
    protocol_identity: &'static str,
    accounted_objects: &'static str,
    cleanup_boundary: RedisQuotaCleanupBoundary,
    measurement: RedisRoleCapacityMeasurement,
    default_mib: u64,
) -> RedisRoleCapacityCalibration {
    let minimum_bytes = measurement.total_bytes().div_ceil(MIB) * MIB;
    let default_bytes = default_mib * MIB;
    RedisRoleCapacityCalibration {
        role,
        protocol_identity,
        accounted_objects,
        cleanup_boundary,
        measurement,
        minimum_bytes,
        default_bytes,
        maximum_bytes: default_bytes,
    }
}

pub fn calibrated_role_capacity(role: CoordinationRole) -> RedisRoleCapacityCalibration {
    CALIBRATED_ROLE_CAPACITIES[role as usize]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisRoleCapacityLimit {
    pub role: CoordinationRole,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisCapacityProfile {
    configured_capacity_bytes: u64,
    required_reserve_bytes: u64,
    role_capacity_bytes: [u64; 13],
    role_capacity_sum_bytes: u64,
}

impl RedisCapacityProfile {
    pub fn admit(
        configured_capacity_bytes: u64,
        limits: Vec<RedisRoleCapacityLimit>,
    ) -> Result<Self, RedisCapacityFailure> {
        if configured_capacity_bytes == 0 {
            return Err(RedisCapacityFailure::UnboundedCapacity);
        }

        let mut role_capacity_bytes = [0; 13];
        let mut seen = [false; 13];
        for limit in limits {
            let index = limit.role as usize;
            if seen[index] {
                return Err(RedisCapacityFailure::DuplicateRoleLimit);
            }
            seen[index] = true;
            let calibration = calibrated_role_capacity(limit.role);
            if !(calibration.minimum_bytes..=calibration.maximum_bytes)
                .contains(&limit.memory_bytes)
            {
                return Err(RedisCapacityFailure::RoleLimitOutsideBounds);
            }
            role_capacity_bytes[index] = limit.memory_bytes;
        }
        if seen.iter().any(|present| !present) {
            return Err(RedisCapacityFailure::MissingRoleLimit);
        }

        let role_capacity_sum_bytes = role_capacity_bytes
            .iter()
            .try_fold(0_u64, |sum, value| sum.checked_add(*value))
            .ok_or(RedisCapacityFailure::CapacityOverflow)?;
        let required_reserve_bytes =
            MIN_FORMATION_RESERVE_BYTES.max(configured_capacity_bytes / FORMATION_RESERVE_DIVISOR);
        let committed = role_capacity_sum_bytes
            .checked_add(required_reserve_bytes)
            .ok_or(RedisCapacityFailure::CapacityOverflow)?;
        if committed > configured_capacity_bytes {
            return Err(RedisCapacityFailure::InsufficientReserve);
        }

        Ok(Self {
            configured_capacity_bytes,
            required_reserve_bytes,
            role_capacity_bytes,
            role_capacity_sum_bytes,
        })
    }

    pub fn configured_capacity_bytes(&self) -> u64 {
        self.configured_capacity_bytes
    }

    pub fn required_reserve_bytes(&self) -> u64 {
        self.required_reserve_bytes
    }

    pub fn role_capacity_sum_bytes(&self) -> u64 {
        self.role_capacity_sum_bytes
    }

    pub fn role_capacity_bytes(&self, role: CoordinationRole) -> u64 {
        self.role_capacity_bytes[role as usize]
    }

    pub fn default_candidate(configured_capacity_bytes: u64) -> Result<Self, RedisCapacityFailure> {
        Self::admit(
            configured_capacity_bytes,
            CALIBRATED_ROLE_CAPACITIES
                .iter()
                .map(|calibration| RedisRoleCapacityLimit {
                    role: calibration.role,
                    memory_bytes: calibration.default_bytes,
                })
                .collect(),
        )
    }

    pub fn validate_server(
        &self,
        maxmemory_bytes: u64,
        maxmemory_policy: &str,
        used_memory_bytes: u64,
    ) -> Result<(), RedisCapacityFailure> {
        if maxmemory_bytes == 0 {
            return Err(RedisCapacityFailure::UnboundedCapacity);
        }
        if maxmemory_policy != "noeviction" {
            return Err(RedisCapacityFailure::EvictionPolicy);
        }
        if maxmemory_bytes != self.configured_capacity_bytes {
            return Err(RedisCapacityFailure::CapacityMismatch);
        }
        let required = used_memory_bytes
            .checked_add(self.role_capacity_sum_bytes)
            .and_then(|value| value.checked_add(self.required_reserve_bytes))
            .ok_or(RedisCapacityFailure::CapacityOverflow)?;
        if required > maxmemory_bytes {
            return Err(RedisCapacityFailure::InsufficientReserve);
        }
        Ok(())
    }

    pub fn projection(&self, used_memory_bytes: u64) -> RedisCapacityProjection {
        RedisCapacityProjection {
            configured_capacity_bytes: self.configured_capacity_bytes,
            used_memory_bytes,
            required_reserve_bytes: self.required_reserve_bytes,
            role_capacity_sum_bytes: self.role_capacity_sum_bytes,
            role_limits: CALIBRATED_ROLE_CAPACITIES
                .iter()
                .map(|calibration| RedisRoleCapacityProjection {
                    role: role_name(calibration.role).to_owned(),
                    protocol_identity: calibration.protocol_identity,
                    accounted_objects: calibration.accounted_objects,
                    terminal_cleanup_boundary: cleanup_boundary_name(calibration.cleanup_boundary),
                    max_bytes: self.role_capacity_bytes(calibration.role),
                    calibrated_minimum_bytes: calibration.minimum_bytes,
                    calibrated_maximum_bytes: calibration.maximum_bytes,
                    protocol_records_bytes: calibration.measurement.protocol_records_bytes,
                    pending_delivery_metadata_bytes: calibration
                        .measurement
                        .pending_delivery_metadata_bytes,
                    script_overhead_bytes: calibration.measurement.script_overhead_bytes,
                    aof_progress_headroom_bytes: calibration
                        .measurement
                        .aof_progress_headroom_bytes,
                    restart_reconstruction_headroom_bytes: calibration
                        .measurement
                        .restart_reconstruction_headroom_bytes,
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisCapacityProjection {
    pub configured_capacity_bytes: u64,
    pub used_memory_bytes: u64,
    pub required_reserve_bytes: u64,
    pub role_capacity_sum_bytes: u64,
    pub role_limits: Vec<RedisRoleCapacityProjection>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisRoleCapacityProjection {
    pub role: String,
    pub protocol_identity: &'static str,
    pub accounted_objects: &'static str,
    pub terminal_cleanup_boundary: &'static str,
    pub max_bytes: u64,
    pub calibrated_minimum_bytes: u64,
    pub calibrated_maximum_bytes: u64,
    pub protocol_records_bytes: u64,
    pub pending_delivery_metadata_bytes: u64,
    pub script_overhead_bytes: u64,
    pub aof_progress_headroom_bytes: u64,
    pub restart_reconstruction_headroom_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisCapacityFailure {
    UnboundedCapacity,
    EvictionPolicy,
    CapacityMismatch,
    MissingRoleLimit,
    DuplicateRoleLimit,
    RoleLimitOutsideBounds,
    InsufficientReserve,
    CapacityOverflow,
}

impl RedisCapacityFailure {
    pub fn description(self) -> &'static str {
        match self {
            Self::UnboundedCapacity => "Redis capacity is not finite",
            Self::EvictionPolicy => "Redis maxmemory policy is not noeviction",
            Self::CapacityMismatch => "Redis capacity does not match the admitted formation",
            Self::MissingRoleLimit => "a mandatory Redis role capacity limit is missing",
            Self::DuplicateRoleLimit => "a Redis role capacity limit is duplicated",
            Self::RoleLimitOutsideBounds => {
                "a Redis role capacity limit is outside calibrated bounds"
            }
            Self::InsufficientReserve => "Redis capacity does not leave the formation reserve",
            Self::CapacityOverflow => "Redis capacity accounting overflowed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisQuotaCleanupBoundary {
    ReplyDeliveredOrExpired,
    DispatchTerminal,
    TaskEventRelayed,
    CancellationTerminal,
    ArchiveCommitted,
    LifecycleSettled,
    LogArchiveCommitted,
    WorkflowArchived,
    IngressTerminal,
    LivenessTerminal,
    NotificationReconciled,
    ObservationExpired,
    EventIngressTerminal,
}

pub const fn terminal_cleanup_boundary(role: CoordinationRole) -> RedisQuotaCleanupBoundary {
    match role {
        CoordinationRole::CommandBus => RedisQuotaCleanupBoundary::ReplyDeliveredOrExpired,
        CoordinationRole::TaskDispatch => RedisQuotaCleanupBoundary::DispatchTerminal,
        CoordinationRole::TaskEvents => RedisQuotaCleanupBoundary::TaskEventRelayed,
        CoordinationRole::TaskCancellation => RedisQuotaCleanupBoundary::CancellationTerminal,
        CoordinationRole::CompactionStaging => RedisQuotaCleanupBoundary::ArchiveCommitted,
        CoordinationRole::LifecycleWork => RedisQuotaCleanupBoundary::LifecycleSettled,
        CoordinationRole::LogStaging => RedisQuotaCleanupBoundary::LogArchiveCommitted,
        CoordinationRole::ScopeStore => RedisQuotaCleanupBoundary::WorkflowArchived,
        CoordinationRole::IngressIdempotencyStore => RedisQuotaCleanupBoundary::IngressTerminal,
        CoordinationRole::LivenessWatchdog => RedisQuotaCleanupBoundary::LivenessTerminal,
        CoordinationRole::SignalAppliedNotifier => {
            RedisQuotaCleanupBoundary::NotificationReconciled
        }
        CoordinationRole::ExecutorFleetStatus => RedisQuotaCleanupBoundary::ObservationExpired,
        CoordinationRole::EventIngress => RedisQuotaCleanupBoundary::EventIngressTerminal,
    }
}

const fn cleanup_boundary_name(boundary: RedisQuotaCleanupBoundary) -> &'static str {
    match boundary {
        RedisQuotaCleanupBoundary::ReplyDeliveredOrExpired => "reply_delivered_or_expired",
        RedisQuotaCleanupBoundary::DispatchTerminal => "dispatch_terminal",
        RedisQuotaCleanupBoundary::TaskEventRelayed => "task_event_relayed",
        RedisQuotaCleanupBoundary::CancellationTerminal => "cancellation_terminal",
        RedisQuotaCleanupBoundary::ArchiveCommitted => "archive_committed",
        RedisQuotaCleanupBoundary::LifecycleSettled => "lifecycle_settled",
        RedisQuotaCleanupBoundary::LogArchiveCommitted => "log_archive_committed",
        RedisQuotaCleanupBoundary::WorkflowArchived => "workflow_archived",
        RedisQuotaCleanupBoundary::IngressTerminal => "ingress_terminal",
        RedisQuotaCleanupBoundary::LivenessTerminal => "liveness_terminal",
        RedisQuotaCleanupBoundary::NotificationReconciled => "notification_reconciled",
        RedisQuotaCleanupBoundary::ObservationExpired => "observation_expired",
        RedisQuotaCleanupBoundary::EventIngressTerminal => "event_ingress_terminal",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisQuotaPressure {
    BelowSoftThreshold,
    SoftThreshold,
    HardLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RedisQuotaState {
    pub used: u64,
    pub soft_threshold: u64,
    pub hard_limit: u64,
    pub accepted_identities: u64,
    pub pressure: RedisQuotaPressure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisQuotaAdmission {
    Accepted(RedisQuotaState),
    Replay(RedisQuotaState),
    Fenced(RedisQuotaState),
}

pub struct RedisQuotaGuard {
    usage_key: String,
    identities_key: String,
    soft_threshold: u64,
    hard_limit: u64,
    cleanup_boundary: RedisQuotaCleanupBoundary,
}

impl RedisQuotaGuard {
    pub fn new(
        namespace: &str,
        role: CoordinationRole,
        soft_threshold: u64,
        hard_limit: u64,
    ) -> Result<Self, RedisQuotaError> {
        if !valid_namespace(namespace)
            || soft_threshold == 0
            || soft_threshold >= hard_limit
            || hard_limit > i64::MAX as u64
        {
            return Err(RedisQuotaError::new(RedisQuotaFailure::InvalidDefinition));
        }
        let role_name = role_name(role);
        let hash_tag = format!("{{{namespace}:{role_name}}}");
        Ok(Self {
            usage_key: format!("tickr:{hash_tag}:quota:used"),
            identities_key: format!("tickr:{hash_tag}:quota:accepted"),
            soft_threshold,
            hard_limit,
            cleanup_boundary: terminal_cleanup_boundary(role),
        })
    }

    pub async fn accept(
        &self,
        connection: &mut MultiplexedConnection,
        identity: &str,
        units: u64,
    ) -> Result<RedisQuotaAdmission, RedisQuotaError> {
        if identity.is_empty() || units == 0 || units > i64::MAX as u64 {
            return Err(RedisQuotaError::new(RedisQuotaFailure::InvalidOperation));
        }
        let result: Vec<i64> = redis::cmd("EVAL")
            .arg(ACCEPT_SCRIPT)
            .arg(2)
            .arg(&self.usage_key)
            .arg(&self.identities_key)
            .arg(identity)
            .arg(units)
            .arg(self.hard_limit)
            .query_async(connection)
            .await
            .map_err(classify_quota_error)?;
        let (disposition, used, accepted_identities) = decode_result(&result)?;
        let state = self.state(used, accepted_identities);
        match disposition {
            0 => Ok(RedisQuotaAdmission::Accepted(state)),
            1 => Ok(RedisQuotaAdmission::Replay(state)),
            2 => Ok(RedisQuotaAdmission::Fenced(state)),
            3 => Err(RedisQuotaError::new(RedisQuotaFailure::IdentityConflict)),
            _ => Err(RedisQuotaError::new(
                RedisQuotaFailure::AccountingInconsistent,
            )),
        }
    }

    pub async fn release_at_terminal_boundary(
        &self,
        connection: &mut MultiplexedConnection,
        identity: &str,
        expected_units: u64,
        boundary: RedisQuotaCleanupBoundary,
    ) -> Result<RedisQuotaState, RedisQuotaError> {
        if boundary != self.cleanup_boundary {
            return Err(RedisQuotaError::new(
                RedisQuotaFailure::UnsafeCleanupBoundary,
            ));
        }
        if identity.is_empty() || expected_units == 0 || expected_units > i64::MAX as u64 {
            return Err(RedisQuotaError::new(RedisQuotaFailure::InvalidOperation));
        }
        let result: Vec<i64> = redis::cmd("EVAL")
            .arg(RELEASE_SCRIPT)
            .arg(2)
            .arg(&self.usage_key)
            .arg(&self.identities_key)
            .arg(identity)
            .arg(expected_units)
            .query_async(connection)
            .await
            .map_err(classify_quota_error)?;
        let (disposition, used, accepted_identities) = decode_result(&result)?;
        match disposition {
            0 => Ok(self.state(used, accepted_identities)),
            1 => Err(RedisQuotaError::new(
                RedisQuotaFailure::MissingAcceptedIdentity,
            )),
            _ => Err(RedisQuotaError::new(
                RedisQuotaFailure::AccountingInconsistent,
            )),
        }
    }

    pub async fn verify_accepted(
        &self,
        connection: &mut MultiplexedConnection,
        identity: &str,
        expected_units: u64,
    ) -> Result<(), RedisQuotaError> {
        let actual: Option<u64> = redis::cmd("HGET")
            .arg(&self.identities_key)
            .arg(identity)
            .query_async(connection)
            .await
            .map_err(classify_quota_error)?;
        match actual {
            Some(actual) if actual == expected_units => Ok(()),
            Some(_) => Err(RedisQuotaError::new(
                RedisQuotaFailure::AccountingInconsistent,
            )),
            None => Err(RedisQuotaError::new(
                RedisQuotaFailure::MissingAcceptedIdentity,
            )),
        }
    }

    pub async fn audit_exact(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisQuotaState, RedisQuotaError> {
        let result: Vec<i64> = redis::cmd("EVAL")
            .arg(AUDIT_SCRIPT)
            .arg(2)
            .arg(&self.usage_key)
            .arg(&self.identities_key)
            .query_async(connection)
            .await
            .map_err(classify_quota_error)?;
        let (disposition, used, accepted_identities) = decode_result(&result)?;
        if disposition != 0 {
            return Err(RedisQuotaError::new(
                RedisQuotaFailure::AccountingInconsistent,
            ));
        }
        Ok(self.state(used, accepted_identities))
    }

    fn state(&self, used: u64, accepted_identities: u64) -> RedisQuotaState {
        let pressure = if used >= self.hard_limit {
            RedisQuotaPressure::HardLimit
        } else if used >= self.soft_threshold {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisQuotaState {
            used,
            soft_threshold: self.soft_threshold,
            hard_limit: self.hard_limit,
            accepted_identities,
            pressure,
        }
    }
}

fn decode_result(result: &[i64]) -> Result<(i64, u64, u64), RedisQuotaError> {
    if result.len() != 3 || result[1] < 0 || result[2] < 0 {
        return Err(RedisQuotaError::new(
            RedisQuotaFailure::AccountingInconsistent,
        ));
    }
    Ok((result[0], result[1] as u64, result[2] as u64))
}

fn classify_quota_error(error: redis::RedisError) -> RedisQuotaError {
    if error.code() == Some("OOM") {
        RedisQuotaError::new(RedisQuotaFailure::OutOfMemory)
    } else if error.kind() == ErrorKind::ReadOnly || error.code() == Some("READONLY") {
        RedisQuotaError::new(RedisQuotaFailure::ReadOnlyPrimary)
    } else {
        RedisQuotaError::new(RedisQuotaFailure::RedisUnavailable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisQuotaFailure {
    InvalidDefinition,
    InvalidOperation,
    IdentityConflict,
    HardLimit,
    UnsafeCleanupBoundary,
    MissingAcceptedIdentity,
    AccountingInconsistent,
    OutOfMemory,
    ReadOnlyPrimary,
    RedisUnavailable,
}

impl RedisQuotaFailure {
    pub fn is_capability_failure(self) -> bool {
        matches!(
            self,
            Self::MissingAcceptedIdentity
                | Self::AccountingInconsistent
                | Self::OutOfMemory
                | Self::ReadOnlyPrimary
                | Self::RedisUnavailable
        )
    }

    fn description(self) -> &'static str {
        match self {
            Self::InvalidDefinition => "Redis quota definition is invalid",
            Self::InvalidOperation => "Redis quota operation is invalid",
            Self::IdentityConflict => "Redis quota identity conflicts with accepted state",
            Self::HardLimit => "Redis quota hard limit is reached",
            Self::UnsafeCleanupBoundary => "Redis quota cleanup boundary is not terminally safe",
            Self::MissingAcceptedIdentity => "Redis quota accepted identity is missing",
            Self::AccountingInconsistent => "Redis quota accounting is inconsistent",
            Self::OutOfMemory => "Redis reported out of memory",
            Self::ReadOnlyPrimary => "Redis primary is read-only",
            Self::RedisUnavailable => "Redis quota operation is unavailable",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RedisQuotaError {
    failure: RedisQuotaFailure,
}

impl RedisQuotaError {
    fn new(failure: RedisQuotaFailure) -> Self {
        Self { failure }
    }

    pub fn failure(&self) -> RedisQuotaFailure {
        self.failure
    }
}

impl fmt::Display for RedisQuotaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.failure.description())
    }
}

impl fmt::Debug for RedisQuotaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisQuotaError")
            .field("failure", &self.failure)
            .finish()
    }
}

impl std::error::Error for RedisQuotaError {}

fn valid_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace.len() <= 63
        && namespace
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn role_name(role: CoordinationRole) -> &'static str {
    match role {
        CoordinationRole::CommandBus => "command-bus",
        CoordinationRole::TaskDispatch => "task-dispatch",
        CoordinationRole::TaskEvents => "task-events",
        CoordinationRole::TaskCancellation => "task-cancellation",
        CoordinationRole::CompactionStaging => "compaction-staging",
        CoordinationRole::LifecycleWork => "lifecycle-work",
        CoordinationRole::LogStaging => "log-staging",
        CoordinationRole::ScopeStore => "scope-store",
        CoordinationRole::IngressIdempotencyStore => "ingress-idempotency-store",
        CoordinationRole::LivenessWatchdog => "liveness-watchdog",
        CoordinationRole::SignalAppliedNotifier => "signal-applied-notifier",
        CoordinationRole::ExecutorFleetStatus => "executor-fleet-status",
        CoordinationRole::EventIngress => "event-ingress",
    }
}

const ACCEPT_SCRIPT: &str = r#"
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
    if tonumber(existing) == tonumber(ARGV[2]) then
        return {1, used, redis.call('HLEN', KEYS[2])}
    end
    return {3, used, redis.call('HLEN', KEYS[2])}
end
local requested = tonumber(ARGV[2])
local hard = tonumber(ARGV[3])
if used < 0 then
    return {4, used, redis.call('HLEN', KEYS[2])}
end
if used + requested > hard then
    return {2, used, redis.call('HLEN', KEYS[2])}
end
redis.call('HSET', KEYS[2], ARGV[1], requested)
redis.call('SET', KEYS[1], used + requested)
return {0, used + requested, redis.call('HLEN', KEYS[2])}
"#;

const RELEASE_SCRIPT: &str = r#"
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if not existing then
    return {1, used, redis.call('HLEN', KEYS[2])}
end
local expected = tonumber(ARGV[2])
if tonumber(existing) ~= expected or used < expected then
    return {2, used, redis.call('HLEN', KEYS[2])}
end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('SET', KEYS[1], used - expected)
return {0, used - expected, redis.call('HLEN', KEYS[2])}
"#;

const AUDIT_SCRIPT: &str = r#"
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local values = redis.call('HVALS', KEYS[2])
local exact = 0
for _, value in ipairs(values) do
    exact = exact + tonumber(value)
end
if used ~= exact then
    return {1, used, #values}
end
return {0, used, #values}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formation::ALL_COORDINATION_ROLES;

    #[test]
    fn calibrated_defaults_cover_every_role_and_fit_two_gibibytes() {
        for role in ALL_COORDINATION_ROLES {
            let calibration = calibrated_role_capacity(role);
            assert_eq!(calibration.role, role);
            assert_eq!(
                calibration.cleanup_boundary,
                terminal_cleanup_boundary(role)
            );
            assert!(calibration.measurement.total_bytes() <= calibration.minimum_bytes);
            assert!(calibration.minimum_bytes < calibration.default_bytes);
            assert_eq!(calibration.default_bytes, calibration.maximum_bytes);
            assert!(!calibration.protocol_identity.is_empty());
            assert!(!calibration.accounted_objects.is_empty());
        }
        let profile = RedisCapacityProfile::default_candidate(2 * 1024 * MIB).unwrap();
        assert!(
            profile.role_capacity_sum_bytes() + profile.required_reserve_bytes()
                <= profile.configured_capacity_bytes()
        );
        let projection = profile.projection(123);
        assert_eq!(projection.used_memory_bytes, 123);
        assert_eq!(projection.role_limits.len(), ALL_COORDINATION_ROLES.len());
        assert_eq!(projection.role_limits[0].role, "command-bus");
        assert_eq!(
            projection.role_limits[0].max_bytes,
            calibrated_role_capacity(CoordinationRole::CommandBus).default_bytes
        );
        assert_eq!(
            projection.role_limits[0].protocol_records_bytes,
            calibrated_role_capacity(CoordinationRole::CommandBus)
                .measurement
                .protocol_records_bytes
        );
    }

    #[test]
    fn profile_rejects_missing_out_of_bounds_and_reserve_consuming_limits() {
        let candidates = CALIBRATED_ROLE_CAPACITIES
            .iter()
            .map(|calibration| RedisRoleCapacityLimit {
                role: calibration.role,
                memory_bytes: calibration.default_bytes,
            })
            .collect::<Vec<_>>();

        let mut missing = candidates.clone();
        missing.pop();
        assert_eq!(
            RedisCapacityProfile::admit(2 * 1024 * MIB, missing),
            Err(RedisCapacityFailure::MissingRoleLimit)
        );

        let mut outside = candidates.clone();
        outside[0].memory_bytes = 1;
        assert_eq!(
            RedisCapacityProfile::admit(2 * 1024 * MIB, outside),
            Err(RedisCapacityFailure::RoleLimitOutsideBounds)
        );

        assert_eq!(
            RedisCapacityProfile::admit(64 * MIB, candidates),
            Err(RedisCapacityFailure::InsufficientReserve)
        );
    }

    #[test]
    fn server_validation_requires_finite_matching_noeviction_capacity_and_reserve() {
        let capacity = 2 * 1024 * MIB;
        let profile = RedisCapacityProfile::default_candidate(capacity).unwrap();
        assert_eq!(
            profile.validate_server(0, "noeviction", 0),
            Err(RedisCapacityFailure::UnboundedCapacity)
        );
        assert_eq!(
            profile.validate_server(capacity, "allkeys-lru", 0),
            Err(RedisCapacityFailure::EvictionPolicy)
        );
        assert_eq!(
            profile.validate_server(3 * 1024 * MIB, "noeviction", 0),
            Err(RedisCapacityFailure::CapacityMismatch)
        );
        assert_eq!(
            profile.validate_server(capacity, "noeviction", 600 * MIB),
            Err(RedisCapacityFailure::InsufficientReserve)
        );
        assert_eq!(profile.validate_server(capacity, "noeviction", 0), Ok(()));
    }

    #[test]
    fn terminal_cleanup_boundaries_are_complete_and_role_specific() {
        let boundaries = ALL_COORDINATION_ROLES.map(terminal_cleanup_boundary);
        for (index, boundary) in boundaries.iter().enumerate() {
            assert!(!boundaries[..index].contains(boundary));
        }
    }
}
