use std::{collections::BTreeMap, fmt};

use redis::aio::MultiplexedConnection;
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::formation::{
    CoordinationRole, ExecutorTopology, FinalLogStore, FormationProfile, HttpCommandIngress,
    ResolvedFormationDescriptor, RoleImplementation, SqlImplementation, Topology, WriterTopology,
    ALL_COORDINATION_ROLES,
};
use crate::redis_capacity::{
    RedisCapacityFailure, RedisCapacityProfile, RedisRoleCapacityLimit, ROLE_MEMORY_LIMIT_NAME,
};
use crate::redis_operation_manifest::{
    RedisOperationManifest, RedisOperationManifestFailure, RedisOperationManifestProjection,
    RedisOperationManifestSet,
};

const IDENTITY_SCHEMA: &str = "tickr.all-redis.formation-identity/v1";
const REDIS_SERVER_CLASS: &str = "redis-oss-7.4.x";
const REDIS_TOPOLOGY_CLASS: &str = "single-writable-primary";
const CANARY_TTL_MILLIS: u64 = 30_000;

/// The operator-selected identity of one dedicated greenfield Tickr Redis namespace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisNamespaceIdentity(String);

impl RedisNamespaceIdentity {
    pub fn new(value: impl Into<String>) -> Result<Self, RedisFormationIdentityError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 63
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
        if !valid {
            return Err(RedisFormationIdentityError::new(
                RedisFormationIdentityFailure::InvalidNamespaceIdentity,
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn key_prefix(&self) -> String {
        format!("tickr:{}:", self.0)
    }
}

/// Named, role-owned limits included in the capability fingerprint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisRoleLimits {
    pub role: CoordinationRole,
    quotas: BTreeMap<String, u64>,
    retention_limits: BTreeMap<String, u64>,
}

impl RedisRoleLimits {
    pub fn new(
        role: CoordinationRole,
        quotas: BTreeMap<String, u64>,
        retention_limits: BTreeMap<String, u64>,
    ) -> Result<Self, RedisFormationIdentityError> {
        if quotas.is_empty() || retention_limits.is_empty() {
            return Err(RedisFormationIdentityError::new(
                RedisFormationIdentityFailure::IncompleteRoleLimits,
            ));
        }
        if quotas
            .iter()
            .chain(retention_limits.iter())
            .any(|(name, value)| !valid_limit_name(name) || *value == 0)
        {
            return Err(RedisFormationIdentityError::new(
                RedisFormationIdentityFailure::InvalidRoleLimit,
            ));
        }
        Ok(Self {
            role,
            quotas,
            retention_limits,
        })
    }

    pub fn quotas(&self) -> &BTreeMap<String, u64> {
        &self.quotas
    }

    pub fn retention_limits(&self) -> &BTreeMap<String, u64> {
        &self.retention_limits
    }
}

fn valid_limit_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

/// Configuration whose change can alter the admitted Redis durability guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RedisDurabilityConfiguration {
    pub aof_enabled: bool,
    pub append_fsync_always: bool,
    pub required_local_fsyncs: u16,
    pub required_replica_acknowledgements: u16,
    pub noeviction: bool,
    pub configured_capacity_bytes: u64,
}

impl RedisDurabilityConfiguration {
    pub const fn primary_local_aof(configured_capacity_bytes: u64) -> Self {
        Self {
            aof_enabled: true,
            append_fsync_always: true,
            required_local_fsyncs: 1,
            required_replica_acknowledgements: 0,
            noeviction: true,
            configured_capacity_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NormalizedFormationIdentity {
    pub schema: String,
    pub profile: String,
    pub namespace_identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CapabilityFingerprintProjection {
    pub profile: String,
    pub topology: String,
    pub sql: String,
    pub sql_migration_identity: ProtocolProjection,
    pub final_logs: String,
    pub writer_topology: String,
    pub executors: String,
    pub http_commands: String,
    pub roles: Vec<RoleProjection>,
    pub operation_manifests: Vec<RedisOperationManifestProjection>,
    pub choreography: ChoreographyProjection,
    pub redis_capability_class: RedisCapabilityClassProjection,
    pub namespace_identity: String,
    pub role_limits: Vec<RoleLimitsProjection>,
    pub durability: RedisDurabilityConfiguration,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProtocolProjection {
    pub name: String,
    pub version: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RoleProjection {
    pub role: String,
    pub implementation: String,
    pub protocol: ProtocolProjection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ChoreographyProjection {
    pub safe_pickup_handoff: bool,
    pub safe_attempt_outcome_handoff: bool,
    pub safe_cancellation_fence: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisCapabilityClassProjection {
    pub server: String,
    pub topology: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RoleLimitsProjection {
    pub role: String,
    pub quotas: BTreeMap<String, u64>,
    pub retention_limits: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityFingerprint(String);

impl CapabilityFingerprint {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Side-effect-free identity and fingerprint values awaiting complete admission proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisFormationAdmissionCandidate {
    descriptor: ResolvedFormationDescriptor,
    operation_manifests: RedisOperationManifestSet,
    namespace: RedisNamespaceIdentity,
    normalized_identity: NormalizedFormationIdentity,
    normalized_identity_json: String,
    fingerprint_projection: CapabilityFingerprintProjection,
    capability_fingerprint: CapabilityFingerprint,
    capacity_profile: RedisCapacityProfile,
}

impl RedisFormationAdmissionCandidate {
    pub fn construct(
        descriptor: &ResolvedFormationDescriptor,
        operation_manifests: Vec<RedisOperationManifest>,
        namespace: RedisNamespaceIdentity,
        role_limits: Vec<RedisRoleLimits>,
        durability: RedisDurabilityConfiguration,
    ) -> Result<Self, RedisFormationIdentityError> {
        if descriptor.profile != FormationProfile::AllRedis
            || descriptor
                .roles
                .iter()
                .any(|role| role.implementation != RoleImplementation::Redis)
        {
            return Err(RedisFormationIdentityError::new(
                RedisFormationIdentityFailure::NotAllRedisFormation,
            ));
        }
        let operation_manifests = RedisOperationManifestSet::admit(descriptor, operation_manifests)
            .map_err(|error| {
                RedisFormationIdentityError::new(
                    RedisFormationIdentityFailure::InvalidOperationManifest(error.failure()),
                )
            })?;
        let capacity_profile = RedisCapacityProfile::admit(
            durability.configured_capacity_bytes,
            role_limits
                .iter()
                .map(|limits| {
                    limits
                        .quotas
                        .get(ROLE_MEMORY_LIMIT_NAME)
                        .copied()
                        .map(|memory_bytes| RedisRoleCapacityLimit {
                            role: limits.role,
                            memory_bytes,
                        })
                        .ok_or(RedisCapacityFailure::MissingRoleLimit)
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|failure| {
                    RedisFormationIdentityError::new(
                        RedisFormationIdentityFailure::InvalidRoleCapacity(failure),
                    )
                })?,
        )
        .map_err(|failure| {
            RedisFormationIdentityError::new(RedisFormationIdentityFailure::InvalidRoleCapacity(
                failure,
            ))
        })?;

        let normalized_limits = normalize_role_limits(role_limits)?;
        let normalized_identity = NormalizedFormationIdentity {
            schema: IDENTITY_SCHEMA.to_owned(),
            profile: descriptor.profile.name().to_owned(),
            namespace_identity: namespace.as_str().to_owned(),
        };
        let fingerprint_projection = CapabilityFingerprintProjection {
            profile: descriptor.profile.name().to_owned(),
            topology: topology_name(descriptor.topology).to_owned(),
            sql: sql_name(descriptor.sql).to_owned(),
            sql_migration_identity: ProtocolProjection {
                name: descriptor.sql_migration_identity.name.to_owned(),
                version: descriptor.sql_migration_identity.version,
            },
            final_logs: final_log_name(descriptor.final_logs).to_owned(),
            writer_topology: writer_topology_name(descriptor.writer_topology).to_owned(),
            executors: executor_topology_name(descriptor.executors),
            http_commands: http_command_name(descriptor.http_commands).to_owned(),
            roles: descriptor
                .roles
                .iter()
                .map(|role| RoleProjection {
                    role: role_name(role.role).to_owned(),
                    implementation: role_implementation_name(role.implementation).to_owned(),
                    protocol: ProtocolProjection {
                        name: role.protocol.name.to_owned(),
                        version: role.protocol.version,
                    },
                })
                .collect(),
            operation_manifests: operation_manifests.identity_projection(),
            choreography: ChoreographyProjection {
                safe_pickup_handoff: descriptor.choreography.safe_pickup_handoff,
                safe_attempt_outcome_handoff: descriptor.choreography.safe_attempt_outcome_handoff,
                safe_cancellation_fence: descriptor.choreography.safe_cancellation_fence,
            },
            redis_capability_class: RedisCapabilityClassProjection {
                server: REDIS_SERVER_CLASS.to_owned(),
                topology: REDIS_TOPOLOGY_CLASS.to_owned(),
            },
            namespace_identity: namespace.as_str().to_owned(),
            role_limits: normalized_limits,
            durability,
        };

        let normalized_identity_json =
            serde_json::to_string(&normalized_identity).map_err(|_| {
                RedisFormationIdentityError::new(RedisFormationIdentityFailure::NormalizationFailed)
            })?;
        let projection_json = serde_json::to_vec(&fingerprint_projection).map_err(|_| {
            RedisFormationIdentityError::new(RedisFormationIdentityFailure::NormalizationFailed)
        })?;
        let capability_fingerprint =
            CapabilityFingerprint(format!("{:x}", Sha256::digest(projection_json)));

        Ok(Self {
            descriptor: *descriptor,
            namespace,
            operation_manifests,
            normalized_identity,
            normalized_identity_json,
            fingerprint_projection,
            capability_fingerprint,
            capacity_profile,
        })
    }

    pub const fn descriptor(&self) -> &ResolvedFormationDescriptor {
        &self.descriptor
    }

    pub fn namespace(&self) -> &RedisNamespaceIdentity {
        &self.namespace
    }

    pub fn operation_manifests(&self) -> &RedisOperationManifestSet {
        &self.operation_manifests
    }

    pub fn normalized_identity(&self) -> &NormalizedFormationIdentity {
        &self.normalized_identity
    }

    pub fn normalized_identity_json(&self) -> &str {
        &self.normalized_identity_json
    }

    pub fn fingerprint_projection(&self) -> &CapabilityFingerprintProjection {
        &self.fingerprint_projection
    }

    pub fn capability_fingerprint(&self) -> &CapabilityFingerprint {
        &self.capability_fingerprint
    }

    pub fn capacity_profile(&self) -> &RedisCapacityProfile {
        &self.capacity_profile
    }

    pub fn identity_key(&self) -> String {
        format!("{}formation:identity", self.namespace.key_prefix())
    }

    pub fn fingerprint_key(&self) -> String {
        format!(
            "{}formation:capability-fingerprint",
            self.namespace.key_prefix()
        )
    }
}

fn normalize_role_limits(
    role_limits: Vec<RedisRoleLimits>,
) -> Result<Vec<RoleLimitsProjection>, RedisFormationIdentityError> {
    let mut by_role = BTreeMap::new();
    for limits in role_limits {
        if by_role.insert(limits.role as usize, limits).is_some() {
            return Err(RedisFormationIdentityError::new(
                RedisFormationIdentityFailure::DuplicateRoleLimits,
            ));
        }
    }

    ALL_COORDINATION_ROLES
        .iter()
        .map(|role| {
            let limits = by_role.remove(&(*role as usize)).ok_or_else(|| {
                RedisFormationIdentityError::new(
                    RedisFormationIdentityFailure::IncompleteRoleLimits,
                )
            })?;
            Ok(RoleLimitsProjection {
                role: role_name(*role).to_owned(),
                quotas: limits.quotas,
                retention_limits: limits.retention_limits,
            })
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisNamespaceInspection {
    Empty,
    Matching,
}

struct RedisNamespaceSnapshot {
    identity: Option<String>,
    fingerprint: Option<String>,
    tickr_key_count: usize,
}

/// Reads only the selected Tickr namespace and never installs candidate identity.
pub async fn inspect_redis_namespace(
    connection: &mut MultiplexedConnection,
    candidate: &RedisFormationAdmissionCandidate,
) -> Result<RedisNamespaceInspection, RedisFormationIdentityError> {
    let identity = redis::cmd("GET")
        .arg(candidate.identity_key())
        .query_async(connection)
        .await
        .map_err(|_| {
            RedisFormationIdentityError::new(RedisFormationIdentityFailure::NamespaceReadFailed)
        })?;
    let fingerprint = redis::cmd("GET")
        .arg(candidate.fingerprint_key())
        .query_async(connection)
        .await
        .map_err(|_| {
            RedisFormationIdentityError::new(RedisFormationIdentityFailure::NamespaceReadFailed)
        })?;
    let tickr_key_count = count_namespace_keys(connection, candidate.namespace()).await?;

    inspect_namespace_snapshot(
        candidate,
        RedisNamespaceSnapshot {
            identity,
            fingerprint,
            tickr_key_count,
        },
    )
}

async fn count_namespace_keys(
    connection: &mut MultiplexedConnection,
    namespace: &RedisNamespaceIdentity,
) -> Result<usize, RedisFormationIdentityError> {
    let prefix = namespace.key_prefix();
    let pattern = format!("{prefix}*");
    let canary_prefix = format!("{prefix}admission:canary:");
    let mut cursor = 0_u64;
    let mut count = 0_usize;
    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(128)
            .query_async(&mut *connection)
            .await
            .map_err(|_| {
                RedisFormationIdentityError::new(RedisFormationIdentityFailure::NamespaceReadFailed)
            })?;
        let durable_keys = keys
            .iter()
            .filter(|key| is_formation_state_key(key, &canary_prefix))
            .count();
        count = count.checked_add(durable_keys).ok_or_else(|| {
            RedisFormationIdentityError::new(RedisFormationIdentityFailure::NamespaceReadFailed)
        })?;
        cursor = next_cursor;
        if cursor == 0 {
            return Ok(count);
        }
    }
}

fn is_formation_state_key(key: &str, canary_prefix: &str) -> bool {
    !key.starts_with(canary_prefix)
}

fn inspect_namespace_snapshot(
    candidate: &RedisFormationAdmissionCandidate,
    snapshot: RedisNamespaceSnapshot,
) -> Result<RedisNamespaceInspection, RedisFormationIdentityError> {
    if snapshot.tickr_key_count == 0
        && snapshot.identity.is_none()
        && snapshot.fingerprint.is_none()
    {
        return Ok(RedisNamespaceInspection::Empty);
    }
    let identity = snapshot.identity.ok_or_else(|| {
        RedisFormationIdentityError::new(RedisFormationIdentityFailure::MissingFormationIdentity)
    })?;
    let fingerprint = snapshot.fingerprint.ok_or_else(|| {
        RedisFormationIdentityError::new(
            RedisFormationIdentityFailure::MissingCapabilityFingerprint,
        )
    })?;
    if identity != candidate.normalized_identity_json() {
        return Err(RedisFormationIdentityError::new(
            RedisFormationIdentityFailure::FormationIdentityMismatch,
        ));
    }
    if fingerprint != candidate.capability_fingerprint().as_str() {
        return Err(RedisFormationIdentityError::new(
            RedisFormationIdentityFailure::CapabilityFingerprintMismatch,
        ));
    }
    Ok(RedisNamespaceInspection::Matching)
}

/// Runs one bounded write/read/delete canary wholly inside the selected Tickr namespace.
pub async fn prove_redis_probe_canary(
    connection: &mut MultiplexedConnection,
    candidate: &RedisFormationAdmissionCandidate,
    purpose: &str,
) -> Result<(), RedisFormationIdentityError> {
    let (key, token) = new_probe_canary(candidate.namespace(), purpose)?;
    let installed: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg(&token)
        .arg("NX")
        .arg("PX")
        .arg(CANARY_TTL_MILLIS)
        .query_async(&mut *connection)
        .await
        .map_err(|_| {
            RedisFormationIdentityError::new(RedisFormationIdentityFailure::CanaryWriteFailed)
        })?;
    if installed.as_deref() != Some("OK") {
        return Err(RedisFormationIdentityError::new(
            RedisFormationIdentityFailure::CanaryCollision,
        ));
    }

    let proof: Result<Option<String>, _> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut *connection)
        .await;
    let cleanup: Result<u64, _> = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut *connection)
        .await;
    if !matches!(cleanup, Ok(1)) {
        return Err(RedisFormationIdentityError::new(
            RedisFormationIdentityFailure::CanaryCleanupFailed,
        ));
    }
    match proof {
        Ok(Some(actual)) if actual == token => Ok(()),
        _ => Err(RedisFormationIdentityError::new(
            RedisFormationIdentityFailure::CanaryProofFailed,
        )),
    }
}

fn new_probe_canary(
    namespace: &RedisNamespaceIdentity,
    purpose: &str,
) -> Result<(String, String), RedisFormationIdentityError> {
    if !valid_limit_name(purpose) {
        return Err(RedisFormationIdentityError::new(
            RedisFormationIdentityFailure::InvalidCanaryPurpose,
        ));
    }
    let key = format!(
        "{}admission:canary:{purpose}:{}",
        namespace.key_prefix(),
        Uuid::new_v4().simple()
    );
    Ok((key, Uuid::new_v4().simple().to_string()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisFormationIdentityFailure {
    InvalidNamespaceIdentity,
    NotAllRedisFormation,
    InvalidOperationManifest(RedisOperationManifestFailure),
    IncompleteRoleLimits,
    DuplicateRoleLimits,
    InvalidRoleLimit,
    InvalidRoleCapacity(RedisCapacityFailure),
    NormalizationFailed,
    NamespaceReadFailed,
    MissingFormationIdentity,
    MissingCapabilityFingerprint,
    FormationIdentityMismatch,
    CapabilityFingerprintMismatch,
    InvalidCanaryPurpose,
    CanaryWriteFailed,
    CanaryCollision,
    CanaryProofFailed,
    CanaryCleanupFailed,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RedisFormationIdentityError {
    failure: RedisFormationIdentityFailure,
}

impl RedisFormationIdentityError {
    fn new(failure: RedisFormationIdentityFailure) -> Self {
        Self { failure }
    }

    pub fn failure(&self) -> RedisFormationIdentityFailure {
        self.failure
    }
}

impl fmt::Display for RedisFormationIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = match self.failure {
            RedisFormationIdentityFailure::InvalidNamespaceIdentity => {
                "namespace identity is not normalized"
            }
            RedisFormationIdentityFailure::NotAllRedisFormation => {
                "the Resolved formation descriptor is not all-Redis"
            }
            RedisFormationIdentityFailure::InvalidOperationManifest(failure) => {
                failure.description()
            }
            RedisFormationIdentityFailure::IncompleteRoleLimits => {
                "every coordination role requires quota and retention limits"
            }
            RedisFormationIdentityFailure::DuplicateRoleLimits => {
                "coordination role limits are duplicated"
            }
            RedisFormationIdentityFailure::InvalidRoleLimit => {
                "a coordination role limit is invalid"
            }
            RedisFormationIdentityFailure::InvalidRoleCapacity(failure) => failure.description(),
            RedisFormationIdentityFailure::NormalizationFailed => {
                "formation identity normalization failed"
            }
            RedisFormationIdentityFailure::NamespaceReadFailed => {
                "Tickr Redis namespace inspection failed"
            }
            RedisFormationIdentityFailure::MissingFormationIdentity => {
                "nonempty Tickr Redis namespace has no formation identity"
            }
            RedisFormationIdentityFailure::MissingCapabilityFingerprint => {
                "nonempty Tickr Redis namespace has no capability fingerprint"
            }
            RedisFormationIdentityFailure::FormationIdentityMismatch => {
                "Tickr Redis formation identity does not match"
            }
            RedisFormationIdentityFailure::CapabilityFingerprintMismatch => {
                "Tickr Redis capability fingerprint does not match"
            }
            RedisFormationIdentityFailure::InvalidCanaryPurpose => {
                "Redis probe canary purpose is invalid"
            }
            RedisFormationIdentityFailure::CanaryWriteFailed => "Redis probe canary write failed",
            RedisFormationIdentityFailure::CanaryCollision => {
                "Redis probe canary identity collided"
            }
            RedisFormationIdentityFailure::CanaryProofFailed => "Redis probe canary proof failed",
            RedisFormationIdentityFailure::CanaryCleanupFailed => {
                "Redis probe canary cleanup failed"
            }
        };
        write!(
            formatter,
            "Redis formation identity admission failed: {description}"
        )
    }
}

impl fmt::Debug for RedisFormationIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisFormationIdentityError")
            .field("failure", &self.failure)
            .finish()
    }
}

impl std::error::Error for RedisFormationIdentityError {}

fn topology_name(value: Topology) -> &'static str {
    match value {
        Topology::Distributed => "distributed",
        Topology::SingleNode => "single-node",
    }
}

fn sql_name(value: SqlImplementation) -> &'static str {
    match value {
        SqlImplementation::Postgres => "postgres",
        SqlImplementation::Sqlite => "sqlite",
    }
}

fn final_log_name(value: FinalLogStore) -> &'static str {
    match value {
        FinalLogStore::ObjectStore => "object-store",
        FinalLogStore::LocalFiles => "local-files",
    }
}

fn writer_topology_name(value: WriterTopology) -> &'static str {
    match value {
        WriterTopology::Distributed => "distributed",
        WriterTopology::ConductorOwned => "conductor-owned",
    }
}

fn executor_topology_name(value: ExecutorTopology) -> String {
    match value {
        ExecutorTopology::DistributedFleet => "distributed-fleet".to_owned(),
        ExecutorTopology::Exactly(count) => format!("exactly-{count}"),
    }
}

fn http_command_name(value: HttpCommandIngress) -> &'static str {
    match value {
        HttpCommandIngress::Enabled => "enabled",
        HttpCommandIngress::Disabled => "disabled",
    }
}

fn role_implementation_name(value: RoleImplementation) -> &'static str {
    match value {
        RoleImplementation::NatsJetStream => "nats-jetstream",
        RoleImplementation::Redis => "redis",
        RoleImplementation::LocalRequestReply => "local-request-reply",
        RoleImplementation::LocalSqlite => "local-sqlite",
        RoleImplementation::LocalJournal => "local-journal",
        RoleImplementation::LocalNotification => "local-notification",
        RoleImplementation::LocalObservation => "local-observation",
        RoleImplementation::Disabled => "disabled",
    }
}

fn role_name(value: CoordinationRole) -> &'static str {
    match value {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formation::{resolve_formation, FormationSelection};
    use crate::redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation,
        RedisRequiredOperationCanary,
    };

    const TEST_CAPACITY_BYTES: u64 = 2_000_000_000;

    fn role_limits() -> Vec<RedisRoleLimits> {
        ALL_COORDINATION_ROLES
            .iter()
            .enumerate()
            .map(|(index, role)| {
                RedisRoleLimits::new(
                    *role,
                    BTreeMap::from([
                        (
                            "max-bytes".to_owned(),
                            crate::redis_capacity::calibrated_role_capacity(*role).default_bytes,
                        ),
                        ("max-records".to_owned(), 1_000 + index as u64),
                    ]),
                    BTreeMap::from([
                        ("completed-seconds".to_owned(), 3_600 + index as u64),
                        ("pending-seconds".to_owned(), 600 + index as u64),
                    ]),
                )
                .unwrap()
            })
            .collect()
    }

    fn manifest_pattern(role: CoordinationRole) -> &'static str {
        match role {
            CoordinationRole::CommandBus => "tickr:{namespace}:command-bus:records:*",
            CoordinationRole::TaskDispatch => "tickr:{namespace}:task-dispatch:records:*",
            CoordinationRole::TaskEvents => "tickr:{namespace}:task-events:records:*",
            CoordinationRole::TaskCancellation => "tickr:{namespace}:task-cancellation:records:*",
            CoordinationRole::CompactionStaging => "tickr:{namespace}:compaction-staging:records:*",
            CoordinationRole::LifecycleWork => "tickr:{namespace}:lifecycle-work:records:*",
            CoordinationRole::LogStaging => "tickr:{namespace}:log-staging:records:*",
            CoordinationRole::ScopeStore => "tickr:{namespace}:scope-store:records:*",
            CoordinationRole::IngressIdempotencyStore => {
                "tickr:{namespace}:ingress-idempotency-store:records:*"
            }
            CoordinationRole::LivenessWatchdog => "tickr:{namespace}:liveness-watchdog:records:*",
            CoordinationRole::SignalAppliedNotifier => {
                "tickr:{namespace}:signal-applied-notifier:records:*"
            }
            CoordinationRole::ExecutorFleetStatus => {
                "tickr:{namespace}:executor-fleet-status:records:*"
            }
            CoordinationRole::EventIngress => "tickr:{namespace}:event-ingress:records:*",
        }
    }

    fn operation_manifests(
        descriptor: &ResolvedFormationDescriptor,
    ) -> Vec<RedisOperationManifest> {
        ALL_COORDINATION_ROLES
            .iter()
            .map(|role| {
                let pattern = manifest_pattern(*role);
                let cross_role = if *role == CoordinationRole::CommandBus {
                    CoordinationRole::TaskDispatch
                } else {
                    CoordinationRole::CommandBus
                };
                RedisOperationManifest::new(
                    *role,
                    descriptor.roles.get(*role).protocol,
                    vec!["GET", "SET"],
                    vec![],
                    vec![pattern],
                    vec![],
                    vec![RedisRequiredOperationCanary::new(
                        RedisOperation::command("SET"),
                        RedisNamespacePattern::key(pattern),
                    )],
                    vec![
                        RedisForbiddenOperation::cross_role(
                            RedisOperation::command("GET"),
                            cross_role,
                        ),
                        RedisForbiddenOperation::administrative("CONFIG GET"),
                    ],
                )
                .unwrap()
            })
            .collect()
    }

    fn candidate(
        role_limits: Vec<RedisRoleLimits>,
        durability: RedisDurabilityConfiguration,
    ) -> RedisFormationAdmissionCandidate {
        let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
        RedisFormationAdmissionCandidate::construct(
            &descriptor,
            operation_manifests(&descriptor),
            RedisNamespaceIdentity::new("formation-a").unwrap(),
            role_limits,
            durability,
        )
        .unwrap()
    }

    #[test]
    fn fingerprint_normalization_is_stable_and_complete() {
        let durability = RedisDurabilityConfiguration::primary_local_aof(TEST_CAPACITY_BYTES);
        let first = candidate(role_limits(), durability);
        let mut reversed = role_limits();
        reversed.reverse();
        let second = candidate(reversed, durability);

        assert_eq!(
            first.capability_fingerprint(),
            second.capability_fingerprint()
        );
        let projection = first.fingerprint_projection();
        assert_eq!(projection.profile, "all-redis");
        assert_eq!(projection.topology, "distributed");
        assert_eq!(projection.roles.len(), ALL_COORDINATION_ROLES.len());
        assert!(projection
            .roles
            .iter()
            .all(|role| role.implementation == "redis" && role.protocol.version == 1));
        assert!(projection.choreography.safe_pickup_handoff);
        assert!(projection.choreography.safe_attempt_outcome_handoff);
        assert!(projection.choreography.safe_cancellation_fence);
        assert_eq!(projection.redis_capability_class.server, REDIS_SERVER_CLASS);
        assert_eq!(projection.namespace_identity, "formation-a");
        assert_eq!(projection.role_limits.len(), ALL_COORDINATION_ROLES.len());
        assert_eq!(projection.durability, durability);
    }

    #[test]
    fn fingerprint_projection_has_no_location_or_secret_input() {
        let candidate = candidate(
            role_limits(),
            RedisDurabilityConfiguration::primary_local_aof(TEST_CAPACITY_BYTES),
        );
        let diagnostic = format!(
            "{} {} {}",
            serde_json::to_string(candidate.normalized_identity()).unwrap(),
            serde_json::to_string(candidate.fingerprint_projection()).unwrap(),
            candidate.capability_fingerprint().as_str()
        );
        for forbidden in [
            "rediss://redis.internal:6379",
            "role-user",
            "very-secret-password",
            "credential=",
            "BEGIN CERTIFICATE",
            "trust-root",
        ] {
            assert!(!diagnostic.contains(forbidden));
        }
    }

    #[test]
    fn exact_identity_and_fingerprint_match_or_fail_closed() {
        let candidate = candidate(
            role_limits(),
            RedisDurabilityConfiguration::primary_local_aof(TEST_CAPACITY_BYTES),
        );
        assert_eq!(
            inspect_namespace_snapshot(
                &candidate,
                RedisNamespaceSnapshot {
                    identity: None,
                    fingerprint: None,
                    tickr_key_count: 0,
                }
            )
            .unwrap(),
            RedisNamespaceInspection::Empty
        );
        assert_eq!(
            inspect_namespace_snapshot(
                &candidate,
                RedisNamespaceSnapshot {
                    identity: Some(candidate.normalized_identity_json().to_owned()),
                    fingerprint: Some(candidate.capability_fingerprint().as_str().to_owned()),
                    tickr_key_count: 7,
                }
            )
            .unwrap(),
            RedisNamespaceInspection::Matching
        );

        for (snapshot, failure) in [
            (
                RedisNamespaceSnapshot {
                    identity: None,
                    fingerprint: None,
                    tickr_key_count: 1,
                },
                RedisFormationIdentityFailure::MissingFormationIdentity,
            ),
            (
                RedisNamespaceSnapshot {
                    identity: Some(candidate.normalized_identity_json().to_owned()),
                    fingerprint: None,
                    tickr_key_count: 1,
                },
                RedisFormationIdentityFailure::MissingCapabilityFingerprint,
            ),
            (
                RedisNamespaceSnapshot {
                    identity: Some("unrelated-tickr-state".to_owned()),
                    fingerprint: Some(candidate.capability_fingerprint().as_str().to_owned()),
                    tickr_key_count: 2,
                },
                RedisFormationIdentityFailure::FormationIdentityMismatch,
            ),
            (
                RedisNamespaceSnapshot {
                    identity: Some(candidate.normalized_identity_json().to_owned()),
                    fingerprint: Some("changed-role-protocol-v2".to_owned()),
                    tickr_key_count: 2,
                },
                RedisFormationIdentityFailure::CapabilityFingerprintMismatch,
            ),
        ] {
            assert_eq!(
                inspect_namespace_snapshot(&candidate, snapshot)
                    .unwrap_err()
                    .failure(),
                failure
            );
        }
    }

    #[test]
    fn durability_change_requires_a_fresh_namespace() {
        let baseline = candidate(
            role_limits(),
            RedisDurabilityConfiguration::primary_local_aof(TEST_CAPACITY_BYTES),
        );
        let mut changed_durability =
            RedisDurabilityConfiguration::primary_local_aof(TEST_CAPACITY_BYTES);
        changed_durability.required_local_fsyncs = 2;
        let changed = candidate(role_limits(), changed_durability);
        assert_ne!(
            baseline.capability_fingerprint(),
            changed.capability_fingerprint()
        );
        assert_eq!(
            inspect_namespace_snapshot(
                &changed,
                RedisNamespaceSnapshot {
                    identity: Some(baseline.normalized_identity_json().to_owned()),
                    fingerprint: Some(baseline.capability_fingerprint().as_str().to_owned()),
                    tickr_key_count: 2,
                }
            )
            .unwrap_err()
            .failure(),
            RedisFormationIdentityFailure::CapabilityFingerprintMismatch
        );
    }

    #[test]
    fn admission_canary_residue_is_not_formation_state() {
        let canary_prefix = "tickr:formation-a:admission:canary:";
        assert!(!is_formation_state_key(
            "tickr:formation-a:admission:canary:durability:attempt",
            canary_prefix,
        ));
        assert!(is_formation_state_key(
            "tickr:formation-a:task-events:accepted",
            canary_prefix,
        ));
    }

    #[test]
    fn candidate_construction_is_side_effect_free_and_canaries_are_namespaced() {
        let candidate = candidate(
            role_limits(),
            RedisDurabilityConfiguration::primary_local_aof(TEST_CAPACITY_BYTES),
        );
        let first = new_probe_canary(candidate.namespace(), "durability").unwrap();
        let second = new_probe_canary(candidate.namespace(), "durability").unwrap();

        assert_ne!(first, second);
        assert!(first
            .0
            .starts_with("tickr:formation-a:admission:canary:durability:"));
        assert!(!first.0.contains("nats"));
        assert_eq!(
            candidate.normalized_identity().namespace_identity,
            "formation-a"
        );
    }
}
