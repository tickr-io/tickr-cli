use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    marker::PhantomData,
    num::NonZeroUsize,
    sync::Arc,
};

use redis::{aio::MultiplexedConnection, FromRedisValue, Value};
use tickr_api::commands::client::CommandBus;
use tickr_conductor::api_commands_consumer::CommandBusConsumer;
use tickr_conductor::ingress_idempotency::{IngressCoordinator, IngressIdempotencyStore};
use tickr_conductor::lifecycle_work::LifecycleWork;
use tickr_conductor::nats_ingress::EventIngress;
use tickr_conductor::signal_applied_notifier::SignalAppliedNotificationRoles;
use tickr_conductor::system_tasks::compaction_receiver::{
    CompactionScopeSnapshotReader, RoleCompactionScopeSnapshotReader,
};
use tickr_conductor::system_tasks::CompactionLogStaging;
use tickr_executor::local_pickup::{
    ExecutorFleetStatus, SafeAttemptOutcomeHandoff, SafeCancellationRole, SafeHandoffCoordinator,
    SafeLivenessDeadlineSweeper, SafeLivenessWatchdog, SafePickupWriter,
};
use tickr_executor::log_stream::LogStreamProvider;
use tickr_migrations::{backend::WriterRepositoryBundle, scope_repository::ScopeStore};
use tickr_proto::coord::{
    CompactionStaging, TaskCancellationAckConsumer, TaskCancellationPublisher,
    TaskDispatchPublisher, TaskEventConsumer, TaskEventWriter,
};
use tokio_util::sync::CancellationToken;

use crate::{
    formation::{CoordinationRole, ALL_COORDINATION_ROLES},
    redis_admission::{
        admit_redis_capability, AdmittedRedisCapability, RedisAdmissionFailure,
        RedisConnectionDescriptor,
    },
    redis_capability_monitor::{
        RedisCapabilityDiagnostics, RedisCapabilityMonitor, RedisCapabilityMonitorError,
        RedisRoleCapabilityRegistration, RedisRoleRegistrationError,
    },
    redis_command_bus::{
        redis_command_bus_operation_manifest, MonitoredRedisCommandCapability, RedisCommandBus,
        RedisCommandBusConfig, RedisCommandBusError, RedisCommandBusRoleRegistration,
    },
    redis_compaction_staging::{
        redis_compaction_staging_operation_manifest, MonitoredRedisCompactionStagingCapability,
        RedisCompactionStaging, RedisCompactionStagingConfig, RedisCompactionStagingError,
        RedisCompactionStagingRoleRegistration,
    },
    redis_durability::RedisDurabilityGuard,
    redis_event_ingress::{
        redis_event_ingress_operation_manifest, MonitoredRedisEventIngressCapability,
        RedisEventIngress, RedisEventIngressConfig, RedisEventIngressError,
        RedisEventIngressRoleRegistration,
    },
    redis_executor_fleet_status::{
        redis_executor_fleet_status_operation_manifest,
        MonitoredRedisExecutorFleetStatusCapability, RedisExecutorFleetStatus,
        RedisExecutorFleetStatusConfig, RedisExecutorFleetStatusError,
        RedisExecutorFleetStatusRoleRegistration,
    },
    redis_formation_identity::{
        inspect_redis_namespace, RedisFormationAdmissionCandidate, RedisFormationIdentityFailure,
        RedisNamespaceInspection,
    },
    redis_ingress_idempotency::{
        redis_ingress_idempotency_operation_manifest, MonitoredRedisIngressIdempotencyCapability,
        RedisIngressIdempotencyConfig, RedisIngressIdempotencyError,
        RedisIngressIdempotencyRoleRegistration,
    },
    redis_lifecycle_work::{
        redis_lifecycle_work_operation_manifest, MonitoredRedisLifecycleWorkCapability,
        RedisLifecycleReconstruction, RedisLifecycleWorkConfig, RedisLifecycleWorkError,
        RedisLifecycleWorkRoleRegistration,
    },
    redis_log_staging::{
        redis_log_staging_operation_manifest, MonitoredRedisLogStagingCapability,
        RedisLogStagingConfig, RedisLogStagingError, RedisLogStagingRoleRegistration,
    },
    redis_operation_manifest::{
        RedisForbiddenTarget, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisOperationManifestSet,
        RedisScriptIdentity,
    },
    redis_scope_store::{
        redis_scope_store_operation_manifest, MonitoredRedisScopeStoreCapability,
        RedisScopeStoreConfig, RedisScopeStoreError, RedisScopeStoreRoleRegistration,
    },
    redis_signal_applied_notifier::{
        redis_signal_applied_notifier_operation_manifest,
        MonitoredRedisSignalAppliedNotifierCapability, RedisSignalAppliedNotifierConfig,
        RedisSignalAppliedNotifierError, RedisSignalAppliedNotifierRole,
        RedisSignalAppliedNotifierRoleRegistration,
    },
    redis_task_cancellation::{
        redis_task_cancellation_operation_manifest, MonitoredRedisTaskCancellationCapability,
        RedisTaskCancellation, RedisTaskCancellationConfig, RedisTaskCancellationError,
        RedisTaskCancellationRoleRegistration,
    },
    redis_task_events::{
        redis_task_events_operation_manifest, MonitoredRedisTaskEventCapability,
        RedisTaskEventError, RedisTaskEvents, RedisTaskEventsConfig,
        RedisTaskEventsRoleRegistration,
    },
    redis_task_liveness::{
        redis_liveness_watchdog_operation_manifest, MonitoredRedisLivenessWatchdogCapability,
        RedisLivenessWatchdog, RedisLivenessWatchdogConfig, RedisLivenessWatchdogError,
        RedisLivenessWatchdogRoleRegistration,
    },
    redis_task_pickup::{
        redis_task_dispatch_operation_manifest, MonitoredRedisTaskDispatchCapability,
        RedisTaskDispatch, RedisTaskDispatchConfig, RedisTaskDispatchError,
        RedisTaskDispatchRoleRegistration,
    },
};

const IDENTITY_INSTALL_SCRIPT: &str = r#"
local identity = redis.call('GET', KEYS[1])
local fingerprint = redis.call('GET', KEYS[2])
if identity or fingerprint then
  if identity == ARGV[1] and fingerprint == ARGV[2] then
    return 0
  end
  return -1
end
for _, key in ipairs(redis.call('KEYS', ARGV[3])) do
  if string.sub(key, 1, string.len(ARGV[4])) ~= ARGV[4] then
    return -2
  end
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])
return 1
"#;

/// Collects the one adapter-owned operation manifest for every Redis role.
pub fn canonical_redis_operation_manifests(
) -> Result<Vec<RedisOperationManifest>, RedisOperationManifestError> {
    Ok(vec![
        redis_command_bus_operation_manifest()?,
        redis_task_dispatch_operation_manifest()?,
        redis_task_events_operation_manifest()?,
        redis_task_cancellation_operation_manifest()?,
        redis_compaction_staging_operation_manifest()?,
        redis_lifecycle_work_operation_manifest()?,
        redis_log_staging_operation_manifest()?,
        redis_scope_store_operation_manifest()?,
        redis_ingress_idempotency_operation_manifest()?,
        redis_liveness_watchdog_operation_manifest()?,
        redis_signal_applied_notifier_operation_manifest()?,
        redis_executor_fleet_status_operation_manifest()?,
        redis_event_ingress_operation_manifest()?,
    ])
}

/// One configured role ACL identity. Debug output never exposes either secret field.
pub struct RedisRoleCredential {
    role: CoordinationRole,
    identity: String,
    secret: String,
}

impl RedisRoleCredential {
    pub fn new(
        role: CoordinationRole,
        identity: impl Into<String>,
        secret: impl Into<String>,
    ) -> Self {
        Self {
            role,
            identity: identity.into(),
            secret: secret.into(),
        }
    }

    pub const fn role(&self) -> CoordinationRole {
        self.role
    }
}

impl fmt::Debug for RedisRoleCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisRoleCredential")
            .field("role", &self.role)
            .field("identity", &"[REDACTED]")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

/// The complete, distinct role credential set admitted before opening sockets.
pub struct RedisRoleCredentialSet {
    by_role: BTreeMap<usize, RedisRoleCredential>,
}

impl RedisRoleCredentialSet {
    pub fn admit(credentials: Vec<RedisRoleCredential>) -> Result<Self, RedisAclAdmissionError> {
        let mut by_role = BTreeMap::new();
        let mut identities = HashSet::new();
        let mut secrets = HashSet::new();
        for credential in credentials {
            let role = credential.role;
            if credential.identity.is_empty() || credential.secret.is_empty() {
                return Err(RedisAclAdmissionError::role(
                    role,
                    RedisAclAdmissionFailure::InvalidRoleCredential,
                ));
            }
            if by_role.insert(role as usize, credential).is_some() {
                return Err(RedisAclAdmissionError::role(
                    role,
                    RedisAclAdmissionFailure::DuplicateRoleCredential,
                ));
            }
        }
        for role in ALL_COORDINATION_ROLES {
            let credential = by_role.get(&(role as usize)).ok_or_else(|| {
                RedisAclAdmissionError::role(role, RedisAclAdmissionFailure::MissingRoleCredential)
            })?;
            if !identities.insert(credential.identity.as_str()) {
                return Err(RedisAclAdmissionError::role(
                    role,
                    RedisAclAdmissionFailure::DuplicateAclIdentity,
                ));
            }
            if !secrets.insert(credential.secret.as_str()) {
                return Err(RedisAclAdmissionError::role(
                    role,
                    RedisAclAdmissionFailure::DuplicateAclSecret,
                ));
            }
        }
        if by_role.len() != ALL_COORDINATION_ROLES.len() {
            return Err(RedisAclAdmissionError::new(
                RedisAclAdmissionFailure::UnexpectedRoleCredential,
            ));
        }
        Ok(Self { by_role })
    }

    fn get(&self, role: CoordinationRole) -> &RedisRoleCredential {
        &self.by_role[&(role as usize)]
    }
}

impl fmt::Debug for RedisRoleCredentialSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisRoleCredentialSet")
            .field("role_count", &self.by_role.len())
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisRoleAclPolicy {
    role: CoordinationRole,
    manifest_identity: RedisOperationManifestIdentity,
    command_rules: Vec<String>,
    scripts: Vec<RedisScriptIdentity>,
    key_patterns: Vec<String>,
    channel_patterns: Vec<String>,
}

impl RedisRoleAclPolicy {
    pub const fn role(&self) -> CoordinationRole {
        self.role
    }

    pub fn manifest_identity(&self) -> &RedisOperationManifestIdentity {
        &self.manifest_identity
    }

    pub fn command_rules(&self) -> &[String] {
        &self.command_rules
    }

    pub fn scripts(&self) -> &[RedisScriptIdentity] {
        &self.scripts
    }

    pub fn key_patterns(&self) -> &[String] {
        &self.key_patterns
    }

    pub fn channel_patterns(&self) -> &[String] {
        &self.channel_patterns
    }
}

/// Secret-free canonical policy generated only from the admitted manifest set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisCanonicalAclPolicy {
    roles: Vec<RedisRoleAclPolicy>,
}

impl RedisCanonicalAclPolicy {
    pub fn generate(
        manifests: &RedisOperationManifestSet,
        namespace: &str,
    ) -> Result<Self, RedisAclAdmissionError> {
        let mut roles = Vec::with_capacity(ALL_COORDINATION_ROLES.len());
        for role in ALL_COORDINATION_ROLES {
            let manifest = manifests.get(role);
            if !manifest.scripts().is_empty() && manifest.commands().binary_search(&"EVAL").is_err()
            {
                return Err(RedisAclAdmissionError::role(
                    role,
                    RedisAclAdmissionFailure::InvalidManifestPolicy,
                ));
            }
            let mut command_rules = manifest
                .commands()
                .iter()
                .map(|command| format!("+{}", command.to_ascii_lowercase().replace(' ', "|")))
                .collect::<Vec<_>>();
            command_rules.sort_unstable();
            let key_patterns = manifest
                .key_patterns()
                .iter()
                .map(|pattern| concretize_namespace(pattern, namespace))
                .collect();
            let channel_patterns = manifest
                .channel_patterns()
                .iter()
                .map(|pattern| concretize_namespace(pattern, namespace))
                .collect();
            roles.push(RedisRoleAclPolicy {
                role,
                manifest_identity: manifest.identity().clone(),
                command_rules,
                scripts: manifest.scripts().to_vec(),
                key_patterns,
                channel_patterns,
            });
        }
        Ok(Self { roles })
    }

    pub fn roles(&self) -> &[RedisRoleAclPolicy] {
        &self.roles
    }

    pub fn get(&self, role: CoordinationRole) -> &RedisRoleAclPolicy {
        &self.roles[role as usize]
    }
}

fn concretize_namespace(pattern: &str, namespace: &str) -> String {
    // Runtime role keys share one Redis Cluster hash tag even though Cluster
    // itself is inadmissible; ACL patterns must preserve those literal braces.
    pattern.replace("{namespace}", &format!("{{{namespace}}}"))
}

fn concrete_canary_target(pattern: &str, namespace: &str) -> String {
    concretize_namespace(pattern, namespace).replace('*', "admission-canary")
}

mod private {
    pub trait Sealed {}
}

pub(crate) trait RedisRoleMarker: private::Sealed {
    const ROLE: CoordinationRole;
}

macro_rules! define_role_markers {
    ($(($marker:ident, $role:ident)),+ $(,)?) => {
        $(
            pub(crate) enum $marker {}
            impl private::Sealed for $marker {}
            impl RedisRoleMarker for $marker {
                const ROLE: CoordinationRole = CoordinationRole::$role;
            }
        )+
    };
}

define_role_markers!(
    (CommandBusRole, CommandBus),
    (TaskDispatchRole, TaskDispatch),
    (TaskEventsRole, TaskEvents),
    (TaskCancellationRole, TaskCancellation),
    (CompactionStagingRole, CompactionStaging),
    (LifecycleWorkRole, LifecycleWork),
    (LogStagingRole, LogStaging),
    (ScopeStoreRole, ScopeStore),
    (IngressIdempotencyStoreRole, IngressIdempotencyStore),
    (LivenessWatchdogRole, LivenessWatchdog),
    (SignalAppliedNotifierRole, SignalAppliedNotifier),
    (ExecutorFleetStatusRole, ExecutorFleetStatus),
    (EventIngressRole, EventIngress),
);

/// A client authenticated only as one admitted role. Its optional reconnect
/// handle is bound to that same ACL identity and never exposed across roles.
pub(crate) struct RedisRoleClient<R: RedisRoleMarker> {
    connection: MultiplexedConnection,
    reconnect_client: Option<redis::Client>,
    manifest_identity: RedisOperationManifestIdentity,
    marker: PhantomData<R>,
}

impl<R: RedisRoleMarker> RedisRoleClient<R> {
    pub(crate) const fn role(&self) -> CoordinationRole {
        R::ROLE
    }

    pub(crate) fn manifest_identity(&self) -> &RedisOperationManifestIdentity {
        &self.manifest_identity
    }
}

impl<R: RedisRoleMarker> fmt::Debug for RedisRoleClient<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisRoleClient")
            .field("role", &R::ROLE)
            .field("manifest_identity", &self.manifest_identity)
            .field("connection", &"[ROLE-SCOPED]")
            .finish()
    }
}

struct OpaqueRoleClient {
    role: CoordinationRole,
    _connection: MultiplexedConnection,
    _reconnect_client: Option<redis::Client>,
    _manifest_identity: RedisOperationManifestIdentity,
}

impl<R: RedisRoleMarker> From<RedisRoleClient<R>> for OpaqueRoleClient {
    fn from(client: RedisRoleClient<R>) -> Self {
        Self {
            role: client.role(),
            _manifest_identity: client.manifest_identity().clone(),
            _reconnect_client: client.reconnect_client,
            _connection: client.connection,
        }
    }
}

struct UntypedRoleClient {
    connection: MultiplexedConnection,
    reconnect_client: Option<redis::Client>,
    manifest_identity: RedisOperationManifestIdentity,
}

pub(crate) struct RedisRuntimeRoleClients {
    command_bus: Option<RedisRoleClient<CommandBusRole>>,
    task_dispatch: Option<RedisRoleClient<TaskDispatchRole>>,
    task_events: Option<RedisRoleClient<TaskEventsRole>>,
    task_cancellation: Option<RedisRoleClient<TaskCancellationRole>>,
    compaction_staging: Option<RedisRoleClient<CompactionStagingRole>>,
    lifecycle_work: Option<RedisRoleClient<LifecycleWorkRole>>,
    log_staging: Option<RedisRoleClient<LogStagingRole>>,
    scope_store: Option<RedisRoleClient<ScopeStoreRole>>,
    ingress_idempotency_store: Option<RedisRoleClient<IngressIdempotencyStoreRole>>,
    liveness_watchdog: Option<RedisRoleClient<LivenessWatchdogRole>>,
    signal_applied_notifier: Option<RedisRoleClient<SignalAppliedNotifierRole>>,
    executor_fleet_status: Option<RedisRoleClient<ExecutorFleetStatusRole>>,
    event_ingress: Option<RedisRoleClient<EventIngressRole>>,
}

impl RedisRuntimeRoleClients {
    fn from_untyped(
        mut clients: BTreeMap<usize, UntypedRoleClient>,
    ) -> Result<Self, RedisAclAdmissionError> {
        macro_rules! take {
            ($role:ident, $marker:ident) => {{
                let client = clients
                    .remove(&(CoordinationRole::$role as usize))
                    .ok_or_else(|| {
                        RedisAclAdmissionError::role(
                            CoordinationRole::$role,
                            RedisAclAdmissionFailure::MissingRoleClient,
                        )
                    })?;
                Some(RedisRoleClient::<$marker> {
                    connection: client.connection,
                    reconnect_client: client.reconnect_client,
                    manifest_identity: client.manifest_identity,
                    marker: PhantomData,
                })
            }};
        }
        let result = Self {
            command_bus: take!(CommandBus, CommandBusRole),
            task_dispatch: take!(TaskDispatch, TaskDispatchRole),
            task_events: take!(TaskEvents, TaskEventsRole),
            task_cancellation: take!(TaskCancellation, TaskCancellationRole),
            compaction_staging: take!(CompactionStaging, CompactionStagingRole),
            lifecycle_work: take!(LifecycleWork, LifecycleWorkRole),
            log_staging: take!(LogStaging, LogStagingRole),
            scope_store: take!(ScopeStore, ScopeStoreRole),
            ingress_idempotency_store: take!(IngressIdempotencyStore, IngressIdempotencyStoreRole),
            liveness_watchdog: take!(LivenessWatchdog, LivenessWatchdogRole),
            signal_applied_notifier: take!(SignalAppliedNotifier, SignalAppliedNotifierRole),
            executor_fleet_status: take!(ExecutorFleetStatus, ExecutorFleetStatusRole),
            event_ingress: take!(EventIngress, EventIngressRole),
        };
        if !clients.is_empty() {
            return Err(RedisAclAdmissionError::new(
                RedisAclAdmissionFailure::UnexpectedRoleClient,
            ));
        }
        Ok(result)
    }

    pub(crate) fn take_command_bus(&mut self) -> Option<RedisRoleClient<CommandBusRole>> {
        self.command_bus.take()
    }

    pub(crate) fn take_task_dispatch(&mut self) -> Option<RedisRoleClient<TaskDispatchRole>> {
        self.task_dispatch.take()
    }

    pub(crate) fn take_task_events(&mut self) -> Option<RedisRoleClient<TaskEventsRole>> {
        self.task_events.take()
    }

    pub(crate) fn take_task_cancellation(
        &mut self,
    ) -> Option<RedisRoleClient<TaskCancellationRole>> {
        self.task_cancellation.take()
    }

    pub(crate) fn take_compaction_staging(
        &mut self,
    ) -> Option<RedisRoleClient<CompactionStagingRole>> {
        self.compaction_staging.take()
    }

    pub(crate) fn take_lifecycle_work(&mut self) -> Option<RedisRoleClient<LifecycleWorkRole>> {
        self.lifecycle_work.take()
    }

    pub(crate) fn take_log_staging(&mut self) -> Option<RedisRoleClient<LogStagingRole>> {
        self.log_staging.take()
    }

    pub(crate) fn take_scope_store(&mut self) -> Option<RedisRoleClient<ScopeStoreRole>> {
        self.scope_store.take()
    }

    pub(crate) fn take_ingress_idempotency_store(
        &mut self,
    ) -> Option<RedisRoleClient<IngressIdempotencyStoreRole>> {
        self.ingress_idempotency_store.take()
    }

    pub(crate) fn take_liveness_watchdog(
        &mut self,
    ) -> Option<RedisRoleClient<LivenessWatchdogRole>> {
        self.liveness_watchdog.take()
    }

    pub(crate) fn take_signal_applied_notifier(
        &mut self,
    ) -> Option<RedisRoleClient<SignalAppliedNotifierRole>> {
        self.signal_applied_notifier.take()
    }

    pub(crate) fn take_executor_fleet_status(
        &mut self,
    ) -> Option<RedisRoleClient<ExecutorFleetStatusRole>> {
        self.executor_fleet_status.take()
    }

    pub(crate) fn take_event_ingress(&mut self) -> Option<RedisRoleClient<EventIngressRole>> {
        self.event_ingress.take()
    }
    fn has(&self, role: CoordinationRole) -> bool {
        match role {
            CoordinationRole::CommandBus => self.command_bus.is_some(),
            CoordinationRole::TaskDispatch => self.task_dispatch.is_some(),
            CoordinationRole::TaskEvents => self.task_events.is_some(),
            CoordinationRole::TaskCancellation => self.task_cancellation.is_some(),
            CoordinationRole::CompactionStaging => self.compaction_staging.is_some(),
            CoordinationRole::LifecycleWork => self.lifecycle_work.is_some(),
            CoordinationRole::LogStaging => self.log_staging.is_some(),
            CoordinationRole::ScopeStore => self.scope_store.is_some(),
            CoordinationRole::IngressIdempotencyStore => self.ingress_idempotency_store.is_some(),
            CoordinationRole::LivenessWatchdog => self.liveness_watchdog.is_some(),
            CoordinationRole::SignalAppliedNotifier => self.signal_applied_notifier.is_some(),
            CoordinationRole::ExecutorFleetStatus => self.executor_fleet_status.is_some(),
            CoordinationRole::EventIngress => self.event_ingress.is_some(),
        }
    }
}

pub struct AdmittedRedisFormation {
    capability: AdmittedRedisCapability,
    candidate: RedisFormationAdmissionCandidate,
    role_clients: RedisRuntimeRoleClients,
    command_bus: Option<Arc<RedisCommandBus>>,
    task_dispatch: Option<RedisTaskDispatch>,
    task_cancellation: Option<Arc<RedisTaskCancellation>>,
    task_events: Option<Arc<RedisTaskEvents>>,
    liveness_watchdog: Option<RedisLivenessWatchdog>,
    lifecycle_work: Option<LifecycleWork>,
    executor_fleet_status: Option<Arc<RedisExecutorFleetStatus>>,
    signal_applied: Option<(RedisSignalAppliedNotifierRole, CancellationToken)>,
    compaction_staging: Option<Arc<RedisCompactionStaging>>,
    compaction_log_staging: Option<Arc<dyn CompactionLogStaging>>,
    log_streams: Option<Arc<dyn LogStreamProvider>>,
    scope_store: Option<Arc<dyn ScopeStore>>,
    ingress_idempotency: Option<Arc<dyn IngressIdempotencyStore>>,
    event_ingress: Option<Arc<RedisEventIngress>>,
}

impl AdmittedRedisFormation {
    pub fn capability(&self) -> &AdmittedRedisCapability {
        &self.capability
    }

    /// Immutable, substrate-neutral authority admitted for this process.
    pub const fn descriptor(&self) -> &crate::formation::ResolvedFormationDescriptor {
        self.candidate.descriptor()
    }

    pub fn candidate(&self) -> &RedisFormationAdmissionCandidate {
        &self.candidate
    }

    /// Compose the admitted Command-bus role before the capability monitor can
    /// reconstruct and open readiness.
    pub fn compose_command_bus(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
        consumer_id: impl Into<String>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.command_bus.is_some() {
            return Err(RedisFormationCompositionError::CommandBusAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_command_bus()
            .ok_or(RedisFormationCompositionError::CommandBusAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::CommandBus);
        let mut config =
            RedisCommandBusConfig::new(self.candidate.namespace().as_str(), consumer_id);
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisCommandBusRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::CommandBus)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::CommandBus,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisCommandCapability::new(
            monitor.fence(),
            reporter,
        ));
        let command_bus = registration
            .build_bus(capability)
            .map_err(RedisFormationCompositionError::CommandBus)?;
        self.command_bus = Some(Arc::new(command_bus));
        Ok(())
    }

    /// Compose TaskDispatch before reconstruction so Conductor publication and
    /// Executor pickup receive only the role and safe-handoff interfaces.
    pub fn compose_task_dispatch(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
        consumer_id: impl Into<String>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.task_dispatch.is_some() {
            return Err(RedisFormationCompositionError::TaskDispatchAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_task_dispatch()
            .ok_or(RedisFormationCompositionError::TaskDispatchAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::TaskDispatch);
        let mut config =
            RedisTaskDispatchConfig::new(self.candidate.namespace().as_str(), consumer_id);
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisTaskDispatchRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::TaskDispatch)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::TaskDispatch,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisTaskDispatchCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.task_dispatch = Some(
            registration
                .build_adapter(capability)
                .map_err(RedisFormationCompositionError::TaskDispatch)?,
        );
        Ok(())
    }

    /// Compose TaskCancellation only after TaskDispatch so the durable fence
    /// can bind the exact pickup generation before any owner action.
    pub fn compose_task_cancellation(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.task_cancellation.is_some() {
            return Err(RedisFormationCompositionError::TaskCancellationAlreadyComposed);
        }
        let dispatch = self
            .task_dispatch
            .clone()
            .ok_or(RedisFormationCompositionError::TaskDispatchRequired)?;
        let role_client = self
            .role_clients
            .take_task_cancellation()
            .ok_or(RedisFormationCompositionError::TaskCancellationAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::TaskCancellation);
        let mut config = RedisTaskCancellationConfig::new(self.candidate.namespace().as_str());
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisTaskCancellationRoleRegistration::new(
                role_client.connection,
                dispatch,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::TaskCancellation)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::TaskCancellation,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisTaskCancellationCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.task_cancellation = Some(Arc::new(
            registration
                .build_adapter(capability)
                .map_err(RedisFormationCompositionError::TaskCancellation)?,
        ));
        Ok(())
    }

    /// Compose TaskEvents before reconstruction so producers and the Conductor
    /// drain receive only the admitted role interfaces after readiness opens.
    pub fn compose_task_events(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
        consumer_id: impl Into<String>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.task_events.is_some() {
            return Err(RedisFormationCompositionError::TaskEventsAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_task_events()
            .ok_or(RedisFormationCompositionError::TaskEventsAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::TaskEvents);
        let mut config =
            RedisTaskEventsConfig::new(self.candidate.namespace().as_str(), consumer_id);
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisTaskEventsRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::TaskEvents)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::TaskEvents,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisTaskEventCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.task_events = Some(Arc::new(
            registration
                .build_adapter(capability)
                .map_err(RedisFormationCompositionError::TaskEvents)?,
        ));
        Ok(())
    }

    /// Compose the admitted LivenessWatchdog before capability reconstruction.
    /// Executor and Conductor roots receive only its formation-neutral API.
    pub fn compose_liveness_watchdog(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.liveness_watchdog.is_some() {
            return Err(RedisFormationCompositionError::LivenessWatchdogAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_liveness_watchdog()
            .ok_or(RedisFormationCompositionError::LivenessWatchdogAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::LivenessWatchdog);
        let mut config = RedisLivenessWatchdogConfig::new(self.candidate.namespace().as_str());
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisLivenessWatchdogRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::LivenessWatchdog)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::LivenessWatchdog,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisLivenessWatchdogCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.liveness_watchdog = Some(
            registration
                .build_watchdog(capability)
                .map_err(RedisFormationCompositionError::LivenessWatchdog)?,
        );
        Ok(())
    }
    /// Compose ExecutorFleetStatus before reconstruction. Executor reporting
    /// and Health receive only its observational role interface.
    pub fn compose_executor_fleet_status(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.executor_fleet_status.is_some() {
            return Err(RedisFormationCompositionError::ExecutorFleetStatusAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_executor_fleet_status()
            .ok_or(RedisFormationCompositionError::ExecutorFleetStatusAlreadyComposed)?;
        let reconnect_client = role_client
            .reconnect_client
            .ok_or(RedisFormationCompositionError::ExecutorFleetStatusClientUnavailable)?;
        let config = RedisExecutorFleetStatusConfig::new(self.candidate.namespace().as_str());
        let registration = Arc::new(
            RedisExecutorFleetStatusRoleRegistration::new(
                reconnect_client,
                role_client.connection,
                config,
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::ExecutorFleetStatus)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::ExecutorFleetStatus,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisExecutorFleetStatusCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.executor_fleet_status =
            Some(Arc::new(registration.build_role(capability).map_err(
                RedisFormationCompositionError::ExecutorFleetStatus,
            )?));
        Ok(())
    }

    /// Compose LifecycleWork before reconstruction. The subscription remains
    /// lazy; SQL-authoritative pending work is reconstructed before readiness.
    pub fn compose_lifecycle_work(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
        repositories: Option<Arc<WriterRepositoryBundle>>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.lifecycle_work.is_some() {
            return Err(RedisFormationCompositionError::LifecycleWorkAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_lifecycle_work()
            .ok_or(RedisFormationCompositionError::LifecycleWorkAlreadyComposed)?;
        let reconnect_client = role_client
            .reconnect_client
            .ok_or(RedisFormationCompositionError::LifecycleWorkClientUnavailable)?;
        let registration = Arc::new(
            RedisLifecycleWorkRoleRegistration::new(
                reconnect_client,
                role_client.connection,
                RedisLifecycleWorkConfig::new(self.candidate.namespace().as_str()),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::LifecycleWork)?,
        );
        let capability = Arc::new(MonitoredRedisLifecycleWorkCapability::new(monitor.fence()));
        let role = registration
            .build_role(capability.clone())
            .map_err(RedisFormationCompositionError::LifecycleWork)?;
        let lifecycle_work = role.conductor_lifecycle_work(
            monitor.fence(),
            NonZeroUsize::new(64).expect("LifecycleWork capacity is non-zero"),
        );
        let reconstruction = Arc::new(RedisLifecycleReconstruction::new(
            registration.clone(),
            repositories,
            lifecycle_work.wakeups(),
        ));
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::LifecycleWork,
                registration,
                reconstruction,
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        capability.install_reporter(reporter);
        self.lifecycle_work = Some(lifecycle_work);
        Ok(())
    }

    /// Compose SignalAppliedNotifier before reconstruction so publication and
    /// ByTag observation receive only the advisory role interfaces.
    pub async fn compose_signal_applied_notifier(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
        shutdown: CancellationToken,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.signal_applied.is_some() {
            return Err(RedisFormationCompositionError::SignalAppliedNotifierAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_signal_applied_notifier()
            .ok_or(RedisFormationCompositionError::SignalAppliedNotifierAlreadyComposed)?;
        let reconnect_client = role_client
            .reconnect_client
            .ok_or(RedisFormationCompositionError::SignalAppliedNotifierClientUnavailable)?;
        let config = RedisSignalAppliedNotifierConfig::new(self.candidate.namespace().as_str());
        let registration = Arc::new(
            RedisSignalAppliedNotifierRoleRegistration::new(
                reconnect_client,
                role_client.connection,
                config,
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::SignalAppliedNotifier)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::SignalAppliedNotifier,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisSignalAppliedNotifierCapability::new(
            monitor.fence(),
            reporter,
        ));
        let role = registration
            .build_role(capability)
            .map_err(RedisFormationCompositionError::SignalAppliedNotifier)?;
        self.signal_applied = Some((role, shutdown));
        Ok(())
    }

    /// Compose CompactionStaging before reconstruction so relay receipt and
    /// drain selection receive only the durable role interface.
    pub async fn compose_compaction_staging(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
        consumer_id: impl Into<String>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.compaction_staging.is_some() {
            return Err(RedisFormationCompositionError::CompactionStagingAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_compaction_staging()
            .ok_or(RedisFormationCompositionError::CompactionStagingAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::CompactionStaging);
        let mut config =
            RedisCompactionStagingConfig::new(self.candidate.namespace().as_str(), consumer_id);
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisCompactionStagingRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::CompactionStaging)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::CompactionStaging,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisCompactionStagingCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.compaction_staging = Some(Arc::new(
            registration
                .build_adapter(capability)
                .await
                .map_err(RedisFormationCompositionError::CompactionStaging)?,
        ));
        Ok(())
    }

    /// Compose LogStaging before reconstruction so the Task shipper and API
    /// live-Log reader receive only the common provider contract.
    pub fn compose_log_staging(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.log_streams.is_some() {
            return Err(RedisFormationCompositionError::LogStagingAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_log_staging()
            .ok_or(RedisFormationCompositionError::LogStagingAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::LogStaging);
        let mut config = RedisLogStagingConfig::new(self.candidate.namespace().as_str());
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisLogStagingRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::LogStaging)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::LogStaging,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisLogStagingCapability::new(
            monitor.fence(),
            reporter,
        ));
        let provider = Arc::new(
            registration
                .build_provider(capability)
                .map_err(RedisFormationCompositionError::LogStaging)?,
        );
        self.log_streams = Some(provider.clone());
        self.compaction_log_staging = Some(provider);
        Ok(())
    }

    /// Compose ScopeStore before reconstruction so tickr-ctx and Compaction
    /// receive only opaque scope operations and verified snapshots.
    pub async fn compose_scope_store(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.scope_store.is_some() {
            return Err(RedisFormationCompositionError::ScopeStoreAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_scope_store()
            .ok_or(RedisFormationCompositionError::ScopeStoreAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::ScopeStore);
        let mut config = RedisScopeStoreConfig::new(self.candidate.namespace().as_str());
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisScopeStoreRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::ScopeStore)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::ScopeStore,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisScopeStoreCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.scope_store = Some(Arc::new(
            registration
                .build_store(capability)
                .await
                .map_err(RedisFormationCompositionError::ScopeStore)?,
        ));
        Ok(())
    }

    /// Compose producer idempotency before reconstruction; the ingress
    /// coordinator receives only its role interface, never this Redis client.
    pub fn compose_ingress_idempotency_store(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.ingress_idempotency.is_some() {
            return Err(RedisFormationCompositionError::IngressIdempotencyAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_ingress_idempotency_store()
            .ok_or(RedisFormationCompositionError::IngressIdempotencyAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::IngressIdempotencyStore);
        let mut config = RedisIngressIdempotencyConfig::new(self.candidate.namespace().as_str());
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisIngressIdempotencyRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::IngressIdempotency)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::IngressIdempotencyStore,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisIngressIdempotencyCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.ingress_idempotency =
            Some(Arc::new(registration.build_store(capability).map_err(
                RedisFormationCompositionError::IngressIdempotency,
            )?));
        Ok(())
    }

    /// Compose EventIngress separately from producer idempotency. The
    /// Conductor receives only the transport and coordinator interfaces.
    pub async fn compose_event_ingress(
        &mut self,
        monitor: &Arc<RedisCapabilityMonitor>,
        consumer_id: impl Into<String>,
    ) -> Result<(), RedisFormationCompositionError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisFormationCompositionError::CandidateMismatch);
        }
        if self.event_ingress.is_some() {
            return Err(RedisFormationCompositionError::EventIngressAlreadyComposed);
        }
        let role_client = self
            .role_clients
            .take_event_ingress()
            .ok_or(RedisFormationCompositionError::EventIngressAlreadyComposed)?;
        let hard_limit_bytes = self
            .candidate
            .capacity_profile()
            .role_capacity_bytes(CoordinationRole::EventIngress);
        let mut config =
            RedisEventIngressConfig::new(self.candidate.namespace().as_str(), consumer_id);
        config.hard_limit_bytes = hard_limit_bytes;
        config.soft_limit_bytes = hard_limit_bytes.saturating_mul(4) / 5;

        let registration = Arc::new(
            RedisEventIngressRoleRegistration::new(
                role_client.connection,
                config,
                RedisDurabilityGuard::default(),
                role_client.manifest_identity,
            )
            .map_err(RedisFormationCompositionError::EventIngress)?,
        );
        let reporter = monitor
            .register_role(RedisRoleCapabilityRegistration::new(
                CoordinationRole::EventIngress,
                registration.clone(),
                registration.clone(),
            ))
            .map_err(RedisFormationCompositionError::RoleRegistration)?;
        let capability = Arc::new(MonitoredRedisEventIngressCapability::new(
            monitor.fence(),
            reporter,
        ));
        self.event_ingress = Some(Arc::new(
            registration
                .build_adapter(capability)
                .await
                .map_err(RedisFormationCompositionError::EventIngress)?,
        ));
        Ok(())
    }

    pub async fn reconstruct_before_readiness(
        self,
        monitor: Arc<RedisCapabilityMonitor>,
    ) -> Result<ReadyRedisFormation, RedisCapabilityMonitorError> {
        if !monitor.matches_candidate(&self.candidate) {
            return Err(RedisCapabilityMonitorError::CandidateMismatch);
        }
        monitor.reconstruct_before_readiness().await?;
        Ok(ReadyRedisFormation {
            admitted: self,
            monitor,
        })
    }
}

/// The only state from which runtime role clients can be released to component roots.
pub struct ReadyRedisFormation {
    admitted: AdmittedRedisFormation,
    monitor: Arc<RedisCapabilityMonitor>,
}

impl ReadyRedisFormation {
    pub const fn descriptor(&self) -> &crate::formation::ResolvedFormationDescriptor {
        self.admitted.descriptor()
    }

    pub fn capability(&self) -> &AdmittedRedisCapability {
        self.admitted.capability()
    }

    pub fn monitor(&self) -> &Arc<RedisCapabilityMonitor> {
        &self.monitor
    }

    pub fn role_inventory(&self) -> Vec<CoordinationRole> {
        ALL_COORDINATION_ROLES
            .into_iter()
            .filter(|role| {
                self.admitted.role_clients.has(*role)
                    || (*role == CoordinationRole::CompactionStaging
                        && self.admitted.compaction_staging.is_some())
                    || (*role == CoordinationRole::LogStaging
                        && self.admitted.log_streams.is_some())
                    || (*role == CoordinationRole::ScopeStore
                        && self.admitted.scope_store.is_some())
                    || (*role == CoordinationRole::LifecycleWork
                        && self.admitted.lifecycle_work.is_some())
                    || (*role == CoordinationRole::SignalAppliedNotifier
                        && self.admitted.signal_applied.is_some())
                    || (*role == CoordinationRole::ExecutorFleetStatus
                        && self.admitted.executor_fleet_status.is_some())
                    || (*role == CoordinationRole::EventIngress
                        && self.admitted.event_ingress.is_some())
            })
            .collect()
    }

    /// Erases Redis internals at the component boundary while retaining all
    /// thirteen authenticated clients under their role identities.
    pub async fn into_role_bundle(
        mut self,
    ) -> Result<DistributedCoordinationBundle, RedisFormationCompositionError> {
        let command_bus = self.admitted.command_bus.take();
        let task_dispatch = self.admitted.task_dispatch.take();
        let task_cancellation = self.admitted.task_cancellation.take();
        let liveness_watchdog = self.admitted.liveness_watchdog.take().map(Arc::new);
        let task_events = self.admitted.task_events.take();
        let lifecycle_work = self.admitted.lifecycle_work.take();
        let compaction_staging = self.admitted.compaction_staging.take();
        let compaction_log_staging = self.admitted.compaction_log_staging.take();
        let log_streams = self.admitted.log_streams.take();
        let scope_store = self.admitted.scope_store.take();
        let mut critical_children = Vec::new();
        let mut role_shutdown = None;
        let signal_applied = match self.admitted.signal_applied.take() {
            Some((role, shutdown)) => {
                let notifications = role
                    .subscribe()
                    .await
                    .map_err(RedisFormationCompositionError::SignalAppliedNotifier)?;
                let (notifier, publisher) = role.bounded_notifier(
                    NonZeroUsize::new(64).expect("SignalAppliedNotifier capacity is non-zero"),
                );
                role_shutdown = Some(shutdown.clone());
                critical_children.push(tokio::spawn(publisher.run(shutdown.clone())));
                critical_children.push(tokio::spawn(async move {
                    role.run_expiry_sweeper(shutdown).await;
                }));
                Some(SignalAppliedNotificationRoles::new(notifier, notifications))
            }
            None => None,
        };
        let executor_fleet_status = self.admitted.executor_fleet_status.take();
        let ingress_idempotency = self.admitted.ingress_idempotency.take();
        let event_ingress = self.admitted.event_ingress.take();
        let mut clients = Vec::with_capacity(ALL_COORDINATION_ROLES.len());
        if let Some(client) = self.admitted.role_clients.take_command_bus() {
            clients.push(OpaqueRoleClient::from(client));
        }
        if let Some(client) = self.admitted.role_clients.take_liveness_watchdog() {
            clients.push(OpaqueRoleClient::from(client));
        }
        macro_rules! take {
            ($method:ident) => {
                clients.push(OpaqueRoleClient::from(
                    self.admitted
                        .role_clients
                        .$method()
                        .expect("complete admission constructs every role client"),
                ))
            };
        }
        if task_dispatch.is_none() {
            take!(take_task_dispatch);
        }
        if task_events.is_none() {
            take!(take_task_events);
        }
        if task_cancellation.is_none() {
            take!(take_task_cancellation);
        }
        if compaction_staging.is_none() {
            take!(take_compaction_staging);
        }
        if lifecycle_work.is_none() {
            take!(take_lifecycle_work);
        }
        if log_streams.is_none() {
            take!(take_log_staging);
        }
        if scope_store.is_none() {
            take!(take_scope_store);
        }
        if ingress_idempotency.is_none() {
            take!(take_ingress_idempotency_store);
        }
        if signal_applied.is_none() {
            take!(take_signal_applied_notifier);
        }
        if executor_fleet_status.is_none() {
            take!(take_executor_fleet_status);
        }
        if event_ingress.is_none() {
            take!(take_event_ingress);
        }
        Ok(DistributedCoordinationBundle {
            admitted_descriptor: self.admitted.candidate,
            monitor: self.monitor,
            command_bus,
            task_dispatch,
            task_cancellation,
            task_events,
            liveness_watchdog,
            lifecycle_work,
            compaction_staging,
            compaction_log_staging,
            log_streams,
            scope_store,
            signal_applied,
            executor_fleet_status,
            ingress_idempotency,
            event_ingress,
            clients,
            role_shutdown,
            critical_children,
        })
    }
}

/// Substrate-neutral component boundary produced only after reconstruction.
pub struct DistributedCoordinationBundle {
    admitted_descriptor: RedisFormationAdmissionCandidate,
    monitor: Arc<RedisCapabilityMonitor>,
    command_bus: Option<Arc<RedisCommandBus>>,
    task_dispatch: Option<RedisTaskDispatch>,
    task_cancellation: Option<Arc<RedisTaskCancellation>>,
    task_events: Option<Arc<RedisTaskEvents>>,
    liveness_watchdog: Option<Arc<RedisLivenessWatchdog>>,
    lifecycle_work: Option<LifecycleWork>,
    compaction_staging: Option<Arc<RedisCompactionStaging>>,
    compaction_log_staging: Option<Arc<dyn CompactionLogStaging>>,
    log_streams: Option<Arc<dyn LogStreamProvider>>,
    scope_store: Option<Arc<dyn ScopeStore>>,
    signal_applied: Option<SignalAppliedNotificationRoles>,
    executor_fleet_status: Option<Arc<RedisExecutorFleetStatus>>,
    ingress_idempotency: Option<Arc<dyn IngressIdempotencyStore>>,
    event_ingress: Option<Arc<RedisEventIngress>>,
    clients: Vec<OpaqueRoleClient>,
    role_shutdown: Option<CancellationToken>,
    critical_children: Vec<tokio::task::JoinHandle<()>>,
}

impl DistributedCoordinationBundle {
    /// Immutable, secret-free process state for the admitted formation.
    pub const fn admitted_descriptor(&self) -> &RedisFormationAdmissionCandidate {
        &self.admitted_descriptor
    }

    pub const fn descriptor(&self) -> &crate::formation::ResolvedFormationDescriptor {
        self.admitted_descriptor.descriptor()
    }

    pub fn role_inventory(&self) -> Vec<CoordinationRole> {
        ALL_COORDINATION_ROLES
            .into_iter()
            .filter(|role| {
                (*role == CoordinationRole::CommandBus && self.command_bus.is_some())
                    || (*role == CoordinationRole::TaskDispatch && self.task_dispatch.is_some())
                    || (*role == CoordinationRole::TaskCancellation
                        && self.task_cancellation.is_some())
                    || (*role == CoordinationRole::TaskEvents && self.task_events.is_some())
                    || (*role == CoordinationRole::LivenessWatchdog
                        && self.liveness_watchdog.is_some())
                    || (*role == CoordinationRole::LifecycleWork && self.lifecycle_work.is_some())
                    || (*role == CoordinationRole::CompactionStaging
                        && self.compaction_staging.is_some())
                    || (*role == CoordinationRole::LogStaging && self.log_streams.is_some())
                    || (*role == CoordinationRole::ScopeStore && self.scope_store.is_some())
                    || (*role == CoordinationRole::IngressIdempotencyStore
                        && self.ingress_idempotency.is_some())
                    || (*role == CoordinationRole::SignalAppliedNotifier
                        && self.signal_applied.is_some())
                    || (*role == CoordinationRole::ExecutorFleetStatus
                        && self.executor_fleet_status.is_some())
                    || (*role == CoordinationRole::EventIngress && self.event_ingress.is_some())
                    || self.clients.iter().any(|client| client.role == *role)
            })
            .collect()
    }

    pub fn command_bus_client(&self) -> Option<CommandBus> {
        self.command_bus
            .as_ref()
            .map(|bus| CommandBus::redis(bus.clone(), bus.max_in_flight()))
    }

    pub fn command_bus_consumer(&self) -> Option<Arc<dyn CommandBusConsumer>> {
        self.command_bus
            .as_ref()
            .map(|bus| bus.clone() as Arc<dyn CommandBusConsumer>)
    }

    pub fn task_dispatch_publisher(&self) -> Option<Arc<dyn TaskDispatchPublisher>> {
        self.task_dispatch
            .as_ref()
            .map(|dispatch| Arc::new(dispatch.clone()) as Arc<dyn TaskDispatchPublisher>)
    }
    pub fn executor_task_handoff(
        &self,
    ) -> Option<
        impl SafePickupWriter + SafeAttemptOutcomeHandoff + SafeLivenessDeadlineSweeper + Clone,
    > {
        let dispatch = self.task_dispatch.clone()?;
        let liveness = self.liveness_watchdog.clone()?;
        let task_events = self.task_events.as_ref()?.clone() as Arc<dyn TaskEventWriter>;
        Some(SafeHandoffCoordinator::with_task_events(
            dispatch,
            liveness,
            task_events,
        ))
    }

    pub fn task_cancellation_publisher(&self) -> Option<Arc<dyn TaskCancellationPublisher>> {
        self.task_cancellation
            .as_ref()
            .map(|cancellation| cancellation.clone() as Arc<dyn TaskCancellationPublisher>)
    }

    pub fn task_cancellation_ack_consumer(&self) -> Option<Arc<dyn TaskCancellationAckConsumer>> {
        self.task_cancellation
            .as_ref()
            .map(|cancellation| cancellation.clone() as Arc<dyn TaskCancellationAckConsumer>)
    }

    pub fn executor_task_cancellation(&self) -> Option<impl SafeCancellationRole + Clone> {
        self.task_cancellation
            .as_ref()
            .map(|cancellation| cancellation.as_ref().clone())
    }

    pub fn task_event_writer(&self) -> Option<Arc<dyn TaskEventWriter>> {
        self.task_events
            .as_ref()
            .map(|task_events| task_events.clone() as Arc<dyn TaskEventWriter>)
    }

    pub fn task_event_consumer(&self) -> Option<Arc<dyn TaskEventConsumer>> {
        self.task_events
            .as_ref()
            .map(|task_events| task_events.clone() as Arc<dyn TaskEventConsumer>)
    }

    /// Relay producer and drain consumer boundary with Redis internals erased.
    pub fn compaction_staging(&self) -> Option<Arc<dyn CompactionStaging>> {
        self.compaction_staging
            .as_ref()
            .map(|staging| staging.clone() as Arc<dyn CompactionStaging>)
    }

    /// Compaction-only accepted-Log seal and verified-purge boundary.
    pub fn compaction_log_staging(&self) -> Option<Arc<dyn CompactionLogStaging>> {
        self.compaction_log_staging.clone()
    }

    /// Shared Task-writer/API-reader boundary with Redis internals erased.
    pub fn log_stream_provider(&self) -> Option<Arc<dyn LogStreamProvider>> {
        self.log_streams.clone()
    }

    /// Shared tickr-ctx writer and Compaction snapshot boundary.
    pub fn scope_store(&self) -> Option<Arc<dyn ScopeStore>> {
        self.scope_store.clone()
    }

    /// Compaction receives one immutable selected-role snapshot reader.
    pub fn compaction_scope_reader(&self) -> Option<Arc<dyn CompactionScopeSnapshotReader>> {
        self.scope_store.as_ref().map(|store| {
            Arc::new(RoleCompactionScopeSnapshotReader::new(store.clone()))
                as Arc<dyn CompactionScopeSnapshotReader>
        })
    }

    /// Publisher and bounded reconciliation stream with Redis internals erased.
    pub fn signal_applied_roles(&self) -> Option<SignalAppliedNotificationRoles> {
        self.signal_applied.clone()
    }

    /// SQL-authoritative lifecycle wakeups and claim admission with Redis erased.
    pub fn lifecycle_work(&mut self) -> Option<LifecycleWork> {
        self.lifecycle_work.take()
    }

    /// Producer-idempotency processing boundary with Redis internals erased.
    pub fn ingress_coordinator(&self) -> Option<IngressCoordinator> {
        self.ingress_idempotency
            .as_ref()
            .map(|store| IngressCoordinator::new(store.clone()))
    }

    /// External Event delivery transport with Redis internals erased.
    pub fn event_ingress(&self) -> Option<Arc<dyn EventIngress>> {
        self.event_ingress
            .as_ref()
            .map(|ingress| ingress.clone() as Arc<dyn EventIngress>)
    }

    /// Executor-side owner renewal authority with its substrate erased.
    pub fn executor_liveness_watchdog(&self) -> Option<Arc<dyn SafeLivenessWatchdog>> {
        self.liveness_watchdog
            .as_ref()
            .map(|watchdog| watchdog.clone() as Arc<dyn SafeLivenessWatchdog>)
    }

    /// Shared Executor-reporting and Health-snapshot boundary with Redis erased.
    pub fn executor_fleet_status(&self) -> Option<Arc<dyn ExecutorFleetStatus>> {
        self.executor_fleet_status
            .as_ref()
            .map(|status| status.clone() as Arc<dyn ExecutorFleetStatus>)
    }

    /// Conductor-side bounded deadline selector and terminal TaskEvent handoff.
    pub fn conductor_liveness_sweeper(&self) -> Option<Arc<dyn SafeLivenessDeadlineSweeper>> {
        self.executor_task_handoff()
            .map(|sweeper| Arc::new(sweeper) as Arc<dyn SafeLivenessDeadlineSweeper>)
    }

    /// Secret-free snapshot source for the operator diagnostics surface.
    pub fn diagnostics_probe(&self) -> Arc<dyn Fn() -> RedisCapabilityDiagnostics + Send + Sync> {
        let monitor = Arc::clone(&self.monitor);
        Arc::new(move || monitor.diagnostics())
    }

    /// Keeps capability recovery and the component under one shutdown fate
    /// without exposing the monitor or Redis types to runtime consumers.
    pub fn start_capability_monitor(
        &self,
        interval: std::time::Duration,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let monitor = Arc::clone(&self.monitor);
        tokio::spawn(async move {
            monitor
                .run(interval, shutdown)
                .await
                .map_err(anyhow::Error::new)
        })
    }

    /// Cancels and joins every role-owned child created during composition.
    pub async fn shutdown_critical_children(&mut self) -> Result<(), tokio::task::JoinError> {
        if let Some(shutdown) = self.role_shutdown.take() {
            shutdown.cancel();
        }
        let mut failure = None;
        while let Some(child) = self.critical_children.pop() {
            if let Err(error) = child.await {
                failure.get_or_insert(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn readiness_probe(&self) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let monitor = Arc::clone(&self.monitor);
        Arc::new(move || monitor.fence().snapshot().ready)
    }

    pub fn is_ready(&self) -> bool {
        self.monitor.fence().snapshot().ready
    }
}

#[derive(Debug)]
pub enum RedisFormationCompositionError {
    Admission(RedisAclAdmissionError),
    Capability(RedisCapabilityMonitorError),
    CommandBus(RedisCommandBusError),
    TaskDispatch(RedisTaskDispatchError),
    TaskCancellation(RedisTaskCancellationError),
    TaskEvents(RedisTaskEventError),
    LivenessWatchdog(RedisLivenessWatchdogError),
    ExecutorFleetStatus(RedisExecutorFleetStatusError),
    LifecycleWork(RedisLifecycleWorkError),
    SignalAppliedNotifier(RedisSignalAppliedNotifierError),
    CompactionStaging(RedisCompactionStagingError),
    LogStaging(RedisLogStagingError),
    ScopeStore(RedisScopeStoreError),
    IngressIdempotency(RedisIngressIdempotencyError),
    EventIngress(RedisEventIngressError),
    RoleRegistration(RedisRoleRegistrationError),
    CandidateMismatch,
    CommandBusAlreadyComposed,
    TaskDispatchAlreadyComposed,
    TaskCancellationAlreadyComposed,
    TaskDispatchRequired,
    TaskEventsAlreadyComposed,
    LivenessWatchdogAlreadyComposed,
    LifecycleWorkAlreadyComposed,
    LifecycleWorkClientUnavailable,
    ExecutorFleetStatusAlreadyComposed,
    ExecutorFleetStatusClientUnavailable,
    SignalAppliedNotifierAlreadyComposed,
    SignalAppliedNotifierClientUnavailable,
    CompactionStagingAlreadyComposed,
    LogStagingAlreadyComposed,
    ScopeStoreAlreadyComposed,
    IngressIdempotencyAlreadyComposed,
    EventIngressAlreadyComposed,
}

impl fmt::Display for RedisFormationCompositionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(formatter),
            Self::Capability(error) => error.fmt(formatter),
            Self::CommandBus(error) => error.fmt(formatter),
            Self::TaskDispatch(error) => error.fmt(formatter),
            Self::TaskCancellation(error) => error.fmt(formatter),
            Self::TaskEvents(error) => error.fmt(formatter),
            Self::LivenessWatchdog(error) => error.fmt(formatter),
            Self::LifecycleWork(error) => error.fmt(formatter),
            Self::ExecutorFleetStatus(error) => error.fmt(formatter),
            Self::SignalAppliedNotifier(error) => error.fmt(formatter),
            Self::CompactionStaging(error) => error.fmt(formatter),
            Self::LogStaging(error) => error.fmt(formatter),
            Self::ScopeStore(error) => error.fmt(formatter),
            Self::IngressIdempotency(error) => error.fmt(formatter),
            Self::EventIngress(error) => error.fmt(formatter),
            Self::RoleRegistration(error) => error.fmt(formatter),
            Self::CandidateMismatch => {
                formatter.write_str("the Redis capability monitor candidate does not match")
            }
            Self::CommandBusAlreadyComposed => {
                formatter.write_str("the Redis Command-bus role is already composed")
            }
            Self::TaskDispatchAlreadyComposed => {
                formatter.write_str("the Redis TaskDispatch role is already composed")
            }
            Self::TaskCancellationAlreadyComposed => {
                formatter.write_str("the Redis TaskCancellation role is already composed")
            }
            Self::TaskDispatchRequired => {
                formatter.write_str("Redis TaskCancellation requires composed TaskDispatch")
            }
            Self::TaskEventsAlreadyComposed => {
                formatter.write_str("the Redis TaskEvents role is already composed")
            }
            Self::LivenessWatchdogAlreadyComposed => {
                formatter.write_str("the Redis LivenessWatchdog role is already composed")
            }
            Self::LifecycleWorkAlreadyComposed => {
                formatter.write_str("the Redis LifecycleWork role is already composed")
            }
            Self::LifecycleWorkClientUnavailable => {
                formatter.write_str("the admitted Redis LifecycleWork client is unavailable")
            }
            Self::ExecutorFleetStatusAlreadyComposed => {
                formatter.write_str("the Redis ExecutorFleetStatus role is already composed")
            }
            Self::ExecutorFleetStatusClientUnavailable => {
                formatter.write_str("the admitted Redis ExecutorFleetStatus client is unavailable")
            }
            Self::SignalAppliedNotifierAlreadyComposed => {
                formatter.write_str("the Redis SignalAppliedNotifier role is already composed")
            }
            Self::SignalAppliedNotifierClientUnavailable => formatter
                .write_str("the admitted Redis SignalAppliedNotifier client is unavailable"),
            Self::CompactionStagingAlreadyComposed => {
                formatter.write_str("the Redis CompactionStaging role is already composed")
            }
            Self::LogStagingAlreadyComposed => {
                formatter.write_str("the Redis LogStaging role is already composed")
            }
            Self::ScopeStoreAlreadyComposed => {
                formatter.write_str("the Redis ScopeStore role is already composed")
            }
            Self::IngressIdempotencyAlreadyComposed => {
                formatter.write_str("the Redis IngressIdempotencyStore role is already composed")
            }
            Self::EventIngressAlreadyComposed => {
                formatter.write_str("the Redis EventIngress role is already composed")
            }
        }
    }
}

impl std::error::Error for RedisFormationCompositionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Capability(error) => Some(error),
            Self::CommandBus(error) => Some(error),
            Self::TaskDispatch(error) => Some(error),
            Self::TaskCancellation(error) => Some(error),
            Self::TaskEvents(error) => Some(error),
            Self::LivenessWatchdog(error) => Some(error),
            Self::ExecutorFleetStatus(error) => Some(error),
            Self::LifecycleWork(error) => Some(error),
            Self::SignalAppliedNotifier(error) => Some(error),
            Self::CompactionStaging(error) => Some(error),
            Self::LogStaging(error) => Some(error),
            Self::ScopeStore(error) => Some(error),
            Self::IngressIdempotency(error) => Some(error),
            Self::EventIngress(error) => Some(error),
            Self::RoleRegistration(error) => Some(error),
            Self::CandidateMismatch
            | Self::CommandBusAlreadyComposed
            | Self::TaskDispatchAlreadyComposed
            | Self::TaskCancellationAlreadyComposed
            | Self::TaskDispatchRequired
            | Self::TaskEventsAlreadyComposed
            | Self::LivenessWatchdogAlreadyComposed
            | Self::LifecycleWorkAlreadyComposed
            | Self::LifecycleWorkClientUnavailable
            | Self::ExecutorFleetStatusAlreadyComposed
            | Self::ExecutorFleetStatusClientUnavailable
            | Self::SignalAppliedNotifierAlreadyComposed
            | Self::SignalAppliedNotifierClientUnavailable
            | Self::CompactionStagingAlreadyComposed
            | Self::LogStagingAlreadyComposed
            | Self::ScopeStoreAlreadyComposed
            | Self::IngressIdempotencyAlreadyComposed
            | Self::EventIngressAlreadyComposed => None,
        }
    }
}

/// Runs the complete all-Redis admission and reconstruction sequence and
/// crosses the substrate-neutral component boundary only after readiness opens.
pub async fn admit_and_reconstruct_all_redis(
    descriptor: &RedisConnectionDescriptor,
    candidate: RedisFormationAdmissionCandidate,
    credentials: RedisRoleCredentialSet,
    monitor: Arc<RedisCapabilityMonitor>,
    lifecycle_repositories: Option<Arc<WriterRepositoryBundle>>,
    shutdown: CancellationToken,
) -> Result<DistributedCoordinationBundle, RedisFormationCompositionError> {
    let admitted = admit_complete_redis_formation(descriptor, candidate, credentials)
        .await
        .map_err(RedisFormationCompositionError::Admission)?;
    compose_and_reconstruct_all_redis(admitted, monitor, lifecycle_repositories, shutdown).await
}

/// Composes every admitted role, reconstructs pending work, and only then
/// crosses the substrate-neutral boundary used by component roots.
pub async fn compose_and_reconstruct_all_redis(
    mut admitted: AdmittedRedisFormation,
    monitor: Arc<RedisCapabilityMonitor>,
    lifecycle_repositories: Option<Arc<WriterRepositoryBundle>>,
    shutdown: CancellationToken,
) -> Result<DistributedCoordinationBundle, RedisFormationCompositionError> {
    let process_id = uuid::Uuid::new_v4();
    admitted.compose_command_bus(&monitor, format!("command-bus-{process_id}"))?;
    admitted.compose_task_dispatch(&monitor, format!("task-dispatch-{process_id}"))?;
    admitted.compose_task_cancellation(&monitor)?;
    admitted.compose_task_events(&monitor, format!("task-events-{process_id}"))?;
    admitted.compose_liveness_watchdog(&monitor)?;
    admitted.compose_lifecycle_work(&monitor, lifecycle_repositories)?;
    admitted.compose_executor_fleet_status(&monitor)?;
    admitted
        .compose_signal_applied_notifier(&monitor, shutdown)
        .await?;
    admitted
        .compose_compaction_staging(&monitor, format!("compaction-{process_id}"))
        .await?;
    admitted.compose_log_staging(&monitor)?;
    admitted.compose_scope_store(&monitor).await?;
    admitted.compose_ingress_idempotency_store(&monitor)?;
    admitted
        .compose_event_ingress(&monitor, format!("event-ingress-{process_id}"))
        .await?;
    let ready = admitted
        .reconstruct_before_readiness(monitor)
        .await
        .map_err(RedisFormationCompositionError::Capability)?;
    ready.into_role_bundle().await
}

/// Completes every probe before the sole identity commit and returns no probe credential.
pub async fn admit_complete_redis_formation(
    descriptor: &RedisConnectionDescriptor,
    candidate: RedisFormationAdmissionCandidate,
    credentials: RedisRoleCredentialSet,
) -> Result<AdmittedRedisFormation, RedisAclAdmissionError> {
    let policy = RedisCanonicalAclPolicy::generate(
        candidate.operation_manifests(),
        candidate.namespace().as_str(),
    )?;
    let mut probe_connection = descriptor
        .connect_probe()
        .await
        .map_err(|error| RedisAclAdmissionError::base(error.failure()))?;
    let initial_namespace = inspect_redis_namespace(&mut probe_connection, &candidate)
        .await
        .map_err(|error| RedisAclAdmissionError::identity(error.failure()))?;

    let capability = admit_redis_capability(descriptor, &candidate)
        .await
        .map_err(|error| RedisAclAdmissionError::base(error.failure()))?;

    for role in ALL_COORDINATION_ROLES {
        let credential = credentials.get(role);
        let manifest = candidate.operation_manifests().get(role);
        let mut role_probe = descriptor
            .connect_with_credentials(&credential.identity, &credential.secret)
            .await
            .map_err(|_| {
                RedisAclAdmissionError::role(role, RedisAclAdmissionFailure::RoleCredentialRejected)
            })?;
        probe_required_operations(
            &mut probe_connection,
            &mut role_probe,
            credential.identity.as_str(),
            manifest,
            &candidate,
        )
        .await?;
        probe_forbidden_operations(
            &mut probe_connection,
            &mut role_probe,
            credential.identity.as_str(),
            manifest,
            &candidate,
        )
        .await?;
        verify_exact_acl_policy(
            &mut probe_connection,
            credential.identity.as_str(),
            policy.get(role),
        )
        .await?;
    }

    let after_probes = inspect_redis_namespace(&mut probe_connection, &candidate)
        .await
        .map_err(|error| RedisAclAdmissionError::identity(error.failure()))?;
    if after_probes != initial_namespace {
        return Err(RedisAclAdmissionError::new(
            RedisAclAdmissionFailure::NamespaceChangedDuringAdmission,
        ));
    }
    install_candidate_identity(&mut probe_connection, &candidate, initial_namespace).await?;
    drop(probe_connection);

    // Runtime clients are a post-admission product. A failed probe can never
    // leak a usable role connection into a partially admitted process.
    let mut clients = BTreeMap::new();
    for role in ALL_COORDINATION_ROLES {
        let credential = credentials.get(role);
        let connection = descriptor
            .connect_with_credentials(&credential.identity, &credential.secret)
            .await
            .map_err(|_| {
                RedisAclAdmissionError::role(role, RedisAclAdmissionFailure::RoleCredentialRejected)
            })?;
        let reconnect_client = if matches!(
            role,
            CoordinationRole::LifecycleWork
                | CoordinationRole::SignalAppliedNotifier
                | CoordinationRole::ExecutorFleetStatus
        ) {
            Some(
                descriptor
                    .client_with_credentials(&credential.identity, &credential.secret)
                    .map_err(|_| {
                        RedisAclAdmissionError::role(
                            role,
                            RedisAclAdmissionFailure::RoleCredentialRejected,
                        )
                    })?,
            )
        } else {
            None
        };
        clients.insert(
            role as usize,
            UntypedRoleClient {
                connection,
                reconnect_client,
                manifest_identity: candidate.operation_manifests().get(role).identity().clone(),
            },
        );
    }

    Ok(AdmittedRedisFormation {
        capability,
        candidate,
        role_clients: RedisRuntimeRoleClients::from_untyped(clients)?,
        command_bus: None,
        task_dispatch: None,
        task_cancellation: None,
        task_events: None,
        liveness_watchdog: None,
        lifecycle_work: None,
        executor_fleet_status: None,
        signal_applied: None,
        compaction_staging: None,
        compaction_log_staging: None,
        log_streams: None,
        scope_store: None,
        ingress_idempotency: None,
        event_ingress: None,
    })
}

async fn probe_required_operations(
    probe_connection: &mut MultiplexedConnection,
    role_connection: &mut MultiplexedConnection,
    identity: &str,
    manifest: &RedisOperationManifest,
    candidate: &RedisFormationAdmissionCandidate,
) -> Result<(), RedisAclAdmissionError> {
    for canary in manifest.required_canaries() {
        let admitted = match canary.operation() {
            RedisOperation::Script(_) => {
                execute_script_probe(role_connection, canary.target(), candidate)
                    .await
                    .is_ok()
            }
            RedisOperation::Command(_) => {
                let mut command = redis::cmd("ACL");
                command.arg("DRYRUN").arg(identity);
                append_probe_operation(
                    &mut command,
                    canary.operation(),
                    canary.target(),
                    candidate,
                )?;
                let result: Result<String, _> = command.query_async(&mut *probe_connection).await;
                matches!(result.as_deref(), Ok("OK"))
            }
        };
        if !admitted {
            return Err(RedisAclAdmissionError::role(
                manifest.role(),
                match canary.operation() {
                    RedisOperation::Script(_) => RedisAclAdmissionFailure::RequiredScriptOperation,
                    RedisOperation::Command(_) => {
                        RedisAclAdmissionFailure::RequiredCommandOperation
                    }
                },
            ));
        }
    }
    Ok(())
}

async fn probe_forbidden_operations(
    probe_connection: &mut MultiplexedConnection,
    role_connection: &mut MultiplexedConnection,
    identity: &str,
    manifest: &RedisOperationManifest,
    candidate: &RedisFormationAdmissionCandidate,
) -> Result<(), RedisAclAdmissionError> {
    for forbidden in manifest.forbidden_operations() {
        let denied = match forbidden.target() {
            RedisForbiddenTarget::CoordinationRole(target_role) => {
                let target_manifest = candidate.operation_manifests().get(target_role);
                let target = forbidden_target(forbidden.operation(), target_manifest)?;
                match forbidden.operation() {
                    RedisOperation::Script(_) => {
                        execute_cross_role_key_probe(role_connection, target, candidate)
                            .await
                            .is_err()
                    }
                    RedisOperation::Command(_) => {
                        let mut command = redis::cmd("ACL");
                        command.arg("DRYRUN").arg(identity);
                        append_probe_operation(
                            &mut command,
                            forbidden.operation(),
                            target,
                            candidate,
                        )?;
                        let result: Result<String, _> =
                            command.query_async(&mut *probe_connection).await;
                        result.is_err()
                    }
                }
            }
            RedisForbiddenTarget::Administrative => {
                let _: RedisOperation = forbidden.operation();
                let result: Result<Value, _> = redis::cmd("ACL")
                    .arg("LIST")
                    .query_async(&mut *role_connection)
                    .await;
                result.is_err()
            }
        };
        if !denied {
            return Err(RedisAclAdmissionError::role(
                manifest.role(),
                RedisAclAdmissionFailure::ForbiddenOperation,
            ));
        }
    }
    Ok(())
}

async fn execute_script_probe(
    connection: &mut MultiplexedConnection,
    target: RedisNamespacePattern,
    candidate: &RedisFormationAdmissionCandidate,
) -> Result<Value, redis::RedisError> {
    let target = concrete_canary_target(target.as_str(), candidate.namespace().as_str());
    redis::cmd("EVAL")
        .arg(
            "local result = redis.pcall('HGET', KEYS[1], ARGV[1]); \
             if type(result) == 'table' and result.err then \
               if string.find(result.err, 'NOPERM') then return redis.error_reply(result.err) end; \
               return false \
             end; \
             return result",
        )
        .arg(1)
        .arg(target)
        .arg("admission-canary")
        .query_async(connection)
        .await
}
async fn execute_cross_role_key_probe(
    connection: &mut MultiplexedConnection,
    target: RedisNamespacePattern,
    candidate: &RedisFormationAdmissionCandidate,
) -> Result<Value, redis::RedisError> {
    let target = concrete_canary_target(target.as_str(), candidate.namespace().as_str());
    redis::cmd("HGET")
        .arg(target)
        .arg("admission-canary")
        .query_async(connection)
        .await
}

fn forbidden_target(
    operation: RedisOperation,
    target_manifest: &RedisOperationManifest,
) -> Result<RedisNamespacePattern, RedisAclAdmissionError> {
    match operation {
        RedisOperation::Script(_) => target_manifest
            .key_patterns()
            .first()
            .copied()
            .map(RedisNamespacePattern::key),
        RedisOperation::Command(command)
            if command.contains("SUBSCRIBE") || command == "PUBLISH" =>
        {
            target_manifest
                .channel_patterns()
                .first()
                .copied()
                .map(RedisNamespacePattern::channel)
        }
        RedisOperation::Command(_) => target_manifest
            .key_patterns()
            .first()
            .copied()
            .map(RedisNamespacePattern::key),
    }
    .ok_or_else(|| RedisAclAdmissionError::new(RedisAclAdmissionFailure::InvalidManifestPolicy))
}

fn append_probe_operation(
    command: &mut redis::Cmd,
    operation: RedisOperation,
    target: RedisNamespacePattern,
    candidate: &RedisFormationAdmissionCandidate,
) -> Result<(), RedisAclAdmissionError> {
    let target = concrete_canary_target(target.as_str(), candidate.namespace().as_str());
    match operation {
        RedisOperation::Script(_) => {
            command
                .arg("EVAL")
                .arg("return redis.call('HGET', KEYS[1], ARGV[1])")
                .arg(1)
                .arg(target)
                .arg("admission-canary");
        }
        RedisOperation::Command("PSUBSCRIBE") => {
            command.arg("PUBLISH").arg(target).arg("admission-canary");
        }
        RedisOperation::Command(name) => {
            for component in name.split(' ') {
                command.arg(component);
            }
            append_command_arguments(command, name, target)?;
        }
    }
    Ok(())
}

fn append_command_arguments(
    command: &mut redis::Cmd,
    name: &str,
    target: String,
) -> Result<(), RedisAclAdmissionError> {
    match name {
        "GET" | "DEL" | "EXISTS" | "HGETALL" | "HLEN" | "HVALS" | "XRANGE" | "XLEN" | "ZCARD"
        | "PSUBSCRIBE" | "SUBSCRIBE" => {
            command.arg(target);
        }
        "SET" => {
            command.arg(target).arg("admission-canary");
        }
        "HGET" | "HDEL" => {
            command.arg(target).arg("field");
        }
        "HSET" => {
            command.arg(target).arg("field").arg("value");
        }
        "ZADD" => {
            command.arg(target).arg(0).arg("member");
        }
        "ZRANGEBYSCORE" | "ZREMRANGEBYSCORE" => {
            command.arg(target).arg("-inf").arg("+inf");
        }
        "ZREM" => {
            command.arg(target).arg("member");
        }
        "PUBLISH" => {
            command.arg(target).arg("admission-canary");
        }
        "XADD" => {
            command.arg(target).arg("*").arg("field").arg("value");
        }
        "XACK" | "XDEL" => {
            command.arg(target).arg("group").arg("0-1");
        }
        "XGROUP CREATE" => {
            command.arg(target).arg("group").arg("$").arg("MKSTREAM");
        }
        "XREADGROUP" => {
            command
                .arg("GROUP")
                .arg("group")
                .arg("consumer")
                .arg("STREAMS")
                .arg(target)
                .arg(">");
        }
        "XAUTOCLAIM" => {
            command
                .arg(target)
                .arg("group")
                .arg("consumer")
                .arg(1)
                .arg("0-0");
        }
        "TIME" => {}
        _ => {
            return Err(RedisAclAdmissionError::new(
                RedisAclAdmissionFailure::UnsupportedCanaryOperation,
            ));
        }
    }
    Ok(())
}

async fn verify_exact_acl_policy(
    connection: &mut MultiplexedConnection,
    identity: &str,
    expected: &RedisRoleAclPolicy,
) -> Result<(), RedisAclAdmissionError> {
    let actual: Result<HashMap<String, Value>, _> = redis::cmd("ACL")
        .arg("GETUSER")
        .arg(identity)
        .query_async(connection)
        .await;
    let Ok(actual) = actual else {
        return Err(RedisAclAdmissionError::role(
            expected.role,
            RedisAclAdmissionFailure::PolicyDrift,
        ));
    };

    let flags = acl_field::<Vec<String>>(&actual, "flags");
    let passwords = acl_field::<Vec<String>>(&actual, "passwords");
    let commands = acl_field::<String>(&actual, "commands");
    let keys = acl_rule_list(&actual, "keys");
    let channels = acl_rule_list(&actual, "channels");
    let selectors = acl_field::<Vec<Value>>(&actual, "selectors");
    let expected_commands = std::iter::once("-@all".to_owned())
        .chain(expected.command_rules.iter().cloned())
        .collect::<BTreeSet<_>>();
    let actual_commands = commands
        .as_deref()
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let expected_keys = expected
        .key_patterns
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_channels = expected
        .channel_patterns
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    let drift = |failure| Err(RedisAclAdmissionError::role(expected.role, failure));
    let expected_flags = BTreeSet::from(["on", "sanitize-payload"]);
    if !flags.as_ref().is_some_and(|values| {
        values.iter().map(String::as_str).collect::<BTreeSet<_>>() == expected_flags
    }) {
        return drift(RedisAclAdmissionFailure::PolicyFlagDrift);
    }
    if !passwords.as_ref().is_some_and(|values| values.len() == 1) {
        return drift(RedisAclAdmissionFailure::PolicyPasswordCountDrift);
    }
    if actual_commands != expected_commands {
        return drift(RedisAclAdmissionFailure::PolicyCommandDrift);
    }
    let actual_keys = keys
        .unwrap_or_default()
        .into_iter()
        .map(|value| normalize_acl_key_pattern(&value).to_owned())
        .collect::<BTreeSet<_>>();
    if !expected_keys.is_subset(&actual_keys) {
        return drift(RedisAclAdmissionFailure::PolicyKeyMissingGrant);
    }
    if !actual_keys.is_subset(&expected_keys) {
        return drift(RedisAclAdmissionFailure::PolicyKeyExtraGrant);
    }
    if !channels
        .map(|values| {
            values
                .into_iter()
                .map(|value| value.trim_start_matches('&').to_owned())
                .collect::<BTreeSet<_>>()
        })
        .is_some_and(|values| values == expected_channels)
    {
        return drift(RedisAclAdmissionFailure::PolicyChannelDrift);
    }
    if !selectors.is_some_and(|values| values.is_empty()) {
        return drift(RedisAclAdmissionFailure::PolicySelectorDrift);
    }
    Ok(())
}

fn normalize_acl_key_pattern(value: &str) -> &str {
    value
        .strip_prefix("%RW~")
        .or_else(|| value.strip_prefix("%R~"))
        .or_else(|| value.strip_prefix("%W~"))
        .or_else(|| value.strip_prefix('~'))
        .unwrap_or(value)
}

fn acl_rule_list(values: &HashMap<String, Value>, key: &str) -> Option<Vec<String>> {
    let value = values.get(key)?;
    let encoded = Vec::<String>::from_redis_value(value).ok().or_else(|| {
        String::from_redis_value(value)
            .ok()
            .map(|rules| vec![rules])
    })?;
    let mut rules = Vec::new();
    for value in encoded {
        rules.extend(value.split_whitespace().map(str::to_owned));
    }
    Some(rules)
}

fn acl_field<T: FromRedisValue>(values: &HashMap<String, Value>, key: &str) -> Option<T> {
    values
        .get(key)
        .and_then(|value| T::from_redis_value(value).ok())
}

async fn install_candidate_identity(
    connection: &mut MultiplexedConnection,
    candidate: &RedisFormationAdmissionCandidate,
    inspection: RedisNamespaceInspection,
) -> Result<(), RedisAclAdmissionError> {
    let namespace_pattern = format!("tickr:{}:*", candidate.namespace().as_str());
    let canary_prefix = format!("tickr:{}:admission:canary:", candidate.namespace().as_str());
    let installed: i64 = redis::cmd("EVAL")
        .arg(IDENTITY_INSTALL_SCRIPT)
        .arg(2)
        .arg(candidate.identity_key())
        .arg(candidate.fingerprint_key())
        .arg(candidate.normalized_identity_json())
        .arg(candidate.capability_fingerprint().as_str())
        .arg(namespace_pattern)
        .arg(canary_prefix)
        .query_async(&mut *connection)
        .await
        .map_err(|_| RedisAclAdmissionError::new(RedisAclAdmissionFailure::IdentityCommit))?;
    let expected = match inspection {
        RedisNamespaceInspection::Empty => 1,
        RedisNamespaceInspection::Matching => 0,
    };
    if installed != expected {
        return Err(RedisAclAdmissionError::new(
            RedisAclAdmissionFailure::IdentityCommit,
        ));
    }
    let fsync: (i64, i64) = redis::cmd("WAITAOF")
        .arg(1)
        .arg(0)
        .arg(2_000)
        .query_async(&mut *connection)
        .await
        .map_err(|_| RedisAclAdmissionError::new(RedisAclAdmissionFailure::IdentityCommit))?;
    if fsync.0 < 1 {
        return Err(RedisAclAdmissionError::new(
            RedisAclAdmissionFailure::IdentityCommit,
        ));
    }
    let final_inspection = inspect_redis_namespace(connection, candidate)
        .await
        .map_err(|error| RedisAclAdmissionError::identity(error.failure()))?;
    if final_inspection != RedisNamespaceInspection::Matching {
        return Err(RedisAclAdmissionError::new(
            RedisAclAdmissionFailure::IdentityCommit,
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisAclAdmissionFailure {
    MissingRoleCredential,
    DuplicateRoleCredential,
    UnexpectedRoleCredential,
    DuplicateAclIdentity,
    DuplicateAclSecret,
    InvalidRoleCredential,
    InvalidManifestPolicy,
    UnsupportedCanaryOperation,
    BaseCapability(RedisAdmissionFailure),
    Namespace(RedisFormationIdentityFailure),
    RoleCredentialRejected,
    RequiredScriptOperation,
    RequiredCommandOperation,
    ForbiddenOperation,
    PolicyDrift,
    PolicyFlagDrift,
    PolicyPasswordCountDrift,
    PolicyCommandDrift,
    PolicyKeyMissingGrant,
    PolicyKeyExtraGrant,
    PolicyChannelDrift,
    PolicySelectorDrift,
    MissingRoleClient,
    UnexpectedRoleClient,
    NamespaceChangedDuringAdmission,
    IdentityCommit,
}

impl RedisAclAdmissionFailure {
    const fn description(self) -> &'static str {
        match self {
            Self::MissingRoleCredential => "a role ACL credential is missing",
            Self::DuplicateRoleCredential => "a role ACL credential is duplicated",
            Self::UnexpectedRoleCredential => "an unexpected role ACL credential was supplied",
            Self::DuplicateAclIdentity => "role ACL identities must be distinct",
            Self::DuplicateAclSecret => "role ACL secrets must be distinct",
            Self::InvalidRoleCredential => "a role ACL identity or secret is empty",
            Self::InvalidManifestPolicy => {
                "an operation manifest cannot produce an exact ACL policy"
            }
            Self::UnsupportedCanaryOperation => "a required operation has no safe admission canary",
            Self::BaseCapability(_) => "a required Redis formation capability failed",
            Self::Namespace(_) => "the Redis namespace identity check failed",
            Self::RoleCredentialRejected => "the role ACL credential was rejected",
            Self::RequiredScriptOperation => "a required role script operation was denied",
            Self::RequiredCommandOperation => "a required role command operation was denied",
            Self::ForbiddenOperation => "a representative forbidden role operation succeeded",
            Self::PolicyDrift => "the installed role ACL differs from its operation manifest",
            Self::PolicyFlagDrift => "the installed role ACL flags differ from its manifest",
            Self::PolicyPasswordCountDrift => {
                "the installed role ACL password count differs from its configured secret"
            }
            Self::PolicyCommandDrift => {
                "the installed role ACL command grants differ from its manifest"
            }
            Self::PolicyKeyMissingGrant => {
                "the installed role ACL is missing a manifest key pattern"
            }
            Self::PolicyKeyExtraGrant => {
                "the installed role ACL grants a key pattern absent from its manifest"
            }
            Self::PolicyChannelDrift => {
                "the installed role ACL channel patterns differ from its manifest"
            }
            Self::PolicySelectorDrift => {
                "the installed role ACL selectors differ from its manifest"
            }
            Self::MissingRoleClient => "an admitted role client is missing",
            Self::UnexpectedRoleClient => "an unexpected admitted role client exists",
            Self::NamespaceChangedDuringAdmission => "the Redis namespace changed during admission",
            Self::IdentityCommit => "the admitted formation identity could not be committed",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RedisAclAdmissionError {
    role: Option<CoordinationRole>,
    failure: RedisAclAdmissionFailure,
}

impl RedisAclAdmissionError {
    const fn new(failure: RedisAclAdmissionFailure) -> Self {
        Self {
            role: None,
            failure,
        }
    }

    const fn role(role: CoordinationRole, failure: RedisAclAdmissionFailure) -> Self {
        Self {
            role: Some(role),
            failure,
        }
    }

    const fn base(failure: RedisAdmissionFailure) -> Self {
        Self::new(RedisAclAdmissionFailure::BaseCapability(failure))
    }

    const fn identity(failure: RedisFormationIdentityFailure) -> Self {
        Self::new(RedisAclAdmissionFailure::Namespace(failure))
    }

    pub const fn role_context(&self) -> Option<CoordinationRole> {
        self.role
    }

    pub const fn failure(&self) -> RedisAclAdmissionFailure {
        self.failure
    }
}

impl fmt::Display for RedisAclAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("complete Redis ACL admission failed")?;
        if let Some(role) = self.role {
            write!(formatter, " for {}", role_name(role))?;
        }
        write!(formatter, ": {}", self.failure.description())
    }
}

impl fmt::Debug for RedisAclAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisAclAdmissionError")
            .field("role", &self.role.map(role_name))
            .field("failure", &self.failure)
            .finish()
    }
}

impl std::error::Error for RedisAclAdmissionError {}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        formation::{resolve_formation, FormationSelection},
        redis_capacity::{calibrated_role_capacity, ROLE_MEMORY_LIMIT_NAME},
        redis_formation_identity::{
            RedisDurabilityConfiguration, RedisNamespaceIdentity, RedisRoleLimits,
        },
    };

    const CAPACITY: u64 = 2_000_000_000;

    fn candidate() -> RedisFormationAdmissionCandidate {
        let descriptor = resolve_formation(&FormationSelection::all_redis()).unwrap();
        let limits = ALL_COORDINATION_ROLES
            .iter()
            .map(|role| {
                RedisRoleLimits::new(
                    *role,
                    BTreeMap::from([(
                        ROLE_MEMORY_LIMIT_NAME.to_owned(),
                        calibrated_role_capacity(*role).default_bytes,
                    )]),
                    BTreeMap::from([("retention-seconds".to_owned(), 60)]),
                )
                .unwrap()
            })
            .collect();
        RedisFormationAdmissionCandidate::construct(
            &descriptor,
            canonical_redis_operation_manifests().unwrap(),
            RedisNamespaceIdentity::new("acl-unit").unwrap(),
            limits,
            RedisDurabilityConfiguration::primary_local_aof(CAPACITY),
        )
        .unwrap()
    }

    fn credentials() -> Vec<RedisRoleCredential> {
        ALL_COORDINATION_ROLES
            .iter()
            .enumerate()
            .map(|(index, role)| {
                RedisRoleCredential::new(*role, format!("role-{index}"), format!("secret-{index}"))
            })
            .collect()
    }

    #[test]
    fn canonical_policy_is_an_exact_projection_of_all_thirteen_manifests() {
        let candidate = candidate();
        let policy = RedisCanonicalAclPolicy::generate(
            candidate.operation_manifests(),
            candidate.namespace().as_str(),
        )
        .unwrap();
        assert_eq!(policy.roles().len(), ALL_COORDINATION_ROLES.len());
        for role in ALL_COORDINATION_ROLES {
            let manifest = candidate.operation_manifests().get(role);
            let grants = policy.get(role);
            assert_eq!(grants.role(), role);
            assert_eq!(grants.manifest_identity(), manifest.identity());
            assert_eq!(grants.scripts(), manifest.scripts());
            assert_eq!(grants.command_rules().len(), manifest.commands().len());
            assert_eq!(grants.key_patterns().len(), manifest.key_patterns().len());
            assert_eq!(
                grants.channel_patterns().len(),
                manifest.channel_patterns().len()
            );
            assert!(grants
                .key_patterns()
                .iter()
                .chain(grants.channel_patterns())
                .all(|pattern| !pattern.contains("{namespace}") && pattern.contains("acl-unit")));
        }
    }

    #[test]
    fn credentials_require_one_distinct_identity_and_secret_per_role() {
        let mut missing = credentials();
        let missing_role = missing.pop().unwrap().role();
        let error = RedisRoleCredentialSet::admit(missing).unwrap_err();
        assert_eq!(error.role_context(), Some(missing_role));
        assert_eq!(
            error.failure(),
            RedisAclAdmissionFailure::MissingRoleCredential
        );

        let mut duplicate_role = credentials();
        duplicate_role.push(RedisRoleCredential::new(
            CoordinationRole::CommandBus,
            "another",
            "another-secret",
        ));
        let error = RedisRoleCredentialSet::admit(duplicate_role).unwrap_err();
        assert_eq!(
            error.failure(),
            RedisAclAdmissionFailure::DuplicateRoleCredential
        );

        let mut duplicate_identity = credentials();
        duplicate_identity[1].identity = duplicate_identity[0].identity.clone();
        let error = RedisRoleCredentialSet::admit(duplicate_identity).unwrap_err();
        assert_eq!(
            error.failure(),
            RedisAclAdmissionFailure::DuplicateAclIdentity
        );

        let mut duplicate_secret = credentials();
        duplicate_secret[1].secret = duplicate_secret[0].secret.clone();
        let error = RedisRoleCredentialSet::admit(duplicate_secret).unwrap_err();
        assert_eq!(
            error.failure(),
            RedisAclAdmissionFailure::DuplicateAclSecret
        );
        assert!(!format!("{error:?} {error}").contains("secret-0"));
    }
}
