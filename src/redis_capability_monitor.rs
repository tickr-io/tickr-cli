use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

use async_trait::async_trait;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::{
    formation::{CoordinationRole, ALL_COORDINATION_ROLES},
    redis_admission::{
        admit_redis_capability, AdmittedRedisCapability, RedisAdmissionFailure,
        RedisConnectionDescriptor,
    },
    redis_capacity::{RedisCapacityFailure, RedisCapacityProjection, RedisQuotaState},
    redis_formation_identity::{
        RedisFormationAdmissionCandidate, RoleLimitsProjection, RoleProjection,
    },
    redis_operation_manifest::{RedisOperationManifestIdentity, RedisOperationManifestProjection},
};

const PRIMARY_LOCAL_DURABILITY_CLASS: &str =
    "one local-primary AOF fsync, zero required replica acknowledgements";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedisCapabilityFenceState {
    Closed,
    Reconstructing,
    Open,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RedisCapabilityFenceSnapshot {
    pub state: RedisCapabilityFenceState,
    pub generation: u64,
    pub ready: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisGenerationPermit {
    generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisGenerationFenceError {
    Closed,
    StaleGeneration,
}

impl fmt::Display for RedisGenerationFenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Closed => "the Redis capability generation fence is closed",
            Self::StaleGeneration => "the Redis capability generation changed",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RedisGenerationFenceError {}

#[derive(Clone)]
pub struct RedisGenerationFence {
    state: Arc<Mutex<RuntimeCapabilityState>>,
}

impl RedisGenerationFence {
    pub fn guard_admission(&self) -> Result<RedisGenerationPermit, RedisGenerationFenceError> {
        let state = lock_mutex(&self.state);
        if state.fence != RedisCapabilityFenceState::Open {
            return Err(RedisGenerationFenceError::Closed);
        }
        Ok(RedisGenerationPermit {
            generation: state.generation,
        })
    }

    pub fn guard_acknowledgement(
        &self,
        permit: RedisGenerationPermit,
    ) -> Result<(), RedisGenerationFenceError> {
        let state = lock_mutex(&self.state);
        if state.fence != RedisCapabilityFenceState::Open {
            return Err(RedisGenerationFenceError::Closed);
        }
        if state.generation != permit.generation {
            return Err(RedisGenerationFenceError::StaleGeneration);
        }
        Ok(())
    }

    pub fn snapshot(&self) -> RedisCapabilityFenceSnapshot {
        let state = lock_mutex(&self.state);
        state.fence_snapshot()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisBaseCapabilityFailure {
    Transport,
    Topology,
    Persistence,
    NoEviction,
    Reserve,
    ServerTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisRoleCapabilityFailure {
    ReadOnly,
    OutOfMemory,
    LocalFsync,
    Accounting,
    MissingAcceptedIdentity,
    UnexpectedTrim,
    RequiredOperation,
    RepresentativeDenial,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisReconstructionFailure {
    PendingEvidenceUnavailable,
    DurabilityUnproved,
    GenerationConflict,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisCapabilityFailureProjection {
    pub capability: String,
    pub role: Option<String>,
    pub reason: String,
}

impl RedisCapabilityFailureProjection {
    fn base(failure: RedisBaseCapabilityFailure) -> Self {
        let (capability, reason) = match failure {
            RedisBaseCapabilityFailure::Transport => {
                ("transport", "Redis transport capability was lost")
            }
            RedisBaseCapabilityFailure::Topology => (
                "topology",
                "the admitted writable-primary topology was lost",
            ),
            RedisBaseCapabilityFailure::Persistence => (
                "aof_fsync",
                "the admitted primary-local AOF fsync capability was lost",
            ),
            RedisBaseCapabilityFailure::NoEviction => {
                ("no_eviction", "the admitted no-eviction policy was lost")
            }
            RedisBaseCapabilityFailure::Reserve => {
                ("reserve", "the admitted Redis capacity reserve was lost")
            }
            RedisBaseCapabilityFailure::ServerTime => {
                ("server_time", "Redis server time became unsuitable")
            }
        };
        Self {
            capability: capability.to_owned(),
            role: None,
            reason: reason.to_owned(),
        }
    }

    fn role(role: CoordinationRole, failure: RedisRoleCapabilityFailure) -> Self {
        let reason = match failure {
            RedisRoleCapabilityFailure::ReadOnly => "a role operation observed read-only Redis",
            RedisRoleCapabilityFailure::OutOfMemory => "a role operation observed Redis OOM",
            RedisRoleCapabilityFailure::LocalFsync => {
                "a role operation could not prove primary-local AOF fsync"
            }
            RedisRoleCapabilityFailure::Accounting => {
                "a role observed inconsistent exact quota accounting"
            }
            RedisRoleCapabilityFailure::MissingAcceptedIdentity => {
                "a role could not find an accepted stable identity"
            }
            RedisRoleCapabilityFailure::UnexpectedTrim => {
                "a role observed correctness-critical state trimmed unexpectedly"
            }
            RedisRoleCapabilityFailure::RequiredOperation => {
                "a registered required-operation probe failed"
            }
            RedisRoleCapabilityFailure::RepresentativeDenial => {
                "a registered representative forbidden operation succeeded"
            }
        };
        Self {
            capability: "role_operation".to_owned(),
            role: Some(role_name(role).to_owned()),
            reason: reason.to_owned(),
        }
    }

    fn fingerprint_changed() -> Self {
        Self {
            capability: "capability_fingerprint".to_owned(),
            role: None,
            reason: "the capability fingerprint changed".to_owned(),
        }
    }

    fn reconstruction(role: CoordinationRole, failure: RedisReconstructionFailure) -> Self {
        let reason = match failure {
            RedisReconstructionFailure::PendingEvidenceUnavailable => {
                "registered pending evidence could not be reconstructed"
            }
            RedisReconstructionFailure::DurabilityUnproved => {
                "reconstruction could not prove Redis durability"
            }
            RedisReconstructionFailure::GenerationConflict => {
                "reconstruction observed a generation conflict"
            }
        };
        Self {
            capability: "reconstruction".to_owned(),
            role: Some(role_name(role).to_owned()),
            reason: reason.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisCapabilityObservation {
    capability_fingerprint: String,
    capability: AdmittedRedisCapability,
}

impl RedisCapabilityObservation {
    pub fn new(
        capability_fingerprint: impl Into<String>,
        capability: AdmittedRedisCapability,
    ) -> Self {
        Self {
            capability_fingerprint: capability_fingerprint.into(),
            capability,
        }
    }

    pub fn capability_fingerprint(&self) -> &str {
        &self.capability_fingerprint
    }

    pub fn capability(&self) -> &AdmittedRedisCapability {
        &self.capability
    }
}

#[async_trait]
pub trait RedisFormationCapabilityProbe: Send + Sync {
    async fn probe(&self) -> Result<RedisCapabilityObservation, RedisBaseCapabilityFailure>;
}

pub struct RedisAdmissionCapabilityProbe {
    descriptor: Arc<RedisConnectionDescriptor>,
    candidate: RedisFormationAdmissionCandidate,
}

impl RedisAdmissionCapabilityProbe {
    pub fn new(
        descriptor: Arc<RedisConnectionDescriptor>,
        candidate: RedisFormationAdmissionCandidate,
    ) -> Self {
        Self {
            descriptor,
            candidate,
        }
    }
}

#[async_trait]
impl RedisFormationCapabilityProbe for RedisAdmissionCapabilityProbe {
    async fn probe(&self) -> Result<RedisCapabilityObservation, RedisBaseCapabilityFailure> {
        let capability = admit_redis_capability(&self.descriptor, &self.candidate)
            .await
            .map_err(|error| classify_admission_failure(error.failure()))?;
        Ok(RedisCapabilityObservation::new(
            self.candidate.capability_fingerprint().as_str(),
            capability,
        ))
    }
}

fn classify_admission_failure(failure: RedisAdmissionFailure) -> RedisBaseCapabilityFailure {
    match failure {
        RedisAdmissionFailure::MalformedDescriptor
        | RedisAdmissionFailure::NoEndpoints
        | RedisAdmissionFailure::PlaintextTransport
        | RedisAdmissionFailure::MissingTrustRoots
        | RedisAdmissionFailure::MissingCredentials
        | RedisAdmissionFailure::EndpointParameters
        | RedisAdmissionFailure::CredentialsInEndpoint
        | RedisAdmissionFailure::SentinelTopology
        | RedisAdmissionFailure::TlsValidation
        | RedisAdmissionFailure::CredentialRejected
        | RedisAdmissionFailure::ProbeTimedOut
        | RedisAdmissionFailure::ProbeProtocol => RedisBaseCapabilityFailure::Transport,
        RedisAdmissionFailure::ServerIdentity
        | RedisAdmissionFailure::ServerVersion
        | RedisAdmissionFailure::RequiredCommandBehavior
        | RedisAdmissionFailure::ReadOnlyOrReplica
        | RedisAdmissionFailure::ClusterTopology
        | RedisAdmissionFailure::MultipleWritablePrimaries => RedisBaseCapabilityFailure::Topology,
        RedisAdmissionFailure::ServerTimeUnavailable | RedisAdmissionFailure::ServerTimeInvalid => {
            RedisBaseCapabilityFailure::ServerTime
        }
        RedisAdmissionFailure::AofDisabled
        | RedisAdmissionFailure::AppendFsyncNotAlways
        | RedisAdmissionFailure::LocalFsyncProofFailed
        | RedisAdmissionFailure::DurabilityCanaryFailed => RedisBaseCapabilityFailure::Persistence,
        RedisAdmissionFailure::InvalidCapacity(RedisCapacityFailure::EvictionPolicy) => {
            RedisBaseCapabilityFailure::NoEviction
        }
        RedisAdmissionFailure::InvalidCapacity(_) => RedisBaseCapabilityFailure::Reserve,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisRoleProbeContext {
    role: CoordinationRole,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisRoleProbeContext {
    pub fn role(&self) -> CoordinationRole {
        self.role
    }

    pub fn manifest_identity(&self) -> &RedisOperationManifestIdentity {
        &self.manifest_identity
    }
}

#[async_trait]
pub trait RedisRoleCapabilityProbe: Send + Sync {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure>;

    async fn probe_representative_denials(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure>;
}

#[async_trait]
pub trait RedisReconstructionCallback: Send + Sync {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure>;
}

pub struct RedisRoleCapabilityRegistration {
    role: CoordinationRole,
    probe: Arc<dyn RedisRoleCapabilityProbe>,
    reconstruction: Arc<dyn RedisReconstructionCallback>,
}

impl RedisRoleCapabilityRegistration {
    pub fn new(
        role: CoordinationRole,
        probe: Arc<dyn RedisRoleCapabilityProbe>,
        reconstruction: Arc<dyn RedisReconstructionCallback>,
    ) -> Self {
        Self {
            role,
            probe,
            reconstruction,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisRoleRegistrationError {
    DuplicateRole,
}

impl fmt::Display for RedisRoleRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the Redis role capability is already registered")
    }
}

impl std::error::Error for RedisRoleRegistrationError {}

#[derive(Clone)]
pub struct RedisRoleCapabilityReporter {
    role: CoordinationRole,
    state: Arc<Mutex<RuntimeCapabilityState>>,
}

impl RedisRoleCapabilityReporter {
    pub fn report(&self, failure: RedisRoleCapabilityFailure) {
        close_with_failure(
            &mut lock_mutex(&self.state),
            RedisCapabilityFailureProjection::role(self.role, failure),
        );
    }

    pub fn report_quota_state(&self, quota_state: RedisQuotaState) {
        lock_mutex(&self.state)
            .quota_states
            .insert(self.role as usize, quota_state);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisRoleQuotaDiagnostics {
    pub role: String,
    pub state: RedisQuotaState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedisCapabilityDiagnostics {
    pub capability_fingerprint: String,
    pub profile: String,
    pub redis_implementation: Option<String>,
    pub redis_version: Option<String>,
    pub topology_class: Option<String>,
    pub role_protocols: Vec<RoleProjection>,
    pub operation_manifests: Vec<RedisOperationManifestProjection>,
    pub durability_class: String,
    pub normalized_limits: Vec<RoleLimitsProjection>,
    pub capacity: Option<RedisCapacityProjection>,
    pub quota_state: Vec<RedisRoleQuotaDiagnostics>,
    pub fence: RedisCapabilityFenceSnapshot,
    pub last_capability_failure: Option<RedisCapabilityFailureProjection>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisCapabilityMonitorError {
    CapabilityLost,
    ConcurrentCapabilityLoss,
    ReconstructionFailed,
    InvalidInterval,
    IncompleteRoleSet,
    CandidateMismatch,
}

impl fmt::Display for RedisCapabilityMonitorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::CapabilityLost => "a required Redis capability is unavailable",
            Self::ConcurrentCapabilityLoss => {
                "Redis capability changed while recovery was in progress"
            }
            Self::ReconstructionFailed => "registered Redis reconstruction failed",
            Self::IncompleteRoleSet => {
                "all thirteen Redis role probes and reconstruction callbacks are required"
            }
            Self::CandidateMismatch => {
                "the capability monitor does not match the admitted Redis formation"
            }
            Self::InvalidInterval => "Redis capability monitor interval must be non-zero",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RedisCapabilityMonitorError {}

pub struct RedisCapabilityMonitor {
    candidate: RedisFormationAdmissionCandidate,
    formation_probe: Arc<dyn RedisFormationCapabilityProbe>,
    roles: RwLock<Vec<RegisteredRoleCapability>>,
    state: Arc<Mutex<RuntimeCapabilityState>>,
    run_lock: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
struct RegisteredRoleCapability {
    context: RedisRoleProbeContext,
    probe: Arc<dyn RedisRoleCapabilityProbe>,
    reconstruction: Arc<dyn RedisReconstructionCallback>,
}

#[derive(Clone, Debug)]
struct RuntimeCapabilityState {
    fence: RedisCapabilityFenceState,
    generation: u64,
    last_failure: Option<RedisCapabilityFailureProjection>,
    admitted_capability: Option<AdmittedRedisCapability>,
    quota_states: BTreeMap<usize, RedisQuotaState>,
}

impl RuntimeCapabilityState {
    fn fence_snapshot(&self) -> RedisCapabilityFenceSnapshot {
        RedisCapabilityFenceSnapshot {
            state: self.fence,
            generation: self.generation,
            ready: self.fence == RedisCapabilityFenceState::Open,
        }
    }
}

impl RedisCapabilityMonitor {
    pub fn new(
        candidate: RedisFormationAdmissionCandidate,
        formation_probe: Arc<dyn RedisFormationCapabilityProbe>,
    ) -> Self {
        Self {
            candidate,
            formation_probe,
            roles: RwLock::new(Vec::new()),
            state: Arc::new(Mutex::new(RuntimeCapabilityState {
                fence: RedisCapabilityFenceState::Closed,
                generation: 0,
                last_failure: None,
                admitted_capability: None,
                quota_states: BTreeMap::new(),
            })),
            run_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn register_role(
        &self,
        registration: RedisRoleCapabilityRegistration,
    ) -> Result<RedisRoleCapabilityReporter, RedisRoleRegistrationError> {
        let mut roles = write_lock(&self.roles);
        if roles
            .iter()
            .any(|registered| registered.context.role == registration.role)
        {
            return Err(RedisRoleRegistrationError::DuplicateRole);
        }
        let manifest_identity = self
            .candidate
            .operation_manifests()
            .get(registration.role)
            .identity()
            .clone();
        roles.push(RegisteredRoleCapability {
            context: RedisRoleProbeContext {
                role: registration.role,
                manifest_identity,
            },
            probe: registration.probe,
            reconstruction: registration.reconstruction,
        });
        Ok(RedisRoleCapabilityReporter {
            role: registration.role,
            state: Arc::clone(&self.state),
        })
    }

    pub fn fence(&self) -> RedisGenerationFence {
        RedisGenerationFence {
            state: Arc::clone(&self.state),
        }
    }

    pub fn diagnostics(&self) -> RedisCapabilityDiagnostics {
        let projection = self.candidate.fingerprint_projection();
        let state = lock_mutex(&self.state);
        RedisCapabilityDiagnostics {
            capability_fingerprint: self.candidate.capability_fingerprint().as_str().to_owned(),
            profile: projection.profile.clone(),
            redis_implementation: state
                .admitted_capability
                .as_ref()
                .map(|_| "redis_oss".to_owned()),
            redis_version: state
                .admitted_capability
                .as_ref()
                .map(|capability| capability.server_version.clone()),
            topology_class: state
                .admitted_capability
                .as_ref()
                .map(|_| "single_writable_primary".to_owned()),
            role_protocols: projection.roles.clone(),
            operation_manifests: projection.operation_manifests.clone(),
            durability_class: PRIMARY_LOCAL_DURABILITY_CLASS.to_owned(),
            normalized_limits: projection.role_limits.clone(),
            capacity: state.admitted_capability.as_ref().map(|capability| {
                capability
                    .capacity_profile
                    .projection(capability.used_memory_bytes)
            }),
            quota_state: state
                .quota_states
                .iter()
                .map(|(role, quota_state)| RedisRoleQuotaDiagnostics {
                    role: role_name_by_index(*role).to_owned(),
                    state: *quota_state,
                })
                .collect(),
            fence: state.fence_snapshot(),
            last_capability_failure: state.last_failure.clone(),
        }
    }

    pub fn matches_candidate(&self, candidate: &RedisFormationAdmissionCandidate) -> bool {
        self.candidate.capability_fingerprint() == candidate.capability_fingerprint()
    }

    pub fn has_complete_role_set(&self) -> bool {
        let roles = read_lock(&self.roles);
        roles.len() == ALL_COORDINATION_ROLES.len()
            && ALL_COORDINATION_ROLES.iter().all(|role| {
                roles
                    .iter()
                    .any(|registered| registered.context.role == *role)
            })
    }

    /// Performs the complete capability pass and every role reconstruction
    /// before opening the generation fence used as formation readiness.
    pub async fn reconstruct_before_readiness(&self) -> Result<(), RedisCapabilityMonitorError> {
        if !self.has_complete_role_set() {
            return Err(RedisCapabilityMonitorError::IncompleteRoleSet);
        }
        self.run_once().await
    }

    pub async fn run_once(&self) -> Result<(), RedisCapabilityMonitorError> {
        let _run_guard = self.run_lock.lock().await;
        let starting = self.fence().snapshot();
        let observation = match self.formation_probe.probe().await {
            Ok(observation) => observation,
            Err(failure) => {
                close_with_failure(
                    &mut lock_mutex(&self.state),
                    RedisCapabilityFailureProjection::base(failure),
                );
                return Err(RedisCapabilityMonitorError::CapabilityLost);
            }
        };
        if observation.capability_fingerprint() != self.candidate.capability_fingerprint().as_str()
        {
            close_with_failure(
                &mut lock_mutex(&self.state),
                RedisCapabilityFailureProjection::fingerprint_changed(),
            );
            return Err(RedisCapabilityMonitorError::CapabilityLost);
        }

        let roles = read_lock(&self.roles).clone();
        for registered in &roles {
            if let Err(failure) = registered
                .probe
                .probe_required_operations(&registered.context)
                .await
            {
                close_with_failure(
                    &mut lock_mutex(&self.state),
                    RedisCapabilityFailureProjection::role(registered.context.role, failure),
                );
                return Err(RedisCapabilityMonitorError::CapabilityLost);
            }
            if let Err(failure) = registered
                .probe
                .probe_representative_denials(&registered.context)
                .await
            {
                close_with_failure(
                    &mut lock_mutex(&self.state),
                    RedisCapabilityFailureProjection::role(registered.context.role, failure),
                );
                return Err(RedisCapabilityMonitorError::CapabilityLost);
            }
        }

        {
            let mut state = lock_mutex(&self.state);
            state.admitted_capability = Some(observation.capability().clone());
            if state.generation != starting.generation {
                return Err(RedisCapabilityMonitorError::ConcurrentCapabilityLoss);
            }
            if state.fence == RedisCapabilityFenceState::Open {
                return Ok(());
            }
            if state.fence != RedisCapabilityFenceState::Closed {
                return Err(RedisCapabilityMonitorError::ConcurrentCapabilityLoss);
            }
            state.fence = RedisCapabilityFenceState::Reconstructing;
        }

        for registered in &roles {
            if let Err(failure) = registered
                .reconstruction
                .reconstruct(&registered.context)
                .await
            {
                close_with_failure(
                    &mut lock_mutex(&self.state),
                    RedisCapabilityFailureProjection::reconstruction(
                        registered.context.role,
                        failure,
                    ),
                );
                return Err(RedisCapabilityMonitorError::ReconstructionFailed);
            }
        }

        let mut state = lock_mutex(&self.state);
        if state.generation != starting.generation
            || state.fence != RedisCapabilityFenceState::Reconstructing
        {
            return Err(RedisCapabilityMonitorError::ConcurrentCapabilityLoss);
        }
        state.fence = RedisCapabilityFenceState::Open;
        Ok(())
    }

    pub async fn run(
        &self,
        interval: Duration,
        shutdown: CancellationToken,
    ) -> Result<(), RedisCapabilityMonitorError> {
        if interval.is_zero() {
            return Err(RedisCapabilityMonitorError::InvalidInterval);
        }
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticks.tick() => {
                    let _ = self.run_once().await;
                }
            }
        }
    }
}

fn close_with_failure(
    state: &mut RuntimeCapabilityState,
    failure: RedisCapabilityFailureProjection,
) {
    state.generation = state.generation.wrapping_add(1);
    state.fence = RedisCapabilityFenceState::Closed;
    state.last_failure = Some(failure);
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

fn role_name_by_index(index: usize) -> &'static str {
    match index {
        0 => role_name(CoordinationRole::CommandBus),
        1 => role_name(CoordinationRole::TaskDispatch),
        2 => role_name(CoordinationRole::TaskEvents),
        3 => role_name(CoordinationRole::TaskCancellation),
        4 => role_name(CoordinationRole::CompactionStaging),
        5 => role_name(CoordinationRole::LifecycleWork),
        6 => role_name(CoordinationRole::LogStaging),
        7 => role_name(CoordinationRole::ScopeStore),
        8 => role_name(CoordinationRole::IngressIdempotencyStore),
        9 => role_name(CoordinationRole::LivenessWatchdog),
        10 => role_name(CoordinationRole::SignalAppliedNotifier),
        11 => role_name(CoordinationRole::ExecutorFleetStatus),
        12 => role_name(CoordinationRole::EventIngress),
        _ => "unknown-role",
    }
}
