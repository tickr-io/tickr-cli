use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::MultiplexedConnection;
use sha2::{Digest, Sha256};
use tickr_executor::{
    local_pickup::{
        CancellationReconciliation, LocalAttemptOutcome, LocalCancellationFence,
        SafeCancellationCoordinator, SafeCancellationFence, SafeCancellationRole, TerminalElection,
    },
    wire::{encode_cancel_ack, CancelRequest, KillOutcome},
};
use tickr_proto::coord::{
    TaskCancellationAckConsumer, TaskCancellationAckDelivery, TaskCancellationFuture,
    TaskCancellationPublisher,
};

use crate::{
    formation::{CoordinationRole, ProtocolIdentity},
    redis_capability_monitor::{
        RedisCapabilityFenceState, RedisGenerationFence, RedisReconstructionCallback,
        RedisReconstructionFailure, RedisRoleCapabilityFailure, RedisRoleCapabilityProbe,
        RedisRoleCapabilityReporter, RedisRoleProbeContext,
    },
    redis_capacity::{RedisQuotaPressure, RedisQuotaState},
    redis_durability::{
        RedisDurabilityError, RedisDurabilityFailure, RedisDurabilityGuard, RedisMutationError,
        RedisStableMutation, RedisStableMutationOutcome, RedisStableMutationRecovery,
        RedisStableOperation,
    },
    redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisRequiredOperationCanary,
        RedisScriptIdentity,
    },
    redis_task_pickup::{RedisCancellationBinding, RedisTaskDispatch},
};

pub const REDIS_TASK_CANCELLATION_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.task-cancellation.redis-fence", 1);

const DEFAULT_MAX_REQUESTS: usize = 4096;
const DEFAULT_MAX_FENCES: usize = 4096;
const DEFAULT_MAX_ACKNOWLEDGEMENTS: usize = 16_384;
const DEFAULT_SOFT_BYTES: u64 = 24 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 28 * 1024 * 1024;
const REQUEST_ACCOUNTED_BYTES: u64 = 192;
const FENCE_ACCOUNTED_BYTES: u64 = 256;
const ACK_ACCOUNTED_BYTES: u64 = 128;
const SCAN_LIMIT: usize = 256;

const REDIS_TASK_CANCELLATION_COMMANDS: &[&str] = &[
    "EVAL", "GET", "HGET", "HINCRBY", "HMGET", "HSCAN", "HSET", "SET", "WAITAOF",
];

const TASK_CANCELLATION_SCRIPT_NAME: &str = "task-cancellation-v1";
const TASK_CANCELLATION_SCRIPT_SHA256: &str =
    "6611267b7d1056ef320e8d43cf227988a13c24421e12341c268fd33b987cf7bd";

const TASK_CANCELLATION_SCRIPT: &str = r#"local operation = ARGV[1]
local stable_digest = ARGV[2]
local identity = ARGV[3]
local request_digest = ARGV[4]
local task_key = ARGV[5]
local expected_dispatch = ARGV[6]
local expected_generation = ARGV[7]
local expected_owner = ARGV[8]
local requested_outcome = ARGV[9]
local killed_ack = ARGV[10]
local no_process_ack = ARGV[11]
local request_units = tonumber(ARGV[12])
local ack_units = tonumber(ARGV[13])
local max_requests = tonumber(ARGV[14])
local max_fences = tonumber(ARGV[15])
local max_acknowledgements = tonumber(ARGV[16])
local hard_bytes = tonumber(ARGV[17])

local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function state(status, detail)
    return {
        status,
        detail or '',
        redis.call('HGET', KEYS[4], identity) or '',
        redis.call('HGET', KEYS[5], identity) or '',
        redis.call('HGET', KEYS[6], identity) or '',
        redis.call('HGET', KEYS[7], identity) or '',
        redis.call('HGET', KEYS[8], identity) or '',
        redis.call('HGET', KEYS[9], identity) or '',
        redis.call('HGET', KEYS[10], identity) or '',
        redis.call('HGET', KEYS[11], identity) or '',
        tostring(number_field(KEYS[15], 'used_bytes')),
        tostring(number_field(KEYS[15], 'request_records')),
        tostring(number_field(KEYS[15], 'fences')),
        tostring(number_field(KEYS[15], 'acknowledgement_records'))
    }
end

local prior_operation = redis.call('GET', KEYS[17])
if prior_operation and prior_operation ~= stable_digest then
    return {'conflict'}
end
if not prior_operation then
    redis.call('SET', KEYS[17], stable_digest)
end

local function cancellation_capacity(extra_bytes, extra_requests, extra_fences, extra_acks)
    local used = number_field(KEYS[15], 'used_bytes')
    return extra_bytes <= hard_bytes and used <= hard_bytes - extra_bytes
        and number_field(KEYS[15], 'request_records') + extra_requests <= max_requests
        and number_field(KEYS[15], 'fences') + extra_fences <= max_fences
        and number_field(KEYS[15], 'acknowledgement_records') + extra_acks <= max_acknowledgements
end


if operation == 'commit' then
    local prior = redis.call('HGET', KEYS[2], identity)
    if prior then
        if prior ~= request_digest or redis.call('HGET', KEYS[3], identity) ~= task_key then
            return state('identity-conflict', '')
        end
        return state('replayed', '')
    end
    if not cancellation_capacity(request_units, 1, 1, 0) then
        return state('fenced', '')
    end

    local dispatch_key = expected_dispatch
    local generation = expected_generation
    local owner = expected_owner
    local terminal = requested_outcome

    redis.call('HSET', KEYS[2], identity, request_digest)
    redis.call('HSET', KEYS[3], identity, task_key)
    redis.call('HSET', KEYS[4], identity, dispatch_key)
    redis.call('HSET', KEYS[5], identity, generation)
    redis.call('HSET', KEYS[6], identity, owner)
    redis.call('HSET', KEYS[7], identity, terminal)
    redis.call('HSET', KEYS[8], identity, '0')
    redis.call('HSET', KEYS[12], identity, '0')
    redis.call('HSET', KEYS[13], identity, '0')
    redis.call('HSET', KEYS[14], task_key, identity)
    redis.call('HSET', KEYS[16], identity, owner)
    redis.call('HSET', KEYS[11], identity, tostring(request_units))
    redis.call('HINCRBY', KEYS[15], 'used_bytes', request_units)
    redis.call('HINCRBY', KEYS[15], 'request_records', 1)
    redis.call('HINCRBY', KEYS[15], 'fences', 1)
    return state('committed', '')
end

if redis.call('HGET', KEYS[2], identity) ~= request_digest
    or redis.call('HGET', KEYS[3], identity) ~= task_key then
    return state('identity-conflict', '')
end

local stored_dispatch = redis.call('HGET', KEYS[4], identity) or ''
local stored_generation = redis.call('HGET', KEYS[5], identity) or ''
local stored_owner = redis.call('HGET', KEYS[6], identity) or ''
if stored_dispatch ~= expected_dispatch or stored_generation ~= expected_generation
    or stored_owner ~= expected_owner then
    return state('stale', '')
end

if operation == 'notify' then
    if stored_owner == '' or redis.call('HGET', KEYS[9], identity) then
        return state('stale', '')
    end
    redis.call('HSET', KEYS[8], identity, '1')
    redis.call('HSET', KEYS[16], identity, stored_owner)
    return state('notified', '')
end

if operation == 'settle' then
    local prior_ack = redis.call('HGET', KEYS[10], identity)
    if prior_ack then return state('settled', redis.call('HGET', KEYS[7], identity) or '') end
    if not cancellation_capacity(ack_units, 0, 0, 1) then return state('fenced', '') end

    local elected = requested_outcome

    local reconciliation = 'already-exited'
    local acknowledgement = no_process_ack
    if elected == 'cancellation-killed' then
        reconciliation = 'killed'
        acknowledgement = killed_ack
    elseif elected == 'cancellation-no-process' then
        reconciliation = 'no-process'
    elseif elected == 'cancellation-already-exited' then
        reconciliation = 'already-exited'
    end
    redis.call('HSET', KEYS[7], identity, elected)
    redis.call('HSET', KEYS[9], identity, reconciliation)
    redis.call('HSET', KEYS[10], identity, acknowledgement)
    redis.call('HINCRBY', KEYS[11], identity, ack_units)
    redis.call('HINCRBY', KEYS[15], 'used_bytes', ack_units)
    redis.call('HINCRBY', KEYS[15], 'acknowledgement_records', 1)
    return state('settled', elected)
end

if operation == 'ack-forwarded' then
    if not redis.call('HGET', KEYS[10], identity) then return state('pending', '') end
    redis.call('HSET', KEYS[12], identity, '1')
    return state('ack-forwarded', '')
end

if operation == 'complete-source' then
    if redis.call('HGET', KEYS[12], identity) ~= '1'
        or not redis.call('HGET', KEYS[9], identity)
        or not redis.call('HGET', KEYS[10], identity) then
        return state('pending', '')
    end
    if redis.call('HGET', KEYS[13], identity) == '1' then return state('completed', '') end
    local units = tonumber(redis.call('HGET', KEYS[11], identity) or '0')
    if units <= 0 or number_field(KEYS[15], 'used_bytes') < units then
        return state('accounting', '')
    end
    redis.call('HSET', KEYS[13], identity, '1')
    redis.call('HINCRBY', KEYS[15], 'used_bytes', -units)
    redis.call('HINCRBY', KEYS[15], 'request_records', -1)
    redis.call('HINCRBY', KEYS[15], 'fences', -1)
    return state('completed', '')
end

return redis.error_reply('unknown task-cancellation operation')"#;

#[derive(Debug, Clone)]
pub struct RedisTaskCancellationConfig {
    pub namespace: String,
    pub max_requests: NonZeroUsize,
    pub max_fences: NonZeroUsize,
    pub max_acknowledgements: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisTaskCancellationConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            max_requests: NonZeroUsize::new(DEFAULT_MAX_REQUESTS).expect("non-zero constant"),
            max_fences: NonZeroUsize::new(DEFAULT_MAX_FENCES).expect("non-zero constant"),
            max_acknowledgements: NonZeroUsize::new(DEFAULT_MAX_ACKNOWLEDGEMENTS)
                .expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisTaskCancellationError> {
        let valid_namespace = !self.namespace.is_empty()
            && self.namespace.len() <= 127
            && self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        let minimum = REQUEST_ACCOUNTED_BYTES
            .saturating_add(FENCE_ACCOUNTED_BYTES)
            .saturating_add(ACK_ACCOUNTED_BYTES)
            .saturating_add(512);
        if !valid_namespace
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes < minimum
        {
            return Err(RedisTaskCancellationError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisTaskCancellationBoundary {
    AfterFenceCommit,
    AfterOwnerNotification,
    BeforeTerminalElection,
    AfterTerminalElection,
    AfterAcknowledgementStaging,
}

pub trait RedisTaskCancellationCheckpoint: Send + Sync + 'static {
    fn reached(&self, boundary: RedisTaskCancellationBoundary) -> Result<(), String>;
}

#[derive(Debug, Default)]
struct NoopRedisTaskCancellationCheckpoint;

impl RedisTaskCancellationCheckpoint for NoopRedisTaskCancellationCheckpoint {
    fn reached(&self, _boundary: RedisTaskCancellationBoundary) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone)]
struct RedisTaskCancellationKeys {
    request_digests: String,
    task_keys: String,
    dispatch_keys: String,
    generations: String,
    owners: String,
    terminal_outcomes: String,
    owner_notified: String,
    reconciliations: String,
    acknowledgements: String,
    record_units: String,
    ack_forwarded: String,
    source_completed: String,
    task_fences: String,
    quota: String,
    notifications: String,
    operations_prefix: String,
}

impl RedisTaskCancellationKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:task-cancellation");
        Self {
            request_digests: format!("{prefix}:request-digests"),
            task_keys: format!("{prefix}:task-keys"),
            dispatch_keys: format!("{prefix}:dispatch-keys"),
            generations: format!("{prefix}:generations"),
            owners: format!("{prefix}:owners"),
            terminal_outcomes: format!("{prefix}:terminal-outcomes"),
            owner_notified: format!("{prefix}:owner-notified"),
            reconciliations: format!("{prefix}:reconciliations"),
            acknowledgements: format!("{prefix}:acknowledgements"),
            record_units: format!("{prefix}:record-units"),
            ack_forwarded: format!("{prefix}:ack-forwarded"),
            source_completed: format!("{prefix}:source-completed"),
            task_fences: format!("{prefix}:task-fences"),
            quota: format!("{prefix}:quota"),
            notifications: format!("{prefix}:notifications"),
            operations_prefix: format!("{prefix}:operations:"),
        }
    }

    fn operation(&self, identity: &str, phase: &str) -> String {
        format!(
            "{}{}:{phase}",
            self.operations_prefix,
            digest_hex(identity.as_bytes())
        )
    }
}

pub trait RedisTaskCancellationCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisTaskCancellationError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskCancellationError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisTaskCancellationQuotaState);
}

#[derive(Clone)]
pub struct MonitoredRedisTaskCancellationCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisTaskCancellationCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisTaskCancellationCapability for MonitoredRedisTaskCancellationCapability {
    fn guard_admission(&self) -> Result<u64, RedisTaskCancellationError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisTaskCancellationError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisTaskCancellationError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskCancellationError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisTaskCancellationError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisTaskCancellationQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisTaskCancellationQuotaState {
    pub used_bytes: u64,
    pub request_records: u64,
    pub fences: u64,
    pub acknowledgement_records: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisTaskCancellationQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self.request_records,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisTaskCancellation {
    connection: MultiplexedConnection,
    dispatch: RedisTaskDispatch,
    keys: RedisTaskCancellationKeys,
    config: Arc<RedisTaskCancellationConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisTaskCancellationCapability>,
    checkpoint: Arc<dyn RedisTaskCancellationCheckpoint>,
}

impl RedisTaskCancellation {
    pub async fn connect(
        client: redis::Client,
        dispatch: RedisTaskDispatch,
        config: RedisTaskCancellationConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisTaskCancellationCapability>,
    ) -> Result<Self, RedisTaskCancellationError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisTaskCancellationError::Unavailable)?;
        Self::from_connection(connection, dispatch, config, durability, capability)
    }

    pub(crate) fn from_connection(
        connection: MultiplexedConnection,
        dispatch: RedisTaskDispatch,
        config: RedisTaskCancellationConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisTaskCancellationCapability>,
    ) -> Result<Self, RedisTaskCancellationError> {
        config.validate()?;
        Ok(Self {
            connection,
            dispatch,
            keys: RedisTaskCancellationKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
            checkpoint: Arc::new(NoopRedisTaskCancellationCheckpoint),
        })
    }

    pub fn with_checkpoint(mut self, checkpoint: Arc<dyn RedisTaskCancellationCheckpoint>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    fn reach(&self, boundary: RedisTaskCancellationBoundary) -> Result<(), String> {
        self.checkpoint.reached(boundary)
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_task_cancellation_operation_manifest()
    }

    pub async fn load(
        &self,
        acknowledgement_identity: &str,
    ) -> Result<Option<LocalCancellationFence>, RedisTaskCancellationError> {
        if !valid_identity(acknowledgement_identity) {
            return Err(RedisTaskCancellationError::InvalidOperation);
        }
        let mut connection = self.connection.clone();
        let values: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<u8>,
        ) = redis::pipe()
            .cmd("HGET")
            .arg(&self.keys.request_digests)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.task_keys)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.dispatch_keys)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.generations)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.owners)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.terminal_outcomes)
            .arg(acknowledgement_identity)
            .cmd("HGET")
            .arg(&self.keys.owner_notified)
            .arg(acknowledgement_identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.0.is_none() {
            return Ok(None);
        }
        let task_key = values.1.ok_or_else(|| self.accounting_failure())?;
        let request = parse_task_key(&task_key)?;
        let mut fence = LocalCancellationFence {
            acknowledgement_identity: acknowledgement_identity.to_owned(),
            request,
            dispatch_key: values.2.filter(|value| !value.is_empty()),
            pickup_generation: values.3.and_then(|value| value.parse().ok()),
            owner: values.4.filter(|value| !value.is_empty()),
            owner_notified: values.6 == Some(1),
            liveness_deadline: None,
            terminal_outcome: values.5.as_deref().and_then(parse_outcome),
        };
        let binding = self
            .dispatch
            .load_cancellation_binding(acknowledgement_identity)
            .await
            .map_err(|_| RedisTaskCancellationError::Unavailable)?
            .ok_or_else(|| self.accounting_failure())?;
        if !binding_matches_fence(&binding, &fence) {
            return Err(RedisTaskCancellationError::StaleGeneration);
        }
        fence.liveness_deadline = binding.liveness_deadline;
        fence.terminal_outcome = binding.terminal_outcome.or(fence.terminal_outcome);
        Ok(Some(fence))
    }

    pub async fn select_owner_notification(
        &self,
        owner: &str,
    ) -> Result<Option<LocalCancellationFence>, RedisTaskCancellationError> {
        if !valid_identity(owner) {
            return Err(RedisTaskCancellationError::InvalidOperation);
        }
        let mut connection = self.connection.clone();
        let (_, entries): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
            .arg(&self.keys.notifications)
            .arg(0)
            .arg("COUNT")
            .arg(SCAN_LIMIT)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        for (identity, recorded_owner) in entries {
            if recorded_owner == owner {
                return self.load(&identity).await;
            }
        }
        Ok(None)
    }

    pub async fn acknowledgement(
        &self,
        acknowledgement_identity: &str,
    ) -> Result<Option<Vec<u8>>, RedisTaskCancellationError> {
        let mut connection = self.connection.clone();
        redis::cmd("HGET")
            .arg(&self.keys.acknowledgements)
            .arg(acknowledgement_identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))
    }

    async fn select_pending_acknowledgement(
        &self,
    ) -> Result<Option<(LocalCancellationFence, Vec<u8>)>, RedisTaskCancellationError> {
        let mut connection = self.connection.clone();
        let (_, entries): (u64, Vec<(String, Vec<u8>)>) = redis::cmd("HSCAN")
            .arg(&self.keys.acknowledgements)
            .arg(0)
            .arg("COUNT")
            .arg(SCAN_LIMIT)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        for (identity, acknowledgement) in entries {
            let forwarded: Option<u8> = redis::cmd("HGET")
                .arg(&self.keys.ack_forwarded)
                .arg(&identity)
                .query_async(&mut connection)
                .await
                .map_err(|error| self.redis_error(error))?;
            if forwarded != Some(1) {
                let fence = self
                    .load(&identity)
                    .await?
                    .ok_or_else(|| self.accounting_failure())?;
                return Ok(Some((fence, acknowledgement)));
            }
        }
        Ok(None)
    }

    pub async fn mark_acknowledgement_forwarded(
        &self,
        fence: &LocalCancellationFence,
    ) -> Result<bool, RedisTaskCancellationError> {
        self.simple_transition(fence, "ack-forwarded").await
    }

    pub async fn complete_source(
        &self,
        fence: &LocalCancellationFence,
    ) -> Result<bool, RedisTaskCancellationError> {
        self.simple_transition(fence, "complete-source").await
    }

    pub async fn source_completed(
        &self,
        acknowledgement_identity: &str,
    ) -> Result<bool, RedisTaskCancellationError> {
        let mut connection = self.connection.clone();
        let completed: Option<u8> = redis::cmd("HGET")
            .arg(&self.keys.source_completed)
            .arg(acknowledgement_identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        Ok(completed == Some(1))
    }

    pub async fn quota_state(
        &self,
    ) -> Result<RedisTaskCancellationQuotaState, RedisTaskCancellationError> {
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "request_records",
                "fences",
                "acknowledgement_records",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.len() != 4 {
            return Err(self.accounting_failure());
        }
        Ok(self.quota_state_from(
            values[0].unwrap_or(0),
            values[1].unwrap_or(0),
            values[2].unwrap_or(0),
            values[3].unwrap_or(0),
        ))
    }

    async fn simple_transition(
        &self,
        fence: &LocalCancellationFence,
        operation: &'static str,
    ) -> Result<bool, RedisTaskCancellationError> {
        let generation = self.capability.guard_admission()?;
        let mutation = CancellationMutation::new(
            &self.keys,
            fence,
            operation,
            reconciliation_outcome(CancellationReconciliation::NoProcess),
            Vec::new(),
            Vec::new(),
            0,
            0,
            &self.config,
        )?;
        let state = self.decode_state(&self.commit_mutation(&mutation).await?)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "ack-forwarded" | "completed" => Ok(true),
            "pending" | "stale" => Ok(false),
            "identity-conflict" => Err(RedisTaskCancellationError::IdentityConflict),
            "accounting" => Err(self.accounting_failure()),
            _ => Err(self.accounting_failure()),
        }
    }

    async fn commit_mutation(
        &self,
        mutation: &CancellationMutation,
    ) -> Result<Vec<Vec<u8>>, RedisTaskCancellationError> {
        let mut connection = self.connection.clone();
        match self.durability.execute(&mut connection, mutation).await {
            Ok(committed) => Ok(committed.into_output()),
            Err(error)
                if matches!(
                    error.failure(),
                    RedisDurabilityFailure::AmbiguousMutation
                        | RedisDurabilityFailure::AmbiguousLocalFsync
                ) =>
            {
                self.durability
                    .resolve_ambiguous(&mut connection, mutation)
                    .await
                    .map(|committed| committed.into_output())
                    .map_err(|error| self.durability_error(error))
            }
            Err(error) => Err(self.durability_error(error)),
        }
    }

    fn decode_state(
        &self,
        output: &[Vec<u8>],
    ) -> Result<CancellationState, RedisTaskCancellationError> {
        if output.len() == 1 && output[0].as_slice() == b"conflict" {
            return Err(RedisTaskCancellationError::IdentityConflict);
        }
        if output.len() != 14 {
            return Err(self.accounting_failure());
        }
        let text = |index: usize| {
            std::str::from_utf8(&output[index])
                .map(str::to_owned)
                .map_err(|_| self.accounting_failure())
        };
        let number = |index: usize| {
            std::str::from_utf8(&output[index])
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| self.accounting_failure())
        };
        Ok(CancellationState {
            status: text(0)?,
            quota: self.quota_state_from(number(10)?, number(11)?, number(12)?, number(13)?),
        })
    }

    fn quota_state_from(
        &self,
        used_bytes: u64,
        request_records: u64,
        fences: u64,
        acknowledgement_records: u64,
    ) -> RedisTaskCancellationQuotaState {
        let pressure = if used_bytes >= self.config.hard_limit_bytes
            || request_records >= self.config.max_requests.get() as u64
            || fences >= self.config.max_fences.get() as u64
            || acknowledgement_records >= self.config.max_acknowledgements.get() as u64
        {
            RedisQuotaPressure::HardLimit
        } else if used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisTaskCancellationQuotaState {
            used_bytes,
            request_records,
            fences,
            acknowledgement_records,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            pressure,
        }
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisTaskCancellationError {
        use crate::redis_durability::{RedisMutationError, RedisMutationFailure};
        match RedisMutationError::from_redis(error).failure() {
            RedisMutationFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisMutationFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisTaskCancellationError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisTaskCancellationError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisTaskCancellationError::IdentityConflict
            }
            RedisDurabilityFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisDurabilityFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisDurabilityFailure::AmbiguousLocalFsync
            | RedisDurabilityFailure::LocalFsyncUnavailable => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::LocalFsync),
            RedisDurabilityFailure::InvalidOperation
            | RedisDurabilityFailure::AmbiguousMutation
            | RedisDurabilityFailure::MutationRejected => {}
        }
        RedisTaskCancellationError::Durability(error.failure())
    }

    fn accounting_failure(&self) -> RedisTaskCancellationError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisTaskCancellationError::Accounting
    }
}

impl SafeCancellationFence for RedisTaskCancellation {
    async fn commit_cancellation_fence(
        &self,
        acknowledgement_identity: &str,
        request: CancelRequest,
        _now: DateTime<Utc>,
    ) -> Result<LocalCancellationFence, String> {
        if !valid_identity(acknowledgement_identity) {
            return Err(RedisTaskCancellationError::InvalidOperation.to_string());
        }
        let generation = self
            .capability
            .guard_admission()
            .map_err(|error| error.to_string())?;
        let binding = self
            .dispatch
            .bind_cancellation(acknowledgement_identity, request)
            .await
            .map_err(|error| error.to_string())?;
        let task_key = request_task_key(request);
        let request_units = REQUEST_ACCOUNTED_BYTES
            .saturating_add(FENCE_ACCOUNTED_BYTES)
            .saturating_add(task_key.len() as u64)
            .saturating_add(acknowledgement_identity.len() as u64);
        let fence = cancellation_fence_from_binding(request, binding);
        let mutation = CancellationMutation::new(
            &self.keys,
            &fence,
            "commit",
            fence.terminal_outcome.map(outcome_name).unwrap_or(""),
            Vec::new(),
            Vec::new(),
            request_units,
            0,
            &self.config,
        )
        .map_err(|error| error.to_string())?;
        let output = self
            .commit_mutation(&mutation)
            .await
            .map_err(|error| error.to_string())?;
        let state = self
            .decode_state(&output)
            .map_err(|error| error.to_string())?;
        self.capability.report_quota(state.quota);
        self.capability
            .guard_acknowledgement(generation)
            .map_err(|error| error.to_string())?;
        let fence = match state.status.as_str() {
            "committed" | "replayed" => self
                .load(acknowledgement_identity)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "committed Redis cancellation fence is missing".to_owned())?,
            "fenced" => return Err(RedisTaskCancellationError::CapacityFenced.to_string()),
            "identity-conflict" => {
                return Err(RedisTaskCancellationError::IdentityConflict.to_string())
            }
            _ => return Err(self.accounting_failure().to_string()),
        };
        self.reach(RedisTaskCancellationBoundary::AfterFenceCommit)?;
        Ok(fence)
    }

    async fn mark_cancellation_owner_notified(
        &self,
        fence: &LocalCancellationFence,
        _now: DateTime<Utc>,
    ) -> Result<bool, String> {
        let generation = self
            .capability
            .guard_admission()
            .map_err(|error| error.to_string())?;
        let mutation = CancellationMutation::new(
            &self.keys,
            fence,
            "notify",
            reconciliation_outcome(CancellationReconciliation::NoProcess),
            Vec::new(),
            Vec::new(),
            0,
            0,
            &self.config,
        )
        .map_err(|error| error.to_string())?;
        let output = self
            .commit_mutation(&mutation)
            .await
            .map_err(|error| error.to_string())?;
        let state = self
            .decode_state(&output)
            .map_err(|error| error.to_string())?;
        self.capability.report_quota(state.quota);
        self.capability
            .guard_acknowledgement(generation)
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            "notified" => {
                self.reach(RedisTaskCancellationBoundary::AfterOwnerNotification)?;
                Ok(true)
            }
            "stale" => Ok(false),
            "identity-conflict" => Err(RedisTaskCancellationError::IdentityConflict.to_string()),
            _ => Err(self.accounting_failure().to_string()),
        }
    }

    async fn settle_cancellation(
        &self,
        fence: &LocalCancellationFence,
        reconciliation: CancellationReconciliation,
        _acknowledgement: &[u8],
        _now: DateTime<Utc>,
    ) -> Result<Option<TerminalElection>, String> {
        let generation = self
            .capability
            .guard_admission()
            .map_err(|error| error.to_string())?;
        let binding = self
            .dispatch
            .load_cancellation_binding(&fence.acknowledgement_identity)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "Redis TaskDispatch cancellation binding is missing".to_owned())?;
        if !binding_matches_fence(&binding, fence) {
            return Err(RedisTaskCancellationError::StaleGeneration.to_string());
        }
        self.reach(RedisTaskCancellationBoundary::BeforeTerminalElection)?;
        let election = self
            .dispatch
            .elect_cancellation(&binding, reconciliation)
            .await
            .map_err(|error| error.to_string())?;
        self.reach(RedisTaskCancellationBoundary::AfterTerminalElection)?;
        let elected_outcome = match election {
            Some(TerminalElection::Settled(outcome)) => outcome,
            Some(TerminalElection::Won) => cancellation_outcome(reconciliation),
            None => fence
                .terminal_outcome
                .unwrap_or_else(|| cancellation_outcome(reconciliation)),
        };
        let killed_ack = encode_cancel_ack(
            fence.request.task_instance_id,
            fence.request.workflow_instance_id,
            KillOutcome::Killed,
        );
        let no_process_ack = encode_cancel_ack(
            fence.request.task_instance_id,
            fence.request.workflow_instance_id,
            KillOutcome::NoSuchTask,
        );
        let ack_units =
            ACK_ACCOUNTED_BYTES.saturating_add(killed_ack.len().max(no_process_ack.len()) as u64);
        let mutation = CancellationMutation::new(
            &self.keys,
            fence,
            "settle",
            outcome_name(elected_outcome),
            killed_ack,
            no_process_ack,
            0,
            ack_units,
            &self.config,
        )
        .map_err(|error| error.to_string())?;
        let output = self
            .commit_mutation(&mutation)
            .await
            .map_err(|error| error.to_string())?;
        let state = self
            .decode_state(&output)
            .map_err(|error| error.to_string())?;
        self.capability.report_quota(state.quota);
        self.capability
            .guard_acknowledgement(generation)
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            "settled" => {
                self.reach(RedisTaskCancellationBoundary::AfterAcknowledgementStaging)?;
                Ok(election)
            }
            "fenced" => Err(RedisTaskCancellationError::CapacityFenced.to_string()),
            "stale" => Err(RedisTaskCancellationError::StaleGeneration.to_string()),
            "identity-conflict" => Err(RedisTaskCancellationError::IdentityConflict.to_string()),
            "accounting" => Err(self.accounting_failure().to_string()),
            _ => Err(self.accounting_failure().to_string()),
        }
    }

    async fn select_unresolved_cancellation(
        &self,
    ) -> Result<Option<LocalCancellationFence>, String> {
        let mut connection = self.connection.clone();
        let (_, entries): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
            .arg(&self.keys.request_digests)
            .arg(0)
            .arg("COUNT")
            .arg(SCAN_LIMIT)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error).to_string())?;
        for (identity, _) in entries {
            let acknowledgement: Option<Vec<u8>> = redis::cmd("HGET")
                .arg(&self.keys.acknowledgements)
                .arg(&identity)
                .query_async(&mut connection)
                .await
                .map_err(|error| self.redis_error(error).to_string())?;
            if acknowledgement.is_none() {
                return self
                    .load(&identity)
                    .await
                    .map_err(|error| error.to_string());
            }
        }
        Ok(None)
    }
}

impl SafeCancellationRole for RedisTaskCancellation {
    async fn select_owner_cancellation(
        &self,
        owner: &str,
    ) -> Result<Option<LocalCancellationFence>, String> {
        self.select_owner_notification(owner)
            .await
            .map_err(|error| error.to_string())
    }
}

impl TaskCancellationPublisher for RedisTaskCancellation {
    fn prepare(&self) -> TaskCancellationFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn stage<'a>(
        &'a self,
        encoded_cancellation: &'a [u8],
    ) -> TaskCancellationFuture<'a, Result<(), String>> {
        Box::pin(async move {
            SafeCancellationCoordinator::new(self.clone())
                .stage(encoded_cancellation)
                .await
                .map(|_| ())
        })
    }
}

struct RedisCancellationAckDelivery {
    adapter: RedisTaskCancellation,
    fence: LocalCancellationFence,
    acknowledgement: Vec<u8>,
}

impl TaskCancellationAckDelivery for RedisCancellationAckDelivery {
    fn payload(&self) -> &[u8] {
        &self.acknowledgement
    }

    fn complete(self: Box<Self>) -> TaskCancellationFuture<'static, Result<(), String>> {
        Box::pin(async move {
            if !self
                .adapter
                .mark_acknowledgement_forwarded(&self.fence)
                .await
                .map_err(|error| error.to_string())?
            {
                return Err("cancellation acknowledgement forwarding fence is stale".to_owned());
            }
            if !self
                .adapter
                .complete_source(&self.fence)
                .await
                .map_err(|error| error.to_string())?
            {
                return Err("cancellation source completion fence is stale".to_owned());
            }
            Ok(())
        })
    }

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> TaskCancellationFuture<'static, Result<(), String>> {
        Box::pin(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            Ok(())
        })
    }
}

impl TaskCancellationAckConsumer for RedisTaskCancellation {
    fn next(
        &self,
    ) -> TaskCancellationFuture<'_, Result<Option<Box<dyn TaskCancellationAckDelivery>>, String>>
    {
        Box::pin(async move {
            self.select_pending_acknowledgement()
                .await
                .map_err(|error| error.to_string())
                .map(|delivery| {
                    delivery.map(|(fence, acknowledgement)| {
                        Box::new(RedisCancellationAckDelivery {
                            adapter: self.clone(),
                            fence,
                            acknowledgement,
                        }) as Box<dyn TaskCancellationAckDelivery>
                    })
                })
        })
    }
}

pub(crate) struct RedisTaskCancellationRoleRegistration {
    connection: MultiplexedConnection,
    dispatch: RedisTaskDispatch,
    keys: RedisTaskCancellationKeys,
    config: RedisTaskCancellationConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisTaskCancellationRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        dispatch: RedisTaskDispatch,
        config: RedisTaskCancellationConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisTaskCancellationError> {
        config.validate()?;
        Ok(Self {
            connection,
            dispatch,
            keys: RedisTaskCancellationKeys::new(&config.namespace),
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) fn build_adapter(
        &self,
        capability: Arc<dyn RedisTaskCancellationCapability>,
    ) -> Result<RedisTaskCancellation, RedisTaskCancellationError> {
        RedisTaskCancellation::from_connection(
            self.connection.clone(),
            self.dispatch.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::TaskCancellation
            && context.manifest_identity() == &self.manifest_identity
            && RedisTaskCancellation::operation_manifest()
                .map(|manifest| manifest.identity() == context.manifest_identity())
                .unwrap_or(false)
    }

    async fn representative_denial(&self, command: &mut redis::Cmd) -> bool {
        let mut connection = self.connection.clone();
        match command.query_async::<redis::Value>(&mut connection).await {
            Err(error) => error.code() == Some("NOPERM"),
            Ok(_) => false,
        }
    }
}

#[async_trait]
impl RedisRoleCapabilityProbe for RedisTaskCancellationRoleRegistration {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::Accounting);
        }
        redis::cmd("EVAL")
            .arg("return redis.call('HGET', KEYS[1], ARGV[1])")
            .arg(1)
            .arg(&self.keys.quota)
            .arg("runtime-capability-canary")
            .query_async::<redis::Value>(&mut self.connection.clone())
            .await
            .map(|_| ())
            .map_err(|_| RedisRoleCapabilityFailure::RequiredOperation)
    }

    async fn probe_representative_denials(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::RepresentativeDenial);
        }
        let cross_role_key = format!(
            "tickr:{{{}}}:task-dispatch:runtime-capability-canary",
            self.config.namespace
        );
        let cross_role_denied = self
            .representative_denial(redis::cmd("GET").arg(cross_role_key))
            .await;
        let admin_denied = self
            .representative_denial(redis::cmd("ACL").arg("LIST"))
            .await;
        (cross_role_denied && admin_denied)
            .then_some(())
            .ok_or(RedisRoleCapabilityFailure::RepresentativeDenial)
    }
}

#[async_trait]
impl RedisReconstructionCallback for RedisTaskCancellationRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mut connection = self.connection.clone();
        let (_, requests): (u64, Vec<(String, String)>) = redis::cmd("HSCAN")
            .arg(&self.keys.request_digests)
            .arg(0)
            .arg("COUNT")
            .arg(self.config.max_requests.get())
            .query_async(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        if requests.len() > self.config.max_requests.get() {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "request_records",
                "fences",
                "acknowledgement_records",
            ])
            .query_async::<Vec<Option<u64>>>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub fn redis_task_cancellation_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(
        TASK_CANCELLATION_SCRIPT_NAME,
        TASK_CANCELLATION_SCRIPT_SHA256,
    )?;
    let fence_pattern =
        RedisNamespacePattern::key("tickr:{namespace}:task-cancellation:request-digests");
    RedisOperationManifest::new(
        CoordinationRole::TaskCancellation,
        REDIS_TASK_CANCELLATION_PROTOCOL,
        REDIS_TASK_CANCELLATION_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:task-cancellation:request-digests",
            "tickr:{namespace}:task-cancellation:task-keys",
            "tickr:{namespace}:task-cancellation:dispatch-keys",
            "tickr:{namespace}:task-cancellation:generations",
            "tickr:{namespace}:task-cancellation:owners",
            "tickr:{namespace}:task-cancellation:terminal-outcomes",
            "tickr:{namespace}:task-cancellation:owner-notified",
            "tickr:{namespace}:task-cancellation:reconciliations",
            "tickr:{namespace}:task-cancellation:acknowledgements",
            "tickr:{namespace}:task-cancellation:record-units",
            "tickr:{namespace}:task-cancellation:ack-forwarded",
            "tickr:{namespace}:task-cancellation:source-completed",
            "tickr:{namespace}:task-cancellation:task-fences",
            "tickr:{namespace}:task-cancellation:quota",
            "tickr:{namespace}:task-cancellation:notifications",
            "tickr:{namespace}:task-cancellation:operations:*",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            fence_pattern,
        )],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::TaskEvents,
            ),
            RedisForbiddenOperation::administrative("FLUSHALL"),
        ],
    )
}

struct CancellationState {
    status: String,
    quota: RedisTaskCancellationQuotaState,
}

#[derive(Clone)]
struct CancellationMutation {
    operation: RedisStableOperation,
    stable_digest: String,
    operation_key: String,
    keys: RedisTaskCancellationKeys,
    identity: String,
    request_digest: String,
    task_key: String,
    dispatch_key: String,
    generation: String,
    owner: String,
    requested_outcome: &'static str,
    killed_ack: Vec<u8>,
    no_process_ack: Vec<u8>,
    request_units: u64,
    ack_units: u64,
    operation_name: &'static str,
    config: MutationConfig,
}

#[derive(Clone, Copy)]
struct MutationConfig {
    max_requests: u64,
    max_fences: u64,
    max_acknowledgements: u64,
    hard_limit_bytes: u64,
}

impl CancellationMutation {
    #[allow(clippy::too_many_arguments)]
    fn new(
        keys: &RedisTaskCancellationKeys,
        fence: &LocalCancellationFence,
        operation_name: &'static str,
        requested_outcome: &'static str,
        killed_ack: Vec<u8>,
        no_process_ack: Vec<u8>,
        request_units: u64,
        ack_units: u64,
        config: &RedisTaskCancellationConfig,
    ) -> Result<Self, RedisTaskCancellationError> {
        let identity = fence.acknowledgement_identity.clone();
        let task_key = request_task_key(fence.request);
        let request_digest = digest_hex(format!("{identity}:{task_key}").as_bytes());
        let stable_payload = match operation_name {
            "commit" => request_digest.as_bytes().to_vec(),
            _ => [request_digest.as_bytes(), operation_name.as_bytes()].concat(),
        };
        let operation_key = keys.operation(&identity, operation_name);
        let stable_digest = digest_hex(&stable_payload);
        let operation = RedisStableOperation::new(operation_key.clone(), &stable_payload)
            .map_err(|_| RedisTaskCancellationError::InvalidOperation)?;
        Ok(Self {
            operation,
            stable_digest,
            operation_key,
            keys: keys.clone(),
            identity,
            request_digest,
            task_key,
            dispatch_key: fence.dispatch_key.clone().unwrap_or_default(),
            generation: fence
                .pickup_generation
                .map(|value| value.to_string())
                .unwrap_or_default(),
            owner: fence.owner.clone().unwrap_or_default(),
            requested_outcome,
            killed_ack,
            no_process_ack,
            request_units,
            ack_units,
            operation_name,
            config: MutationConfig {
                max_requests: config.max_requests.get() as u64,
                max_fences: config.max_fences.get() as u64,
                max_acknowledgements: config.max_acknowledgements.get() as u64,
                hard_limit_bytes: config.hard_limit_bytes,
            },
        })
    }
}

#[async_trait]
impl RedisStableMutation for CancellationMutation {
    type Output = Vec<Vec<u8>>;

    fn operation(&self) -> &RedisStableOperation {
        &self.operation
    }

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError> {
        let output: Vec<Vec<u8>> = redis::cmd("EVAL")
            .arg(TASK_CANCELLATION_SCRIPT)
            .arg(17)
            .arg(&self.keys.request_digests)
            .arg(&self.keys.request_digests)
            .arg(&self.keys.task_keys)
            .arg(&self.keys.dispatch_keys)
            .arg(&self.keys.generations)
            .arg(&self.keys.owners)
            .arg(&self.keys.terminal_outcomes)
            .arg(&self.keys.owner_notified)
            .arg(&self.keys.reconciliations)
            .arg(&self.keys.acknowledgements)
            .arg(&self.keys.record_units)
            .arg(&self.keys.ack_forwarded)
            .arg(&self.keys.source_completed)
            .arg(&self.keys.task_fences)
            .arg(&self.keys.quota)
            .arg(&self.keys.notifications)
            .arg(&self.operation_key)
            .arg(self.operation_name)
            .arg(&self.stable_digest)
            .arg(&self.identity)
            .arg(&self.request_digest)
            .arg(&self.task_key)
            .arg(&self.dispatch_key)
            .arg(&self.generation)
            .arg(&self.owner)
            .arg(self.requested_outcome)
            .arg(&self.killed_ack)
            .arg(&self.no_process_ack)
            .arg(self.request_units)
            .arg(self.ack_units)
            .arg(self.config.max_requests)
            .arg(self.config.max_fences)
            .arg(self.config.max_acknowledgements)
            .arg(self.config.hard_limit_bytes)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        match output.first().map(Vec::as_slice) {
            Some(b"conflict") => Ok(RedisStableMutationOutcome::IdentityConflict),
            Some(b"replayed" | b"settled" | b"completed" | b"ack-forwarded") => {
                Ok(RedisStableMutationOutcome::Replayed(output))
            }
            Some(
                b"committed" | b"notified" | b"fenced" | b"pending" | b"stale"
                | b"identity-conflict" | b"accounting",
            ) => Ok(RedisStableMutationOutcome::Applied(output)),
            _ => Err(RedisMutationError::rejected()),
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let actual: Option<String> = redis::cmd("GET")
            .arg(&self.operation_key)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        Ok(match actual {
            Some(actual) if actual == self.stable_digest => RedisStableMutationRecovery::Matching,
            Some(_) => RedisStableMutationRecovery::IdentityConflict,
            None => RedisStableMutationRecovery::Missing,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisTaskCancellationError {
    InvalidConfiguration,
    InvalidOperation,
    Unavailable,
    IdentityConflict,
    StaleGeneration,
    CapacityFenced,
    Accounting,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisTaskCancellationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Redis TaskCancellation configuration is invalid",
            Self::InvalidOperation => "Redis TaskCancellation operation is invalid",
            Self::Unavailable => "Redis TaskCancellation role is unavailable",
            Self::IdentityConflict => {
                "Redis TaskCancellation identity conflicts with durable state"
            }
            Self::StaleGeneration => "Redis TaskCancellation fence is stale",
            Self::CapacityFenced => "Redis TaskCancellation capacity is fenced",
            Self::Accounting => "Redis TaskCancellation accounting is inconsistent",
            Self::Durability(_) => "Redis TaskCancellation durability was not proved",
        })
    }
}

impl std::error::Error for RedisTaskCancellationError {}

fn request_task_key(request: CancelRequest) -> String {
    format!(
        "{}:{}",
        request.workflow_instance_id, request.task_instance_id
    )
}

fn parse_task_key(task_key: &str) -> Result<CancelRequest, RedisTaskCancellationError> {
    let (workflow, task) = task_key
        .split_once(':')
        .ok_or(RedisTaskCancellationError::Accounting)?;
    Ok(CancelRequest {
        workflow_instance_id: workflow
            .parse()
            .map_err(|_| RedisTaskCancellationError::Accounting)?,
        task_instance_id: task
            .parse()
            .map_err(|_| RedisTaskCancellationError::Accounting)?,
    })
}

fn reconciliation_outcome(reconciliation: CancellationReconciliation) -> &'static str {
    match reconciliation {
        CancellationReconciliation::Killed => "cancellation-killed",
        CancellationReconciliation::AlreadyExited => "cancellation-already-exited",
        CancellationReconciliation::NoProcess => "cancellation-no-process",
    }
}

fn cancellation_outcome(reconciliation: CancellationReconciliation) -> LocalAttemptOutcome {
    match reconciliation {
        CancellationReconciliation::Killed => LocalAttemptOutcome::CancellationKilled,
        CancellationReconciliation::AlreadyExited => LocalAttemptOutcome::CancellationAlreadyExited,
        CancellationReconciliation::NoProcess => LocalAttemptOutcome::CancellationNoProcess,
    }
}

fn outcome_name(outcome: LocalAttemptOutcome) -> &'static str {
    match outcome {
        LocalAttemptOutcome::ProcessExitedSuccess => "process-exited-success",
        LocalAttemptOutcome::ProcessExitedFailure => "process-exited-failure",
        LocalAttemptOutcome::ProcessSetupFailed => "process-setup-failed",
        LocalAttemptOutcome::LivenessExpired => "liveness-expired",
        LocalAttemptOutcome::CancellationKilled => "cancellation-killed",
        LocalAttemptOutcome::CancellationAlreadyExited => "cancellation-already-exited",
        LocalAttemptOutcome::CancellationNoProcess => "cancellation-no-process",
    }
}

fn cancellation_fence_from_binding(
    request: CancelRequest,
    binding: RedisCancellationBinding,
) -> LocalCancellationFence {
    LocalCancellationFence {
        acknowledgement_identity: binding.acknowledgement_identity,
        request,
        dispatch_key: binding.dispatch_key,
        pickup_generation: binding.pickup_generation,
        owner: binding.owner,
        owner_notified: false,
        liveness_deadline: binding.liveness_deadline,
        terminal_outcome: binding.terminal_outcome,
    }
}

fn binding_matches_fence(
    binding: &RedisCancellationBinding,
    fence: &LocalCancellationFence,
) -> bool {
    binding.acknowledgement_identity == fence.acknowledgement_identity
        && binding.task_key == request_task_key(fence.request)
        && binding.dispatch_key == fence.dispatch_key
        && binding.pickup_generation == fence.pickup_generation
        && binding.owner == fence.owner
}

fn parse_outcome(value: &str) -> Option<LocalAttemptOutcome> {
    Some(match value {
        "process-exited-success" => LocalAttemptOutcome::ProcessExitedSuccess,
        "process-exited-failure" => LocalAttemptOutcome::ProcessExitedFailure,
        "process-setup-failed" => LocalAttemptOutcome::ProcessSetupFailed,
        "liveness-expired" => LocalAttemptOutcome::LivenessExpired,
        "cancellation-killed" => LocalAttemptOutcome::CancellationKilled,
        "cancellation-already-exited" => LocalAttemptOutcome::CancellationAlreadyExited,
        "cancellation-no-process" => LocalAttemptOutcome::CancellationNoProcess,
        _ => return None,
    })
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_exact_and_rejects_unregistered_operations() {
        let manifest = redis_task_cancellation_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::TaskCancellation);
        assert_eq!(manifest.protocol(), REDIS_TASK_CANCELLATION_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_TASK_CANCELLATION_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), TASK_CANCELLATION_SCRIPT_NAME);
        assert_eq!(
            digest_hex(TASK_CANCELLATION_SCRIPT.as_bytes()),
            TASK_CANCELLATION_SCRIPT_SHA256
        );
        assert!(!manifest.commands().contains(&"PUBLISH"));
        assert!(!manifest.commands().contains(&"DEL"));
        assert!(manifest
            .key_patterns()
            .contains(&"tickr:{namespace}:task-cancellation:task-fences"));
        assert!(!manifest
            .key_patterns()
            .iter()
            .any(|pattern| pattern.contains(":task-dispatch:")));
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);

        let error = RedisOperationManifest::new(
            manifest.role(),
            manifest.protocol(),
            manifest.commands().to_vec(),
            manifest.scripts().to_vec(),
            manifest.key_patterns().to_vec(),
            manifest.channel_patterns().to_vec(),
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("PUBLISH"),
                RedisNamespacePattern::key("tickr:{namespace}:task-cancellation:request-digests"),
            )],
            manifest.forbidden_operations().to_vec(),
        )
        .unwrap_err();
        assert_eq!(
            error.failure(),
            crate::redis_operation_manifest::RedisOperationManifestFailure::UnregisteredOperation
        );
    }
}
