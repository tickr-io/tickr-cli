#![allow(async_fn_in_trait)]

use std::{fmt, num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::MultiplexedConnection;
use sha2::{Digest, Sha256};
use tickr_executor::local_pickup::{
    DueLocalPickup, ElectedAttemptOutcome, LocalAttemptOutcome, LocalPickupClaim,
    SafeLivenessWatchdog, TerminalElection,
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
};

pub const REDIS_LIVENESS_WATCHDOG_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.liveness-watchdog.redis-deadline-election", 1);

const DEFAULT_MAX_RECORDS: usize = 256;
const DEFAULT_MAX_STAGED_OUTCOMES: usize = 16_384;
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_TERMINAL_EVENT_BYTES: usize = 1024 * 1024;
const DEFAULT_SOFT_BYTES: u64 = 48 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 56 * 1024 * 1024;
const RECORD_ACCOUNTED_BYTES: u64 = 384;

const REDIS_LIVENESS_WATCHDOG_COMMANDS: &[&str] = &[
    "EVAL",
    "GET",
    "HDEL",
    "HGET",
    "HINCRBY",
    "HSET",
    "SET",
    "TIME",
    "WAITAOF",
    "ZADD",
    "ZRANGEBYSCORE",
    "ZREM",
];
const LIVENESS_WATCHDOG_SCRIPT_NAME: &str = "liveness-watchdog-v1";
const LIVENESS_WATCHDOG_SCRIPT_SHA256: &str =
    "649000edb74bc9e7ceef7cd5107d7fd9d9e38797ce8006998dac5eecc9cfe1f1";

const LIVENESS_WATCHDOG_SCRIPT: &str = r#"local operation = ARGV[1]
local stable_digest = ARGV[2]
local dispatch_key = ARGV[3]
local generation = tonumber(ARGV[4])
local owner = ARGV[5]
local timeout_ms = tonumber(ARGV[6])
local payload = ARGV[7]
local outcome = ARGV[8]
local event = ARGV[9]
local reserved_units = tonumber(ARGV[10])
local max_records = tonumber(ARGV[11])
local max_outcomes = tonumber(ARGV[12])
local hard_bytes = tonumber(ARGV[13])

local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function server_millis()
    local now = redis.call('TIME')
    return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end

local function exact_claim()
    return tonumber(redis.call('HGET', KEYS[1], dispatch_key) or '-1') == generation
        and (redis.call('HGET', KEYS[2], dispatch_key) or '') == owner
end

local function state(status, detail, selected_event)
    return {
        status,
        detail or '',
        selected_event or '',
        tostring(number_field(KEYS[10], 'used_bytes')),
        tostring(number_field(KEYS[10], 'liveness_records')),
        tostring(number_field(KEYS[10], 'deadline_entries')),
        tostring(number_field(KEYS[10], 'durable_outcomes')),
        tostring(number_field(KEYS[10], 'staged_events')),
        redis.call('HGET', KEYS[2], dispatch_key) or '',
        redis.call('HGET', KEYS[3], dispatch_key) or '0',
        redis.call('HGET', KEYS[6], dispatch_key) or '0',
        redis.call('HGET', KEYS[4], dispatch_key) or '',
        dispatch_key
    }
end

if operation == 'select_due' then
    local now = server_millis()
    local due = redis.call('ZRANGEBYSCORE', KEYS[9], '-inf', now, 'LIMIT', 0, 1)
    if #due == 0 then
        return state('none', '', '')
    end
    dispatch_key = due[1]
    local deadline = tonumber(redis.call('HGET', KEYS[3], dispatch_key) or '0')
    if deadline <= 0 or deadline > now or redis.call('HGET', KEYS[7], dispatch_key) then
        return state('none', '', '')
    end
    return state('due', redis.call('HGET', KEYS[1], dispatch_key) or '', '')
end

if operation == 'prove' then
    if exact_claim() and not redis.call('HGET', KEYS[7], dispatch_key) then
        return state('proved', '', '')
    end
    return state('stale', '', '')
end

if operation == 'quota' then
    return state('quota', '', '')
end

local prior = redis.call('GET', KEYS[11])
if prior then
    if prior ~= stable_digest then
        return {'conflict'}
    end
    if operation == 'elect' then
        return state(
            'settled',
            redis.call('HGET', KEYS[7], dispatch_key) or '',
            redis.call('HGET', KEYS[8], dispatch_key) or ''
        )
    end
    return state('replayed', redis.call('HGET', KEYS[7], dispatch_key) or '', redis.call('HGET', KEYS[8], dispatch_key) or '')
end

if operation == 'arm' then
    local current = redis.call('HGET', KEYS[1], dispatch_key)
    if current then
        if exact_claim() and not redis.call('HGET', KEYS[7], dispatch_key) then
            return state('replayed', '', '')
        end
        return state('stale', '', '')
    end
    local records = number_field(KEYS[10], 'liveness_records')
    local used = number_field(KEYS[10], 'used_bytes')
    if records >= max_records or reserved_units > hard_bytes or used > hard_bytes - reserved_units then
        return state('fenced', '', '')
    end
    local deadline = server_millis() + timeout_ms
    redis.call('HSET', KEYS[1], dispatch_key, tostring(generation))
    redis.call('HSET', KEYS[2], dispatch_key, owner)
    redis.call('HSET', KEYS[3], dispatch_key, tostring(deadline))
    redis.call('HSET', KEYS[4], dispatch_key, payload)
    redis.call('HSET', KEYS[5], dispatch_key, tostring(reserved_units))
    redis.call('HSET', KEYS[6], dispatch_key, '0')
    redis.call('ZADD', KEYS[9], deadline, dispatch_key)
    redis.call('HINCRBY', KEYS[10], 'used_bytes', reserved_units)
    redis.call('HINCRBY', KEYS[10], 'liveness_records', 1)
    redis.call('HINCRBY', KEYS[10], 'deadline_entries', 1)
    redis.call('SET', KEYS[11], stable_digest)
    return state('armed', '', '')
end

if not exact_claim() then
    return state('stale', '', '')
end
local elected = redis.call('HGET', KEYS[7], dispatch_key)
if elected then
    if operation == 'elect' then
        return state('settled', elected, redis.call('HGET', KEYS[8], dispatch_key) or '')
    end
    if operation ~= 'cleanup' then
        return state('stale', '', '')
    end
end

if operation == 'renew' or operation == 'register_failure' then
    local deadline = server_millis()
    if operation == 'renew' then
        deadline = deadline + timeout_ms
    end
    redis.call('HSET', KEYS[3], dispatch_key, tostring(deadline))
    redis.call('ZADD', KEYS[9], deadline, dispatch_key)
    redis.call('SET', KEYS[11], stable_digest)
    return state(operation == 'renew' and 'renewed' or 'registered', '', '')
end

if operation == 'complete_source' then
    redis.call('HSET', KEYS[6], dispatch_key, '1')
    redis.call('SET', KEYS[11], stable_digest)
    return state('completed', '', '')
end

if operation == 'elect' then
    if event == '' or outcome == '' then
        return redis.error_reply('terminal election requires outcome and event')
    end
    local outcomes = number_field(KEYS[10], 'durable_outcomes')
    if outcomes >= max_outcomes then
        return state('fenced', '', '')
    end
    redis.call('HSET', KEYS[7], dispatch_key, outcome)
    redis.call('HSET', KEYS[8], dispatch_key, event)
    if redis.call('ZREM', KEYS[9], dispatch_key) == 1 then
        redis.call('HINCRBY', KEYS[10], 'deadline_entries', -1)
    end
    redis.call('HINCRBY', KEYS[10], 'durable_outcomes', 1)
    redis.call('HINCRBY', KEYS[10], 'staged_events', 1)
    redis.call('SET', KEYS[11], stable_digest)
    return state('won', outcome, event)
end

if operation == 'cleanup' then
    if redis.call('HGET', KEYS[6], dispatch_key) ~= '1'
        or not redis.call('HGET', KEYS[7], dispatch_key)
        or not redis.call('HGET', KEYS[8], dispatch_key) then
        return state('pending', '', '')
    end
    local units = tonumber(redis.call('HGET', KEYS[5], dispatch_key) or '-1')
    if units < 0 then
        return state('accounting', '', '')
    end
    redis.call('HDEL', KEYS[1], dispatch_key)
    redis.call('HDEL', KEYS[2], dispatch_key)
    redis.call('HDEL', KEYS[3], dispatch_key)
    redis.call('HDEL', KEYS[4], dispatch_key)
    redis.call('HDEL', KEYS[5], dispatch_key)
    redis.call('HDEL', KEYS[6], dispatch_key)
    redis.call('HDEL', KEYS[7], dispatch_key)
    redis.call('HDEL', KEYS[8], dispatch_key)
    if redis.call('ZREM', KEYS[9], dispatch_key) == 1 then
        redis.call('HINCRBY', KEYS[10], 'deadline_entries', -1)
    end
    redis.call('HINCRBY', KEYS[10], 'used_bytes', -units)
    redis.call('HINCRBY', KEYS[10], 'liveness_records', -1)
    redis.call('HINCRBY', KEYS[10], 'durable_outcomes', -1)
    redis.call('HINCRBY', KEYS[10], 'staged_events', -1)
    redis.call('SET', KEYS[11], stable_digest)
    return state('cleaned', '', '')
end

return redis.error_reply('unknown liveness-watchdog operation')"#;

#[derive(Clone, Debug)]
pub struct RedisLivenessWatchdogConfig {
    pub namespace: String,
    pub max_records: NonZeroUsize,
    pub max_staged_outcomes: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
    pub max_terminal_event_bytes: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisLivenessWatchdogConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            max_records: NonZeroUsize::new(DEFAULT_MAX_RECORDS).expect("non-zero constant"),
            max_staged_outcomes: NonZeroUsize::new(DEFAULT_MAX_STAGED_OUTCOMES)
                .expect("non-zero constant"),
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("non-zero constant"),
            max_terminal_event_bytes: NonZeroUsize::new(DEFAULT_MAX_TERMINAL_EVENT_BYTES)
                .expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisLivenessWatchdogError> {
        let valid_namespace = !self.namespace.is_empty()
            && self.namespace.len() <= 127
            && self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if !valid_namespace
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.max_terminal_event_bytes.get() as u64 >= self.hard_limit_bytes
        {
            return Err(RedisLivenessWatchdogError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisLivenessWatchdogKeys {
    generations: String,
    owners: String,
    deadlines: String,
    payloads: String,
    reserved_units: String,
    source_completed: String,
    terminal_outcomes: String,
    terminal_events: String,
    deadline_index: String,
    quota: String,
    operations_prefix: String,
}

impl RedisLivenessWatchdogKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:liveness-watchdog");
        Self {
            generations: format!("{prefix}:generations"),
            owners: format!("{prefix}:owners"),
            deadlines: format!("{prefix}:deadlines"),
            payloads: format!("{prefix}:payloads"),
            reserved_units: format!("{prefix}:reserved-units"),
            source_completed: format!("{prefix}:source-completed"),
            terminal_outcomes: format!("{prefix}:terminal-outcomes"),
            terminal_events: format!("{prefix}:terminal-events"),
            deadline_index: format!("{prefix}:deadline-index"),
            quota: format!("{prefix}:quota"),
            operations_prefix: format!("{prefix}:operations"),
        }
    }

    fn operation(&self, identity: &[u8]) -> String {
        format!("{}:{:x}", self.operations_prefix, Sha256::digest(identity))
    }
}

pub trait RedisLivenessWatchdogCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisLivenessWatchdogError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisLivenessWatchdogError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisLivenessWatchdogQuotaState);
}

pub struct MonitoredRedisLivenessWatchdogCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisLivenessWatchdogCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisLivenessWatchdogCapability for MonitoredRedisLivenessWatchdogCapability {
    fn guard_admission(&self) -> Result<u64, RedisLivenessWatchdogError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisLivenessWatchdogError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        (snapshot.state == RedisCapabilityFenceState::Open)
            .then_some(snapshot.generation)
            .ok_or(RedisLivenessWatchdogError::Unavailable)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisLivenessWatchdogError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisLivenessWatchdogError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisLivenessWatchdogQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisLivenessWatchdogQuotaState {
    pub used_bytes: u64,
    pub liveness_records: u64,
    pub deadline_entries: u64,
    pub durable_outcomes: u64,
    pub staged_events: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisLivenessWatchdogQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self.liveness_records + self.durable_outcomes,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisLivenessWatchdog {
    connection: MultiplexedConnection,
    keys: RedisLivenessWatchdogKeys,
    config: Arc<RedisLivenessWatchdogConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisLivenessWatchdogCapability>,
}

/// Post-admission LivenessWatchdog role. It owns the authenticated connection
/// while the monitor owns capability probes and reconstruction ordering.
pub(crate) struct RedisLivenessWatchdogRoleRegistration {
    connection: MultiplexedConnection,
    keys: RedisLivenessWatchdogKeys,
    config: RedisLivenessWatchdogConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisLivenessWatchdogRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisLivenessWatchdogConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisLivenessWatchdogError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisLivenessWatchdogKeys::new(&config.namespace),
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) fn build_watchdog(
        &self,
        capability: Arc<dyn RedisLivenessWatchdogCapability>,
    ) -> Result<RedisLivenessWatchdog, RedisLivenessWatchdogError> {
        RedisLivenessWatchdog::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::LivenessWatchdog
            && context.manifest_identity() == &self.manifest_identity
            && RedisLivenessWatchdog::operation_manifest()
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

    async fn read_reconstructable_status(
        &self,
        operation: &'static str,
    ) -> Result<Vec<Vec<u8>>, RedisReconstructionFailure> {
        let claim = LocalPickupClaim {
            dispatch_key: String::new(),
            pickup_generation: 0,
            owner: String::new(),
            liveness_deadline: DateTime::<Utc>::UNIX_EPOCH,
        };
        let mutation = WatchdogMutation::new(
            &self.keys,
            operation,
            &claim,
            &[],
            "",
            &[],
            0,
            0,
            0,
            &self.config,
        )
        .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        let mut connection = self.connection.clone();
        mutation
            .apply_raw(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

#[async_trait]
impl RedisRoleCapabilityProbe for RedisLivenessWatchdogRoleRegistration {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::Accounting);
        }
        let mut connection = self.connection.clone();
        redis::cmd("EVAL")
            .arg("return redis.call('HGET', KEYS[1], ARGV[1])")
            .arg(1)
            .arg(&self.keys.quota)
            .arg("runtime-capability-canary")
            .query_async::<redis::Value>(&mut connection)
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
impl RedisReconstructionCallback for RedisLivenessWatchdogRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let quota = self.read_reconstructable_status("quota").await?;
        if quota.len() != 13 || quota.first().map(Vec::as_slice) != Some(b"quota") {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        let due = self.read_reconstructable_status("select_due").await?;
        if due.len() != 13 || !matches!(due.first().map(Vec::as_slice), Some(b"none" | b"due")) {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        Ok(())
    }
}

impl RedisLivenessWatchdog {
    pub async fn connect(
        client: redis::Client,
        config: RedisLivenessWatchdogConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisLivenessWatchdogCapability>,
    ) -> Result<Self, RedisLivenessWatchdogError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisLivenessWatchdogError::Unavailable)?;
        Self::from_connection(connection, config, durability, capability)
    }

    fn from_connection(
        connection: MultiplexedConnection,
        config: RedisLivenessWatchdogConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisLivenessWatchdogCapability>,
    ) -> Result<Self, RedisLivenessWatchdogError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisLivenessWatchdogKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
        })
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_liveness_watchdog_operation_manifest()
    }

    pub async fn quota_state(
        &self,
    ) -> Result<RedisLivenessWatchdogQuotaState, RedisLivenessWatchdogError> {
        let state = self.read("quota", None).await?;
        if state.status != "quota" {
            return Err(self.accounting_failure());
        }
        self.capability.report_quota(state.quota);
        Ok(state.quota)
    }

    pub async fn complete_staged_handoff(
        &self,
        claim: &LocalPickupClaim,
    ) -> Result<bool, RedisLivenessWatchdogError> {
        let state = self.mutate("cleanup", claim, &[], "", &[], 0, 0).await?;
        match state.status.as_str() {
            "cleaned" | "replayed" => Ok(true),
            "pending" | "stale" => Ok(false),
            "accounting" => Err(self.accounting_failure()),
            _ => Err(self.accounting_failure()),
        }
    }

    async fn mutate(
        &self,
        operation: &'static str,
        claim: &LocalPickupClaim,
        payload: &[u8],
        outcome: &str,
        event: &[u8],
        timeout_ms: u64,
        nonce: i64,
    ) -> Result<WatchdogState, RedisLivenessWatchdogError> {
        let permit = self.capability.guard_admission()?;
        let reserved_units = RECORD_ACCOUNTED_BYTES
            .checked_add(payload.len() as u64)
            .and_then(|units| units.checked_add(self.config.max_terminal_event_bytes.get() as u64))
            .ok_or(RedisLivenessWatchdogError::InvalidOperation)?;
        let mutation = WatchdogMutation::new(
            &self.keys,
            operation,
            claim,
            payload,
            outcome,
            event,
            timeout_ms,
            nonce,
            reserved_units,
            &self.config,
        )?;
        let output = self.commit(&mutation).await?;
        let state = self.decode(output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(permit)?;
        Ok(state)
    }

    async fn read(
        &self,
        operation: &'static str,
        claim: Option<&LocalPickupClaim>,
    ) -> Result<WatchdogState, RedisLivenessWatchdogError> {
        let placeholder = LocalPickupClaim {
            dispatch_key: String::new(),
            pickup_generation: 0,
            owner: String::new(),
            liveness_deadline: DateTime::<Utc>::UNIX_EPOCH,
        };
        let claim = claim.unwrap_or(&placeholder);
        let mutation = WatchdogMutation::new(
            &self.keys,
            operation,
            claim,
            &[],
            "",
            &[],
            0,
            0,
            0,
            &self.config,
        )?;
        let mut connection = self.connection.clone();
        mutation
            .apply_raw(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))
            .and_then(|output| self.decode(output))
    }

    async fn commit(
        &self,
        mutation: &WatchdogMutation,
    ) -> Result<Vec<Vec<u8>>, RedisLivenessWatchdogError> {
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

    fn decode(&self, output: Vec<Vec<u8>>) -> Result<WatchdogState, RedisLivenessWatchdogError> {
        if output.len() == 1 && output[0].as_slice() == b"conflict" {
            return Err(RedisLivenessWatchdogError::IdentityConflict);
        }
        if output.len() != 13 {
            return Err(self.accounting_failure());
        }
        let text = |index: usize| {
            std::str::from_utf8(&output[index])
                .map(str::to_owned)
                .map_err(|_| RedisLivenessWatchdogError::Accounting)
        };
        let number = |index: usize| {
            std::str::from_utf8(&output[index])
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(RedisLivenessWatchdogError::Accounting)
        };
        let used_bytes = number(3)?;
        let quota = RedisLivenessWatchdogQuotaState {
            used_bytes,
            liveness_records: number(4)?,
            deadline_entries: number(5)?,
            durable_outcomes: number(6)?,
            staged_events: number(7)?,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            pressure: if used_bytes >= self.config.hard_limit_bytes {
                RedisQuotaPressure::HardLimit
            } else if used_bytes >= self.config.soft_limit_bytes {
                RedisQuotaPressure::SoftThreshold
            } else {
                RedisQuotaPressure::BelowSoftThreshold
            },
        };
        let deadline = std::str::from_utf8(&output[9])
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .and_then(DateTime::from_timestamp_millis)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        Ok(WatchdogState {
            status: text(0)?,
            detail: text(1)?,
            event: output[2].clone(),
            quota,
            owner: text(8)?,
            deadline,
            source_completed: output[10].as_slice() == b"1",
            payload: output[11].clone(),
            dispatch_key: text(12)?,
        })
    }

    fn redis_error(&self, error: RedisMutationError) -> RedisLivenessWatchdogError {
        use crate::redis_durability::RedisMutationFailure;
        match error.failure() {
            RedisMutationFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisMutationFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisLivenessWatchdogError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisLivenessWatchdogError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisLivenessWatchdogError::IdentityConflict
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
        RedisLivenessWatchdogError::Durability(error.failure())
    }

    fn accounting_failure(&self) -> RedisLivenessWatchdogError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisLivenessWatchdogError::Accounting
    }
}

#[async_trait::async_trait]
impl SafeLivenessWatchdog for RedisLivenessWatchdog {
    async fn arm_liveness(
        &self,
        claim: &LocalPickupClaim,
        payload: &[u8],
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        if payload.is_empty() || payload.len() > self.config.max_payload_bytes.get() {
            return Err(RedisLivenessWatchdogError::InvalidOperation.to_string());
        }
        let timeout_ms =
            positive_timeout_millis(deadline - now).map_err(|error| error.to_string())?;
        let state = self
            .mutate(
                "arm",
                claim,
                payload,
                "",
                &[],
                timeout_ms,
                deadline.timestamp_millis(),
            )
            .await
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            "armed" | "replayed" => Ok(true),
            "stale" => Ok(false),
            "fenced" => Err(RedisLivenessWatchdogError::CapacityFenced.to_string()),
            _ => Err(self.accounting_failure().to_string()),
        }
    }

    async fn prove_liveness_armed(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        let state = self
            .read("prove", Some(claim))
            .await
            .map_err(|error| error.to_string())?;
        Ok(state.status == "proved")
    }

    async fn complete_source(&self, claim: &LocalPickupClaim) -> Result<bool, String> {
        let state = self
            .mutate("complete_source", claim, &[], "", &[], 0, 0)
            .await
            .map_err(|error| error.to_string())?;
        Ok(matches!(state.status.as_str(), "completed" | "replayed") && state.source_completed)
    }

    async fn renew_liveness(
        &self,
        claim: &LocalPickupClaim,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        let timeout_ms =
            positive_timeout_millis(deadline - now).map_err(|error| error.to_string())?;
        let state = self
            .mutate(
                "renew",
                claim,
                &[],
                "",
                &[],
                timeout_ms,
                deadline.timestamp_millis(),
            )
            .await
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            "renewed" | "replayed" => Ok(true),
            "stale" => Ok(false),
            _ => Err(self.accounting_failure().to_string()),
        }
    }

    async fn select_due_liveness(
        &self,
        _now: DateTime<Utc>,
    ) -> Result<Option<DueLocalPickup>, String> {
        let state = self
            .read("select_due", None)
            .await
            .map_err(|error| error.to_string())?;
        if state.status == "none" {
            return Ok(None);
        }
        if state.status != "due" {
            return Err(self.accounting_failure().to_string());
        }
        let generation = state
            .detail
            .parse::<i64>()
            .map_err(|_| self.accounting_failure().to_string())?;
        Ok(Some(DueLocalPickup {
            claim: LocalPickupClaim {
                dispatch_key: state.dispatch_key,
                pickup_generation: generation,
                owner: state.owner,
                liveness_deadline: state.deadline,
            },
            payload: state.payload,
        }))
    }

    async fn register_liveness_failure(
        &self,
        claim: &LocalPickupClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, String> {
        let state = self
            .mutate(
                "register_failure",
                claim,
                &[],
                "",
                &[],
                0,
                now.timestamp_millis(),
            )
            .await
            .map_err(|error| error.to_string())?;
        match state.status.as_str() {
            "registered" | "replayed" => Ok(true),
            "stale" => Ok(false),
            _ => Err(self.accounting_failure().to_string()),
        }
    }

    async fn elect_terminal(
        &self,
        claim: &LocalPickupClaim,
        outcome: LocalAttemptOutcome,
        terminal_event: &[u8],
        _now: DateTime<Utc>,
    ) -> Result<ElectedAttemptOutcome, String> {
        if terminal_event.is_empty()
            || terminal_event.len() > self.config.max_terminal_event_bytes.get()
        {
            return Err(RedisLivenessWatchdogError::InvalidOperation.to_string());
        }
        let state = self
            .mutate(
                "elect",
                claim,
                &[],
                outcome_name(outcome),
                terminal_event,
                0,
                0,
            )
            .await
            .map_err(|error| error.to_string())?;
        let selected =
            parse_outcome(&state.detail).ok_or_else(|| self.accounting_failure().to_string())?;
        match state.status.as_str() {
            "won" => Ok(ElectedAttemptOutcome {
                election: TerminalElection::Won,
                outcome: selected,
                terminal_event: state.event,
            }),
            "settled" | "replayed" => Ok(ElectedAttemptOutcome {
                election: TerminalElection::Settled(selected),
                outcome: selected,
                terminal_event: state.event,
            }),
            "stale" => Err(format!(
                "terminal election rejected stale or non-owner Redis liveness generation {}",
                claim.pickup_generation
            )),
            "fenced" => Err(RedisLivenessWatchdogError::CapacityFenced.to_string()),
            _ => Err(self.accounting_failure().to_string()),
        }
    }
}

pub fn redis_liveness_watchdog_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(
        LIVENESS_WATCHDOG_SCRIPT_NAME,
        LIVENESS_WATCHDOG_SCRIPT_SHA256,
    )?;
    RedisOperationManifest::new(
        CoordinationRole::LivenessWatchdog,
        REDIS_LIVENESS_WATCHDOG_PROTOCOL,
        REDIS_LIVENESS_WATCHDOG_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:liveness-watchdog:generations",
            "tickr:{namespace}:liveness-watchdog:owners",
            "tickr:{namespace}:liveness-watchdog:deadlines",
            "tickr:{namespace}:liveness-watchdog:payloads",
            "tickr:{namespace}:liveness-watchdog:reserved-units",
            "tickr:{namespace}:liveness-watchdog:source-completed",
            "tickr:{namespace}:liveness-watchdog:terminal-outcomes",
            "tickr:{namespace}:liveness-watchdog:terminal-events",
            "tickr:{namespace}:liveness-watchdog:deadline-index",
            "tickr:{namespace}:liveness-watchdog:quota",
            "tickr:{namespace}:liveness-watchdog:operations:*",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            RedisNamespacePattern::key("tickr:{namespace}:liveness-watchdog:generations"),
        )],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::TaskDispatch,
            ),
            RedisForbiddenOperation::administrative("FLUSHALL"),
        ],
    )
}

struct WatchdogState {
    status: String,
    detail: String,
    event: Vec<u8>,
    quota: RedisLivenessWatchdogQuotaState,
    owner: String,
    deadline: DateTime<Utc>,
    source_completed: bool,
    payload: Vec<u8>,
    dispatch_key: String,
}

struct WatchdogMutation {
    stable: RedisStableOperation,
    stable_digest: String,
    operation_key: String,
    keys: RedisLivenessWatchdogKeys,
    arguments: Vec<Vec<u8>>,
}

impl WatchdogMutation {
    #[allow(clippy::too_many_arguments)]
    fn new(
        keys: &RedisLivenessWatchdogKeys,
        operation: &'static str,
        claim: &LocalPickupClaim,
        payload: &[u8],
        outcome: &str,
        event: &[u8],
        timeout_ms: u64,
        nonce: i64,
        reserved_units: u64,
        config: &RedisLivenessWatchdogConfig,
    ) -> Result<Self, RedisLivenessWatchdogError> {
        let mut fingerprint = Vec::new();
        for part in [
            operation.as_bytes(),
            claim.dispatch_key.as_bytes(),
            claim.owner.as_bytes(),
            payload,
            outcome.as_bytes(),
            event,
        ] {
            fingerprint.extend_from_slice(&(part.len() as u64).to_be_bytes());
            fingerprint.extend_from_slice(part);
        }
        fingerprint.extend_from_slice(&claim.pickup_generation.to_be_bytes());
        fingerprint.extend_from_slice(&timeout_ms.to_be_bytes());
        fingerprint.extend_from_slice(&nonce.to_be_bytes());
        let stable_digest = format!("{:x}", Sha256::digest(&fingerprint));
        let operation_key = keys.operation(&fingerprint);
        let stable = RedisStableOperation::new(&operation_key, stable_digest.as_bytes())
            .map_err(|_| RedisLivenessWatchdogError::InvalidOperation)?;
        Ok(Self {
            stable,
            stable_digest,
            operation_key,
            keys: keys.clone(),
            arguments: vec![
                operation.as_bytes().to_vec(),
                Vec::new(),
                claim.dispatch_key.as_bytes().to_vec(),
                claim.pickup_generation.to_string().into_bytes(),
                claim.owner.as_bytes().to_vec(),
                timeout_ms.to_string().into_bytes(),
                payload.to_vec(),
                outcome.as_bytes().to_vec(),
                event.to_vec(),
                reserved_units.to_string().into_bytes(),
                config.max_records.get().to_string().into_bytes(),
                config.max_staged_outcomes.get().to_string().into_bytes(),
                config.hard_limit_bytes.to_string().into_bytes(),
            ],
        })
    }

    async fn apply_raw(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<Vec<Vec<u8>>, RedisMutationError> {
        let mut arguments = self.arguments.clone();
        arguments[1] = self.stable_digest.as_bytes().to_vec();
        redis::cmd("EVAL")
            .arg(LIVENESS_WATCHDOG_SCRIPT)
            .arg(11)
            .arg(&self.keys.generations)
            .arg(&self.keys.owners)
            .arg(&self.keys.deadlines)
            .arg(&self.keys.payloads)
            .arg(&self.keys.reserved_units)
            .arg(&self.keys.source_completed)
            .arg(&self.keys.terminal_outcomes)
            .arg(&self.keys.terminal_events)
            .arg(&self.keys.deadline_index)
            .arg(&self.keys.quota)
            .arg(&self.operation_key)
            .arg(arguments)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)
    }
}

#[async_trait]
impl RedisStableMutation for WatchdogMutation {
    type Output = Vec<Vec<u8>>;

    fn operation(&self) -> &RedisStableOperation {
        &self.stable
    }

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError> {
        let output = self.apply_raw(connection).await?;
        match output.first().map(Vec::as_slice) {
            Some(b"conflict") => Ok(RedisStableMutationOutcome::IdentityConflict),
            Some(b"replayed" | b"settled") => Ok(RedisStableMutationOutcome::Replayed(output)),
            Some(
                b"armed" | b"renewed" | b"registered" | b"completed" | b"won" | b"cleaned"
                | b"stale" | b"fenced" | b"pending" | b"accounting",
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
pub enum RedisLivenessWatchdogError {
    InvalidConfiguration,
    InvalidOperation,
    IdentityConflict,
    CapacityFenced,
    Accounting,
    Unavailable,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisLivenessWatchdogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("invalid Redis LivenessWatchdog configuration")
            }
            Self::InvalidOperation => {
                formatter.write_str("invalid Redis LivenessWatchdog operation")
            }
            Self::IdentityConflict => {
                formatter.write_str("Redis LivenessWatchdog identity conflicts with accepted state")
            }
            Self::CapacityFenced => {
                formatter.write_str("Redis LivenessWatchdog capacity is fenced")
            }
            Self::Accounting => {
                formatter.write_str("Redis LivenessWatchdog accounting is inconsistent")
            }
            Self::Unavailable => {
                formatter.write_str("Redis LivenessWatchdog capability is unavailable")
            }
            Self::Durability(failure) => write!(
                formatter,
                "Redis LivenessWatchdog durability failed: {failure:?}"
            ),
        }
    }
}

impl std::error::Error for RedisLivenessWatchdogError {}

fn positive_timeout_millis(duration: chrono::Duration) -> Result<u64, RedisLivenessWatchdogError> {
    let millis = duration.num_milliseconds();
    u64::try_from(millis)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(RedisLivenessWatchdogError::InvalidOperation)
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

fn parse_outcome(value: &str) -> Option<LocalAttemptOutcome> {
    match value {
        "process-exited-success" => Some(LocalAttemptOutcome::ProcessExitedSuccess),
        "process-exited-failure" => Some(LocalAttemptOutcome::ProcessExitedFailure),
        "process-setup-failed" => Some(LocalAttemptOutcome::ProcessSetupFailed),
        "liveness-expired" => Some(LocalAttemptOutcome::LivenessExpired),
        "cancellation-killed" => Some(LocalAttemptOutcome::CancellationKilled),
        "cancellation-already-exited" => Some(LocalAttemptOutcome::CancellationAlreadyExited),
        "cancellation-no-process" => Some(LocalAttemptOutcome::CancellationNoProcess),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_exact_and_rejects_unregistered_operations() {
        let manifest = redis_liveness_watchdog_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::LivenessWatchdog);
        assert_eq!(manifest.protocol(), REDIS_LIVENESS_WATCHDOG_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_LIVENESS_WATCHDOG_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.key_patterns().len(), 11);
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);
        assert!(!manifest.commands().contains(&"XADD"));
        assert!(!manifest
            .key_patterns()
            .iter()
            .any(|pattern| pattern.contains(":task-dispatch:")));

        let rejected = RedisOperationManifest::new(
            CoordinationRole::LivenessWatchdog,
            REDIS_LIVENESS_WATCHDOG_PROTOCOL,
            REDIS_LIVENESS_WATCHDOG_COMMANDS.to_vec(),
            manifest.scripts().to_vec(),
            vec!["tickr:{namespace}:liveness-watchdog:generations"],
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("XADD"),
                RedisNamespacePattern::key("tickr:{namespace}:liveness-watchdog:generations"),
            )],
            manifest.forbidden_operations().to_vec(),
        )
        .unwrap_err();
        assert_eq!(
            rejected.failure(),
            crate::redis_operation_manifest::RedisOperationManifestFailure::UnregisteredOperation
        );
    }

    #[test]
    fn script_digest_is_pinned() {
        assert_eq!(
            format!("{:x}", Sha256::digest(LIVENESS_WATCHDOG_SCRIPT.as_bytes())),
            LIVENESS_WATCHDOG_SCRIPT_SHA256
        );
    }
}
