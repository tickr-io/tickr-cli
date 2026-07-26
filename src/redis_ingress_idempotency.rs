use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use sha2::{Digest, Sha256};
use tickr_conductor::ingress_idempotency::{
    IngressEffects, IngressIdempotencyStore, IngressOperation, IngressOutcomeProof,
    IngressReservation, IngressTerminalOutcome, RelayIntent, ReservationOutcome,
};
use uuid::Uuid;

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

pub const REDIS_INGRESS_IDEMPOTENCY_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.ingress-idempotency.redis-lease", 1);

const DEFAULT_LEASE: Duration = Duration::from_secs(30);
const DEFAULT_TERMINAL_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_MAX_RECORDS: usize = 4096;
const DEFAULT_MAX_EFFECT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_RESULT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_INTENT_BYTES: usize = 1024 * 1024;
const DEFAULT_SOFT_BYTES: u64 = 48 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 56 * 1024 * 1024;
const RESERVATION_ACCOUNTED_BYTES: u64 = 384;
const EFFECT_ACCOUNTED_BYTES: u64 = 192;
const RESULT_ACCOUNTED_BYTES: u64 = 128;
const INTENT_ACCOUNTED_BYTES: u64 = 128;
const REJECTION_ACCOUNTED_BYTES: u64 = 192;

const REDIS_INGRESS_IDEMPOTENCY_COMMANDS: &[&str] = &[
    "DEL", "EVAL", "HGET", "HINCRBY", "HMGET", "HSET", "TIME", "WAITAOF",
];
const INGRESS_IDEMPOTENCY_SCRIPT_NAME: &str = "ingress-idempotency-v1";
const INGRESS_IDEMPOTENCY_SCRIPT_SHA256: &str =
    "949a4fa9f48e80e2896123dc41a90ecb9cfb7c47c3a8ec586b0512124f90d031";

const INGRESS_IDEMPOTENCY_SCRIPT: &str = r#"local action = ARGV[1]
local operation_field = ARGV[2]
local operation_digest = ARGV[3]
local producer_digest = ARGV[4]
local payload_digest = ARGV[5]
local owner = ARGV[6]
local signal_id = ARGV[7]
local lease_ms = tonumber(ARGV[8])
local retention_ms = tonumber(ARGV[9])
local max_records = tonumber(ARGV[10])
local soft_bytes = tonumber(ARGV[11])
local hard_bytes = tonumber(ARGV[12])
local effect = ARGV[13]
local results = ARGV[14]
local intents = ARGV[15]
local intent_count = tonumber(ARGV[16])
local reason = ARGV[17]
local requested_units = tonumber(ARGV[18])

local function number_field(field)
    return tonumber(redis.call('HGET', KEYS[3], field) or '0')
end

local function now_ms()
    local time = redis.call('TIME')
    return (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
end

local function state(status)
    return {
        status,
        redis.call('HGET', KEYS[1], 'signal_id') or '',
        redis.call('HGET', KEYS[1], 'owner') or '',
        redis.call('HGET', KEYS[1], 'lease_until') or '0',
        redis.call('HGET', KEYS[1], 'phase') or '',
        redis.call('HGET', KEYS[1], 'effect') or '',
        redis.call('HGET', KEYS[1], 'results') or '',
        redis.call('HGET', KEYS[1], 'intents') or '',
        redis.call('HGET', KEYS[1], 'reason') or '',
        tostring(number_field('used_bytes')),
        tostring(number_field('producer_records')),
        tostring(number_field('effect_records')),
        tostring(number_field('result_records')),
        tostring(number_field('rejection_records')),
        tostring(number_field('relay_intent_records')),
        redis.call('HGET', KEYS[1], 'payload_digest') or ''
    }
end

local prior_operation = redis.call('HGET', KEYS[2], operation_field)
if action ~= 'reserve' and prior_operation and prior_operation ~= operation_digest then
    return state('identity_conflict')
end

if action == 'reserve' then
    local phase = redis.call('HGET', KEYS[1], 'phase')
    if not phase then
        local records = number_field('producer_records')
        local used = number_field('used_bytes')
        if records + 1 > max_records or used + requested_units > hard_bytes then
            return state('fenced')
        end
        local now = now_ms()
        redis.call('HSET', KEYS[1],
            'version', '1',
            'producer_digest', producer_digest,
            'payload_digest', payload_digest,
            'signal_id', signal_id,
            'owner', owner,
            'lease_until', tostring(now + lease_ms),
            'phase', 'reserved',
            'units', tostring(requested_units))
        redis.call('HSET', KEYS[2], operation_field, operation_digest)
        redis.call('HINCRBY', KEYS[3], 'used_bytes', requested_units)
        redis.call('HINCRBY', KEYS[3], 'producer_records', 1)
        return state('acquired')
    end
    if redis.call('HGET', KEYS[1], 'producer_digest') ~= producer_digest then
        return state('accounting')
    end
    if redis.call('HGET', KEYS[1], 'payload_digest') ~= payload_digest then
        return state('conflict')
    end
    if phase == 'ready' then return state('ready') end
    if phase == 'relayed' then return state('complete') end
    if phase == 'rejected' then return state('rejected') end
    if phase ~= 'reserved' then return state('accounting') end
    local now = now_ms()
    local lease_until = tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0')
    local current_owner = redis.call('HGET', KEYS[1], 'owner') or ''
    if current_owner == owner or lease_until <= now then
        redis.call('HSET', KEYS[2], operation_field, operation_digest)
        redis.call('HSET', KEYS[1], 'owner', owner, 'lease_until', tostring(now + lease_ms))
        return state('acquired')
    end
    return state('pending')
end

local phase = redis.call('HGET', KEYS[1], 'phase')
if not phase then return state('missing') end
if redis.call('HGET', KEYS[1], 'producer_digest') ~= producer_digest or
   redis.call('HGET', KEYS[1], 'payload_digest') ~= payload_digest then
    return state('conflict')
end

if action == 'ready' then
    if phase == 'ready' then
        redis.call('HSET', KEYS[2], operation_field, operation_digest)
        return state('ready')
    end
    if phase ~= 'reserved' then return state('invalid_phase') end
    local now = now_ms()
    if redis.call('HGET', KEYS[1], 'owner') ~= owner or
       tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') < now then
        return state('lease_lost')
    end
    local used = number_field('used_bytes')
    if used + requested_units > hard_bytes then return state('fenced') end
    redis.call('HSET', KEYS[2], operation_field, operation_digest)
    redis.call('HSET', KEYS[1],
        'effect', effect,
        'results', results,
        'intents', intents,
        'intent_count', tostring(intent_count),
        'phase', 'ready',
        'units', tostring(tonumber(redis.call('HGET', KEYS[1], 'units')) + requested_units))
    redis.call('HINCRBY', KEYS[3], 'used_bytes', requested_units)
    redis.call('HINCRBY', KEYS[3], 'effect_records', 1)
    redis.call('HINCRBY', KEYS[3], 'result_records', 1)
    redis.call('HINCRBY', KEYS[3], 'relay_intent_records', intent_count)
    return state('ready')
end

if action == 'relayed' then
    if phase == 'relayed' then
        redis.call('HSET', KEYS[2], operation_field, operation_digest)
        return state('complete')
    end
    if phase ~= 'ready' then return state('invalid_phase') end
    redis.call('HSET', KEYS[2], operation_field, operation_digest)
    redis.call('HSET', KEYS[1], 'phase', 'relayed', 'retention_until', tostring(now_ms() + retention_ms))
    return state('complete')
end

if action == 'reject' then
    if phase == 'rejected' then
        redis.call('HSET', KEYS[2], operation_field, operation_digest)
        return state('rejected')
    end
    if phase ~= 'reserved' then return state('invalid_phase') end
    local now = now_ms()
    if redis.call('HGET', KEYS[1], 'owner') ~= owner or
       tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') < now then
        return state('lease_lost')
    end
    local used = number_field('used_bytes')
    if used + requested_units > hard_bytes then return state('fenced') end
    redis.call('HSET', KEYS[2], operation_field, operation_digest)
    redis.call('HSET', KEYS[1],
        'phase', 'rejected',
        'reason', reason,
        'retention_until', tostring(now + retention_ms),
        'units', tostring(tonumber(redis.call('HGET', KEYS[1], 'units')) + requested_units))
    redis.call('HINCRBY', KEYS[3], 'used_bytes', requested_units)
    redis.call('HINCRBY', KEYS[3], 'rejection_records', 1)
    return state('rejected')
end

if action == 'abandon' then
    if phase ~= 'reserved' then return state('invalid_phase') end
    if redis.call('HGET', KEYS[1], 'owner') ~= owner then return state('lease_lost') end
    redis.call('HSET', KEYS[2], operation_field, operation_digest)
    redis.call('HSET', KEYS[1], 'lease_until', '0')
    return state('abandoned')
end

if action == 'cleanup' then
    if phase ~= 'relayed' and phase ~= 'rejected' then return state('invalid_phase') end
    if tonumber(redis.call('HGET', KEYS[1], 'retention_until') or '0') > now_ms() then
        return state('retained')
    end
    local units = tonumber(redis.call('HGET', KEYS[1], 'units') or '-1')
    local effect_records = tonumber(redis.call('HGET', KEYS[1], 'effect') and '1' or '0')
    local result_records = tonumber(redis.call('HGET', KEYS[1], 'results') and '1' or '0')
    local rejection_records = phase == 'rejected' and 1 or 0
    local relay_intents = tonumber(redis.call('HGET', KEYS[1], 'intent_count') or '0')
    if units < 0 or number_field('used_bytes') < units or number_field('producer_records') < 1 or
       number_field('effect_records') < effect_records or number_field('result_records') < result_records or
       number_field('rejection_records') < rejection_records or number_field('relay_intent_records') < relay_intents then
        return state('accounting')
    end
    redis.call('DEL', KEYS[1], KEYS[2])
    redis.call('HINCRBY', KEYS[3], 'used_bytes', -units)
    redis.call('HINCRBY', KEYS[3], 'producer_records', -1)
    if effect_records > 0 then redis.call('HINCRBY', KEYS[3], 'effect_records', -effect_records) end
    if result_records > 0 then redis.call('HINCRBY', KEYS[3], 'result_records', -result_records) end
    if rejection_records > 0 then redis.call('HINCRBY', KEYS[3], 'rejection_records', -rejection_records) end
    if relay_intents > 0 then redis.call('HINCRBY', KEYS[3], 'relay_intent_records', -relay_intents) end
    return state('cleaned')
end

return redis.error_reply('unknown ingress idempotency operation')"#;

#[derive(Clone, Debug)]
pub struct RedisIngressIdempotencyConfig {
    pub namespace: String,
    pub claim_lease: Duration,
    pub terminal_retention: Duration,
    pub max_producer_records: NonZeroUsize,
    pub max_effect_bytes: NonZeroUsize,
    pub max_result_bytes: NonZeroUsize,
    pub max_intent_bytes: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisIngressIdempotencyConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            claim_lease: DEFAULT_LEASE,
            terminal_retention: DEFAULT_TERMINAL_RETENTION,
            max_producer_records: NonZeroUsize::new(DEFAULT_MAX_RECORDS)
                .expect("non-zero constant"),
            max_effect_bytes: NonZeroUsize::new(DEFAULT_MAX_EFFECT_BYTES)
                .expect("non-zero constant"),
            max_result_bytes: NonZeroUsize::new(DEFAULT_MAX_RESULT_BYTES)
                .expect("non-zero constant"),
            max_intent_bytes: NonZeroUsize::new(DEFAULT_MAX_INTENT_BYTES)
                .expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisIngressIdempotencyError> {
        if !valid_symbol(&self.namespace)
            || self.claim_lease.is_zero()
            || self.terminal_retention.is_zero()
            || millis(self.claim_lease) == 0
            || millis(self.terminal_retention) == 0
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes < RESERVATION_ACCOUNTED_BYTES
        {
            return Err(RedisIngressIdempotencyError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisIngressIdempotencyKeys {
    prefix: String,
    quota: String,
}

impl RedisIngressIdempotencyKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:ingress-idempotency-store");
        Self {
            quota: format!("{prefix}:quota"),
            prefix,
        }
    }

    fn producer(&self, producer_digest: &str) -> String {
        format!("{}:producer:{producer_digest}", self.prefix)
    }

    fn operations(&self, producer_digest: &str) -> String {
        format!("{}:operations:{producer_digest}", self.prefix)
    }
}

pub trait RedisIngressIdempotencyCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisIngressIdempotencyError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisIngressIdempotencyError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisIngressIdempotencyQuotaState);
}

pub struct MonitoredRedisIngressIdempotencyCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisIngressIdempotencyCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisIngressIdempotencyCapability for MonitoredRedisIngressIdempotencyCapability {
    fn guard_admission(&self) -> Result<u64, RedisIngressIdempotencyError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisIngressIdempotencyError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisIngressIdempotencyError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisIngressIdempotencyError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisIngressIdempotencyError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisIngressIdempotencyQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisIngressEffects {
    pub signal_effect: Vec<u8>,
    pub event_results: Vec<u8>,
    pub relay_intents: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisIngressTerminalOutcome {
    Accepted,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisIngressOutcomeProof {
    producer_digest: String,
    payload_digest: String,
    outcome: RedisIngressTerminalOutcome,
}

impl RedisIngressOutcomeProof {
    pub fn outcome(&self) -> RedisIngressTerminalOutcome {
        self.outcome
    }

    pub(crate) fn producer_digest(&self) -> &str {
        &self.producer_digest
    }

    pub(crate) fn payload_digest(&self) -> &str {
        &self.payload_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisIngressConflict {
    pub original_signal_id: Uuid,
    pub original_payload_digest: String,
    pub proof: RedisIngressOutcomeProof,
}

#[derive(Clone)]
pub enum RedisIngressReservationOutcome {
    Acquired(RedisIngressReservation),
    Pending,
    Ready(RedisIngressOperation, RedisIngressEffects),
    Complete(RedisIngressOutcomeProof),
    Rejected(RedisIngressOutcomeProof),
    Conflict(RedisIngressConflict),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisIngressIdempotencyQuotaState {
    pub used_bytes: u64,
    pub producer_records: u64,
    pub effect_records: u64,
    pub result_records: u64,
    pub rejection_records: u64,
    pub relay_intent_records: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub max_producer_records: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisIngressIdempotencyQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self.producer_records,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisIngressIdempotencyStore {
    connection: MultiplexedConnection,
    keys: RedisIngressIdempotencyKeys,
    config: Arc<RedisIngressIdempotencyConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisIngressIdempotencyCapability>,
}

#[derive(Clone)]
pub struct RedisIngressOperation {
    store: RedisIngressIdempotencyStore,
    producer_digest: String,
    payload_digest: String,
}

#[derive(Clone)]
pub struct RedisIngressReservation {
    operation: RedisIngressOperation,
    owner: Uuid,
    signal_id: Uuid,
}

pub(crate) struct RedisIngressIdempotencyRoleRegistration {
    connection: MultiplexedConnection,
    config: RedisIngressIdempotencyConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisIngressIdempotencyRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisIngressIdempotencyConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisIngressIdempotencyError> {
        config.validate()?;
        Ok(Self {
            connection,
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) fn build_store(
        &self,
        capability: Arc<dyn RedisIngressIdempotencyCapability>,
    ) -> Result<RedisIngressIdempotencyStore, RedisIngressIdempotencyError> {
        RedisIngressIdempotencyStore::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::IngressIdempotencyStore
            && context.manifest_identity() == &self.manifest_identity
            && RedisIngressIdempotencyStore::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisIngressIdempotencyRoleRegistration {
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
            .arg(RedisIngressIdempotencyKeys::new(&self.config.namespace).quota)
            .arg("used_bytes")
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
            "tickr:{{{}}}:event-ingress:dispositions",
            self.config.namespace
        );
        let cross_role_denied = self
            .representative_denial(
                redis::cmd("EVAL")
                    .arg("return redis.call('HGET', KEYS[1], ARGV[1])")
                    .arg(1)
                    .arg(cross_role_key)
                    .arg("runtime-capability-canary"),
            )
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
impl RedisReconstructionCallback for RedisIngressIdempotencyRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(RedisIngressIdempotencyKeys::new(&self.config.namespace).quota)
            .arg(&[
                "used_bytes",
                "producer_records",
                "effect_records",
                "result_records",
                "rejection_records",
                "relay_intent_records",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        (values.len() == 6)
            .then_some(())
            .ok_or(RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

impl RedisIngressIdempotencyStore {
    pub async fn connect(
        client: redis::Client,
        config: RedisIngressIdempotencyConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisIngressIdempotencyCapability>,
    ) -> Result<Self, RedisIngressIdempotencyError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisIngressIdempotencyError::Unavailable)?;
        Self::from_connection(connection, config, durability, capability)
    }

    pub(crate) fn from_connection(
        connection: MultiplexedConnection,
        config: RedisIngressIdempotencyConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisIngressIdempotencyCapability>,
    ) -> Result<Self, RedisIngressIdempotencyError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisIngressIdempotencyKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
        })
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_ingress_idempotency_operation_manifest()
    }

    pub async fn reserve(
        &self,
        producer_key: &str,
        payload_sha256: &[u8; 32],
    ) -> Result<RedisIngressReservationOutcome, RedisIngressIdempotencyError> {
        if producer_key.is_empty() || producer_key.len() > 1024 {
            return Err(RedisIngressIdempotencyError::InvalidOperation);
        }
        let generation = self.capability.guard_admission()?;
        let producer_digest = digest_hex(producer_key.as_bytes());
        let payload_digest = bytes_hex(payload_sha256);
        let owner = Uuid::new_v4();
        let signal_id = stable_signal_id(&producer_digest, &payload_digest);
        let units = RESERVATION_ACCOUNTED_BYTES
            .checked_add(producer_digest.len() as u64)
            .and_then(|value| value.checked_add(payload_digest.len() as u64))
            .ok_or(RedisIngressIdempotencyError::InvalidOperation)?;
        let mutation = IdempotencyMutation::new(
            &self.keys,
            "reserve",
            "reserve".to_owned(),
            producer_digest.clone(),
            payload_digest.clone(),
            owner.to_string(),
            signal_id.to_string(),
            units,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
            String::new(),
            &self.config,
        )?;
        let output = self.commit(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        let operation = RedisIngressOperation {
            store: self.clone(),
            producer_digest: producer_digest.clone(),
            payload_digest: payload_digest.clone(),
        };
        let proof = |outcome| RedisIngressOutcomeProof {
            producer_digest: producer_digest.clone(),
            payload_digest: payload_digest.clone(),
            outcome,
        };
        match state.status.as_str() {
            "acquired" => Ok(RedisIngressReservationOutcome::Acquired(
                RedisIngressReservation {
                    operation,
                    owner,
                    signal_id: parse_uuid(&state.signal_id)?,
                },
            )),
            "pending" => Ok(RedisIngressReservationOutcome::Pending),
            "ready" => Ok(RedisIngressReservationOutcome::Ready(
                operation,
                decode_effects(&state)?,
            )),
            "complete" => Ok(RedisIngressReservationOutcome::Complete(proof(
                RedisIngressTerminalOutcome::Accepted,
            ))),
            "rejected" => Ok(RedisIngressReservationOutcome::Rejected(proof(
                RedisIngressTerminalOutcome::Rejected,
            ))),
            "conflict" => Ok(RedisIngressReservationOutcome::Conflict(
                RedisIngressConflict {
                    original_signal_id: parse_uuid(&state.signal_id)?,
                    original_payload_digest: state.payload_digest.clone(),
                    proof: proof(RedisIngressTerminalOutcome::Rejected),
                },
            )),
            "fenced" => Err(RedisIngressIdempotencyError::CapacityFenced),
            "accounting" | "missing" => Err(self.accounting_failure()),
            _ => Err(RedisIngressIdempotencyError::InvalidState),
        }
    }

    pub async fn quota_state(
        &self,
    ) -> Result<RedisIngressIdempotencyQuotaState, RedisIngressIdempotencyError> {
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "producer_records",
                "effect_records",
                "result_records",
                "rejection_records",
                "relay_intent_records",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.len() != 6 {
            return Err(self.accounting_failure());
        }
        Ok(self.quota_from(
            values[0].unwrap_or(0),
            values[1].unwrap_or(0),
            values[2].unwrap_or(0),
            values[3].unwrap_or(0),
            values[4].unwrap_or(0),
            values[5].unwrap_or(0),
        ))
    }

    pub async fn cleanup_terminal(
        &self,
        producer_key: &str,
        payload_sha256: &[u8; 32],
    ) -> Result<bool, RedisIngressIdempotencyError> {
        let generation = self.capability.guard_admission()?;
        let producer_digest = digest_hex(producer_key.as_bytes());
        let payload_digest = bytes_hex(payload_sha256);
        let mutation = IdempotencyMutation::new(
            &self.keys,
            "cleanup",
            "cleanup".to_owned(),
            producer_digest,
            payload_digest,
            String::new(),
            String::new(),
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
            String::new(),
            &self.config,
        )?;
        let output = self.commit(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "cleaned" => Ok(true),
            "retained" => Ok(false),
            "accounting" | "missing" => Err(self.accounting_failure()),
            _ => Err(RedisIngressIdempotencyError::InvalidState),
        }
    }

    async fn commit(
        &self,
        mutation: &IdempotencyMutation,
    ) -> Result<Vec<Vec<u8>>, RedisIngressIdempotencyError> {
        let mut connection = self.connection.clone();
        self.durability
            .execute(&mut connection, mutation)
            .await
            .map(|committed| committed.into_output())
            .map_err(|error| self.durability_error(error))
    }

    fn decode_state(
        &self,
        output: &[Vec<u8>],
    ) -> Result<DecodedState, RedisIngressIdempotencyError> {
        if output.len() != 16 {
            return Err(self.accounting_failure());
        }
        let text = |index: usize| {
            String::from_utf8(output[index].clone())
                .map_err(|_| RedisIngressIdempotencyError::InvalidState)
        };
        let number = |index: usize| {
            std::str::from_utf8(&output[index])
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| self.accounting_failure())
        };
        Ok(DecodedState {
            status: text(0)?,
            signal_id: text(1)?,
            payload_digest: text(15)?,
            effect: output[5].clone(),
            results: output[6].clone(),
            intents: output[7].clone(),
            quota: self.quota_from(
                number(9)?,
                number(10)?,
                number(11)?,
                number(12)?,
                number(13)?,
                number(14)?,
            ),
        })
    }

    fn quota_from(
        &self,
        used_bytes: u64,
        producer_records: u64,
        effect_records: u64,
        result_records: u64,
        rejection_records: u64,
        relay_intent_records: u64,
    ) -> RedisIngressIdempotencyQuotaState {
        let pressure = if used_bytes >= self.config.hard_limit_bytes
            || producer_records >= self.config.max_producer_records.get() as u64
        {
            RedisQuotaPressure::HardLimit
        } else if used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisIngressIdempotencyQuotaState {
            used_bytes,
            producer_records,
            effect_records,
            result_records,
            rejection_records,
            relay_intent_records,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            max_producer_records: self.config.max_producer_records.get() as u64,
            pressure,
        }
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisIngressIdempotencyError {
        use crate::redis_durability::RedisMutationFailure;
        match RedisMutationError::from_redis(error).failure() {
            RedisMutationFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisMutationFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisIngressIdempotencyError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisIngressIdempotencyError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisIngressIdempotencyError::IdentityConflict
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
        RedisIngressIdempotencyError::Durability(error.failure())
    }

    fn accounting_failure(&self) -> RedisIngressIdempotencyError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisIngressIdempotencyError::Accounting
    }
}

impl RedisIngressReservation {
    pub fn signal_id(&self) -> Uuid {
        self.signal_id
    }

    pub fn operation(&self) -> RedisIngressOperation {
        self.operation.clone()
    }

    pub async fn persist_effects(
        &self,
        signal_effect: Vec<u8>,
        event_results: Vec<u8>,
        relay_intents: Vec<Vec<u8>>,
    ) -> Result<RedisIngressEffects, RedisIngressIdempotencyError> {
        let config = &self.operation.store.config;
        let intents = encode_intents(&relay_intents)?;
        if signal_effect.is_empty()
            || signal_effect.len() > config.max_effect_bytes.get()
            || event_results.len() > config.max_result_bytes.get()
            || intents.len() > config.max_intent_bytes.get()
        {
            return Err(RedisIngressIdempotencyError::InvalidOperation);
        }
        let units = EFFECT_ACCOUNTED_BYTES
            .checked_add(signal_effect.len() as u64)
            .and_then(|value| value.checked_add(RESULT_ACCOUNTED_BYTES))
            .and_then(|value| value.checked_add(event_results.len() as u64))
            .and_then(|value| {
                value.checked_add(INTENT_ACCOUNTED_BYTES.saturating_mul(relay_intents.len() as u64))
            })
            .and_then(|value| value.checked_add(intents.len() as u64))
            .ok_or(RedisIngressIdempotencyError::InvalidOperation)?;
        let mut fingerprint =
            Vec::with_capacity(signal_effect.len() + event_results.len() + intents.len() + 24);
        append_framed(&mut fingerprint, &signal_effect);
        append_framed(&mut fingerprint, &event_results);
        append_framed(&mut fingerprint, &intents);
        let generation = self.operation.store.capability.guard_admission()?;
        let mutation = IdempotencyMutation::new_with_fingerprint(
            &self.operation.store.keys,
            "ready",
            format!("ready:{}", self.owner),
            self.operation.producer_digest.clone(),
            self.operation.payload_digest.clone(),
            self.owner.to_string(),
            self.signal_id.to_string(),
            units,
            signal_effect,
            event_results,
            intents,
            relay_intents.len() as u64,
            String::new(),
            &self.operation.store.config,
            fingerprint,
        )?;
        let output = self.operation.store.commit(&mutation).await?;
        let state = self.operation.store.decode_state(&output)?;
        self.operation.store.capability.report_quota(state.quota);
        self.operation
            .store
            .capability
            .guard_acknowledgement(generation)?;
        if state.status == "fenced" {
            return Err(RedisIngressIdempotencyError::CapacityFenced);
        }
        if state.status != "ready" {
            return Err(match state.status.as_str() {
                "lease_lost" => RedisIngressIdempotencyError::LeaseLost,
                "accounting" | "missing" => self.operation.store.accounting_failure(),
                _ => RedisIngressIdempotencyError::InvalidState,
            });
        }
        decode_effects(&state)
    }

    pub async fn reject(
        &self,
        reason: impl Into<String>,
    ) -> Result<RedisIngressOutcomeProof, RedisIngressIdempotencyError> {
        let reason = reason.into();
        if reason.is_empty() || reason.len() > 4096 {
            return Err(RedisIngressIdempotencyError::InvalidOperation);
        }
        let generation = self.operation.store.capability.guard_admission()?;
        let units = REJECTION_ACCOUNTED_BYTES
            .checked_add(reason.len() as u64)
            .ok_or(RedisIngressIdempotencyError::InvalidOperation)?;
        let mutation = IdempotencyMutation::new(
            &self.operation.store.keys,
            "reject",
            format!("reject:{}", self.owner),
            self.operation.producer_digest.clone(),
            self.operation.payload_digest.clone(),
            self.owner.to_string(),
            self.signal_id.to_string(),
            units,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
            reason,
            &self.operation.store.config,
        )?;
        let output = self.operation.store.commit(&mutation).await?;
        let state = self.operation.store.decode_state(&output)?;
        self.operation.store.capability.report_quota(state.quota);
        self.operation
            .store
            .capability
            .guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "rejected" => Ok(self.operation.proof(RedisIngressTerminalOutcome::Rejected)),
            "fenced" => Err(RedisIngressIdempotencyError::CapacityFenced),
            "lease_lost" => Err(RedisIngressIdempotencyError::LeaseLost),
            "accounting" | "missing" => Err(self.operation.store.accounting_failure()),
            _ => Err(RedisIngressIdempotencyError::InvalidState),
        }
    }

    pub async fn abandon(&self) -> Result<(), RedisIngressIdempotencyError> {
        let generation = self.operation.store.capability.guard_admission()?;
        let mutation = IdempotencyMutation::new(
            &self.operation.store.keys,
            "abandon",
            format!("abandon:{}", self.owner),
            self.operation.producer_digest.clone(),
            self.operation.payload_digest.clone(),
            self.owner.to_string(),
            self.signal_id.to_string(),
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
            String::new(),
            &self.operation.store.config,
        )?;
        let output = self.operation.store.commit(&mutation).await?;
        let state = self.operation.store.decode_state(&output)?;
        self.operation.store.capability.report_quota(state.quota);
        self.operation
            .store
            .capability
            .guard_acknowledgement(generation)?;
        if state.status == "abandoned" {
            Ok(())
        } else {
            Err(RedisIngressIdempotencyError::LeaseLost)
        }
    }
}

impl RedisIngressOperation {
    pub async fn mark_relayed(
        &self,
    ) -> Result<RedisIngressOutcomeProof, RedisIngressIdempotencyError> {
        let generation = self.store.capability.guard_admission()?;
        let mutation = IdempotencyMutation::new(
            &self.store.keys,
            "relayed",
            "relayed".to_owned(),
            self.producer_digest.clone(),
            self.payload_digest.clone(),
            String::new(),
            String::new(),
            0,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
            String::new(),
            &self.store.config,
        )?;
        let output = self.store.commit(&mutation).await?;
        let state = self.store.decode_state(&output)?;
        self.store.capability.report_quota(state.quota);
        self.store.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "complete" => Ok(self.proof(RedisIngressTerminalOutcome::Accepted)),
            "accounting" | "missing" => Err(self.store.accounting_failure()),
            _ => Err(RedisIngressIdempotencyError::InvalidState),
        }
    }

    fn proof(&self, outcome: RedisIngressTerminalOutcome) -> RedisIngressOutcomeProof {
        RedisIngressOutcomeProof {
            producer_digest: self.producer_digest.clone(),
            payload_digest: self.payload_digest.clone(),
            outcome,
        }
    }
}

#[async_trait]
impl IngressIdempotencyStore for RedisIngressIdempotencyStore {
    async fn reserve(
        &self,
        producer_key: &str,
        payload_sha256: &[u8; 32],
    ) -> anyhow::Result<ReservationOutcome> {
        let outcome = RedisIngressIdempotencyStore::reserve(self, producer_key, payload_sha256)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(match outcome {
            RedisIngressReservationOutcome::Acquired(reservation) => {
                ReservationOutcome::Acquired(Arc::new(reservation))
            }
            RedisIngressReservationOutcome::Pending => ReservationOutcome::Pending,
            RedisIngressReservationOutcome::Ready(operation, effects) => {
                ReservationOutcome::Ready(Arc::new(operation), decode_common_effects(effects)?)
            }
            RedisIngressReservationOutcome::Complete(proof) => {
                ReservationOutcome::Complete(common_proof(&proof))
            }
            RedisIngressReservationOutcome::Rejected(proof) => {
                ReservationOutcome::Rejected(common_proof(&proof))
            }
            RedisIngressReservationOutcome::Conflict(conflict) => ReservationOutcome::Conflict {
                original_signal_id: conflict.original_signal_id,
                original_hash: conflict.original_payload_digest,
                proof: common_proof(&conflict.proof),
            },
        })
    }
}

#[async_trait]
impl IngressReservation for RedisIngressReservation {
    fn signal_id(&self) -> Uuid {
        RedisIngressReservation::signal_id(self)
    }

    fn operation(&self) -> Arc<dyn IngressOperation> {
        Arc::new(RedisIngressReservation::operation(self))
    }

    async fn persist_effects(&self, effects: IngressEffects) -> anyhow::Result<IngressEffects> {
        let relay_intents = effects
            .relay_intents
            .iter()
            .map(serde_json::to_vec)
            .collect::<Result<Vec<_>, _>>()?;
        let persisted = RedisIngressReservation::persist_effects(
            self,
            effects.signal_effect,
            effects.event_results,
            relay_intents,
        )
        .await
        .map_err(anyhow::Error::new)?;
        decode_common_effects(persisted)
    }

    async fn reject(&self, reason: String) -> anyhow::Result<IngressOutcomeProof> {
        RedisIngressReservation::reject(self, reason)
            .await
            .map(|proof| common_proof(&proof))
            .map_err(anyhow::Error::new)
    }

    async fn abandon(&self) -> anyhow::Result<()> {
        RedisIngressReservation::abandon(self)
            .await
            .map_err(anyhow::Error::new)
    }
}

#[async_trait]
impl IngressOperation for RedisIngressOperation {
    async fn mark_relayed(&self) -> anyhow::Result<IngressOutcomeProof> {
        RedisIngressOperation::mark_relayed(self)
            .await
            .map(|proof| common_proof(&proof))
            .map_err(anyhow::Error::new)
    }
}

fn common_proof(proof: &RedisIngressOutcomeProof) -> IngressOutcomeProof {
    IngressOutcomeProof::new(
        proof.producer_digest().to_owned(),
        proof.payload_digest().to_owned(),
        match proof.outcome() {
            RedisIngressTerminalOutcome::Accepted => IngressTerminalOutcome::Accepted,
            RedisIngressTerminalOutcome::Rejected => IngressTerminalOutcome::Rejected,
        },
    )
}

fn decode_common_effects(effects: RedisIngressEffects) -> anyhow::Result<IngressEffects> {
    let relay_intents = effects
        .relay_intents
        .into_iter()
        .map(|bytes| serde_json::from_slice(&bytes))
        .collect::<Result<Vec<RelayIntent>, _>>()?;
    Ok(IngressEffects {
        signal_effect: effects.signal_effect,
        event_results: effects.event_results,
        relay_intents,
    })
}

pub fn redis_ingress_idempotency_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(
        INGRESS_IDEMPOTENCY_SCRIPT_NAME,
        INGRESS_IDEMPOTENCY_SCRIPT_SHA256,
    )?;
    RedisOperationManifest::new(
        CoordinationRole::IngressIdempotencyStore,
        REDIS_INGRESS_IDEMPOTENCY_PROTOCOL,
        REDIS_INGRESS_IDEMPOTENCY_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:ingress-idempotency-store:operations:*",
            "tickr:{namespace}:ingress-idempotency-store:producer:*",
            "tickr:{namespace}:ingress-idempotency-store:quota",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            RedisNamespacePattern::key("tickr:{namespace}:ingress-idempotency-store:producer:*"),
        )],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::EventIngress,
            ),
            RedisForbiddenOperation::administrative("FLUSHALL"),
        ],
    )
}

struct DecodedState {
    status: String,
    signal_id: String,
    payload_digest: String,
    effect: Vec<u8>,
    results: Vec<u8>,
    intents: Vec<u8>,
    quota: RedisIngressIdempotencyQuotaState,
}

struct IdempotencyMutation {
    operation: RedisStableOperation,
    producer_key: String,
    operations_key: String,
    quota_key: String,
    args: Vec<Vec<u8>>,
    operation_field: String,
    operation_digest: String,
}

#[allow(clippy::too_many_arguments)]
impl IdempotencyMutation {
    fn new(
        keys: &RedisIngressIdempotencyKeys,
        action: &str,
        operation_field: String,
        producer_digest: String,
        payload_digest: String,
        owner: String,
        signal_id: String,
        units: u64,
        effect: Vec<u8>,
        results: Vec<u8>,
        intents: Vec<u8>,
        intent_count: u64,
        reason: String,
        config: &RedisIngressIdempotencyConfig,
    ) -> Result<Self, RedisIngressIdempotencyError> {
        let mut fingerprint = Vec::new();
        append_framed(&mut fingerprint, action.as_bytes());
        append_framed(&mut fingerprint, payload_digest.as_bytes());
        append_framed(&mut fingerprint, owner.as_bytes());
        append_framed(&mut fingerprint, &effect);
        append_framed(&mut fingerprint, &results);
        append_framed(&mut fingerprint, &intents);
        append_framed(&mut fingerprint, reason.as_bytes());
        Self::new_with_fingerprint(
            keys,
            action,
            operation_field,
            producer_digest,
            payload_digest,
            owner,
            signal_id,
            units,
            effect,
            results,
            intents,
            intent_count,
            reason,
            config,
            fingerprint,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_fingerprint(
        keys: &RedisIngressIdempotencyKeys,
        action: &str,
        operation_field: String,
        producer_digest: String,
        payload_digest: String,
        owner: String,
        signal_id: String,
        units: u64,
        effect: Vec<u8>,
        results: Vec<u8>,
        intents: Vec<u8>,
        intent_count: u64,
        reason: String,
        config: &RedisIngressIdempotencyConfig,
        fingerprint: Vec<u8>,
    ) -> Result<Self, RedisIngressIdempotencyError> {
        let producer_key = keys.producer(&producer_digest);
        let operations_key = keys.operations(&producer_digest);
        let operation_digest = digest_hex(&fingerprint);
        let operation =
            RedisStableOperation::new(format!("{operations_key}:{operation_field}"), &fingerprint)
                .map_err(|_| RedisIngressIdempotencyError::InvalidOperation)?;
        let args = vec![
            action.as_bytes().to_vec(),
            operation_field.as_bytes().to_vec(),
            operation_digest.as_bytes().to_vec(),
            producer_digest.as_bytes().to_vec(),
            payload_digest.as_bytes().to_vec(),
            owner.as_bytes().to_vec(),
            signal_id.as_bytes().to_vec(),
            millis(config.claim_lease).to_string().into_bytes(),
            millis(config.terminal_retention).to_string().into_bytes(),
            config.max_producer_records.get().to_string().into_bytes(),
            config.soft_limit_bytes.to_string().into_bytes(),
            config.hard_limit_bytes.to_string().into_bytes(),
            effect,
            results,
            intents,
            intent_count.to_string().into_bytes(),
            reason.into_bytes(),
            units.to_string().into_bytes(),
        ];
        Ok(Self {
            operation,
            producer_key,
            operations_key,
            quota_key: keys.quota.clone(),
            args,
            operation_field,
            operation_digest,
        })
    }
}

#[async_trait]
impl RedisStableMutation for IdempotencyMutation {
    type Output = Vec<Vec<u8>>;

    fn operation(&self) -> &RedisStableOperation {
        &self.operation
    }

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError> {
        let output: Vec<Vec<u8>> = redis::cmd("EVAL")
            .arg(INGRESS_IDEMPOTENCY_SCRIPT)
            .arg(3)
            .arg(&self.producer_key)
            .arg(&self.operations_key)
            .arg(&self.quota_key)
            .arg(&self.args)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        if output.first().map(Vec::as_slice) == Some(b"identity_conflict") {
            Ok(RedisStableMutationOutcome::IdentityConflict)
        } else {
            Ok(RedisStableMutationOutcome::Applied(output))
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let digest: Option<String> = redis::cmd("HGET")
            .arg(&self.operations_key)
            .arg(&self.operation_field)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        Ok(match digest {
            None => RedisStableMutationRecovery::Missing,
            Some(actual) if actual == self.operation_digest => {
                RedisStableMutationRecovery::Matching
            }
            Some(_) => RedisStableMutationRecovery::IdentityConflict,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisIngressIdempotencyError {
    InvalidConfiguration,
    InvalidOperation,
    Unavailable,
    IdentityConflict,
    CapacityFenced,
    LeaseLost,
    InvalidState,
    Accounting,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisIngressIdempotencyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Redis ingress idempotency configuration is invalid",
            Self::InvalidOperation => "Redis ingress idempotency operation is invalid",
            Self::Unavailable => "Redis ingress idempotency role is unavailable",
            Self::IdentityConflict => "Redis ingress idempotency operation identity conflicts",
            Self::CapacityFenced => "Redis ingress idempotency capacity is fenced",
            Self::LeaseLost => "Redis ingress idempotency lease is not owned",
            Self::InvalidState => "Redis ingress idempotency phase is invalid",
            Self::Accounting => "Redis ingress idempotency accounting is inconsistent",
            Self::Durability(_) => "Redis ingress idempotency durability was not proved",
        })
    }
}

impl std::error::Error for RedisIngressIdempotencyError {}

fn decode_effects(
    state: &DecodedState,
) -> Result<RedisIngressEffects, RedisIngressIdempotencyError> {
    if state.effect.is_empty() {
        return Err(RedisIngressIdempotencyError::InvalidState);
    }
    let relay_intents = serde_json::from_slice(&state.intents)
        .map_err(|_| RedisIngressIdempotencyError::InvalidState)?;
    Ok(RedisIngressEffects {
        signal_effect: state.effect.clone(),
        event_results: state.results.clone(),
        relay_intents,
    })
}

fn encode_intents(intents: &[Vec<u8>]) -> Result<Vec<u8>, RedisIngressIdempotencyError> {
    serde_json::to_vec(intents).map_err(|_| RedisIngressIdempotencyError::InvalidOperation)
}

fn parse_uuid(value: &str) -> Result<Uuid, RedisIngressIdempotencyError> {
    Uuid::parse_str(value).map_err(|_| RedisIngressIdempotencyError::InvalidState)
}

fn stable_signal_id(producer_digest: &str, payload_digest: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("{producer_digest}:{payload_digest}").as_bytes(),
    )
}

fn append_framed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn bytes_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn valid_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 127
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_exact_and_forbids_pubsub_and_cross_role_access() {
        let manifest = redis_ingress_idempotency_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::IngressIdempotencyStore);
        assert_eq!(manifest.protocol(), REDIS_INGRESS_IDEMPOTENCY_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_INGRESS_IDEMPOTENCY_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(
            manifest.scripts()[0].name(),
            INGRESS_IDEMPOTENCY_SCRIPT_NAME
        );
        assert_eq!(
            manifest.scripts()[0].sha256(),
            INGRESS_IDEMPOTENCY_SCRIPT_SHA256
        );
        assert_eq!(
            digest_hex(INGRESS_IDEMPOTENCY_SCRIPT.as_bytes()),
            INGRESS_IDEMPOTENCY_SCRIPT_SHA256
        );
        assert!(!manifest.commands().contains(&"PUBLISH"));
        assert!(!manifest.commands().contains(&"XTRIM"));
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);
    }

    #[test]
    fn producer_identity_and_signal_identity_are_stable() {
        let producer = digest_hex(b"producer-42");
        assert_eq!(
            stable_signal_id(&producer, "a"),
            stable_signal_id(&producer, "a")
        );
        assert_ne!(producer, "42-0");
    }
}
