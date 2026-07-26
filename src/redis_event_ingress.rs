use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamAutoClaimOptions, StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands as _,
};
use sha2::{Digest, Sha256};
use tickr_conductor::{
    ingress_idempotency::{
        IngressOutcomeProof as CommonIngressOutcomeProof,
        IngressTerminalOutcome as CommonIngressTerminalOutcome,
    },
    nats_ingress::{EventIngress, EventIngressDelivery},
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
    redis_ingress_idempotency::{RedisIngressOutcomeProof, RedisIngressTerminalOutcome},
    redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisRequiredOperationCanary,
        RedisScriptIdentity,
    },
};

pub const REDIS_EVENT_INGRESS_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.event-ingress.redis-stream", 1);
pub const REDIS_EVENT_INGRESS_GROUP: &str = "tickr-event-ingress-v1";

const DEFAULT_RECLAIM_IDLE: Duration = Duration::from_secs(30);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_PRODUCER_KEY_BYTES: usize = 1024;
const DEFAULT_MAX_DELIVERIES: usize = 4096;
const DEFAULT_SOFT_BYTES: u64 = 48 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 56 * 1024 * 1024;
const DELIVERY_ACCOUNTED_BYTES: u64 = 256;

const REDIS_EVENT_INGRESS_COMMANDS: &[&str] = &[
    "EVAL",
    "HDEL",
    "HGET",
    "HINCRBY",
    "HMGET",
    "HSET",
    "WAITAOF",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XDEL",
    "XGROUP CREATE",
    "XRANGE",
    "XREADGROUP",
];
const EVENT_INGRESS_SCRIPT_NAME: &str = "event-ingress-v1";
const EVENT_INGRESS_SCRIPT_SHA256: &str =
    "7a08c6b4b8c0f7a8ae815fb19b625861c1fb7c81a797e0eee7e684656eb61086";

const EVENT_INGRESS_SCRIPT: &str = r#"local action = ARGV[1]
local group = ARGV[2]
local operation_identity = ARGV[3]
local operation_digest = ARGV[4]
local stream_id = ARGV[5]
local producer_key = ARGV[6]
local producer_digest = ARGV[7]
local payload_digest = ARGV[8]
local payload = ARGV[9]
local units = tonumber(ARGV[10])
local max_deliveries = tonumber(ARGV[11])
local soft_bytes = tonumber(ARGV[12])
local hard_bytes = tonumber(ARGV[13])
local outcome = ARGV[14]
local rejection_digest = ARGV[15]

local function number_field(field)
    return tonumber(redis.call('HGET', KEYS[7], field) or '0')
end

local function state(status, detail)
    return {
        status,
        detail or '',
        tostring(number_field('used_bytes')),
        tostring(number_field('delivery_records')),
        tostring(number_field('pending_deliveries')),
        tostring(number_field('accepted_deliveries')),
        tostring(number_field('rejected_deliveries'))
    }
end

local prior = redis.call('HGET', KEYS[2], operation_identity)
if prior then
    if prior ~= operation_digest then return state('identity_conflict', '') end
    local prior_id = redis.call('HGET', KEYS[3], operation_identity)
    if prior_id and redis.call('HGET', KEYS[6], prior_id) then
        return state('completed', prior_id)
    end
    if prior_id then return state('replayed', prior_id) end
end

if action == 'ensure_group' then
    local created = redis.pcall('XGROUP', 'CREATE', KEYS[1], group, '0', 'MKSTREAM')
    if type(created) == 'table' and created.err then
        if string.find(created.err, 'BUSYGROUP', 1, true) then return state('replayed', '') end
        return redis.error_reply(created.err)
    end
    redis.call('HSET', KEYS[2], operation_identity, operation_digest)
    return state('created', '')
end

if action == 'append' then
    if number_field('delivery_records') + 1 > max_deliveries or
       number_field('used_bytes') + units > hard_bytes then
        return state('fenced', '')
    end
    local entry = redis.call('XADD', KEYS[1], '*',
        'operation_identity', operation_identity,
        'producer_key', producer_key,
        'producer_digest', producer_digest,
        'payload_digest', payload_digest,
        'units', tostring(units),
        'payload', payload)
    redis.call('HSET', KEYS[2], operation_identity, operation_digest)
    redis.call('HSET', KEYS[3], operation_identity, entry)
    redis.call('HSET', KEYS[4], entry, units)
    redis.call('HINCRBY', KEYS[7], 'used_bytes', units)
    redis.call('HINCRBY', KEYS[7], 'delivery_records', 1)
    return state('appended', entry)
end

if action == 'claim' then
    local rows = redis.call('XRANGE', KEYS[1], stream_id, stream_id, 'COUNT', 1)
    if #rows ~= 1 then return state('missing', '') end
    local accepted_units = tonumber(redis.call('HGET', KEYS[4], stream_id) or '-1')
    if accepted_units ~= units then return state('accounting', '') end
    local pending_units = redis.call('HGET', KEYS[5], stream_id)
    if pending_units then
        if tonumber(pending_units) ~= units then return state('accounting', '') end
        redis.call('HSET', KEYS[2], operation_identity, operation_digest)
        return state('claimed', stream_id)
    end
    redis.call('HSET', KEYS[2], operation_identity, operation_digest)
    redis.call('HSET', KEYS[5], stream_id, units)
    redis.call('HINCRBY', KEYS[7], 'pending_deliveries', 1)
    return state('claimed', stream_id)
end

if action == 'complete' then
    if outcome ~= 'accepted' and outcome ~= 'rejected' then
        return redis.error_reply('invalid ingress completion outcome')
    end
    local prior_completion = redis.call('HGET', KEYS[6], stream_id)
    local completion = outcome .. ':' .. producer_digest .. ':' .. payload_digest .. ':' .. rejection_digest
    if prior_completion then
        if prior_completion == completion then
            redis.call('HSET', KEYS[2], operation_identity, operation_digest)
            return state('completed', stream_id)
        end
        return state('conflict', stream_id)
    end
    local rows = redis.call('XRANGE', KEYS[1], stream_id, stream_id, 'COUNT', 1)
    if #rows ~= 1 then return state('missing', '') end
    local accepted_units = tonumber(redis.call('HGET', KEYS[4], stream_id) or '-1')
    if accepted_units ~= units or number_field('used_bytes') < units or
       number_field('delivery_records') < 1 then
        return state('accounting', '')
    end
    local pending_units = redis.call('HGET', KEYS[5], stream_id)
    if not pending_units or tonumber(pending_units) ~= units then return state('accounting', '') end
    local acknowledged = redis.call('XACK', KEYS[1], group, stream_id)
    if acknowledged ~= 1 then return state('accounting', '') end
    local deleted = redis.call('XDEL', KEYS[1], stream_id)
    if deleted ~= 1 then return state('accounting', '') end
    redis.call('HSET', KEYS[2], operation_identity, operation_digest)
    redis.call('HSET', KEYS[6], stream_id, completion)
    redis.call('HDEL', KEYS[4], stream_id)
    redis.call('HDEL', KEYS[5], stream_id)
    redis.call('HINCRBY', KEYS[7], 'used_bytes', -units)
    redis.call('HINCRBY', KEYS[7], 'delivery_records', -1)
    redis.call('HINCRBY', KEYS[7], 'pending_deliveries', -1)
    if outcome == 'accepted' then
        redis.call('HINCRBY', KEYS[7], 'accepted_deliveries', 1)
    else
        redis.call('HINCRBY', KEYS[7], 'rejected_deliveries', 1)
    end
    return state('completed', stream_id)
end

return redis.error_reply('unknown event ingress operation')"#;

#[derive(Clone, Debug)]
pub struct RedisEventIngressConfig {
    pub namespace: String,
    pub consumer_id: String,
    pub reclaim_idle: Duration,
    pub poll_interval: Duration,
    pub max_payload_bytes: NonZeroUsize,
    pub max_producer_key_bytes: NonZeroUsize,
    pub max_deliveries: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisEventIngressConfig {
    pub fn new(namespace: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            consumer_id: consumer_id.into(),
            reclaim_idle: DEFAULT_RECLAIM_IDLE,
            poll_interval: DEFAULT_POLL_INTERVAL,
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("non-zero constant"),
            max_producer_key_bytes: NonZeroUsize::new(DEFAULT_MAX_PRODUCER_KEY_BYTES)
                .expect("non-zero constant"),
            max_deliveries: NonZeroUsize::new(DEFAULT_MAX_DELIVERIES).expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisEventIngressError> {
        let minimum = DELIVERY_ACCOUNTED_BYTES
            .saturating_add(self.max_payload_bytes.get() as u64)
            .saturating_add(self.max_producer_key_bytes.get() as u64);
        if !valid_symbol(&self.namespace)
            || !valid_symbol(&self.consumer_id)
            || self.reclaim_idle.is_zero()
            || self.poll_interval.is_zero()
            || millis(self.reclaim_idle) == 0
            || millis(self.poll_interval) == 0
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes < minimum
        {
            return Err(RedisEventIngressError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisEventIngressKeys {
    stream: String,
    operations: String,
    entries: String,
    units: String,
    pending: String,
    completed: String,
    quota: String,
}

impl RedisEventIngressKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:event-ingress");
        Self {
            stream: format!("{prefix}:stream"),
            operations: format!("{prefix}:operations"),
            entries: format!("{prefix}:entries"),
            units: format!("{prefix}:units"),
            pending: format!("{prefix}:pending"),
            completed: format!("{prefix}:completed"),
            quota: format!("{prefix}:quota"),
        }
    }
}

pub trait RedisEventIngressCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisEventIngressError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisEventIngressError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisEventIngressQuotaState);
}

pub struct MonitoredRedisEventIngressCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisEventIngressCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisEventIngressCapability for MonitoredRedisEventIngressCapability {
    fn guard_admission(&self) -> Result<u64, RedisEventIngressError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisEventIngressError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisEventIngressError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisEventIngressError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisEventIngressError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisEventIngressQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisEventIngressAcceptance {
    Appended,
    ReplayedPending,
    ReplayedCompleted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisEventIngressDelivery {
    pub stream_id: String,
    pub producer_key: String,
    pub payload: Vec<u8>,
    operation_identity: String,
    producer_digest: String,
    payload_digest: String,
    units: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisEventIngressQuotaState {
    pub used_bytes: u64,
    pub delivery_records: u64,
    pub pending_deliveries: u64,
    pub accepted_deliveries: u64,
    pub rejected_deliveries: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub max_deliveries: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisEventIngressQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self.delivery_records,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisEventIngress {
    connection: MultiplexedConnection,
    keys: RedisEventIngressKeys,
    config: Arc<RedisEventIngressConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisEventIngressCapability>,
}

pub(crate) struct RedisEventIngressRoleRegistration {
    connection: MultiplexedConnection,
    config: RedisEventIngressConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisEventIngressRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisEventIngressConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisEventIngressError> {
        config.validate()?;
        Ok(Self {
            connection,
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) async fn build_adapter(
        &self,
        capability: Arc<dyn RedisEventIngressCapability>,
    ) -> Result<RedisEventIngress, RedisEventIngressError> {
        RedisEventIngress::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::EventIngress
            && context.manifest_identity() == &self.manifest_identity
            && RedisEventIngress::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisEventIngressRoleRegistration {
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
            .arg(RedisEventIngressKeys::new(&self.config.namespace).quota)
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
            "tickr:{{{}}}:ingress-idempotency-store:records",
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
impl RedisReconstructionCallback for RedisEventIngressRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let keys = RedisEventIngressKeys::new(&self.config.namespace);
        let mutation = EventIngressMutation::new(
            &keys,
            "ensure_group",
            "ensure-group",
            "",
            "",
            "",
            "",
            Vec::new(),
            0,
            "",
            "",
            &self.config,
        )
        .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        let mut connection = self.connection.clone();
        let committed = match self.durability.execute(&mut connection, &mutation).await {
            Ok(committed) => committed,
            Err(error) if error.failure() == RedisDurabilityFailure::AmbiguousMutation => self
                .durability
                .resolve_ambiguous(&mut connection, &mutation)
                .await
                .map_err(|_| RedisReconstructionFailure::DurabilityUnproved)?,
            Err(_) => return Err(RedisReconstructionFailure::DurabilityUnproved),
        };
        if !matches!(
            committed.into_output().first().map(Vec::as_slice),
            Some(b"created" | b"replayed")
        ) {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&keys.quota)
            .arg(&[
                "used_bytes",
                "delivery_records",
                "pending_deliveries",
                "accepted_deliveries",
                "rejected_deliveries",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        (values.len() == 5)
            .then_some(())
            .ok_or(RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

impl RedisEventIngress {
    pub async fn connect(
        client: redis::Client,
        config: RedisEventIngressConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisEventIngressCapability>,
    ) -> Result<Self, RedisEventIngressError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisEventIngressError::Unavailable)?;
        let adapter = Self::from_connection(connection, config, durability, capability)?;
        adapter.ensure_group().await?;
        Ok(adapter)
    }

    pub(crate) fn from_connection(
        connection: MultiplexedConnection,
        config: RedisEventIngressConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisEventIngressCapability>,
    ) -> Result<Self, RedisEventIngressError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisEventIngressKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
        })
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_event_ingress_operation_manifest()
    }

    pub async fn append(
        &self,
        transport_operation_identity: &str,
        producer_key: &str,
        payload: Vec<u8>,
    ) -> Result<(RedisEventIngressAcceptance, String), RedisEventIngressError> {
        if !valid_identity(transport_operation_identity)
            || producer_key.is_empty()
            || producer_key.len() > self.config.max_producer_key_bytes.get()
            || payload.is_empty()
            || payload.len() > self.config.max_payload_bytes.get()
        {
            return Err(RedisEventIngressError::InvalidOperation);
        }
        let generation = self.capability.guard_admission()?;
        let producer_digest = digest_hex(producer_key.as_bytes());
        let payload_digest = digest_hex(&payload);
        let units = DELIVERY_ACCOUNTED_BYTES
            .checked_add(producer_key.len() as u64)
            .and_then(|value| value.checked_add(payload.len() as u64))
            .ok_or(RedisEventIngressError::InvalidOperation)?;
        let append_identity = format!("append:{transport_operation_identity}");
        let mutation = EventIngressMutation::new(
            &self.keys,
            "append",
            &append_identity,
            "",
            producer_key,
            &producer_digest,
            &payload_digest,
            payload,
            units,
            "",
            "",
            &self.config,
        )?;
        let output = self.commit(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "appended" => Ok((RedisEventIngressAcceptance::Appended, state.detail)),
            "replayed" => Ok((RedisEventIngressAcceptance::ReplayedPending, state.detail)),
            "completed" => Ok((RedisEventIngressAcceptance::ReplayedCompleted, state.detail)),
            "fenced" => Err(RedisEventIngressError::CapacityFenced),
            "accounting" | "missing" => Err(self.accounting_failure()),
            _ => Err(RedisEventIngressError::InvalidState),
        }
    }

    pub async fn next_delivery(
        &self,
    ) -> Result<Option<RedisEventIngressDelivery>, RedisEventIngressError> {
        let Some(entry) = self.next_entry().await? else {
            return Ok(None);
        };
        let delivery = self.decode_delivery(entry)?;
        let generation = self.capability.guard_admission()?;
        let mutation = EventIngressMutation::new(
            &self.keys,
            "claim",
            &format!("claim:{}", delivery.stream_id),
            &delivery.stream_id,
            &delivery.producer_key,
            &delivery.producer_digest,
            &delivery.payload_digest,
            delivery.payload.clone(),
            delivery.units,
            "",
            "",
            &self.config,
        )?;
        let output = self.commit(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "claimed" => Ok(Some(delivery)),
            "accounting" | "missing" => Err(self.accounting_failure()),
            _ => Err(RedisEventIngressError::InvalidState),
        }
    }

    pub async fn complete(
        &self,
        delivery: &RedisEventIngressDelivery,
        proof: &RedisIngressOutcomeProof,
    ) -> Result<(), RedisEventIngressError> {
        self.complete_matching(
            delivery,
            proof.producer_digest(),
            proof.payload_digest(),
            match proof.outcome() {
                RedisIngressTerminalOutcome::Accepted => "accepted",
                RedisIngressTerminalOutcome::Rejected => "rejected",
            },
        )
        .await
    }

    async fn complete_common(
        &self,
        delivery: &RedisEventIngressDelivery,
        proof: &CommonIngressOutcomeProof,
    ) -> Result<(), RedisEventIngressError> {
        self.complete_matching(
            delivery,
            proof.producer_digest(),
            proof.payload_digest(),
            match proof.outcome() {
                CommonIngressTerminalOutcome::Accepted => "accepted",
                CommonIngressTerminalOutcome::Rejected => "rejected",
            },
        )
        .await
    }

    async fn complete_matching(
        &self,
        delivery: &RedisEventIngressDelivery,
        producer_digest: &str,
        payload_digest: &str,
        outcome: &str,
    ) -> Result<(), RedisEventIngressError> {
        if delivery.producer_digest != producer_digest || delivery.payload_digest != payload_digest
        {
            return Err(RedisEventIngressError::OutcomeProofMismatch);
        }
        let generation = self.capability.guard_admission()?;
        let mutation = EventIngressMutation::new(
            &self.keys,
            "complete",
            &format!("complete:{}:{outcome}", delivery.stream_id),
            &delivery.stream_id,
            &delivery.producer_key,
            &delivery.producer_digest,
            &delivery.payload_digest,
            delivery.payload.clone(),
            delivery.units,
            outcome,
            "",
            &self.config,
        )?;
        let output = self.commit(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "completed" => Ok(()),
            "conflict" => Err(RedisEventIngressError::OutcomeProofMismatch),
            "accounting" | "missing" => Err(self.accounting_failure()),
            _ => Err(RedisEventIngressError::InvalidState),
        }
    }

    pub async fn complete_permanent_rejection(
        &self,
        delivery: &RedisEventIngressDelivery,
        reason: &str,
    ) -> Result<(), RedisEventIngressError> {
        if reason.is_empty() || reason.len() > 4096 {
            return Err(RedisEventIngressError::InvalidOperation);
        }
        let generation = self.capability.guard_admission()?;
        let outcome = "rejected";
        let rejection_digest = digest_hex(reason.as_bytes());
        let mutation = EventIngressMutation::new(
            &self.keys,
            "complete",
            &format!(
                "complete:{}:{outcome}:{rejection_digest}",
                delivery.stream_id
            ),
            &delivery.stream_id,
            &delivery.producer_key,
            &delivery.producer_digest,
            &delivery.payload_digest,
            delivery.payload.clone(),
            delivery.units,
            outcome,
            &rejection_digest,
            &self.config,
        )?;
        let output = self.commit(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state.quota);
        self.capability.guard_acknowledgement(generation)?;
        match state.status.as_str() {
            "completed" => Ok(()),
            "conflict" => Err(RedisEventIngressError::OutcomeProofMismatch),
            "accounting" | "missing" => Err(self.accounting_failure()),
            _ => Err(RedisEventIngressError::InvalidState),
        }
    }

    pub async fn quota_state(&self) -> Result<RedisEventIngressQuotaState, RedisEventIngressError> {
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "delivery_records",
                "pending_deliveries",
                "accepted_deliveries",
                "rejected_deliveries",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.len() != 5 {
            return Err(self.accounting_failure());
        }
        Ok(self.quota_from(
            values[0].unwrap_or(0),
            values[1].unwrap_or(0),
            values[2].unwrap_or(0),
            values[3].unwrap_or(0),
            values[4].unwrap_or(0),
        ))
    }

    async fn ensure_group(&self) -> Result<(), RedisEventIngressError> {
        let generation = self.capability.guard_admission()?;
        let mutation = EventIngressMutation::new(
            &self.keys,
            "ensure_group",
            "ensure-group",
            "",
            "",
            "",
            "",
            Vec::new(),
            0,
            "",
            "",
            &self.config,
        )?;
        self.commit(&mutation).await?;
        self.capability.guard_acknowledgement(generation)
    }

    async fn next_entry(&self) -> Result<Option<StreamId>, RedisEventIngressError> {
        let mut connection = self.connection.clone();
        let claimed: redis::streams::StreamAutoClaimReply = connection
            .xautoclaim_options(
                &self.keys.stream,
                REDIS_EVENT_INGRESS_GROUP,
                &self.config.consumer_id,
                millis(self.config.reclaim_idle),
                "0-0",
                StreamAutoClaimOptions::default().count(1),
            )
            .await
            .map_err(|error| self.redis_error(error))?;
        if let Some(entry) = claimed.claimed.into_iter().next() {
            return Ok(Some(entry));
        }
        let options = StreamReadOptions::default()
            .group(REDIS_EVENT_INGRESS_GROUP, &self.config.consumer_id)
            .count(1)
            .block(usize::try_from(millis(self.config.poll_interval)).unwrap_or(usize::MAX));
        let reply: Option<StreamReadReply> = connection
            .xread_options(&[&self.keys.stream], &[">"], &options)
            .await
            .map_err(|error| self.redis_error(error))?;
        Ok(reply
            .and_then(|reply| reply.keys.into_iter().next())
            .and_then(|stream| stream.ids.into_iter().next()))
    }

    fn decode_delivery(
        &self,
        entry: StreamId,
    ) -> Result<RedisEventIngressDelivery, RedisEventIngressError> {
        let operation_identity = entry
            .get::<String>("operation_identity")
            .filter(|value| valid_identity(value))
            .ok_or_else(|| self.missing_identity())?;
        let producer_key = entry
            .get::<String>("producer_key")
            .filter(|value| {
                !value.is_empty() && value.len() <= self.config.max_producer_key_bytes.get()
            })
            .ok_or_else(|| self.missing_identity())?;
        let producer_digest = entry
            .get::<String>("producer_digest")
            .filter(|value| valid_digest(value))
            .ok_or_else(|| self.missing_identity())?;
        let payload_digest = entry
            .get::<String>("payload_digest")
            .filter(|value| valid_digest(value))
            .ok_or_else(|| self.missing_identity())?;
        let units = entry
            .get::<u64>("units")
            .filter(|value| *value >= DELIVERY_ACCOUNTED_BYTES)
            .ok_or_else(|| self.missing_identity())?;
        let payload = entry
            .get::<Vec<u8>>("payload")
            .filter(|value| !value.is_empty() && value.len() <= self.config.max_payload_bytes.get())
            .ok_or_else(|| self.missing_identity())?;
        if producer_digest != digest_hex(producer_key.as_bytes())
            || payload_digest != digest_hex(&payload)
            || units
                != DELIVERY_ACCOUNTED_BYTES
                    .saturating_add(producer_key.len() as u64)
                    .saturating_add(payload.len() as u64)
        {
            return Err(self.missing_identity());
        }
        Ok(RedisEventIngressDelivery {
            stream_id: entry.id,
            producer_key,
            payload,
            operation_identity,
            producer_digest,
            payload_digest,
            units,
        })
    }

    async fn commit(
        &self,
        mutation: &EventIngressMutation,
    ) -> Result<Vec<Vec<u8>>, RedisEventIngressError> {
        let mut connection = self.connection.clone();
        self.durability
            .execute(&mut connection, mutation)
            .await
            .map(|committed| committed.into_output())
            .map_err(|error| self.durability_error(error))
    }

    fn decode_state(&self, output: &[Vec<u8>]) -> Result<DecodedState, RedisEventIngressError> {
        if output.len() != 7 {
            return Err(self.accounting_failure());
        }
        let text = |index: usize| {
            String::from_utf8(output[index].clone())
                .map_err(|_| RedisEventIngressError::InvalidState)
        };
        let number = |index: usize| {
            std::str::from_utf8(&output[index])
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| self.accounting_failure())
        };
        Ok(DecodedState {
            status: text(0)?,
            detail: text(1)?,
            quota: self.quota_from(number(2)?, number(3)?, number(4)?, number(5)?, number(6)?),
        })
    }

    fn quota_from(
        &self,
        used_bytes: u64,
        delivery_records: u64,
        pending_deliveries: u64,
        accepted_deliveries: u64,
        rejected_deliveries: u64,
    ) -> RedisEventIngressQuotaState {
        let pressure = if used_bytes >= self.config.hard_limit_bytes
            || delivery_records >= self.config.max_deliveries.get() as u64
        {
            RedisQuotaPressure::HardLimit
        } else if used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisEventIngressQuotaState {
            used_bytes,
            delivery_records,
            pending_deliveries,
            accepted_deliveries,
            rejected_deliveries,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            max_deliveries: self.config.max_deliveries.get() as u64,
            pressure,
        }
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisEventIngressError {
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
        RedisEventIngressError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisEventIngressError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisEventIngressError::IdentityConflict
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
        RedisEventIngressError::Durability(error.failure())
    }

    fn accounting_failure(&self) -> RedisEventIngressError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisEventIngressError::Accounting
    }

    fn missing_identity(&self) -> RedisEventIngressError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
        RedisEventIngressError::Accounting
    }
}

struct RedisSelectedEventIngressDelivery {
    ingress: RedisEventIngress,
    delivery: RedisEventIngressDelivery,
}

#[async_trait]
impl EventIngressDelivery for RedisSelectedEventIngressDelivery {
    fn transport_identity(&self) -> &str {
        &self.delivery.stream_id
    }

    fn producer_key(&self) -> Option<&str> {
        Some(&self.delivery.producer_key)
    }

    fn payload(&self) -> &[u8] {
        &self.delivery.payload
    }

    async fn complete(
        self: Box<Self>,
        producer_key: &str,
        proof: &CommonIngressOutcomeProof,
    ) -> anyhow::Result<()> {
        if self.delivery.producer_key != producer_key {
            return Err(anyhow::Error::new(
                RedisEventIngressError::OutcomeProofMismatch,
            ));
        }
        self.ingress
            .complete_common(&self.delivery, proof)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn reject_malformed(self: Box<Self>, reason: String) -> anyhow::Result<()> {
        self.ingress
            .complete_permanent_rejection(&self.delivery, &reason)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn leave_pending(self: Box<Self>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl EventIngress for RedisEventIngress {
    async fn next_delivery(&self) -> anyhow::Result<Option<Box<dyn EventIngressDelivery>>> {
        RedisEventIngress::next_delivery(self)
            .await
            .map(|delivery| {
                delivery.map(|delivery| {
                    Box::new(RedisSelectedEventIngressDelivery {
                        ingress: self.clone(),
                        delivery,
                    }) as Box<dyn EventIngressDelivery>
                })
            })
            .map_err(anyhow::Error::new)
    }
}

pub fn redis_event_ingress_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(EVENT_INGRESS_SCRIPT_NAME, EVENT_INGRESS_SCRIPT_SHA256)?;
    RedisOperationManifest::new(
        CoordinationRole::EventIngress,
        REDIS_EVENT_INGRESS_PROTOCOL,
        REDIS_EVENT_INGRESS_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:event-ingress:completed",
            "tickr:{namespace}:event-ingress:entries",
            "tickr:{namespace}:event-ingress:operations",
            "tickr:{namespace}:event-ingress:pending",
            "tickr:{namespace}:event-ingress:quota",
            "tickr:{namespace}:event-ingress:stream",
            "tickr:{namespace}:event-ingress:units",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            RedisNamespacePattern::key("tickr:{namespace}:event-ingress:stream"),
        )],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::IngressIdempotencyStore,
            ),
            RedisForbiddenOperation::administrative("FLUSHALL"),
        ],
    )
}

struct DecodedState {
    status: String,
    detail: String,
    quota: RedisEventIngressQuotaState,
}

struct EventIngressMutation {
    operation: RedisStableOperation,
    keys: RedisEventIngressKeys,
    args: Vec<Vec<u8>>,
    operation_identity: String,
    operation_digest: String,
}

#[allow(clippy::too_many_arguments)]
impl EventIngressMutation {
    fn new(
        keys: &RedisEventIngressKeys,
        action: &str,
        operation_identity: &str,
        stream_id: &str,
        producer_key: &str,
        producer_digest: &str,
        payload_digest: &str,
        payload: Vec<u8>,
        units: u64,
        outcome: &str,
        rejection_digest: &str,
        config: &RedisEventIngressConfig,
    ) -> Result<Self, RedisEventIngressError> {
        let mut fingerprint = Vec::new();
        append_framed(&mut fingerprint, action.as_bytes());
        append_framed(&mut fingerprint, stream_id.as_bytes());
        append_framed(&mut fingerprint, producer_digest.as_bytes());
        append_framed(&mut fingerprint, payload_digest.as_bytes());
        append_framed(&mut fingerprint, &payload);
        append_framed(&mut fingerprint, outcome.as_bytes());
        append_framed(&mut fingerprint, rejection_digest.as_bytes());
        let operation_digest = digest_hex(&fingerprint);
        let operation = RedisStableOperation::new(
            format!("{}:{operation_identity}", keys.operations),
            &fingerprint,
        )
        .map_err(|_| RedisEventIngressError::InvalidOperation)?;
        Ok(Self {
            operation,
            keys: keys.clone(),
            args: vec![
                action.as_bytes().to_vec(),
                REDIS_EVENT_INGRESS_GROUP.as_bytes().to_vec(),
                operation_identity.as_bytes().to_vec(),
                operation_digest.as_bytes().to_vec(),
                stream_id.as_bytes().to_vec(),
                producer_key.as_bytes().to_vec(),
                producer_digest.as_bytes().to_vec(),
                payload_digest.as_bytes().to_vec(),
                payload,
                units.to_string().into_bytes(),
                config.max_deliveries.get().to_string().into_bytes(),
                config.soft_limit_bytes.to_string().into_bytes(),
                config.hard_limit_bytes.to_string().into_bytes(),
                outcome.as_bytes().to_vec(),
                rejection_digest.as_bytes().to_vec(),
            ],
            operation_identity: operation_identity.to_owned(),
            operation_digest,
        })
    }
}

#[async_trait]
impl RedisStableMutation for EventIngressMutation {
    type Output = Vec<Vec<u8>>;

    fn operation(&self) -> &RedisStableOperation {
        &self.operation
    }

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError> {
        let output: Vec<Vec<u8>> = redis::cmd("EVAL")
            .arg(EVENT_INGRESS_SCRIPT)
            .arg(7)
            .arg(&self.keys.stream)
            .arg(&self.keys.operations)
            .arg(&self.keys.entries)
            .arg(&self.keys.units)
            .arg(&self.keys.pending)
            .arg(&self.keys.completed)
            .arg(&self.keys.quota)
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
            .arg(&self.keys.operations)
            .arg(&self.operation_identity)
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
pub enum RedisEventIngressError {
    InvalidConfiguration,
    InvalidOperation,
    Unavailable,
    IdentityConflict,
    CapacityFenced,
    OutcomeProofMismatch,
    InvalidState,
    Accounting,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisEventIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Redis Event ingress configuration is invalid",
            Self::InvalidOperation => "Redis Event ingress operation is invalid",
            Self::Unavailable => "Redis Event ingress role is unavailable",
            Self::IdentityConflict => "Redis Event ingress operation identity conflicts",
            Self::CapacityFenced => "Redis Event ingress capacity is fenced",
            Self::OutcomeProofMismatch => {
                "Redis Event ingress outcome proof does not match delivery"
            }
            Self::InvalidState => "Redis Event ingress delivery state is invalid",
            Self::Accounting => "Redis Event ingress accounting is inconsistent",
            Self::Durability(_) => "Redis Event ingress durability was not proved",
        })
    }
}

impl std::error::Error for RedisEventIngressError {}

fn append_framed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
    fn manifest_is_exact_and_rejects_unregistered_pubsub_or_trim_operations() {
        let manifest = redis_event_ingress_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::EventIngress);
        assert_eq!(manifest.protocol(), REDIS_EVENT_INGRESS_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_EVENT_INGRESS_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), EVENT_INGRESS_SCRIPT_NAME);
        assert_eq!(manifest.scripts()[0].sha256(), EVENT_INGRESS_SCRIPT_SHA256);
        assert_eq!(
            digest_hex(EVENT_INGRESS_SCRIPT.as_bytes()),
            EVENT_INGRESS_SCRIPT_SHA256
        );
        assert!(!manifest.commands().contains(&"PUBLISH"));
        assert!(!manifest.commands().contains(&"SUBSCRIBE"));
        assert!(!manifest.commands().contains(&"XTRIM"));
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);
    }

    #[test]
    fn delivery_and_producer_identities_are_distinct() {
        let stream_id = "1700000000000-0";
        let producer_digest = digest_hex(b"producer-key");
        assert_ne!(stream_id, producer_digest);
        assert!(valid_identity("transport-attempt:42"));
        assert!(!valid_identity("producer key is payload data"));
    }
}
