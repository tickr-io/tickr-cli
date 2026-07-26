use std::{fmt, future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamAutoClaimOptions, StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands as _,
};
use sha2::{Digest, Sha256};
use tickr_proto::coord::{TaskEventConsumer, TaskEventDelivery, TaskEventFuture, TaskEventWriter};

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

pub const REDIS_TASK_EVENTS_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.task-events.redis-stream", 1);
pub const REDIS_TASK_EVENTS_GROUP: &str = "tickr-task-events-v1";

const DEFAULT_RECLAIM_IDLE: Duration = Duration::from_millis(100);
const DEFAULT_POLL: Duration = Duration::from_millis(25);
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_RECORDS: usize = 4096;
const DEFAULT_SOFT_BYTES: u64 = 48 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 56 * 1024 * 1024;
const DEFAULT_COMPLETION_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const RECORD_ACCOUNTED_BYTES: u64 = 192;

const REDIS_TASK_EVENTS_COMMANDS: &[&str] = &[
    "EVAL",
    "GET",
    "HDEL",
    "HGET",
    "HINCRBY",
    "HMGET",
    "HSET",
    "SET",
    "WAITAOF",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XDEL",
    "XGROUP CREATE",
    "XRANGE",
    "XREADGROUP",
];

const TASK_EVENTS_SCRIPT_NAME: &str = "task-events-v1";
const TASK_EVENTS_SCRIPT_SHA256: &str =
    "ed4b6b9adc921cd141895b42be91831e169d96be9c7286304b332acfc7bb9d4a";

const TASK_EVENTS_SCRIPT: &str = r#"local operation = ARGV[1]
local group = ARGV[2]
local identity = ARGV[3]
local digest = ARGV[4]
local payload = ARGV[5]
local units = tonumber(ARGV[6])
local max_records = tonumber(ARGV[7])
local hard_bytes = tonumber(ARGV[8])
local stream_id = ARGV[9]
local completion_retention_ms = ARGV[10]

local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function state(status, detail)
    return {
        status,
        detail or '',
        tostring(number_field(KEYS[6], 'used_bytes')),
        tostring(number_field(KEYS[6], 'accepted_records')),
        tostring(number_field(KEYS[6], 'pending_deliveries'))
    }
end

local function entry_exists(id)
    local rows = redis.call('XRANGE', KEYS[1], id, id, 'COUNT', 1)
    return #rows == 1
end

if operation == 'ensure_group' then
    local created = redis.pcall('XGROUP', 'CREATE', KEYS[1], group, '0', 'MKSTREAM')
    if type(created) == 'table' and created.err then
        if string.find(created.err, 'BUSYGROUP', 1, true) then
            return state('replayed', '')
        end
        return redis.error_reply(created.err)
    end
    return state('created', '')
end

if operation == 'append' then
    local completed = redis.call('GET', KEYS[7])
    if completed then
        if completed == digest then
            return state('completed', '')
        end
        return state('conflict', '')
    end

    local prior = redis.call('HGET', KEYS[2], identity)
    if prior then
        if prior ~= digest then
            return state('conflict', '')
        end
        local prior_id = redis.call('HGET', KEYS[3], identity)
        local prior_units = tonumber(redis.call('HGET', KEYS[4], identity) or '-1')
        if not prior_id or prior_units ~= units then
            return state('accounting', '')
        end
        if not entry_exists(prior_id) then
            return state('trimmed', prior_id)
        end
        return state('replayed', prior_id)
    end

    local used = number_field(KEYS[6], 'used_bytes')
    local accepted = number_field(KEYS[6], 'accepted_records')
    if accepted >= max_records or units > hard_bytes or used > hard_bytes - units then
        return state('fenced', '')
    end

    local id = redis.call(
        'XADD', KEYS[1], '*',
        'identity', identity,
        'digest', digest,
        'units', tostring(units),
        'payload', payload
    )
    redis.call('HSET', KEYS[2], identity, digest)
    redis.call('HSET', KEYS[3], identity, id)
    redis.call('HSET', KEYS[4], identity, tostring(units))
    redis.call('HINCRBY', KEYS[6], 'used_bytes', units)
    redis.call('HINCRBY', KEYS[6], 'accepted_records', 1)
    return state('appended', id)
end

if operation == 'claim' then
    local prior = redis.call('HGET', KEYS[5], stream_id)
    if prior then
        if prior == identity then
            return state('replayed', stream_id)
        end
        return state('accounting', stream_id)
    end
    local stored_digest = redis.call('HGET', KEYS[2], identity)
    local stored_id = redis.call('HGET', KEYS[3], identity)
    local stored_units = tonumber(redis.call('HGET', KEYS[4], identity) or '-1')
    if stored_digest ~= digest or stored_id ~= stream_id or stored_units ~= units then
        return state('missing', stream_id)
    end
    if not entry_exists(stream_id) then
        return state('trimmed', stream_id)
    end
    redis.call('HSET', KEYS[5], stream_id, identity)
    redis.call('HINCRBY', KEYS[6], 'pending_deliveries', 1)
    return state('claimed', stream_id)
end

if operation == 'complete' then
    local stored_digest = redis.call('HGET', KEYS[2], identity)
    if not stored_digest then
        local completed = redis.call('GET', KEYS[7])
        if completed == digest then
            return state('replayed', stream_id)
        end
        if completed then
            return state('conflict', stream_id)
        end
        return state('missing', stream_id)
    end
    local stored_id = redis.call('HGET', KEYS[3], identity)
    local stored_units = tonumber(redis.call('HGET', KEYS[4], identity) or '-1')
    local pending_identity = redis.call('HGET', KEYS[5], stream_id)
    if stored_digest ~= digest or stored_id ~= stream_id or stored_units ~= units
        or pending_identity ~= identity then
        return state('accounting', stream_id)
    end
    if not entry_exists(stream_id) then
        return state('trimmed', stream_id)
    end

    redis.call('XACK', KEYS[1], group, stream_id)
    redis.call('XDEL', KEYS[1], stream_id)
    redis.call('HDEL', KEYS[2], identity)
    redis.call('HDEL', KEYS[3], identity)
    redis.call('HDEL', KEYS[4], identity)
    redis.call('HDEL', KEYS[5], stream_id)
    redis.call('HINCRBY', KEYS[6], 'used_bytes', -units)
    redis.call('HINCRBY', KEYS[6], 'accepted_records', -1)
    redis.call('HINCRBY', KEYS[6], 'pending_deliveries', -1)
    redis.call('SET', KEYS[7], digest, 'PX', completion_retention_ms)

    if number_field(KEYS[6], 'used_bytes') < 0
        or number_field(KEYS[6], 'accepted_records') < 0
        or number_field(KEYS[6], 'pending_deliveries') < 0 then
        return state('accounting', stream_id)
    end
    return state('completed', stream_id)
end

return redis.error_reply('unknown task-events operation')"#;

#[derive(Clone, Debug)]
pub struct RedisTaskEventsConfig {
    pub namespace: String,
    pub consumer_id: String,
    pub reclaim_idle: Duration,
    pub poll_interval: Duration,
    pub max_payload_bytes: NonZeroUsize,
    pub max_records: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub completion_retention: Duration,
}

impl RedisTaskEventsConfig {
    pub fn new(namespace: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            consumer_id: consumer_id.into(),
            reclaim_idle: DEFAULT_RECLAIM_IDLE,
            poll_interval: DEFAULT_POLL,
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("non-zero constant"),
            max_records: NonZeroUsize::new(DEFAULT_MAX_RECORDS).expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_BYTES,
            completion_retention: DEFAULT_COMPLETION_RETENTION,
        }
    }

    fn validate(&self) -> Result<(), RedisTaskEventError> {
        let valid_symbol = |value: &str| {
            !value.is_empty()
                && value.len() <= 127
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        };
        let minimum_record =
            RECORD_ACCOUNTED_BYTES.saturating_add(self.max_payload_bytes.get() as u64);
        if !valid_symbol(&self.namespace)
            || !valid_symbol(&self.consumer_id)
            || self.reclaim_idle.is_zero()
            || self.poll_interval.is_zero()
            || self.completion_retention.is_zero()
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes < minimum_record
            || millis(self.reclaim_idle) == 0
            || millis(self.poll_interval) == 0
            || millis(self.completion_retention) == 0
        {
            return Err(RedisTaskEventError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisTaskEventKeys {
    stream: String,
    digests: String,
    entries: String,
    units: String,
    pending: String,
    quota: String,
    completed_prefix: String,
    operations_prefix: String,
}

impl RedisTaskEventKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:task-events");
        Self {
            stream: format!("{prefix}:stream"),
            digests: format!("{prefix}:digests"),
            entries: format!("{prefix}:entries"),
            units: format!("{prefix}:units"),
            pending: format!("{prefix}:pending"),
            quota: format!("{prefix}:quota"),
            completed_prefix: format!("{prefix}:completed:"),
            operations_prefix: format!("{prefix}:operations:"),
        }
    }

    fn completed(&self, identity: &str) -> String {
        format!(
            "{}{}",
            self.completed_prefix,
            digest_hex(identity.as_bytes())
        )
    }

    fn operation(&self, identity: &str, phase: &str) -> String {
        format!(
            "{}{}:{phase}",
            self.operations_prefix,
            digest_hex(identity.as_bytes())
        )
    }
}

pub trait RedisTaskEventCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisTaskEventError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskEventError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisTaskEventQuotaState);
}

pub struct MonitoredRedisTaskEventCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisTaskEventCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisTaskEventCapability for MonitoredRedisTaskEventCapability {
    fn guard_admission(&self) -> Result<u64, RedisTaskEventError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisTaskEventError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisTaskEventError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisTaskEventError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisTaskEventError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisTaskEventQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisTaskEventAcceptance {
    Appended,
    ReplayedPending,
    ReplayedCompleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisTaskEventForwardOutcome {
    Idle,
    Forwarded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisTaskEventQuotaState {
    pub used_bytes: u64,
    pub accepted_records: u64,
    pub pending_deliveries: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub max_records: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisTaskEventQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self.accepted_records,
            pressure: self.pressure,
        }
    }
}

#[derive(Clone)]
pub struct RedisTaskEvents {
    connection: MultiplexedConnection,
    keys: RedisTaskEventKeys,
    config: Arc<RedisTaskEventsConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisTaskEventCapability>,
}

pub struct RedisTaskEventDelivery {
    adapter: RedisTaskEvents,
    delivery: Delivery,
    generation: u64,
}

impl RedisTaskEventDelivery {
    pub fn payload(&self) -> &[u8] {
        &self.delivery.payload
    }

    pub async fn complete(self) -> Result<(), RedisTaskEventError> {
        self.adapter
            .complete_delivery(self.delivery, self.generation)
            .await
    }
}

impl RedisTaskEvents {
    pub async fn connect(
        client: redis::Client,
        config: RedisTaskEventsConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisTaskEventCapability>,
    ) -> Result<Self, RedisTaskEventError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisTaskEventError::Unavailable)?;
        let adapter = Self::from_connection(connection, config, durability, capability)?;
        adapter.ensure_group().await?;
        Ok(adapter)
    }

    pub(crate) fn from_connection(
        connection: MultiplexedConnection,
        config: RedisTaskEventsConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisTaskEventCapability>,
    ) -> Result<Self, RedisTaskEventError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisTaskEventKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
        })
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_task_events_operation_manifest()
    }

    pub async fn append(
        &self,
        identity: &str,
        encoded_task_event: Vec<u8>,
    ) -> Result<RedisTaskEventAcceptance, RedisTaskEventError> {
        if !valid_identity(identity)
            || encoded_task_event.is_empty()
            || encoded_task_event.len() > self.config.max_payload_bytes.get()
        {
            return Err(RedisTaskEventError::InvalidOperation);
        }
        let generation = self.capability.guard_admission()?;
        let digest = digest_hex(&encoded_task_event);
        let units = RECORD_ACCOUNTED_BYTES
            .checked_add(encoded_task_event.len() as u64)
            .ok_or(RedisTaskEventError::InvalidOperation)?;
        let mutation = ScriptMutation::append(
            &self.keys,
            identity,
            digest,
            encoded_task_event,
            units,
            &self.config,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let state = self.decode_state(&output)?;
        let status = output.first().map(Vec::as_slice);
        self.capability.report_quota(state);
        self.capability.guard_acknowledgement(generation)?;
        match status {
            Some(b"appended") => Ok(RedisTaskEventAcceptance::Appended),
            Some(b"replayed") => Ok(RedisTaskEventAcceptance::ReplayedPending),
            Some(b"completed") => Ok(RedisTaskEventAcceptance::ReplayedCompleted),
            Some(b"fenced") => Err(RedisTaskEventError::CapacityFenced),
            Some(b"conflict") => Err(RedisTaskEventError::IdentityConflict),
            Some(b"trimmed") => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::UnexpectedTrim);
                Err(RedisTaskEventError::Accounting)
            }
            Some(b"accounting") => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::Accounting);
                Err(RedisTaskEventError::Accounting)
            }
            _ => Err(RedisTaskEventError::Accounting),
        }
    }

    pub async fn forward_one<F, Fut>(
        &self,
        forward: F,
    ) -> Result<RedisTaskEventForwardOutcome, RedisTaskEventError>
    where
        F: FnOnce(Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), ()>>,
    {
        let Some(delivery) = self.next_delivery().await? else {
            return Ok(RedisTaskEventForwardOutcome::Idle);
        };
        forward(delivery.payload().to_vec())
            .await
            .map_err(|()| RedisTaskEventError::ForwardingUnavailable)?;
        delivery.complete().await?;
        Ok(RedisTaskEventForwardOutcome::Forwarded)
    }

    pub async fn next_delivery(
        &self,
    ) -> Result<Option<RedisTaskEventDelivery>, RedisTaskEventError> {
        let Some(entry) = self.next_entry().await? else {
            return Ok(None);
        };
        let delivery = self.decode_delivery(entry)?;
        let claim = ScriptMutation::claim(&self.keys, &delivery, &self.config)?;
        let claim_output = self.commit_mutation(&claim).await?;
        let claim_state = self.decode_state(&claim_output)?;
        self.capability.report_quota(claim_state);
        match claim_output.first().map(Vec::as_slice) {
            Some(b"claimed" | b"replayed") => {}
            Some(b"trimmed") => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::UnexpectedTrim);
                return Err(RedisTaskEventError::Accounting);
            }
            Some(b"missing") => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
                return Err(RedisTaskEventError::Accounting);
            }
            _ => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::Accounting);
                return Err(RedisTaskEventError::Accounting);
            }
        }
        let generation = self.capability.guard_admission()?;
        Ok(Some(RedisTaskEventDelivery {
            adapter: self.clone(),
            delivery,
            generation,
        }))
    }

    async fn complete_delivery(
        &self,
        delivery: Delivery,
        generation: u64,
    ) -> Result<(), RedisTaskEventError> {
        self.capability.guard_acknowledgement(generation)?;
        let completion = ScriptMutation::complete(
            &self.keys,
            &delivery.stream_id,
            &delivery.identity,
            delivery.digest,
            delivery.units,
            &self.config,
        )?;
        let output = self.commit_mutation(&completion).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state);
        self.capability.guard_acknowledgement(generation)?;
        match output.first().map(Vec::as_slice) {
            Some(b"completed" | b"replayed") => Ok(()),
            Some(b"trimmed") => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::UnexpectedTrim);
                Err(RedisTaskEventError::Accounting)
            }
            Some(b"missing") => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
                Err(RedisTaskEventError::Accounting)
            }
            _ => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::Accounting);
                Err(RedisTaskEventError::Accounting)
            }
        }
    }

    pub async fn quota_state(&self) -> Result<RedisTaskEventQuotaState, RedisTaskEventError> {
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&["used_bytes", "accepted_records", "pending_deliveries"])
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.len() != 3 {
            return Err(RedisTaskEventError::Accounting);
        }
        Ok(self.quota_state_from(
            values[0].unwrap_or(0),
            values[1].unwrap_or(0),
            values[2].unwrap_or(0),
        ))
    }

    async fn next_entry(&self) -> Result<Option<StreamId>, RedisTaskEventError> {
        let mut connection = self.connection.clone();
        let claimed: redis::streams::StreamAutoClaimReply = connection
            .xautoclaim_options(
                &self.keys.stream,
                REDIS_TASK_EVENTS_GROUP,
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
            .group(REDIS_TASK_EVENTS_GROUP, &self.config.consumer_id)
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

    fn decode_delivery(&self, entry: StreamId) -> Result<Delivery, RedisTaskEventError> {
        let identity = entry
            .get::<String>("identity")
            .filter(|identity| valid_identity(identity))
            .ok_or_else(|| self.missing_identity())?;
        let digest = entry
            .get::<String>("digest")
            .filter(|digest| valid_digest(digest))
            .ok_or_else(|| self.missing_identity())?;
        let units = entry
            .get::<u64>("units")
            .filter(|units| *units >= RECORD_ACCOUNTED_BYTES)
            .ok_or_else(|| self.missing_identity())?;
        let payload = entry
            .get::<Vec<u8>>("payload")
            .filter(|payload| {
                !payload.is_empty() && payload.len() <= self.config.max_payload_bytes.get()
            })
            .ok_or_else(|| self.missing_identity())?;
        if digest_hex(&payload) != digest
            || units != RECORD_ACCOUNTED_BYTES.saturating_add(payload.len() as u64)
        {
            return Err(self.missing_identity());
        }
        Ok(Delivery {
            stream_id: entry.id,
            identity,
            digest,
            units,
            payload,
        })
    }

    async fn ensure_group(&self) -> Result<(), RedisTaskEventError> {
        let generation = self.capability.guard_admission()?;
        let mutation = ScriptMutation::ensure_group(&self.keys, &self.config)?;
        self.commit_mutation(&mutation).await?;
        self.capability.guard_acknowledgement(generation)
    }

    async fn commit_mutation(
        &self,
        mutation: &ScriptMutation,
    ) -> Result<Vec<Vec<u8>>, RedisTaskEventError> {
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
    ) -> Result<RedisTaskEventQuotaState, RedisTaskEventError> {
        if output.len() != 5 {
            return Err(RedisTaskEventError::Accounting);
        }
        let parse = |value: &[u8]| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(RedisTaskEventError::Accounting)
        };
        Ok(self.quota_state_from(parse(&output[2])?, parse(&output[3])?, parse(&output[4])?))
    }

    fn quota_state_from(
        &self,
        used_bytes: u64,
        accepted_records: u64,
        pending_deliveries: u64,
    ) -> RedisTaskEventQuotaState {
        let pressure = if used_bytes >= self.config.hard_limit_bytes
            || accepted_records >= self.config.max_records.get() as u64
        {
            RedisQuotaPressure::HardLimit
        } else if used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisTaskEventQuotaState {
            used_bytes,
            accepted_records,
            pending_deliveries,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            max_records: self.config.max_records.get() as u64,
            pressure,
        }
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisTaskEventError {
        let failure = RedisMutationError::from_redis(error).failure();
        use crate::redis_durability::RedisMutationFailure;
        match failure {
            RedisMutationFailure::ReadOnlyPrimary => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::ReadOnly);
            }
            RedisMutationFailure::OutOfMemory => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::OutOfMemory);
            }
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisTaskEventError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisTaskEventError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisTaskEventError::IdentityConflict
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
        RedisTaskEventError::Durability(error.failure())
    }

    fn missing_identity(&self) -> RedisTaskEventError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
        RedisTaskEventError::Accounting
    }
}

impl TaskEventWriter for RedisTaskEvents {
    fn prepare(&self) -> TaskEventFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.quota_state()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }

    fn stage<'a>(
        &'a self,
        identity: &'a str,
        encoded_task_event: &'a [u8],
    ) -> TaskEventFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.append(identity, encoded_task_event.to_vec())
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }
}

impl TaskEventDelivery for RedisTaskEventDelivery {
    fn payload(&self) -> &[u8] {
        self.payload()
    }

    fn complete(self: Box<Self>) -> TaskEventFuture<'static, Result<(), String>> {
        Box::pin(async move { (*self).complete().await.map_err(|error| error.to_string()) })
    }

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> TaskEventFuture<'static, Result<(), String>> {
        Box::pin(async move {
            drop(self);
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            Ok(())
        })
    }
}

impl TaskEventConsumer for RedisTaskEvents {
    fn next(&self) -> TaskEventFuture<'_, Result<Option<Box<dyn TaskEventDelivery>>, String>> {
        Box::pin(async move {
            self.next_delivery()
                .await
                .map(|delivery| {
                    delivery.map(|delivery| Box::new(delivery) as Box<dyn TaskEventDelivery>)
                })
                .map_err(|error| error.to_string())
        })
    }
}

/// Admitted TaskEvents role registered with capability probes and reconstruction
/// before either production component receives its role interfaces.
pub(crate) struct RedisTaskEventsRoleRegistration {
    connection: MultiplexedConnection,
    keys: RedisTaskEventKeys,
    config: RedisTaskEventsConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisTaskEventsRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisTaskEventsConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisTaskEventError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisTaskEventKeys::new(&config.namespace),
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) fn build_adapter(
        &self,
        capability: Arc<dyn RedisTaskEventCapability>,
    ) -> Result<RedisTaskEvents, RedisTaskEventError> {
        RedisTaskEvents::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::TaskEvents
            && context.manifest_identity() == &self.manifest_identity
            && RedisTaskEvents::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisTaskEventsRoleRegistration {
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
impl RedisReconstructionCallback for RedisTaskEventsRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mutation = ScriptMutation::ensure_group(&self.keys, &self.config)
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
        redis::cmd("XRANGE")
            .arg(&self.keys.stream)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(self.config.max_records.get())
            .query_async::<redis::Value>(&mut connection)
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&["used_bytes", "accepted_records", "pending_deliveries"])
            .query_async::<Vec<Option<u64>>>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub fn redis_task_events_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(TASK_EVENTS_SCRIPT_NAME, TASK_EVENTS_SCRIPT_SHA256)?;
    let quota_pattern = RedisNamespacePattern::key("tickr:{namespace}:task-events:quota");
    RedisOperationManifest::new(
        CoordinationRole::TaskEvents,
        REDIS_TASK_EVENTS_PROTOCOL,
        REDIS_TASK_EVENTS_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:task-events:stream",
            "tickr:{namespace}:task-events:digests",
            "tickr:{namespace}:task-events:entries",
            "tickr:{namespace}:task-events:units",
            "tickr:{namespace}:task-events:pending",
            "tickr:{namespace}:task-events:quota",
            "tickr:{namespace}:task-events:completed:*",
            "tickr:{namespace}:task-events:operations:*",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            quota_pattern,
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

struct Delivery {
    stream_id: String,
    identity: String,
    digest: String,
    units: u64,
    payload: Vec<u8>,
}

struct ScriptMutation {
    operation: RedisStableOperation,
    keys: RedisTaskEventKeys,
    kind: MutationKind,
    config: MutationConfig,
}

enum MutationKind {
    EnsureGroup,
    Append {
        identity: String,
        digest: String,
        payload: Vec<u8>,
        units: u64,
    },
    Claim {
        stream_id: String,
        identity: String,
        digest: String,
        units: u64,
    },
    Complete {
        stream_id: String,
        identity: String,
        digest: String,
        units: u64,
    },
}

impl ScriptMutation {
    fn ensure_group(
        keys: &RedisTaskEventKeys,
        config: &RedisTaskEventsConfig,
    ) -> Result<Self, RedisTaskEventError> {
        Self::new(
            keys,
            keys.operation("group", "ensure"),
            b"ensure-group".to_vec(),
            MutationKind::EnsureGroup,
            config,
        )
    }

    fn append(
        keys: &RedisTaskEventKeys,
        identity: &str,
        digest: String,
        payload: Vec<u8>,
        units: u64,
        config: &RedisTaskEventsConfig,
    ) -> Result<Self, RedisTaskEventError> {
        Self::new(
            keys,
            keys.operation(identity, "append"),
            digest.as_bytes().to_vec(),
            MutationKind::Append {
                identity: identity.to_owned(),
                digest,
                payload,
                units,
            },
            config,
        )
    }

    fn claim(
        keys: &RedisTaskEventKeys,
        delivery: &Delivery,
        config: &RedisTaskEventsConfig,
    ) -> Result<Self, RedisTaskEventError> {
        let stable_payload = format!(
            "{}:{}:{}:{}",
            delivery.stream_id, delivery.identity, delivery.digest, delivery.units
        )
        .into_bytes();
        Self::new(
            keys,
            keys.operation(&delivery.stream_id, "claim"),
            stable_payload,
            MutationKind::Claim {
                stream_id: delivery.stream_id.clone(),
                identity: delivery.identity.clone(),
                digest: delivery.digest.clone(),
                units: delivery.units,
            },
            config,
        )
    }

    fn complete(
        keys: &RedisTaskEventKeys,
        stream_id: &str,
        identity: &str,
        digest: String,
        units: u64,
        config: &RedisTaskEventsConfig,
    ) -> Result<Self, RedisTaskEventError> {
        let stable_payload = format!("{stream_id}:{identity}:{digest}:{units}").into_bytes();
        Self::new(
            keys,
            keys.operation(stream_id, "complete"),
            stable_payload,
            MutationKind::Complete {
                stream_id: stream_id.to_owned(),
                identity: identity.to_owned(),
                digest,
                units,
            },
            config,
        )
    }

    fn new(
        keys: &RedisTaskEventKeys,
        operation_key: String,
        stable_payload: Vec<u8>,
        kind: MutationKind,
        config: &RedisTaskEventsConfig,
    ) -> Result<Self, RedisTaskEventError> {
        let operation = RedisStableOperation::new(operation_key, &stable_payload)
            .map_err(|_| RedisTaskEventError::InvalidOperation)?;
        Ok(Self {
            operation,
            keys: keys.clone(),
            kind,
            config: MutationConfig {
                max_records: config.max_records.get() as u64,
                hard_limit_bytes: config.hard_limit_bytes,
                completion_retention_millis: millis(config.completion_retention),
            },
        })
    }

    fn arguments(&self) -> (&str, &str, &str, &[u8], u64) {
        match &self.kind {
            MutationKind::EnsureGroup => ("ensure_group", "", "", &[], 0),
            MutationKind::Append {
                identity,
                digest,
                payload,
                units,
            } => ("append", identity, digest, payload, *units),
            MutationKind::Claim {
                identity,
                digest,
                units,
                ..
            } => ("claim", identity, digest, &[], *units),
            MutationKind::Complete {
                identity,
                digest,
                units,
                ..
            } => ("complete", identity, digest, &[], *units),
        }
    }

    fn stream_id(&self) -> &str {
        match &self.kind {
            MutationKind::Claim { stream_id, .. } | MutationKind::Complete { stream_id, .. } => {
                stream_id
            }
            MutationKind::EnsureGroup | MutationKind::Append { .. } => "",
        }
    }
}

#[async_trait]
impl RedisStableMutation for ScriptMutation {
    type Output = Vec<Vec<u8>>;

    fn operation(&self) -> &RedisStableOperation {
        &self.operation
    }

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError> {
        let (operation, identity, digest, payload, units) = self.arguments();
        let config = self.config;
        let completed_key = self.keys.completed(identity);
        let output: Vec<Vec<u8>> = redis::cmd("EVAL")
            .arg(TASK_EVENTS_SCRIPT)
            .arg(7)
            .arg(&self.keys.stream)
            .arg(&self.keys.digests)
            .arg(&self.keys.entries)
            .arg(&self.keys.units)
            .arg(&self.keys.pending)
            .arg(&self.keys.quota)
            .arg(completed_key)
            .arg(operation)
            .arg(REDIS_TASK_EVENTS_GROUP)
            .arg(identity)
            .arg(digest)
            .arg(payload)
            .arg(units)
            .arg(config.max_records)
            .arg(config.hard_limit_bytes)
            .arg(self.stream_id())
            .arg(config.completion_retention_millis)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        match output.first().map(Vec::as_slice) {
            Some(b"conflict") => Ok(RedisStableMutationOutcome::IdentityConflict),
            Some(b"replayed") | Some(b"completed") if operation != "append" => {
                Ok(RedisStableMutationOutcome::Replayed(output))
            }
            Some(
                b"created" | b"appended" | b"claimed" | b"completed" | b"fenced" | b"missing"
                | b"trimmed" | b"accounting" | b"replayed",
            ) => Ok(RedisStableMutationOutcome::Applied(output)),
            _ => Err(RedisMutationError::rejected()),
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let (_, identity, digest, _, _) = self.arguments();
        match &self.kind {
            MutationKind::EnsureGroup => Ok(RedisStableMutationRecovery::Missing),
            MutationKind::Append { .. } => {
                let actual: Option<String> = redis::cmd("HGET")
                    .arg(&self.keys.digests)
                    .arg(identity)
                    .query_async(&mut *connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                if let Some(actual) = actual {
                    return Ok(if actual == digest {
                        RedisStableMutationRecovery::Matching
                    } else {
                        RedisStableMutationRecovery::IdentityConflict
                    });
                }
                let completed: Option<String> = redis::cmd("GET")
                    .arg(self.keys.completed(identity))
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                Ok(match completed {
                    Some(actual) if actual == digest => RedisStableMutationRecovery::Matching,
                    Some(_) => RedisStableMutationRecovery::IdentityConflict,
                    None => RedisStableMutationRecovery::Missing,
                })
            }
            MutationKind::Claim { stream_id, .. } => {
                let actual: Option<String> = redis::cmd("HGET")
                    .arg(&self.keys.pending)
                    .arg(stream_id)
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                Ok(match actual {
                    Some(actual) if actual == identity => RedisStableMutationRecovery::Matching,
                    Some(_) => RedisStableMutationRecovery::IdentityConflict,
                    None => RedisStableMutationRecovery::Missing,
                })
            }
            MutationKind::Complete { .. } => {
                let completed: Option<String> = redis::cmd("GET")
                    .arg(self.keys.completed(identity))
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                Ok(match completed {
                    Some(actual) if actual == digest => RedisStableMutationRecovery::Matching,
                    Some(_) => RedisStableMutationRecovery::IdentityConflict,
                    None => RedisStableMutationRecovery::Missing,
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
struct MutationConfig {
    max_records: u64,
    hard_limit_bytes: u64,
    completion_retention_millis: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisTaskEventError {
    InvalidConfiguration,
    InvalidOperation,
    Unavailable,
    IdentityConflict,
    CapacityFenced,
    ForwardingUnavailable,
    Accounting,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisTaskEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Redis TaskEvent configuration is invalid",
            Self::InvalidOperation => "Redis TaskEvent operation is invalid",
            Self::Unavailable => "Redis TaskEvent role is unavailable",
            Self::IdentityConflict => "Redis TaskEvent identity conflicts with accepted bytes",
            Self::CapacityFenced => "Redis TaskEvent backlog capacity is fenced",
            Self::ForwardingUnavailable => "Conductor relay forwarding is unavailable",
            Self::Accounting => "Redis TaskEvent accounting is inconsistent",
            Self::Durability(_) => "Redis TaskEvent durability was not proved",
        })
    }
}

impl std::error::Error for RedisTaskEventError {}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
        let manifest = redis_task_events_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::TaskEvents);
        assert_eq!(manifest.protocol(), REDIS_TASK_EVENTS_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_TASK_EVENTS_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), TASK_EVENTS_SCRIPT_NAME);
        assert_eq!(manifest.scripts()[0].sha256(), TASK_EVENTS_SCRIPT_SHA256);
        assert_eq!(
            digest_hex(TASK_EVENTS_SCRIPT.as_bytes()),
            TASK_EVENTS_SCRIPT_SHA256
        );
        assert!(!manifest.commands().contains(&"PUBLISH"));
        assert!(!manifest.commands().contains(&"XTRIM"));
        assert!(manifest
            .key_patterns()
            .contains(&"tickr:{namespace}:task-events:stream"));
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);
    }

    #[test]
    fn configuration_and_operation_identities_are_bounded() {
        let mut config = RedisTaskEventsConfig::new("formation", "conductor");
        assert!(config.validate().is_ok());
        config.soft_limit_bytes = config.hard_limit_bytes;
        assert_eq!(
            config.validate(),
            Err(RedisTaskEventError::InvalidConfiguration)
        );
        assert!(valid_identity("dispatch:7:terminal"));
        assert!(!valid_identity("tenant payload must not become a key"));
    }
}
