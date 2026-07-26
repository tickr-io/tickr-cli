use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use prost::Message as _;
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamAutoClaimOptions, StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands as _,
};
use sha2::{Digest, Sha256};
use tickr_api::commands::client::{RedisCommandRequestClient, RedisCommandRequestError};
use tickr_conductor::api_commands_consumer::{CommandBusConsumer, CommandBusHandler};
use tickr_proto::{
    coord::command_bus::{CommandRequestMetadata, DEFAULT_MAX_PAYLOAD_BYTES},
    tickr_api::{self as api, ApiCommandResponse},
};
use tokio_util::sync::CancellationToken;
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

pub const REDIS_COMMAND_BUS_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.command-bus.redis-request-reply", 1);
pub const REDIS_COMMAND_BUS_GROUP: &str = "tickr-command-bus-v1";

const CORRELATION_ACCOUNTED_BYTES: u64 = 256;
const DEFAULT_LEASE: Duration = Duration::from_secs(2);
const DEFAULT_REPLY_RETENTION: Duration = Duration::from_secs(2);
const DEFAULT_POLL: Duration = Duration::from_millis(5);
const DEFAULT_MAX_RECORDS: usize = 1_024;
const DEFAULT_SOFT_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_HARD_BYTES: u64 = 10 * 1024 * 1024;
const CLEANUP_BATCH: usize = 16;

const REDIS_COMMAND_BUS_COMMANDS: &[&str] = &[
    "DEL",
    "EVAL",
    "GET",
    "HGET",
    "HINCRBY",
    "HMGET",
    "HSET",
    "SET",
    "TIME",
    "WAITAOF",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XDEL",
    "XGROUP CREATE",
    "XREADGROUP",
    "ZADD",
    "ZCARD",
    "ZRANGEBYSCORE",
    "ZREM",
    "ZREMRANGEBYSCORE",
    "ZSCORE",
];

const COMMAND_BUS_SCRIPT_NAME: &str = "command-bus-v1";
const COMMAND_BUS_SCRIPT_SHA256: &str =
    "0ab83fe713da54968849526627f8bbc4241a846e94499fe41bf4aa58f5df8167";

const COMMAND_BUS_SCRIPT: &str = r#"local operation = ARGV[1]

local function now_ms()
    local value = redis.call('TIME')
    return (tonumber(value[1]) * 1000) + math.floor(tonumber(value[2]) / 1000)
end

local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function quota_result(status, quota, detail)
    return {
        status,
        detail or '',
        tostring(number_field(quota, 'used_bytes')),
        tostring(number_field(quota, 'request_records')),
        tostring(number_field(quota, 'correlation_records')),
        tostring(number_field(quota, 'reply_records')),
        tostring(number_field(quota, 'reply_reservations'))
    }
end

local function commit_reply(stream, correlation, reply, deadlines, quota, group, stream_id,
                            correlation_id, response, response_digest, cleanup_at)
    local request_bytes = number_field(correlation, 'request_bytes')
    local reserved_reply_bytes = number_field(correlation, 'reserved_reply_bytes')
    local response_bytes = string.len(response)
    redis.call('SET', reply, response)
    redis.call('HSET', correlation,
        'state', 'replied',
        'response_digest', response_digest,
        'response_bytes', response_bytes,
        'cleanup_at', cleanup_at)
    redis.call('XACK', stream, group, stream_id)
    redis.call('XDEL', stream, stream_id)
    redis.call('ZADD', deadlines, cleanup_at, correlation_id)
    redis.call('HINCRBY', quota, 'used_bytes', response_bytes - request_bytes - reserved_reply_bytes)
    redis.call('HINCRBY', quota, 'request_records', -1)
    redis.call('HINCRBY', quota, 'reply_reservations', -1)
    redis.call('HINCRBY', quota, 'reply_records', 1)
end

if operation == 'ensure-group' then
    local result = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[2], '0', 'MKSTREAM')
    if type(result) == 'table' and result.err and not string.find(result.err, 'BUSYGROUP') then
        return redis.error_reply(result.err)
    end
    return {'ready'}
end

if operation == 'heartbeat' then
    local now = now_ms()
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[2])
    return {'live', tostring(now + tonumber(ARGV[3]))}
end

if operation == 'remove-consumer' then
    redis.call('ZREM', KEYS[1], ARGV[2])
    return {'removed'}
end

if operation == 'admit' then
    local stream = KEYS[1]
    local correlation = KEYS[2]
    local deadlines = KEYS[3]
    local consumers = KEYS[4]
    local quota = KEYS[5]
    local correlation_id = ARGV[2]
    local deadline = tonumber(ARGV[3])
    local payload = ARGV[4]
    local request_digest = ARGV[5]
    local reserved_reply_bytes = tonumber(ARGV[6])
    local hard_bytes = tonumber(ARGV[7])
    local max_records = tonumber(ARGV[8])
    local correlation_bytes = tonumber(ARGV[9])
    local retrying = ARGV[10] == '1'
    local now = now_ms()

    redis.call('ZREMRANGEBYSCORE', consumers, '-inf', now)
    if redis.call('ZCARD', consumers) == 0 then
        return quota_result('unavailable', quota)
    end
    if deadline <= now then
        return quota_result('expired', quota)
    end
    local prior_digest = redis.call('HGET', correlation, 'request_digest')
    if prior_digest then
        if retrying and prior_digest == request_digest then
            return quota_result(
                'admitted',
                quota,
                redis.call('HGET', correlation, 'stream_id'))
        end
        return quota_result('duplicate', quota)
    end

    local used = number_field(quota, 'used_bytes')
    local records = number_field(quota, 'request_records')
        + number_field(quota, 'correlation_records')
        + number_field(quota, 'reply_records')
        + number_field(quota, 'reply_reservations')
    local admitted_bytes = string.len(payload) + reserved_reply_bytes + correlation_bytes
    if used + admitted_bytes > hard_bytes or records + 3 > max_records then
        return quota_result('fenced', quota)
    end

    local stream_id = redis.call('XADD', stream, '*',
        'correlation', correlation_id,
        'deadline', tostring(deadline),
        'request_digest', request_digest,
        'payload', payload)
    redis.call('HSET', correlation,
        'request_digest', request_digest,
        'deadline', deadline,
        'request_bytes', string.len(payload),
        'reserved_reply_bytes', reserved_reply_bytes,
        'stream_id', stream_id,
        'state', 'pending',
        'response_bytes', 0,
        'correlation_bytes', correlation_bytes)
    redis.call('ZADD', deadlines, deadline, correlation_id)
    redis.call('HINCRBY', quota, 'used_bytes', admitted_bytes)
    redis.call('HINCRBY', quota, 'request_records', 1)
    redis.call('HINCRBY', quota, 'correlation_records', 1)
    redis.call('HINCRBY', quota, 'reply_reservations', 1)
    return quota_result('admitted', quota, stream_id)
end

if operation == 'claim' then
    local stream = KEYS[1]
    local correlation = KEYS[2]
    local reply = KEYS[3]
    local deadlines = KEYS[4]
    local quota = KEYS[5]
    local group = ARGV[2]
    local stream_id = ARGV[3]
    local correlation_id = ARGV[4]
    local owner = ARGV[5]
    local lease_ms = tonumber(ARGV[6])
    local expired_reply = ARGV[7]
    local expired_digest = ARGV[8]
    local reply_retention_ms = tonumber(ARGV[9])
    local now = now_ms()
    local state = redis.call('HGET', correlation, 'state')

    if not state then
        return quota_result('missing', quota)
    end
    if redis.call('HGET', correlation, 'stream_id') ~= stream_id then
        return quota_result('conflict', quota)
    end
    if state == 'replied' then
        redis.call('XACK', stream, group, stream_id)
        redis.call('XDEL', stream, stream_id)
        return quota_result('replied', quota)
    end
    if tonumber(redis.call('HGET', correlation, 'deadline')) <= now then
        commit_reply(stream, correlation, reply, deadlines, quota, group, stream_id,
            correlation_id, expired_reply, expired_digest, now + reply_retention_ms)
        return quota_result('expired', quota)
    end
    if state == 'processing' then
        local processing_until = number_field(correlation, 'processing_until')
        local processing_owner = redis.call('HGET', correlation, 'processing_owner')
        if processing_until > now and processing_owner ~= owner then
            return quota_result('busy', quota)
        end
    end
    redis.call('HSET', correlation,
        'state', 'processing',
        'processing_owner', owner,
        'processing_until', now + lease_ms)
    return quota_result('process', quota)
end

if operation == 'reply' then
    local stream = KEYS[1]
    local correlation = KEYS[2]
    local reply = KEYS[3]
    local deadlines = KEYS[4]
    local quota = KEYS[5]
    local group = ARGV[2]
    local stream_id = ARGV[3]
    local correlation_id = ARGV[4]
    local owner = ARGV[5]
    local response = ARGV[6]
    local response_digest = ARGV[7]
    local reply_retention_ms = tonumber(ARGV[8])
    local now = now_ms()
    local state = redis.call('HGET', correlation, 'state')

    if not state then
        return quota_result('missing', quota)
    end
    if state == 'replied' then
        if redis.call('HGET', correlation, 'response_digest') == response_digest then
            return quota_result('replayed', quota)
        end
        return quota_result('conflict', quota)
    end
    if state ~= 'processing'
        or redis.call('HGET', correlation, 'processing_owner') ~= owner
        or redis.call('HGET', correlation, 'stream_id') ~= stream_id then
        return quota_result('conflict', quota)
    end

    commit_reply(stream, correlation, reply, deadlines, quota, group, stream_id,
        correlation_id, response, response_digest, now + reply_retention_ms)
    return quota_result('replied', quota)
end

if operation == 'cleanup' then
    local stream = KEYS[1]
    local correlation = KEYS[2]
    local reply = KEYS[3]
    local deadlines = KEYS[4]
    local quota = KEYS[5]
    local correlation_id = ARGV[2]
    local group = ARGV[3]
    local delivered = ARGV[4] == '1'
    local expired_reply = ARGV[5]
    local expired_digest = ARGV[6]
    local reply_retention_ms = tonumber(ARGV[7])
    local now = now_ms()
    local state = redis.call('HGET', correlation, 'state')

    if not state then
        redis.call('ZREM', deadlines, correlation_id)
        return quota_result('cleaned', quota)
    end
    local stream_id = redis.call('HGET', correlation, 'stream_id')
    local deadline = tonumber(redis.call('HGET', correlation, 'deadline'))
    if state ~= 'replied' and deadline <= now then
        local processing_until = number_field(correlation, 'processing_until')
        if state ~= 'processing' or processing_until <= now then
            commit_reply(stream, correlation, reply, deadlines, quota, group, stream_id,
                correlation_id, expired_reply, expired_digest, now + reply_retention_ms)
            return quota_result('expired', quota)
        end
        return quota_result('active', quota)
    end
    if state ~= 'replied' then
        return quota_result('active', quota)
    end
    local cleanup_at = number_field(correlation, 'cleanup_at')
    if not delivered and cleanup_at > now then
        return quota_result('retained', quota)
    end

    local response_bytes = number_field(correlation, 'response_bytes')
    local correlation_bytes = number_field(correlation, 'correlation_bytes')
    redis.call('DEL', reply)
    redis.call('DEL', correlation)
    redis.call('ZREM', deadlines, correlation_id)
    redis.call('XACK', stream, group, stream_id)
    redis.call('XDEL', stream, stream_id)
    redis.call('HINCRBY', quota, 'used_bytes', -response_bytes - correlation_bytes)
    redis.call('HINCRBY', quota, 'correlation_records', -1)
    redis.call('HINCRBY', quota, 'reply_records', -1)
    return quota_result('cleaned', quota)
end

return redis.error_reply('unknown command-bus operation')
"#;

#[derive(Clone, Debug)]
pub struct RedisCommandBusConfig {
    pub namespace: String,
    pub consumer_id: String,
    pub consumer_lease: Duration,
    pub reply_retention: Duration,
    pub poll_interval: Duration,
    pub max_payload_bytes: NonZeroUsize,
    pub max_records: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisCommandBusConfig {
    pub fn new(namespace: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            consumer_id: consumer_id.into(),
            consumer_lease: DEFAULT_LEASE,
            reply_retention: DEFAULT_REPLY_RETENTION,
            poll_interval: DEFAULT_POLL,
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("non-zero constant"),
            max_records: NonZeroUsize::new(DEFAULT_MAX_RECORDS).expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisCommandBusError> {
        let valid_symbol = |value: &str| {
            !value.is_empty()
                && value.len() <= 127
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        };
        let minimum_bytes = CORRELATION_ACCOUNTED_BYTES
            .saturating_add(self.max_payload_bytes.get() as u64)
            .saturating_add(1);
        if !valid_symbol(&self.namespace)
            || !valid_symbol(&self.consumer_id)
            || self.consumer_lease.is_zero()
            || self.reply_retention.is_zero()
            || self.poll_interval.is_zero()
            || self.max_records.get() < 3
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes < minimum_bytes
            || millis(self.consumer_lease) == 0
            || millis(self.reply_retention) == 0
        {
            return Err(RedisCommandBusError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisCommandKeys {
    requests: String,
    correlation_prefix: String,
    reply_prefix: String,
    deadlines: String,
    consumers: String,
    quota: String,
}

impl RedisCommandKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:command-bus");
        Self {
            requests: format!("{prefix}:requests"),
            correlation_prefix: format!("{prefix}:correlations:"),
            reply_prefix: format!("{prefix}:replies:"),
            deadlines: format!("{prefix}:deadlines"),
            consumers: format!("{prefix}:consumers"),
            quota: format!("{prefix}:quota"),
        }
    }

    fn correlation(&self, id: Uuid) -> String {
        format!("{}{id}", self.correlation_prefix)
    }

    fn reply(&self, id: Uuid) -> String {
        format!("{}{id}", self.reply_prefix)
    }
}

/// Capability boundary consumed by the Redis Command-bus adapter. Production
/// uses [`MonitoredRedisCommandCapability`]; the trait keeps the law suite from
/// constructing monitor internals.
pub trait RedisCommandCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisCommandBusError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisCommandBusError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisQuotaState);
}

pub struct MonitoredRedisCommandCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisCommandCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisCommandCapability for MonitoredRedisCommandCapability {
    fn guard_admission(&self) -> Result<u64, RedisCommandBusError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisCommandBusError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisCommandBusError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisCommandBusError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisCommandBusError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisQuotaState) {
        self.reporter.report_quota_state(state);
    }
}

#[derive(Clone)]
pub struct RedisCommandBus {
    connection: MultiplexedConnection,
    keys: RedisCommandKeys,
    config: Arc<RedisCommandBusConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisCommandCapability>,
    expired_reply: Arc<Vec<u8>>,
    expired_reply_digest: Arc<String>,
}

/// Post-admission Command-bus role used to register capability probes and
/// reconstruction before exposing the runtime bus to either component.
pub(crate) struct RedisCommandBusRoleRegistration {
    connection: MultiplexedConnection,
    keys: RedisCommandKeys,
    config: RedisCommandBusConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisCommandBusRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisCommandBusConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisCommandBusError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisCommandKeys::new(&config.namespace),
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) fn build_bus(
        &self,
        capability: Arc<dyn RedisCommandCapability>,
    ) -> Result<RedisCommandBus, RedisCommandBusError> {
        RedisCommandBus::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::CommandBus
            && context.manifest_identity() == &self.manifest_identity
            && RedisCommandBus::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisCommandBusRoleRegistration {
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
impl RedisReconstructionCallback for RedisCommandBusRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let mutation = ScriptMutation::new(
            RedisStableOperation::new(&self.keys.requests, COMMAND_BUS_SCRIPT.as_bytes())
                .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?,
            MutationKind::EnsureGroup {
                stream: self.keys.requests.clone(),
            },
        );
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
        if committed.into_output().first().map(Vec::as_slice) != Some(b"ready") {
            return Err(RedisReconstructionFailure::PendingEvidenceUnavailable);
        }
        redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "request_records",
                "correlation_records",
                "reply_records",
                "reply_reservations",
            ])
            .query_async::<Vec<Option<u64>>>(&mut connection)
            .await
            .map(|_| ())
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

impl RedisCommandBus {
    pub async fn connect(
        client: redis::Client,
        config: RedisCommandBusConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisCommandCapability>,
    ) -> Result<Self, RedisCommandBusError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisCommandBusError::Unavailable)?;
        Self::from_connection(connection, config, durability, capability)
    }

    pub fn from_connection(
        connection: MultiplexedConnection,
        config: RedisCommandBusConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisCommandCapability>,
    ) -> Result<Self, RedisCommandBusError> {
        config.validate()?;
        let expired_reply = ApiCommandResponse {
            status_code: 408,
            payload: Some(api::api_command_response::Payload::Error(
                api::ErrorPayload {
                    code: api::CommandErrorCode::BadRequest as i32,
                    message: "command deadline expired before dispatch".to_owned(),
                },
            )),
        }
        .encode_to_vec();
        let expired_reply_digest = digest_hex(&expired_reply);
        Ok(Self {
            connection,
            keys: RedisCommandKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
            expired_reply: Arc::new(expired_reply),
            expired_reply_digest: Arc::new(expired_reply_digest),
        })
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_command_bus_operation_manifest()
    }

    pub(crate) fn max_in_flight(&self) -> NonZeroUsize {
        self.config.max_records
    }

    pub async fn serve_with_handler<F, Fut>(
        &self,
        shutdown: CancellationToken,
        handler: F,
    ) -> Result<(), RedisCommandBusError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Vec<u8>> + Send,
    {
        self.ensure_group().await?;
        self.heartbeat().await?;
        let mut heartbeat_at = tokio::time::Instant::now() + self.config.consumer_lease / 3;
        let mut reclaim_cursor = "0-0".to_owned();

        loop {
            if shutdown.is_cancelled() {
                self.remove_consumer().await?;
                return Ok(());
            }
            if tokio::time::Instant::now() >= heartbeat_at {
                self.heartbeat().await?;
                self.cleanup_expired().await?;
                heartbeat_at = tokio::time::Instant::now() + self.config.consumer_lease / 3;
            }

            let mut connection = self.connection.clone();
            let claimed: redis::streams::StreamAutoClaimReply = connection
                .xautoclaim_options(
                    &self.keys.requests,
                    REDIS_COMMAND_BUS_GROUP,
                    &self.config.consumer_id,
                    millis(self.config.consumer_lease),
                    &reclaim_cursor,
                    StreamAutoClaimOptions::default().count(1),
                )
                .await
                .map_err(|error| self.redis_error(error))?;
            reclaim_cursor = if claimed.next_stream_id == "0-0" {
                "0-0".to_owned()
            } else {
                claimed.next_stream_id
            };
            if let Some(entry) = claimed.claimed.into_iter().next() {
                self.process_entry(entry, &handler).await?;
                continue;
            }

            let options = StreamReadOptions::default()
                .group(REDIS_COMMAND_BUS_GROUP, &self.config.consumer_id)
                .count(1)
                .block(25);
            let reply: Option<StreamReadReply> = connection
                .xread_options(&[&self.keys.requests], &[">"], &options)
                .await
                .map_err(|error| self.redis_error(error))?;
            if let Some(entry) = reply
                .and_then(|reply| reply.keys.into_iter().next())
                .and_then(|stream| stream.ids.into_iter().next())
            {
                self.process_entry(entry, &handler).await?;
            }
        }
    }

    pub async fn cleanup_expired(&self) -> Result<RedisQuotaState, RedisCommandBusError> {
        let mut connection = self.connection.clone();
        let now = redis_time_ms(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let due: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&self.keys.deadlines)
            .arg("-inf")
            .arg(now)
            .arg("LIMIT")
            .arg(0)
            .arg(CLEANUP_BATCH)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let mut state = self.empty_quota_state();
        for correlation in due {
            let Ok(correlation_id) = correlation.parse::<Uuid>() else {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::Accounting);
                return Err(RedisCommandBusError::Accounting);
            };
            let mutation = self.cleanup_mutation(correlation_id, false)?;
            let output = self.commit_mutation(&mutation).await?;
            state = self.decode_quota(&output)?;
        }
        self.capability.report_quota(state);
        Ok(state)
    }

    pub async fn quota_state(&self) -> Result<RedisQuotaState, RedisCommandBusError> {
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "request_records",
                "correlation_records",
                "reply_records",
                "reply_reservations",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.len() != 5 {
            return Err(RedisCommandBusError::Accounting);
        }
        let used = values[0].unwrap_or(0);
        let accepted_identities = values[2].unwrap_or(0);
        Ok(self.quota_state_from(used, accepted_identities))
    }

    async fn process_entry<F, Fut>(
        &self,
        entry: StreamId,
        handler: &F,
    ) -> Result<(), RedisCommandBusError>
    where
        F: Fn(Vec<u8>) -> Fut + Send + Sync,
        Fut: Future<Output = Vec<u8>> + Send,
    {
        let correlation = entry
            .get::<String>("correlation")
            .and_then(|value| value.parse::<Uuid>().ok())
            .ok_or_else(|| {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
                RedisCommandBusError::Accounting
            })?;
        let payload = entry.get::<Vec<u8>>("payload").ok_or_else(|| {
            self.capability
                .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
            RedisCommandBusError::Accounting
        })?;
        let claim = self.claim_mutation(correlation, &entry.id)?;
        let output = self.commit_mutation(&claim).await?;
        let status = output.first().map(Vec::as_slice).unwrap_or_default();
        let quota = self.decode_quota(&output)?;
        self.capability.report_quota(quota);
        match status {
            b"expired" | b"replied" | b"busy" => return Ok(()),
            b"process" => {}
            b"missing" => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity);
                return Err(RedisCommandBusError::Accounting);
            }
            _ => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::Accounting);
                return Err(RedisCommandBusError::Accounting);
            }
        }

        let generation = self.capability.guard_admission()?;
        let response = handler(payload);
        tokio::pin!(response);
        let mut response = loop {
            tokio::select! {
                response = &mut response => break response,
                _ = tokio::time::sleep(self.config.consumer_lease / 3) => {
                    let renewal = self.claim_mutation(correlation, &entry.id)?;
                    let output = self.commit_mutation(&renewal).await?;
                    if output.first().map(Vec::as_slice) != Some(b"process") {
                        self.capability
                            .report_failure(RedisRoleCapabilityFailure::Accounting);
                        return Err(RedisCommandBusError::Accounting);
                    }
                }
            }
        };
        if response.len() > self.config.max_payload_bytes.get() {
            response = oversized_response();
        }
        let reply = self.reply_mutation(correlation, &entry.id, response)?;
        let output = self
            .commit_mutation_with_generation(&reply, generation)
            .await?;
        let quota = self.decode_quota(&output)?;
        self.capability.report_quota(quota);
        match output.first().map(Vec::as_slice) {
            Some(b"replied" | b"replayed") => Ok(()),
            _ => {
                self.capability
                    .report_failure(RedisRoleCapabilityFailure::Accounting);
                Err(RedisCommandBusError::Accounting)
            }
        }
    }

    async fn ensure_group(&self) -> Result<(), RedisCommandBusError> {
        let mutation = ScriptMutation::new(
            RedisStableOperation::new(&self.keys.requests, COMMAND_BUS_SCRIPT.as_bytes())?,
            MutationKind::EnsureGroup {
                stream: self.keys.requests.clone(),
            },
        );
        self.commit_mutation(&mutation).await.map(|_| ())
    }

    async fn heartbeat(&self) -> Result<(), RedisCommandBusError> {
        let payload = stable_payload(&[
            self.config.consumer_id.as_bytes(),
            &millis(self.config.consumer_lease).to_be_bytes(),
        ]);
        let mutation = ScriptMutation::new(
            RedisStableOperation::new(
                format!("{}:{}", self.keys.consumers, self.config.consumer_id),
                &payload,
            )?,
            MutationKind::Heartbeat {
                consumers: self.keys.consumers.clone(),
                consumer_id: self.config.consumer_id.clone(),
                lease_ms: millis(self.config.consumer_lease),
            },
        );
        self.commit_mutation(&mutation).await.map(|_| ())
    }

    async fn remove_consumer(&self) -> Result<(), RedisCommandBusError> {
        let mutation = ScriptMutation::new(
            RedisStableOperation::new(
                format!("{}:{}:remove", self.keys.consumers, self.config.consumer_id),
                self.config.consumer_id.as_bytes(),
            )?,
            MutationKind::RemoveConsumer {
                consumers: self.keys.consumers.clone(),
                consumer_id: self.config.consumer_id.clone(),
            },
        );
        self.commit_mutation(&mutation).await.map(|_| ())
    }

    fn admission_mutation(
        &self,
        metadata: CommandRequestMetadata,
        payload: Vec<u8>,
    ) -> Result<ScriptMutation, RedisCommandBusError> {
        let digest = digest_hex(&payload);
        Ok(ScriptMutation::new(
            RedisStableOperation::new(self.keys.correlation(metadata.correlation_id), &payload)?,
            MutationKind::Admit {
                stream: self.keys.requests.clone(),
                correlation: self.keys.correlation(metadata.correlation_id),
                deadlines: self.keys.deadlines.clone(),
                consumers: self.keys.consumers.clone(),
                quota: self.keys.quota.clone(),
                correlation_id: metadata.correlation_id,
                deadline_ms: metadata.deadline_unix_ms,
                payload,
                request_digest: digest,
                reserved_reply_bytes: self.config.max_payload_bytes.get() as u64,
                hard_bytes: self.config.hard_limit_bytes,
                max_records: self.config.max_records.get(),
            },
        ))
    }

    fn claim_mutation(
        &self,
        correlation_id: Uuid,
        stream_id: &str,
    ) -> Result<ScriptMutation, RedisCommandBusError> {
        let payload = stable_payload(&[
            stream_id.as_bytes(),
            self.config.consumer_id.as_bytes(),
            self.expired_reply_digest.as_bytes(),
        ]);
        Ok(ScriptMutation::new(
            RedisStableOperation::new(self.keys.correlation(correlation_id), &payload)?,
            MutationKind::Claim {
                stream: self.keys.requests.clone(),
                correlation: self.keys.correlation(correlation_id),
                reply: self.keys.reply(correlation_id),
                deadlines: self.keys.deadlines.clone(),
                quota: self.keys.quota.clone(),
                group: REDIS_COMMAND_BUS_GROUP.to_owned(),
                stream_id: stream_id.to_owned(),
                correlation_id,
                owner: self.config.consumer_id.clone(),
                lease_ms: millis(self.config.consumer_lease),
                expired_reply: (*self.expired_reply).clone(),
                expired_digest: (*self.expired_reply_digest).clone(),
                retention_ms: millis(self.config.reply_retention),
            },
        ))
    }

    fn reply_mutation(
        &self,
        correlation_id: Uuid,
        stream_id: &str,
        response: Vec<u8>,
    ) -> Result<ScriptMutation, RedisCommandBusError> {
        let response_digest = digest_hex(&response);
        Ok(ScriptMutation::new(
            RedisStableOperation::new(self.keys.correlation(correlation_id), &response)?,
            MutationKind::Reply {
                stream: self.keys.requests.clone(),
                correlation: self.keys.correlation(correlation_id),
                reply: self.keys.reply(correlation_id),
                deadlines: self.keys.deadlines.clone(),
                quota: self.keys.quota.clone(),
                group: REDIS_COMMAND_BUS_GROUP.to_owned(),
                stream_id: stream_id.to_owned(),
                correlation_id,
                owner: self.config.consumer_id.clone(),
                response,
                response_digest,
                retention_ms: millis(self.config.reply_retention),
            },
        ))
    }

    fn cleanup_mutation(
        &self,
        correlation_id: Uuid,
        delivered: bool,
    ) -> Result<ScriptMutation, RedisCommandBusError> {
        let payload = stable_payload(&[
            correlation_id.as_bytes(),
            &[u8::from(delivered)],
            self.expired_reply_digest.as_bytes(),
        ]);
        Ok(ScriptMutation::new(
            RedisStableOperation::new(self.keys.correlation(correlation_id), &payload)?,
            MutationKind::Cleanup {
                stream: self.keys.requests.clone(),
                correlation: self.keys.correlation(correlation_id),
                reply: self.keys.reply(correlation_id),
                deadlines: self.keys.deadlines.clone(),
                quota: self.keys.quota.clone(),
                correlation_id,
                group: REDIS_COMMAND_BUS_GROUP.to_owned(),
                delivered,
                expired_reply: (*self.expired_reply).clone(),
                expired_digest: (*self.expired_reply_digest).clone(),
                retention_ms: millis(self.config.reply_retention),
            },
        ))
    }

    async fn commit_mutation(
        &self,
        mutation: &ScriptMutation,
    ) -> Result<Vec<Vec<u8>>, RedisCommandBusError> {
        let generation = self.capability.guard_admission()?;
        self.commit_mutation_with_generation(mutation, generation)
            .await
    }

    async fn commit_mutation_with_generation(
        &self,
        mutation: &ScriptMutation,
        generation: u64,
    ) -> Result<Vec<Vec<u8>>, RedisCommandBusError> {
        let mut connection = self.connection.clone();
        let committed = match self.durability.execute(&mut connection, mutation).await {
            Ok(committed) => committed,
            Err(error) if error.failure() == RedisDurabilityFailure::AmbiguousMutation => self
                .durability
                .resolve_ambiguous(&mut connection, mutation)
                .await
                .map_err(|error| self.durability_error(error))?,
            Err(error) => return Err(self.durability_error(error)),
        };
        self.capability.guard_acknowledgement(generation)?;
        Ok(committed.into_output())
    }

    fn decode_quota(&self, output: &[Vec<u8>]) -> Result<RedisQuotaState, RedisCommandBusError> {
        if output.len() < 7 {
            return Err(RedisCommandBusError::Accounting);
        }
        let parse = |index: usize| {
            std::str::from_utf8(&output[index])
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(RedisCommandBusError::Accounting)
        };
        let used = parse(2)?;
        let accepted_identities = parse(4)?;
        for index in 3..7 {
            parse(index)?;
        }
        Ok(self.quota_state_from(used, accepted_identities))
    }

    fn quota_state_from(&self, used: u64, accepted_identities: u64) -> RedisQuotaState {
        let pressure = if used >= self.config.hard_limit_bytes {
            RedisQuotaPressure::HardLimit
        } else if used >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisQuotaState {
            used,
            soft_threshold: self.config.soft_limit_bytes,
            hard_limit: self.config.hard_limit_bytes,
            accepted_identities,
            pressure,
        }
    }

    fn empty_quota_state(&self) -> RedisQuotaState {
        self.quota_state_from(0, 0)
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisCommandBusError {
        let failure = if error.code() == Some("OOM") {
            RedisRoleCapabilityFailure::OutOfMemory
        } else if error.kind() == redis::ErrorKind::ReadOnly || error.code() == Some("READONLY") {
            RedisRoleCapabilityFailure::ReadOnly
        } else {
            RedisRoleCapabilityFailure::RequiredOperation
        };
        self.capability.report_failure(failure);
        RedisCommandBusError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisCommandBusError {
        let failure = match error.failure() {
            RedisDurabilityFailure::ReadOnlyPrimary => RedisRoleCapabilityFailure::ReadOnly,
            RedisDurabilityFailure::OutOfMemory => RedisRoleCapabilityFailure::OutOfMemory,
            RedisDurabilityFailure::LocalFsyncUnavailable
            | RedisDurabilityFailure::AmbiguousLocalFsync => RedisRoleCapabilityFailure::LocalFsync,
            RedisDurabilityFailure::IdentityConflict => RedisRoleCapabilityFailure::Accounting,
            RedisDurabilityFailure::InvalidOperation
            | RedisDurabilityFailure::AmbiguousMutation
            | RedisDurabilityFailure::MutationRejected => {
                RedisRoleCapabilityFailure::RequiredOperation
            }
        };
        self.capability.report_failure(failure);
        RedisCommandBusError::Unavailable
    }
}

#[async_trait]
impl CommandBusConsumer for RedisCommandBus {
    async fn serve(
        &self,
        handler: Arc<dyn CommandBusHandler>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        self.serve_with_handler(cancel, move |payload| {
            let handler = Arc::clone(&handler);
            async move { handler.handle(payload).await }
        })
        .await
        .map_err(anyhow::Error::new)
    }
}

#[async_trait]
impl RedisCommandRequestClient for RedisCommandBus {
    async fn request(
        &self,
        encoded_request: Vec<u8>,
        metadata: CommandRequestMetadata,
    ) -> Result<Vec<u8>, RedisCommandRequestError> {
        if encoded_request.len() > self.config.max_payload_bytes.get() {
            return Err(RedisCommandRequestError::TooLarge);
        }
        let mutation = self
            .admission_mutation(metadata, encoded_request)
            .map_err(|_| RedisCommandRequestError::Unavailable)?;
        let output = self
            .commit_mutation(&mutation)
            .await
            .map_err(|_| RedisCommandRequestError::Unavailable)?;
        let quota = self
            .decode_quota(&output)
            .map_err(|_| RedisCommandRequestError::Unavailable)?;
        self.capability.report_quota(quota);
        match output.first().map(Vec::as_slice) {
            Some(b"admitted") => {}
            Some(b"duplicate") => return Err(RedisCommandRequestError::DuplicateCorrelation),
            Some(b"expired") => return Err(RedisCommandRequestError::Timeout),
            Some(b"unavailable" | b"fenced") => return Err(RedisCommandRequestError::Unavailable),
            _ => return Err(RedisCommandRequestError::Unavailable),
        }

        let timeout = metadata
            .remaining()
            .ok_or(RedisCommandRequestError::Timeout)?;
        let started = tokio::time::Instant::now();
        loop {
            let mut connection = self.connection.clone();
            let reply: Option<Vec<u8>> = redis::cmd("GET")
                .arg(self.keys.reply(metadata.correlation_id))
                .query_async(&mut connection)
                .await
                .map_err(|error| {
                    self.redis_error(error);
                    RedisCommandRequestError::Unavailable
                })?;
            if let Some(reply) = reply {
                let cleanup = self
                    .cleanup_mutation(metadata.correlation_id, true)
                    .map_err(|_| RedisCommandRequestError::Unavailable)?;
                self.commit_mutation(&cleanup)
                    .await
                    .map_err(|_| RedisCommandRequestError::Unavailable)?;
                return Ok(reply);
            }
            if started.elapsed() >= timeout {
                let _ = self.cleanup_expired().await;
                return Err(RedisCommandRequestError::Timeout);
            }
            tokio::time::sleep(
                self.config
                    .poll_interval
                    .min(timeout.saturating_sub(started.elapsed())),
            )
            .await;
        }
    }
}

pub fn redis_command_bus_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(COMMAND_BUS_SCRIPT_NAME, COMMAND_BUS_SCRIPT_SHA256)?;
    let quota_pattern = RedisNamespacePattern::key("tickr:{namespace}:command-bus:quota");
    RedisOperationManifest::new(
        CoordinationRole::CommandBus,
        REDIS_COMMAND_BUS_PROTOCOL,
        REDIS_COMMAND_BUS_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:command-bus:requests",
            "tickr:{namespace}:command-bus:correlations:*",
            "tickr:{namespace}:command-bus:replies:*",
            "tickr:{namespace}:command-bus:deadlines",
            "tickr:{namespace}:command-bus:consumers",
            "tickr:{namespace}:command-bus:quota",
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

struct ScriptMutation {
    operation: RedisStableOperation,
    kind: MutationKind,
    attempts: AtomicUsize,
}

impl ScriptMutation {
    fn new(operation: RedisStableOperation, kind: MutationKind) -> Self {
        Self {
            operation,
            kind,
            attempts: AtomicUsize::new(0),
        }
    }
}

enum MutationKind {
    EnsureGroup {
        stream: String,
    },
    Heartbeat {
        consumers: String,
        consumer_id: String,
        lease_ms: u64,
    },
    RemoveConsumer {
        consumers: String,
        consumer_id: String,
    },
    Admit {
        stream: String,
        correlation: String,
        deadlines: String,
        consumers: String,
        quota: String,
        correlation_id: Uuid,
        deadline_ms: u64,
        payload: Vec<u8>,
        request_digest: String,
        reserved_reply_bytes: u64,
        hard_bytes: u64,
        max_records: usize,
    },
    Claim {
        stream: String,
        correlation: String,
        reply: String,
        deadlines: String,
        quota: String,
        group: String,
        stream_id: String,
        correlation_id: Uuid,
        owner: String,
        lease_ms: u64,
        expired_reply: Vec<u8>,
        expired_digest: String,
        retention_ms: u64,
    },
    Reply {
        stream: String,
        correlation: String,
        reply: String,
        deadlines: String,
        quota: String,
        group: String,
        stream_id: String,
        correlation_id: Uuid,
        owner: String,
        response: Vec<u8>,
        response_digest: String,
        retention_ms: u64,
    },
    Cleanup {
        stream: String,
        correlation: String,
        reply: String,
        deadlines: String,
        quota: String,
        correlation_id: Uuid,
        group: String,
        delivered: bool,
        expired_reply: Vec<u8>,
        expired_digest: String,
        retention_ms: u64,
    },
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
        let mut command = redis::cmd("EVAL");
        command.arg(COMMAND_BUS_SCRIPT);
        let retrying = self.attempts.fetch_add(1, Ordering::SeqCst) > 0;
        match &self.kind {
            MutationKind::EnsureGroup { stream } => {
                command
                    .arg(1)
                    .arg(stream)
                    .arg("ensure-group")
                    .arg(REDIS_COMMAND_BUS_GROUP);
            }
            MutationKind::Heartbeat {
                consumers,
                consumer_id,
                lease_ms,
            } => {
                command
                    .arg(1)
                    .arg(consumers)
                    .arg("heartbeat")
                    .arg(consumer_id)
                    .arg(lease_ms);
            }
            MutationKind::RemoveConsumer {
                consumers,
                consumer_id,
            } => {
                command
                    .arg(1)
                    .arg(consumers)
                    .arg("remove-consumer")
                    .arg(consumer_id);
            }
            MutationKind::Admit {
                stream,
                correlation,
                deadlines,
                consumers,
                quota,
                correlation_id,
                deadline_ms,
                payload,
                request_digest,
                reserved_reply_bytes,
                hard_bytes,
                max_records,
            } => {
                command
                    .arg(5)
                    .arg(stream)
                    .arg(correlation)
                    .arg(deadlines)
                    .arg(consumers)
                    .arg(quota)
                    .arg("admit")
                    .arg(correlation_id.to_string())
                    .arg(deadline_ms)
                    .arg(payload)
                    .arg(request_digest)
                    .arg(reserved_reply_bytes)
                    .arg(hard_bytes)
                    .arg(max_records)
                    .arg(CORRELATION_ACCOUNTED_BYTES)
                    .arg(u8::from(retrying));
            }
            MutationKind::Claim {
                stream,
                correlation,
                reply,
                deadlines,
                quota,
                group,
                stream_id,
                correlation_id,
                owner,
                lease_ms,
                expired_reply,
                expired_digest,
                retention_ms,
            } => {
                command
                    .arg(5)
                    .arg(stream)
                    .arg(correlation)
                    .arg(reply)
                    .arg(deadlines)
                    .arg(quota)
                    .arg("claim")
                    .arg(group)
                    .arg(stream_id)
                    .arg(correlation_id.to_string())
                    .arg(owner)
                    .arg(lease_ms)
                    .arg(expired_reply)
                    .arg(expired_digest)
                    .arg(retention_ms);
            }
            MutationKind::Reply {
                stream,
                correlation,
                reply,
                deadlines,
                quota,
                group,
                stream_id,
                correlation_id,
                owner,
                response,
                response_digest,
                retention_ms,
            } => {
                command
                    .arg(5)
                    .arg(stream)
                    .arg(correlation)
                    .arg(reply)
                    .arg(deadlines)
                    .arg(quota)
                    .arg("reply")
                    .arg(group)
                    .arg(stream_id)
                    .arg(correlation_id.to_string())
                    .arg(owner)
                    .arg(response)
                    .arg(response_digest)
                    .arg(retention_ms);
            }
            MutationKind::Cleanup {
                stream,
                correlation,
                reply,
                deadlines,
                quota,
                correlation_id,
                group,
                delivered,
                expired_reply,
                expired_digest,
                retention_ms,
            } => {
                command
                    .arg(5)
                    .arg(stream)
                    .arg(correlation)
                    .arg(reply)
                    .arg(deadlines)
                    .arg(quota)
                    .arg("cleanup")
                    .arg(correlation_id.to_string())
                    .arg(group)
                    .arg(u8::from(*delivered))
                    .arg(expired_reply)
                    .arg(expired_digest)
                    .arg(retention_ms);
            }
        }
        let result = command
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        Ok(RedisStableMutationOutcome::Applied(result))
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let recovery = match &self.kind {
            MutationKind::EnsureGroup { .. } => RedisStableMutationRecovery::Missing,
            MutationKind::Heartbeat {
                consumers,
                consumer_id,
                ..
            } => {
                let score: Option<u64> = redis::cmd("ZSCORE")
                    .arg(consumers)
                    .arg(consumer_id)
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                if score.is_some() {
                    RedisStableMutationRecovery::Matching
                } else {
                    RedisStableMutationRecovery::Missing
                }
            }
            MutationKind::RemoveConsumer {
                consumers,
                consumer_id,
            } => {
                let score: Option<u64> = redis::cmd("ZSCORE")
                    .arg(consumers)
                    .arg(consumer_id)
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                if score.is_none() {
                    RedisStableMutationRecovery::Matching
                } else {
                    RedisStableMutationRecovery::Missing
                }
            }
            MutationKind::Admit {
                correlation,
                request_digest,
                ..
            } => hash_recovery(connection, correlation, "request_digest", request_digest).await?,
            MutationKind::Claim {
                correlation, owner, ..
            } => hash_recovery(connection, correlation, "processing_owner", owner).await?,
            MutationKind::Reply {
                correlation,
                response_digest,
                ..
            } => hash_recovery(connection, correlation, "response_digest", response_digest).await?,
            MutationKind::Cleanup { correlation, .. } => {
                let state: Option<String> = redis::cmd("HGET")
                    .arg(correlation)
                    .arg("state")
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                if state.is_none() {
                    RedisStableMutationRecovery::Matching
                } else {
                    RedisStableMutationRecovery::Missing
                }
            }
        };
        Ok(recovery)
    }
}

async fn hash_recovery(
    connection: &mut MultiplexedConnection,
    key: &str,
    field: &str,
    expected: &str,
) -> Result<RedisStableMutationRecovery, RedisMutationError> {
    let actual: Option<String> = redis::cmd("HGET")
        .arg(key)
        .arg(field)
        .query_async(connection)
        .await
        .map_err(RedisMutationError::from_redis)?;
    Ok(match actual {
        None => RedisStableMutationRecovery::Missing,
        Some(actual) if actual == expected => RedisStableMutationRecovery::Matching,
        Some(_) => RedisStableMutationRecovery::IdentityConflict,
    })
}

async fn redis_time_ms(connection: &mut MultiplexedConnection) -> Result<u64, redis::RedisError> {
    let value: Vec<u64> = redis::cmd("TIME").query_async(connection).await?;
    if value.len() != 2 {
        return Err(redis::RedisError::from((
            redis::ErrorKind::TypeError,
            "Redis TIME returned an invalid value",
        )));
    }
    Ok(value[0]
        .saturating_mul(1_000)
        .saturating_add(value[1] / 1_000))
}

fn oversized_response() -> Vec<u8> {
    ApiCommandResponse {
        status_code: 503,
        payload: Some(api::api_command_response::Payload::Error(
            api::ErrorPayload {
                code: api::CommandErrorCode::Unavailable as i32,
                message: "command response exceeded payload limit".to_owned(),
            },
        )),
    }
    .encode_to_vec()
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn stable_payload(parts: &[&[u8]]) -> Vec<u8> {
    let capacity = parts
        .iter()
        .fold(0usize, |total, part| total.saturating_add(8 + part.len()));
    let mut payload = Vec::with_capacity(capacity);
    for part in parts {
        payload.extend_from_slice(&(part.len() as u64).to_be_bytes());
        payload.extend_from_slice(part);
    }
    payload
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisCommandBusError {
    InvalidConfiguration,
    Unavailable,
    Accounting,
}

impl From<RedisDurabilityError> for RedisCommandBusError {
    fn from(_: RedisDurabilityError) -> Self {
        Self::Unavailable
    }
}

impl fmt::Display for RedisCommandBusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Redis Command-bus configuration is invalid",
            Self::Unavailable => "Redis Command-bus capability is unavailable",
            Self::Accounting => "Redis Command-bus accounting is inconsistent",
        })
    }
}

impl std::error::Error for RedisCommandBusError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_exact_and_rejects_unregistered_operations() {
        let manifest = redis_command_bus_operation_manifest().unwrap();
        assert_eq!(manifest.role(), CoordinationRole::CommandBus);
        assert_eq!(manifest.protocol(), REDIS_COMMAND_BUS_PROTOCOL);
        assert_eq!(manifest.commands(), REDIS_COMMAND_BUS_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(
            manifest.scripts()[0].sha256(),
            digest_hex(COMMAND_BUS_SCRIPT.as_bytes())
        );
        assert!(!manifest.commands().contains(&"APPEND"));

        let script = manifest.scripts()[0];
        let failure = RedisOperationManifest::new(
            CoordinationRole::CommandBus,
            REDIS_COMMAND_BUS_PROTOCOL,
            REDIS_COMMAND_BUS_COMMANDS.to_vec(),
            vec![script],
            manifest.key_patterns().to_vec(),
            vec![],
            vec![RedisRequiredOperationCanary::new(
                RedisOperation::command("APPEND"),
                RedisNamespacePattern::key("tickr:{namespace}:command-bus:requests"),
            )],
            vec![
                RedisForbiddenOperation::cross_role(
                    RedisOperation::script(script),
                    CoordinationRole::TaskDispatch,
                ),
                RedisForbiddenOperation::administrative("FLUSHALL"),
            ],
        )
        .unwrap_err();
        assert_eq!(
            failure.failure(),
            crate::redis_operation_manifest::RedisOperationManifestFailure::UnregisteredOperation
        );
    }
}
