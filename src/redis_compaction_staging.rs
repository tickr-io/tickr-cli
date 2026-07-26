//! Redis Compaction staging and drain ordering.
//!
//! Raw `CompactionEnvelope` bytes cross the primary-local fsync boundary before
//! an acknowledgement can be built. Drain deliveries remain pending until the
//! immutable Log/scope seal has been archived, verified, committed, and purged.

use std::{fmt, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamAutoClaimOptions, StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands as _,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickr_conductor::system_tasks::build_ack;
use tickr_migrations::scope_repository::{
    ScopeCleanupOutcome, ScopeSnapshotOutcome, TickrCtxScopeSnapshot,
};
use tickr_proto::{
    codec::compaction::decode_envelope,
    coord::{
        log_stream::LogSeal, CompactionFuture, CompactionStaging, CompactionStagingDelivery,
        CompactionStagingSeal,
    },
    ConductorRelayMessage,
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
    redis_log_staging::{RedisLogStagingError, RedisLogStagingStream},
    redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisRequiredOperationCanary,
        RedisScriptIdentity,
    },
    redis_scope_store::{RedisScopeStore, RedisScopeStoreError},
};

pub const REDIS_COMPACTION_STAGING_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.compaction-staging.redis-stream", 1);
pub const REDIS_COMPACTION_STAGING_GROUP: &str = "tickr-compaction-drain-v1";

const DEFAULT_RECLAIM_IDLE: Duration = Duration::from_millis(100);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_ENVELOPES: usize = 1024;
const DEFAULT_MAX_SEALED_REFERENCES: usize = 16_384;
const DEFAULT_SOFT_LIMIT_BYTES: u64 = 192 * 1024 * 1024;
const DEFAULT_HARD_LIMIT_BYTES: u64 = 256 * 1024 * 1024;
const ENVELOPE_ACCOUNTED_BYTES: u64 = 256;

const REDIS_COMPACTION_STAGING_COMMANDS: &[&str] = &[
    "EVAL",
    "HDEL",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HLEN",
    "HMGET",
    "HSET",
    "HVALS",
    "WAITAOF",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XDEL",
    "XGROUP CREATE",
    "XRANGE",
    "XREADGROUP",
];
const COMPACTION_STAGING_SCRIPT_NAME: &str = "compaction-staging-v1";
const COMPACTION_STAGING_SCRIPT_SHA256: &str =
    "35cdc220705d18c6c2ed8baab53d39ac888847a31b3c1a4299e94ff9565c43ee";

const COMPACTION_STAGING_SCRIPT: &str = r#"local operation = ARGV[1]
local group = ARGV[2]
local identity = ARGV[3]
local digest = ARGV[4]
local payload = ARGV[5]
local units = tonumber(ARGV[6])
local max_envelopes = tonumber(ARGV[7])
local hard_bytes = tonumber(ARGV[8])
local stream_id = ARGV[9]
local reference_count = tonumber(ARGV[10])
local max_references = tonumber(ARGV[11])
local archive_digest = ARGV[12]

local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function state(status, detail)
    return {
        status,
        detail or '',
        tostring(number_field(KEYS[10], 'used_bytes')),
        tostring(number_field(KEYS[10], 'staged_envelopes')),
        tostring(number_field(KEYS[10], 'pending_deliveries')),
        tostring(number_field(KEYS[10], 'sealed_references')),
        tostring(number_field(KEYS[10], 'archive_commits'))
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

if operation == 'stage' then
    local completed = redis.call('HGET', KEYS[11], identity)
    if completed then
        if string.sub(completed, 1, 64) == digest then
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
        local prior_payload = redis.call('HGET', KEYS[4], identity)
        local prior_units = tonumber(redis.call('HGET', KEYS[5], identity) or '-1')
        if not prior_id or prior_payload ~= payload or prior_units < units then
            return state('accounting', '')
        end
        if not entry_exists(prior_id) then
            return state('trimmed', prior_id)
        end
        return state('replayed', prior_id)
    end
    local used = number_field(KEYS[10], 'used_bytes')
    local staged = number_field(KEYS[10], 'staged_envelopes')
    if staged >= max_envelopes or units > hard_bytes or used > hard_bytes - units then
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
    redis.call('HSET', KEYS[4], identity, payload)
    redis.call('HSET', KEYS[5], identity, tostring(units))
    redis.call('HINCRBY', KEYS[10], 'used_bytes', units)
    redis.call('HINCRBY', KEYS[10], 'staged_envelopes', 1)
    return state('staged', id)
end

if operation == 'claim' then
    local prior = redis.call('HGET', KEYS[6], stream_id)
    if prior then
        if prior == identity then
            return state('replayed', stream_id)
        end
        return state('accounting', stream_id)
    end
    local stored_digest = redis.call('HGET', KEYS[2], identity)
    local stored_id = redis.call('HGET', KEYS[3], identity)
    local stored_payload = redis.call('HGET', KEYS[4], identity)
    local stored_units = tonumber(redis.call('HGET', KEYS[5], identity) or '-1')
    if stored_digest ~= digest or stored_id ~= stream_id or stored_payload ~= payload
        or stored_units < units then
        return state('missing', stream_id)
    end
    if not entry_exists(stream_id) then
        return state('trimmed', stream_id)
    end
    redis.call('HSET', KEYS[6], stream_id, identity)
    redis.call('HINCRBY', KEYS[10], 'pending_deliveries', 1)
    return state('claimed', stream_id)
end

if operation == 'seal' then
    if not redis.call('HGET', KEYS[2], identity) then
        return state('missing', '')
    end
    local prior = redis.call('HGET', KEYS[7], identity)
    if prior then
        if prior == payload then
            return state('replayed', '')
        end
        return state('conflict', '')
    end
    local used = number_field(KEYS[10], 'used_bytes')
    local references = number_field(KEYS[10], 'sealed_references')
    if reference_count > max_references or references > max_references - reference_count
        or units > hard_bytes or used > hard_bytes - units then
        return state('fenced', '')
    end
    redis.call('HSET', KEYS[7], identity, payload)
    redis.call('HSET', KEYS[12], identity, tostring(reference_count))
    redis.call('HINCRBY', KEYS[5], identity, units)
    redis.call('HINCRBY', KEYS[10], 'used_bytes', units)
    redis.call('HINCRBY', KEYS[10], 'sealed_references', reference_count)
    return state('sealed', '')
end

if operation == 'archive' then
    if not redis.call('HGET', KEYS[7], identity) then
        return state('ineligible', '')
    end
    local prior_digest = redis.call('HGET', KEYS[9], identity)
    if prior_digest then
        if prior_digest == archive_digest and redis.call('HGET', KEYS[8], identity) == payload then
            return state('replayed', '')
        end
        return state('conflict', '')
    end
    local used = number_field(KEYS[10], 'used_bytes')
    if units > hard_bytes or used > hard_bytes - units then
        return state('fenced', '')
    end
    redis.call('HSET', KEYS[8], identity, payload)
    redis.call('HSET', KEYS[9], identity, archive_digest)
    redis.call('HINCRBY', KEYS[5], identity, units)
    redis.call('HINCRBY', KEYS[10], 'used_bytes', units)
    redis.call('HINCRBY', KEYS[10], 'archive_commits', 1)
    return state('archived', '')
end

if operation == 'complete' then
    local completed = redis.call('HGET', KEYS[11], identity)
    if completed then
        if completed == digest .. ':' .. archive_digest then
            return state('replayed_complete', stream_id)
        end
        return state('conflict', stream_id)
    end
    local stored_digest = redis.call('HGET', KEYS[2], identity)
    local stored_id = redis.call('HGET', KEYS[3], identity)
    local stored_units = tonumber(redis.call('HGET', KEYS[5], identity) or '-1')
    local pending_identity = redis.call('HGET', KEYS[6], stream_id)
    local stored_archive_digest = redis.call('HGET', KEYS[9], identity)
    local stored_references = tonumber(redis.call('HGET', KEYS[12], identity) or '-1')
    if stored_digest ~= digest or stored_id ~= stream_id or stored_units < 0
        or pending_identity ~= identity or stored_archive_digest ~= archive_digest
        or stored_references < 1 then
        return state('ineligible', stream_id)
    end
    if not entry_exists(stream_id) then
        return state('trimmed', stream_id)
    end
    redis.call('XACK', KEYS[1], group, stream_id)
    redis.call('XDEL', KEYS[1], stream_id)
    redis.call('HDEL', KEYS[2], identity)
    redis.call('HDEL', KEYS[3], identity)
    redis.call('HDEL', KEYS[4], identity)
    redis.call('HDEL', KEYS[5], identity)
    redis.call('HDEL', KEYS[6], stream_id)
    redis.call('HDEL', KEYS[7], identity)
    redis.call('HDEL', KEYS[8], identity)
    redis.call('HDEL', KEYS[9], identity)
    redis.call('HDEL', KEYS[12], identity)
    redis.call('HSET', KEYS[11], identity, digest .. ':' .. archive_digest)
    redis.call('HINCRBY', KEYS[10], 'used_bytes', -stored_units)
    redis.call('HINCRBY', KEYS[10], 'staged_envelopes', -1)
    redis.call('HINCRBY', KEYS[10], 'pending_deliveries', -1)
    redis.call('HINCRBY', KEYS[10], 'sealed_references', -stored_references)
    redis.call('HINCRBY', KEYS[10], 'archive_commits', -1)
    if number_field(KEYS[10], 'used_bytes') < 0
        or number_field(KEYS[10], 'staged_envelopes') < 0
        or number_field(KEYS[10], 'pending_deliveries') < 0
        or number_field(KEYS[10], 'sealed_references') < 0
        or number_field(KEYS[10], 'archive_commits') < 0 then
        return state('accounting', stream_id)
    end
    return state('completed', stream_id)
end

return redis.error_reply('unknown compaction-staging operation')"#;

#[derive(Clone, Debug)]
pub struct RedisCompactionStagingConfig {
    pub namespace: String,
    pub consumer_id: String,
    pub reclaim_idle: Duration,
    pub poll_interval: Duration,
    pub max_payload_bytes: NonZeroUsize,
    pub max_envelopes: NonZeroUsize,
    pub max_sealed_references: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisCompactionStagingConfig {
    pub fn new(namespace: impl Into<String>, consumer_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            consumer_id: consumer_id.into(),
            reclaim_idle: DEFAULT_RECLAIM_IDLE,
            poll_interval: DEFAULT_POLL_INTERVAL,
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("non-zero constant"),
            max_envelopes: NonZeroUsize::new(DEFAULT_MAX_ENVELOPES).expect("non-zero constant"),
            max_sealed_references: NonZeroUsize::new(DEFAULT_MAX_SEALED_REFERENCES)
                .expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_LIMIT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_LIMIT_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisCompactionStagingError> {
        if !valid_component(&self.namespace)
            || !valid_component(&self.consumer_id)
            || self.reclaim_idle.is_zero()
            || self.poll_interval.is_zero()
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes
                <= ENVELOPE_ACCOUNTED_BYTES.saturating_add(self.max_payload_bytes.get() as u64 * 2)
        {
            return Err(RedisCompactionStagingError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisCompactionStagingKeys {
    stream: String,
    digests: String,
    entries: String,
    payloads: String,
    units: String,
    pending: String,
    seals: String,
    archives: String,
    archive_digests: String,
    quota: String,
    completed: String,
    reference_counts: String,
}

impl RedisCompactionStagingKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:compaction-staging");
        Self {
            stream: format!("{prefix}:stream"),
            digests: format!("{prefix}:digests"),
            entries: format!("{prefix}:entries"),
            payloads: format!("{prefix}:payloads"),
            units: format!("{prefix}:units"),
            pending: format!("{prefix}:pending"),
            seals: format!("{prefix}:seals"),
            archives: format!("{prefix}:archives"),
            archive_digests: format!("{prefix}:archive-digests"),
            quota: format!("{prefix}:quota"),
            completed: format!("{prefix}:completed"),
            reference_counts: format!("{prefix}:reference-counts"),
        }
    }

    fn operation(&self, identity: &str, kind: &str) -> String {
        format!("{}:{kind}:{identity}", self.stream)
    }
}

pub trait RedisCompactionStagingCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisCompactionStagingError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisCompactionStagingError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisCompactionStagingQuotaState);
}

pub struct MonitoredRedisCompactionStagingCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisCompactionStagingCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisCompactionStagingCapability for MonitoredRedisCompactionStagingCapability {
    fn guard_admission(&self) -> Result<u64, RedisCompactionStagingError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisCompactionStagingError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisCompactionStagingError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisCompactionStagingError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisCompactionStagingError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisCompactionStagingQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RedisCompactionStagingQuotaState {
    pub used_bytes: u64,
    pub staged_envelopes: u64,
    pub pending_deliveries: u64,
    pub sealed_references: u64,
    pub archive_commits: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub max_envelopes: u64,
    pub max_sealed_references: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisCompactionStagingQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self
                .staged_envelopes
                .saturating_add(self.pending_deliveries)
                .saturating_add(self.sealed_references)
                .saturating_add(self.archive_commits),
            pressure: self.pressure,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisCompactionStageOutcome {
    Staged,
    ReplayedPending,
    ReplayedCompleted,
}

#[derive(Clone, Debug)]
pub struct RedisCompactionDelivery {
    stream_id: String,
    identity: String,
    payload_digest: String,
    units: u64,
    payload: Vec<u8>,
}

impl RedisCompactionDelivery {
    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Debug)]
pub struct RedisCompactionSeal {
    scope: TickrCtxScopeSnapshot,
    logs: Vec<LogSeal>,
    digest: String,
}

impl RedisCompactionSeal {
    pub fn scope(&self) -> &TickrCtxScopeSnapshot {
        &self.scope
    }

    pub fn logs(&self) -> &[LogSeal] {
        &self.logs
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisCompactionArchiveInstallation {
    identity: Vec<u8>,
}

impl RedisCompactionArchiveInstallation {
    pub fn new(identity: Vec<u8>) -> Result<Self, RedisCompactionArchiveError> {
        if identity.is_empty() {
            return Err(RedisCompactionArchiveError);
        }
        Ok(Self { identity })
    }

    pub fn identity(&self) -> &[u8] {
        &self.identity
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisCompactionArchiveError;

#[async_trait]
pub trait RedisCompactionArchive: Send + Sync {
    async fn write_final_logs(
        &self,
        envelope: &[u8],
        seal: &RedisCompactionSeal,
    ) -> Result<RedisCompactionArchiveInstallation, RedisCompactionArchiveError>;

    async fn verify_final_logs(
        &self,
        installation: &RedisCompactionArchiveInstallation,
    ) -> Result<(), RedisCompactionArchiveError>;

    async fn commit_archive(
        &self,
        envelope: &[u8],
        seal: &RedisCompactionSeal,
        installation: &RedisCompactionArchiveInstallation,
    ) -> Result<Vec<u8>, RedisCompactionArchiveError>;
}

#[derive(Clone)]
pub struct RedisCompactionStaging {
    connection: MultiplexedConnection,
    keys: RedisCompactionStagingKeys,
    config: Arc<RedisCompactionStagingConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisCompactionStagingCapability>,
}

impl RedisCompactionStaging {
    pub async fn connect(
        client: redis::Client,
        config: RedisCompactionStagingConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisCompactionStagingCapability>,
    ) -> Result<Self, RedisCompactionStagingError> {
        config.validate()?;
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisCompactionStagingError::Unavailable)?;
        let adapter = Self::from_connection(connection, config, durability, capability).await?;
        adapter.prepare_role().await?;
        Ok(adapter)
    }

    async fn from_connection(
        connection: MultiplexedConnection,
        config: RedisCompactionStagingConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisCompactionStagingCapability>,
    ) -> Result<Self, RedisCompactionStagingError> {
        config.validate()?;
        Ok(Self {
            connection,
            keys: RedisCompactionStagingKeys::new(&config.namespace),
            config: Arc::new(config),
            durability,
            capability,
        })
    }

    async fn prepare_role(&self) -> Result<(), RedisCompactionStagingError> {
        self.ensure_group().await?;
        self.quota_state().await?;
        Ok(())
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_compaction_staging_operation_manifest()
    }

    pub async fn stage(
        &self,
        payload: Vec<u8>,
    ) -> Result<RedisCompactionStageOutcome, RedisCompactionStagingError> {
        let identity = compaction_identity(&payload)?;
        if payload.is_empty() || payload.len() > self.config.max_payload_bytes.get() {
            return Err(RedisCompactionStagingError::InvalidOperation);
        }
        let generation = self.capability.guard_admission()?;
        let digest = digest_hex(&payload);
        let units = ENVELOPE_ACCOUNTED_BYTES
            .checked_add((payload.len() as u64).saturating_mul(2))
            .ok_or(RedisCompactionStagingError::InvalidOperation)?;
        let mutation =
            ScriptMutation::stage(&self.keys, identity, digest, payload, units, &self.config)?;
        observe_boundary(CompactionBoundary::BeforeStagingMutation);
        let output = self.commit_mutation(&mutation).await?;
        observe_boundary(CompactionBoundary::AfterDurabilityProof);
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state);
        self.capability.guard_acknowledgement(generation)?;
        match output.first().map(Vec::as_slice) {
            Some(b"staged") => Ok(RedisCompactionStageOutcome::Staged),
            Some(b"replayed") => Ok(RedisCompactionStageOutcome::ReplayedPending),
            Some(b"completed") => Ok(RedisCompactionStageOutcome::ReplayedCompleted),
            Some(b"fenced") => Err(RedisCompactionStagingError::CapacityFenced),
            Some(b"conflict") => Err(RedisCompactionStagingError::IdentityConflict),
            Some(b"trimmed") => {
                Err(self.accounting_failure(RedisRoleCapabilityFailure::UnexpectedTrim))
            }
            Some(b"accounting") => {
                Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting))
            }
            _ => Err(RedisCompactionStagingError::Accounting),
        }
    }

    pub async fn stage_for_relay(
        &self,
        payload: Vec<u8>,
    ) -> Result<ConductorRelayMessage, RedisCompactionStagingError> {
        let envelope =
            decode_envelope(&payload).map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
        let projection = envelope
            .projection
            .as_ref()
            .ok_or(RedisCompactionStagingError::InvalidOperation)?;
        self.stage(payload).await?;
        observe_boundary(CompactionBoundary::BeforeCrossPlaneAcknowledgement);
        let acknowledgement = build_ack(&projection.id, &envelope.correlation);
        observe_boundary(CompactionBoundary::AfterCrossPlaneAcknowledgement);
        Ok(acknowledgement)
    }

    pub async fn claim_next(
        &self,
    ) -> Result<Option<RedisCompactionDelivery>, RedisCompactionStagingError> {
        let Some(entry) = self.next_entry().await? else {
            return Ok(None);
        };
        observe_boundary(CompactionBoundary::AfterDrainReceipt);
        let delivery = self.decode_delivery(entry)?;
        let mutation = ScriptMutation::claim(&self.keys, &delivery, &self.config)?;
        let output = self.commit_mutation(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state);
        match output.first().map(Vec::as_slice) {
            Some(b"claimed" | b"replayed") => Ok(Some(delivery)),
            Some(b"trimmed") => {
                Err(self.accounting_failure(RedisRoleCapabilityFailure::UnexpectedTrim))
            }
            Some(b"missing") => {
                Err(self.accounting_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity))
            }
            _ => Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting)),
        }
    }

    pub async fn drain_claimed<A: RedisCompactionArchive>(
        &self,
        delivery: RedisCompactionDelivery,
        scope_namespace: &str,
        scope_store: &RedisScopeStore,
        mut log_streams: Vec<RedisLogStagingStream>,
        archive: &A,
    ) -> Result<(), RedisCompactionStagingError> {
        if !valid_component(scope_namespace)
            || digest_hex(&delivery.payload) != delivery.payload_digest
        {
            return Err(RedisCompactionStagingError::InvalidOperation);
        }
        let envelope = decode_envelope(&delivery.payload)
            .map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
        let projection = envelope
            .projection
            .as_ref()
            .ok_or(RedisCompactionStagingError::InvalidOperation)?;
        if projection.id != delivery.identity {
            return Err(RedisCompactionStagingError::IdentityConflict);
        }
        validate_log_inventory(projection, &log_streams)?;

        let seal = match self.load_seal(&delivery.identity).await? {
            Some(seal) => seal,
            None => {
                observe_boundary(CompactionBoundary::BeforeScopeSeal);
                let scope = match scope_store
                    .snapshot_tickr_ctx_scope_for_run(scope_namespace, &projection.id, Utc::now())
                    .await
                    .map_err(RedisCompactionStagingError::Scope)?
                {
                    ScopeSnapshotOutcome::Committed(snapshot)
                    | ScopeSnapshotOutcome::Idempotent(snapshot) => snapshot,
                    ScopeSnapshotOutcome::Missing => {
                        return Err(RedisCompactionStagingError::MissingScope)
                    }
                    ScopeSnapshotOutcome::Bound(_) | ScopeSnapshotOutcome::Quarantined { .. } => {
                        return Err(RedisCompactionStagingError::InvalidScope)
                    }
                };
                observe_boundary(CompactionBoundary::AfterScopeSeal);
                let mut logs = Vec::with_capacity(log_streams.len());
                for stream in &mut log_streams {
                    observe_boundary(CompactionBoundary::BeforeLogSeal);
                    logs.push(
                        stream
                            .seal()
                            .await
                            .map_err(RedisCompactionStagingError::Log)?,
                    );
                    observe_boundary(CompactionBoundary::AfterLogSeal);
                }
                let seal = build_seal(scope, logs)?;
                self.record_seal(&delivery.identity, &seal).await?;
                seal
            }
        };

        let archive_identity = match self.load_archive(&delivery.identity).await? {
            Some(identity) => identity,
            None => {
                observe_boundary(CompactionBoundary::BeforeArchiveWrite);
                let installation = archive
                    .write_final_logs(&delivery.payload, &seal)
                    .await
                    .map_err(|_| RedisCompactionStagingError::Archive)?;
                observe_boundary(CompactionBoundary::AfterArchiveWrite);
                observe_boundary(CompactionBoundary::BeforeArchiveVerification);
                archive
                    .verify_final_logs(&installation)
                    .await
                    .map_err(|_| RedisCompactionStagingError::Archive)?;
                observe_boundary(CompactionBoundary::AfterArchiveVerification);
                observe_boundary(CompactionBoundary::BeforeArchiveCommit);
                let identity = archive
                    .commit_archive(&delivery.payload, &seal, &installation)
                    .await
                    .map_err(|_| RedisCompactionStagingError::Archive)?;
                if identity.is_empty() {
                    return Err(RedisCompactionStagingError::Archive);
                }
                observe_boundary(CompactionBoundary::AfterArchiveCommit);
                self.record_archive(&delivery.identity, &identity).await?;
                identity
            }
        };

        for stream in &mut log_streams {
            let log_seal = seal
                .logs
                .iter()
                .find(|candidate| candidate.stream() == stream.identity())
                .ok_or(RedisCompactionStagingError::LogInventory)?;
            observe_boundary(CompactionBoundary::BeforeLogPurge);
            match stream
                .purge_after_verified_archive_commit(log_seal, &archive_identity)
                .await
            {
                Ok(_) => {}
                Err(RedisLogStagingError::ArchiveNotCommitted) => {
                    stream
                        .record_verified_archive_commit(log_seal, &archive_identity)
                        .await
                        .map_err(RedisCompactionStagingError::Log)?;
                    stream
                        .purge_after_verified_archive_commit(log_seal, &archive_identity)
                        .await
                        .map_err(RedisCompactionStagingError::Log)?;
                }
                Err(error) => return Err(RedisCompactionStagingError::Log(error)),
            }
            observe_boundary(CompactionBoundary::AfterLogPurge);
        }

        observe_boundary(CompactionBoundary::BeforeScopePurge);
        match scope_store
            .cleanup_tickr_ctx_scope(
                seal.scope.scope_id,
                &seal.scope.digest,
                &archive_identity,
                Utc::now(),
            )
            .await
        {
            Ok(ScopeCleanupOutcome::Cleaned | ScopeCleanupOutcome::AlreadyCleaned) => {}
            Ok(ScopeCleanupOutcome::SnapshotRequired)
            | Err(RedisScopeStoreError::ArchiveNotCommitted) => {
                scope_store
                    .record_verified_archive_commit(
                        seal.scope.scope_id,
                        &seal.scope.digest,
                        &archive_identity,
                        Utc::now(),
                    )
                    .await
                    .map_err(RedisCompactionStagingError::Scope)?;
                match scope_store
                    .cleanup_tickr_ctx_scope(
                        seal.scope.scope_id,
                        &seal.scope.digest,
                        &archive_identity,
                        Utc::now(),
                    )
                    .await
                    .map_err(RedisCompactionStagingError::Scope)?
                {
                    ScopeCleanupOutcome::Cleaned | ScopeCleanupOutcome::AlreadyCleaned => {}
                    _ => return Err(RedisCompactionStagingError::InvalidScope),
                }
            }
            Ok(_) => return Err(RedisCompactionStagingError::InvalidScope),
            Err(error) => return Err(RedisCompactionStagingError::Scope(error)),
        }
        observe_boundary(CompactionBoundary::AfterScopePurge);

        observe_boundary(CompactionBoundary::BeforeStagingCompletion);
        self.complete(&delivery, &archive_identity).await?;
        observe_boundary(CompactionBoundary::AfterStagingCompletion);
        Ok(())
    }

    pub async fn quota_state(
        &self,
    ) -> Result<RedisCompactionStagingQuotaState, RedisCompactionStagingError> {
        let mut connection = self.connection.clone();
        let values: Vec<Option<u64>> = redis::cmd("HMGET")
            .arg(&self.keys.quota)
            .arg(&[
                "used_bytes",
                "staged_envelopes",
                "pending_deliveries",
                "sealed_references",
                "archive_commits",
            ])
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if values.len() != 5 {
            return Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting));
        }
        let state = self.quota_state_from(
            values[0].unwrap_or(0),
            values[1].unwrap_or(0),
            values[2].unwrap_or(0),
            values[3].unwrap_or(0),
            values[4].unwrap_or(0),
        );
        self.audit_quota(&mut connection, state).await?;
        Ok(state)
    }

    async fn ensure_group(&self) -> Result<(), RedisCompactionStagingError> {
        let generation = self.capability.guard_admission()?;
        let mutation = ScriptMutation::ensure_group(&self.keys, &self.config)?;
        self.commit_mutation(&mutation).await?;
        self.capability.guard_acknowledgement(generation)
    }

    async fn next_entry(&self) -> Result<Option<StreamId>, RedisCompactionStagingError> {
        let mut connection = self.connection.clone();
        let claimed: redis::streams::StreamAutoClaimReply = connection
            .xautoclaim_options(
                &self.keys.stream,
                REDIS_COMPACTION_STAGING_GROUP,
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
            .group(REDIS_COMPACTION_STAGING_GROUP, &self.config.consumer_id)
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
    ) -> Result<RedisCompactionDelivery, RedisCompactionStagingError> {
        let identity = entry
            .get::<String>("identity")
            .filter(|identity| Uuid::parse_str(identity).is_ok())
            .ok_or_else(|| {
                self.accounting_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity)
            })?;
        let payload_digest = entry
            .get::<String>("digest")
            .filter(|digest| valid_digest(digest))
            .ok_or_else(|| {
                self.accounting_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity)
            })?;
        let units = entry
            .get::<u64>("units")
            .filter(|units| *units >= ENVELOPE_ACCOUNTED_BYTES)
            .ok_or_else(|| self.accounting_failure(RedisRoleCapabilityFailure::Accounting))?;
        let payload = entry
            .get::<Vec<u8>>("payload")
            .filter(|payload| {
                !payload.is_empty() && payload.len() <= self.config.max_payload_bytes.get()
            })
            .ok_or_else(|| {
                self.accounting_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity)
            })?;
        let expected_units = ENVELOPE_ACCOUNTED_BYTES
            .checked_add((payload.len() as u64).saturating_mul(2))
            .ok_or(RedisCompactionStagingError::Accounting)?;
        if digest_hex(&payload) != payload_digest || units != expected_units {
            return Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting));
        }
        Ok(RedisCompactionDelivery {
            stream_id: entry.id,
            identity,
            payload_digest,
            units,
            payload,
        })
    }

    async fn record_seal(
        &self,
        identity: &str,
        seal: &RedisCompactionSeal,
    ) -> Result<(), RedisCompactionStagingError> {
        let source_references = u64::try_from(seal.logs.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or(RedisCompactionStagingError::InvalidOperation)?;
        let staging_seal =
            CompactionStagingSeal::new(encode_seal(seal)?, seal.digest.clone(), source_references);
        self.record_staging_seal(identity, &staging_seal).await
    }

    async fn record_staging_seal(
        &self,
        identity: &str,
        seal: &CompactionStagingSeal,
    ) -> Result<(), RedisCompactionStagingError> {
        if seal.encoded().is_empty()
            || !valid_digest(seal.digest())
            || seal.source_references() == 0
        {
            return Err(RedisCompactionStagingError::InvalidOperation);
        }
        let generation = self.capability.guard_admission()?;
        let encoded = serde_json::to_vec(&StoredRoleSeal::from(seal))
            .map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
        let units = encoded.len() as u64;
        let mutation = ScriptMutation::seal(
            &self.keys,
            identity,
            seal.digest().to_owned(),
            encoded,
            units,
            seal.source_references(),
            &self.config,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state);
        self.capability.guard_acknowledgement(generation)?;
        match output.first().map(Vec::as_slice) {
            Some(b"sealed" | b"replayed") => Ok(()),
            Some(b"fenced") => Err(RedisCompactionStagingError::CapacityFenced),
            Some(b"conflict") => Err(RedisCompactionStagingError::IdentityConflict),
            Some(b"missing") => {
                Err(self.accounting_failure(RedisRoleCapabilityFailure::MissingAcceptedIdentity))
            }
            _ => Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting)),
        }
    }

    async fn record_archive(
        &self,
        identity: &str,
        archive_identity: &[u8],
    ) -> Result<(), RedisCompactionStagingError> {
        let generation = self.capability.guard_admission()?;
        let archive_digest = digest_hex(archive_identity);
        let mutation = ScriptMutation::archive(
            &self.keys,
            identity,
            archive_digest,
            archive_identity.to_vec(),
            archive_identity.len() as u64,
            &self.config,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state);
        self.capability.guard_acknowledgement(generation)?;
        match output.first().map(Vec::as_slice) {
            Some(b"archived" | b"replayed") => Ok(()),
            Some(b"fenced") => Err(RedisCompactionStagingError::CapacityFenced),
            Some(b"conflict") => Err(RedisCompactionStagingError::IdentityConflict),
            Some(b"ineligible") => Err(RedisCompactionStagingError::ArchiveNotCommitted),
            _ => Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting)),
        }
    }

    async fn complete(
        &self,
        delivery: &RedisCompactionDelivery,
        archive_identity: &[u8],
    ) -> Result<(), RedisCompactionStagingError> {
        let generation = self.capability.guard_admission()?;
        let mutation = ScriptMutation::complete(
            &self.keys,
            delivery,
            digest_hex(archive_identity),
            &self.config,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let state = self.decode_state(&output)?;
        self.capability.report_quota(state);
        self.capability.guard_acknowledgement(generation)?;
        match output.first().map(Vec::as_slice) {
            Some(b"completed" | b"replayed_complete") => Ok(()),
            Some(b"conflict") => Err(RedisCompactionStagingError::IdentityConflict),
            Some(b"trimmed") => {
                Err(self.accounting_failure(RedisRoleCapabilityFailure::UnexpectedTrim))
            }
            Some(b"ineligible") => Err(RedisCompactionStagingError::ArchiveNotCommitted),
            _ => Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting)),
        }
    }

    async fn load_seal(
        &self,
        identity: &str,
    ) -> Result<Option<RedisCompactionSeal>, RedisCompactionStagingError> {
        let Some(staging_seal) = self.load_staging_seal(identity).await? else {
            return Ok(None);
        };
        let seal = decode_seal(staging_seal.encoded())?;
        let expected_references = u64::try_from(seal.logs.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or(RedisCompactionStagingError::Accounting)?;
        if seal.digest != staging_seal.digest()
            || expected_references != staging_seal.source_references()
        {
            return Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting));
        }
        Ok(Some(seal))
    }

    async fn load_staging_seal(
        &self,
        identity: &str,
    ) -> Result<Option<CompactionStagingSeal>, RedisCompactionStagingError> {
        let mut connection = self.connection.clone();
        let encoded: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(&self.keys.seals)
            .arg(identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let Some(encoded) = encoded else {
            return Ok(None);
        };
        let stored: StoredRoleSeal = serde_json::from_slice(&encoded)
            .map_err(|_| self.accounting_failure(RedisRoleCapabilityFailure::Accounting))?;
        if stored.encoded.is_empty()
            || !valid_digest(&stored.digest)
            || stored.source_references == 0
        {
            return Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting));
        }
        Ok(Some(stored.into()))
    }

    async fn load_archive(
        &self,
        identity: &str,
    ) -> Result<Option<Vec<u8>>, RedisCompactionStagingError> {
        let mut connection = self.connection.clone();
        let archive: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(&self.keys.archives)
            .arg(identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let Some(archive) = archive else {
            return Ok(None);
        };
        let digest: Option<String> = redis::cmd("HGET")
            .arg(&self.keys.archive_digests)
            .arg(identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if digest.as_deref() != Some(digest_hex(&archive).as_str()) {
            return Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting));
        }
        Ok(Some(archive))
    }

    async fn commit_mutation(
        &self,
        mutation: &ScriptMutation,
    ) -> Result<Vec<Vec<u8>>, RedisCompactionStagingError> {
        let mut connection = self.connection.clone();
        observe_boundary(CompactionBoundary::BeforeDurabilityProof);
        self.durability
            .execute(&mut connection, mutation)
            .await
            .map(|committed| committed.into_output())
            .map_err(|error| self.durability_error(error))
    }

    fn decode_state(
        &self,
        output: &[Vec<u8>],
    ) -> Result<RedisCompactionStagingQuotaState, RedisCompactionStagingError> {
        if output.len() != 7 {
            return Err(RedisCompactionStagingError::Accounting);
        }
        let parse = |value: &[u8]| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(RedisCompactionStagingError::Accounting)
        };
        Ok(self.quota_state_from(
            parse(&output[2])?,
            parse(&output[3])?,
            parse(&output[4])?,
            parse(&output[5])?,
            parse(&output[6])?,
        ))
    }

    fn quota_state_from(
        &self,
        used_bytes: u64,
        staged_envelopes: u64,
        pending_deliveries: u64,
        sealed_references: u64,
        archive_commits: u64,
    ) -> RedisCompactionStagingQuotaState {
        let pressure = if used_bytes >= self.config.hard_limit_bytes
            || staged_envelopes >= self.config.max_envelopes.get() as u64
            || sealed_references >= self.config.max_sealed_references.get() as u64
        {
            RedisQuotaPressure::HardLimit
        } else if used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisCompactionStagingQuotaState {
            used_bytes,
            staged_envelopes,
            pending_deliveries,
            sealed_references,
            archive_commits,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            max_envelopes: self.config.max_envelopes.get() as u64,
            max_sealed_references: self.config.max_sealed_references.get() as u64,
            pressure,
        }
    }

    async fn audit_quota(
        &self,
        connection: &mut MultiplexedConnection,
        state: RedisCompactionStagingQuotaState,
    ) -> Result<(), RedisCompactionStagingError> {
        let units: Vec<u64> = redis::cmd("HVALS")
            .arg(&self.keys.units)
            .query_async(&mut *connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let references: Vec<u64> = redis::cmd("HVALS")
            .arg(&self.keys.reference_counts)
            .query_async(&mut *connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let staged: u64 = redis::cmd("HLEN")
            .arg(&self.keys.digests)
            .query_async(&mut *connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let pending: u64 = redis::cmd("HLEN")
            .arg(&self.keys.pending)
            .query_async(&mut *connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let archives: u64 = redis::cmd("HLEN")
            .arg(&self.keys.archives)
            .query_async(connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let exact_units = units.into_iter().try_fold(0_u64, u64::checked_add);
        let exact_references = references.into_iter().try_fold(0_u64, u64::checked_add);
        if exact_units != Some(state.used_bytes)
            || exact_references != Some(state.sealed_references)
            || staged != state.staged_envelopes
            || pending != state.pending_deliveries
            || archives != state.archive_commits
        {
            return Err(self.accounting_failure(RedisRoleCapabilityFailure::Accounting));
        }
        Ok(())
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisCompactionStagingError {
        let failure = RedisMutationError::from_redis(error).failure();
        use crate::redis_durability::RedisMutationFailure;
        match failure {
            RedisMutationFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisMutationFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisCompactionStagingError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisCompactionStagingError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisCompactionStagingError::IdentityConflict
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
        RedisCompactionStagingError::Durability(error.failure())
    }

    fn accounting_failure(
        &self,
        failure: RedisRoleCapabilityFailure,
    ) -> RedisCompactionStagingError {
        self.capability.report_failure(failure);
        RedisCompactionStagingError::Accounting
    }
}

struct RedisCompactionRoleDelivery {
    staging: RedisCompactionStaging,
    delivery: RedisCompactionDelivery,
}

impl CompactionStagingDelivery for RedisCompactionRoleDelivery {
    fn payload(&self) -> &[u8] {
        self.delivery.payload()
    }

    fn load_seal(&self) -> CompactionFuture<'_, Result<Option<CompactionStagingSeal>, String>> {
        Box::pin(async move {
            self.staging
                .load_staging_seal(self.delivery.identity())
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn record_seal<'a>(
        &'a self,
        seal: &'a CompactionStagingSeal,
    ) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.staging
                .record_staging_seal(self.delivery.identity(), seal)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn load_archive_identity(&self) -> CompactionFuture<'_, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move {
            self.staging
                .load_archive(self.delivery.identity())
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn record_archive_identity<'a>(
        &'a self,
        archive_identity: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.staging
                .record_archive(self.delivery.identity(), archive_identity)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn complete<'a>(
        self: Box<Self>,
        archive_identity: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.staging
                .complete(&self.delivery, archive_identity)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> CompactionFuture<'static, Result<(), String>> {
        Box::pin(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            drop(self);
            Ok(())
        })
    }
}

impl CompactionStaging for RedisCompactionStaging {
    fn prepare(&self) -> CompactionFuture<'_, Result<(), String>> {
        Box::pin(async move { self.prepare_role().await.map_err(|error| error.to_string()) })
    }

    fn stage<'a>(
        &'a self,
        encoded_compaction: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            RedisCompactionStaging::stage(self, encoded_compaction.to_vec())
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }

    fn next(
        &self,
    ) -> CompactionFuture<'_, Result<Option<Box<dyn CompactionStagingDelivery>>, String>> {
        Box::pin(async move {
            self.claim_next()
                .await
                .map(|delivery| {
                    delivery.map(|delivery| {
                        Box::new(RedisCompactionRoleDelivery {
                            staging: self.clone(),
                            delivery,
                        }) as Box<dyn CompactionStagingDelivery>
                    })
                })
                .map_err(|error| error.to_string())
        })
    }
}

struct ReconstructionCompactionCapability;

impl RedisCompactionStagingCapability for ReconstructionCompactionCapability {
    fn guard_admission(&self) -> Result<u64, RedisCompactionStagingError> {
        Ok(0)
    }

    fn guard_acknowledgement(&self, _generation: u64) -> Result<(), RedisCompactionStagingError> {
        Ok(())
    }

    fn report_failure(&self, _failure: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _state: RedisCompactionStagingQuotaState) {}
}

pub(crate) struct RedisCompactionStagingRoleRegistration {
    connection: MultiplexedConnection,
    config: RedisCompactionStagingConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisCompactionStagingRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisCompactionStagingConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisCompactionStagingError> {
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
        capability: Arc<dyn RedisCompactionStagingCapability>,
    ) -> Result<RedisCompactionStaging, RedisCompactionStagingError> {
        RedisCompactionStaging::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
        .await
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::CompactionStaging
            && context.manifest_identity() == &self.manifest_identity
            && RedisCompactionStaging::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisCompactionStagingRoleRegistration {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::Accounting);
        }
        let key = format!(
            "tickr:{{{}}}:compaction-staging:stream",
            self.config.namespace
        );
        let mut connection = self.connection.clone();
        redis::cmd("EVAL")
            .arg("return redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', 1)")
            .arg(1)
            .arg(key)
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
            "tickr:{{{}}}:log-staging:stream-index",
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
impl RedisReconstructionCallback for RedisCompactionStagingRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        let adapter = RedisCompactionStaging::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            Arc::new(ReconstructionCompactionCapability),
        )
        .await
        .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)?;
        adapter
            .prepare_role()
            .await
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub fn redis_compaction_staging_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(
        COMPACTION_STAGING_SCRIPT_NAME,
        COMPACTION_STAGING_SCRIPT_SHA256,
    )?;
    let stream_pattern = RedisNamespacePattern::key("tickr:{namespace}:compaction-staging:stream");
    RedisOperationManifest::new(
        CoordinationRole::CompactionStaging,
        REDIS_COMPACTION_STAGING_PROTOCOL,
        REDIS_COMPACTION_STAGING_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:compaction-staging:stream",
            "tickr:{namespace}:compaction-staging:digests",
            "tickr:{namespace}:compaction-staging:entries",
            "tickr:{namespace}:compaction-staging:payloads",
            "tickr:{namespace}:compaction-staging:units",
            "tickr:{namespace}:compaction-staging:pending",
            "tickr:{namespace}:compaction-staging:seals",
            "tickr:{namespace}:compaction-staging:archives",
            "tickr:{namespace}:compaction-staging:archive-digests",
            "tickr:{namespace}:compaction-staging:quota",
            "tickr:{namespace}:compaction-staging:completed",
            "tickr:{namespace}:compaction-staging:reference-counts",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            stream_pattern,
        )],
        vec![
            RedisForbiddenOperation::cross_role(
                RedisOperation::script(script),
                CoordinationRole::LogStaging,
            ),
            RedisForbiddenOperation::administrative("FLUSHALL"),
        ],
    )
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredScopeSnapshot {
    scope_id: Uuid,
    bytes: Vec<u8>,
    digest: String,
    row_count: usize,
    value_bytes: usize,
}

impl From<&TickrCtxScopeSnapshot> for StoredScopeSnapshot {
    fn from(snapshot: &TickrCtxScopeSnapshot) -> Self {
        Self {
            scope_id: snapshot.scope_id,
            bytes: snapshot.bytes.clone(),
            digest: snapshot.digest.clone(),
            row_count: snapshot.row_count,
            value_bytes: snapshot.value_bytes,
        }
    }
}

impl From<StoredScopeSnapshot> for TickrCtxScopeSnapshot {
    fn from(snapshot: StoredScopeSnapshot) -> Self {
        Self {
            scope_id: snapshot.scope_id,
            bytes: snapshot.bytes,
            digest: snapshot.digest,
            row_count: snapshot.row_count,
            value_bytes: snapshot.value_bytes,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct StoredRoleSeal {
    encoded: Vec<u8>,
    digest: String,
    source_references: u64,
}

impl From<&CompactionStagingSeal> for StoredRoleSeal {
    fn from(seal: &CompactionStagingSeal) -> Self {
        Self {
            encoded: seal.encoded().to_vec(),
            digest: seal.digest().to_owned(),
            source_references: seal.source_references(),
        }
    }
}

impl From<StoredRoleSeal> for CompactionStagingSeal {
    fn from(seal: StoredRoleSeal) -> Self {
        Self::new(seal.encoded, seal.digest, seal.source_references)
    }
}

#[derive(Deserialize, Serialize)]
struct StoredCompactionSeal {
    scope: StoredScopeSnapshot,
    logs: Vec<LogSeal>,
    digest: String,
}

fn build_seal(
    scope: TickrCtxScopeSnapshot,
    mut logs: Vec<LogSeal>,
) -> Result<RedisCompactionSeal, RedisCompactionStagingError> {
    logs.sort_by(|left, right| left.stream().cmp(right.stream()));
    let content = (&StoredScopeSnapshot::from(&scope), &logs);
    let encoded =
        serde_json::to_vec(&content).map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
    Ok(RedisCompactionSeal {
        scope,
        logs,
        digest: digest_hex(&encoded),
    })
}

fn encode_seal(seal: &RedisCompactionSeal) -> Result<Vec<u8>, RedisCompactionStagingError> {
    serde_json::to_vec(&StoredCompactionSeal {
        scope: StoredScopeSnapshot::from(&seal.scope),
        logs: seal.logs.clone(),
        digest: seal.digest.clone(),
    })
    .map_err(|_| RedisCompactionStagingError::InvalidOperation)
}

fn decode_seal(encoded: &[u8]) -> Result<RedisCompactionSeal, RedisCompactionStagingError> {
    let stored: StoredCompactionSeal =
        serde_json::from_slice(encoded).map_err(|_| RedisCompactionStagingError::Accounting)?;
    let seal = RedisCompactionSeal {
        scope: stored.scope.into(),
        logs: stored.logs,
        digest: stored.digest,
    };
    let rebuilt = build_seal(seal.scope.clone(), seal.logs.clone())?;
    if rebuilt.digest != seal.digest {
        return Err(RedisCompactionStagingError::Accounting);
    }
    Ok(seal)
}

fn validate_log_inventory(
    projection: &tickr_proto::archive::ArchiveProjection,
    streams: &[RedisLogStagingStream],
) -> Result<(), RedisCompactionStagingError> {
    let expected = projection
        .task_instances
        .iter()
        .map(|task| Uuid::parse_str(&task.id))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
    let actual = streams
        .iter()
        .map(|stream| stream.identity().task_instance_id)
        .collect::<std::collections::BTreeSet<_>>();
    if expected == actual && expected.len() == streams.len() {
        Ok(())
    } else {
        Err(RedisCompactionStagingError::LogInventory)
    }
}

fn compaction_identity(payload: &[u8]) -> Result<String, RedisCompactionStagingError> {
    let envelope =
        decode_envelope(payload).map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
    let projection = envelope
        .projection
        .as_ref()
        .ok_or(RedisCompactionStagingError::InvalidOperation)?;
    Uuid::parse_str(&projection.id).map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
    Ok(projection.id.clone())
}

struct ScriptMutation {
    operation: RedisStableOperation,
    keys: RedisCompactionStagingKeys,
    kind: MutationKind,
    config: MutationConfig,
}

enum MutationKind {
    EnsureGroup,
    Stage {
        identity: String,
        digest: String,
        payload: Vec<u8>,
        units: u64,
    },
    Claim {
        stream_id: String,
        identity: String,
        digest: String,
        payload: Vec<u8>,
        units: u64,
    },
    Seal {
        identity: String,
        digest: String,
        payload: Vec<u8>,
        units: u64,
        reference_count: u64,
    },
    Archive {
        identity: String,
        archive_digest: String,
        payload: Vec<u8>,
        units: u64,
    },
    Complete {
        stream_id: String,
        identity: String,
        digest: String,
        archive_digest: String,
    },
}

impl ScriptMutation {
    fn ensure_group(
        keys: &RedisCompactionStagingKeys,
        config: &RedisCompactionStagingConfig,
    ) -> Result<Self, RedisCompactionStagingError> {
        Self::new(
            keys,
            keys.operation("ensure", "group"),
            b"ensure-group".to_vec(),
            MutationKind::EnsureGroup,
            config,
        )
    }

    fn stage(
        keys: &RedisCompactionStagingKeys,
        identity: String,
        digest: String,
        payload: Vec<u8>,
        units: u64,
        config: &RedisCompactionStagingConfig,
    ) -> Result<Self, RedisCompactionStagingError> {
        Self::new(
            keys,
            keys.operation(&identity, "stage"),
            digest.as_bytes().to_vec(),
            MutationKind::Stage {
                identity,
                digest,
                payload,
                units,
            },
            config,
        )
    }

    fn claim(
        keys: &RedisCompactionStagingKeys,
        delivery: &RedisCompactionDelivery,
        config: &RedisCompactionStagingConfig,
    ) -> Result<Self, RedisCompactionStagingError> {
        let stable = format!(
            "{}:{}:{}:{}",
            delivery.stream_id, delivery.identity, delivery.payload_digest, delivery.units
        )
        .into_bytes();
        Self::new(
            keys,
            keys.operation(&delivery.stream_id, "claim"),
            stable,
            MutationKind::Claim {
                stream_id: delivery.stream_id.clone(),
                identity: delivery.identity.clone(),
                digest: delivery.payload_digest.clone(),
                payload: delivery.payload.clone(),
                units: delivery.units,
            },
            config,
        )
    }

    fn seal(
        keys: &RedisCompactionStagingKeys,
        identity: &str,
        digest: String,
        payload: Vec<u8>,
        units: u64,
        reference_count: u64,
        config: &RedisCompactionStagingConfig,
    ) -> Result<Self, RedisCompactionStagingError> {
        Self::new(
            keys,
            keys.operation(identity, "seal"),
            digest.as_bytes().to_vec(),
            MutationKind::Seal {
                identity: identity.to_owned(),
                digest,
                payload,
                units,
                reference_count,
            },
            config,
        )
    }

    fn archive(
        keys: &RedisCompactionStagingKeys,
        identity: &str,
        archive_digest: String,
        payload: Vec<u8>,
        units: u64,
        config: &RedisCompactionStagingConfig,
    ) -> Result<Self, RedisCompactionStagingError> {
        Self::new(
            keys,
            keys.operation(identity, "archive"),
            archive_digest.as_bytes().to_vec(),
            MutationKind::Archive {
                identity: identity.to_owned(),
                archive_digest,
                payload,
                units,
            },
            config,
        )
    }

    fn complete(
        keys: &RedisCompactionStagingKeys,
        delivery: &RedisCompactionDelivery,
        archive_digest: String,
        config: &RedisCompactionStagingConfig,
    ) -> Result<Self, RedisCompactionStagingError> {
        let stable = format!(
            "{}:{}:{}:{}",
            delivery.stream_id, delivery.identity, delivery.payload_digest, archive_digest
        )
        .into_bytes();
        Self::new(
            keys,
            keys.operation(&delivery.identity, "complete"),
            stable,
            MutationKind::Complete {
                stream_id: delivery.stream_id.clone(),
                identity: delivery.identity.clone(),
                digest: delivery.payload_digest.clone(),
                archive_digest,
            },
            config,
        )
    }

    fn new(
        keys: &RedisCompactionStagingKeys,
        operation_key: String,
        stable_payload: Vec<u8>,
        kind: MutationKind,
        config: &RedisCompactionStagingConfig,
    ) -> Result<Self, RedisCompactionStagingError> {
        let operation = RedisStableOperation::new(operation_key, &stable_payload)
            .map_err(|_| RedisCompactionStagingError::InvalidOperation)?;
        Ok(Self {
            operation,
            keys: keys.clone(),
            kind,
            config: MutationConfig {
                max_envelopes: config.max_envelopes.get() as u64,
                hard_limit_bytes: config.hard_limit_bytes,
                max_sealed_references: config.max_sealed_references.get() as u64,
            },
        })
    }

    fn arguments(&self) -> (&str, &str, &str, &[u8], u64, &str, u64, &str) {
        match &self.kind {
            MutationKind::EnsureGroup => ("ensure_group", "", "", &[], 0, "", 0, ""),
            MutationKind::Stage {
                identity,
                digest,
                payload,
                units,
            } => ("stage", identity, digest, payload, *units, "", 0, ""),
            MutationKind::Claim {
                stream_id,
                identity,
                digest,
                payload,
                units,
            } => ("claim", identity, digest, payload, *units, stream_id, 0, ""),
            MutationKind::Seal {
                identity,
                digest,
                payload,
                units,
                reference_count,
            } => (
                "seal",
                identity,
                digest,
                payload,
                *units,
                "",
                *reference_count,
                "",
            ),
            MutationKind::Archive {
                identity,
                archive_digest,
                payload,
                units,
            } => (
                "archive",
                identity,
                "",
                payload,
                *units,
                "",
                0,
                archive_digest,
            ),
            MutationKind::Complete {
                stream_id,
                identity,
                digest,
                archive_digest,
            } => (
                "complete",
                identity,
                digest,
                &[],
                0,
                stream_id,
                0,
                archive_digest,
            ),
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
        let (operation, identity, digest, payload, units, stream_id, references, archive_digest) =
            self.arguments();
        let output: Vec<Vec<u8>> = redis::cmd("EVAL")
            .arg(COMPACTION_STAGING_SCRIPT)
            .arg(12)
            .arg(&self.keys.stream)
            .arg(&self.keys.digests)
            .arg(&self.keys.entries)
            .arg(&self.keys.payloads)
            .arg(&self.keys.units)
            .arg(&self.keys.pending)
            .arg(&self.keys.seals)
            .arg(&self.keys.archives)
            .arg(&self.keys.archive_digests)
            .arg(&self.keys.quota)
            .arg(&self.keys.completed)
            .arg(&self.keys.reference_counts)
            .arg(operation)
            .arg(REDIS_COMPACTION_STAGING_GROUP)
            .arg(identity)
            .arg(digest)
            .arg(payload)
            .arg(units)
            .arg(self.config.max_envelopes)
            .arg(self.config.hard_limit_bytes)
            .arg(stream_id)
            .arg(references)
            .arg(self.config.max_sealed_references)
            .arg(archive_digest)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        if matches!(self.kind, MutationKind::Stage { .. }) {
            observe_boundary(CompactionBoundary::AfterStagingMutation);
        }
        match output.first().map(Vec::as_slice) {
            Some(b"conflict") => Ok(RedisStableMutationOutcome::IdentityConflict),
            Some(b"replayed" | b"completed" | b"replayed_complete") => {
                Ok(RedisStableMutationOutcome::Replayed(output))
            }
            Some(
                b"created" | b"staged" | b"claimed" | b"sealed" | b"archived" | b"fenced"
                | b"missing" | b"trimmed" | b"accounting" | b"ineligible",
            ) => Ok(RedisStableMutationOutcome::Applied(output)),
            _ => Err(RedisMutationError::rejected()),
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let (_, identity, digest, payload, _, stream_id, _, archive_digest) = self.arguments();
        match &self.kind {
            MutationKind::EnsureGroup => Ok(RedisStableMutationRecovery::Missing),
            MutationKind::Stage { .. } => {
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
                let completed: Option<String> = redis::cmd("HGET")
                    .arg(&self.keys.completed)
                    .arg(identity)
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                Ok(match completed {
                    Some(actual) if actual.starts_with(digest) => {
                        RedisStableMutationRecovery::Matching
                    }
                    Some(_) => RedisStableMutationRecovery::IdentityConflict,
                    None => RedisStableMutationRecovery::Missing,
                })
            }
            MutationKind::Claim { .. } => {
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
            MutationKind::Seal { .. } => {
                let actual: Option<Vec<u8>> = redis::cmd("HGET")
                    .arg(&self.keys.seals)
                    .arg(identity)
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                Ok(match actual {
                    Some(actual) if actual == payload => RedisStableMutationRecovery::Matching,
                    Some(_) => RedisStableMutationRecovery::IdentityConflict,
                    None => RedisStableMutationRecovery::Missing,
                })
            }
            MutationKind::Archive { .. } => {
                let actual: Option<String> = redis::cmd("HGET")
                    .arg(&self.keys.archive_digests)
                    .arg(identity)
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                Ok(match actual {
                    Some(actual) if actual == archive_digest => {
                        RedisStableMutationRecovery::Matching
                    }
                    Some(_) => RedisStableMutationRecovery::IdentityConflict,
                    None => RedisStableMutationRecovery::Missing,
                })
            }
            MutationKind::Complete { .. } => {
                let actual: Option<String> = redis::cmd("HGET")
                    .arg(&self.keys.completed)
                    .arg(identity)
                    .query_async(connection)
                    .await
                    .map_err(RedisMutationError::from_redis)?;
                let expected = format!("{digest}:{archive_digest}");
                Ok(match actual {
                    Some(actual) if actual == expected => RedisStableMutationRecovery::Matching,
                    Some(_) => RedisStableMutationRecovery::IdentityConflict,
                    None => RedisStableMutationRecovery::Missing,
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
struct MutationConfig {
    max_envelopes: u64,
    hard_limit_bytes: u64,
    max_sealed_references: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisCompactionStagingError {
    InvalidConfiguration,
    InvalidOperation,
    IdentityConflict,
    CapacityFenced,
    MissingScope,
    InvalidScope,
    LogInventory,
    Archive,
    ArchiveNotCommitted,
    Accounting,
    Unavailable,
    Durability(RedisDurabilityFailure),
    Log(RedisLogStagingError),
    Scope(RedisScopeStoreError),
}

impl fmt::Display for RedisCompactionStagingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidConfiguration => "invalid Redis CompactionStaging configuration",
            Self::InvalidOperation => "invalid Redis CompactionStaging operation",
            Self::IdentityConflict => "Redis CompactionStaging identity conflict",
            Self::CapacityFenced => "Redis CompactionStaging capacity fenced",
            Self::MissingScope => "Redis CompactionStaging scope is missing",
            Self::InvalidScope => "Redis CompactionStaging scope is invalid",
            Self::LogInventory => "Redis CompactionStaging Log inventory is invalid",
            Self::Archive => "Redis CompactionStaging archive operation failed",
            Self::ArchiveNotCommitted => "Redis CompactionStaging archive is not committed",
            Self::Accounting => "Redis CompactionStaging accounting is inconsistent",
            Self::Unavailable => "Redis CompactionStaging is unavailable",
            Self::Durability(_) => "Redis CompactionStaging durability boundary failed",
            Self::Log(_) => "Redis CompactionStaging Log operation failed",
            Self::Scope(_) => "Redis CompactionStaging scope operation failed",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RedisCompactionStagingError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactionBoundary {
    BeforeStagingMutation,
    AfterStagingMutation,
    BeforeDurabilityProof,
    AfterDurabilityProof,
    BeforeCrossPlaneAcknowledgement,
    AfterCrossPlaneAcknowledgement,
    AfterDrainReceipt,
    BeforeScopeSeal,
    AfterScopeSeal,
    BeforeLogSeal,
    AfterLogSeal,
    BeforeArchiveWrite,
    AfterArchiveWrite,
    BeforeArchiveVerification,
    AfterArchiveVerification,
    BeforeArchiveCommit,
    AfterArchiveCommit,
    BeforeLogPurge,
    AfterLogPurge,
    BeforeScopePurge,
    AfterScopePurge,
    BeforeStagingCompletion,
    AfterStagingCompletion,
}

#[cfg(not(debug_assertions))]
#[inline]
const fn observe_boundary(_: CompactionBoundary) {}

#[cfg(debug_assertions)]
fn observe_boundary(boundary: CompactionBoundary) {
    let Ok(requested) = std::env::var("TICKR_REDIS_COMPACTION_CRASH_AT") else {
        return;
    };
    let actual = match boundary {
        CompactionBoundary::BeforeStagingMutation => "before-staging-mutation",
        CompactionBoundary::AfterStagingMutation => "after-staging-mutation",
        CompactionBoundary::BeforeDurabilityProof => "before-fsync-proof",
        CompactionBoundary::AfterDurabilityProof => "after-fsync-proof",
        CompactionBoundary::BeforeCrossPlaneAcknowledgement => "before-cross-plane-ack",
        CompactionBoundary::AfterCrossPlaneAcknowledgement => "after-cross-plane-ack",
        CompactionBoundary::AfterDrainReceipt => "after-drain-receipt",
        CompactionBoundary::BeforeScopeSeal => "before-scope-seal",
        CompactionBoundary::AfterScopeSeal => "after-scope-seal",
        CompactionBoundary::BeforeLogSeal => "before-log-seal",
        CompactionBoundary::AfterLogSeal => "after-log-seal",
        CompactionBoundary::BeforeArchiveWrite => "before-archive-write",
        CompactionBoundary::AfterArchiveWrite => "after-archive-write",
        CompactionBoundary::BeforeArchiveVerification => "before-archive-verification",
        CompactionBoundary::AfterArchiveVerification => "after-archive-verification",
        CompactionBoundary::BeforeArchiveCommit => "before-archive-commit",
        CompactionBoundary::AfterArchiveCommit => "after-archive-commit",
        CompactionBoundary::BeforeLogPurge => "before-log-purge",
        CompactionBoundary::AfterLogPurge => "after-log-purge",
        CompactionBoundary::BeforeScopePurge => "before-scope-purge",
        CompactionBoundary::AfterScopePurge => "after-scope-purge",
        CompactionBoundary::BeforeStagingCompletion => "before-staging-completion",
        CompactionBoundary::AfterStagingCompletion => "after-staging-completion",
    };
    if requested == actual {
        std::process::exit(86);
    }
}

fn valid_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 127
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_manifest_registers_every_runtime_operation() {
        let manifest = redis_compaction_staging_operation_manifest().expect("valid manifest");
        assert_eq!(manifest.commands(), REDIS_COMPACTION_STAGING_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), COMPACTION_STAGING_SCRIPT_NAME);
        assert_eq!(
            manifest.scripts()[0].sha256(),
            COMPACTION_STAGING_SCRIPT_SHA256
        );
        assert_eq!(
            digest_hex(COMPACTION_STAGING_SCRIPT.as_bytes()),
            COMPACTION_STAGING_SCRIPT_SHA256
        );
        let mut attempted = REDIS_COMPACTION_STAGING_COMMANDS.to_vec();
        attempted.push("PUBLISH");
        assert!(attempted
            .iter()
            .any(|operation| manifest.commands().binary_search(operation).is_err()));
    }
}
