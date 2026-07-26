use std::{collections::BTreeMap, fmt, num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickr_executor::log_stream::{LogStream, LogStreamProvider, LogStreamRoute};
use tickr_proto::coord::log_stream::{
    AcceptOutcome, AcceptedLogRecord, GapOutcome, LogExit, LogRecordSubmission, LogSeal,
    LogStreamIdentity, LogStreamState, LogTerminal, PreAcceptanceGap, ReplayedLogRecord,
    TerminalOutcome,
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
        RedisMutationFailure, RedisStableMutation, RedisStableMutationOutcome,
        RedisStableMutationRecovery, RedisStableOperation,
    },
    redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisRequiredOperationCanary,
        RedisScriptIdentity,
    },
};

pub const REDIS_LOG_STAGING_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.log-staging.redis-accepted-stream", 1);

const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;
const DEFAULT_SOFT_LIMIT_BYTES: u64 = 192 * 1024 * 1024;
const DEFAULT_HARD_LIMIT_BYTES: u64 = 256 * 1024 * 1024;
const STATE_ACCOUNTED_BYTES: u64 = 256;

const REDIS_LOG_STAGING_COMMANDS: &[&str] = &[
    "DEL", "EVAL", "GET", "HDEL", "HGET", "HGETALL", "HINCRBY", "HMGET", "HSET", "SET", "WAITAOF",
];
const LOG_STAGING_SCRIPT_NAME: &str = "log-staging-v1";
const LOG_STAGING_SCRIPT_SHA256: &str =
    "7d48a98b3c17035d169ded41fc50dece33444d0a2580869f90a89482a6ece5b9";

const LOG_STAGING_SCRIPT: &str = r#"local operation = ARGV[1]
local operation_identity = ARGV[2]
local operation_fingerprint = ARGV[3]
local expected_state = ARGV[4]
local next_state = ARGV[5]
local expected_metrics = ARGV[6]
local next_metrics = ARGV[7]
local stream_identity = ARGV[8]
local stream_document = ARGV[9]
local hard_limit = tonumber(ARGV[10])

local metric_fields = {
    'used_bytes',
    'accepted_records',
    'declared_gaps',
    'frontiers',
    'terminal_records',
    'sealed_streams',
    'archive_commits'
}

local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function state(status)
    return {
        status,
        tostring(number_field(KEYS[4], 'used_bytes')),
        tostring(number_field(KEYS[4], 'accepted_records')),
        tostring(number_field(KEYS[4], 'declared_gaps')),
        tostring(number_field(KEYS[4], 'frontiers')),
        tostring(number_field(KEYS[4], 'terminal_records')),
        tostring(number_field(KEYS[4], 'sealed_streams')),
        tostring(number_field(KEYS[4], 'archive_commits'))
    }
end

local purged = redis.call('HGET', KEYS[5], stream_identity)
if purged then
    if operation == 'purge' and purged == operation_fingerprint then
        return state('replayed_purge')
    end
    return state('conflict')
end

local prior = redis.call('HGET', KEYS[2], operation_identity)
if prior then
    if prior ~= operation_fingerprint then
        return state('conflict')
    end
    if not redis.call('GET', KEYS[1])
        or redis.call('HGET', KEYS[6], stream_identity) ~= stream_document then
        return state('accounting')
    end
    return state('replayed')
end

local current_state = redis.call('GET', KEYS[1]) or ''
local current_metrics = redis.call('HGET', KEYS[3], stream_identity) or ''
if current_state ~= expected_state then
    return state('stale')
end
if current_metrics ~= expected_metrics then
    return state('accounting')
end

local old = nil
if current_metrics ~= '' then
    old = cjson.decode(current_metrics)
end

if operation == 'purge' then
    if current_state == '' or not old then
        return state('ineligible')
    end
    local document = cjson.decode(current_state)
    if not document.seal or not document.archive_commit then
        return state('ineligible')
    end
    if document.seal.record_digest ~= document.archive_commit.record_digest
        or document.archive_commit.archive_identity_digest ~= operation_fingerprint then
        return state('ineligible')
    end
    for _, field in ipairs(metric_fields) do
        if number_field(KEYS[4], field) < tonumber(old[field]) then
            return state('accounting')
        end
    end
    redis.call('DEL', KEYS[1], KEYS[2])
    redis.call('HDEL', KEYS[3], stream_identity)
    redis.call('HDEL', KEYS[6], stream_identity)
    for _, field in ipairs(metric_fields) do
        redis.call('HINCRBY', KEYS[4], field, -tonumber(old[field]))
    end
    redis.call('HSET', KEYS[5], stream_identity, operation_fingerprint)
    return state('purged')
end

if next_state == '' or next_metrics == '' then
    return state('accounting')
end
local new = cjson.decode(next_metrics)
if tonumber(new.used_bytes) ~= string.len(next_state) + 256 then
    return state('accounting')
end
local old_used = old and tonumber(old.used_bytes) or 0
local projected = number_field(KEYS[4], 'used_bytes') - old_used + tonumber(new.used_bytes)
if projected > hard_limit then
    return state('fenced')
end
for _, field in ipairs(metric_fields) do
    local old_value = old and tonumber(old[field]) or 0
    if number_field(KEYS[4], field) < old_value then
        return state('accounting')
    end
end

redis.call('SET', KEYS[1], next_state)
redis.call('HSET', KEYS[2], operation_identity, operation_fingerprint)
redis.call('HSET', KEYS[6], stream_identity, stream_document)
redis.call('HSET', KEYS[3], stream_identity, next_metrics)
for _, field in ipairs(metric_fields) do
    local old_value = old and tonumber(old[field]) or 0
    redis.call('HINCRBY', KEYS[4], field, tonumber(new[field]) - old_value)
end
return state('applied')"#;

#[derive(Clone, Debug)]
pub struct RedisLogStagingConfig {
    pub namespace: String,
    pub max_record_bytes: NonZeroUsize,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
}

impl RedisLogStagingConfig {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            max_record_bytes: NonZeroUsize::new(DEFAULT_MAX_RECORD_BYTES)
                .expect("non-zero constant"),
            soft_limit_bytes: DEFAULT_SOFT_LIMIT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_LIMIT_BYTES,
        }
    }

    fn validate(&self) -> Result<(), RedisLogStagingError> {
        let valid_namespace = !self.namespace.is_empty()
            && self.namespace.len() <= 127
            && self
                .namespace
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
        if !valid_namespace
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_bytes
                <= STATE_ACCOUNTED_BYTES.saturating_add(self.max_record_bytes.get() as u64)
        {
            return Err(RedisLogStagingError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisLogStagingKeys {
    stream: String,
    operations: String,
    stream_metrics: String,
    quota: String,
    purged: String,
    stream_index: String,
    stream_identity: String,
    stream_document: Vec<u8>,
}

impl RedisLogStagingKeys {
    fn new(namespace: &str, identity: &LogStreamIdentity) -> Result<Self, RedisLogStagingError> {
        let encoded =
            serde_json::to_vec(identity).map_err(|_| RedisLogStagingError::InvalidOperation)?;
        let stream_identity = digest_hex(&encoded);
        let prefix = format!("tickr:{{{namespace}}}:log-staging");
        Ok(Self {
            stream: format!("{prefix}:streams:{stream_identity}"),
            operations: format!("{prefix}:operations:{stream_identity}"),
            stream_metrics: format!("{prefix}:stream-metrics"),
            quota: format!("{prefix}:quota"),
            purged: format!("{prefix}:purged"),
            stream_index: format!("{prefix}:stream-index"),
            stream_identity,
            stream_document: encoded,
        })
    }
}

pub trait RedisLogStagingCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisLogStagingError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisLogStagingError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisLogStagingQuotaState);
}

pub struct MonitoredRedisLogStagingCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisLogStagingCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisLogStagingCapability for MonitoredRedisLogStagingCapability {
    fn guard_admission(&self) -> Result<u64, RedisLogStagingError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisLogStagingError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisLogStagingError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisLogStagingError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisLogStagingError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisLogStagingQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RedisLogStagingQuotaState {
    pub used_bytes: u64,
    pub accepted_records: u64,
    pub declared_gaps: u64,
    pub frontiers: u64,
    pub terminal_records: u64,
    pub sealed_streams: u64,
    pub archive_commits: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisLogStagingQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self
                .accepted_records
                .saturating_add(self.declared_gaps)
                .saturating_add(self.frontiers)
                .saturating_add(self.terminal_records)
                .saturating_add(self.sealed_streams)
                .saturating_add(self.archive_commits),
            pressure: self.pressure,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct VerifiedArchiveCommit {
    record_digest: String,
    archive_identity_digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredLogStream {
    identity: LogStreamIdentity,
    accepted_records: Vec<AcceptedLogRecord>,
    declared_gaps: Vec<PreAcceptanceGap>,
    committed_frontier: Option<u64>,
    terminal: Option<LogTerminal>,
    seal: Option<LogSeal>,
    archive_commit: Option<VerifiedArchiveCommit>,
}

impl StoredLogStream {
    fn empty(identity: LogStreamIdentity) -> Self {
        Self {
            identity,
            accepted_records: Vec::new(),
            declared_gaps: Vec::new(),
            committed_frontier: None,
            terminal: None,
            seal: None,
            archive_commit: None,
        }
    }

    fn from_state(state: &LogStreamState) -> Self {
        Self {
            identity: state.identity().clone(),
            accepted_records: state.accepted_records(),
            declared_gaps: state.declared_gaps(),
            committed_frontier: state.committed_frontier(),
            terminal: state.terminal().cloned(),
            seal: None,
            archive_commit: None,
        }
    }

    fn state(&self) -> Result<LogStreamState, RedisLogStagingError> {
        let mut state = LogStreamState::new(self.identity.clone());
        for record in &self.accepted_records {
            state
                .apply_accepted(LogRecordSubmission {
                    identity: record.identity.clone(),
                    content_digest: record.content_digest.clone(),
                    bytes: record.bytes.clone(),
                })
                .map_err(|_| RedisLogStagingError::Accounting)?;
        }
        for gap in &self.declared_gaps {
            state
                .apply_gap(gap.clone())
                .map_err(|_| RedisLogStagingError::Accounting)?;
        }
        if let Some(terminal) = &self.terminal {
            state
                .apply_terminal(terminal.clone())
                .map_err(|_| RedisLogStagingError::Accounting)?;
        }
        if state.committed_frontier() != self.committed_frontier {
            return Err(RedisLogStagingError::Accounting);
        }
        match (&self.seal, &self.archive_commit) {
            (Some(seal), archive) => {
                let expected = state.seal().map_err(|_| RedisLogStagingError::Accounting)?;
                if seal != &expected
                    || archive
                        .as_ref()
                        .is_some_and(|commit| commit.record_digest != seal.record_digest())
                {
                    return Err(RedisLogStagingError::Accounting);
                }
            }
            (None, Some(_)) => return Err(RedisLogStagingError::Accounting),
            (None, None) => {}
        }
        Ok(state)
    }

    fn with_state(&self, state: &LogStreamState) -> Self {
        let mut stored = Self::from_state(state);
        stored.seal = self.seal.clone();
        stored.archive_commit = self.archive_commit.clone();
        stored
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct StoredMetrics {
    used_bytes: u64,
    accepted_records: u64,
    declared_gaps: u64,
    frontiers: u64,
    terminal_records: u64,
    sealed_streams: u64,
    archive_commits: u64,
}

impl StoredMetrics {
    fn for_encoded(stored: &StoredLogStream, encoded_len: usize) -> Self {
        Self {
            used_bytes: STATE_ACCOUNTED_BYTES.saturating_add(encoded_len as u64),
            accepted_records: stored.accepted_records.len() as u64,
            declared_gaps: stored.declared_gaps.len() as u64,
            frontiers: u64::from(
                !stored.accepted_records.is_empty() || !stored.declared_gaps.is_empty(),
            ),
            terminal_records: u64::from(stored.terminal.is_some()),
            sealed_streams: u64::from(stored.seal.is_some()),
            archive_commits: u64::from(stored.archive_commit.is_some()),
        }
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            used_bytes: self.used_bytes.checked_add(other.used_bytes)?,
            accepted_records: self.accepted_records.checked_add(other.accepted_records)?,
            declared_gaps: self.declared_gaps.checked_add(other.declared_gaps)?,
            frontiers: self.frontiers.checked_add(other.frontiers)?,
            terminal_records: self.terminal_records.checked_add(other.terminal_records)?,
            sealed_streams: self.sealed_streams.checked_add(other.sealed_streams)?,
            archive_commits: self.archive_commits.checked_add(other.archive_commits)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisArchiveCommitOutcome {
    Recorded,
    AlreadyRecorded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisLogPurgeOutcome {
    Purged,
    AlreadyPurged,
}

pub struct RedisLogStagingStream {
    connection: MultiplexedConnection,
    keys: RedisLogStagingKeys,
    config: Arc<RedisLogStagingConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisLogStagingCapability>,
    stored: StoredLogStream,
    state: LogStreamState,
    persisted: bool,
    purged: bool,
}

impl RedisLogStagingStream {
    pub async fn connect(
        client: redis::Client,
        identity: LogStreamIdentity,
        config: RedisLogStagingConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisLogStagingCapability>,
    ) -> Result<Self, RedisLogStagingError> {
        config.validate()?;
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisLogStagingError::Unavailable)?;
        Self::from_connection(
            connection,
            identity,
            Arc::new(config),
            durability,
            capability,
            true,
        )
        .await
    }

    async fn from_connection(
        connection: MultiplexedConnection,
        identity: LogStreamIdentity,
        config: Arc<RedisLogStagingConfig>,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisLogStagingCapability>,
        audit_quota: bool,
    ) -> Result<Self, RedisLogStagingError> {
        config.validate()?;
        let keys = RedisLogStagingKeys::new(&config.namespace, &identity)?;
        let stored = StoredLogStream::empty(identity.clone());
        let state = LogStreamState::new(identity);
        let mut stream = Self {
            connection,
            keys,
            config,
            durability,
            capability,
            stored,
            state,
            persisted: false,
            purged: false,
        };
        match stream.reload().await {
            Ok(()) | Err(RedisLogStagingError::Purged) => {}
            Err(error) => return Err(error),
        }
        if audit_quota {
            stream.quota_state().await?;
        }
        Ok(stream)
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_log_staging_operation_manifest()
    }

    pub fn identity(&self) -> &LogStreamIdentity {
        self.state.identity()
    }

    pub fn committed_frontier(&self) -> Option<u64> {
        self.state.committed_frontier()
    }

    pub async fn accept(
        &mut self,
        submission: LogRecordSubmission,
    ) -> Result<AcceptOutcome, RedisLogStagingError> {
        if submission.bytes.is_empty()
            || submission.bytes.len() > self.config.max_record_bytes.get()
        {
            return Err(RedisLogStagingError::InvalidOperation);
        }
        self.reload().await?;
        let mut prospective = self.state.clone();
        let outcome = prospective
            .apply_accepted(submission.clone())
            .map_err(|_| RedisLogStagingError::IdentityConflict)?;
        let next = self.stored.with_state(&prospective);
        let operation_identity = format!("accept:{}", submission.identity.sequence);
        let stable_payload =
            serde_json::to_vec(&submission).map_err(|_| RedisLogStagingError::InvalidOperation)?;
        let status = self
            .commit_state(operation_identity, stable_payload, next)
            .await?;
        match status {
            MutationStatus::Applied => Ok(outcome),
            MutationStatus::Replayed => Ok(AcceptOutcome::AlreadyAccepted),
            MutationStatus::Fenced => Err(RedisLogStagingError::CapacityFenced),
            _ => Err(self.status_error(status)),
        }
    }

    pub async fn declare_pre_acceptance_gap(
        &mut self,
        gap: PreAcceptanceGap,
    ) -> Result<GapOutcome, RedisLogStagingError> {
        self.reload().await?;
        let mut prospective = self.state.clone();
        let outcome = prospective
            .apply_gap(gap.clone())
            .map_err(|_| RedisLogStagingError::IdentityConflict)?;
        let next = self.stored.with_state(&prospective);
        let operation_identity = format!("gap:{}", gap.first_sequence);
        let stable_payload =
            serde_json::to_vec(&gap).map_err(|_| RedisLogStagingError::InvalidOperation)?;
        let status = self
            .commit_state(operation_identity, stable_payload, next)
            .await?;
        match status {
            MutationStatus::Applied => Ok(outcome),
            MutationStatus::Replayed => Ok(GapOutcome::AlreadyDeclared),
            MutationStatus::Fenced => Err(RedisLogStagingError::CapacityFenced),
            _ => Err(self.status_error(status)),
        }
    }

    pub async fn finish_cleanly(
        &mut self,
        exit: LogExit,
    ) -> Result<TerminalOutcome, RedisLogStagingError> {
        self.write_terminal(LogTerminal::EndOfStream { exit }).await
    }

    pub async fn recover_abnormal_closure(
        &mut self,
    ) -> Result<TerminalOutcome, RedisLogStagingError> {
        self.reload().await?;
        if self.state.terminal().is_some() {
            return Ok(TerminalOutcome::AlreadyRecorded);
        }
        let terminal = LogTerminal::AbnormalClosure {
            committed_frontier: self.state.committed_frontier(),
        };
        self.write_terminal_from_loaded(terminal).await
    }

    pub async fn replay(&mut self) -> Result<Vec<ReplayedLogRecord>, RedisLogStagingError> {
        self.reload().await?;
        Ok(self.state.replay())
    }

    pub async fn seal(&mut self) -> Result<LogSeal, RedisLogStagingError> {
        self.reload().await?;
        let seal = self
            .state
            .seal()
            .map_err(|_| RedisLogStagingError::InvalidOperation)?;
        if self
            .stored
            .seal
            .as_ref()
            .is_some_and(|stored| stored != &seal)
        {
            return Err(RedisLogStagingError::Accounting);
        }
        let mut next = self.stored.clone();
        next.seal = Some(seal.clone());
        let status = self
            .commit_state(
                "seal".to_owned(),
                seal.record_digest().as_bytes().to_vec(),
                next,
            )
            .await?;
        match status {
            MutationStatus::Applied | MutationStatus::Replayed => Ok(seal),
            MutationStatus::Fenced => Err(RedisLogStagingError::CapacityFenced),
            _ => Err(self.status_error(status)),
        }
    }

    pub async fn record_verified_archive_commit(
        &mut self,
        seal: &LogSeal,
        archive_identity: &[u8],
    ) -> Result<RedisArchiveCommitOutcome, RedisLogStagingError> {
        if archive_identity.is_empty() {
            return Err(RedisLogStagingError::InvalidOperation);
        }
        self.reload().await?;
        if self.stored.seal.as_ref() != Some(seal) {
            return Err(RedisLogStagingError::ArchiveNotCommitted);
        }
        let archive_identity_digest = digest_hex(archive_identity);
        let commit = VerifiedArchiveCommit {
            record_digest: seal.record_digest().to_owned(),
            archive_identity_digest: archive_identity_digest.clone(),
        };
        let already_recorded = self.stored.archive_commit.as_ref() == Some(&commit);
        if self.stored.archive_commit.is_some() && !already_recorded {
            return Err(RedisLogStagingError::IdentityConflict);
        }
        let mut next = self.stored.clone();
        next.archive_commit = Some(commit);
        let status = self
            .commit_state("archive".to_owned(), archive_identity.to_vec(), next)
            .await?;
        match status {
            MutationStatus::Applied if !already_recorded => Ok(RedisArchiveCommitOutcome::Recorded),
            MutationStatus::Applied | MutationStatus::Replayed => {
                Ok(RedisArchiveCommitOutcome::AlreadyRecorded)
            }
            MutationStatus::Fenced => Err(RedisLogStagingError::CapacityFenced),
            _ => Err(self.status_error(status)),
        }
    }

    pub async fn purge_after_verified_archive_commit(
        &mut self,
        seal: &LogSeal,
        archive_identity: &[u8],
    ) -> Result<RedisLogPurgeOutcome, RedisLogStagingError> {
        if archive_identity.is_empty() || seal.stream() != self.identity() {
            return Err(RedisLogStagingError::InvalidOperation);
        }
        if !self.purged {
            match self.reload().await {
                Ok(()) => {}
                Err(RedisLogStagingError::Purged) => {}
                Err(error) => return Err(error),
            }
        }
        if !self.purged {
            let archive_identity_digest = digest_hex(archive_identity);
            let expected = VerifiedArchiveCommit {
                record_digest: seal.record_digest().to_owned(),
                archive_identity_digest,
            };
            if self.stored.seal.as_ref() != Some(seal)
                || self.stored.archive_commit.as_ref() != Some(&expected)
            {
                return Err(RedisLogStagingError::ArchiveNotCommitted);
            }
        }
        let generation = self.capability.guard_admission()?;
        let mutation = ScriptMutation::purge(
            &self.keys,
            &self.stored,
            self.persisted,
            archive_identity.to_vec(),
            self.config.hard_limit_bytes,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let status = decode_status(&output)?;
        let quota = self.decode_quota(&output)?;
        self.capability.report_quota(quota);
        self.capability.guard_acknowledgement(generation)?;
        match status {
            MutationStatus::Purged => {
                self.persisted = false;
                self.purged = true;
                Ok(RedisLogPurgeOutcome::Purged)
            }
            MutationStatus::ReplayedPurge => {
                self.persisted = false;
                self.purged = true;
                Ok(RedisLogPurgeOutcome::AlreadyPurged)
            }
            _ => Err(self.status_error(status)),
        }
    }

    pub async fn quota_state(&self) -> Result<RedisLogStagingQuotaState, RedisLogStagingError> {
        let mut connection = self.connection.clone();
        let aggregate = read_metrics(&mut connection, &self.keys.quota).await?;
        let entries: Vec<(String, String)> = redis::cmd("HGETALL")
            .arg(&self.keys.stream_metrics)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let mut exact = StoredMetrics {
            used_bytes: 0,
            accepted_records: 0,
            declared_gaps: 0,
            frontiers: 0,
            terminal_records: 0,
            sealed_streams: 0,
            archive_commits: 0,
        };
        for (_, encoded) in entries {
            let metrics: StoredMetrics =
                serde_json::from_str(&encoded).map_err(|_| self.accounting_failure())?;
            exact = exact
                .checked_add(metrics)
                .ok_or_else(|| self.accounting_failure())?;
        }
        if aggregate != exact {
            return Err(self.accounting_failure());
        }
        let state = self.quota_from(aggregate);
        self.capability.report_quota(state);
        Ok(state)
    }

    async fn write_terminal(
        &mut self,
        terminal: LogTerminal,
    ) -> Result<TerminalOutcome, RedisLogStagingError> {
        self.reload().await?;
        self.write_terminal_from_loaded(terminal).await
    }

    async fn write_terminal_from_loaded(
        &mut self,
        terminal: LogTerminal,
    ) -> Result<TerminalOutcome, RedisLogStagingError> {
        let mut prospective = self.state.clone();
        let outcome = prospective
            .apply_terminal(terminal.clone())
            .map_err(|_| RedisLogStagingError::IdentityConflict)?;
        let next = self.stored.with_state(&prospective);
        let stable_payload =
            serde_json::to_vec(&terminal).map_err(|_| RedisLogStagingError::InvalidOperation)?;
        let status = self
            .commit_state("terminal".to_owned(), stable_payload, next)
            .await?;
        match status {
            MutationStatus::Applied => Ok(outcome),
            MutationStatus::Replayed => Ok(TerminalOutcome::AlreadyRecorded),
            MutationStatus::Fenced => Err(RedisLogStagingError::CapacityFenced),
            _ => Err(self.status_error(status)),
        }
    }

    async fn commit_state(
        &mut self,
        operation_identity: String,
        stable_payload: Vec<u8>,
        next: StoredLogStream,
    ) -> Result<MutationStatus, RedisLogStagingError> {
        let generation = self.capability.guard_admission()?;
        let mutation = ScriptMutation::replace(
            &self.keys,
            operation_identity,
            stable_payload,
            &self.stored,
            self.persisted,
            next,
            self.config.hard_limit_bytes,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let status = decode_status(&output)?;
        let quota = self.decode_quota(&output)?;
        self.capability.report_quota(quota);
        match status {
            MutationStatus::Applied | MutationStatus::Replayed => self.reload().await?,
            _ => {}
        }
        self.capability.guard_acknowledgement(generation)?;
        Ok(status)
    }

    async fn commit_mutation(
        &self,
        mutation: &ScriptMutation,
    ) -> Result<Vec<Vec<u8>>, RedisLogStagingError> {
        let mut connection = self.connection.clone();
        self.durability
            .execute(&mut connection, mutation)
            .await
            .map(|committed| committed.into_output())
            .map_err(|error| self.durability_error(error))
    }

    async fn reload(&mut self) -> Result<(), RedisLogStagingError> {
        if self.purged {
            return Err(RedisLogStagingError::Purged);
        }
        let mut connection = self.connection.clone();
        let (encoded, purged): (Option<Vec<u8>>, Option<String>) = redis::pipe()
            .cmd("GET")
            .arg(&self.keys.stream)
            .cmd("HGET")
            .arg(&self.keys.purged)
            .arg(&self.keys.stream_identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        if purged.is_some() {
            self.purged = true;
            self.persisted = false;
            return Err(RedisLogStagingError::Purged);
        }
        let Some(encoded) = encoded else {
            self.persisted = false;
            self.stored = StoredLogStream::empty(self.state.identity().clone());
            self.state = LogStreamState::new(self.state.identity().clone());
            return Ok(());
        };
        let stored: StoredLogStream =
            serde_json::from_slice(&encoded).map_err(|_| self.accounting_failure())?;
        if stored.identity != *self.state.identity() {
            return Err(self.accounting_failure());
        }
        let state = stored.state()?;
        let actual_metrics: Option<String> = redis::cmd("HGET")
            .arg(&self.keys.stream_metrics)
            .arg(&self.keys.stream_identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let expected_metrics = StoredMetrics::for_encoded(&stored, encoded.len());
        if actual_metrics
            .as_deref()
            .and_then(|value| serde_json::from_str::<StoredMetrics>(value).ok())
            != Some(expected_metrics)
        {
            return Err(self.accounting_failure());
        }
        self.stored = stored;
        self.state = state;
        self.persisted = true;
        Ok(())
    }

    fn decode_quota(
        &self,
        output: &[Vec<u8>],
    ) -> Result<RedisLogStagingQuotaState, RedisLogStagingError> {
        if output.len() != 8 {
            return Err(self.accounting_failure());
        }
        let parse = |value: &[u8]| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| self.accounting_failure())
        };
        Ok(self.quota_from(StoredMetrics {
            used_bytes: parse(&output[1])?,
            accepted_records: parse(&output[2])?,
            declared_gaps: parse(&output[3])?,
            frontiers: parse(&output[4])?,
            terminal_records: parse(&output[5])?,
            sealed_streams: parse(&output[6])?,
            archive_commits: parse(&output[7])?,
        }))
    }

    fn quota_from(&self, metrics: StoredMetrics) -> RedisLogStagingQuotaState {
        let pressure = if metrics.used_bytes >= self.config.hard_limit_bytes {
            RedisQuotaPressure::HardLimit
        } else if metrics.used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisLogStagingQuotaState {
            used_bytes: metrics.used_bytes,
            accepted_records: metrics.accepted_records,
            declared_gaps: metrics.declared_gaps,
            frontiers: metrics.frontiers,
            terminal_records: metrics.terminal_records,
            sealed_streams: metrics.sealed_streams,
            archive_commits: metrics.archive_commits,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            pressure,
        }
    }

    fn status_error(&self, status: MutationStatus) -> RedisLogStagingError {
        match status {
            MutationStatus::Conflict => RedisLogStagingError::IdentityConflict,
            MutationStatus::Fenced => RedisLogStagingError::CapacityFenced,
            MutationStatus::Ineligible => RedisLogStagingError::ArchiveNotCommitted,
            MutationStatus::Stale => RedisLogStagingError::Unavailable,
            MutationStatus::Accounting => self.accounting_failure(),
            MutationStatus::Applied
            | MutationStatus::Replayed
            | MutationStatus::Purged
            | MutationStatus::ReplayedPurge => RedisLogStagingError::Accounting,
        }
    }

    fn accounting_failure(&self) -> RedisLogStagingError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisLogStagingError::Accounting
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisLogStagingError {
        match RedisMutationError::from_redis(error).failure() {
            RedisMutationFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisMutationFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisLogStagingError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisLogStagingError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisLogStagingError::IdentityConflict
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
        RedisLogStagingError::Durability(error.failure())
    }
}

#[async_trait]
impl LogStream for RedisLogStagingStream {
    fn identity(&self) -> &LogStreamIdentity {
        RedisLogStagingStream::identity(self)
    }

    fn committed_frontier(&self) -> Option<u64> {
        RedisLogStagingStream::committed_frontier(self)
    }

    async fn accept(&mut self, submission: LogRecordSubmission) -> anyhow::Result<AcceptOutcome> {
        Ok(RedisLogStagingStream::accept(self, submission).await?)
    }

    async fn declare_pre_acceptance_gap(
        &mut self,
        gap: PreAcceptanceGap,
    ) -> anyhow::Result<GapOutcome> {
        Ok(RedisLogStagingStream::declare_pre_acceptance_gap(self, gap).await?)
    }

    async fn finish_cleanly(&mut self, exit: LogExit) -> anyhow::Result<TerminalOutcome> {
        Ok(RedisLogStagingStream::finish_cleanly(self, exit).await?)
    }

    async fn recover_abnormal_closure(&mut self) -> anyhow::Result<TerminalOutcome> {
        Ok(RedisLogStagingStream::recover_abnormal_closure(self).await?)
    }

    async fn replay(&mut self) -> anyhow::Result<Vec<ReplayedLogRecord>> {
        Ok(RedisLogStagingStream::replay(self).await?)
    }
}

async fn audit_persisted_streams(
    connection: &MultiplexedConnection,
    config: &RedisLogStagingConfig,
) -> Result<Vec<LogStreamIdentity>, RedisLogStagingError> {
    let prefix = format!("tickr:{{{}}}:log-staging", config.namespace);
    let stream_index = format!("{prefix}:stream-index");
    let stream_metrics = format!("{prefix}:stream-metrics");
    let quota = format!("{prefix}:quota");
    let mut connection = connection.clone();
    let indexed: Vec<(String, Vec<u8>)> = redis::cmd("HGETALL")
        .arg(&stream_index)
        .query_async(&mut connection)
        .await
        .map_err(|_| RedisLogStagingError::Unavailable)?;
    let metrics: Vec<(String, String)> = redis::cmd("HGETALL")
        .arg(&stream_metrics)
        .query_async(&mut connection)
        .await
        .map_err(|_| RedisLogStagingError::Unavailable)?;
    let mut metrics_by_stream = metrics.into_iter().collect::<BTreeMap<_, _>>();
    let mut exact = StoredMetrics::default();
    let mut identities = Vec::with_capacity(indexed.len());

    for (stream_key, encoded_identity) in indexed {
        let identity: LogStreamIdentity = serde_json::from_slice(&encoded_identity)
            .map_err(|_| RedisLogStagingError::Accounting)?;
        let keys = RedisLogStagingKeys::new(&config.namespace, &identity)?;
        if keys.stream_identity != stream_key || keys.stream_document != encoded_identity {
            return Err(RedisLogStagingError::Accounting);
        }
        let (encoded, actual_metrics, purged): (Option<Vec<u8>>, Option<String>, Option<String>) =
            redis::pipe()
                .cmd("GET")
                .arg(&keys.stream)
                .cmd("HGET")
                .arg(&keys.stream_metrics)
                .arg(&keys.stream_identity)
                .cmd("HGET")
                .arg(&keys.purged)
                .arg(&keys.stream_identity)
                .query_async(&mut connection)
                .await
                .map_err(|_| RedisLogStagingError::Unavailable)?;
        let encoded = encoded.ok_or(RedisLogStagingError::Accounting)?;
        if purged.is_some() {
            return Err(RedisLogStagingError::Accounting);
        }
        let stored: StoredLogStream =
            serde_json::from_slice(&encoded).map_err(|_| RedisLogStagingError::Accounting)?;
        if stored.identity != identity {
            return Err(RedisLogStagingError::Accounting);
        }
        stored.state()?;
        let expected_metrics = StoredMetrics::for_encoded(&stored, encoded.len());
        let indexed_metrics = metrics_by_stream
            .remove(&stream_key)
            .and_then(|value| serde_json::from_str::<StoredMetrics>(&value).ok());
        let actual_metrics =
            actual_metrics.and_then(|value| serde_json::from_str::<StoredMetrics>(&value).ok());
        if indexed_metrics != Some(expected_metrics) || actual_metrics != Some(expected_metrics) {
            return Err(RedisLogStagingError::Accounting);
        }
        exact = exact
            .checked_add(expected_metrics)
            .ok_or(RedisLogStagingError::Accounting)?;
        identities.push(identity);
    }

    if !metrics_by_stream.is_empty() || read_metrics(&mut connection, &quota).await? != exact {
        return Err(RedisLogStagingError::Accounting);
    }
    identities.sort();
    Ok(identities)
}

/// Formation-selected Redis LogStaging entry point. Production Task and API
/// components receive only the common provider contract.
#[derive(Clone)]
pub struct RedisLogStreamProvider {
    connection: MultiplexedConnection,
    config: Arc<RedisLogStagingConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisLogStagingCapability>,
}

impl RedisLogStreamProvider {
    pub async fn connect(
        client: redis::Client,
        config: RedisLogStagingConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisLogStagingCapability>,
    ) -> Result<Self, RedisLogStagingError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisLogStagingError::Unavailable)?;
        Self::new(connection, config, durability, capability)
    }

    fn new(
        connection: MultiplexedConnection,
        config: RedisLogStagingConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisLogStagingCapability>,
    ) -> Result<Self, RedisLogStagingError> {
        config.validate()?;
        Ok(Self {
            connection,
            config: Arc::new(config),
            durability,
            capability,
        })
    }
}

#[async_trait]
impl LogStreamProvider for RedisLogStreamProvider {
    async fn prepare(&self) -> anyhow::Result<()> {
        audit_persisted_streams(&self.connection, &self.config)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn open(
        &self,
        route: LogStreamRoute,
        identity: LogStreamIdentity,
    ) -> anyhow::Result<Box<dyn LogStream>> {
        if route.task_instance_id != identity.task_instance_id {
            return Err(anyhow::anyhow!("Log route does not match stream identity"));
        }
        Ok(Box::new(
            RedisLogStagingStream::from_connection(
                self.connection.clone(),
                identity,
                self.config.clone(),
                self.durability,
                self.capability.clone(),
                true,
            )
            .await?,
        ))
    }

    async fn replay_task(&self, route: LogStreamRoute) -> anyhow::Result<Vec<ReplayedLogRecord>> {
        let identities = audit_persisted_streams(&self.connection, &self.config).await?;
        let mut records = Vec::new();
        for identity in identities
            .into_iter()
            .filter(|identity| identity.task_instance_id == route.task_instance_id)
        {
            let mut stream = RedisLogStagingStream::from_connection(
                self.connection.clone(),
                identity,
                self.config.clone(),
                self.durability,
                self.capability.clone(),
                false,
            )
            .await?;
            records.extend(stream.replay().await?);
        }
        Ok(records)
    }

    async fn seal_task_for_compaction(
        &self,
        route: LogStreamRoute,
    ) -> anyhow::Result<Vec<LogSeal>> {
        let identities = audit_persisted_streams(&self.connection, &self.config).await?;
        let mut seals = Vec::new();
        for identity in identities
            .into_iter()
            .filter(|identity| identity.task_instance_id == route.task_instance_id)
        {
            let mut stream = RedisLogStagingStream::from_connection(
                self.connection.clone(),
                identity,
                self.config.clone(),
                self.durability,
                self.capability.clone(),
                false,
            )
            .await?;
            let seal = match stream.seal().await {
                Ok(seal) => seal,
                Err(RedisLogStagingError::InvalidOperation) => {
                    stream.recover_abnormal_closure().await?;
                    stream.seal().await?
                }
                Err(error) => return Err(error.into()),
            };
            seals.push(seal);
        }
        seals.sort_by(|left, right| left.stream().cmp(right.stream()));
        Ok(seals)
    }

    async fn record_verified_archive_commit(
        &self,
        seals: &[LogSeal],
        archive_identity: &[u8],
    ) -> anyhow::Result<()> {
        for seal in seals {
            let mut stream = RedisLogStagingStream::from_connection(
                self.connection.clone(),
                seal.stream().clone(),
                self.config.clone(),
                self.durability,
                self.capability.clone(),
                false,
            )
            .await?;
            stream
                .record_verified_archive_commit(seal, archive_identity)
                .await?;
        }
        Ok(())
    }

    async fn purge_after_verified_archive_commit(
        &self,
        seals: &[LogSeal],
        archive_identity: &[u8],
    ) -> anyhow::Result<()> {
        for seal in seals {
            let mut stream = RedisLogStagingStream::from_connection(
                self.connection.clone(),
                seal.stream().clone(),
                self.config.clone(),
                self.durability,
                self.capability.clone(),
                false,
            )
            .await?;
            stream
                .purge_after_verified_archive_commit(seal, archive_identity)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl tickr_conductor::system_tasks::CompactionLogStaging for RedisLogStreamProvider {
    async fn seal_task(
        &self,
        workflow_id: uuid::Uuid,
        workflow_instance_id: uuid::Uuid,
        task_instance_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<LogSeal>> {
        <Self as LogStreamProvider>::seal_task_for_compaction(
            self,
            LogStreamRoute {
                workflow_id,
                workflow_instance_id,
                task_instance_id,
            },
        )
        .await
    }

    async fn purge_task_after_archive(
        &self,
        _workflow_id: uuid::Uuid,
        _workflow_instance_id: uuid::Uuid,
        _task_instance_id: uuid::Uuid,
        seals: &[LogSeal],
        archive_identity: &[u8],
    ) -> anyhow::Result<()> {
        <Self as LogStreamProvider>::record_verified_archive_commit(self, seals, archive_identity)
            .await?;
        <Self as LogStreamProvider>::purge_after_verified_archive_commit(
            self,
            seals,
            archive_identity,
        )
        .await
    }
}

/// Admitted LogStaging role registered before readiness and released only as
/// the common provider contract.
pub(crate) struct RedisLogStagingRoleRegistration {
    connection: MultiplexedConnection,
    config: RedisLogStagingConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisLogStagingRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisLogStagingConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisLogStagingError> {
        config.validate()?;
        Ok(Self {
            connection,
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) fn build_provider(
        &self,
        capability: Arc<dyn RedisLogStagingCapability>,
    ) -> Result<RedisLogStreamProvider, RedisLogStagingError> {
        RedisLogStreamProvider::new(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::LogStaging
            && context.manifest_identity() == &self.manifest_identity
            && RedisLogStagingStream::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisLogStagingRoleRegistration {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::Accounting);
        }
        let index = format!(
            "tickr:{{{}}}:log-staging:stream-index",
            self.config.namespace
        );
        let mut connection = self.connection.clone();
        redis::cmd("EVAL")
            .arg("return redis.call('HGET', KEYS[1], ARGV[1])")
            .arg(1)
            .arg(index)
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
impl RedisReconstructionCallback for RedisLogStagingRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        audit_persisted_streams(&self.connection, &self.config)
            .await
            .map(|_| ())
            .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub fn redis_log_staging_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(LOG_STAGING_SCRIPT_NAME, LOG_STAGING_SCRIPT_SHA256)?;
    RedisOperationManifest::new(
        CoordinationRole::LogStaging,
        REDIS_LOG_STAGING_PROTOCOL,
        REDIS_LOG_STAGING_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:log-staging:streams:*",
            "tickr:{namespace}:log-staging:operations:*",
            "tickr:{namespace}:log-staging:stream-metrics",
            "tickr:{namespace}:log-staging:quota",
            "tickr:{namespace}:log-staging:purged",
            "tickr:{namespace}:log-staging:stream-index",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            RedisNamespacePattern::key("tickr:{namespace}:log-staging:streams:*"),
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
    operation_identity: String,
    operation_fingerprint: String,
    operation_kind: &'static str,
    keys: RedisLogStagingKeys,
    expected_state: Vec<u8>,
    next_state: Vec<u8>,
    expected_metrics: Vec<u8>,
    next_metrics: Vec<u8>,
    hard_limit_bytes: u64,
}

impl ScriptMutation {
    fn replace(
        keys: &RedisLogStagingKeys,
        operation_identity: String,
        stable_payload: Vec<u8>,
        current: &StoredLogStream,
        persisted: bool,
        next: StoredLogStream,
        hard_limit_bytes: u64,
    ) -> Result<Self, RedisLogStagingError> {
        let expected_state = if persisted {
            serde_json::to_vec(current).map_err(|_| RedisLogStagingError::InvalidOperation)?
        } else {
            Vec::new()
        };
        let expected_metrics = if persisted {
            serde_json::to_vec(&StoredMetrics::for_encoded(current, expected_state.len()))
                .map_err(|_| RedisLogStagingError::InvalidOperation)?
        } else {
            Vec::new()
        };
        let next_state =
            serde_json::to_vec(&next).map_err(|_| RedisLogStagingError::InvalidOperation)?;
        let next_metrics = serde_json::to_vec(&StoredMetrics::for_encoded(&next, next_state.len()))
            .map_err(|_| RedisLogStagingError::InvalidOperation)?;
        Self::new(
            keys,
            "replace",
            operation_identity,
            stable_payload,
            expected_state,
            next_state,
            expected_metrics,
            next_metrics,
            hard_limit_bytes,
        )
    }

    fn purge(
        keys: &RedisLogStagingKeys,
        current: &StoredLogStream,
        persisted: bool,
        stable_payload: Vec<u8>,
        hard_limit_bytes: u64,
    ) -> Result<Self, RedisLogStagingError> {
        let expected_state = if persisted {
            serde_json::to_vec(current).map_err(|_| RedisLogStagingError::InvalidOperation)?
        } else {
            Vec::new()
        };
        let expected_metrics = if persisted {
            serde_json::to_vec(&StoredMetrics::for_encoded(current, expected_state.len()))
                .map_err(|_| RedisLogStagingError::InvalidOperation)?
        } else {
            Vec::new()
        };
        Self::new(
            keys,
            "purge",
            "purge".to_owned(),
            stable_payload,
            expected_state,
            Vec::new(),
            expected_metrics,
            Vec::new(),
            hard_limit_bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        keys: &RedisLogStagingKeys,
        operation_kind: &'static str,
        operation_identity: String,
        stable_payload: Vec<u8>,
        expected_state: Vec<u8>,
        next_state: Vec<u8>,
        expected_metrics: Vec<u8>,
        next_metrics: Vec<u8>,
        hard_limit_bytes: u64,
    ) -> Result<Self, RedisLogStagingError> {
        let operation_key = format!("{}#{operation_identity}", keys.operations);
        let operation = RedisStableOperation::new(operation_key, &stable_payload)
            .map_err(|_| RedisLogStagingError::InvalidOperation)?;
        Ok(Self {
            operation,
            operation_identity,
            operation_fingerprint: digest_hex(&stable_payload),
            operation_kind,
            keys: keys.clone(),
            expected_state,
            next_state,
            expected_metrics,
            next_metrics,
            hard_limit_bytes,
        })
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
        let output: Vec<Vec<u8>> = redis::cmd("EVAL")
            .arg(LOG_STAGING_SCRIPT)
            .arg(6)
            .arg(&self.keys.stream)
            .arg(&self.keys.operations)
            .arg(&self.keys.stream_metrics)
            .arg(&self.keys.quota)
            .arg(&self.keys.purged)
            .arg(&self.keys.stream_index)
            .arg(self.operation_kind)
            .arg(&self.operation_identity)
            .arg(&self.operation_fingerprint)
            .arg(&self.expected_state)
            .arg(&self.next_state)
            .arg(&self.expected_metrics)
            .arg(&self.next_metrics)
            .arg(&self.keys.stream_identity)
            .arg(&self.keys.stream_document)
            .arg(self.hard_limit_bytes)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        match decode_status(&output).map_err(|_| RedisMutationError::rejected())? {
            MutationStatus::Conflict => Ok(RedisStableMutationOutcome::IdentityConflict),
            MutationStatus::Replayed | MutationStatus::ReplayedPurge => {
                Ok(RedisStableMutationOutcome::Replayed(output))
            }
            MutationStatus::Applied
            | MutationStatus::Fenced
            | MutationStatus::Purged
            | MutationStatus::Ineligible
            | MutationStatus::Stale
            | MutationStatus::Accounting => Ok(RedisStableMutationOutcome::Applied(output)),
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        if self.operation_kind == "purge" {
            let purged: Option<String> = redis::cmd("HGET")
                .arg(&self.keys.purged)
                .arg(&self.keys.stream_identity)
                .query_async(&mut *connection)
                .await
                .map_err(RedisMutationError::from_redis)?;
            if let Some(purged) = purged {
                return Ok(if purged == self.operation_fingerprint {
                    RedisStableMutationRecovery::Matching
                } else {
                    RedisStableMutationRecovery::IdentityConflict
                });
            }
        }
        let actual: Option<String> = redis::cmd("HGET")
            .arg(&self.keys.operations)
            .arg(&self.operation_identity)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        Ok(match actual {
            Some(actual) if actual == self.operation_fingerprint => {
                RedisStableMutationRecovery::Matching
            }
            Some(_) => RedisStableMutationRecovery::IdentityConflict,
            None => RedisStableMutationRecovery::Missing,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationStatus {
    Applied,
    Replayed,
    Fenced,
    Conflict,
    Purged,
    ReplayedPurge,
    Ineligible,
    Stale,
    Accounting,
}

fn decode_status(output: &[Vec<u8>]) -> Result<MutationStatus, RedisLogStagingError> {
    match output.first().map(Vec::as_slice) {
        Some(b"applied") => Ok(MutationStatus::Applied),
        Some(b"replayed") => Ok(MutationStatus::Replayed),
        Some(b"fenced") => Ok(MutationStatus::Fenced),
        Some(b"conflict") => Ok(MutationStatus::Conflict),
        Some(b"purged") => Ok(MutationStatus::Purged),
        Some(b"replayed_purge") => Ok(MutationStatus::ReplayedPurge),
        Some(b"ineligible") => Ok(MutationStatus::Ineligible),
        Some(b"stale") => Ok(MutationStatus::Stale),
        Some(b"accounting") => Ok(MutationStatus::Accounting),
        _ => Err(RedisLogStagingError::Accounting),
    }
}

async fn read_metrics(
    connection: &mut MultiplexedConnection,
    quota_key: &str,
) -> Result<StoredMetrics, RedisLogStagingError> {
    let values: Vec<Option<u64>> = redis::cmd("HMGET")
        .arg(quota_key)
        .arg(&[
            "used_bytes",
            "accepted_records",
            "declared_gaps",
            "frontiers",
            "terminal_records",
            "sealed_streams",
            "archive_commits",
        ])
        .query_async(connection)
        .await
        .map_err(|_| RedisLogStagingError::Unavailable)?;
    if values.len() != 7 {
        return Err(RedisLogStagingError::Accounting);
    }
    Ok(StoredMetrics {
        used_bytes: values[0].unwrap_or(0),
        accepted_records: values[1].unwrap_or(0),
        declared_gaps: values[2].unwrap_or(0),
        frontiers: values[3].unwrap_or(0),
        terminal_records: values[4].unwrap_or(0),
        sealed_streams: values[5].unwrap_or(0),
        archive_commits: values[6].unwrap_or(0),
    })
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisLogStagingError {
    InvalidConfiguration,
    InvalidOperation,
    IdentityConflict,
    CapacityFenced,
    ArchiveNotCommitted,
    Purged,
    Accounting,
    Unavailable,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisLogStagingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidConfiguration => "invalid Redis LogStaging configuration",
            Self::InvalidOperation => "invalid Redis LogStaging operation",
            Self::IdentityConflict => "Redis LogStaging identity conflict",
            Self::CapacityFenced => "Redis LogStaging capacity fenced",
            Self::ArchiveNotCommitted => "Redis LogStaging archive commit is not verified",
            Self::Purged => "Redis LogStaging stream was purged",
            Self::Accounting => "Redis LogStaging accounting is inconsistent",
            Self::Unavailable => "Redis LogStaging is unavailable",
            Self::Durability(_) => "Redis LogStaging durability boundary failed",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RedisLogStagingError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_manifest_registers_every_runtime_operation() {
        let manifest = redis_log_staging_operation_manifest().expect("valid manifest");
        assert_eq!(manifest.commands(), REDIS_LOG_STAGING_COMMANDS);
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), LOG_STAGING_SCRIPT_NAME);
        assert_eq!(manifest.scripts()[0].sha256(), LOG_STAGING_SCRIPT_SHA256);
        assert_eq!(
            digest_hex(LOG_STAGING_SCRIPT.as_bytes()),
            LOG_STAGING_SCRIPT_SHA256
        );

        let mut attempted = REDIS_LOG_STAGING_COMMANDS.to_vec();
        attempted.push("XADD");
        assert!(attempted
            .iter()
            .any(|operation| manifest.commands().binary_search(operation).is_err()));
    }
}
