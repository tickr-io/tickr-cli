use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, DeleteTickrCtxScopeInput, ScopeBoundViolation, ScopeCleanupOutcome,
    ScopeCreationOutcome, ScopeDeleteOutcome, ScopeEnvelopeRejection, ScopeMutationRejection,
    ScopeReadOutcome, ScopeSnapshotOutcome, ScopeStore, ScopeStoreError as BoxedScopeStoreError,
    ScopeStoreFuture, ScopeValueInput, ScopeWriteOutcome, StoredScopeValue, TickrCtxScopeSnapshot,
    TickrCtxScopeState, WriteTickrCtxScopeInput, MAX_SCOPE_AGE_SECONDS, MAX_SCOPE_BYTES,
    MAX_SCOPE_REQUEST_BYTES, MAX_SCOPE_REQUEST_VALUES, MAX_SCOPE_ROWS, MAX_SCOPE_VALUE_BYTES,
    TICKR_CTX_SCOPE_PROTOCOL_VERSION,
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
        RedisMutationFailure, RedisStableMutation, RedisStableMutationOutcome,
        RedisStableMutationRecovery, RedisStableOperation,
    },
    redis_operation_manifest::{
        RedisForbiddenOperation, RedisNamespacePattern, RedisOperation, RedisOperationManifest,
        RedisOperationManifestError, RedisOperationManifestIdentity, RedisRequiredOperationCanary,
        RedisScriptIdentity,
    },
};

pub const REDIS_SCOPE_STORE_PROTOCOL: ProtocolIdentity =
    ProtocolIdentity::new("tickr.scope-store.redis-opaque-snapshot", 1);

const MAX_FORMATION_NAMESPACE_BYTES: usize = 127;
const MAX_SCOPE_NAMESPACE_BYTES: usize = 128;
const MAX_RUN_ID_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 512;
const DEFAULT_SOFT_LIMIT_BYTES: u64 = 192 * 1024 * 1024;
const DEFAULT_HARD_LIMIT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_HARD_LIMIT_SCOPES: u64 = 32_768;
const DEFAULT_HARD_LIMIT_VALUES: u64 = 1_048_576;
const OPERATION_ACCOUNTED_BYTES: u64 = 96;
const SNAPSHOT_MAGIC: &[u8] = b"TICKR_CTX_SCOPE\0\x01";

const REDIS_SCOPE_STORE_COMMANDS: &[&str] = &[
    "DEL", "EVAL", "GET", "HDEL", "HGET", "HGETALL", "HINCRBY", "HMGET", "HSET", "SET", "WAITAOF",
];
const SCOPE_STORE_SCRIPT_NAME: &str = "scope-store-v1";
const SCOPE_STORE_SCRIPT_SHA256: &str =
    "0de299966fcef24ede65c723df409b667c30e30b7285366ec676d4443e7c2189";

const SCOPE_STORE_SCRIPT: &str = r#"local operation_kind = ARGV[1]
local operation_identity = ARGV[2]
local operation_fingerprint = ARGV[3]
local scope_identity = ARGV[4]
local owner_identity = ARGV[5]
local expected_state = ARGV[6]
local next_state = ARGV[7]
local expected_metrics = ARGV[8]
local next_metrics = ARGV[9]
local hard_limit_bytes = tonumber(ARGV[10])
local hard_limit_scopes = tonumber(ARGV[11])
local hard_limit_values = tonumber(ARGV[12])
local enforce_limit = ARGV[13] == '1'

local metric_fields = {
    'used_bytes',
    'namespace_records',
    'scope_values',
    'operation_records',
    'snapshots',
    'archive_commits'
}

local function number_field(key, field)
    return tonumber(redis.call('HGET', key, field) or '0')
end

local function result(status, detail)
    return {
        status,
        detail or '',
        tostring(number_field(KEYS[4], 'used_bytes')),
        tostring(number_field(KEYS[4], 'namespace_records')),
        tostring(number_field(KEYS[4], 'scope_values')),
        tostring(number_field(KEYS[4], 'operation_records')),
        tostring(number_field(KEYS[4], 'snapshots')),
        tostring(number_field(KEYS[4], 'archive_commits'))
    }
end

local claim_value = scope_identity .. '|' .. operation_fingerprint
local prior_claim = redis.call('HGET', KEYS[6], operation_identity)
if prior_claim then
    if prior_claim == claim_value then
        if operation_kind == 'cleanup' and not redis.call('HGET', KEYS[7], scope_identity) then
            return result('accounting')
        end
        return result(operation_kind == 'cleanup' and 'replayed_cleanup' or 'replayed')
    end
    return result('claim_conflict')
end

local cleaned = redis.call('HGET', KEYS[7], scope_identity)
if cleaned then
    return result('cleaned')
end

local current_state = redis.call('GET', KEYS[1]) or ''
local current_metrics = redis.call('HGET', KEYS[3], scope_identity) or ''
if operation_kind == 'create' then
    local owner = redis.call('HGET', KEYS[5], owner_identity)
    if owner and owner ~= scope_identity then
        return result('collision', owner)
    end
elseif current_state == '' then
    return result('missing')
end
if current_state ~= expected_state then
    return result('stale')
end
if current_metrics ~= expected_metrics then
    return result('accounting')
end

local old = nil
if current_metrics ~= '' then
    old = cjson.decode(current_metrics)
end

if operation_kind == 'cleanup' then
    if current_state == '' or not old or next_state == '' then
        return result('accounting')
    end
    for _, field in ipairs(metric_fields) do
        if number_field(KEYS[4], field) < tonumber(old[field]) then
            return result('accounting')
        end
    end
    redis.call('DEL', KEYS[1])
    redis.call('HDEL', KEYS[3], scope_identity)
    for _, field in ipairs(metric_fields) do
        redis.call('HINCRBY', KEYS[4], field, -tonumber(old[field]))
    end
    redis.call('HSET', KEYS[6], operation_identity, claim_value)
    redis.call('HSET', KEYS[7], scope_identity, next_state)
    return result('cleaned')
end

if next_state == '' or next_metrics == '' then
    return result('accounting')
end
local new = cjson.decode(next_metrics)
local projected = {}
for _, field in ipairs(metric_fields) do
    local old_value = old and tonumber(old[field]) or 0
    if number_field(KEYS[4], field) < old_value then
        return result('accounting')
    end
    projected[field] = number_field(KEYS[4], field) - old_value + tonumber(new[field])
end
if enforce_limit and (projected.used_bytes > hard_limit_bytes
    or projected.namespace_records > hard_limit_scopes
    or projected.scope_values > hard_limit_values) then
    return result('fenced')
end

redis.call('SET', KEYS[1], next_state)
redis.call('HSET', KEYS[3], scope_identity, next_metrics)
redis.call('HSET', KEYS[6], operation_identity, claim_value)
if operation_kind == 'create' then
    redis.call('HSET', KEYS[5], owner_identity, scope_identity)
end
for _, field in ipairs(metric_fields) do
    local old_value = old and tonumber(old[field]) or 0
    redis.call('HINCRBY', KEYS[4], field, tonumber(new[field]) - old_value)
end
return result('applied')"#;

#[derive(Clone, Debug)]
pub struct RedisScopeStoreConfig {
    pub formation_namespace: String,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub hard_limit_scopes: u64,
    pub hard_limit_values: u64,
}

impl RedisScopeStoreConfig {
    pub fn new(formation_namespace: impl Into<String>) -> Self {
        Self {
            formation_namespace: formation_namespace.into(),
            soft_limit_bytes: DEFAULT_SOFT_LIMIT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_LIMIT_BYTES,
            hard_limit_scopes: DEFAULT_HARD_LIMIT_SCOPES,
            hard_limit_values: DEFAULT_HARD_LIMIT_VALUES,
        }
    }

    fn validate(&self) -> Result<(), RedisScopeStoreError> {
        let namespace = self.formation_namespace.as_bytes();
        if namespace.is_empty()
            || namespace.len() > MAX_FORMATION_NAMESPACE_BYTES
            || !namespace
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || self.soft_limit_bytes == 0
            || self.soft_limit_bytes >= self.hard_limit_bytes
            || self.hard_limit_scopes == 0
            || self.hard_limit_values == 0
        {
            return Err(RedisScopeStoreError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RedisScopeStoreKeys {
    prefix: String,
    scope_metrics: String,
    quota: String,
    owners: String,
    claims: String,
    cleaned: String,
}

impl RedisScopeStoreKeys {
    fn new(namespace: &str) -> Self {
        let prefix = format!("tickr:{{{namespace}}}:scope-store");
        Self {
            scope_metrics: format!("{prefix}:scope-metrics"),
            quota: format!("{prefix}:quota"),
            owners: format!("{prefix}:owners"),
            claims: format!("{prefix}:claims"),
            cleaned: format!("{prefix}:cleaned"),
            prefix,
        }
    }

    fn scope(&self, scope_id: Uuid) -> String {
        format!("{}:scopes:{scope_id}", self.prefix)
    }
}

pub trait RedisScopeStoreCapability: Send + Sync {
    fn guard_admission(&self) -> Result<u64, RedisScopeStoreError>;
    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisScopeStoreError>;
    fn report_failure(&self, failure: RedisRoleCapabilityFailure);
    fn report_quota(&self, state: RedisScopeStoreQuotaState);
}

pub struct MonitoredRedisScopeStoreCapability {
    fence: RedisGenerationFence,
    reporter: RedisRoleCapabilityReporter,
}

impl MonitoredRedisScopeStoreCapability {
    pub fn new(fence: RedisGenerationFence, reporter: RedisRoleCapabilityReporter) -> Self {
        Self { fence, reporter }
    }
}

impl RedisScopeStoreCapability for MonitoredRedisScopeStoreCapability {
    fn guard_admission(&self) -> Result<u64, RedisScopeStoreError> {
        self.fence
            .guard_admission()
            .map_err(|_| RedisScopeStoreError::Unavailable)?;
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open {
            return Err(RedisScopeStoreError::Unavailable);
        }
        Ok(snapshot.generation)
    }

    fn guard_acknowledgement(&self, generation: u64) -> Result<(), RedisScopeStoreError> {
        let snapshot = self.fence.snapshot();
        if snapshot.state != RedisCapabilityFenceState::Open || snapshot.generation != generation {
            return Err(RedisScopeStoreError::Unavailable);
        }
        Ok(())
    }

    fn report_failure(&self, failure: RedisRoleCapabilityFailure) {
        self.reporter.report(failure);
    }

    fn report_quota(&self, state: RedisScopeStoreQuotaState) {
        self.reporter.report_quota_state(state.role_projection());
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct StoredMetrics {
    used_bytes: u64,
    namespace_records: u64,
    scope_values: u64,
    operation_records: u64,
    snapshots: u64,
    archive_commits: u64,
}

impl StoredMetrics {
    fn for_scope(
        scope: &StoredScope,
        encoded_len: usize,
        operation_records: u64,
    ) -> Result<Self, RedisScopeStoreError> {
        let encoded_len =
            u64::try_from(encoded_len).map_err(|_| RedisScopeStoreError::Accounting)?;
        let operation_bytes = operation_records
            .checked_mul(OPERATION_ACCOUNTED_BYTES)
            .ok_or(RedisScopeStoreError::Accounting)?;
        Ok(Self {
            used_bytes: encoded_len
                .checked_add(operation_bytes)
                .ok_or(RedisScopeStoreError::Accounting)?,
            namespace_records: 1,
            scope_values: u64::try_from(scope.values.len())
                .map_err(|_| RedisScopeStoreError::Accounting)?,
            operation_records,
            snapshots: u64::from(scope.snapshot.is_some()),
            archive_commits: u64::from(scope.archive_commit.is_some()),
        })
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            used_bytes: self.used_bytes.checked_add(other.used_bytes)?,
            namespace_records: self
                .namespace_records
                .checked_add(other.namespace_records)?,
            scope_values: self.scope_values.checked_add(other.scope_values)?,
            operation_records: self
                .operation_records
                .checked_add(other.operation_records)?,
            snapshots: self.snapshots.checked_add(other.snapshots)?,
            archive_commits: self.archive_commits.checked_add(other.archive_commits)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RedisScopeStoreQuotaState {
    pub used_bytes: u64,
    pub namespace_records: u64,
    pub scope_values: u64,
    pub operation_records: u64,
    pub snapshots: u64,
    pub archive_commits: u64,
    pub soft_limit_bytes: u64,
    pub hard_limit_bytes: u64,
    pub hard_limit_scopes: u64,
    pub hard_limit_values: u64,
    pub pressure: RedisQuotaPressure,
}

impl RedisScopeStoreQuotaState {
    fn role_projection(self) -> RedisQuotaState {
        RedisQuotaState {
            used: self.used_bytes,
            soft_threshold: self.soft_limit_bytes,
            hard_limit: self.hard_limit_bytes,
            accepted_identities: self
                .namespace_records
                .saturating_add(self.scope_values)
                .saturating_add(self.operation_records)
                .saturating_add(self.snapshots)
                .saturating_add(self.archive_commits),
            pressure: self.pressure,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredScopeState {
    Active,
    Snapshotted,
    Cleaned,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredValue {
    value_identity: String,
    envelope: Vec<u8>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredSnapshot {
    bytes: Vec<u8>,
    digest: String,
    row_count: usize,
    value_bytes: usize,
}

impl StoredSnapshot {
    fn public(&self, scope_id: Uuid) -> TickrCtxScopeSnapshot {
        TickrCtxScopeSnapshot {
            scope_id,
            bytes: self.bytes.clone(),
            digest: self.digest.clone(),
            row_count: self.row_count,
            value_bytes: self.value_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredArchiveCommit {
    snapshot_digest: String,
    archive_identity_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StoredScope {
    protocol_version: i64,
    scope_id: Uuid,
    namespace: String,
    run_id: String,
    creation_claim_id: Uuid,
    creation_request_digest: String,
    state: StoredScopeState,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    values: BTreeMap<String, StoredValue>,
    snapshot: Option<StoredSnapshot>,
    archive_commit: Option<StoredArchiveCommit>,
}

impl StoredScope {
    fn public_snapshot(&self) -> Result<TickrCtxScopeSnapshot, RedisScopeStoreError> {
        let snapshot = self
            .snapshot
            .as_ref()
            .ok_or(RedisScopeStoreError::CorruptScope)?;
        validate_snapshot(snapshot)?;
        Ok(snapshot.public(self.scope_id))
    }

    fn cleaned_proof(&self) -> Self {
        let mut cleaned = self.clone();
        cleaned.state = StoredScopeState::Cleaned;
        cleaned.values.clear();
        cleaned
    }
}

#[derive(Clone)]
pub struct RedisScopeStore {
    connection: MultiplexedConnection,
    keys: RedisScopeStoreKeys,
    config: Arc<RedisScopeStoreConfig>,
    durability: RedisDurabilityGuard,
    capability: Arc<dyn RedisScopeStoreCapability>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisScopeArchiveCommitOutcome {
    Recorded,
    AlreadyRecorded,
}

impl RedisScopeStore {
    pub async fn connect(
        client: redis::Client,
        config: RedisScopeStoreConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisScopeStoreCapability>,
    ) -> Result<Self, RedisScopeStoreError> {
        let connection = client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|_| RedisScopeStoreError::Unavailable)?;
        Self::from_connection(connection, config, durability, capability).await
    }

    pub(crate) async fn from_connection(
        connection: MultiplexedConnection,
        config: RedisScopeStoreConfig,
        durability: RedisDurabilityGuard,
        capability: Arc<dyn RedisScopeStoreCapability>,
    ) -> Result<Self, RedisScopeStoreError> {
        config.validate()?;
        let store = Self {
            connection,
            keys: RedisScopeStoreKeys::new(&config.formation_namespace),
            config: Arc::new(config),
            durability,
            capability,
        };
        store.quota_state().await?;
        Ok(store)
    }

    pub fn operation_manifest() -> Result<RedisOperationManifest, RedisOperationManifestError> {
        redis_scope_store_operation_manifest()
    }

    pub async fn create_tickr_ctx_scope(
        &self,
        input: CreateTickrCtxScopeInput<'_>,
    ) -> Result<ScopeCreationOutcome, RedisScopeStoreError> {
        validate_scope_identity(input.namespace, input.run_id)?;
        let request_digest = mutation_digest(input.scope_id, input.values)?;
        if let Some(rejection) = validate_mutation(input.values, true) {
            return Ok(ScopeCreationOutcome::Rejected(rejection));
        }
        let operation_identity = claim_identity(input.claim_id);
        let stable_payload = create_payload(
            input.scope_id,
            input.namespace,
            input.run_id,
            &request_digest,
        );
        match self
            .claim_status(&operation_identity, input.scope_id, &stable_payload)
            .await?
        {
            ClaimStatus::Matching => {
                self.guard_existing_commit()?;
                return Ok(ScopeCreationOutcome::Idempotent);
            }
            ClaimStatus::Conflict => return Ok(ScopeCreationOutcome::ClaimConflict),
            ClaimStatus::Missing => {}
        }
        match self.load(input.scope_id).await? {
            LoadedScope::Current(LoadedCurrent {
                scope: existing, ..
            })
            | LoadedScope::Cleaned(existing) => {
                if existing.creation_claim_id == input.claim_id
                    && existing.namespace == input.namespace
                    && existing.run_id == input.run_id
                    && existing.creation_request_digest == request_digest
                {
                    self.guard_existing_commit()?;
                    return Ok(ScopeCreationOutcome::Idempotent);
                }
                return Ok(ScopeCreationOutcome::Collision {
                    existing_scope_id: existing.scope_id,
                });
            }
            LoadedScope::Missing => {}
        }

        let mut values = BTreeMap::new();
        for value in input.values {
            values.insert(
                value.key.to_owned(),
                StoredValue {
                    value_identity: value_identity(input.scope_id, value.key, value.envelope),
                    envelope: value.envelope.to_vec(),
                    created_at: input.now,
                    updated_at: input.now,
                },
            );
        }
        let next = StoredScope {
            protocol_version: TICKR_CTX_SCOPE_PROTOCOL_VERSION,
            scope_id: input.scope_id,
            namespace: input.namespace.to_owned(),
            run_id: input.run_id.to_owned(),
            creation_claim_id: input.claim_id,
            creation_request_digest: request_digest,
            state: StoredScopeState::Active,
            created_at: input.now,
            updated_at: input.now,
            values,
            snapshot: None,
            archive_commit: None,
        };
        let mutation = ScriptMutation::new(
            &self.keys,
            MutationKind::Create,
            operation_identity,
            stable_payload,
            input.scope_id,
            owner_identity(input.namespace, input.run_id),
            None,
            next,
            self.config.as_ref(),
            self.capability.guard_admission()?,
        )?;
        let output = self.commit_mutation(&mutation).await?;
        let status = decode_status(&output)?;
        self.finish_operation(&output, mutation.generation).await?;
        match status {
            MutationStatus::Applied => Ok(ScopeCreationOutcome::Created),
            MutationStatus::Replayed => Ok(ScopeCreationOutcome::Idempotent),
            MutationStatus::ClaimConflict => Ok(ScopeCreationOutcome::ClaimConflict),
            MutationStatus::Collision => {
                let existing_scope_id = output
                    .get(1)
                    .and_then(|value| std::str::from_utf8(value).ok())
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or_else(|| self.accounting_failure())?;
                Ok(ScopeCreationOutcome::Collision { existing_scope_id })
            }
            MutationStatus::Fenced => Err(RedisScopeStoreError::CapacityFenced),
            _ => Err(self.status_error(status)),
        }
    }

    pub async fn write_tickr_ctx_scope(
        &self,
        input: WriteTickrCtxScopeInput<'_>,
    ) -> Result<ScopeWriteOutcome, RedisScopeStoreError> {
        let request_digest = mutation_digest(input.scope_id, input.values)?;
        if let Some(rejection) = validate_mutation(input.values, false) {
            return Ok(ScopeWriteOutcome::Rejected(rejection));
        }
        let operation_identity = claim_identity(input.claim_id);
        let stable_payload = mutation_payload(b"write", input.scope_id, request_digest.as_bytes());
        match self
            .claim_status(&operation_identity, input.scope_id, &stable_payload)
            .await?
        {
            ClaimStatus::Matching => {
                self.guard_existing_commit()?;
                return Ok(ScopeWriteOutcome::Idempotent);
            }
            ClaimStatus::Conflict => return Ok(ScopeWriteOutcome::ClaimConflict),
            ClaimStatus::Missing => {}
        }

        for _ in 0..4 {
            let LoadedScope::Current(LoadedCurrent {
                scope: mut current,
                encoded: old_encoded,
                metrics: old_metrics,
            }) = self.load(input.scope_id).await?
            else {
                return Ok(ScopeWriteOutcome::Missing);
            };
            if current.state != StoredScopeState::Active {
                return Ok(ScopeWriteOutcome::NotWritable(public_state(current.state)));
            }
            if let Some(bound) = age_bound(current.created_at, input.now) {
                return Ok(ScopeWriteOutcome::Rejected(ScopeMutationRejection::Bound(
                    bound,
                )));
            }
            let mut inserted = 0;
            let mut updated = 0;
            for value in input.values {
                let previous = current.values.get(value.key);
                if previous.is_some() {
                    updated += 1;
                } else {
                    inserted += 1;
                }
                current.values.insert(
                    value.key.to_owned(),
                    StoredValue {
                        value_identity: value_identity(input.scope_id, value.key, value.envelope),
                        envelope: value.envelope.to_vec(),
                        created_at: previous.map_or(input.now, |stored| stored.created_at),
                        updated_at: input.now,
                    },
                );
            }
            if let Some(bound) = stored_bounds(&current.values) {
                return Ok(ScopeWriteOutcome::Rejected(ScopeMutationRejection::Bound(
                    bound,
                )));
            }
            current.updated_at = input.now;
            let mutation = ScriptMutation::new(
                &self.keys,
                MutationKind::Write,
                operation_identity.clone(),
                stable_payload.clone(),
                input.scope_id,
                String::new(),
                Some((old_encoded, old_metrics)),
                current,
                self.config.as_ref(),
                self.capability.guard_admission()?,
            )?;
            let output = self.commit_mutation(&mutation).await?;
            let status = decode_status(&output)?;
            self.finish_operation(&output, mutation.generation).await?;
            match status {
                MutationStatus::Applied => {
                    return Ok(ScopeWriteOutcome::Applied { inserted, updated });
                }
                MutationStatus::Replayed => return Ok(ScopeWriteOutcome::Idempotent),
                MutationStatus::ClaimConflict => return Ok(ScopeWriteOutcome::ClaimConflict),
                MutationStatus::Fenced => return Err(RedisScopeStoreError::CapacityFenced),
                MutationStatus::Stale => continue,
                MutationStatus::Missing | MutationStatus::Cleaned => {
                    return Ok(ScopeWriteOutcome::Missing);
                }
                _ => return Err(self.status_error(status)),
            }
        }
        Err(RedisScopeStoreError::Unavailable)
    }

    pub async fn delete_tickr_ctx_scope_value(
        &self,
        input: DeleteTickrCtxScopeInput<'_>,
    ) -> Result<ScopeDeleteOutcome, RedisScopeStoreError> {
        validate_key(input.key)?;
        let request_digest = delete_digest(input.scope_id, input.key);
        let operation_identity = claim_identity(input.claim_id);
        let stable_payload = mutation_payload(b"delete", input.scope_id, request_digest.as_bytes());
        match self
            .claim_status(&operation_identity, input.scope_id, &stable_payload)
            .await?
        {
            ClaimStatus::Matching => {
                self.guard_existing_commit()?;
                return Ok(ScopeDeleteOutcome::Idempotent);
            }
            ClaimStatus::Conflict => return Ok(ScopeDeleteOutcome::ClaimConflict),
            ClaimStatus::Missing => {}
        }

        for _ in 0..4 {
            let LoadedScope::Current(LoadedCurrent {
                scope: mut current,
                encoded: old_encoded,
                metrics: old_metrics,
            }) = self.load(input.scope_id).await?
            else {
                return Ok(ScopeDeleteOutcome::Missing);
            };
            if current.state != StoredScopeState::Active {
                return Ok(ScopeDeleteOutcome::NotWritable(public_state(current.state)));
            }
            if let Some(bound) = age_bound(current.created_at, input.now) {
                return Ok(ScopeDeleteOutcome::Bound(bound));
            }
            let deleted = current.values.remove(input.key).is_some();
            current.updated_at = input.now;
            let mutation = ScriptMutation::new(
                &self.keys,
                MutationKind::Delete,
                operation_identity.clone(),
                stable_payload.clone(),
                input.scope_id,
                String::new(),
                Some((old_encoded, old_metrics)),
                current,
                self.config.as_ref(),
                self.capability.guard_admission()?,
            )?;
            let output = self.commit_mutation(&mutation).await?;
            let status = decode_status(&output)?;
            self.finish_operation(&output, mutation.generation).await?;
            match status {
                MutationStatus::Applied => {
                    return Ok(if deleted {
                        ScopeDeleteOutcome::Deleted
                    } else {
                        ScopeDeleteOutcome::MissingKey
                    });
                }
                MutationStatus::Replayed => return Ok(ScopeDeleteOutcome::Idempotent),
                MutationStatus::ClaimConflict => return Ok(ScopeDeleteOutcome::ClaimConflict),
                MutationStatus::Stale => continue,
                MutationStatus::Missing | MutationStatus::Cleaned => {
                    return Ok(ScopeDeleteOutcome::Missing);
                }
                _ => return Err(self.status_error(status)),
            }
        }
        Err(RedisScopeStoreError::Unavailable)
    }

    pub async fn read_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ScopeReadOutcome, RedisScopeStoreError> {
        match self.load(scope_id).await? {
            LoadedScope::Missing => Ok(ScopeReadOutcome::Missing),
            LoadedScope::Cleaned(scope) => Ok(ScopeReadOutcome::Archived(scope.public_snapshot()?)),
            LoadedScope::Current(LoadedCurrent { scope, .. })
                if scope.state == StoredScopeState::Snapshotted =>
            {
                Ok(ScopeReadOutcome::Archived(scope.public_snapshot()?))
            }
            LoadedScope::Current(LoadedCurrent { scope, .. }) => {
                if let Some(bound) = age_bound(scope.created_at, now) {
                    return Ok(ScopeReadOutcome::Bound(bound));
                }
                if let Some(bound) = stored_bounds(&scope.values) {
                    return Ok(ScopeReadOutcome::Bound(bound));
                }
                Ok(ScopeReadOutcome::Present(
                    scope
                        .values
                        .into_iter()
                        .map(|(key, value)| StoredScopeValue {
                            key,
                            value_identity: value.value_identity,
                            envelope: value.envelope,
                            created_at: value.created_at,
                            updated_at: value.updated_at,
                        })
                        .collect(),
                ))
            }
        }
    }

    pub async fn snapshot_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ScopeSnapshotOutcome, RedisScopeStoreError> {
        for _ in 0..4 {
            let loaded = self.load(scope_id).await?;
            let LoadedScope::Current(LoadedCurrent {
                scope: mut current,
                encoded: old_encoded,
                metrics: old_metrics,
            }) = loaded
            else {
                return match loaded {
                    LoadedScope::Missing => Ok(ScopeSnapshotOutcome::Missing),
                    LoadedScope::Cleaned(scope) => {
                        self.guard_existing_commit()?;
                        Ok(ScopeSnapshotOutcome::Idempotent(scope.public_snapshot()?))
                    }
                    LoadedScope::Current(_) => unreachable!(),
                };
            };
            if current.state == StoredScopeState::Snapshotted {
                self.guard_existing_commit()?;
                return Ok(ScopeSnapshotOutcome::Idempotent(current.public_snapshot()?));
            }
            if let Some(bound) = age_bound(current.created_at, now) {
                return Ok(ScopeSnapshotOutcome::Bound(bound));
            }
            if let Some(bound) = stored_bounds(&current.values) {
                return Ok(ScopeSnapshotOutcome::Bound(bound));
            }
            let snapshot = snapshot_from_values(scope_id, &current.values)?;
            current.state = StoredScopeState::Snapshotted;
            current.updated_at = now;
            current.snapshot = Some(StoredSnapshot {
                bytes: snapshot.bytes.clone(),
                digest: snapshot.digest.clone(),
                row_count: snapshot.row_count,
                value_bytes: snapshot.value_bytes,
            });
            let stable_payload =
                mutation_payload(b"snapshot", scope_id, snapshot.digest.as_bytes());
            let mutation = ScriptMutation::new(
                &self.keys,
                MutationKind::Snapshot,
                format!("snapshot:{scope_id}"),
                stable_payload,
                scope_id,
                String::new(),
                Some((old_encoded, old_metrics)),
                current,
                self.config.as_ref(),
                self.capability.guard_admission()?,
            )?;
            let output = self.commit_mutation(&mutation).await?;
            let status = decode_status(&output)?;
            self.finish_operation(&output, mutation.generation).await?;
            match status {
                MutationStatus::Applied => return Ok(ScopeSnapshotOutcome::Committed(snapshot)),
                MutationStatus::Replayed => {
                    return Ok(ScopeSnapshotOutcome::Idempotent(snapshot));
                }
                MutationStatus::Stale => continue,
                MutationStatus::Missing => return Ok(ScopeSnapshotOutcome::Missing),
                _ => return Err(self.status_error(status)),
            }
        }
        Err(RedisScopeStoreError::Unavailable)
    }

    pub async fn snapshot_tickr_ctx_scope_for_run(
        &self,
        namespace: &str,
        run_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ScopeSnapshotOutcome, RedisScopeStoreError> {
        validate_scope_identity(namespace, run_id)?;
        let mut connection = self.connection.clone();
        let scope_id: Option<String> = redis::cmd("HGET")
            .arg(&self.keys.owners)
            .arg(owner_identity(namespace, run_id))
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let Some(scope_id) = scope_id else {
            return Ok(ScopeSnapshotOutcome::Missing);
        };
        let scope_id = Uuid::parse_str(&scope_id).map_err(|_| self.accounting_failure())?;
        self.snapshot_tickr_ctx_scope(scope_id, now).await
    }

    pub async fn record_verified_archive_commit(
        &self,
        scope_id: Uuid,
        snapshot_digest: &str,
        archive_identity: &[u8],
        now: DateTime<Utc>,
    ) -> Result<RedisScopeArchiveCommitOutcome, RedisScopeStoreError> {
        if snapshot_digest.len() != 64 || archive_identity.is_empty() {
            return Err(RedisScopeStoreError::InvalidOperation);
        }
        for _ in 0..4 {
            let LoadedScope::Current(LoadedCurrent {
                scope: mut current,
                encoded: old_encoded,
                metrics: old_metrics,
            }) = self.load(scope_id).await?
            else {
                return Err(RedisScopeStoreError::ArchiveNotCommitted);
            };
            if current.state != StoredScopeState::Snapshotted {
                return Err(RedisScopeStoreError::ArchiveNotCommitted);
            }
            let snapshot = current.public_snapshot()?;
            if snapshot.digest != snapshot_digest {
                return Err(RedisScopeStoreError::IdentityConflict);
            }
            let archive_identity_digest = digest_hex(archive_identity);
            let commit = StoredArchiveCommit {
                snapshot_digest: snapshot_digest.to_owned(),
                archive_identity_digest: archive_identity_digest.clone(),
            };
            if let Some(existing) = &current.archive_commit {
                return if existing == &commit {
                    self.guard_existing_commit()?;
                    Ok(RedisScopeArchiveCommitOutcome::AlreadyRecorded)
                } else {
                    Err(RedisScopeStoreError::IdentityConflict)
                };
            }
            current.archive_commit = Some(commit);
            current.updated_at = now;
            let stable_payload = archive_payload(scope_id, snapshot_digest, archive_identity);
            let mutation = ScriptMutation::new(
                &self.keys,
                MutationKind::Archive,
                format!("archive:{scope_id}"),
                stable_payload,
                scope_id,
                String::new(),
                Some((old_encoded, old_metrics)),
                current,
                self.config.as_ref(),
                self.capability.guard_admission()?,
            )?;
            let output = self.commit_mutation(&mutation).await?;
            let status = decode_status(&output)?;
            self.finish_operation(&output, mutation.generation).await?;
            match status {
                MutationStatus::Applied => {
                    return Ok(RedisScopeArchiveCommitOutcome::Recorded);
                }
                MutationStatus::Replayed => {
                    return Ok(RedisScopeArchiveCommitOutcome::AlreadyRecorded);
                }
                MutationStatus::Stale => continue,
                _ => return Err(self.status_error(status)),
            }
        }
        Err(RedisScopeStoreError::Unavailable)
    }

    pub async fn cleanup_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        snapshot_digest: &str,
        archive_identity: &[u8],
        now: DateTime<Utc>,
    ) -> Result<ScopeCleanupOutcome, RedisScopeStoreError> {
        if snapshot_digest.len() != 64 || archive_identity.is_empty() {
            return Err(RedisScopeStoreError::InvalidOperation);
        }
        for _ in 0..4 {
            let loaded = self.load(scope_id).await?;
            let LoadedScope::Current(LoadedCurrent {
                scope: mut current,
                encoded: old_encoded,
                metrics: old_metrics,
            }) = loaded
            else {
                return match loaded {
                    LoadedScope::Missing => Ok(ScopeCleanupOutcome::Missing),
                    LoadedScope::Cleaned(scope) => {
                        verify_archive_evidence(&scope, snapshot_digest, archive_identity)?;
                        self.guard_existing_commit()?;
                        Ok(ScopeCleanupOutcome::AlreadyCleaned)
                    }
                    LoadedScope::Current(_) => unreachable!(),
                };
            };
            if current.state == StoredScopeState::Active {
                return Ok(ScopeCleanupOutcome::SnapshotRequired);
            }
            verify_archive_evidence(&current, snapshot_digest, archive_identity)?;
            current.updated_at = now;
            let cleaned = current.cleaned_proof();
            let stable_payload = archive_payload(scope_id, snapshot_digest, archive_identity);
            let mutation = ScriptMutation::new(
                &self.keys,
                MutationKind::Cleanup,
                format!("cleanup:{scope_id}"),
                stable_payload,
                scope_id,
                String::new(),
                Some((old_encoded, old_metrics)),
                cleaned,
                self.config.as_ref(),
                self.capability.guard_admission()?,
            )?;
            let output = self.commit_mutation(&mutation).await?;
            let status = decode_status(&output)?;
            self.finish_operation(&output, mutation.generation).await?;
            match status {
                MutationStatus::Cleaned => return Ok(ScopeCleanupOutcome::Cleaned),
                MutationStatus::ReplayedCleanup => {
                    return Ok(ScopeCleanupOutcome::AlreadyCleaned);
                }
                MutationStatus::Stale => continue,
                _ => return Err(self.status_error(status)),
            }
        }
        Err(RedisScopeStoreError::Unavailable)
    }

    pub async fn quota_state(&self) -> Result<RedisScopeStoreQuotaState, RedisScopeStoreError> {
        let mut connection = self.connection.clone();
        let aggregate = read_metrics(&mut connection, &self.keys.quota)
            .await
            .map_err(|_| self.accounting_failure())?;
        let entries: Vec<(String, String)> = redis::cmd("HGETALL")
            .arg(&self.keys.scope_metrics)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let mut exact = StoredMetrics::default();
        for (scope_id, encoded) in entries {
            let metrics: StoredMetrics =
                serde_json::from_str(&encoded).map_err(|_| self.accounting_failure())?;
            let scope_id = Uuid::parse_str(&scope_id).map_err(|_| self.accounting_failure())?;
            let encoded_scope: Option<Vec<u8>> = redis::cmd("GET")
                .arg(self.keys.scope(scope_id))
                .query_async(&mut connection)
                .await
                .map_err(|error| self.redis_error(error))?;
            if encoded_scope.is_none() {
                return Err(self.accounting_failure());
            }
            exact = exact
                .checked_add(metrics)
                .ok_or_else(|| self.accounting_failure())?;
        }
        if exact != aggregate {
            return Err(self.accounting_failure());
        }
        let state = self.quota_from(aggregate);
        self.capability.report_quota(state);
        Ok(state)
    }

    fn guard_existing_commit(&self) -> Result<(), RedisScopeStoreError> {
        let generation = self.capability.guard_admission()?;
        self.capability.guard_acknowledgement(generation)
    }

    async fn claim_status(
        &self,
        operation_identity: &str,
        scope_id: Uuid,
        stable_payload: &[u8],
    ) -> Result<ClaimStatus, RedisScopeStoreError> {
        let mut connection = self.connection.clone();
        let actual: Option<String> = redis::cmd("HGET")
            .arg(&self.keys.claims)
            .arg(operation_identity)
            .query_async(&mut connection)
            .await
            .map_err(|error| self.redis_error(error))?;
        let expected = format!("{scope_id}|{}", digest_hex(stable_payload));
        match actual {
            None => Ok(ClaimStatus::Missing),
            Some(actual) if actual == expected => match self.load(scope_id).await? {
                LoadedScope::Missing => Err(self.accounting_failure()),
                LoadedScope::Current(_) | LoadedScope::Cleaned(_) => Ok(ClaimStatus::Matching),
            },
            Some(_) => Ok(ClaimStatus::Conflict),
        }
    }

    async fn load(&self, scope_id: Uuid) -> Result<LoadedScope, RedisScopeStoreError> {
        let mut connection = self.connection.clone();
        let (current, cleaned, metrics): (Option<Vec<u8>>, Option<Vec<u8>>, Option<String>) =
            redis::pipe()
                .cmd("GET")
                .arg(self.keys.scope(scope_id))
                .cmd("HGET")
                .arg(&self.keys.cleaned)
                .arg(scope_id.to_string())
                .cmd("HGET")
                .arg(&self.keys.scope_metrics)
                .arg(scope_id.to_string())
                .query_async(&mut connection)
                .await
                .map_err(|error| self.redis_error(error))?;
        match (current, cleaned, metrics) {
            (Some(_), Some(_), _) => Err(self.accounting_failure()),
            (Some(encoded), None, Some(encoded_metrics)) => {
                let scope = self.decode_scope(scope_id, &encoded, false)?;
                let metrics: StoredMetrics = serde_json::from_str(&encoded_metrics)
                    .map_err(|_| self.accounting_failure())?;
                let expected =
                    StoredMetrics::for_scope(&scope, encoded.len(), metrics.operation_records)?;
                if metrics != expected {
                    return Err(self.accounting_failure());
                }
                Ok(LoadedScope::Current(LoadedCurrent {
                    scope,
                    encoded,
                    metrics,
                }))
            }
            (None, Some(encoded), None) => {
                let scope = self.decode_scope(scope_id, &encoded, true)?;
                Ok(LoadedScope::Cleaned(scope))
            }
            (None, None, None) => Ok(LoadedScope::Missing),
            _ => Err(self.accounting_failure()),
        }
    }

    fn decode_scope(
        &self,
        scope_id: Uuid,
        encoded: &[u8],
        cleaned: bool,
    ) -> Result<StoredScope, RedisScopeStoreError> {
        let scope: StoredScope =
            serde_json::from_slice(encoded).map_err(|_| self.corrupt_scope())?;
        if scope.protocol_version != TICKR_CTX_SCOPE_PROTOCOL_VERSION
            || scope.scope_id != scope_id
            || cleaned != (scope.state == StoredScopeState::Cleaned)
            || validate_scope_identity(&scope.namespace, &scope.run_id).is_err()
            || scope.values.iter().any(|(key, value)| {
                validate_key(key).is_err()
                    || validate_envelope(&value.envelope).is_err()
                    || value.value_identity != value_identity(scope_id, key, &value.envelope)
            })
        {
            return Err(self.corrupt_scope());
        }
        match scope.state {
            StoredScopeState::Active
                if scope.snapshot.is_none() && scope.archive_commit.is_none() => {}
            StoredScopeState::Snapshotted if scope.snapshot.is_some() => {
                let snapshot = scope.public_snapshot().map_err(|_| self.corrupt_scope())?;
                let expected = snapshot_from_values(scope_id, &scope.values)
                    .map_err(|_| self.corrupt_scope())?;
                if snapshot != expected {
                    return Err(self.corrupt_scope());
                }
            }
            StoredScopeState::Cleaned
                if scope.values.is_empty()
                    && scope.snapshot.is_some()
                    && scope.archive_commit.is_some() =>
            {
                scope.public_snapshot().map_err(|_| self.corrupt_scope())?;
            }
            _ => return Err(self.corrupt_scope()),
        }
        Ok(scope)
    }

    async fn commit_mutation(
        &self,
        mutation: &ScriptMutation,
    ) -> Result<Vec<Vec<u8>>, RedisScopeStoreError> {
        let mut connection = self.connection.clone();
        self.durability
            .execute(&mut connection, mutation)
            .await
            .map(|committed| committed.into_output())
            .map_err(|error| self.durability_error(error))
    }

    async fn finish_operation(
        &self,
        output: &[Vec<u8>],
        generation: u64,
    ) -> Result<(), RedisScopeStoreError> {
        let quota = self.decode_quota(output)?;
        self.capability.report_quota(quota);
        self.capability.guard_acknowledgement(generation)
    }

    fn decode_quota(
        &self,
        output: &[Vec<u8>],
    ) -> Result<RedisScopeStoreQuotaState, RedisScopeStoreError> {
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
            used_bytes: parse(&output[2])?,
            namespace_records: parse(&output[3])?,
            scope_values: parse(&output[4])?,
            operation_records: parse(&output[5])?,
            snapshots: parse(&output[6])?,
            archive_commits: parse(&output[7])?,
        }))
    }

    fn quota_from(&self, metrics: StoredMetrics) -> RedisScopeStoreQuotaState {
        let pressure = if metrics.used_bytes >= self.config.hard_limit_bytes
            || metrics.namespace_records >= self.config.hard_limit_scopes
            || metrics.scope_values >= self.config.hard_limit_values
        {
            RedisQuotaPressure::HardLimit
        } else if metrics.used_bytes >= self.config.soft_limit_bytes {
            RedisQuotaPressure::SoftThreshold
        } else {
            RedisQuotaPressure::BelowSoftThreshold
        };
        RedisScopeStoreQuotaState {
            used_bytes: metrics.used_bytes,
            namespace_records: metrics.namespace_records,
            scope_values: metrics.scope_values,
            operation_records: metrics.operation_records,
            snapshots: metrics.snapshots,
            archive_commits: metrics.archive_commits,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            hard_limit_scopes: self.config.hard_limit_scopes,
            hard_limit_values: self.config.hard_limit_values,
            pressure,
        }
    }

    fn status_error(&self, status: MutationStatus) -> RedisScopeStoreError {
        match status {
            MutationStatus::Fenced => RedisScopeStoreError::CapacityFenced,
            MutationStatus::ClaimConflict | MutationStatus::Collision => {
                RedisScopeStoreError::IdentityConflict
            }
            MutationStatus::Missing | MutationStatus::Cleaned | MutationStatus::Stale => {
                RedisScopeStoreError::Unavailable
            }
            MutationStatus::Accounting => self.accounting_failure(),
            MutationStatus::Applied
            | MutationStatus::Replayed
            | MutationStatus::ReplayedCleanup => RedisScopeStoreError::Accounting,
        }
    }

    fn corrupt_scope(&self) -> RedisScopeStoreError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisScopeStoreError::CorruptScope
    }

    fn accounting_failure(&self) -> RedisScopeStoreError {
        self.capability
            .report_failure(RedisRoleCapabilityFailure::Accounting);
        RedisScopeStoreError::Accounting
    }

    fn redis_error(&self, error: redis::RedisError) -> RedisScopeStoreError {
        match RedisMutationError::from_redis(error).failure() {
            RedisMutationFailure::ReadOnlyPrimary => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::ReadOnly),
            RedisMutationFailure::OutOfMemory => self
                .capability
                .report_failure(RedisRoleCapabilityFailure::OutOfMemory),
            RedisMutationFailure::AmbiguousTransport | RedisMutationFailure::Rejected => {}
        }
        RedisScopeStoreError::Unavailable
    }

    fn durability_error(&self, error: RedisDurabilityError) -> RedisScopeStoreError {
        match error.failure() {
            RedisDurabilityFailure::IdentityConflict => {
                return RedisScopeStoreError::IdentityConflict;
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
        RedisScopeStoreError::Durability(error.failure())
    }
}

impl ScopeStore for RedisScopeStore {
    fn create_tickr_ctx_scope<'a>(
        &'a self,
        input: CreateTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeCreationOutcome> {
        Box::pin(async move {
            RedisScopeStore::create_tickr_ctx_scope(self, input)
                .await
                .map_err(|error| Box::new(error) as BoxedScopeStoreError)
        })
    }

    fn write_tickr_ctx_scope<'a>(
        &'a self,
        input: WriteTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeWriteOutcome> {
        Box::pin(async move {
            RedisScopeStore::write_tickr_ctx_scope(self, input)
                .await
                .map_err(|error| Box::new(error) as BoxedScopeStoreError)
        })
    }

    fn delete_tickr_ctx_scope_value<'a>(
        &'a self,
        input: DeleteTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeDeleteOutcome> {
        Box::pin(async move {
            RedisScopeStore::delete_tickr_ctx_scope_value(self, input)
                .await
                .map_err(|error| Box::new(error) as BoxedScopeStoreError)
        })
    }

    fn read_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'_, ScopeReadOutcome> {
        Box::pin(async move {
            RedisScopeStore::read_tickr_ctx_scope(self, scope_id, now)
                .await
                .map_err(|error| Box::new(error) as BoxedScopeStoreError)
        })
    }

    fn snapshot_tickr_ctx_scope_for_run<'a>(
        &'a self,
        namespace: &'a str,
        run_id: &'a str,
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'a, ScopeSnapshotOutcome> {
        Box::pin(async move {
            RedisScopeStore::snapshot_tickr_ctx_scope_for_run(self, namespace, run_id, now)
                .await
                .map_err(|error| Box::new(error) as BoxedScopeStoreError)
        })
    }

    fn record_verified_archive_commit<'a>(
        &'a self,
        scope_id: Uuid,
        snapshot_digest: &'a str,
        archive_identity: &'a [u8],
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'a, ()> {
        Box::pin(async move {
            RedisScopeStore::record_verified_archive_commit(
                self,
                scope_id,
                snapshot_digest,
                archive_identity,
                now,
            )
            .await
            .map(|_| ())
            .map_err(|error| Box::new(error) as BoxedScopeStoreError)
        })
    }

    fn cleanup_after_verified_archive_commit<'a>(
        &'a self,
        scope_id: Uuid,
        snapshot_digest: &'a str,
        archive_identity: &'a [u8],
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'a, ScopeCleanupOutcome> {
        Box::pin(async move {
            RedisScopeStore::cleanup_tickr_ctx_scope(
                self,
                scope_id,
                snapshot_digest,
                archive_identity,
                now,
            )
            .await
            .map_err(|error| Box::new(error) as BoxedScopeStoreError)
        })
    }
}

struct ReconstructionScopeCapability;

impl RedisScopeStoreCapability for ReconstructionScopeCapability {
    fn guard_admission(&self) -> Result<u64, RedisScopeStoreError> {
        Ok(0)
    }

    fn guard_acknowledgement(&self, _generation: u64) -> Result<(), RedisScopeStoreError> {
        Ok(())
    }

    fn report_failure(&self, _failure: RedisRoleCapabilityFailure) {}

    fn report_quota(&self, _state: RedisScopeStoreQuotaState) {}
}

/// Admitted ScopeStore role registered before readiness and released only as
/// the backend-neutral scope contract.
pub(crate) struct RedisScopeStoreRoleRegistration {
    connection: MultiplexedConnection,
    config: RedisScopeStoreConfig,
    durability: RedisDurabilityGuard,
    manifest_identity: RedisOperationManifestIdentity,
}

impl RedisScopeStoreRoleRegistration {
    pub(crate) fn new(
        connection: MultiplexedConnection,
        config: RedisScopeStoreConfig,
        durability: RedisDurabilityGuard,
        manifest_identity: RedisOperationManifestIdentity,
    ) -> Result<Self, RedisScopeStoreError> {
        config.validate()?;
        Ok(Self {
            connection,
            config,
            durability,
            manifest_identity,
        })
    }

    pub(crate) async fn build_store(
        &self,
        capability: Arc<dyn RedisScopeStoreCapability>,
    ) -> Result<RedisScopeStore, RedisScopeStoreError> {
        RedisScopeStore::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            capability,
        )
        .await
    }

    fn context_matches(&self, context: &RedisRoleProbeContext) -> bool {
        context.role() == CoordinationRole::ScopeStore
            && context.manifest_identity() == &self.manifest_identity
            && RedisScopeStore::operation_manifest()
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
impl RedisRoleCapabilityProbe for RedisScopeStoreRoleRegistration {
    async fn probe_required_operations(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisRoleCapabilityFailure> {
        if !self.context_matches(context) {
            return Err(RedisRoleCapabilityFailure::Accounting);
        }
        let owners = RedisScopeStoreKeys::new(&self.config.formation_namespace).owners;
        let mut connection = self.connection.clone();
        redis::cmd("EVAL")
            .arg("return redis.call('HGET', KEYS[1], ARGV[1])")
            .arg(1)
            .arg(owners)
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
            "tickr:{{{}}}:log-staging:runtime-capability-canary",
            self.config.formation_namespace
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
impl RedisReconstructionCallback for RedisScopeStoreRoleRegistration {
    async fn reconstruct(
        &self,
        context: &RedisRoleProbeContext,
    ) -> Result<(), RedisReconstructionFailure> {
        if !self.context_matches(context) {
            return Err(RedisReconstructionFailure::GenerationConflict);
        }
        RedisScopeStore::from_connection(
            self.connection.clone(),
            self.config.clone(),
            self.durability,
            Arc::new(ReconstructionScopeCapability),
        )
        .await
        .map(|_| ())
        .map_err(|_| RedisReconstructionFailure::PendingEvidenceUnavailable)
    }
}

pub fn redis_scope_store_operation_manifest(
) -> Result<RedisOperationManifest, RedisOperationManifestError> {
    let script = RedisScriptIdentity::new(SCOPE_STORE_SCRIPT_NAME, SCOPE_STORE_SCRIPT_SHA256)?;
    RedisOperationManifest::new(
        CoordinationRole::ScopeStore,
        REDIS_SCOPE_STORE_PROTOCOL,
        REDIS_SCOPE_STORE_COMMANDS.to_vec(),
        vec![script],
        vec![
            "tickr:{namespace}:scope-store:claims",
            "tickr:{namespace}:scope-store:cleaned",
            "tickr:{namespace}:scope-store:owners",
            "tickr:{namespace}:scope-store:quota",
            "tickr:{namespace}:scope-store:scope-metrics",
            "tickr:{namespace}:scope-store:scopes:*",
        ],
        vec![],
        vec![RedisRequiredOperationCanary::new(
            RedisOperation::script(script),
            RedisNamespacePattern::key("tickr:{namespace}:scope-store:scopes:*"),
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

#[derive(Clone, Copy)]
enum MutationKind {
    Create,
    Write,
    Delete,
    Snapshot,
    Archive,
    Cleanup,
}

impl MutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Write => "write",
            Self::Delete => "delete",
            Self::Snapshot => "snapshot",
            Self::Archive => "archive",
            Self::Cleanup => "cleanup",
        }
    }

    fn enforces_admission_limit(self) -> bool {
        matches!(self, Self::Create | Self::Write)
    }
}

struct ScriptMutation {
    operation: RedisStableOperation,
    operation_identity: String,
    operation_fingerprint: String,
    kind: MutationKind,
    keys: RedisScopeStoreKeys,
    scope_id: Uuid,
    owner_identity: String,
    expected_state: Vec<u8>,
    next_state: Vec<u8>,
    expected_metrics: Vec<u8>,
    next_metrics: Vec<u8>,
    hard_limit_bytes: u64,
    hard_limit_scopes: u64,
    hard_limit_values: u64,
    generation: u64,
}

impl ScriptMutation {
    #[allow(clippy::too_many_arguments)]
    fn new(
        keys: &RedisScopeStoreKeys,
        kind: MutationKind,
        operation_identity: String,
        stable_payload: Vec<u8>,
        scope_id: Uuid,
        owner_identity: String,
        current: Option<(Vec<u8>, StoredMetrics)>,
        next: StoredScope,
        config: &RedisScopeStoreConfig,
        generation: u64,
    ) -> Result<Self, RedisScopeStoreError> {
        let (expected_state, expected_metrics, operation_records) = match current {
            Some((state, metrics)) => (
                state,
                serde_json::to_vec(&metrics).map_err(|_| RedisScopeStoreError::InvalidOperation)?,
                metrics
                    .operation_records
                    .checked_add(1)
                    .ok_or(RedisScopeStoreError::Accounting)?,
            ),
            None => (Vec::new(), Vec::new(), 1),
        };
        let next_state =
            serde_json::to_vec(&next).map_err(|_| RedisScopeStoreError::InvalidOperation)?;
        let next_metrics = if matches!(kind, MutationKind::Cleanup) {
            Vec::new()
        } else {
            serde_json::to_vec(&StoredMetrics::for_scope(
                &next,
                next_state.len(),
                operation_records,
            )?)
            .map_err(|_| RedisScopeStoreError::InvalidOperation)?
        };
        let operation = RedisStableOperation::new(
            format!("{}#{operation_identity}", keys.claims),
            &stable_payload,
        )
        .map_err(|_| RedisScopeStoreError::InvalidOperation)?;
        Ok(Self {
            operation,
            operation_identity,
            operation_fingerprint: digest_hex(&stable_payload),
            kind,
            keys: keys.clone(),
            scope_id,
            owner_identity,
            expected_state,
            next_state,
            expected_metrics,
            next_metrics,
            hard_limit_bytes: config.hard_limit_bytes,
            hard_limit_scopes: config.hard_limit_scopes,
            hard_limit_values: config.hard_limit_values,
            generation,
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
            .arg(SCOPE_STORE_SCRIPT)
            .arg(7)
            .arg(self.keys.scope(self.scope_id))
            .arg(&self.keys.quota)
            .arg(&self.keys.scope_metrics)
            .arg(&self.keys.quota)
            .arg(&self.keys.owners)
            .arg(&self.keys.claims)
            .arg(&self.keys.cleaned)
            .arg(self.kind.as_str())
            .arg(&self.operation_identity)
            .arg(&self.operation_fingerprint)
            .arg(self.scope_id.to_string())
            .arg(&self.owner_identity)
            .arg(&self.expected_state)
            .arg(&self.next_state)
            .arg(&self.expected_metrics)
            .arg(&self.next_metrics)
            .arg(self.hard_limit_bytes)
            .arg(self.hard_limit_scopes)
            .arg(self.hard_limit_values)
            .arg(u8::from(self.kind.enforces_admission_limit()))
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        match decode_status(&output).map_err(|_| RedisMutationError::rejected())? {
            MutationStatus::Replayed | MutationStatus::ReplayedCleanup => {
                Ok(RedisStableMutationOutcome::Replayed(output))
            }
            _ => Ok(RedisStableMutationOutcome::Applied(output)),
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let actual: Option<String> = redis::cmd("HGET")
            .arg(&self.keys.claims)
            .arg(&self.operation_identity)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        let expected = format!("{}|{}", self.scope_id, self.operation_fingerprint);
        Ok(match actual {
            None => RedisStableMutationRecovery::Missing,
            Some(actual) if actual == expected => RedisStableMutationRecovery::Matching,
            Some(_) => RedisStableMutationRecovery::IdentityConflict,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationStatus {
    Applied,
    Replayed,
    ReplayedCleanup,
    Fenced,
    ClaimConflict,
    Collision,
    Missing,
    Cleaned,
    Stale,
    Accounting,
}

fn decode_status(output: &[Vec<u8>]) -> Result<MutationStatus, RedisScopeStoreError> {
    match output.first().map(Vec::as_slice) {
        Some(b"applied") => Ok(MutationStatus::Applied),
        Some(b"replayed") => Ok(MutationStatus::Replayed),
        Some(b"replayed_cleanup") => Ok(MutationStatus::ReplayedCleanup),
        Some(b"fenced") => Ok(MutationStatus::Fenced),
        Some(b"claim_conflict") => Ok(MutationStatus::ClaimConflict),
        Some(b"collision") => Ok(MutationStatus::Collision),
        Some(b"missing") => Ok(MutationStatus::Missing),
        Some(b"cleaned") => Ok(MutationStatus::Cleaned),
        Some(b"stale") => Ok(MutationStatus::Stale),
        Some(b"accounting") => Ok(MutationStatus::Accounting),
        _ => Err(RedisScopeStoreError::Accounting),
    }
}

#[derive(Clone, Copy)]
enum ClaimStatus {
    Missing,
    Matching,
    Conflict,
}

struct LoadedCurrent {
    scope: StoredScope,
    encoded: Vec<u8>,
    metrics: StoredMetrics,
}

enum LoadedScope {
    Current(LoadedCurrent),
    Cleaned(StoredScope),
    Missing,
}

async fn read_metrics(
    connection: &mut MultiplexedConnection,
    quota_key: &str,
) -> Result<StoredMetrics, RedisScopeStoreError> {
    let values: Vec<Option<u64>> = redis::cmd("HMGET")
        .arg(quota_key)
        .arg(&[
            "used_bytes",
            "namespace_records",
            "scope_values",
            "operation_records",
            "snapshots",
            "archive_commits",
        ])
        .query_async(connection)
        .await
        .map_err(|_| RedisScopeStoreError::Unavailable)?;
    if values.len() != 6 {
        return Err(RedisScopeStoreError::Accounting);
    }
    Ok(StoredMetrics {
        used_bytes: values[0].unwrap_or(0),
        namespace_records: values[1].unwrap_or(0),
        scope_values: values[2].unwrap_or(0),
        operation_records: values[3].unwrap_or(0),
        snapshots: values[4].unwrap_or(0),
        archive_commits: values[5].unwrap_or(0),
    })
}

fn validate_scope_identity(namespace: &str, run_id: &str) -> Result<(), RedisScopeStoreError> {
    if namespace.is_empty()
        || namespace.len() > MAX_SCOPE_NAMESPACE_BYTES
        || run_id.is_empty()
        || run_id.len() > MAX_RUN_ID_BYTES
    {
        return Err(RedisScopeStoreError::InvalidOperation);
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<(), RedisScopeStoreError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(RedisScopeStoreError::InvalidOperation);
    }
    Ok(())
}

fn validate_mutation(
    values: &[ScopeValueInput<'_>],
    allow_empty: bool,
) -> Option<ScopeMutationRejection> {
    if values.is_empty() {
        return (!allow_empty).then_some(ScopeMutationRejection::EmptyRequest);
    }
    if values.len() > MAX_SCOPE_REQUEST_VALUES {
        return Some(ScopeMutationRejection::Bound(
            ScopeBoundViolation::RequestValues {
                actual: values.len(),
                limit: MAX_SCOPE_REQUEST_VALUES,
            },
        ));
    }
    let request_bytes = values.iter().fold(0usize, |total, value| {
        total.saturating_add(value.envelope.len())
    });
    if request_bytes > MAX_SCOPE_REQUEST_BYTES {
        return Some(ScopeMutationRejection::Bound(
            ScopeBoundViolation::RequestBytes {
                actual: request_bytes,
                limit: MAX_SCOPE_REQUEST_BYTES,
            },
        ));
    }
    let mut keys = BTreeSet::new();
    for value in values {
        if validate_key(value.key).is_err() || !keys.insert(value.key) {
            return Some(ScopeMutationRejection::Envelope {
                key: value.key.to_owned(),
                reason: ScopeEnvelopeRejection::Malformed(
                    "invalid or duplicate scope key".to_owned(),
                ),
            });
        }
        if value.envelope.len() > MAX_SCOPE_VALUE_BYTES {
            return Some(ScopeMutationRejection::Bound(
                ScopeBoundViolation::ValueBytes {
                    key: value.key.to_owned(),
                    actual: value.envelope.len(),
                    limit: MAX_SCOPE_VALUE_BYTES,
                },
            ));
        }
        if let Err(reason) = validate_envelope(value.envelope) {
            return Some(ScopeMutationRejection::Envelope {
                key: value.key.to_owned(),
                reason,
            });
        }
    }
    None
}

fn validate_envelope(bytes: &[u8]) -> Result<(), ScopeEnvelopeRejection> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|source| ScopeEnvelopeRejection::Malformed(source.to_string()))?;
    let version = value
        .as_object()
        .and_then(|object| object.get("v"))
        .and_then(Value::as_u64)
        .ok_or(ScopeEnvelopeRejection::MissingVersion)?;
    match version {
        1 | 2 => Ok(()),
        other => Err(ScopeEnvelopeRejection::UnknownVersion(other)),
    }
}

fn stored_bounds(values: &BTreeMap<String, StoredValue>) -> Option<ScopeBoundViolation> {
    if values.len() > MAX_SCOPE_ROWS {
        return Some(ScopeBoundViolation::ScopeRows {
            actual: values.len(),
            limit: MAX_SCOPE_ROWS,
        });
    }
    for (key, value) in values {
        if value.envelope.len() > MAX_SCOPE_VALUE_BYTES {
            return Some(ScopeBoundViolation::ValueBytes {
                key: key.clone(),
                actual: value.envelope.len(),
                limit: MAX_SCOPE_VALUE_BYTES,
            });
        }
    }
    let bytes = values.values().fold(0usize, |total, value| {
        total.saturating_add(value.envelope.len())
    });
    (bytes > MAX_SCOPE_BYTES).then_some(ScopeBoundViolation::ScopeBytes {
        actual: bytes,
        limit: MAX_SCOPE_BYTES,
    })
}

fn age_bound(created_at: DateTime<Utc>, now: DateTime<Utc>) -> Option<ScopeBoundViolation> {
    let age = now.signed_duration_since(created_at).num_seconds().max(0);
    (age > MAX_SCOPE_AGE_SECONDS).then_some(ScopeBoundViolation::ScopeAgeSeconds {
        actual: age,
        limit: MAX_SCOPE_AGE_SECONDS,
    })
}

fn mutation_digest(
    scope_id: Uuid,
    values: &[ScopeValueInput<'_>],
) -> Result<String, RedisScopeStoreError> {
    let mut ordered = values.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|value| value.key);
    let mut seen = BTreeSet::new();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(scope_id.as_bytes());
    for value in ordered {
        validate_key(value.key)?;
        if !seen.insert(value.key) {
            return Err(RedisScopeStoreError::InvalidOperation);
        }
        append_len_prefixed(&mut bytes, value.key.as_bytes());
        append_len_prefixed(&mut bytes, value.envelope);
    }
    Ok(digest_hex(&bytes))
}

fn delete_digest(scope_id: Uuid, key: &str) -> String {
    let mut bytes = Vec::with_capacity(32 + key.len());
    bytes.extend_from_slice(scope_id.as_bytes());
    append_len_prefixed(&mut bytes, b"delete");
    append_len_prefixed(&mut bytes, key.as_bytes());
    digest_hex(&bytes)
}

fn value_identity(scope_id: Uuid, key: &str, envelope: &[u8]) -> String {
    let mut bytes = Vec::with_capacity(32 + key.len() + envelope.len());
    bytes.extend_from_slice(scope_id.as_bytes());
    append_len_prefixed(&mut bytes, key.as_bytes());
    append_len_prefixed(&mut bytes, envelope);
    digest_hex(&bytes)
}

fn snapshot_from_values(
    scope_id: Uuid,
    values: &BTreeMap<String, StoredValue>,
) -> Result<TickrCtxScopeSnapshot, RedisScopeStoreError> {
    if let Some(bound) = stored_bounds(values) {
        return Err(RedisScopeStoreError::Bound(bound));
    }
    let capacity = SNAPSHOT_MAGIC.len()
        + 4
        + values
            .iter()
            .map(|(key, value)| 8 + key.len() + value.envelope.len())
            .sum::<usize>();
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(values.len())
            .expect("scope row bound fits u32")
            .to_be_bytes(),
    );
    let mut value_bytes = 0usize;
    for (key, value) in values {
        append_len_prefixed(&mut bytes, key.as_bytes());
        append_len_prefixed(&mut bytes, &value.envelope);
        value_bytes = value_bytes
            .checked_add(value.envelope.len())
            .ok_or(RedisScopeStoreError::Accounting)?;
    }
    Ok(TickrCtxScopeSnapshot {
        scope_id,
        digest: digest_hex(&bytes),
        bytes,
        row_count: values.len(),
        value_bytes,
    })
}

fn validate_snapshot(snapshot: &StoredSnapshot) -> Result<(), RedisScopeStoreError> {
    if !snapshot.bytes.starts_with(SNAPSHOT_MAGIC)
        || digest_hex(&snapshot.bytes) != snapshot.digest
        || snapshot.row_count > MAX_SCOPE_ROWS
        || snapshot.value_bytes > MAX_SCOPE_BYTES
    {
        return Err(RedisScopeStoreError::CorruptScope);
    }
    let mut offset = SNAPSHOT_MAGIC.len();
    let row_count = read_snapshot_u32(&snapshot.bytes, &mut offset)
        .ok_or(RedisScopeStoreError::CorruptScope)?;
    if row_count != snapshot.row_count {
        return Err(RedisScopeStoreError::CorruptScope);
    }
    let mut previous_key = None;
    let mut value_bytes = 0usize;
    for _ in 0..row_count {
        let key_bytes = read_snapshot_part(&snapshot.bytes, &mut offset)
            .ok_or(RedisScopeStoreError::CorruptScope)?;
        let key = std::str::from_utf8(key_bytes).map_err(|_| RedisScopeStoreError::CorruptScope)?;
        if validate_key(key).is_err() || previous_key.is_some_and(|previous| previous >= key) {
            return Err(RedisScopeStoreError::CorruptScope);
        }
        previous_key = Some(key);
        let envelope = read_snapshot_part(&snapshot.bytes, &mut offset)
            .ok_or(RedisScopeStoreError::CorruptScope)?;
        validate_envelope(envelope).map_err(|_| RedisScopeStoreError::CorruptScope)?;
        value_bytes = value_bytes
            .checked_add(envelope.len())
            .ok_or(RedisScopeStoreError::CorruptScope)?;
    }
    if offset != snapshot.bytes.len() || value_bytes != snapshot.value_bytes {
        return Err(RedisScopeStoreError::CorruptScope);
    }
    Ok(())
}

fn read_snapshot_u32(bytes: &[u8], offset: &mut usize) -> Option<usize> {
    let end = offset.checked_add(4)?;
    let raw = bytes.get(*offset..end)?;
    *offset = end;
    Some(u32::from_be_bytes(raw.try_into().ok()?) as usize)
}

fn read_snapshot_part<'a>(bytes: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
    let length = read_snapshot_u32(bytes, offset)?;
    let end = offset.checked_add(length)?;
    let value = bytes.get(*offset..end)?;
    *offset = end;
    Some(value)
}

fn verify_archive_evidence(
    scope: &StoredScope,
    snapshot_digest: &str,
    archive_identity: &[u8],
) -> Result<(), RedisScopeStoreError> {
    let snapshot = scope.public_snapshot()?;
    let expected = StoredArchiveCommit {
        snapshot_digest: snapshot_digest.to_owned(),
        archive_identity_digest: digest_hex(archive_identity),
    };
    if snapshot.digest != snapshot_digest || scope.archive_commit.as_ref() != Some(&expected) {
        return Err(RedisScopeStoreError::ArchiveNotCommitted);
    }
    Ok(())
}

fn public_state(state: StoredScopeState) -> TickrCtxScopeState {
    match state {
        StoredScopeState::Active => TickrCtxScopeState::Active,
        StoredScopeState::Snapshotted => TickrCtxScopeState::Snapshotted,
        StoredScopeState::Cleaned => TickrCtxScopeState::Cleaned,
    }
}

fn claim_identity(claim_id: Uuid) -> String {
    format!("claim:{claim_id}")
}

fn owner_identity(namespace: &str, run_id: &str) -> String {
    let mut bytes = Vec::with_capacity(namespace.len() + run_id.len() + 8);
    append_len_prefixed(&mut bytes, namespace.as_bytes());
    append_len_prefixed(&mut bytes, run_id.as_bytes());
    digest_hex(&bytes)
}

fn create_payload(scope_id: Uuid, namespace: &str, run_id: &str, digest: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(namespace.len() + run_id.len() + digest.len() + 32);
    bytes.extend_from_slice(scope_id.as_bytes());
    append_len_prefixed(&mut bytes, namespace.as_bytes());
    append_len_prefixed(&mut bytes, run_id.as_bytes());
    append_len_prefixed(&mut bytes, digest.as_bytes());
    bytes
}

fn mutation_payload(kind: &[u8], scope_id: Uuid, digest: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(kind.len() + digest.len() + 32);
    bytes.extend_from_slice(scope_id.as_bytes());
    append_len_prefixed(&mut bytes, kind);
    append_len_prefixed(&mut bytes, digest);
    bytes
}

fn archive_payload(scope_id: Uuid, snapshot_digest: &str, archive_identity: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(snapshot_digest.len() + archive_identity.len() + 32);
    bytes.extend_from_slice(scope_id.as_bytes());
    append_len_prefixed(&mut bytes, snapshot_digest.as_bytes());
    append_len_prefixed(&mut bytes, archive_identity);
    bytes
}

fn append_len_prefixed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(
        &u32::try_from(value.len())
            .expect("scope byte bound fits u32")
            .to_be_bytes(),
    );
    target.extend_from_slice(value);
}

fn digest_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisScopeStoreError {
    InvalidConfiguration,
    InvalidOperation,
    IdentityConflict,
    CapacityFenced,
    ArchiveNotCommitted,
    CorruptScope,
    Bound(ScopeBoundViolation),
    Accounting,
    Unavailable,
    Durability(RedisDurabilityFailure),
}

impl fmt::Display for RedisScopeStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidConfiguration => "invalid Redis ScopeStore configuration",
            Self::InvalidOperation => "invalid Redis ScopeStore operation",
            Self::IdentityConflict => "Redis ScopeStore identity conflict",
            Self::CapacityFenced => "Redis ScopeStore capacity fenced",
            Self::ArchiveNotCommitted => "Redis ScopeStore archive commit is not verified",
            Self::CorruptScope => "Redis ScopeStore state is corrupt",
            Self::Bound(_) => "Redis ScopeStore scope bound was exceeded",
            Self::Accounting => "Redis ScopeStore accounting is inconsistent",
            Self::Unavailable => "Redis ScopeStore is unavailable",
            Self::Durability(_) => "Redis ScopeStore durability boundary failed",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RedisScopeStoreError {}

#[cfg(test)]
mod tests {
    use super::*;

    const ENVELOPE: &[u8] = br#"{ "v": 2, "value": "opaque", "lineage": "kept exactly" }"#;

    #[test]
    fn operation_manifest_registers_every_runtime_operation() {
        let manifest = redis_scope_store_operation_manifest().expect("valid manifest");
        assert_eq!(manifest.role(), CoordinationRole::ScopeStore);
        assert_eq!(manifest.protocol(), REDIS_SCOPE_STORE_PROTOCOL);
        for command in REDIS_SCOPE_STORE_COMMANDS {
            assert!(manifest.commands().contains(command));
        }
        assert_eq!(manifest.commands(), REDIS_SCOPE_STORE_COMMANDS);
        assert!(!manifest.commands().contains(&"FLUSHALL"));
        assert_eq!(manifest.scripts().len(), 1);
        assert_eq!(manifest.scripts()[0].name(), SCOPE_STORE_SCRIPT_NAME);
        assert_eq!(manifest.scripts()[0].sha256(), SCOPE_STORE_SCRIPT_SHA256);
        assert_eq!(
            digest_hex(SCOPE_STORE_SCRIPT.as_bytes()),
            SCOPE_STORE_SCRIPT_SHA256
        );
        assert_eq!(manifest.required_canaries().len(), 1);
        assert_eq!(manifest.forbidden_operations().len(), 2);
    }

    #[test]
    fn snapshot_is_ordered_and_preserves_opaque_envelope_bytes() {
        let scope_id = Uuid::new_v4();
        let now = Utc::now();
        let mut values = BTreeMap::new();
        values.insert(
            "b".to_owned(),
            StoredValue {
                value_identity: value_identity(scope_id, "b", ENVELOPE),
                envelope: ENVELOPE.to_vec(),
                created_at: now,
                updated_at: now,
            },
        );
        values.insert(
            "a".to_owned(),
            StoredValue {
                value_identity: value_identity(scope_id, "a", ENVELOPE),
                envelope: ENVELOPE.to_vec(),
                created_at: now,
                updated_at: now,
            },
        );
        let first = snapshot_from_values(scope_id, &values).expect("snapshot");
        let second = snapshot_from_values(scope_id, &values).expect("snapshot retry");
        assert_eq!(first, second);
        assert_eq!(first.digest, digest_hex(&first.bytes));
        assert_eq!(first.row_count, 2);
        assert!(
            first
                .bytes
                .windows(ENVELOPE.len())
                .filter(|window| *window == ENVELOPE)
                .count()
                == 2
        );
    }
}
