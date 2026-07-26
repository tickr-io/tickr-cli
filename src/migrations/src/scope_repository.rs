use std::{
    collections::{BTreeSet, HashMap},
    error::Error,
    future::Future,
    pin::Pin,
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, RepositoryError, RepositoryErrorKind,
    WriterRepositoryBundle,
};
use crate::encoding::{decode_timestamp, encode_timestamp, encode_uuid};

pub const TICKR_CTX_SCOPE_PROTOCOL_VERSION: i64 = 1;
pub const MAX_SCOPE_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_SCOPE_REQUEST_VALUES: usize = 128;
pub const MAX_SCOPE_REQUEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SCOPE_ROWS: usize = 4096;
pub const MAX_SCOPE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SCOPE_AGE_SECONDS: i64 = 30 * 24 * 60 * 60;

const MAX_NAMESPACE_BYTES: usize = 128;
const MAX_RUN_ID_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 512;
const SNAPSHOT_MAGIC: &[u8] = b"TICKR_CTX_SCOPE\0\x01";

#[derive(Debug, Clone, Copy)]
pub struct ScopeValueInput<'a> {
    pub key: &'a str,
    pub envelope: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub struct CreateTickrCtxScopeInput<'a> {
    pub scope_id: Uuid,
    pub namespace: &'a str,
    pub run_id: &'a str,
    pub claim_id: Uuid,
    pub values: &'a [ScopeValueInput<'a>],
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct WriteTickrCtxScopeInput<'a> {
    pub scope_id: Uuid,
    pub claim_id: Uuid,
    pub values: &'a [ScopeValueInput<'a>],
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
pub struct DeleteTickrCtxScopeInput<'a> {
    pub scope_id: Uuid,
    pub claim_id: Uuid,
    pub key: &'a str,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeBoundViolation {
    ValueBytes {
        key: String,
        actual: usize,
        limit: usize,
    },
    RequestValues {
        actual: usize,
        limit: usize,
    },
    RequestBytes {
        actual: usize,
        limit: usize,
    },
    ScopeRows {
        actual: usize,
        limit: usize,
    },
    ScopeBytes {
        actual: usize,
        limit: usize,
    },
    ScopeAgeSeconds {
        actual: i64,
        limit: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeEnvelopeRejection {
    Malformed(String),
    MissingVersion,
    UnknownVersion(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeMutationRejection {
    EmptyRequest,
    Bound(ScopeBoundViolation),
    Envelope {
        key: String,
        reason: ScopeEnvelopeRejection,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickrCtxScopeState {
    Active,
    Snapshotted,
    Cleaned,
    Quarantined,
}

impl TickrCtxScopeState {
    fn parse(value: &str) -> Result<Self, ScopeRepositoryError> {
        match value {
            "active" => Ok(Self::Active),
            "snapshotted" => Ok(Self::Snapshotted),
            "cleaned" => Ok(Self::Cleaned),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(ScopeRepositoryError::CorruptStoredValue(format!(
                "unknown scope state `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeCreationOutcome {
    Created,
    Idempotent,
    Collision { existing_scope_id: Uuid },
    ClaimConflict,
    Rejected(ScopeMutationRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeWriteOutcome {
    Applied { inserted: usize, updated: usize },
    Idempotent,
    Missing,
    ClaimConflict,
    NotWritable(TickrCtxScopeState),
    Rejected(ScopeMutationRejection),
    Quarantined { scope_id: Uuid, diagnostic: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeDeleteOutcome {
    Deleted,
    MissingKey,
    Idempotent,
    Missing,
    ClaimConflict,
    NotWritable(TickrCtxScopeState),
    Bound(ScopeBoundViolation),
    Quarantined { scope_id: Uuid, diagnostic: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredScopeValue {
    pub key: String,
    pub value_identity: String,
    pub envelope: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickrCtxScopeSnapshot {
    pub scope_id: Uuid,
    pub bytes: Vec<u8>,
    pub digest: String,
    pub row_count: usize,
    pub value_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeReadOutcome {
    Present(Vec<StoredScopeValue>),
    Archived(TickrCtxScopeSnapshot),
    Missing,
    Bound(ScopeBoundViolation),
    Quarantined { scope_id: Uuid, diagnostic: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeSnapshotOutcome {
    Committed(TickrCtxScopeSnapshot),
    Idempotent(TickrCtxScopeSnapshot),
    Missing,
    Bound(ScopeBoundViolation),
    Quarantined { scope_id: Uuid, diagnostic: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeCleanupOutcome {
    Cleaned,
    AlreadyCleaned,
    Missing,
    SnapshotRequired,
    Quarantined { scope_id: Uuid, diagnostic: String },
}

pub type ScopeStoreError = Box<dyn Error + Send + Sync>;
pub type ScopeStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ScopeStoreError>> + Send + 'a>>;

/// Backend-neutral access to the selected tickr-ctx ScopeStore role.
///
/// Callers own scope semantics and opaque envelope bytes; adapters alone own
/// their repository, NATS, or Redis substrate.
pub trait ScopeStore: Send + Sync {
    fn create_tickr_ctx_scope<'a>(
        &'a self,
        input: CreateTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeCreationOutcome>;

    fn write_tickr_ctx_scope<'a>(
        &'a self,
        input: WriteTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeWriteOutcome>;

    fn delete_tickr_ctx_scope_value<'a>(
        &'a self,
        input: DeleteTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeDeleteOutcome>;

    fn read_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'_, ScopeReadOutcome>;

    fn snapshot_tickr_ctx_scope_for_run<'a>(
        &'a self,
        namespace: &'a str,
        run_id: &'a str,
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'a, ScopeSnapshotOutcome>;

    fn record_verified_archive_commit<'a>(
        &'a self,
        _scope_id: Uuid,
        _snapshot_digest: &'a str,
        _archive_identity: &'a [u8],
        _now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'a, ()> {
        Box::pin(async {
            Err(Box::new(std::io::Error::other(
                "selected ScopeStore adapter does not expose archive evidence",
            )) as ScopeStoreError)
        })
    }

    fn cleanup_after_verified_archive_commit<'a>(
        &'a self,
        _scope_id: Uuid,
        _snapshot_digest: &'a str,
        _archive_identity: &'a [u8],
        _now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'a, ScopeCleanupOutcome> {
        Box::pin(async {
            Err(Box::new(std::io::Error::other(
                "selected ScopeStore adapter does not expose verified archive cleanup",
            )) as ScopeStoreError)
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum ScopeRepositoryError {
    #[error("tickr-ctx scope operations require the SQLite writer")]
    RequiresSqlite,
    #[error("namespace must contain 1 to {MAX_NAMESPACE_BYTES} bytes")]
    InvalidNamespace,
    #[error("run id must contain 1 to {MAX_RUN_ID_BYTES} bytes")]
    InvalidRunId,
    #[error("scope key must contain 1 to {MAX_KEY_BYTES} bytes")]
    InvalidKey,
    #[error("scope request contains duplicate key `{0}`")]
    DuplicateKey(String),
    #[error("stored tickr-ctx scope value is corrupt: {0}")]
    CorruptStoredValue(String),
}

#[derive(Debug)]
struct ScopeRecord {
    state: TickrCtxScopeState,
    created_at: DateTime<Utc>,
    snapshot: Option<Vec<u8>>,
    snapshot_digest: Option<String>,
    snapshot_row_count: Option<i64>,
    snapshot_value_bytes: Option<i64>,
    quarantine_reason: Option<String>,
}

impl WriterRepositoryBundle {
    pub async fn create_tickr_ctx_scope(
        &self,
        input: CreateTickrCtxScopeInput<'_>,
    ) -> Result<ScopeCreationOutcome, RepositoryError> {
        validate_scope_identity(input.namespace, input.run_id)?;
        let request_digest = mutation_digest(input.scope_id, input.values)?;
        if let Some(rejection) = validate_mutation(input.values, 0, 0) {
            // A run needs an archivable scope identity even when no task publishes values.
            if rejection != ScopeMutationRejection::EmptyRequest {
                return Ok(ScopeCreationOutcome::Rejected(rejection));
            }
        }

        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(ScopeRepositoryError::RequiresSqlite));
        };
        let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
        let claim_id = encode_uuid(input.claim_id);
        let scope_id = encode_uuid(input.scope_id);

        if let Some(row) = sqlx::query(
            "SELECT scope_id, namespace, run_id, creation_request_digest \
             FROM tickr_ctx_scopes WHERE creation_claim_id = ?1",
        )
        .bind(&claim_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
        {
            let existing_scope_id: String =
                row.try_get("scope_id").map_err(repository_sqlx_error)?;
            let existing_namespace: String =
                row.try_get("namespace").map_err(repository_sqlx_error)?;
            let existing_run_id: String = row.try_get("run_id").map_err(repository_sqlx_error)?;
            let existing_digest: String = row
                .try_get("creation_request_digest")
                .map_err(repository_sqlx_error)?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(
                if existing_scope_id == scope_id
                    && existing_namespace == input.namespace
                    && existing_run_id == input.run_id
                    && existing_digest == request_digest
                {
                    ScopeCreationOutcome::Idempotent
                } else {
                    ScopeCreationOutcome::ClaimConflict
                },
            );
        }

        if let Some(row) = sqlx::query(
            "SELECT scope_id FROM tickr_ctx_scopes WHERE namespace = ?1 AND run_id = ?2",
        )
        .bind(input.namespace)
        .bind(input.run_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
        {
            let existing: String = row.try_get("scope_id").map_err(repository_sqlx_error)?;
            let existing_scope_id = Uuid::parse_str(&existing)
                .map_err(|_| corrupt_value(format!("scope id `{existing}` is not a UUID")))?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeCreationOutcome::Collision { existing_scope_id });
        }

        sqlx::query(
            "INSERT INTO tickr_ctx_scopes \
             (scope_id, namespace, run_id, protocol_version, creation_claim_id, \
              creation_request_digest, state, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?7)",
        )
        .bind(&scope_id)
        .bind(input.namespace)
        .bind(input.run_id)
        .bind(TICKR_CTX_SCOPE_PROTOCOL_VERSION)
        .bind(&claim_id)
        .bind(&request_digest)
        .bind(encode_timestamp(input.now))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;

        insert_scope_values(&mut tx, &scope_id, input.values, input.now).await?;
        insert_claim(&mut tx, &claim_id, &scope_id, &request_digest, input.now).await?;
        tx.commit().await.map_err(repository_sqlx_error)?;
        Ok(ScopeCreationOutcome::Created)
    }

    pub async fn write_tickr_ctx_scope(
        &self,
        input: WriteTickrCtxScopeInput<'_>,
    ) -> Result<ScopeWriteOutcome, RepositoryError> {
        let request_digest = mutation_digest(input.scope_id, input.values)?;
        if let Some(rejection) = validate_mutation(input.values, 0, 0) {
            return Ok(ScopeWriteOutcome::Rejected(rejection));
        }
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(ScopeRepositoryError::RequiresSqlite));
        };
        let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
        let scope_id = encode_uuid(input.scope_id);
        let Some(scope) = load_scope(&mut tx, &scope_id).await? else {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeWriteOutcome::Missing);
        };
        if scope.state == TickrCtxScopeState::Quarantined {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeWriteOutcome::Quarantined {
                scope_id: input.scope_id,
                diagnostic: scope
                    .quarantine_reason
                    .unwrap_or_else(|| "scope is quarantined".to_owned()),
            });
        }
        let claim_id = encode_uuid(input.claim_id);
        if let Some(row) = sqlx::query(
            "SELECT scope_id, request_digest FROM tickr_ctx_scope_claims WHERE claim_id = ?1",
        )
        .bind(&claim_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
        {
            let claimed_scope: String = row.try_get("scope_id").map_err(repository_sqlx_error)?;
            let claimed_digest: String = row
                .try_get("request_digest")
                .map_err(repository_sqlx_error)?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(
                if claimed_scope == scope_id && claimed_digest == request_digest {
                    ScopeWriteOutcome::Idempotent
                } else {
                    ScopeWriteOutcome::ClaimConflict
                },
            );
        }
        if scope.state != TickrCtxScopeState::Active {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeWriteOutcome::NotWritable(scope.state));
        }
        if let Some(bound) = age_bound(scope.created_at, input.now) {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeWriteOutcome::Rejected(ScopeMutationRejection::Bound(
                bound,
            )));
        }

        let rows = sqlx::query(
            "SELECT key, length(envelope) AS envelope_bytes \
             FROM tickr_ctx_scope_values WHERE scope_id = ?1",
        )
        .bind(&scope_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
        let existing = rows
            .into_iter()
            .map(|row| {
                let key: String = row.try_get("key").map_err(repository_sqlx_error)?;
                let bytes: i64 = row
                    .try_get("envelope_bytes")
                    .map_err(repository_sqlx_error)?;
                let bytes = usize::try_from(bytes)
                    .map_err(|_| corrupt_value("stored envelope length is negative".to_owned()))?;
                Ok((key, bytes))
            })
            .collect::<Result<HashMap<_, _>, RepositoryError>>()?;
        if let Some(bound) = resulting_scope_bound(&existing, input.values) {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeWriteOutcome::Rejected(ScopeMutationRejection::Bound(
                bound,
            )));
        }
        let updated = input
            .values
            .iter()
            .filter(|value| existing.contains_key(value.key))
            .count();
        let inserted = input.values.len() - updated;

        insert_scope_values(&mut tx, &scope_id, input.values, input.now).await?;
        insert_claim(&mut tx, &claim_id, &scope_id, &request_digest, input.now).await?;
        sqlx::query("UPDATE tickr_ctx_scopes SET updated_at = ?2 WHERE scope_id = ?1")
            .bind(&scope_id)
            .bind(encode_timestamp(input.now))
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
        tx.commit().await.map_err(repository_sqlx_error)?;
        Ok(ScopeWriteOutcome::Applied { inserted, updated })
    }

    pub async fn delete_tickr_ctx_scope_value(
        &self,
        input: DeleteTickrCtxScopeInput<'_>,
    ) -> Result<ScopeDeleteOutcome, RepositoryError> {
        let request_digest = delete_digest(input.scope_id, input.key)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(ScopeRepositoryError::RequiresSqlite));
        };
        let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
        let scope_id = encode_uuid(input.scope_id);
        let Some(scope) = load_scope(&mut tx, &scope_id).await? else {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeDeleteOutcome::Missing);
        };
        if scope.state == TickrCtxScopeState::Quarantined {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeDeleteOutcome::Quarantined {
                scope_id: input.scope_id,
                diagnostic: scope
                    .quarantine_reason
                    .unwrap_or_else(|| "scope is quarantined".to_owned()),
            });
        }

        let claim_id = encode_uuid(input.claim_id);
        if let Some(row) = sqlx::query(
            "SELECT scope_id, request_digest FROM tickr_ctx_scope_claims WHERE claim_id = ?1",
        )
        .bind(&claim_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?
        {
            let claimed_scope: String = row.try_get("scope_id").map_err(repository_sqlx_error)?;
            let claimed_digest: String = row
                .try_get("request_digest")
                .map_err(repository_sqlx_error)?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(
                if claimed_scope == scope_id && claimed_digest == request_digest {
                    ScopeDeleteOutcome::Idempotent
                } else {
                    ScopeDeleteOutcome::ClaimConflict
                },
            );
        }
        if scope.state != TickrCtxScopeState::Active {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeDeleteOutcome::NotWritable(scope.state));
        }
        if let Some(bound) = age_bound(scope.created_at, input.now) {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeDeleteOutcome::Bound(bound));
        }

        let deleted =
            sqlx::query("DELETE FROM tickr_ctx_scope_values WHERE scope_id = ?1 AND key = ?2")
                .bind(&scope_id)
                .bind(input.key)
                .execute(&mut *tx)
                .await
                .map_err(repository_sqlx_error)?
                .rows_affected();
        insert_claim(&mut tx, &claim_id, &scope_id, &request_digest, input.now).await?;
        sqlx::query("UPDATE tickr_ctx_scopes SET updated_at = ?2 WHERE scope_id = ?1")
            .bind(&scope_id)
            .bind(encode_timestamp(input.now))
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
        tx.commit().await.map_err(repository_sqlx_error)?;
        Ok(if deleted == 0 {
            ScopeDeleteOutcome::MissingKey
        } else {
            ScopeDeleteOutcome::Deleted
        })
    }

    pub async fn read_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ScopeReadOutcome, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(ScopeRepositoryError::RequiresSqlite));
        };
        let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
        let encoded_scope_id = encode_uuid(scope_id);
        let Some(scope) = load_scope(&mut tx, &encoded_scope_id).await? else {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeReadOutcome::Missing);
        };
        if scope.state == TickrCtxScopeState::Quarantined {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeReadOutcome::Quarantined {
                scope_id,
                diagnostic: scope
                    .quarantine_reason
                    .unwrap_or_else(|| "scope is quarantined".to_owned()),
            });
        }
        if matches!(
            scope.state,
            TickrCtxScopeState::Snapshotted | TickrCtxScopeState::Cleaned
        ) {
            let snapshot = match snapshot_from_record(scope_id, &scope) {
                Ok(snapshot) => snapshot,
                Err(diagnostic) => {
                    quarantine_scope(&mut tx, &encoded_scope_id, &diagnostic, now).await?;
                    tx.commit().await.map_err(repository_sqlx_error)?;
                    return Ok(ScopeReadOutcome::Quarantined {
                        scope_id,
                        diagnostic,
                    });
                }
            };
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeReadOutcome::Archived(snapshot));
        }
        if let Some(bound) = age_bound(scope.created_at, now) {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeReadOutcome::Bound(bound));
        }

        let values = load_scope_values(&mut tx, &encoded_scope_id).await?;
        if let Some(bound) = stored_bounds(&values) {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeReadOutcome::Bound(bound));
        }
        if let Some(diagnostic) = first_invalid_stored_envelope(&values) {
            quarantine_scope(&mut tx, &encoded_scope_id, &diagnostic, now).await?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeReadOutcome::Quarantined {
                scope_id,
                diagnostic,
            });
        }
        tx.commit().await.map_err(repository_sqlx_error)?;
        Ok(ScopeReadOutcome::Present(values))
    }

    pub async fn snapshot_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ScopeSnapshotOutcome, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(ScopeRepositoryError::RequiresSqlite));
        };
        let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
        let encoded_scope_id = encode_uuid(scope_id);
        let Some(scope) = load_scope(&mut tx, &encoded_scope_id).await? else {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeSnapshotOutcome::Missing);
        };
        if scope.state == TickrCtxScopeState::Quarantined {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeSnapshotOutcome::Quarantined {
                scope_id,
                diagnostic: scope
                    .quarantine_reason
                    .unwrap_or_else(|| "scope is quarantined".to_owned()),
            });
        }
        if matches!(
            scope.state,
            TickrCtxScopeState::Snapshotted | TickrCtxScopeState::Cleaned
        ) {
            let snapshot = match snapshot_from_record(scope_id, &scope) {
                Ok(snapshot) => snapshot,
                Err(diagnostic) => {
                    quarantine_scope(&mut tx, &encoded_scope_id, &diagnostic, now).await?;
                    tx.commit().await.map_err(repository_sqlx_error)?;
                    return Ok(ScopeSnapshotOutcome::Quarantined {
                        scope_id,
                        diagnostic,
                    });
                }
            };
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeSnapshotOutcome::Idempotent(snapshot));
        }
        if let Some(bound) = age_bound(scope.created_at, now) {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeSnapshotOutcome::Bound(bound));
        }
        let values = load_scope_values(&mut tx, &encoded_scope_id).await?;
        if let Some(bound) = stored_bounds(&values) {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeSnapshotOutcome::Bound(bound));
        }
        if let Some(diagnostic) = first_invalid_stored_envelope(&values) {
            quarantine_scope(&mut tx, &encoded_scope_id, &diagnostic, now).await?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeSnapshotOutcome::Quarantined {
                scope_id,
                diagnostic,
            });
        }

        let bytes = encode_snapshot(&values);
        let digest = sha256_hex(&bytes);
        let row_count = values.len();
        let value_bytes = values.iter().map(|value| value.envelope.len()).sum();
        sqlx::query(
            "UPDATE tickr_ctx_scopes SET state = 'snapshotted', snapshot = ?2, \
             snapshot_digest = ?3, snapshot_row_count = ?4, snapshot_value_bytes = ?5, \
             snapshotted_at = ?6, updated_at = ?6 WHERE scope_id = ?1 AND state = 'active'",
        )
        .bind(&encoded_scope_id)
        .bind(&bytes)
        .bind(&digest)
        .bind(i64::try_from(row_count).expect("scope row limit fits i64"))
        .bind(i64::try_from(value_bytes).expect("scope byte limit fits i64"))
        .bind(encode_timestamp(now))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
        tx.commit().await.map_err(repository_sqlx_error)?;
        Ok(ScopeSnapshotOutcome::Committed(TickrCtxScopeSnapshot {
            scope_id,
            bytes,
            digest,
            row_count,
            value_bytes,
        }))
    }

    /// Resolve the stable scope identity for one namespace/run pair, then
    /// commit or re-read its immutable snapshot. Compaction uses this instead
    /// of guessing an identity from a published archive envelope.
    pub async fn snapshot_tickr_ctx_scope_for_run(
        &self,
        namespace: &str,
        run_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ScopeSnapshotOutcome, RepositoryError> {
        validate_scope_identity(namespace, run_id)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(ScopeRepositoryError::RequiresSqlite));
        };
        let scope_id: Option<String> = sqlx::query_scalar(
            "SELECT scope_id FROM tickr_ctx_scopes WHERE namespace = ?1 AND run_id = ?2",
        )
        .bind(namespace)
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .map_err(repository_sqlx_error)?;
        let Some(scope_id) = scope_id else {
            return Ok(ScopeSnapshotOutcome::Missing);
        };
        let scope_id = Uuid::parse_str(&scope_id)
            .map_err(|_| corrupt_value(format!("scope id `{scope_id}` is not a UUID")))?;
        self.snapshot_tickr_ctx_scope(scope_id, now).await
    }

    pub async fn cleanup_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<ScopeCleanupOutcome, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(ScopeRepositoryError::RequiresSqlite));
        };
        let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
        let encoded_scope_id = encode_uuid(scope_id);
        let Some(scope) = load_scope(&mut tx, &encoded_scope_id).await? else {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeCleanupOutcome::Missing);
        };
        if scope.state == TickrCtxScopeState::Quarantined {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeCleanupOutcome::Quarantined {
                scope_id,
                diagnostic: scope
                    .quarantine_reason
                    .unwrap_or_else(|| "scope is quarantined".to_owned()),
            });
        }
        if scope.state == TickrCtxScopeState::Active {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeCleanupOutcome::SnapshotRequired);
        }
        if let Err(diagnostic) = snapshot_from_record(scope_id, &scope) {
            quarantine_scope(&mut tx, &encoded_scope_id, &diagnostic, now).await?;
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeCleanupOutcome::Quarantined {
                scope_id,
                diagnostic,
            });
        }
        if scope.state == TickrCtxScopeState::Cleaned {
            tx.commit().await.map_err(repository_sqlx_error)?;
            return Ok(ScopeCleanupOutcome::AlreadyCleaned);
        }

        sqlx::query("DELETE FROM tickr_ctx_scope_values WHERE scope_id = ?1")
            .bind(&encoded_scope_id)
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
        sqlx::query("DELETE FROM tickr_ctx_scope_claims WHERE scope_id = ?1")
            .bind(&encoded_scope_id)
            .execute(&mut *tx)
            .await
            .map_err(repository_sqlx_error)?;
        sqlx::query(
            "UPDATE tickr_ctx_scopes SET state = 'cleaned', cleaned_at = ?2, updated_at = ?2 \
             WHERE scope_id = ?1 AND state = 'snapshotted'",
        )
        .bind(&encoded_scope_id)
        .bind(encode_timestamp(now))
        .execute(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
        tx.commit().await.map_err(repository_sqlx_error)?;
        Ok(ScopeCleanupOutcome::Cleaned)
    }
}

impl ScopeStore for WriterRepositoryBundle {
    fn create_tickr_ctx_scope<'a>(
        &'a self,
        input: CreateTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeCreationOutcome> {
        Box::pin(async move {
            WriterRepositoryBundle::create_tickr_ctx_scope(self, input)
                .await
                .map_err(|error| Box::new(error) as ScopeStoreError)
        })
    }

    fn write_tickr_ctx_scope<'a>(
        &'a self,
        input: WriteTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeWriteOutcome> {
        Box::pin(async move {
            WriterRepositoryBundle::write_tickr_ctx_scope(self, input)
                .await
                .map_err(|error| Box::new(error) as ScopeStoreError)
        })
    }

    fn delete_tickr_ctx_scope_value<'a>(
        &'a self,
        input: DeleteTickrCtxScopeInput<'a>,
    ) -> ScopeStoreFuture<'a, ScopeDeleteOutcome> {
        Box::pin(async move {
            WriterRepositoryBundle::delete_tickr_ctx_scope_value(self, input)
                .await
                .map_err(|error| Box::new(error) as ScopeStoreError)
        })
    }

    fn read_tickr_ctx_scope(
        &self,
        scope_id: Uuid,
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'_, ScopeReadOutcome> {
        Box::pin(async move {
            WriterRepositoryBundle::read_tickr_ctx_scope(self, scope_id, now)
                .await
                .map_err(|error| Box::new(error) as ScopeStoreError)
        })
    }

    fn snapshot_tickr_ctx_scope_for_run<'a>(
        &'a self,
        namespace: &'a str,
        run_id: &'a str,
        now: DateTime<Utc>,
    ) -> ScopeStoreFuture<'a, ScopeSnapshotOutcome> {
        Box::pin(async move {
            WriterRepositoryBundle::snapshot_tickr_ctx_scope_for_run(self, namespace, run_id, now)
                .await
                .map_err(|error| Box::new(error) as ScopeStoreError)
        })
    }
}

async fn load_scope(
    tx: &mut Transaction<'_, Sqlite>,
    scope_id: &str,
) -> Result<Option<ScopeRecord>, RepositoryError> {
    let Some(row) = sqlx::query(
        "SELECT protocol_version, state, created_at, snapshot, snapshot_digest, snapshot_row_count, \
         snapshot_value_bytes, quarantine_reason FROM tickr_ctx_scopes WHERE scope_id = ?1",
    )
    .bind(scope_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?
    else {
        return Ok(None);
    };
    let protocol_version: i64 = row
        .try_get("protocol_version")
        .map_err(repository_sqlx_error)?;
    if protocol_version != TICKR_CTX_SCOPE_PROTOCOL_VERSION {
        return Err(corrupt_value(format!(
            "scope `{scope_id}` uses unknown protocol version {protocol_version}"
        )));
    }
    let state: String = row.try_get("state").map_err(repository_sqlx_error)?;
    let created_at: i64 = row.try_get("created_at").map_err(repository_sqlx_error)?;
    Ok(Some(ScopeRecord {
        state: TickrCtxScopeState::parse(&state).map_err(|source| {
            corrupt_value(format!("scope `{scope_id}` has invalid state: {source}"))
        })?,
        created_at: decode_timestamp(created_at).map_err(|source| {
            corrupt_value(format!(
                "scope `{scope_id}` has invalid creation time: {source}"
            ))
        })?,
        snapshot: row.try_get("snapshot").map_err(repository_sqlx_error)?,
        snapshot_digest: row
            .try_get("snapshot_digest")
            .map_err(repository_sqlx_error)?,
        snapshot_row_count: row
            .try_get("snapshot_row_count")
            .map_err(repository_sqlx_error)?,
        snapshot_value_bytes: row
            .try_get("snapshot_value_bytes")
            .map_err(repository_sqlx_error)?,
        quarantine_reason: row
            .try_get("quarantine_reason")
            .map_err(repository_sqlx_error)?,
    }))
}

async fn load_scope_values(
    tx: &mut Transaction<'_, Sqlite>,
    scope_id: &str,
) -> Result<Vec<StoredScopeValue>, RepositoryError> {
    let rows = sqlx::query(
        "SELECT key, value_identity, envelope, created_at, updated_at \
         FROM tickr_ctx_scope_values WHERE scope_id = ?1 ORDER BY key",
    )
    .bind(scope_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    rows.into_iter()
        .map(|row| {
            let key: String = row.try_get("key").map_err(repository_sqlx_error)?;
            let value_identity: String = row
                .try_get("value_identity")
                .map_err(repository_sqlx_error)?;
            let envelope = row.try_get("envelope").map_err(|source| {
                corrupt_value(format!(
                    "scope `{scope_id}` value `{value_identity}` at key `{key}` is unreadable: {source}"
                ))
            })?;
            let created_at: i64 = row.try_get("created_at").map_err(repository_sqlx_error)?;
            let updated_at: i64 = row.try_get("updated_at").map_err(repository_sqlx_error)?;
            Ok(StoredScopeValue {
                key,
                value_identity,
                envelope,
                created_at: decode_timestamp(created_at)
                    .map_err(|source| corrupt_value(source.to_string()))?,
                updated_at: decode_timestamp(updated_at)
                    .map_err(|source| corrupt_value(source.to_string()))?,
            })
        })
        .collect()
}

async fn insert_scope_values(
    tx: &mut Transaction<'_, Sqlite>,
    scope_id: &str,
    values: &[ScopeValueInput<'_>],
    now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    for value in values {
        sqlx::query(
            "INSERT INTO tickr_ctx_scope_values \
             (scope_id, key, value_identity, envelope, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?5) \
             ON CONFLICT(scope_id, key) DO UPDATE SET \
             envelope = excluded.envelope, updated_at = excluded.updated_at",
        )
        .bind(scope_id)
        .bind(value.key)
        .bind(encode_uuid(Uuid::new_v4()))
        .bind(value.envelope)
        .bind(encode_timestamp(now))
        .execute(&mut **tx)
        .await
        .map_err(repository_sqlx_error)?;
    }
    Ok(())
}

async fn insert_claim(
    tx: &mut Transaction<'_, Sqlite>,
    claim_id: &str,
    scope_id: &str,
    request_digest: &str,
    now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    sqlx::query(
        "INSERT INTO tickr_ctx_scope_claims (claim_id, scope_id, request_digest, committed_at) \
         VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(claim_id)
    .bind(scope_id)
    .bind(request_digest)
    .bind(encode_timestamp(now))
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(())
}

async fn quarantine_scope(
    tx: &mut Transaction<'_, Sqlite>,
    scope_id: &str,
    diagnostic: &str,
    now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    eprintln!("tickr-ctx scope {scope_id} quarantined: {diagnostic}");
    sqlx::query(
        "UPDATE tickr_ctx_scopes SET state = 'quarantined', quarantine_reason = ?2, \
         updated_at = ?3 WHERE scope_id = ?1",
    )
    .bind(scope_id)
    .bind(diagnostic)
    .bind(encode_timestamp(now))
    .execute(&mut **tx)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(())
}

fn validate_scope_identity(namespace: &str, run_id: &str) -> Result<(), RepositoryError> {
    if namespace.is_empty() || namespace.len() > MAX_NAMESPACE_BYTES {
        return Err(configuration_error(ScopeRepositoryError::InvalidNamespace));
    }
    if run_id.is_empty() || run_id.len() > MAX_RUN_ID_BYTES {
        return Err(configuration_error(ScopeRepositoryError::InvalidRunId));
    }
    Ok(())
}

fn validate_mutation(
    values: &[ScopeValueInput<'_>],
    existing_rows: usize,
    existing_bytes: usize,
) -> Option<ScopeMutationRejection> {
    if values.is_empty() {
        return Some(ScopeMutationRejection::EmptyRequest);
    }
    if values.len() > MAX_SCOPE_REQUEST_VALUES {
        return Some(ScopeMutationRejection::Bound(
            ScopeBoundViolation::RequestValues {
                actual: values.len(),
                limit: MAX_SCOPE_REQUEST_VALUES,
            },
        ));
    }

    let request_bytes = values
        .iter()
        .map(|value| value.key.len().saturating_add(value.envelope.len()))
        .sum::<usize>();
    if request_bytes > MAX_SCOPE_REQUEST_BYTES {
        return Some(ScopeMutationRejection::Bound(
            ScopeBoundViolation::RequestBytes {
                actual: request_bytes,
                limit: MAX_SCOPE_REQUEST_BYTES,
            },
        ));
    }
    for value in values {
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
    let resulting_rows = existing_rows.saturating_add(values.len());
    if resulting_rows > MAX_SCOPE_ROWS {
        return Some(ScopeMutationRejection::Bound(
            ScopeBoundViolation::ScopeRows {
                actual: resulting_rows,
                limit: MAX_SCOPE_ROWS,
            },
        ));
    }
    let resulting_bytes = existing_bytes.saturating_add(
        values
            .iter()
            .map(|value| value.envelope.len())
            .sum::<usize>(),
    );
    if resulting_bytes > MAX_SCOPE_BYTES {
        return Some(ScopeMutationRejection::Bound(
            ScopeBoundViolation::ScopeBytes {
                actual: resulting_bytes,
                limit: MAX_SCOPE_BYTES,
            },
        ));
    }
    None
}
fn resulting_scope_bound(
    existing: &HashMap<String, usize>,
    values: &[ScopeValueInput<'_>],
) -> Option<ScopeBoundViolation> {
    let inserted = values
        .iter()
        .filter(|value| !existing.contains_key(value.key))
        .count();
    let resulting_rows = existing.len().saturating_add(inserted);
    if resulting_rows > MAX_SCOPE_ROWS {
        return Some(ScopeBoundViolation::ScopeRows {
            actual: resulting_rows,
            limit: MAX_SCOPE_ROWS,
        });
    }
    let replaced_bytes = values
        .iter()
        .filter_map(|value| existing.get(value.key))
        .copied()
        .sum::<usize>();
    let new_bytes = values
        .iter()
        .map(|value| value.envelope.len())
        .sum::<usize>();
    let resulting_bytes = existing
        .values()
        .copied()
        .sum::<usize>()
        .saturating_sub(replaced_bytes)
        .saturating_add(new_bytes);
    (resulting_bytes > MAX_SCOPE_BYTES).then_some(ScopeBoundViolation::ScopeBytes {
        actual: resulting_bytes,
        limit: MAX_SCOPE_BYTES,
    })
}

fn mutation_digest(
    scope_id: Uuid,
    values: &[ScopeValueInput<'_>],
) -> Result<String, RepositoryError> {
    let mut keys = BTreeSet::new();
    for value in values {
        if value.key.is_empty() || value.key.len() > MAX_KEY_BYTES {
            return Err(configuration_error(ScopeRepositoryError::InvalidKey));
        }
        if !keys.insert(value.key) {
            return Err(configuration_error(ScopeRepositoryError::DuplicateKey(
                value.key.to_owned(),
            )));
        }
    }
    let mut ordered = values.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|value| value.key);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(scope_id.as_bytes());
    for value in ordered {
        append_len_prefixed(&mut bytes, value.key.as_bytes());
        append_len_prefixed(&mut bytes, value.envelope);
    }
    Ok(sha256_hex(&bytes))
}

fn delete_digest(scope_id: Uuid, key: &str) -> Result<String, RepositoryError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(configuration_error(ScopeRepositoryError::InvalidKey));
    }
    let mut bytes = Vec::with_capacity(scope_id.as_bytes().len() + key.len() + 16);
    bytes.extend_from_slice(scope_id.as_bytes());
    append_len_prefixed(&mut bytes, b"delete");
    append_len_prefixed(&mut bytes, key.as_bytes());
    Ok(sha256_hex(&bytes))
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

fn first_invalid_stored_envelope(values: &[StoredScopeValue]) -> Option<String> {
    values.iter().find_map(|value| {
        validate_envelope(&value.envelope).err().map(|reason| {
            format!(
                "value {} at key `{}` has {reason:?}",
                value.value_identity, value.key
            )
        })
    })
}

fn stored_bounds(values: &[StoredScopeValue]) -> Option<ScopeBoundViolation> {
    if values.len() > MAX_SCOPE_ROWS {
        return Some(ScopeBoundViolation::ScopeRows {
            actual: values.len(),
            limit: MAX_SCOPE_ROWS,
        });
    }
    for value in values {
        if value.envelope.len() > MAX_SCOPE_VALUE_BYTES {
            return Some(ScopeBoundViolation::ValueBytes {
                key: value.key.clone(),
                actual: value.envelope.len(),
                limit: MAX_SCOPE_VALUE_BYTES,
            });
        }
    }
    let bytes = values.iter().map(|value| value.envelope.len()).sum();
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

fn encode_snapshot(values: &[StoredScopeValue]) -> Vec<u8> {
    let capacity = SNAPSHOT_MAGIC.len()
        + 4
        + values
            .iter()
            .map(|value| 8 + value.key.len() + value.envelope.len())
            .sum::<usize>();
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(values.len())
            .expect("scope row bound fits u32")
            .to_be_bytes(),
    );
    for value in values {
        append_len_prefixed(&mut bytes, value.key.as_bytes());
        append_len_prefixed(&mut bytes, &value.envelope);
    }
    bytes
}

/// Decode a committed scope snapshot without reinterpreting its opaque value
/// envelopes. This is used only to preserve the existing archive response
/// shape while the raw snapshot and digest remain the integrity evidence.
pub fn decode_tickr_ctx_scope_snapshot(
    snapshot: &TickrCtxScopeSnapshot,
) -> Result<Vec<(String, Vec<u8>)>, RepositoryError> {
    if !snapshot.bytes.starts_with(SNAPSHOT_MAGIC) || sha256_hex(&snapshot.bytes) != snapshot.digest
    {
        return Err(corrupt_value("scope snapshot format or digest is invalid"));
    }
    let mut offset = SNAPSHOT_MAGIC.len();
    let count = read_snapshot_length(&snapshot.bytes, &mut offset)? as usize;
    if count != snapshot.row_count {
        return Err(corrupt_value(
            "scope snapshot row count does not match its metadata",
        ));
    }
    let mut values = Vec::with_capacity(count);
    let mut value_bytes = 0usize;
    for _ in 0..count {
        let key = read_snapshot_bytes(&snapshot.bytes, &mut offset)?;
        let key = String::from_utf8(key)
            .map_err(|_| corrupt_value("scope snapshot contains a non-UTF-8 key"))?;
        let envelope = read_snapshot_bytes(&snapshot.bytes, &mut offset)?;
        value_bytes = value_bytes
            .checked_add(envelope.len())
            .ok_or_else(|| corrupt_value("scope snapshot byte count overflowed"))?;
        values.push((key, envelope));
    }
    if offset != snapshot.bytes.len() || value_bytes != snapshot.value_bytes {
        return Err(corrupt_value(
            "scope snapshot payload or byte count does not match its metadata",
        ));
    }
    Ok(values)
}

fn read_snapshot_length(bytes: &[u8], offset: &mut usize) -> Result<u32, RepositoryError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| corrupt_value("scope snapshot offset overflowed"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt_value("scope snapshot is truncated"))?;
    *offset = end;
    Ok(u32::from_be_bytes(
        raw.try_into().expect("slice length is four"),
    ))
}

fn read_snapshot_bytes(bytes: &[u8], offset: &mut usize) -> Result<Vec<u8>, RepositoryError> {
    let length = read_snapshot_length(bytes, offset)? as usize;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| corrupt_value("scope snapshot length overflowed"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt_value("scope snapshot is truncated"))?
        .to_vec();
    *offset = end;
    Ok(value)
}

fn append_len_prefixed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(
        &u32::try_from(value.len())
            .expect("scope byte bound fits u32")
            .to_be_bytes(),
    );
    target.extend_from_slice(value);
}

fn snapshot_from_record(
    scope_id: Uuid,
    scope: &ScopeRecord,
) -> Result<TickrCtxScopeSnapshot, String> {
    let bytes = scope
        .snapshot
        .clone()
        .ok_or_else(|| "snapshot state is missing snapshot bytes".to_owned())?;
    let digest = scope
        .snapshot_digest
        .clone()
        .ok_or_else(|| "snapshot state is missing digest".to_owned())?;
    let row_count = scope
        .snapshot_row_count
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| "snapshot state has invalid row count".to_owned())?;
    let value_bytes = scope
        .snapshot_value_bytes
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| "snapshot state has invalid value-byte count".to_owned())?;
    if !bytes.starts_with(SNAPSHOT_MAGIC) {
        return Err("snapshot uses an unknown format version".to_owned());
    }
    if sha256_hex(&bytes) != digest {
        return Err("snapshot digest does not match committed bytes".to_owned());
    }
    Ok(TickrCtxScopeSnapshot {
        scope_id,
        bytes,
        digest,
        row_count,
        value_bytes,
    })
}

fn configuration_error(source: ScopeRepositoryError) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Configuration, source)
}

fn corrupt_value(message: impl Into<String>) -> RepositoryError {
    RepositoryError::new(
        RepositoryErrorKind::CorruptStoredValue,
        ScopeRepositoryError::CorruptStoredValue(message.into()),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha256(bytes);
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn sha256(input: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity((input.len() + 72) & !63);
    padded.extend_from_slice(input);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    let mut state = [
        0x6a09e667_u32,
        0xbb67ae85,
        0x3c6ef372,
        0xa54ff53a,
        0x510e527f,
        0x9b05688c,
        0x1f83d9ab,
        0x5be0cd19,
    ];
    for chunk in padded.chunks_exact(64) {
        let mut words = [0_u32; 64];
        for (index, word) in words[..16].iter_mut().enumerate() {
            let offset = index * 4;
            *word = u32::from_be_bytes(chunk[offset..offset + 4].try_into().unwrap());
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (current, working) in state.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *current = current.wrapping_add(working);
        }
    }
    let mut digest = [0_u8; 32];
    for (index, word) in state.into_iter().enumerate() {
        digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENVELOPE: &[u8] = br#"{"v":2,"type":"string","value":"x","secret":false,"producer":{"kind":"system","component":"test"},"created_at":"2026-01-01T00:00:00Z","sha256":"x"}"#;

    #[test]
    fn sha256_matches_the_published_empty_vector() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn every_bound_has_a_typed_outcome() {
        let oversized = vec![0_u8; MAX_SCOPE_VALUE_BYTES + 1];
        let value = ScopeValueInput {
            key: "value",
            envelope: &oversized,
        };
        assert!(matches!(
            validate_mutation(&[value], 0, 0),
            Some(ScopeMutationRejection::Bound(
                ScopeBoundViolation::ValueBytes { .. }
            ))
        ));

        let many = vec![
            ScopeValueInput {
                key: "value",
                envelope: ENVELOPE,
            };
            MAX_SCOPE_REQUEST_VALUES + 1
        ];
        assert!(matches!(
            validate_mutation(&many, 0, 0),
            Some(ScopeMutationRejection::Bound(
                ScopeBoundViolation::RequestValues { .. }
            ))
        ));

        let request_bytes = vec![0_u8; MAX_SCOPE_REQUEST_BYTES];
        let request = ScopeValueInput {
            key: "value",
            envelope: &request_bytes,
        };
        assert!(matches!(
            validate_mutation(&[request], 0, 0),
            Some(ScopeMutationRejection::Bound(
                ScopeBoundViolation::RequestBytes { .. }
            ))
        ));

        let valid = ScopeValueInput {
            key: "value",
            envelope: ENVELOPE,
        };
        assert!(matches!(
            validate_mutation(&[valid], MAX_SCOPE_ROWS, 0),
            Some(ScopeMutationRejection::Bound(
                ScopeBoundViolation::ScopeRows { .. }
            ))
        ));
        assert!(matches!(
            validate_mutation(&[valid], 0, MAX_SCOPE_BYTES),
            Some(ScopeMutationRejection::Bound(
                ScopeBoundViolation::ScopeBytes { .. }
            ))
        ));

        let now = Utc::now();
        assert!(matches!(
            age_bound(
                now - chrono::Duration::seconds(MAX_SCOPE_AGE_SECONDS + 1),
                now
            ),
            Some(ScopeBoundViolation::ScopeAgeSeconds { .. })
        ));
    }
}
