//! SQLite-backed staging records for the Tickr Lite Compaction drain.
//!
//! A staging record keeps the published envelope bytes until archive state has
//! committed. A purged tombstone keeps the archive identity and payload digest,
//! so late redelivery cannot create a second archive or replace durable state.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, RepositoryError, RepositoryErrorKind,
    WriterRepositoryBundle,
};
use crate::encoding::{encode_json, encode_timestamp, encode_uuid};

pub const LOCAL_COMPACTION_PROTOCOL_VERSION: i64 = 1;

#[derive(Debug, Clone, Copy)]
pub struct StageLocalCompactionInput<'a> {
    pub workflow_instance_id: Uuid,
    pub payload: &'a [u8],
    pub payload_digest: &'a str,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageLocalCompactionOutcome {
    Staged,
    AlreadyStaged,
    AlreadyComplete,
    AlreadyPurged,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LocalCompactionDrainRecord {
    Staged {
        workflow_instance_id: Uuid,
        payload: Vec<u8>,
        payload_digest: String,
    },
    Complete {
        workflow_instance_id: Uuid,
        payload: Vec<u8>,
        payload_digest: String,
        scope_id: Uuid,
        scope_digest: String,
        final_log_references: Value,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgeLocalCompactionOutcome {
    Purged,
    AlreadyPurged,
    NotComplete,
    Missing,
}

#[derive(Debug, thiserror::Error)]
enum CompactionRepositoryError {
    #[error("local Compaction operations require the SQLite writer")]
    RequiresSqlite,
    #[error("Compaction payload digest must be a 64-character SHA-256 hex string")]
    InvalidDigest,
    #[error("Compaction archive identity {0} was redelivered with different payload bytes")]
    PayloadIdentityConflict(Uuid),
    #[error("stored local Compaction record is corrupt: {0}")]
    CorruptStoredValue(String),
}

impl WriterRepositoryBundle {
    /// Durably stage one published Compaction envelope before its relay ACK.
    pub async fn stage_local_compaction(
        &self,
        input: StageLocalCompactionInput<'_>,
    ) -> Result<StageLocalCompactionOutcome, RepositoryError> {
        validate_digest(input.payload_digest)?;
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                CompactionRepositoryError::RequiresSqlite,
            ));
        };
        stage_local_compaction_sqlite(pool, input).await
    }

    /// Select the oldest record whose drain work is unfinished. A `staged`
    /// record still needs archive commit; a `complete` record needs only
    /// idempotent source cleanup. No database handle escapes this boundary.
    pub async fn select_local_compaction_for_drain(
        &self,
    ) -> Result<Option<LocalCompactionDrainRecord>, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                CompactionRepositoryError::RequiresSqlite,
            ));
        };
        let row: Option<(
            String,
            i64,
            Option<String>,
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = sqlx::query_as(
            "SELECT workflow_instance_id, protocol_version, payload, payload_digest, \
                    state, scope_id, scope_digest, final_log_references \
             FROM local_compaction_staging WHERE state IN ('staged', 'complete') \
             ORDER BY staged_at, workflow_instance_id LIMIT 1",
        )
        .fetch_optional(pool)
        .await
        .map_err(repository_sqlx_error)?;
        row.map(
            |(
                workflow_instance_id,
                protocol_version,
                payload,
                payload_digest,
                state,
                scope_id,
                scope_digest,
                final_log_references,
            )| {
                if protocol_version != LOCAL_COMPACTION_PROTOCOL_VERSION {
                    return Err(corrupt_value(format!(
                        "local Compaction record has unknown protocol version {protocol_version}"
                    )));
                }
                validate_digest(&payload_digest)?;
                let workflow_instance_id =
                    Uuid::parse_str(&workflow_instance_id).map_err(|_| {
                        corrupt_value(format!(
                            "workflow instance id `{workflow_instance_id}` is not a UUID"
                        ))
                    })?;
                let payload = payload
                    .ok_or_else(|| corrupt_value("unfinished Compaction record has no payload"))
                    .and_then(|payload| decode_bytes(&payload))?;
                match state.as_str() {
                    "staged" => {
                        if scope_id.is_some()
                            || scope_digest.is_some()
                            || final_log_references.is_some()
                        {
                            return Err(corrupt_value(
                                "staged Compaction record contains completion evidence",
                            ));
                        }
                        Ok(LocalCompactionDrainRecord::Staged {
                            workflow_instance_id,
                            payload,
                            payload_digest,
                        })
                    }
                    "complete" => {
                        let scope_id = scope_id
                            .ok_or_else(|| {
                                corrupt_value("complete Compaction record has no scope identity")
                            })
                            .and_then(|scope_id| {
                                Uuid::parse_str(&scope_id).map_err(|_| {
                                    corrupt_value(format!(
                                        "scope id `{scope_id}` is not a UUID"
                                    ))
                                })
                            })?;
                        let scope_digest = scope_digest.ok_or_else(|| {
                            corrupt_value("complete Compaction record has no scope digest")
                        })?;
                        validate_digest(&scope_digest)?;
                        let final_log_references = final_log_references
                            .ok_or_else(|| {
                                corrupt_value(
                                    "complete Compaction record has no final-Log references",
                                )
                            })
                            .and_then(|references| {
                                serde_json::from_str(&references).map_err(|error| {
                                    corrupt_value(format!(
                                        "complete Compaction final-Log references are invalid: {error}"
                                    ))
                                })
                            })?;
                        Ok(LocalCompactionDrainRecord::Complete {
                            workflow_instance_id,
                            payload,
                            payload_digest,
                            scope_id,
                            scope_digest,
                            final_log_references,
                        })
                    }
                    state => Err(corrupt_value(format!(
                        "local Compaction record has unknown state `{state}`"
                    ))),
                }
            },
        )
        .transpose()
    }

    /// Remove staged bytes only after archive state and every durable reference
    /// have committed. The retained tombstone makes late redelivery idempotent.
    pub async fn purge_completed_local_compaction(
        &self,
        workflow_instance_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<PurgeLocalCompactionOutcome, RepositoryError> {
        let BackendPool::Sqlite(pool) = &self.pool else {
            return Err(configuration_error(
                CompactionRepositoryError::RequiresSqlite,
            ));
        };
        let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM local_compaction_staging WHERE workflow_instance_id = ?1",
        )
        .bind(encode_uuid(workflow_instance_id))
        .fetch_optional(&mut *tx)
        .await
        .map_err(repository_sqlx_error)?;
        let outcome = match state.as_deref() {
            None => PurgeLocalCompactionOutcome::Missing,
            Some("purged") => PurgeLocalCompactionOutcome::AlreadyPurged,
            Some("complete") => {
                sqlx::query(
                    "UPDATE local_compaction_staging \
                     SET state = 'purged', payload = NULL, purged_at = ?2 \
                     WHERE workflow_instance_id = ?1 AND state = 'complete'",
                )
                .bind(encode_uuid(workflow_instance_id))
                .bind(encode_timestamp(now))
                .execute(&mut *tx)
                .await
                .map_err(repository_sqlx_error)?;
                PurgeLocalCompactionOutcome::Purged
            }
            Some("staged") => PurgeLocalCompactionOutcome::NotComplete,
            Some(state) => {
                return Err(corrupt_value(format!(
                    "local Compaction record has unknown state `{state}`"
                )));
            }
        };
        tx.commit().await.map_err(repository_sqlx_error)?;
        Ok(outcome)
    }
}

async fn stage_local_compaction_sqlite(
    pool: &SqlitePool,
    input: StageLocalCompactionInput<'_>,
) -> Result<StageLocalCompactionOutcome, RepositoryError> {
    let mut tx = pool.begin().await.map_err(repository_sqlx_error)?;
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO local_compaction_staging \
         (workflow_instance_id, protocol_version, payload_digest, payload, state, staged_at) \
         VALUES (?1, ?2, ?3, ?4, 'staged', ?5)",
    )
    .bind(encode_uuid(input.workflow_instance_id))
    .bind(LOCAL_COMPACTION_PROTOCOL_VERSION)
    .bind(input.payload_digest)
    .bind(encode_bytes(input.payload))
    .bind(encode_timestamp(input.now))
    .execute(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;
    if inserted.rows_affected() == 1 {
        tx.commit().await.map_err(repository_sqlx_error)?;
        return Ok(StageLocalCompactionOutcome::Staged);
    }

    let row: Option<(i64, String, String)> = sqlx::query_as(
        "SELECT protocol_version, payload_digest, state \
         FROM local_compaction_staging WHERE workflow_instance_id = ?1",
    )
    .bind(encode_uuid(input.workflow_instance_id))
    .fetch_optional(&mut *tx)
    .await
    .map_err(repository_sqlx_error)?;
    tx.commit().await.map_err(repository_sqlx_error)?;
    let Some((protocol_version, payload_digest, state)) = row else {
        return Err(corrupt_value("staged Compaction row disappeared"));
    };
    if protocol_version != LOCAL_COMPACTION_PROTOCOL_VERSION {
        return Err(corrupt_value(format!(
            "local Compaction record has unknown protocol version {protocol_version}"
        )));
    }
    if payload_digest != input.payload_digest {
        return Err(RepositoryError::new(
            RepositoryErrorKind::ConstraintConflict,
            CompactionRepositoryError::PayloadIdentityConflict(input.workflow_instance_id),
        ));
    }
    match state.as_str() {
        "staged" => Ok(StageLocalCompactionOutcome::AlreadyStaged),
        "complete" => Ok(StageLocalCompactionOutcome::AlreadyComplete),
        "purged" => Ok(StageLocalCompactionOutcome::AlreadyPurged),
        _ => Err(corrupt_value(format!(
            "local Compaction record has unknown state `{state}`"
        ))),
    }
}

fn validate_digest(value: &str) -> Result<(), RepositoryError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(configuration_error(
            CompactionRepositoryError::InvalidDigest,
        ))
    }
}

fn encode_bytes(bytes: &[u8]) -> String {
    encode_json(&Value::Array(
        bytes.iter().map(|byte| Value::from(*byte)).collect(),
    ))
}

fn decode_bytes(value: &str) -> Result<Vec<u8>, RepositoryError> {
    serde_json::from_str(value)
        .map_err(|error| corrupt_value(format!("stored Compaction payload is invalid: {error}")))
}

fn configuration_error(source: CompactionRepositoryError) -> RepositoryError {
    RepositoryError::new(RepositoryErrorKind::Configuration, source)
}

fn corrupt_value(message: impl Into<String>) -> RepositoryError {
    RepositoryError::new(
        RepositoryErrorKind::CorruptStoredValue,
        CompactionRepositoryError::CorruptStoredValue(message.into()),
    )
}
