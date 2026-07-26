//! SQL-backed Signal audit and Trigger-derived Event-variable persistence.
//!
//! Signal transport and NATS working-set state remain outside this repository.
//! The operations here own only the durable audit rows, materialization linkage,
//! terminal cleanup state, and the API's read projection.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, SqlitePool};
use uuid::Uuid;

use crate::backend::{
    repository_sqlx_error, BackendPool, ReadOnlyRepositoryBundle, RepositoryError,
    RepositoryErrorKind, WriterRepositoryBundle,
};
use crate::encoding::{
    decode_json, decode_timestamp, decode_uuid, encode_json, encode_timestamp, encode_uuid,
};

#[derive(Debug, Clone, PartialEq)]
pub struct SignalCapturesInput {
    pub signal_id: Uuid,
    pub workflow_id: Uuid,
    pub workflow_version: Option<i64>,
    pub captures: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignalCapturesRecord {
    pub signal_id: Uuid,
    pub workflow_id: Uuid,
    pub workflow_version: Option<i64>,
    pub captures: Value,
    pub created_at: DateTime<Utc>,
    pub materialized_run_id: Option<Uuid>,
    pub terminal_at: Option<DateTime<Utc>>,
}

impl SignalCapturesRecord {
    pub fn capture_names(&self) -> Vec<&str> {
        self.captures
            .as_array()
            .expect("repository validates capture arrays")
            .iter()
            .map(|capture| {
                capture["name"]
                    .as_str()
                    .expect("repository validates capture names")
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignalCancelInput {
    pub signal_id: Uuid,
    pub applied_count: i32,
    pub target: Value,
    pub note: Option<String>,
}

/// Durable marker for a ByTag cancellation awaiting materialization.
pub const PENDING_SIGNAL_CANCEL_APPLIED_COUNT: i32 = -1;

#[derive(Debug, Clone, PartialEq)]
pub struct SignalCancelRecord {
    pub signal_id: Uuid,
    pub applied_count: i32,
    pub target: Value,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalWakeupInput {
    pub signal_id: Uuid,
    pub name: String,
    pub matched_workflows: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalWakeupRecord {
    pub signal_id: Uuid,
    pub name: String,
    pub matched_workflows: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SignalAuditRecord {
    Wakeup(SignalWakeupRecord),
    Captures(SignalCapturesRecord),
    Cancel(SignalCancelRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalLinkageOutcome {
    Linked,
    AlreadyLinked { workflow_instance_id: Uuid },
    Absent,
}

#[derive(Debug, thiserror::Error)]
#[error("corrupt stored Signal audit value: {0}")]
struct CorruptSignal(String);

impl WriterRepositoryBundle {
    /// Insert one Trigger-derived Event-variable archive. Signal identity is the
    /// idempotency key: a repeated insert preserves the first complete record.
    pub async fn insert_signal_captures(
        &self,
        input: &SignalCapturesInput,
    ) -> Result<bool, RepositoryError> {
        validate_captures(&input.captures)?;
        match &self.pool {
            BackendPool::Postgres(pool) => insert_captures_postgres(pool, input).await,
            BackendPool::Sqlite(pool) => insert_captures_sqlite(pool, input).await,
        }
    }

    /// Read a Trigger-derived Event-variable archive for rehydration.
    pub async fn signal_captures(
        &self,
        signal_id: Uuid,
    ) -> Result<Option<SignalCapturesRecord>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => captures_postgres(pool, signal_id).await,
            BackendPool::Sqlite(pool) => captures_sqlite(pool, signal_id).await,
        }
    }

    /// Link a Signal to at most one materialized Workflow instance.
    pub async fn link_signal_captures(
        &self,
        signal_id: Uuid,
        workflow_instance_id: Uuid,
    ) -> Result<SignalLinkageOutcome, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                link_captures_postgres(pool, signal_id, workflow_instance_id).await
            }
            BackendPool::Sqlite(pool) => {
                link_captures_sqlite(pool, signal_id, workflow_instance_id).await
            }
        }
    }

    /// Atomically mark every unresolved Event-variable archive for one terminal
    /// Workflow instance and return the values needed for NATS cleanup.
    pub async fn mark_signal_captures_terminal(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => mark_terminal_postgres(pool, workflow_instance_id).await,
            BackendPool::Sqlite(pool) => mark_terminal_sqlite(pool, workflow_instance_id).await,
        }
    }

    /// List settled Event-variable archives whose cleanup grace has elapsed.
    pub async fn expired_signal_captures(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => expired_captures_postgres(pool, cutoff).await,
            BackendPool::Sqlite(pool) => expired_captures_sqlite(pool, cutoff).await,
        }
    }

    /// Delete only a still-settled archive older than the supplied cutoff.
    /// Repeated cleanup is a no-op and an unresolved row can never match.
    pub async fn delete_expired_signal_captures(
        &self,
        signal_id: Uuid,
        cutoff: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let rows_affected = match &self.pool {
            BackendPool::Postgres(pool) => sqlx::query(
                "DELETE FROM signal_captures WHERE signal_id = $1 AND terminal_at IS NOT NULL AND terminal_at < $2",
            )
            .bind(signal_id)
            .bind(cutoff)
            .execute(pool)
            .await
            .map(|result| result.rows_affected()),
            BackendPool::Sqlite(pool) => sqlx::query(
                "DELETE FROM signal_captures WHERE signal_id = ?1 AND terminal_at IS NOT NULL AND terminal_at < ?2",
            )
            .bind(encode_uuid(signal_id))
            .bind(encode_timestamp(cutoff))
            .execute(pool)
            .await
            .map(|result| result.rows_affected()),
        }
        .map_err(repository_sqlx_error)?;
        Ok(rows_affected == 1)
    }

    pub async fn insert_signal_cancel(
        &self,
        record: &SignalCancelInput,
    ) -> Result<bool, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                let result = sqlx::query(
                    "INSERT INTO signal_cancels (signal_id, applied_count, target, note) VALUES ($1, $2, $3, $4) ON CONFLICT (signal_id) DO NOTHING",
                )
                .bind(record.signal_id)
                .bind(record.applied_count)
                .bind(&record.target)
                .bind(record.note.as_deref())
                .execute(pool)
                .await
                .map_err(repository_sqlx_error)?;
                Ok(result.rows_affected() == 1)
            }
            BackendPool::Sqlite(pool) => {
                let result = sqlx::query(
                    "INSERT INTO signal_cancels (signal_id, applied_count, target, note) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (signal_id) DO NOTHING",
                )
                .bind(encode_uuid(record.signal_id))
                .bind(record.applied_count)
                .bind(encode_json(&record.target))
                .bind(record.note.as_deref())
                .execute(pool)
                .await
                .map_err(repository_sqlx_error)?;
                Ok(result.rows_affected() == 1)
            }
        }
    }

    /// Read the durable state of one Cancel Signal.
    pub async fn signal_cancel(
        &self,
        signal_id: Uuid,
    ) -> Result<Option<SignalCancelRecord>, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => cancel_postgres(pool, signal_id).await,
            BackendPool::Sqlite(pool) => cancel_sqlite(pool, signal_id).await,
        }
    }

    /// Materialize a pending ByTag cancellation exactly once.
    pub async fn materialize_signal_cancel(
        &self,
        signal_id: Uuid,
        applied_count: i32,
    ) -> Result<bool, RepositoryError> {
        let rows_affected = match &self.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE signal_cancels SET applied_count = $2 WHERE signal_id = $1 AND applied_count = $3",
                )
                .bind(signal_id)
                .bind(applied_count)
                .bind(PENDING_SIGNAL_CANCEL_APPLIED_COUNT)
                .execute(pool)
                .await
                .map(|result| result.rows_affected())
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE signal_cancels SET applied_count = ?2 WHERE signal_id = ?1 AND applied_count = ?3",
                )
                .bind(encode_uuid(signal_id))
                .bind(applied_count)
                .bind(PENDING_SIGNAL_CANCEL_APPLIED_COUNT)
                .execute(pool)
                .await
                .map(|result| result.rows_affected())
            }
        }
        .map_err(repository_sqlx_error)?;
        Ok(rows_affected == 1)
    }

    pub async fn insert_signal_wakeup(
        &self,
        record: &SignalWakeupInput,
    ) -> Result<bool, RepositoryError> {
        match &self.pool {
            BackendPool::Postgres(pool) => {
                let result = sqlx::query(
                    "INSERT INTO signal_wakeups (signal_id, name, matched_workflows) VALUES ($1, $2, $3) ON CONFLICT (signal_id) DO NOTHING",
                )
                .bind(record.signal_id)
                .bind(&record.name)
                .bind(record.matched_workflows)
                .execute(pool)
                .await
                .map_err(repository_sqlx_error)?;
                Ok(result.rows_affected() == 1)
            }
            BackendPool::Sqlite(pool) => {
                let result = sqlx::query(
                    "INSERT INTO signal_wakeups (signal_id, name, matched_workflows) VALUES (?1, ?2, ?3) ON CONFLICT (signal_id) DO NOTHING",
                )
                .bind(encode_uuid(record.signal_id))
                .bind(&record.name)
                .bind(record.matched_workflows)
                .execute(pool)
                .await
                .map_err(repository_sqlx_error)?;
                Ok(result.rows_affected() == 1)
            }
        }
    }
}

impl ReadOnlyRepositoryBundle {
    /// Read the API projection using its existing Wakeup → Trigger → Cancel
    /// precedence. The order is explicit and identical on both implementations.
    pub async fn signal_audit(
        &self,
        signal_id: Uuid,
    ) -> Result<Option<SignalAuditRecord>, RepositoryError> {
        signal_audit(&self.pool, signal_id).await
    }
}

async fn signal_audit(
    pool: &BackendPool,
    signal_id: Uuid,
) -> Result<Option<SignalAuditRecord>, RepositoryError> {
    match pool {
        BackendPool::Postgres(pool) => {
            if let Some(record) = wakeup_postgres(pool, signal_id).await? {
                return Ok(Some(SignalAuditRecord::Wakeup(record)));
            }
            if let Some(record) = captures_postgres(pool, signal_id).await? {
                return Ok(Some(SignalAuditRecord::Captures(record)));
            }
            Ok(cancel_postgres(pool, signal_id)
                .await?
                .map(SignalAuditRecord::Cancel))
        }
        BackendPool::Sqlite(pool) => {
            if let Some(record) = wakeup_sqlite(pool, signal_id).await? {
                return Ok(Some(SignalAuditRecord::Wakeup(record)));
            }
            if let Some(record) = captures_sqlite(pool, signal_id).await? {
                return Ok(Some(SignalAuditRecord::Captures(record)));
            }
            Ok(cancel_sqlite(pool, signal_id)
                .await?
                .map(SignalAuditRecord::Cancel))
        }
    }
}

async fn insert_captures_postgres(
    pool: &PgPool,
    input: &SignalCapturesInput,
) -> Result<bool, RepositoryError> {
    let result = sqlx::query(
        "INSERT INTO signal_captures (signal_id, workflow_id, workflow_version, captures) VALUES ($1, $2, $3, $4) ON CONFLICT (signal_id) DO NOTHING",
    )
    .bind(input.signal_id)
    .bind(input.workflow_id)
    .bind(input.workflow_version)
    .bind(&input.captures)
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(result.rows_affected() == 1)
}

async fn insert_captures_sqlite(
    pool: &SqlitePool,
    input: &SignalCapturesInput,
) -> Result<bool, RepositoryError> {
    let result = sqlx::query(
        "INSERT INTO signal_captures (signal_id, workflow_id, workflow_version, captures) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (signal_id) DO NOTHING",
    )
    .bind(encode_uuid(input.signal_id))
    .bind(encode_uuid(input.workflow_id))
    .bind(input.workflow_version)
    .bind(encode_json(&input.captures))
    .execute(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(result.rows_affected() == 1)
}

type PostgresCapturesRow = (
    Uuid,
    Uuid,
    Option<i64>,
    Value,
    DateTime<Utc>,
    Option<Uuid>,
    Option<DateTime<Utc>>,
);

type SqliteCapturesRow = (
    String,
    String,
    Option<i64>,
    String,
    i64,
    Option<String>,
    Option<i64>,
);

const CAPTURES_COLUMNS: &str = "signal_id, workflow_id, workflow_version, captures, created_at, materialized_run_id, terminal_at";

async fn captures_postgres(
    pool: &PgPool,
    signal_id: Uuid,
) -> Result<Option<SignalCapturesRecord>, RepositoryError> {
    let query = format!("SELECT {CAPTURES_COLUMNS} FROM signal_captures WHERE signal_id = $1");
    let row = sqlx::query_as::<_, PostgresCapturesRow>(&query)
        .bind(signal_id)
        .fetch_optional(pool)
        .await
        .map_err(repository_sqlx_error)?;
    row.map(decode_postgres_captures).transpose()
}

async fn captures_sqlite(
    pool: &SqlitePool,
    signal_id: Uuid,
) -> Result<Option<SignalCapturesRecord>, RepositoryError> {
    let query = format!("SELECT {CAPTURES_COLUMNS} FROM signal_captures WHERE signal_id = ?1");
    let row = sqlx::query_as::<_, SqliteCapturesRow>(&query)
        .bind(encode_uuid(signal_id))
        .fetch_optional(pool)
        .await
        .map_err(repository_sqlx_error)?;
    row.map(decode_sqlite_captures).transpose()
}

fn decode_postgres_captures(
    row: PostgresCapturesRow,
) -> Result<SignalCapturesRecord, RepositoryError> {
    validate_captures(&row.3)?;
    Ok(SignalCapturesRecord {
        signal_id: row.0,
        workflow_id: row.1,
        workflow_version: row.2,
        captures: row.3,
        created_at: row.4,
        materialized_run_id: row.5,
        terminal_at: row.6,
    })
}

fn decode_sqlite_captures(row: SqliteCapturesRow) -> Result<SignalCapturesRecord, RepositoryError> {
    let captures = decode_json(&row.3).map_err(corrupt_value)?;
    validate_captures(&captures)?;
    Ok(SignalCapturesRecord {
        signal_id: decode_uuid(&row.0).map_err(corrupt_value)?,
        workflow_id: decode_uuid(&row.1).map_err(corrupt_value)?,
        workflow_version: row.2,
        captures,
        created_at: decode_timestamp(row.4).map_err(corrupt_value)?,
        materialized_run_id: row
            .5
            .map(|value| decode_uuid(&value).map_err(corrupt_value))
            .transpose()?,
        terminal_at: row
            .6
            .map(|value| decode_timestamp(value).map_err(corrupt_value))
            .transpose()?,
    })
}

async fn link_captures_postgres(
    pool: &PgPool,
    signal_id: Uuid,
    workflow_instance_id: Uuid,
) -> Result<SignalLinkageOutcome, RepositoryError> {
    let linked = sqlx::query_scalar::<_, Uuid>(
        "UPDATE signal_captures SET materialized_run_id = $2 WHERE signal_id = $1 AND materialized_run_id IS NULL RETURNING materialized_run_id",
    )
    .bind(signal_id)
    .bind(workflow_instance_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    if linked.is_some() {
        return Ok(SignalLinkageOutcome::Linked);
    }
    let existing = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT materialized_run_id FROM signal_captures WHERE signal_id = $1",
    )
    .bind(signal_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(match existing.flatten() {
        Some(workflow_instance_id) => SignalLinkageOutcome::AlreadyLinked {
            workflow_instance_id,
        },
        None => SignalLinkageOutcome::Absent,
    })
}

async fn link_captures_sqlite(
    pool: &SqlitePool,
    signal_id: Uuid,
    workflow_instance_id: Uuid,
) -> Result<SignalLinkageOutcome, RepositoryError> {
    let mut transaction = pool.begin().await.map_err(repository_sqlx_error)?;
    let result = sqlx::query(
        "UPDATE signal_captures SET materialized_run_id = ?2 WHERE signal_id = ?1 AND materialized_run_id IS NULL",
    )
    .bind(encode_uuid(signal_id))
    .bind(encode_uuid(workflow_instance_id))
    .execute(&mut *transaction)
    .await
    .map_err(repository_sqlx_error)?;
    let outcome = if result.rows_affected() == 1 {
        SignalLinkageOutcome::Linked
    } else {
        let existing = sqlx::query_scalar::<_, Option<String>>(
            "SELECT materialized_run_id FROM signal_captures WHERE signal_id = ?1",
        )
        .bind(encode_uuid(signal_id))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(repository_sqlx_error)?;
        match existing.flatten() {
            Some(value) => SignalLinkageOutcome::AlreadyLinked {
                workflow_instance_id: decode_uuid(&value).map_err(corrupt_value)?,
            },
            None => SignalLinkageOutcome::Absent,
        }
    };
    transaction.commit().await.map_err(repository_sqlx_error)?;
    Ok(outcome)
}

async fn mark_terminal_postgres(
    pool: &PgPool,
    workflow_instance_id: Uuid,
) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
    let query = format!(
        "UPDATE signal_captures SET terminal_at = now() WHERE materialized_run_id = $1 AND terminal_at IS NULL RETURNING {CAPTURES_COLUMNS}"
    );
    let rows = sqlx::query_as::<_, PostgresCapturesRow>(&query)
        .bind(workflow_instance_id)
        .fetch_all(pool)
        .await
        .map_err(repository_sqlx_error)?;
    decode_and_sort_postgres(rows)
}

async fn mark_terminal_sqlite(
    pool: &SqlitePool,
    workflow_instance_id: Uuid,
) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
    let query = format!(
        "UPDATE signal_captures SET terminal_at = ?2 WHERE materialized_run_id = ?1 AND terminal_at IS NULL RETURNING {CAPTURES_COLUMNS}"
    );
    let rows = sqlx::query_as::<_, SqliteCapturesRow>(&query)
        .bind(encode_uuid(workflow_instance_id))
        .bind(encode_timestamp(Utc::now()))
        .fetch_all(pool)
        .await
        .map_err(repository_sqlx_error)?;
    decode_and_sort_sqlite(rows)
}

async fn expired_captures_postgres(
    pool: &PgPool,
    cutoff: DateTime<Utc>,
) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
    let query = format!(
        "SELECT {CAPTURES_COLUMNS} FROM signal_captures WHERE terminal_at IS NOT NULL AND terminal_at < $1 ORDER BY signal_id"
    );
    let rows = sqlx::query_as::<_, PostgresCapturesRow>(&query)
        .bind(cutoff)
        .fetch_all(pool)
        .await
        .map_err(repository_sqlx_error)?;
    decode_and_sort_postgres(rows)
}

async fn expired_captures_sqlite(
    pool: &SqlitePool,
    cutoff: DateTime<Utc>,
) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
    let query = format!(
        "SELECT {CAPTURES_COLUMNS} FROM signal_captures WHERE terminal_at IS NOT NULL AND terminal_at < ?1 ORDER BY signal_id"
    );
    let rows = sqlx::query_as::<_, SqliteCapturesRow>(&query)
        .bind(encode_timestamp(cutoff))
        .fetch_all(pool)
        .await
        .map_err(repository_sqlx_error)?;
    decode_and_sort_sqlite(rows)
}

fn decode_and_sort_postgres(
    rows: Vec<PostgresCapturesRow>,
) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
    let mut records = rows
        .into_iter()
        .map(decode_postgres_captures)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_unstable_by_key(|record| record.signal_id);
    Ok(records)
}

fn decode_and_sort_sqlite(
    rows: Vec<SqliteCapturesRow>,
) -> Result<Vec<SignalCapturesRecord>, RepositoryError> {
    let mut records = rows
        .into_iter()
        .map(decode_sqlite_captures)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_unstable_by_key(|record| record.signal_id);
    Ok(records)
}

async fn wakeup_postgres(
    pool: &PgPool,
    signal_id: Uuid,
) -> Result<Option<SignalWakeupRecord>, RepositoryError> {
    let row: Option<(Uuid, String, i32, DateTime<Utc>)> = sqlx::query_as(
        "SELECT signal_id, name, matched_workflows, created_at FROM signal_wakeups WHERE signal_id = $1",
    )
    .bind(signal_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(row.map(|row| SignalWakeupRecord {
        signal_id: row.0,
        name: row.1,
        matched_workflows: row.2,
        created_at: row.3,
    }))
}

async fn wakeup_sqlite(
    pool: &SqlitePool,
    signal_id: Uuid,
) -> Result<Option<SignalWakeupRecord>, RepositoryError> {
    let row: Option<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT signal_id, name, matched_workflows, created_at FROM signal_wakeups WHERE signal_id = ?1",
    )
    .bind(encode_uuid(signal_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|row| {
        Ok(SignalWakeupRecord {
            signal_id: decode_uuid(&row.0).map_err(corrupt_value)?,
            name: row.1,
            matched_workflows: i32::try_from(row.2).map_err(corrupt_value)?,
            created_at: decode_timestamp(row.3).map_err(corrupt_value)?,
        })
    })
    .transpose()
}

async fn cancel_postgres(
    pool: &PgPool,
    signal_id: Uuid,
) -> Result<Option<SignalCancelRecord>, RepositoryError> {
    let row: Option<(Uuid, i32, Value, Option<String>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT signal_id, applied_count, target, note, created_at FROM signal_cancels WHERE signal_id = $1",
    )
    .bind(signal_id)
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    Ok(row.map(|row| SignalCancelRecord {
        signal_id: row.0,
        applied_count: row.1,
        target: row.2,
        note: row.3,
        created_at: row.4,
    }))
}

async fn cancel_sqlite(
    pool: &SqlitePool,
    signal_id: Uuid,
) -> Result<Option<SignalCancelRecord>, RepositoryError> {
    let row: Option<(String, i64, String, Option<String>, i64)> = sqlx::query_as(
        "SELECT signal_id, applied_count, target, note, created_at FROM signal_cancels WHERE signal_id = ?1",
    )
    .bind(encode_uuid(signal_id))
    .fetch_optional(pool)
    .await
    .map_err(repository_sqlx_error)?;
    row.map(|row| {
        Ok(SignalCancelRecord {
            signal_id: decode_uuid(&row.0).map_err(corrupt_value)?,
            applied_count: i32::try_from(row.1).map_err(corrupt_value)?,
            target: decode_json(&row.2).map_err(corrupt_value)?,
            note: row.3,
            created_at: decode_timestamp(row.4).map_err(corrupt_value)?,
        })
    })
    .transpose()
}

fn validate_captures(captures: &Value) -> Result<(), RepositoryError> {
    let Some(captures) = captures.as_array() else {
        return Err(corrupt_value("captures must be a JSON array"));
    };
    for capture in captures {
        let Some(capture) = capture.as_object() else {
            return Err(corrupt_value("each capture must be a JSON object"));
        };
        if !capture.get("name").is_some_and(Value::is_string) {
            return Err(corrupt_value("each capture must contain a string name"));
        }
        if !capture.get("envelope").is_some_and(Value::is_object) {
            return Err(corrupt_value(
                "each capture must contain an object envelope",
            ));
        }
    }
    Ok(())
}

fn corrupt_value(source: impl std::fmt::Display) -> RepositoryError {
    RepositoryError::new(
        RepositoryErrorKind::CorruptStoredValue,
        CorruptSignal(source.to_string()),
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
    use tickr_proto::config::DataPlaneSql;

    use super::*;
    use crate::backend::RepositoryFactory;
    use crate::{apply_sqlite, apply_target, sqlite_writer_options, MigrationTarget};

    fn captures(signal_id: Uuid, workflow_id: Uuid) -> SignalCapturesInput {
        SignalCapturesInput {
            signal_id,
            workflow_id,
            workflow_version: Some(7),
            captures: serde_json::json!([{
                "name": "order",
                "envelope": {
                    "present": true,
                    "value": {"id": 42},
                    "producer": {
                        "kind": "Signal",
                        "signal_id": signal_id,
                        "source": {"Manual": {}}
                    },
                    "lineage": [{"segment": "inputs.order"}]
                }
            }]),
        }
    }

    async fn corrupt_capture_shape(writer: &WriterRepositoryBundle, signal_id: Uuid) {
        match &writer.pool {
            BackendPool::Postgres(pool) => {
                sqlx::query(
                    "UPDATE signal_captures SET captures = '{}'::jsonb WHERE signal_id = $1",
                )
                .bind(signal_id)
                .execute(pool)
                .await
                .unwrap();
            }
            BackendPool::Sqlite(pool) => {
                sqlx::query("UPDATE signal_captures SET captures = '{}' WHERE signal_id = ?1")
                    .bind(encode_uuid(signal_id))
                    .execute(pool)
                    .await
                    .unwrap();
            }
        }
    }

    async fn run_laws(selection: DataPlaneSql) {
        let factory = RepositoryFactory::new(selection);
        let writer = factory.open_writer().await.unwrap();
        let reader = factory.open_read_only().await.unwrap();
        let signal_id = Uuid::from_u128(100);
        let workflow_id = Uuid::from_u128(200);
        let workflow_instance_id = Uuid::from_u128(300);
        let cancel_id = Uuid::from_u128(400);

        assert!(writer.signal_captures(signal_id).await.unwrap().is_none());
        assert!(reader.signal_audit(signal_id).await.unwrap().is_none());
        assert_eq!(
            writer
                .link_signal_captures(Uuid::from_u128(999), workflow_instance_id)
                .await
                .unwrap(),
            SignalLinkageOutcome::Absent
        );

        let input = captures(signal_id, workflow_id);
        assert!(writer.insert_signal_captures(&input).await.unwrap());
        let mut conflicting = input.clone();
        conflicting.workflow_id = Uuid::from_u128(201);
        conflicting.workflow_version = Some(8);
        conflicting.captures = Value::Array(Vec::new());
        assert!(!writer.insert_signal_captures(&conflicting).await.unwrap());

        let stored = writer.signal_captures(signal_id).await.unwrap().unwrap();
        assert_eq!(stored.workflow_id, workflow_id);
        assert_eq!(stored.workflow_version, Some(7));
        assert_eq!(stored.captures, input.captures);
        assert_eq!(stored.capture_names(), vec!["order"]);

        assert_eq!(
            writer
                .link_signal_captures(signal_id, workflow_instance_id)
                .await
                .unwrap(),
            SignalLinkageOutcome::Linked
        );
        assert_eq!(
            writer
                .link_signal_captures(signal_id, workflow_instance_id)
                .await
                .unwrap(),
            SignalLinkageOutcome::AlreadyLinked {
                workflow_instance_id
            }
        );
        assert_eq!(
            writer
                .link_signal_captures(signal_id, Uuid::from_u128(301))
                .await
                .unwrap(),
            SignalLinkageOutcome::AlreadyLinked {
                workflow_instance_id
            }
        );

        let cancel = SignalCancelInput {
            signal_id: cancel_id,
            applied_count: 2,
            target: serde_json::json!({"kind": "instance", "id": workflow_instance_id}),
            note: Some("operator stop".to_owned()),
        };
        assert!(writer.insert_signal_cancel(&cancel).await.unwrap());
        let mut duplicate_cancel = cancel.clone();
        duplicate_cancel.applied_count = 99;
        assert!(!writer
            .insert_signal_cancel(&duplicate_cancel)
            .await
            .unwrap());

        let wakeup = SignalWakeupInput {
            signal_id,
            name: "order-paid".to_owned(),
            matched_workflows: 3,
        };
        assert!(writer.insert_signal_wakeup(&wakeup).await.unwrap());
        let mut duplicate_wakeup = wakeup.clone();
        duplicate_wakeup.name = "changed".to_owned();
        assert!(!writer
            .insert_signal_wakeup(&duplicate_wakeup)
            .await
            .unwrap());

        let SignalAuditRecord::Wakeup(wakeup_read) =
            reader.signal_audit(signal_id).await.unwrap().unwrap()
        else {
            panic!("Wakeup must retain precedence over its derived captures");
        };
        assert_eq!(wakeup_read.name, "order-paid");
        assert_eq!(wakeup_read.matched_workflows, 3);

        let SignalAuditRecord::Cancel(cancel_read) =
            reader.signal_audit(cancel_id).await.unwrap().unwrap()
        else {
            panic!("Cancel audit must round-trip");
        };
        assert_eq!(cancel_read.applied_count, 2);
        assert_eq!(cancel_read.target, cancel.target);
        assert_eq!(cancel_read.note, cancel.note);

        reader.close().await;
        writer.close().await;
        let writer = factory.open_writer().await.unwrap();
        let reader = factory.open_read_only().await.unwrap();
        let reopened = writer.signal_captures(signal_id).await.unwrap().unwrap();
        assert_eq!(reopened.materialized_run_id, Some(workflow_instance_id));
        assert_eq!(reopened.captures, input.captures);
        assert!(matches!(
            reader.signal_audit(cancel_id).await.unwrap(),
            Some(SignalAuditRecord::Cancel(_))
        ));

        let terminal = writer
            .mark_signal_captures_terminal(workflow_instance_id)
            .await
            .unwrap();
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].signal_id, signal_id);
        let terminal_at = terminal[0].terminal_at.expect("terminal timestamp");
        assert!(writer
            .mark_signal_captures_terminal(workflow_instance_id)
            .await
            .unwrap()
            .is_empty());
        reader.close().await;
        writer.close().await;
        let writer = factory.open_writer().await.unwrap();
        let reader = factory.open_read_only().await.unwrap();
        let settled_after_restart = writer.signal_captures(signal_id).await.unwrap().unwrap();
        assert_eq!(settled_after_restart.terminal_at, Some(terminal_at));
        assert!(!writer.insert_signal_captures(&input).await.unwrap());
        assert!(writer
            .mark_signal_captures_terminal(workflow_instance_id)
            .await
            .unwrap()
            .is_empty());
        assert!(writer
            .expired_signal_captures(terminal_at)
            .await
            .unwrap()
            .is_empty());
        let after_terminal = terminal_at + chrono::Duration::microseconds(1);
        assert_eq!(
            writer
                .expired_signal_captures(after_terminal)
                .await
                .unwrap()
                .iter()
                .map(|record| record.signal_id)
                .collect::<Vec<_>>(),
            vec![signal_id]
        );

        let live_id = Uuid::from_u128(500);
        assert!(writer
            .insert_signal_captures(&captures(live_id, workflow_id))
            .await
            .unwrap());
        assert!(!writer
            .delete_expired_signal_captures(live_id, Utc::now() + chrono::Duration::days(365))
            .await
            .unwrap());
        assert!(writer
            .delete_expired_signal_captures(signal_id, after_terminal)
            .await
            .unwrap());
        assert!(!writer
            .delete_expired_signal_captures(signal_id, after_terminal)
            .await
            .unwrap());
        assert!(writer.signal_captures(signal_id).await.unwrap().is_none());

        corrupt_capture_shape(&writer, live_id).await;
        let error = writer.signal_captures(live_id).await.unwrap_err();
        assert_eq!(error.kind(), RepositoryErrorKind::CorruptStoredValue);

        reader.close().await;
        writer.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_signal_repository_laws() {
        let container = match Postgres::default()
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .start()
            .await
        {
            Ok(container) => container,
            Err(error) => {
                eprintln!("skipping: testcontainers Postgres unavailable: {error}");
                return;
            }
        };
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
        let migration_pool = PgPoolOptions::new().connect(&url).await.unwrap();
        apply_target(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;
        run_laws(DataPlaneSql::Postgres { url }).await;
    }

    fn sqlite_url(path: &Path) -> String {
        format!("sqlite://{}", path.display())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_backed_sqlite_signal_repository_laws() {
        let directory = TempDir::new().unwrap();
        let url = sqlite_url(&directory.path().join("signals.db"));
        let migration_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;
        run_laws(DataPlaneSql::Sqlite { url }).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_signal_write_contention_is_bounded_and_classified() {
        let directory = TempDir::new().unwrap();
        let url = sqlite_url(&directory.path().join("contention.db"));
        let migration_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;

        let writer = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() })
            .open_writer()
            .await
            .unwrap();

        let connection = match &writer.pool {
            BackendPool::Sqlite(pool) => pool.acquire().await.unwrap(),
            BackendPool::Postgres(_) => unreachable!(),
        };
        let error = writer
            .insert_signal_wakeup(&SignalWakeupInput {
                signal_id: Uuid::new_v4(),
                name: "blocked".to_owned(),
                matched_workflows: 0,
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), RepositoryErrorKind::ContentionTimeout);
        drop(connection);
        writer.close().await;
    }
}
