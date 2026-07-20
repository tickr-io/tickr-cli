//! Read projection of the conductor's `signal_captures` archive — the
//! Postgres source of truth for trigger-derived captures. The API's
//! `GET /api/signals/{id}` endpoint reads this table for trigger signals.
//!
//! Read-only: the conductor owns the capture lifecycle (insert on trigger,
//! mark-materialized on instance creation, mark-terminal + sweep at run end).
//! The API is a second reader against the same schema.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tickr_ctx::envelope::Envelope;
use uuid::Uuid;

/// One archived signal-captures record. The `captures` column is a JSONB
/// array whose element shape matches a single tickr-ctx envelope so reads
/// rehydrate without a shape conversion step.
#[derive(Debug)]
pub struct SignalCapturesRow {
    pub signal_id: Uuid,
    pub workflow_id: Uuid,
    pub captures: Vec<NamedEnvelope>,
    pub created_at: DateTime<Utc>,
    pub materialized_run_id: Option<Uuid>,
    pub terminal_at: Option<DateTime<Utc>>,
}

/// Pair carried in the JSONB column so the rehydration path doesn't need a
/// separate join — the name lives next to its envelope.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NamedEnvelope {
    pub name: String,
    pub envelope: Envelope,
}

/// Read a row by signal_id. Returns `None` for an unknown id or a row that
/// has already been purged by the terminal-state sweep.
// The query-result tuple mirrors the table's column order verbatim; a named
// alias would obscure the 1:1 mapping to the SELECT list.
#[allow(clippy::type_complexity)]
pub async fn read(pool: &PgPool, signal_id: Uuid) -> Result<Option<SignalCapturesRow>> {
    let row: Option<(
        Uuid,
        Uuid,
        serde_json::Value,
        DateTime<Utc>,
        Option<Uuid>,
        Option<DateTime<Utc>>,
    )> = sqlx::query_as(
        r#"
            SELECT signal_id, workflow_id, captures, created_at, materialized_run_id, terminal_at
            FROM signal_captures
            WHERE signal_id = $1
            "#,
    )
    .bind(signal_id)
    .fetch_optional(pool)
    .await
    .context("read signal_captures row")?;

    Ok(row.map(
        |(signal_id, workflow_id, captures_json, created_at, materialized_run_id, terminal_at)| {
            let captures = serde_json::from_value(captures_json).unwrap_or_else(|_| Vec::new());
            SignalCapturesRow {
                signal_id,
                workflow_id,
                captures,
                created_at,
                materialized_run_id,
                terminal_at,
            }
        },
    ))
}
