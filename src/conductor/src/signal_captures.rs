//! Postgres-backed durable archive for trigger-derived captures.
//!
//! The HTTP `/trigger` ingress extracts named captures from the inbound
//! payload, writes them here under the conductor-minted `signal_id`, and
//! mirrors the same envelopes to NATS KV as the working-set cache. Postgres
//! is the source of truth: a conductor restart between HTTP-receive and the
//! server's instance-creation event rehydrates the cache from these rows.
//! Run-bound lifecycle: terminal-state cleanup flips `terminal_at`, and a
//! grace-window sweep deletes the row outright.

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

/// Insert a fresh signal_captures row. Idempotent on `signal_id`: a re-insert
/// with the same id is a no-op (returns the existing row's row count of 0
/// without surfacing a duplicate-key error), matching the
/// idempotency-cache-replay path where a producer retries before the cache
/// entry is durable.
/// `workflow_version` is the live definition version the trigger extraction
/// resolved. Stamping it here makes a future version/Event-variable mismatch
/// visible in data instead of silently inferred from a wrong run. `None` for
/// ingress paths that resolve no live version (e.g. the wakeup path); the
/// column is nullable so those rows stay backward-compatible.
pub async fn insert(
    pool: &PgPool,
    signal_id: Uuid,
    workflow_id: Uuid,
    workflow_version: Option<i64>,
    captures: &[NamedEnvelope],
) -> Result<()> {
    let captures_json =
        serde_json::to_value(captures).context("serialize captures for signal_captures row")?;

    sqlx::query(
        r#"
        INSERT INTO signal_captures (signal_id, workflow_id, workflow_version, captures)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (signal_id) DO NOTHING
        "#,
    )
    .bind(signal_id)
    .bind(workflow_id)
    .bind(workflow_version)
    .bind(&captures_json)
    .execute(pool)
    .await
    .context("insert signal_captures row")?;

    Ok(())
}

/// Read a row by signal_id. Returns `None` for an unknown id or a row that
/// has already been purged by the terminal-state sweep.
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

/// Record the linkage between a signal_id and the workflow_instance_id the
/// server eventually materialized for it. Called from the conductor's
/// instance-creation event handler in a later slice; included here so the
/// repository surface is complete.
pub async fn mark_materialized(pool: &PgPool, signal_id: Uuid, run_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE signal_captures
        SET materialized_run_id = $2
        WHERE signal_id = $1 AND materialized_run_id IS NULL
        "#,
    )
    .bind(signal_id)
    .bind(run_id)
    .execute(pool)
    .await
    .context("mark signal_captures materialized")?;

    Ok(())
}

/// Mark the row terminal so the grace-window sweep can later delete it. The
/// hook is called from the terminal-state compaction path in a later slice;
/// shipped here so the repository's full lifecycle is testable.
pub async fn mark_terminal(pool: &PgPool, signal_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE signal_captures
        SET terminal_at = now()
        WHERE signal_id = $1 AND terminal_at IS NULL
        "#,
    )
    .bind(signal_id)
    .execute(pool)
    .await
    .context("mark signal_captures terminal")?;

    Ok(())
}

/// Delete the row outright. Used by the grace-window sweep after
/// `terminal_at` has aged out. Idempotent — a re-delete of a missing row is
/// not an error.
pub async fn delete(pool: &PgPool, signal_id: Uuid) -> Result<()> {
    sqlx::query("DELETE FROM signal_captures WHERE signal_id = $1")
        .bind(signal_id)
        .execute(pool)
        .await
        .context("delete signal_captures row")?;

    Ok(())
}

/// Find every `signal_captures` row linked to the given `workflow_instance_id`
/// that hasn't yet been marked terminal. The terminal-state cleanup hook
/// loads these so it knows which rows to flip and which NATS keys to delete.
pub async fn list_active_for_run(pool: &PgPool, run_id: Uuid) -> Result<Vec<SignalCapturesRow>> {
    let rows: Vec<(
        Uuid,
        Uuid,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        Option<Uuid>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        r#"
        SELECT signal_id, workflow_id, captures, created_at, materialized_run_id, terminal_at
        FROM signal_captures
        WHERE materialized_run_id = $1 AND terminal_at IS NULL
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await
    .context("list active signal_captures rows for run")?;

    Ok(rows
        .into_iter()
        .map(
            |(
                signal_id,
                workflow_id,
                captures_json,
                created_at,
                materialized_run_id,
                terminal_at,
            )| {
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
        )
        .collect())
}

/// Find signal_ids whose grace window has elapsed. The grace-window sweep
/// takes the list and issues per-row deletes; it doesn't need envelope
/// contents for the delete path so we only project the id.
pub async fn list_expired_for_sweep(pool: &PgPool, grace: chrono::Duration) -> Result<Vec<Uuid>> {
    let cutoff = chrono::Utc::now() - grace;
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        r#"
        SELECT signal_id
        FROM signal_captures
        WHERE terminal_at IS NOT NULL AND terminal_at < $1
        "#,
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .context("list expired signal_captures rows for sweep")?;

    Ok(rows.into_iter().map(|(id,)| id).collect())
}
