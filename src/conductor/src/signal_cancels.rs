//! Durable archive of cancel signals. One row per processed cancel; the
//! `GET /api/signals/{id}` endpoint reads this table for cancel signals in
//! parallel to `signal_captures` reads for trigger signals.

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SignalCancelRow {
    pub signal_id: Uuid,
    pub applied_count: i32,
    pub target: Value,
    pub note: Option<String>,
}

/// Insert a cancel-signal row. Idempotent on `signal_id`: a re-insert
/// with the same key is a no-op (the row was written by the original
/// emission; an idempotency-replay path never reaches this site).
pub async fn insert(pool: &PgPool, row: &SignalCancelRow) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO signal_cancels (signal_id, applied_count, target, note)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (signal_id) DO NOTHING
        "#,
    )
    .bind(row.signal_id)
    .bind(row.applied_count)
    .bind(&row.target)
    .bind(row.note.as_deref())
    .execute(pool)
    .await
    .context("insert signal_cancels row")?;
    Ok(())
}

/// Read a cancel-signal row by `signal_id`. Returns `None` if no row
/// exists — the audit endpoint then falls back to the trigger-side
/// `signal_captures` table.
pub async fn read(pool: &PgPool, signal_id: Uuid) -> Result<Option<SignalCancelRow>> {
    let row: Option<(Uuid, i32, Value, Option<String>)> = sqlx::query_as(
        r#"
        SELECT signal_id, applied_count, target, note
        FROM signal_cancels
        WHERE signal_id = $1
        "#,
    )
    .bind(signal_id)
    .fetch_optional(pool)
    .await
    .context("read signal_cancels row")?;

    Ok(
        row.map(|(signal_id, applied_count, target, note)| SignalCancelRow {
            signal_id,
            applied_count,
            target,
            note,
        }),
    )
}
