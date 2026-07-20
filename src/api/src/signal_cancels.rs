//! Read projection of the conductor's `signal_cancels` archive. The API's
//! `GET /api/signals/{id}` endpoint reads this table for cancel signals in
//! parallel to `signal_captures` reads for trigger signals.
//!
//! Read-only: the conductor owns every write to this table. The API is a
//! second reader against the same schema.

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
