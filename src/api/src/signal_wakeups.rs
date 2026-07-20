//! Read projection of the conductor's `signal_wakeups` archive. The API's
//! `GET /api/signals/{id}` endpoint reads this table for wakeup signals in
//! parallel to `signal_captures` (triggers) and `signal_cancels` (cancels).
//!
//! Read-only: the conductor owns every write to this table. The API is a
//! second reader against the same schema.

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SignalWakeupRow {
    pub signal_id: Uuid,
    pub name: String,
    pub matched_workflows: i32,
}

/// Read a wakeup-signal row by `signal_id`. Returns `None` if no row
/// exists — the audit endpoint then falls through to the next table in
/// the chain.
pub async fn read(pool: &PgPool, signal_id: Uuid) -> Result<Option<SignalWakeupRow>> {
    let row: Option<(Uuid, String, i32)> = sqlx::query_as(
        r#"
        SELECT signal_id, name, matched_workflows
        FROM signal_wakeups
        WHERE signal_id = $1
        "#,
    )
    .bind(signal_id)
    .fetch_optional(pool)
    .await
    .context("read signal_wakeups row")?;

    Ok(
        row.map(|(signal_id, name, matched_workflows)| SignalWakeupRow {
            signal_id,
            name,
            matched_workflows,
        }),
    )
}
