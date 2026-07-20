//! Durable archive of wakeup signals. One row per processed wakeup;
//! the `GET /api/signals/{id}` endpoint reads this table for wakeup
//! signals in parallel to `signal_captures` (triggers) and
//! `signal_cancels` (cancels).

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct SignalWakeupRow {
    pub signal_id: Uuid,
    pub name: String,
    pub matched_workflows: i32,
}

/// Insert a wakeup-signal row. Idempotent on `signal_id`: a re-insert
/// with the same key is a no-op. The wakeup translator writes one row
/// per ingress; an idempotency-replay path never reaches this site
/// because the cache short-circuits before any side effect runs.
pub async fn insert(pool: &PgPool, row: &SignalWakeupRow) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO signal_wakeups (signal_id, name, matched_workflows)
        VALUES ($1, $2, $3)
        ON CONFLICT (signal_id) DO NOTHING
        "#,
    )
    .bind(row.signal_id)
    .bind(&row.name)
    .bind(row.matched_workflows)
    .execute(pool)
    .await
    .context("insert signal_wakeups row")?;
    Ok(())
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
