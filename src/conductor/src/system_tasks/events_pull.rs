//! Events pull cycle: lands tenant-visible server events in the tenant
//! events projection (the conductor-side `events` table).
//!
//! Every 5s tick, each conductor replica attempts a transaction-scoped
//! Postgres advisory lock; the winner derives its pull position from the
//! projection itself (the keyset high-water `(archived_at, id)` row — never
//! a stored cursor), fetches one batch through the coordinator's
//! `GET /api/internal/events` passthrough, and batch-inserts with
//! `ON CONFLICT (id) DO NOTHING`. Losers skip the tick without any wire
//! call.
//!
//! Correctness never depends on the lock: at-least-once delivery +
//! idempotent insert + the server-side stability watermark already
//! guarantee no gaps and no duplicate rows; the lock only deduplicates
//! *work* across replicas. An empty projection means "pull from the
//! beginning" — the rebuild path is the boot path, no seeding step.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Tick interval — one leg of the Event log page's staleness budget
/// (sweep 5s + watermark 2s + pull 5s + UI poll 5s ≈ 17s worst case).
pub const PULL_INTERVAL: Duration = Duration::from_secs(5);

/// Hard client budget for the in-transaction coordinator call, so a hung
/// coordinator holds the advisory lock for seconds, not forever.
const COORDINATOR_TIMEOUT: Duration = Duration::from_secs(3);

/// In-transaction idle backstop (`SET LOCAL`), armed before the coordinator
/// call: if this session's client vanishes mid-call (half-open TCP), the
/// server kills the idle transaction and the lock evaporates with it. No
/// application-level lock-breaking — transaction-scoped locks release on
/// any abort, including this one.
const IDLE_IN_TX_TIMEOUT: &str = "15s";

/// Advisory-lock key for the pull cycle ("tickrEvP" as i64). One lock per
/// data-plane Postgres: any number of replicas may tick, one pulls.
/// Public so tests can occupy the lock to exercise the loser path.
pub const EVENTS_PULL_LOCK_KEY: i64 = 0x7469_636b_7245_7650;

/// Batch cap requested per pull. The serve side caps at 1000; staying at
/// its default keeps one pull comfortably inside the lock window.
const PULL_BATCH_LIMIT: u32 = 500;

/// One served event row as the coordinator's JSON encodes it. Field names
/// match the coordinator's `EventResponse` / the archive's column names.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct PulledEvent {
    pub id: Uuid,
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub archived_at: DateTime<Utc>,
}

/// What one tick did — surfaced for logs and tests.
#[derive(Debug, PartialEq, Eq)]
pub enum PullOutcome {
    /// Another replica holds the lock this tick; no wire call was made.
    Skipped,
    /// The winner pulled a batch; `inserted` counts rows actually new to
    /// the projection (replayed rows collapse via `ON CONFLICT`).
    Pulled { fetched: usize, inserted: u64 },
}

/// Run the pull cycle until shutdown. Spawned once per conductor replica;
/// replica timers are unsynchronized, which may produce a denser-than-5s
/// effective cadence across the fleet — harmless (smaller batches).
pub async fn run_events_pull(
    pool: Arc<PgPool>,
    coordinator_url: String,
    tenant: Uuid,
    shutdown: CancellationToken,
) {
    let client = reqwest::Client::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                println!("events pull: shutdown signal received, stopping.");
                return;
            }
            _ = tokio::time::sleep(PULL_INTERVAL) => {
                match pull_once(&pool, &client, &coordinator_url, tenant).await {
                    Ok(PullOutcome::Skipped) => {}
                    Ok(PullOutcome::Pulled { fetched, inserted }) => {
                        if fetched > 0 {
                            tracing::debug!(
                                "events pull: fetched {} row(s), {} new",
                                fetched,
                                inserted
                            );
                        }
                    }
                    // Bounded staleness is the worst failure mode: an
                    // aborted cycle leaves no partial state (the
                    // transaction rolled back) and the next tick's winner
                    // resumes from committed data.
                    Err(e) => eprintln!("events pull cycle error: {}", e),
                }
            }
        }
    }
}

/// One pull cycle. Everything happens inside a single transaction: lock
/// attempt, cursor derivation, coordinator fetch, batch insert. Any failure
/// aborts the transaction, which releases the advisory lock and discards
/// the partial batch — the next winner re-derives the same cursor and
/// re-pulls.
pub async fn pull_once(
    pool: &PgPool,
    client: &reqwest::Client,
    coordinator_url: &str,
    tenant: Uuid,
) -> anyhow::Result<PullOutcome> {
    let mut tx = pool.begin().await?;

    // Efficiency gate only — correctness comes from idempotent insert +
    // the server-side watermark. Transaction-scoped: released on commit,
    // rollback, or crash; no lock-breaking logic anywhere.
    let won: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(EVENTS_PULL_LOCK_KEY)
        .fetch_one(&mut *tx)
        .await?;
    if !won {
        return Ok(PullOutcome::Skipped);
    }

    // Half-open-connection backstop for the wire call below; scoped to
    // this transaction.
    sqlx::query(&format!(
        "SET LOCAL idle_in_transaction_session_timeout = '{}'",
        IDLE_IN_TX_TIMEOUT
    ))
    .execute(&mut *tx)
    .await?;

    // The archive cursor: derived from the projection's own keyset
    // high-water row, never stored. No cursor row means no crash window
    // between insert and cursor advance, and an empty projection pulls
    // from the beginning — the rebuild path is the boot path.
    let cursor: Option<(DateTime<Utc>, Uuid)> = sqlx::query(
        "SELECT archived_at, id FROM events ORDER BY archived_at DESC, id DESC LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await?
    .map(|row| (row.get("archived_at"), row.get("id")));

    let batch = fetch_batch(client, coordinator_url, tenant, cursor).await?;
    let fetched = batch.len();

    // Insert in response order (keyset order), so `seq` increases with
    // `(archived_at, id)` within a batch. `seq` is commit-ordered only
    // because the advisory lock above serializes writers — remove the
    // lock and the UI cursor (not the pipeline) loses its ordering
    // guarantee.
    let mut inserted = 0u64;
    for event in &batch {
        let result = sqlx::query(
            r#"
            INSERT INTO events (id, ts, event_type, payload, archived_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(event.id)
        .bind(event.ts)
        .bind(&event.event_type)
        .bind(&event.payload)
        .bind(event.archived_at)
        .execute(&mut *tx)
        .await?;
        inserted += result.rows_affected();
    }

    tx.commit().await?;
    Ok(PullOutcome::Pulled { fetched, inserted })
}

/// `GET {coordinator_url}/api/internal/events` with the keyset cursor (absent
/// on first pull / after a rebuild). Non-2xx is an error, never an empty
/// batch — "no new events" and "serve path down" must stay distinguishable.
async fn fetch_batch(
    client: &reqwest::Client,
    coordinator_url: &str,
    tenant: Uuid,
    cursor: Option<(DateTime<Utc>, Uuid)>,
) -> anyhow::Result<Vec<PulledEvent>> {
    let url = format!(
        "{}/api/internal/events",
        coordinator_url.trim_end_matches('/')
    );
    // Scope the pull to this conductor's own tenant — the archive is a shared
    // multi-tenant table, so the projection must receive only its tenant's slice.
    let mut request = client.get(&url).timeout(COORDINATOR_TIMEOUT).query(&[
        ("tenant", tenant.to_string()),
        ("limit", PULL_BATCH_LIMIT.to_string()),
    ]);
    if let Some((archived_at, id)) = cursor {
        request = request.query(&[
            ("after_archived_at", archived_at.to_rfc3339()),
            ("after_id", id.to_string()),
        ]);
    }
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        // The response body crosses from the control plane and may contain
        // internal diagnostics or secrets; retain only the status for logs.
        anyhow::bail!("events serve path returned status {}", status);
    }
    Ok(response.json::<Vec<PulledEvent>>().await?)
}
