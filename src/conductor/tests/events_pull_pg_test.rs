//! Integration tests for the events pull cycle
//! (`system_tasks::events_pull::pull_once`) — the conductor side of the
//! tenant event pipeline.
//!
//! Spins up an ephemeral Postgres via `testcontainers-modules` (conductor
//! migrations applied) and a stub coordinator that mirrors the real
//! `/api/internal/events` keyset-serve semantics. Verifies:
//!   1. A workflow lifecycle's events land in the tenant events projection,
//!      and replaying the same batch (stale cursor / replayed response)
//!      inserts nothing — `seq` does not advance for already-present ids.
//!   2. With the advisory lock held elsewhere, a tick skips entirely — the
//!      loser makes no wire call.
//!   3. An aborted cycle (insert without commit) leaves no rows and no
//!      advanced cursor; the next pull lands the batch exactly once.
//!   4. An empty projection pulls from the beginning (no cursor sent), and
//!      truncating the projection re-pulls everything — the rebuild path is
//!      the boot path.
//!   5. A stalled coordinator aborts the cycle within the hard client timeout
//!      and releases the lock for the next tick.
//!
//! Requires Docker running (testcontainers). Skipped automatically when
//! Docker isn't available.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tickr_conductor::system_tasks::events_pull::EVENTS_PULL_LOCK_KEY;
use tickr_conductor::system_tasks::{pull_once, PullOutcome};
use uuid::Uuid;

mod common;

/// One archive row as the stub serves it — the coordinator's JSON shape.
#[derive(serde::Serialize, Clone)]
struct StubEvent {
    id: Uuid,
    ts: DateTime<Utc>,
    event_type: String,
    payload: serde_json::Value,
    archived_at: DateTime<Utc>,
}

fn stub_event(event_type: &str, archived_at: DateTime<Utc>) -> StubEvent {
    StubEvent {
        id: Uuid::new_v4(),
        ts: archived_at - chrono::Duration::seconds(3),
        event_type: event_type.to_string(),
        payload: serde_json::json!({ event_type: {} }),
        archived_at,
    }
}

/// Stub coordinator behavior knobs shared with the running stub.
struct StubState {
    rows: Vec<StubEvent>,
    /// When true, mirror the real serve semantics (strictly after the
    /// keyset cursor). When false, always serve the full row set — the
    /// stale-cursor / replayed-response shape the idempotency test needs.
    respect_cursor: bool,
    /// When true, never respond — the hung-coordinator shape.
    stall: AtomicBool,
    requests: AtomicUsize,
    /// Whether any request arrived carrying a cursor.
    saw_cursor: AtomicBool,
}

#[derive(serde::Deserialize)]
struct StubQuery {
    after_archived_at: Option<DateTime<Utc>>,
    after_id: Option<Uuid>,
    #[allow(dead_code)]
    limit: Option<u32>,
}

/// Stand up a stub HTTP server mirroring the coordinator's
/// `/api/internal/events` keyset-serve shape.
async fn spawn_stub_coordinator(state: Arc<StubState>) -> String {
    let app = axum::Router::new().route(
        "/api/internal/events",
        axum::routing::get(
            move |axum::extract::Query(q): axum::extract::Query<StubQuery>| {
                let state = Arc::clone(&state);
                async move {
                    state.requests.fetch_add(1, Ordering::SeqCst);
                    if state.stall.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                    let cursor = q.after_archived_at.zip(q.after_id);
                    if cursor.is_some() {
                        state.saw_cursor.store(true, Ordering::SeqCst);
                    }
                    let mut rows: Vec<StubEvent> = state
                        .rows
                        .iter()
                        .filter(|r| {
                            if !state.respect_cursor {
                                return true;
                            }
                            match cursor {
                                Some((ts, id)) => (r.archived_at, r.id) > (ts, id),
                                None => true,
                            }
                        })
                        .cloned()
                        .collect();
                    rows.sort_by_key(|r| (r.archived_at, r.id));
                    axum::Json(rows)
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind stub");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

/// Ephemeral conductor Postgres with migrations applied.
async fn conductor_pg() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

/// A workflow lifecycle's worth of tenant-visible events across two sweep
/// groups (same-`archived_at` rows emulate the sweep's single-transaction
/// stamp).
fn lifecycle_rows() -> Vec<StubEvent> {
    let sweep_1 = Utc::now() - chrono::Duration::seconds(60);
    let sweep_2 = Utc::now() - chrono::Duration::seconds(30);
    vec![
        stub_event("WorkflowInstanceCreated", sweep_1),
        stub_event("WorkflowTriggered", sweep_1),
        stub_event("TaskInstanceCreated", sweep_1),
        stub_event("TaskQueued", sweep_1),
        stub_event("TaskStarted", sweep_2),
        stub_event("TaskCompleted", sweep_2),
        stub_event("WorkflowCompleted", sweep_2),
    ]
}

async fn projection_count(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM events")
        .fetch_one(pool)
        .await
        .expect("count")
        .get(0)
}

async fn projection_max_seq(pool: &PgPool) -> Option<i64> {
    sqlx::query("SELECT max(seq) FROM events")
        .fetch_one(pool)
        .await
        .expect("max seq")
        .get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_lands_in_projection_and_replay_inserts_nothing() {
    let Some((_pg_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = Arc::new(StubState {
        rows: rows.clone(),
        respect_cursor: false, // every pull replays the full batch
        stall: AtomicBool::new(false),
        requests: AtomicUsize::new(0),
        saw_cursor: AtomicBool::new(false),
    });
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = reqwest::Client::new();
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();

    let outcome = pull_once(&pool, &client, &url, tenant)
        .await
        .expect("pull 1");
    assert_eq!(
        outcome,
        PullOutcome::Pulled {
            fetched: rows.len(),
            inserted: rows.len() as u64
        }
    );
    assert_eq!(projection_count(&pool).await, rows.len() as i64);
    let seq_after_first = projection_max_seq(&pool).await;

    // Replay: the stub ignores the cursor and serves the same batch again.
    // Nothing inserts; `seq` does not advance for already-present ids.
    let outcome = pull_once(&pool, &client, &url, tenant)
        .await
        .expect("pull 2");
    assert_eq!(
        outcome,
        PullOutcome::Pulled {
            fetched: rows.len(),
            inserted: 0
        }
    );
    assert_eq!(projection_count(&pool).await, rows.len() as i64);
    assert_eq!(projection_max_seq(&pool).await, seq_after_first);

    // Tenancy-isolation boundary: control-plane types were filtered
    // upstream by the serve query's allowlist — assert the projection
    // carries none anyway.
    let leaked: i64 = sqlx::query("SELECT count(*) FROM events WHERE event_type = ANY($1)")
        .bind(vec![
            "NodesJoined",
            "NodesLeft",
            "GrantOwnership",
            "OwnershipClaimed",
            "SendReplication",
            "ApplyReplication",
            "Shutdown",
            "ArmTimer",
            "RemoveTimer",
        ])
        .fetch_one(&pool)
        .await
        .expect("leak check")
        .get(0);
    assert_eq!(leaked, 0, "no cluster/timer event types in the projection");

    // `seq` follows keyset order within the batch (insert order is
    // response order).
    let ordered: Vec<(i64, DateTime<Utc>, Uuid)> =
        sqlx::query("SELECT seq, archived_at, id FROM events ORDER BY seq")
            .fetch_all(&pool)
            .await
            .expect("ordered rows")
            .into_iter()
            .map(|r| (r.get("seq"), r.get("archived_at"), r.get("id")))
            .collect();
    assert!(
        ordered
            .windows(2)
            .all(|w| (w[0].1, w[0].2) < (w[1].1, w[1].2)),
        "seq order must follow (archived_at, id) keyset order"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_loser_skips_the_tick_without_wire_work() {
    let Some((_pg_guard, pool)) = conductor_pg().await else {
        return;
    };
    let state = Arc::new(StubState {
        rows: lifecycle_rows(),
        respect_cursor: true,
        stall: AtomicBool::new(false),
        requests: AtomicUsize::new(0),
        saw_cursor: AtomicBool::new(false),
    });
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = reqwest::Client::new();
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();

    // Occupy the advisory lock from a second session, transaction-scoped.
    let mut holder = pool.begin().await.expect("holder tx");
    let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(EVENTS_PULL_LOCK_KEY)
        .fetch_one(&mut *holder)
        .await
        .expect("hold lock");
    assert!(held, "test setup: lock must be acquirable");

    let outcome = pull_once(&pool, &client, &url, tenant)
        .await
        .expect("losing pull");
    assert_eq!(outcome, PullOutcome::Skipped);
    assert_eq!(
        state.requests.load(Ordering::SeqCst),
        0,
        "the loser must make no wire call"
    );
    assert_eq!(projection_count(&pool).await, 0);

    // Lock evaporates with the holder's transaction; the next tick wins.
    drop(holder);
    let outcome = pull_once(&pool, &client, &url, tenant)
        .await
        .expect("winning pull");
    assert!(matches!(outcome, PullOutcome::Pulled { .. }));
    assert!(state.requests.load(Ordering::SeqCst) > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_cycle_leaves_nothing_and_next_pull_lands_batch_once() {
    let Some((_pg_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = Arc::new(StubState {
        rows: rows.clone(),
        respect_cursor: true,
        stall: AtomicBool::new(false),
        requests: AtomicUsize::new(0),
        saw_cursor: AtomicBool::new(false),
    });
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = reqwest::Client::new();
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();

    // Simulated crash mid-cycle: insert the whole batch in a transaction
    // that never commits — exactly the state a replica death after insert,
    // before commit, leaves behind.
    {
        let mut tx = pool.begin().await.expect("crash tx");
        for r in &rows {
            sqlx::query(
                "INSERT INTO events (id, ts, event_type, payload, archived_at)
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING",
            )
            .bind(r.id)
            .bind(r.ts)
            .bind(&r.event_type)
            .bind(&r.payload)
            .bind(r.archived_at)
            .execute(&mut *tx)
            .await
            .expect("insert in doomed tx");
        }
        // Dropped without commit — rollback.
    }
    assert_eq!(
        projection_count(&pool).await,
        0,
        "aborted cycle must leave no rows and no advanced cursor"
    );

    // Next tick's winner re-derives the cursor from committed data (none)
    // and lands the batch exactly once.
    let outcome = pull_once(&pool, &client, &url, tenant)
        .await
        .expect("resume pull");
    assert_eq!(
        outcome,
        PullOutcome::Pulled {
            fetched: rows.len(),
            inserted: rows.len() as u64
        }
    );
    assert_eq!(projection_count(&pool).await, rows.len() as i64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_projection_pulls_from_the_beginning_and_truncate_rebuilds() {
    let Some((_pg_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = Arc::new(StubState {
        rows: rows.clone(),
        respect_cursor: true,
        stall: AtomicBool::new(false),
        requests: AtomicUsize::new(0),
        saw_cursor: AtomicBool::new(false),
    });
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = reqwest::Client::new();
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();

    // Boot path: empty projection ⇒ no cursor on the wire.
    pull_once(&pool, &client, &url, tenant)
        .await
        .expect("boot pull");
    assert!(
        !state.saw_cursor.load(Ordering::SeqCst),
        "an empty projection must pull from the beginning (no cursor)"
    );
    assert_eq!(projection_count(&pool).await, rows.len() as i64);

    // Steady state: the next pull carries the derived keyset cursor.
    pull_once(&pool, &client, &url, tenant)
        .await
        .expect("steady pull");
    assert!(state.saw_cursor.load(Ordering::SeqCst));

    // Rebuild path IS the boot path: wipe the projection and the next
    // pull re-populates it in full, no seeding step.
    sqlx::query("TRUNCATE events")
        .execute(&pool)
        .await
        .expect("truncate");
    state.saw_cursor.store(false, Ordering::SeqCst);
    pull_once(&pool, &client, &url, tenant)
        .await
        .expect("rebuild pull");
    assert!(!state.saw_cursor.load(Ordering::SeqCst));
    assert_eq!(projection_count(&pool).await, rows.len() as i64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_coordinator_aborts_within_timeout_and_releases_the_lock() {
    let Some((_pg_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = Arc::new(StubState {
        rows: rows.clone(),
        respect_cursor: true,
        stall: AtomicBool::new(true),
        requests: AtomicUsize::new(0),
        saw_cursor: AtomicBool::new(false),
    });
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = reqwest::Client::new();
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();

    let started = Instant::now();
    let result = pull_once(&pool, &client, &url, tenant).await;
    assert!(
        result.is_err(),
        "a stalled coordinator must abort the cycle"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the hard client timeout must bound the stall (took {:?})",
        started.elapsed()
    );
    assert_eq!(projection_count(&pool).await, 0);

    // The aborted transaction released the lock; a healthy next tick wins
    // and lands the batch.
    state.stall.store(false, Ordering::SeqCst);
    let outcome = pull_once(&pool, &client, &url, tenant)
        .await
        .expect("healthy pull");
    assert_eq!(
        outcome,
        PullOutcome::Pulled {
            fetched: rows.len(),
            inserted: rows.len() as u64
        }
    );
}
