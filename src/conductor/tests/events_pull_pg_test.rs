//! Postgres integration tests for the Event Pull cycle.
//!
//! The cycle derives its Archive cursor from the selected repository, performs
//! the control-plane fetch before insertion begins, and lets atomic idempotent
//! insertion absorb concurrent duplicate fetches.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tickr_conductor::system_tasks::{pull_once, EventsPullClient, EventsPullError, PullOutcome};
use tickr_migrations::backend::{ReadOnlyRepositoryBundle, WriterRepositoryBundle};
use tickr_migrations::event_repository::EventFilter;
use uuid::Uuid;

mod common;

const TEST_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TEST_AUTHORIZATION: &str = "Bearer AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

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
        event_type: event_type.to_owned(),
        payload: serde_json::json!({ event_type: {} }),
        archived_at,
    }
}

struct StubState {
    rows: Vec<StubEvent>,
    respect_cursor: bool,
    stall: AtomicBool,
    response_delay: Duration,
    requests: AtomicUsize,
    saw_cursor: AtomicBool,
    status: StatusCode,
    saw_authorization: AtomicBool,
}

#[derive(serde::Deserialize)]
struct StubQuery {
    after_archived_at: Option<DateTime<Utc>>,
    after_id: Option<Uuid>,
    #[allow(dead_code)]
    limit: Option<u32>,
}

async fn spawn_stub_coordinator(state: Arc<StubState>) -> String {
    let app = axum::Router::new().route(
        "/api/internal/events",
        axum::routing::get(
            move |headers: HeaderMap,
                  axum::extract::Query(query): axum::extract::Query<StubQuery>| {
                let state = Arc::clone(&state);
                async move {
                    state.requests.fetch_add(1, Ordering::SeqCst);
                    if headers
                        .get(AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        != Some(TEST_AUTHORIZATION)
                    {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    state.saw_authorization.store(true, Ordering::SeqCst);
                    if state.status != StatusCode::OK {
                        return state.status.into_response();
                    }
                    if state.stall.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                    tokio::time::sleep(state.response_delay).await;
                    let cursor = query.after_archived_at.zip(query.after_id);
                    if cursor.is_some() {
                        state.saw_cursor.store(true, Ordering::SeqCst);
                    }
                    let mut rows = state
                        .rows
                        .iter()
                        .filter(|row| {
                            !state.respect_cursor
                                || cursor.is_none_or(|cursor| (row.archived_at, row.id) > cursor)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    rows.sort_by_key(|row| (row.archived_at, row.id));
                    axum::Json(rows).into_response()
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind stub");
    let addr = listener.local_addr().expect("stub address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn conductor_pg() -> Option<(common::DbGuard, PgPool)> {
    common::test_db().await
}

fn events_client(url: &str) -> EventsPullClient {
    EventsPullClient::new(url, TEST_BEARER_TOKEN, true).unwrap()
}

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

fn repositories(pool: &PgPool) -> (WriterRepositoryBundle, ReadOnlyRepositoryBundle) {
    (
        WriterRepositoryBundle::from_postgres_pool(pool.clone()),
        ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone()),
    )
}

fn state(rows: Vec<StubEvent>, respect_cursor: bool) -> Arc<StubState> {
    Arc::new(StubState {
        rows,
        respect_cursor,
        stall: AtomicBool::new(false),
        response_delay: Duration::ZERO,
        requests: AtomicUsize::new(0),
        saw_cursor: AtomicBool::new(false),
        status: StatusCode::OK,
        saw_authorization: AtomicBool::new(false),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_lands_once_and_replayed_page_consumes_no_sequence() {
    let Some((_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = state(rows.clone(), false);
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = events_client(&url);
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();
    let (writer, reader) = repositories(&pool);

    assert_eq!(
        pull_once(&writer, &client, tenant).await.unwrap(),
        PullOutcome {
            fetched: rows.len(),
            inserted: rows.len() as u64,
        }
    );
    let first = reader.events(EventFilter::All, None, 500).await.unwrap();
    assert_eq!(first.len(), rows.len());
    assert!(first.windows(2).all(|pair| pair[0].seq > pair[1].seq));
    let highest = first[0].seq;

    assert_eq!(
        pull_once(&writer, &client, tenant).await.unwrap(),
        PullOutcome {
            fetched: rows.len(),
            inserted: 0,
        }
    );
    let replayed = reader.events(EventFilter::All, None, 500).await.unwrap();
    assert_eq!(replayed.len(), rows.len());
    assert_eq!(replayed[0].seq, highest);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_cycles_duplicate_fetch_but_commit_one_dense_projection() {
    let Some((_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = Arc::new(StubState {
        rows: rows.clone(),
        respect_cursor: true,
        stall: AtomicBool::new(false),
        response_delay: Duration::from_millis(150),
        requests: AtomicUsize::new(0),
        saw_cursor: AtomicBool::new(false),
        status: StatusCode::OK,
        saw_authorization: AtomicBool::new(false),
    });
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = events_client(&url);
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();
    let (writer, reader) = repositories(&pool);

    let (left, right) = tokio::join!(
        pull_once(&writer, &client, tenant),
        pull_once(&writer, &client, tenant)
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(state.requests.load(Ordering::SeqCst), 2);
    assert_eq!(left.fetched + right.fetched, rows.len() * 2);
    assert_eq!(left.inserted + right.inserted, rows.len() as u64);
    let projected = reader.events(EventFilter::All, None, 500).await.unwrap();
    assert_eq!(projected.len(), rows.len());
    assert_eq!(
        projected
            .iter()
            .rev()
            .map(|row| row.seq)
            .collect::<Vec<_>>(),
        (1..=rows.len() as i64).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_fetch_completes_before_insertion_waits_for_writer_lock() {
    let Some((_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = state(rows.clone(), true);
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();
    let (writer, reader) = repositories(&pool);
    let client = events_client(&url);

    let mut holder = pool.begin().await.expect("writer lock transaction");
    sqlx::query("LOCK TABLE events IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *holder)
        .await
        .expect("hold Event writer lock");

    let task = tokio::spawn({
        let writer = writer.clone();
        async move { pull_once(&writer, &client, tenant).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.requests.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fetch should occur while the writer lock is occupied");
    assert!(
        !task.is_finished(),
        "insertion must still wait for the writer lock"
    );
    holder.rollback().await.expect("release writer lock");

    assert_eq!(task.await.unwrap().unwrap().inserted, rows.len() as u64);
    assert_eq!(
        reader.event_count(EventFilter::All).await.unwrap(),
        rows.len() as i64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_is_derived_after_commit_and_retention_rebuild_keeps_seq_monotonic() {
    let Some((_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = state(rows.clone(), true);
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = events_client(&url);
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();
    let (writer, reader) = repositories(&pool);

    pull_once(&writer, &client, tenant).await.unwrap();
    assert!(!state.saw_cursor.load(Ordering::SeqCst));
    let old_highest = reader.events(EventFilter::All, None, 1).await.unwrap()[0].seq;

    pull_once(&writer, &client, tenant).await.unwrap();
    assert!(state.saw_cursor.load(Ordering::SeqCst));

    writer
        .delete_events_archived_before(Utc::now() + chrono::Duration::days(1))
        .await
        .unwrap();
    assert!(writer.event_archive_cursor().await.unwrap().is_none());
    state.saw_cursor.store(false, Ordering::SeqCst);
    pull_once(&writer, &client, tenant).await.unwrap();
    assert!(!state.saw_cursor.load(Ordering::SeqCst));
    let rebuilt = reader.events(EventFilter::All, None, 500).await.unwrap();
    assert_eq!(rebuilt.len(), rows.len());
    assert!(rebuilt.iter().all(|row| row.seq > old_highest));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_fetch_times_out_without_open_transaction_or_projection_change() {
    let Some((_guard, pool)) = conductor_pg().await else {
        return;
    };
    let rows = lifecycle_rows();
    let state = state(rows.clone(), true);
    state.stall.store(true, Ordering::SeqCst);
    let url = spawn_stub_coordinator(Arc::clone(&state)).await;
    let client = events_client(&url);
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();
    let (writer, reader) = repositories(&pool);

    let started = Instant::now();
    assert!(matches!(
        pull_once(&writer, &client, tenant).await,
        Err(EventsPullError::Timeout)
    ));
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(reader.event_count(EventFilter::All).await.unwrap(), 0);

    state.stall.store(false, Ordering::SeqCst);
    assert_eq!(
        pull_once(&writer, &client, tenant).await.unwrap().inserted,
        rows.len() as u64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authentication_failures_are_typed_and_leave_the_projection_unchanged() {
    let Some((_guard, pool)) = conductor_pg().await else {
        return;
    };
    let tenant = tickr_proto::TenantId::from_slug("acme").as_uuid();
    let (writer, reader) = repositories(&pool);

    for (status, expected) in [
        (StatusCode::UNAUTHORIZED, EventsPullError::Unauthenticated),
        (StatusCode::FORBIDDEN, EventsPullError::Forbidden),
    ] {
        let state = Arc::new(StubState {
            rows: lifecycle_rows(),
            respect_cursor: true,
            stall: AtomicBool::new(false),
            response_delay: Duration::ZERO,
            requests: AtomicUsize::new(0),
            saw_cursor: AtomicBool::new(false),
            status,
            saw_authorization: AtomicBool::new(false),
        });
        let url = spawn_stub_coordinator(Arc::clone(&state)).await;
        let client = events_client(&url);
        let error = pull_once(&writer, &client, tenant).await.unwrap_err();
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&expected)
        );
        assert!(state.saw_authorization.load(Ordering::SeqCst));
        assert_eq!(reader.event_count(EventFilter::All).await.unwrap(), 0);
        assert!(writer.event_archive_cursor().await.unwrap().is_none());
    }
}
