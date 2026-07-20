//! Integration tests for the Run calendar: the PG terminal-rollup query
//! (tz bucketing, year filter) directly, and the calendar HTTP handler
//! (404 / 400 / empty / degraded-live) against the real router.
//!
//! Requires Docker (+ NATS for the handler cases). Skipped when unavailable.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::sync::Arc;

use async_nats::Client as NatsClient;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_api::http::archive_queries::calendar_terminal_rollup;
use uuid::Uuid;

mod common;

/// Insert a terminal archived instance whose JSONB is a captured drain blob, so
/// the instance-list projection (used by the `?date` path) rehydrates it. `state`
/// is the rendered lifecycle token; `scheduled_at` buckets the calendar day.
async fn insert_full(pool: &sqlx::PgPool, wf: Uuid, state: &str, scheduled_at: &str) {
    common::insert_instance(
        pool,
        &common::instance_blob(Uuid::new_v4(), wf, state, Some(scheduled_at)),
    )
    .await;
}

async fn start_postgres() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    sqlx::PgPool,
)> {
    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: Postgres testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .ok()?;
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &pool)
        .await
        .ok()?;
    Some((container, pool))
}

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    NatsClient,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = Nats::default().with_cmd(&cmd).start().await.ok()?;
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let client = async_nats::connect(format!("nats://127.0.0.1:{}", port))
        .await
        .ok()?;
    Some((container, client))
}

async fn insert_terminal(pool: &sqlx::PgPool, wf: Uuid, state: &str, scheduled_at: &str) {
    sqlx::query(
        r#"
        INSERT INTO workflow_instances (id, workflow_id, name, state, scheduled_at, instance)
        VALUES ($1, $2, 'cal', $3, ($4)::timestamptz, '{}'::jsonb)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(wf)
    .bind(state)
    .bind(scheduled_at)
    .execute(pool)
    .await
    .expect("insert terminal instance");
}

async fn insert_workflow(pool: &sqlx::PgPool, wf: Uuid) {
    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
        VALUES ($1, 0, 'default', 'cal', 'cal', 'Ready', 'testhash', 'testcos', '{}'::jsonb, '')
        "#,
    )
    .bind(wf)
    .execute(pool)
    .await
    .expect("insert workflow");
}

async fn spawn_api(nats: NatsClient, pool: Arc<sqlx::PgPool>) -> String {
    let coordinator = Arc::new(tickr_api::http::coordinator_client::CoordinatorClient::new(
        "http://127.0.0.1:1".to_string(),
    ));
    let s3 = opendal::services::S3::default()
        .bucket("ignored")
        .endpoint("http://127.0.0.1:1")
        .access_key_id("x")
        .secret_access_key("x")
        .region("us-east-1");
    let minio = opendal::Operator::new(s3).expect("s3 stub").finish();
    let logs = Arc::new(tickr_api::http::logs_resolver::LogsResolver::new(
        minio,
        nats.clone(),
    ));
    let state = tickr_api::http::routes::build_app_state(Arc::new(nats), pool, coordinator, logs);
    let app = tickr_api::http::routes::build_router(state);
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollup_buckets_by_local_date_and_filters_year() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_postgres().await else {
        return Ok(());
    };
    let wf = Uuid::new_v4();
    // 2026-06-15T03:00:00Z is still 2026-06-14 (23:00) in America/New_York (UTC-4 summer).
    insert_terminal(&pool, wf, "Completed", "2026-06-15T03:00:00Z").await;
    insert_terminal(&pool, wf, "Failed", "2026-06-20T12:00:00Z").await;
    // A prior-year row must be excluded by the year filter.
    insert_terminal(&pool, wf, "Completed", "2025-06-15T12:00:00Z").await;

    // UTC bucketing: 03:00Z lands on 2026-06-15.
    let utc = calendar_terminal_rollup(&pool, wf, 2026, "UTC").await?;
    let utc_15 = utc
        .iter()
        .find(|d| d.date == "2026-06-15")
        .expect("utc day");
    assert_eq!((utc_15.completed, utc_15.failed), (1, 0));
    assert!(
        utc.iter().all(|d| d.date.starts_with("2026")),
        "year filter holds"
    );

    // NY bucketing: the same instant lands on 2026-06-14.
    let ny = calendar_terminal_rollup(&pool, wf, 2026, "America/New_York").await?;
    assert!(
        ny.iter().any(|d| d.date == "2026-06-14"),
        "03:00Z buckets to prior NY day"
    );
    assert!(
        ny.iter().all(|d| d.date != "2026-06-15"),
        "not on the UTC day under NY tz"
    );
    let failed_day = ny
        .iter()
        .find(|d| d.date == "2026-06-20")
        .expect("failed day");
    assert_eq!((failed_day.completed, failed_day.failed), (0, 1));

    // Empty year.
    let empty = calendar_terminal_rollup(&pool, wf, 2030, "UTC").await?;
    assert!(empty.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn instances_date_filter_scopes_to_local_day() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_postgres().await else {
        return Ok(());
    };
    let Some((_nats, nats)) = start_nats().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);

    let wf = Uuid::new_v4();
    insert_workflow(&pool, wf).await;
    insert_full(&pool, wf, "Completed", "2026-06-15T12:00:00Z").await;
    insert_full(&pool, wf, "Failed", "2026-06-16T12:00:00Z").await;

    let base = spawn_api(nats, Arc::clone(&pool)).await;
    let client = reqwest::Client::new();

    // Unfiltered: both runs.
    let all: serde_json::Value = client
        .get(format!("{}/api/workflows/{}/instances", base, wf))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(all.as_array().unwrap().len(), 2);

    // Filtered to the 15th (UTC): just that run.
    let filtered: serde_json::Value = client
        .get(format!(
            "{}/api/workflows/{}/instances?date=2026-06-15&tz=UTC",
            base, wf
        ))
        .send()
        .await?
        .json()
        .await?;
    let rows = filtered.as_array().unwrap();
    assert_eq!(rows.len(), 1, "only the 2026-06-15 run");
    assert_eq!(rows[0]["state"], "Completed");

    // Invalid tz on the date filter → 400.
    let bad = client
        .get(format!(
            "{}/api/workflows/{}/instances?date=2026-06-15&tz=Not/AZone",
            base, wf
        ))
        .send()
        .await?;
    assert_eq!(bad.status(), 400);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calendar_handler_404_400_empty_and_degraded() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_postgres().await else {
        return Ok(());
    };
    let Some((_nats, nats)) = start_nats().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);

    let wf = Uuid::new_v4();
    insert_workflow(&pool, wf).await;
    insert_terminal(&pool, wf, "Completed", "2026-06-15T12:00:00Z").await;

    let empty_wf = Uuid::new_v4();
    insert_workflow(&pool, empty_wf).await;

    let base = spawn_api(nats, Arc::clone(&pool)).await;
    let client = reqwest::Client::new();

    // Unknown id → 404.
    let r = client
        .get(format!(
            "{}/api/workflows/{}/calendar?year=2026",
            base,
            Uuid::new_v4()
        ))
        .send()
        .await?;
    assert_eq!(r.status(), 404, "unknown id");

    // Invalid IANA name → 400.
    let r = client
        .get(format!(
            "{}/api/workflows/{}/calendar?year=2026&tz=Not/AZone",
            base, wf
        ))
        .send()
        .await?;
    assert_eq!(r.status(), 400, "invalid tz");

    // Known workflow, no instances → 200 with empty days.
    let r = client
        .get(format!(
            "{}/api/workflows/{}/calendar?year=2026",
            base, empty_wf
        ))
        .send()
        .await?;
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().await?;
    assert_eq!(body["days"].as_array().unwrap().len(), 0, "empty days");

    // Terminal data + unreachable coordinator → terminal counts only, header false.
    let r = client
        .get(format!(
            "{}/api/workflows/{}/calendar?year=2026&tz=UTC",
            base, wf
        ))
        .send()
        .await?;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("x-live-data-available").unwrap(), "false");
    let body: serde_json::Value = r.json().await?;
    let day = body["days"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["date"] == "2026-06-15")
        .expect("the seeded day");
    assert_eq!(day["completed"], 1);
    assert_eq!(day["scheduled"], 0, "no live scheduled under degraded read");
    Ok(())
}
