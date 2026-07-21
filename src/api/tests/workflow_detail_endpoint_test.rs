//! Integration test for `GET /api/workflows/{id}?version=X` — the Workflow
//! detail endpoint. Stands up the real API router (`build_router`) against an
//! ephemeral Postgres (conductor migrations) with the coordinator pointed at an
//! unreachable address, so live reads degrade to archive-only and the response
//! reflects the PG composition under test.
//!
//! Asserts: default-version selection, explicit `?version` selection, opaque
//! pass-through of `workflow_definition`, newest-first `available_versions`
//! ordering, populated workflow-aggregate `latest_run_state` / `completed_runs`,
//! 404 on unknown id, 404 on unknown version, 400 on a malformed id.
//!
//! Requires Docker + NATS testcontainer. Skipped automatically when
//! unavailable.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::sync::Arc;

use async_nats::Client as NatsClient;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    NatsClient,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default().with_cmd(&cmd).start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let client = async_nats::connect(format!("nats://127.0.0.1:{}", port))
        .await
        .ok()?;
    Some((container, client))
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

/// Build the API router with the coordinator unreachable (live reads degrade) and
/// stubbed log stores, then serve it on an ephemeral port. Returns the base URL.
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
    let state = tickr_api::http::routes::build_app_state(
        Arc::new(nats),
        Arc::new(
            tickr_migrations::backend::ReadOnlyRepositoryBundle::from_postgres_pool(
                pool.as_ref().clone(),
            ),
        ),
        coordinator,
        logs,
    );
    let app = tickr_api::http::routes::build_router(state);
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind ephemeral");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

async fn insert_version(
    pool: &sqlx::PgPool,
    id: Uuid,
    version: &str,
    status: &str,
    inserted_at: &str,
    nickel_source: &str,
) {
    let definition = serde_json::json!({ "name": "detail-wf", "marker": "opaque-ok" });
    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source, inserted_at)
        VALUES ($1, ($2)::bigint, 'default', 'detail-wf', 'detail-wf', $3, 'testhash', 'testcos', $4, $5, ($6)::timestamptz)
        "#,
    )
    .bind(id)
    .bind(version)
    .bind(status)
    .bind(&definition)
    .bind(nickel_source)
    .bind(inserted_at)
    .execute(pool)
    .await
    .expect("insert workflow version");
}

async fn insert_terminal_instance(pool: &sqlx::PgPool, workflow_id: Uuid, state: &str) {
    sqlx::query(
        r#"
        INSERT INTO workflow_instances
            (id, workflow_id, name, state, scheduled_at, instance)
        VALUES ($1, $2, 'detail-wf', $3, now(), '{}'::jsonb)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .bind(state)
    .execute(pool)
    .await
    .expect("insert terminal instance");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detail_endpoint_composes_header_and_versions() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_pg, pool)) = start_postgres().await else {
        return Ok(());
    };
    let Some((_nats, nats)) = start_nats().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);

    let wf = Uuid::new_v4();
    // Three versions: an older Ready, a newer Submitted, the newest Building.
    // Default version resolves to the latest *live* one — the Submitted v2.
    insert_version(&pool, wf, "1", "Ready", "2026-01-01T00:00:00Z", "src-v1").await;
    insert_version(
        &pool,
        wf,
        "2",
        "Submitted",
        "2026-01-02T00:00:00Z",
        "src-v2",
    )
    .await;
    insert_version(&pool, wf, "3", "Building", "2026-01-03T00:00:00Z", "src-v3").await;
    // One terminal run so the workflow-aggregate scalars are populated.
    insert_terminal_instance(&pool, wf, "Completed").await;

    let base = spawn_api(nats, Arc::clone(&pool)).await;
    let client = reqwest::Client::new();

    // 1. Default landing (no ?version): default version = latest live (Submitted
    //    v2); available_versions newest-first; aggregates populated; the
    //    opaque definition blob passes through untouched.
    let resp = client
        .get(format!("{}/api/workflows/{}", base, wf))
        .send()
        .await?;
    assert_eq!(resp.status(), 200, "default landing is 200");
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["workflow_id"], wf.to_string());
    assert_eq!(
        body["version"].as_i64(),
        Some(2),
        "default = latest live (Submitted)"
    );
    assert_eq!(
        body["nickel_source"], "src-v2",
        "source matches the version"
    );
    assert_eq!(
        body["workflow_definition"]["marker"], "opaque-ok",
        "definition blob passes through opaque"
    );
    let versions: Vec<i64> = body["available_versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["version"].as_i64().unwrap())
        .collect();
    assert_eq!(
        versions,
        vec![3, 2, 1],
        "available_versions ordered newest-first by inserted_at"
    );
    assert_eq!(
        body["latest_run_state"], "Completed",
        "aggregate latest run"
    );
    assert_eq!(body["completed_runs"], 1, "aggregate completed runs");
    // `latest_run_at` is the fired instance's scheduled_at — populated (a
    // non-null RFC3339 string) for a workflow that has run, so the UI calendar
    // can land on its latest active year with zero clicks.
    assert!(
        body["latest_run_at"].is_string(),
        "latest_run_at populated for a workflow with runs, got {:?}",
        body["latest_run_at"]
    );

    // 2. Explicit ?version selects that row (and its source).
    let resp = client
        .get(format!("{}/api/workflows/{}?version=1", base, wf))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["version"].as_i64(), Some(1));
    assert_eq!(body["nickel_source"], "src-v1");

    // 3. Explicit ?version that never existed → 404.
    let resp = client
        .get(format!("{}/api/workflows/{}?version=999", base, wf))
        .send()
        .await?;
    assert_eq!(resp.status(), 404, "unknown version is 404");

    // 4. Unknown workflow id → 404.
    let resp = client
        .get(format!("{}/api/workflows/{}", base, Uuid::new_v4()))
        .send()
        .await?;
    assert_eq!(resp.status(), 404, "unknown workflow id is 404");

    // 5. Malformed id → 400.
    let resp = client
        .get(format!("{}/api/workflows/not-a-uuid", base))
        .send()
        .await?;
    assert_eq!(resp.status(), 400, "malformed id is 400");

    // 6. A never-run workflow (a registered version, but no instances) has a
    //    null `latest_run_at` — the UI reads this as "no runs yet" and falls
    //    back to the current year rather than landing on a phantom date.
    let never_run = Uuid::new_v4();
    insert_version(
        &pool,
        never_run,
        "1",
        "Ready",
        "2026-01-01T00:00:00Z",
        "src-norun",
    )
    .await;
    let resp = client
        .get(format!("{}/api/workflows/{}", base, never_run))
        .send()
        .await?;
    assert_eq!(resp.status(), 200, "never-run workflow still resolves");
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["latest_run_at"].is_null(),
        "latest_run_at is null for a never-run workflow, got {:?}",
        body["latest_run_at"]
    );
    assert!(
        body["latest_run_state"].is_null(),
        "latest_run_state is null for a never-run workflow"
    );

    Ok(())
}
