//! Integration test for the API component's day-clock path. Covers the two
//! data sources the handler merges:
//!
//!   - the selected archive repository returns archived runs windowed by
//!     `scheduled_at`, carrying the instance id, snapshotted Workflow name,
//!     and verbatim state.
//!   - `control_plane_client::dashboard_clock` — the live half (per-instance rows).
//!   - `merge_clock_instances` — dedup-by-id with archive-wins on the
//!     compaction-window collision.
//!
//! The archive half requires Docker (testcontainers Postgres); the
//! The fake Control plane runs purely in-process.

#![cfg(not(madsim))]

use axum::{response::IntoResponse, routing::get, Json, Router};
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_api::http::control_plane_client::ControlPlaneClient;
use tickr_api::http::dto::ClockInstance;
use tickr_api::http::live_archive_merge::merge_clock_instances;
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use uuid::Uuid;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_rows_carry_id_name_and_verbatim_state() -> Result<(), Box<dyn std::error::Error>> {
    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: testcontainers Postgres unavailable: {}", e);
            return Ok(());
        }
    };
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &pool).await?;

    let wf = Uuid::new_v4();
    let inst = Uuid::new_v4();
    // A terminal-not-success state must ride through verbatim (no API folding).
    let mut blob = common::instance_blob(inst, wf, "Failed", Some("2026-06-05T09:00:00+00:00"));
    // The day-clock reads the snapshotted workflow name straight off the JSONB.
    blob.as_object_mut()
        .unwrap()
        .insert("workflow_name".into(), serde_json::json!("nightly-etl"));
    common::insert_instance(&pool, &blob).await;

    let repository = ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone());
    let rows = repository.archived_dashboard_instances(None, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, inst, "row carries the instance id for dedup");
    assert_eq!(rows[0].workflow_id, wf);
    assert_eq!(
        rows[0].workflow_name, "nightly-etl",
        "name read from the snapshotted JSONB field"
    );
    assert_eq!(
        rows[0].state, "Failed",
        "verbatim substrate state, no folding"
    );
    Ok(())
}

/// Fake Control plane serving the live clock route with one row.
async fn spawn_fake_control_plane(live: Vec<serde_json::Value>) -> String {
    let app = Router::new().route(
        "/api/dashboard/clock",
        get(move || {
            let live = live.clone();
            async move { Json(serde_json::Value::Array(live)).into_response() }
        }),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind fake Control plane");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap_or(()) });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_decodes_and_merge_resolves_collision_to_archive(
) -> Result<(), Box<dyn std::error::Error>> {
    let shared = Uuid::new_v4().to_string();
    // Live half claims the shared id is still InProgress; a disjoint live-only row too.
    let base = spawn_fake_control_plane(vec![
        serde_json::json!({
            "id": shared,
            "workflow_id": "00000000-0000-0000-0000-000000000000",
            "workflow_name": "etl",
            "scheduled_at": "2026-06-05T09:00:00+00:00",
            "state": "InProgress",
        }),
        serde_json::json!({
            "id": "11111111-1111-1111-1111-111111111111",
            "workflow_id": "00000000-0000-0000-0000-000000000000",
            "workflow_name": "live-only",
            "scheduled_at": "2026-06-05T10:00:00+00:00",
            "state": "Scheduled",
        }),
    ])
    .await;
    let client =
        ControlPlaneClient::new(base, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", true).unwrap();
    let live = client.dashboard_clock(None, None).await?;
    assert_eq!(live.len(), 2, "live rows decode");

    // Archive half claims the shared id reached Completed (the compaction race).
    let archive = vec![ClockInstance {
        id: shared.clone(),
        workflow_id: "00000000-0000-0000-0000-000000000000".to_string(),
        workflow_name: "etl".to_string(),
        scheduled_at: Some("2026-06-05T09:00:00+00:00".to_string()),
        state: "Completed".to_string(),
    }];

    let merged = merge_clock_instances(live, archive);
    assert_eq!(merged.len(), 2, "shared id deduped; live-only kept");
    let shared_row = merged.iter().find(|c| c.id == shared).unwrap();
    assert_eq!(shared_row.state, "Completed", "archive wins on collision");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_coordinator_errors_so_handler_degrades() {
    // An unroutable address: the client surfaces an error, which the handler
    // maps to an empty live half + `live_data_available = false`.
    let client = ControlPlaneClient::new(
        "http://127.0.0.1:1".to_string(),
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        true,
    )
    .unwrap();
    let res = client.dashboard_clock(None, None).await;
    assert!(
        res.is_err(),
        "live call must fail so the flag flips to degraded"
    );
}
