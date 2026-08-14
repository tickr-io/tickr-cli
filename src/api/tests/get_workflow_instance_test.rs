//! Integration tests for the API component's selected archive-detail read and
//! live Control-plane fallback. Archive assertions cover present/absent terminal
//! rows, projection reconstruction, history, and data-plane field isolation.
//! Control-plane assertions cover success, 404, 503, and timeout behavior.
//!
//! PostgreSQL tests require Docker; fake Control-plane tests run in-process.

#![cfg(not(madsim))]

use axum::{extract::Path, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use std::time::Duration;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_api::http::control_plane_client::{ControlPlaneClient, ControlPlaneClientError};
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use tickr_proto::instance as ip;
use uuid::Uuid;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_query_returns_some_when_row_exists() -> Result<(), Box<dyn std::error::Error>> {
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

    let id = Uuid::new_v4();
    common::insert_instance(
        &pool,
        &common::instance_blob(id, Uuid::new_v4(), "Completed", None),
    )
    .await;

    let repository = ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone());
    let found = repository
        .archived_workflow_detail(id)
        .await?
        .expect("row must be returned")
        .instance;
    assert_eq!(found.id, id.to_string(), "round-trip preserves id");
    assert_eq!(
        found.state, "Completed",
        "round-trip preserves state via JSONB"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_query_returns_none_when_row_absent() -> Result<(), Box<dyn std::error::Error>> {
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

    let missing_id = Uuid::new_v4();
    let repository = ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone());
    let result = repository.archived_workflow_detail(missing_id).await?;
    assert!(result.is_none(), "no row inserted, query must return None");
    Ok(())
}

/// Spawns a fake Control plane on a random port. The handler closure decides
/// the response. Returns the bound base URL.
async fn spawn_fake_control_plane<F, Fut>(handler: F) -> (String, tokio::task::JoinHandle<()>)
where
    F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = axum::response::Response> + Send + 'static,
{
    let app = Router::new().route(
        "/api/workflows/instances/{id}",
        get({
            move |Path(id): Path<String>| {
                let handler = handler.clone();
                async move { handler(id).await }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind fake Control plane");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap_or(());
    });
    (format!("http://{}", addr), handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archived_jsonb_rehydrates_through_projection_with_history_and_no_internals(
) -> Result<(), Box<dyn std::error::Error>> {
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

    let id = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();
    common::insert_instance(
        &pool,
        &common::instance_blob(id, workflow_id, "Completed", None),
    )
    .await;

    // Archive read: the stored union projection is decoded and reconstructed
    // into the instance-detail render, which the handler stamps `storage:
    // archived` to serve. The union embeds the task-instance records, so the
    // task list rehydrates from the one stored shape — no separate task blob.
    let repository = ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone());
    let archived = repository
        .archived_workflow_detail(id)
        .await?
        .expect("archived row")
        .instance;
    let snapshot = tickr_proto::codec::archive::snapshot_from_archived(archived, "archived");
    let json = serde_json::to_value(&snapshot)?;
    assert_eq!(json["storage"], "archived");

    // The recorded transition history survives the round-trip: the instance's
    // four transitions and the derived timestamps, plus the embedded task
    // instance's per-attempt chain.
    assert_eq!(json["transitions"].as_array().unwrap().len(), 4);
    assert!(json["started_at"].is_string());
    assert!(json["triggered_at"].is_string());
    assert!(json["completed_at"].is_string());
    let archived_ti = &json["task_instances"][0];
    assert_eq!(archived_ti["transitions"].as_array().unwrap().len(), 5);
    assert!(archived_ti["started_at"].is_string());
    assert!(archived_ti["completed_at"].is_string());

    // Non-tenant data are structurally absent from the projection, so a
    // tenant reading its own archive cannot recover them.
    let obj = json.as_object().unwrap();
    for forbidden in [
        "owned",
        "tombstoned",
        "timer_id",
        "task_mapping",
        "stalled",
        "stalled_by",
    ] {
        assert!(
            !obj.contains_key(forbidden),
            "cluster internal `{}` leaked onto the wire",
            forbidden
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_client_decodes_live_response() -> Result<(), Box<dyn std::error::Error>> {
    let live_id = Uuid::new_v4();
    // The live half serves the published proto snapshot directly. Build one with
    // the fields under assertion; the client decodes it into the same type.
    let snapshot = ip::InstanceSnapshot {
        id: live_id.to_string(),
        state: "InProgress".to_string(),
        workflow_version: 12,
        storage: "live".to_string(),
        task_count: 1,
        ..Default::default()
    };
    let body = serde_json::to_value(&snapshot)?;
    let (base, _server) = spawn_fake_control_plane(move |_id| {
        let body = body.clone();
        async move { Json(body).into_response() }
    })
    .await;

    let client = ControlPlaneClient::new(base);
    let response = client.get_workflow_instance(live_id).await?;
    assert_eq!(response.id, live_id.to_string());
    assert_eq!(response.state, "InProgress");
    assert_eq!(response.workflow_version, 12);
    assert_eq!(response.storage, "live");
    assert_eq!(response.task_count, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_client_surfaces_503_as_server_error(
) -> Result<(), Box<dyn std::error::Error>> {
    let (base, _server) = spawn_fake_control_plane(|_id| async move {
        (StatusCode::SERVICE_UNAVAILABLE, "live store unreachable").into_response()
    })
    .await;

    let client = ControlPlaneClient::new(base);
    let err = client
        .get_workflow_instance(Uuid::new_v4())
        .await
        .expect_err("expected Server error");
    assert!(
        matches!(err, ControlPlaneClientError::Server { status: 503, .. }),
        "got {:?}",
        err
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_client_maps_404_to_not_found() -> Result<(), Box<dyn std::error::Error>> {
    let (base, _server) = spawn_fake_control_plane(|_id| async move {
        (StatusCode::NOT_FOUND, "missing").into_response()
    })
    .await;

    let client = ControlPlaneClient::new(base);
    let err = client
        .get_workflow_instance(Uuid::new_v4())
        .await
        .expect_err("expected NotFound");
    assert!(
        matches!(err, ControlPlaneClientError::NotFound(_)),
        "got {:?}",
        err
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_client_times_out_on_slow_control_plane(
) -> Result<(), Box<dyn std::error::Error>> {
    let (base, _server) = spawn_fake_control_plane(|_id| async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        (StatusCode::OK, "late").into_response()
    })
    .await;

    let client = ControlPlaneClient::with_timeout(base, Duration::from_millis(100));
    let err = client
        .get_workflow_instance(Uuid::new_v4())
        .await
        .expect_err("expected Timeout");
    assert!(
        matches!(err, ControlPlaneClientError::Timeout),
        "got {:?}",
        err
    );
    Ok(())
}
