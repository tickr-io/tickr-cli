//! Integration test for the API component's merged list-instances path:
//!
//!   - the selected archive repository returns Workflow-instance rows newest
//!     first with stable tie-breaks.
//! - `control_plane_client::list_workflow_instances` decodes the Control
//!   plane's live response into the same DTO the API serves.
//! - The merge resolves collisions with the archive row winning and combines
//!   live and archive rows disjointly.
//!
//! Requires Docker (testcontainers Postgres) for the archive half. The fake
//! Control plane runs purely in-process.

#![cfg(not(madsim))]

use axum::{extract::Path, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_api::http::control_plane_client::ControlPlaneClient;
use tickr_api::http::dto::WorkflowInstanceResponse;
use tickr_api::http::live_archive_merge::merge_instances;
use tickr_migrations::archive_repository::ArchivePage;
use tickr_migrations::backend::ReadOnlyRepositoryBundle;
use uuid::Uuid;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_query_returns_rows_for_workflow_newest_first(
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

    let wf = Uuid::new_v4();
    let earlier_id = Uuid::new_v4();
    common::insert_instance(
        &pool,
        &common::instance_blob(earlier_id, wf, "Completed", None),
    )
    .await;
    // Force a non-zero archived_at gap so the order assertion is meaningful.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let later_id = Uuid::new_v4();
    common::insert_instance(&pool, &common::instance_blob(later_id, wf, "Failed", None)).await;

    let repository = ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone());
    let rows = repository
        .archived_workflow_instances(wf, ArchivePage::unbounded())
        .await?;
    assert_eq!(rows.len(), 2, "both rows must be returned");
    assert_eq!(rows[0].id, later_id.to_string(), "newest archived_at first");
    assert_eq!(rows[1].id, earlier_id.to_string());
    Ok(())
}

/// Spawn an Axum router with the given handler at the Control-plane live-instances route.
async fn spawn_fake_control_plane<F, Fut>(handler: F) -> (String, tokio::task::JoinHandle<()>)
where
    F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = axum::response::Response> + Send + 'static,
{
    let app = Router::new().route(
        "/api/workflows/{id}/instances",
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
    let addr = listener.local_addr().expect("addr");
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap_or(()) });
    (format!("http://{}", addr), handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_client_decodes_list_of_instances() -> Result<(), Box<dyn std::error::Error>>
{
    let wf = Uuid::new_v4();
    let (base, _server) = spawn_fake_control_plane(move |_id| async move {
        Json(vec![
            WorkflowInstanceResponse {
                id: Uuid::new_v4().to_string(),
                workflow_id: wf.to_string(),
                workflow_version: 0,
                name: String::new(),
                state: "Running".to_string(),
                scheduled_at: None,
                task_count: 0,
                completed_tasks: 0,
            },
            WorkflowInstanceResponse {
                id: Uuid::new_v4().to_string(),
                workflow_id: wf.to_string(),
                workflow_version: 0,
                name: String::new(),
                state: "Scheduled".to_string(),
                scheduled_at: None,
                task_count: 0,
                completed_tasks: 0,
            },
        ])
        .into_response()
    })
    .await;

    let client =
        ControlPlaneClient::new(base, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", true).unwrap();
    let live = client.list_workflow_instances(wf).await?;
    assert_eq!(live.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_timeout_surfaces_as_typed_error() -> Result<(), Box<dyn std::error::Error>> {
    let (base, _server) = spawn_fake_control_plane(|_id| async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        (StatusCode::OK, "late").into_response()
    })
    .await;

    // The handler degrades to archive-only on any Control-plane HTTP error; this asserts
    // the client surfaces the timeout the handler keys its `live_data_available:
    // false` branch off of.
    let client = ControlPlaneClient::with_timeout(
        base,
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        true,
        Duration::from_millis(100),
    )
    .unwrap();
    let err = client
        .list_workflow_instances(Uuid::new_v4())
        .await
        .expect_err("expected timeout");
    assert!(
        matches!(
            err,
            tickr_api::http::control_plane_client::ControlPlaneClientError::Timeout
        ),
        "got {:?}",
        err
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_includes_live_and_archive_disjointly() {
    let live = vec![WorkflowInstanceResponse {
        id: "a".to_string(),
        workflow_id: "w".to_string(),
        workflow_version: 0,
        name: String::new(),
        state: "Running".to_string(),
        scheduled_at: None,
        task_count: 0,
        completed_tasks: 0,
    }];
    let archive = vec![WorkflowInstanceResponse {
        id: "b".to_string(),
        workflow_id: "w".to_string(),
        workflow_version: 0,
        name: String::new(),
        state: "Completed".to_string(),
        scheduled_at: None,
        task_count: 0,
        completed_tasks: 0,
    }];
    let merged = merge_instances(live, archive);
    let ids: HashSet<&str> = merged.iter().map(|i| i.id.as_str()).collect();
    assert_eq!(merged.len(), 2);
    assert!(ids.contains("a"));
    assert!(ids.contains("b"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_resolves_collision_to_archive() {
    let live = vec![WorkflowInstanceResponse {
        id: "z".to_string(),
        workflow_id: "w".to_string(),
        workflow_version: 0,
        name: String::new(),
        state: "Running".to_string(),
        scheduled_at: None,
        task_count: 0,
        completed_tasks: 0,
    }];
    let archive = vec![WorkflowInstanceResponse {
        id: "z".to_string(),
        workflow_id: "w".to_string(),
        workflow_version: 0,
        name: String::new(),
        state: "Completed".to_string(),
        scheduled_at: None,
        task_count: 0,
        completed_tasks: 0,
    }];
    let merged = merge_instances(live, archive);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].state, "Completed");
}
