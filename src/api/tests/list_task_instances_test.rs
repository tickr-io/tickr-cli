//! Integration test for the API component's merged task-list path:
//!
//!   - `archive_queries::list_task_instances` rehydrates the archived task
//!     projections in their established completion order.
//!   - `coordinator_client::list_task_instances` decodes a coordinator live response
//!     into the same DTO the API serves.
//!   - `merge_tasks` combines live + archive with archive-wins-on-collision.
//!   - The coordinator client's default timeout is the conductor's 1.5s value.
//!
//! Requires Docker (testcontainers Postgres) for the archive half. The
//! fake-coordinator half runs purely in-process.

#![cfg(not(madsim))]

use axum::{extract::Path, response::IntoResponse, routing::get, Json, Router};
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use std::time::Duration;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_api::http::archive_queries;
use tickr_api::http::coordinator_client::{CoordinatorClient, DEFAULT_TIMEOUT};
use tickr_api::http::dto::TaskInstanceResponse;
use tickr_api::http::live_archive_merge::merge_tasks;
use tickr_proto::instance as ip;
use uuid::Uuid;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_query_returns_task_projections_in_completion_order(
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

    // The parent archive row satisfies the task table's foreign key. Each task
    // payload is constructed from the published snapshot projection directly.
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    common::insert_instance(&pool, &common::instance_blob(wi, wf, "Completed", None)).await;
    let first = snapshot_task(Uuid::new_v4(), Uuid::new_v4(), "extract", "Completed");
    insert_archived_task(&pool, wi, wf, &first).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = snapshot_task(Uuid::new_v4(), Uuid::new_v4(), "load", "Failed");
    insert_archived_task(&pool, wi, wf, &second).await;

    let rows = archive_queries::list_task_instances(&pool, wi).await?;
    assert_eq!(rows.len(), 2, "both task records must be returned");
    assert_eq!(rows[0].id, first.id, "oldest archived_at first");
    assert_eq!(rows[0].name, "extract");
    assert_eq!(rows[1].id, second.id);
    assert_eq!(rows[1].name, "load");
    // The workflow/instance ids the embedded task projection omits are supplied
    // from the task archive's indexed parent identity.
    assert_eq!(rows[0].workflow_instance_id, wi.to_string());
    assert_eq!(rows[0].workflow_id, wf.to_string());
    Ok(())
}

fn snapshot_task(id: Uuid, task_id: Uuid, name: &str, state: &str) -> ip::SnapshotTaskInstance {
    ip::SnapshotTaskInstance {
        id: id.to_string(),
        task_id: task_id.to_string(),
        name: name.to_string(),
        task_type: "RegularTask".to_string(),
        state: state.to_string(),
        executor_id: None,
        attempt: 0,
        started_at: None,
        completed_at: None,
        cancel_reason: None,
        kill_confirmation: None,
        transitions: Vec::new(),
    }
}

async fn insert_archived_task(
    pool: &sqlx::PgPool,
    workflow_instance_id: Uuid,
    workflow_id: Uuid,
    task: &ip::SnapshotTaskInstance,
) {
    sqlx::query(
        r#"
        INSERT INTO task_instances
            (id, workflow_instance_id, workflow_id, task_id, name, state, task_instance, attempt)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(Uuid::parse_str(&task.id).expect("task instance id"))
    .bind(workflow_instance_id)
    .bind(workflow_id)
    .bind(Uuid::parse_str(&task.task_id).expect("task id"))
    .bind(&task.name)
    .bind(&task.state)
    .bind(serde_json::to_value(task).expect("serialize task projection"))
    .bind(task.attempt as i32)
    .execute(pool)
    .await
    .expect("insert archived task");
}

/// Spawn a fake coordinator serving the live task-list route.
async fn spawn_fake_coordinator<F, Fut>(handler: F) -> (String, tokio::task::JoinHandle<()>)
where
    F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = axum::response::Response> + Send + 'static,
{
    let app = Router::new().route(
        "/api/workflows/instances/{id}/tasks",
        get({
            move |Path(id): Path<String>| {
                let handler = handler.clone();
                async move { handler(id).await }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind fake coordinator");
    let addr = listener.local_addr().expect("addr");
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap_or(()) });
    (format!("http://{}", addr), handle)
}

fn task_response(id: &str, state: &str, attempt: u32) -> TaskInstanceResponse {
    TaskInstanceResponse {
        id: id.to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::new_v4().to_string(),
        name: "t".to_string(),
        task_type: "RegularTask".to_string(),
        state: state.to_string(),
        executor_id: None,
        attempt,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_client_decodes_live_task_list() -> Result<(), Box<dyn std::error::Error>> {
    let (base, _server) = spawn_fake_coordinator(|_id| async move {
        Json(vec![
            task_response(&Uuid::new_v4().to_string(), "Running", 0),
            task_response(&Uuid::new_v4().to_string(), "Queued", 0),
        ])
        .into_response()
    })
    .await;

    let client = CoordinatorClient::new(base);
    let live = client.list_task_instances(Uuid::new_v4()).await?;
    assert_eq!(live.len(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_tasks_resolves_collision_to_archive() {
    // Same task id in both halves; archive carries the higher attempt. Archive
    // must win the dedup.
    let live = vec![task_response("shared", "Running", 0)];
    let archive = vec![task_response("shared", "Failed", 2)];
    let merged = merge_tasks(live, archive);
    assert_eq!(merged.len(), 1, "collision collapses to one row");
    assert_eq!(merged[0].state, "Failed", "archive wins");
    assert_eq!(merged[0].attempt, 2, "archived attempt survives");
}

#[test]
fn coordinator_client_default_timeout_is_1500ms() {
    assert_eq!(
        DEFAULT_TIMEOUT,
        Duration::from_millis(1_500),
        "matches the conductor's coordinator-call budget"
    );
}
