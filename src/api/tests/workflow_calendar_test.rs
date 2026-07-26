//! Backend-parameterized API laws for Run-calendar IANA bucketing.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_api::http::dto::WorkflowInstanceResponse;
use tickr_migrations::archive_repository::{
    ArchiveTerminalWorkflowInput, ArchivedCalendarCandidate,
};
use tickr_migrations::backend::{
    ReadOnlyRepositoryBundle, RepositoryFactory, WriterRepositoryBundle,
};
use tickr_proto::archive as ap;
use tickr_proto::config::DataPlaneSql;
use uuid::Uuid;

mod common;

enum SeedPool {
    Postgres(sqlx::PgPool),
    Sqlite(sqlx::SqlitePool),
}

struct BackendHarness {
    _postgres: Option<testcontainers_modules::testcontainers::ContainerAsync<Postgres>>,
    _sqlite: Option<TempDir>,
    seed_pool: SeedPool,
    writer: WriterRepositoryBundle,
    reader: Arc<ReadOnlyRepositoryBundle>,
}

impl BackendHarness {
    async fn insert_workflow(&self, workflow_id: Uuid) {
        match &self.seed_pool {
            SeedPool::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO workflows \
                     (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source) \
                     VALUES ($1, 0, 'default', 'calendar', 'calendar', 'Ready', 'hash', 'cosmetic', '{}'::jsonb, '')",
                )
                .bind(workflow_id)
                .execute(pool)
                .await
                .unwrap();
            }
            SeedPool::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO workflows \
                     (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source) \
                     VALUES (?1, 0, 'default', 'calendar', 'calendar', 'Ready', 'hash', 'cosmetic', '{}', '')",
                )
                .bind(workflow_id.to_string())
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }

    async fn archive(
        &self,
        workflow_id: Uuid,
        instance_id: Uuid,
        state: &str,
        scheduled_at: &str,
        archived_at: &str,
    ) {
        let mut projection: ap::ArchiveProjection = serde_json::from_value(common::instance_blob(
            instance_id,
            workflow_id,
            state,
            Some(scheduled_at),
        ))
        .unwrap();
        projection.task_instances.clear();
        self.writer
            .archive_terminal_workflow(ArchiveTerminalWorkflowInput {
                projection: &projection,
                ctx_envelope: json!([]),
                runtime_params: json!({}),
                log_uris: json!({}),
                archived_at: DateTime::parse_from_rfc3339(archived_at)
                    .unwrap()
                    .with_timezone(&Utc),
            })
            .await
            .unwrap();
    }

    async fn close(&self) {
        self.reader.close().await;
        self.writer.close().await;
        match &self.seed_pool {
            SeedPool::Postgres(pool) => pool.close().await,
            SeedPool::Sqlite(pool) => pool.close().await,
        }
    }
}

async fn start_postgres() -> Option<BackendHarness> {
    let container = match Postgres::default().start().await {
        Ok(container) => container,
        Err(error) => {
            eprintln!("skipping: Postgres testcontainer unavailable: {error}");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let seed_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .ok()?;
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &seed_pool)
        .await
        .ok()?;
    let factory = RepositoryFactory::new(DataPlaneSql::Postgres { url });
    Some(BackendHarness {
        _postgres: Some(container),
        _sqlite: None,
        seed_pool: SeedPool::Postgres(seed_pool.clone()),
        writer: factory.open_writer().await.ok()?,
        reader: Arc::new(factory.open_read_only().await.ok()?),
    })
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

async fn start_sqlite() -> BackendHarness {
    let directory = TempDir::new().unwrap();
    let url = sqlite_url(&directory.path().join("calendar.db"));
    let seed_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    tickr_migrations::apply_sqlite(tickr_migrations::MigrationTarget::Conductor, &seed_pool)
        .await
        .unwrap();
    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url });
    BackendHarness {
        _postgres: None,
        _sqlite: Some(directory),
        seed_pool: SeedPool::Sqlite(seed_pool),
        writer: factory.open_writer().await.unwrap(),
        reader: Arc::new(factory.open_read_only().await.unwrap()),
    }
}

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    NatsClient,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = Nats::default().with_cmd(&cmd).start().await.ok()?;
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let client = async_nats::connect(format!("nats://127.0.0.1:{port}"))
        .await
        .ok()?;
    Some((container, client))
}

async fn spawn_fake_coordinator(rows: Vec<WorkflowInstanceResponse>) -> String {
    let app = axum::Router::new().route(
        "/api/workflows/{workflow_id}/instances",
        axum::routing::get(move || {
            let rows = rows.clone();
            async move { axum::Json(rows) }
        }),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{address}")
}

async fn spawn_api(nats: NatsClient, harness: &BackendHarness, coordinator_url: &str) -> String {
    let coordinator = Arc::new(tickr_api::http::coordinator_client::CoordinatorClient::new(
        coordinator_url.to_string(),
    ));
    let s3 = opendal::services::S3::default()
        .bucket("ignored")
        .endpoint("http://127.0.0.1:1")
        .access_key_id("x")
        .secret_access_key("x")
        .region("us-east-1");
    let minio = opendal::Operator::new(s3).unwrap().finish();
    let logs = Arc::new(tickr_api::http::logs_resolver::LogsResolver::new(
        minio,
        Arc::new(tickr_executor::log_stream::AllNatsLogStreamProvider::new(
            Arc::new(nats.clone()),
            Duration::from_secs(5),
        )),
    ));
    let state = tickr_api::http::routes::build_app_state(
        Arc::new(nats),
        Arc::clone(&harness.reader),
        coordinator,
        logs,
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, tickr_api::http::routes::build_router(state))
            .await
            .unwrap();
    });
    format!("http://{address}")
}

fn live_instance(
    instance_id: Uuid,
    workflow_id: Uuid,
    state: &str,
    scheduled_at: &str,
) -> WorkflowInstanceResponse {
    WorkflowInstanceResponse {
        id: instance_id.to_string(),
        workflow_id: workflow_id.to_string(),
        workflow_version: 0,
        name: format!("live-{instance_id}"),
        state: state.to_string(),
        scheduled_at: Some(scheduled_at.to_string()),
        task_count: 0,
        completed_tasks: 0,
    }
}

async fn seed_identical_fixtures(
    postgres: &BackendHarness,
    sqlite: &BackendHarness,
    workflow_id: Uuid,
) {
    let completed = Uuid::from_u128(10);
    let failed = Uuid::from_u128(11);
    let next_year = Uuid::from_u128(12);
    for backend in [postgres, sqlite] {
        backend.insert_workflow(workflow_id).await;
        // `archived_at` deliberately disagrees with calendar placement.
        backend
            .archive(
                workflow_id,
                completed,
                "Completed",
                "2025-12-31T18:15:00Z",
                "2030-01-01T00:00:00Z",
            )
            .await;
        backend
            .archive(
                workflow_id,
                failed,
                "Failed",
                "2026-12-31T18:14:59.999999Z",
                "2024-01-01T00:00:00Z",
            )
            .await;
        backend
            .archive(
                workflow_id,
                next_year,
                "Completed",
                "2026-12-31T18:15:00Z",
                "2026-01-01T00:00:00Z",
            )
            .await;
    }
}

async fn calendar_body(
    client: &reqwest::Client,
    base: &str,
    workflow_id: Uuid,
    year: i32,
    timezone: &str,
) -> serde_json::Value {
    let response = client
        .get(format!(
            "{base}/api/workflows/{workflow_id}/calendar?year={year}&tz={timezone}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-live-data-available"], "true");
    response.json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_and_sqlite_calendar_api_laws_match() {
    let Some(postgres) = start_postgres().await else {
        return;
    };
    let sqlite = start_sqlite().await;
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let workflow_id = Uuid::from_u128(1);
    seed_identical_fixtures(&postgres, &sqlite, workflow_id).await;

    let live_rows = vec![
        live_instance(
            Uuid::from_u128(20),
            workflow_id,
            "InProgress",
            "2026-06-15T00:00:00Z",
        ),
        live_instance(
            Uuid::from_u128(21),
            workflow_id,
            "Scheduled",
            "2027-01-01T00:00:00Z",
        ),
    ];
    let coordinator = spawn_fake_coordinator(live_rows).await;
    let postgres_api = spawn_api(nats.clone(), &postgres, &coordinator).await;
    let sqlite_api = spawn_api(nats, &sqlite, &coordinator).await;
    let client = reqwest::Client::new();

    let postgres_calendar =
        calendar_body(&client, &postgres_api, workflow_id, 2026, "Asia/Kathmandu").await;
    let sqlite_calendar =
        calendar_body(&client, &sqlite_api, workflow_id, 2026, "Asia/Kathmandu").await;
    assert_eq!(postgres_calendar, sqlite_calendar);
    assert_eq!(
        postgres_calendar,
        json!({
            "year": 2026,
            "tz": "Asia/Kathmandu",
            "days": [
                {
                    "date": "2026-01-01",
                    "completed": 1,
                    "failed": 0,
                    "in_progress": 0,
                    "scheduled": 0,
                    "total": 1
                },
                {
                    "date": "2026-06-15",
                    "completed": 0,
                    "failed": 0,
                    "in_progress": 1,
                    "scheduled": 0,
                    "total": 1
                },
                {
                    "date": "2026-12-31",
                    "completed": 0,
                    "failed": 1,
                    "in_progress": 0,
                    "scheduled": 0,
                    "total": 1
                }
            ],
            "live_data_available": true
        })
    );

    // Viewer timezone changes recompute the year boundary instead of reading a
    // stored date. The complete backend responses still match.
    let postgres_utc = calendar_body(&client, &postgres_api, workflow_id, 2025, "UTC").await;
    let sqlite_utc = calendar_body(&client, &sqlite_api, workflow_id, 2025, "UTC").await;
    assert_eq!(postgres_utc, sqlite_utc);
    assert_eq!(postgres_utc["days"][0]["date"], "2025-12-31");

    // Click-through uses the same envelope and bucketer. Its terminal rows are
    // exactly those counted in the selected cell.
    let postgres_click = client
        .get(format!(
            "{postgres_api}/api/workflows/{workflow_id}/instances?date=2026-01-01&tz=Asia/Kathmandu"
        ))
        .send()
        .await
        .unwrap();
    let sqlite_click = client
        .get(format!(
            "{sqlite_api}/api/workflows/{workflow_id}/instances?date=2026-01-01&tz=Asia/Kathmandu"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(postgres_click.status(), 200);
    assert_eq!(sqlite_click.status(), 200);
    let postgres_click: serde_json::Value = postgres_click.json().await.unwrap();
    let sqlite_click: serde_json::Value = sqlite_click.json().await.unwrap();
    assert_eq!(postgres_click, sqlite_click);
    assert_eq!(postgres_click.as_array().unwrap().len(), 1);
    assert_eq!(postgres_click[0]["id"], Uuid::from_u128(10).to_string());
    assert_eq!(postgres_click[0]["state"], "Completed");

    // Validation precedes definition, archive, and coordinator reads. Closing
    // both selected repositories makes any accidental SQL call observable.
    postgres.reader.close().await;
    sqlite.reader.close().await;
    for base in [&postgres_api, &sqlite_api] {
        for timezone in ["Not/AZone", "%%%"] {
            let response = client
                .get(format!(
                    "{base}/api/workflows/{workflow_id}/calendar?year=2026&tz={timezone}"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400);
            let response = client
                .get(format!(
                    "{base}/api/workflows/{workflow_id}/instances?date=2026-01-01&tz={timezone}"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400);
        }
    }

    postgres.close().await;
    sqlite.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_candidates_match_before_bucketing() {
    let Some(postgres) = start_postgres().await else {
        return;
    };
    let sqlite = start_sqlite().await;
    let workflow_id = Uuid::from_u128(100);
    seed_identical_fixtures(&postgres, &sqlite, workflow_id).await;
    let start = DateTime::parse_from_rfc3339("2025-12-31T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339("2027-01-02T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let project = |rows: Vec<ArchivedCalendarCandidate>| {
        rows.into_iter()
            .map(|candidate| {
                (
                    candidate.instance.id,
                    candidate.instance.workflow_id,
                    candidate.instance.state,
                    candidate.scheduled_at,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        project(
            postgres
                .reader
                .archived_calendar_candidates(workflow_id, start, end)
                .await
                .unwrap()
        ),
        project(
            sqlite
                .reader
                .archived_calendar_candidates(workflow_id, start, end)
                .await
                .unwrap()
        )
    );
    postgres.close().await;
    sqlite.close().await;
}
