//! Behavioral tests for the operator health surface (`GET /api/health`).
//!
//! The endpoint computes each component row **fresh per request** from a cheap
//! check — no cached health table. Tests drive a component into a known state and
//! assert the *reported row*, never a private field:
//!
//! - deterministic, no-Docker: API-self is always healthy; an unreachable
//!   selected repository reports `unhealthy` without leaking connection details;
//!   an unreachable NATS client reports the KV row `unhealthy`; the serialized
//!   shape carries `checked_at` + lowercase status.
//! - Docker-gated (testcontainers, skipped when unavailable): reachable Postgres
//!   and NATS report `healthy`, and the wired `/api/health` route serves the typed
//!   report and reflects a mid-flight state change (proving no cached table) while
//!   the top-level `/health` readiness probe stays `{"status":"ok"}`.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use futures::StreamExt as _;
use prost::Message as _;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_api::commands::client::send_command;
use tickr_api::commands::client::CommandBus;
use tickr_api::commands::local::LocalCommandBusConfig;
use tickr_api::http::control_plane_client::ControlPlaneClient;
use tickr_api::http::health::{
    api_self, build_health_report, build_health_report_with_fleet_status, check_conductor,
    check_control_plane, check_data_plane_sql, check_executor_fleet_observations, check_executors,
    check_nats_kv, ComponentStatus, DataPlaneSqlImplementation, ExecutorCapacityInterpretation,
    HealthCoordinationRole, HealthFinalLogStore, HealthFormationProfile, HealthFormationTopology,
    HealthProtocolIdentity, HealthResolvedRole, HealthRoleImplementation, HealthSubstrateSelection,
    HealthWriterTopology, ResolvedFormationHealth,
};
use tickr_api::http::logs_resolver::{
    LocalTaskLogStore, LogBatchPage, LogsError, LogsResolver, TaskLogs,
};
use tickr_api::http::routes::ConsoleAsset;
use tickr_executor::local_pickup::{
    ExecutorCapacityObservation, ExecutorFleetSnapshot, LocalExecutorCapacity,
};
use tickr_migrations::backend::{ReadOnlyRepositoryBundle, RepositoryFactory};
use tickr_migrations::{apply_sqlite, apply_target, sqlite_writer_options, MigrationTarget};
use tickr_proto::config::DataPlaneSql;
use tickr_proto::coord::{
    component_liveness_key, ComponentLivenessValue, COMPONENT_LIVENESS_BUCKET,
};
use tickr_proto::tickr_api as api;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Deterministic checks — no Docker required.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_self_row_is_always_healthy() {
    // Reaching the handler is the whole claim: the row is a constant healthy.
    assert_eq!(api_self().status, ComponentStatus::Healthy);
}

#[test]
fn expiring_executor_observations_report_freshness_without_capacity_guarantees() {
    let snapshot = ExecutorFleetSnapshot {
        server_time_millis: 1_000,
        observation_ttl_millis: 100,
        observations: vec![
            ExecutorCapacityObservation {
                executor_id: Uuid::new_v4(),
                reporter_id: Uuid::new_v4(),
                sequence: 2,
                configured_process_slots: 4,
                in_flight_count: 5,
                observed_at_server_millis: 990,
                expires_at_server_millis: 1_090,
            },
            ExecutorCapacityObservation {
                executor_id: Uuid::new_v4(),
                reporter_id: Uuid::new_v4(),
                sequence: 1,
                configured_process_slots: 8,
                in_flight_count: 3,
                observed_at_server_millis: 900,
                expires_at_server_millis: 999,
            },
        ],
    };

    let health = check_executor_fleet_observations(&snapshot);
    assert_eq!(health.status, ComponentStatus::Healthy);
    assert_eq!(
        health.capacity_interpretation,
        ExecutorCapacityInterpretation::ObservationOnly
    );
    assert_eq!(health.observed_executors, Some(1));
    assert_eq!(health.configured_process_slots, Some(4));
    assert_eq!(health.in_flight_count, Some(5));
    assert_eq!(health.freshest_observation_age_ms, Some(10));
    assert_eq!(health.oldest_observation_age_ms, Some(10));
    assert_eq!(health.observation_ttl_ms, Some(100));
    assert!(health.detail.contains("not guaranteed available capacity"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_postgres_down_row_is_unhealthy_without_connection_detail() {
    let secret = "top-secret";
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_lazy(&format!(
            "postgres://operator:{secret}@127.0.0.1:1/postgres"
        ))
        .expect("lazy pool builds");
    let row = check_data_plane_sql(&ReadOnlyRepositoryBundle::from_postgres_pool(pool)).await;
    assert_eq!(row.implementation, DataPlaneSqlImplementation::Postgres);
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "unreachable repository must report unhealthy, got detail: {}",
        row.detail
    );
    assert!(
        row.detail.starts_with("repository health check failed: "),
        "failure uses shared classification: {}",
        row.detail
    );
    assert!(
        !row.detail.contains(secret) && !row.detail.contains("postgres://"),
        "failure detail must redact connection material: {}",
        row.detail
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nats_kv_unreachable_row_is_unhealthy() {
    // `retry_on_initial_connect` yields a Client that never reaches a server; a
    // short request timeout bounds the failing KV lookup. The connection is not
    // Connected, so the KV row is unhealthy.
    let client = async_nats::ConnectOptions::new()
        .request_timeout(Some(Duration::from_secs(1)))
        .retry_on_initial_connect()
        .connect("nats://127.0.0.1:1")
        .await
        .expect("client builds even without a server");
    let row = check_nats_kv(&client).await;
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "unreachable JetStream KV must report unhealthy, got detail: {}",
        row.detail
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executors_unreachable_row_is_unhealthy_zero_fleet() {
    // No reachable NATS ⇒ no executor key can be observed ⇒ the pool row reads
    // zero-fleet unhealthy with explicit observational wording. The read never
    // scans the task-liveness bucket or a cached table — it can only see this one.
    let client = async_nats::ConnectOptions::new()
        .request_timeout(Some(Duration::from_secs(1)))
        .retry_on_initial_connect()
        .connect("nats://127.0.0.1:1")
        .await
        .expect("client builds even without a server");
    let row = check_executors(&client, Duration::from_secs(120)).await;
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "no observable executor ⇒ unhealthy, got detail: {}",
        row.detail
    );
    assert_eq!(
        row.detail,
        "0 observed executors · observed load 0/0 configured slots"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conductor_row_unhealthy_when_broker_unreachable() {
    // No reachable broker ⇒ the command-bus Ping cannot be answered ⇒ the
    // Conductor row is command-plane-unresponsive (unhealthy). A short deadline
    // bounds the failing probe.
    let client = async_nats::ConnectOptions::new()
        .request_timeout(Some(Duration::from_secs(1)))
        .retry_on_initial_connect()
        .connect("nats://127.0.0.1:1")
        .await
        .expect("client builds even without a server");
    let row = check_conductor(&client, Duration::from_secs(1)).await;
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "no answer to Ping ⇒ unhealthy, got detail: {}",
        row.detail
    );
    // The row is honestly a command-plane-responsive check, not a relay claim.
    assert!(
        row.detail.contains("command-plane-responsive"),
        "detail labels a command-plane-responsive check, got: {}",
        row.detail
    );
}

/// A fake Frontend serving `/api/internal/health` with a fixed rollup body —
/// stands in for the real Frontend so the Control-plane hop is exercised with no
/// Control plane behind it. Mirrors the fake-Control-plane helpers the other API
/// tests use.
async fn spawn_fake_control_plane(status: &'static str) -> String {
    let app = axum::Router::new().route(
        "/api/internal/health",
        axum::routing::get(
            move || async move { axum::Json(serde_json::json!({ "status": status })) },
        ),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind fake Control plane");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap_or(()) });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_unreachable_frontend_row_is_unhealthy() {
    // The Frontend is the only path the UI reaches the Control plane through, so the
    // Control plane is one rollup reached via one HTTP hop — and an unreachable
    // Frontend makes the row unhealthy, mirroring the Frontend's own
    // live-store-unreachable degrade path.
    let control_plane = ControlPlaneClient::new("http://127.0.0.1:1".to_string());
    let row = check_control_plane(&control_plane).await;
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "unreachable Frontend ⇒ Control-plane row unhealthy, got detail: {}",
        row.detail
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_healthy_when_frontend_rollup_healthy() {
    // A reachable Frontend returning a healthy rollup makes the Control-plane row
    // healthy. Exactly one HTTP hop to /api/internal/health; the rollup is
    // reported as one row, never split per-component.
    let base = spawn_fake_control_plane("healthy").await;
    let control_plane = ControlPlaneClient::new(base);
    let row = check_control_plane(&control_plane).await;
    assert_eq!(
        row.status,
        ComponentStatus::Healthy,
        "healthy rollup ⇒ control-plane row healthy, got detail: {}",
        row.detail
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serialized_report_shape_is_stable_and_lowercase() {
    // Build a report against a down pool + unreachable client, then assert the
    // wire shape: a global `checked_at` and one lowercase-status row per component,
    // each carrying `status`, `detail`, and a `detection_window`.
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
        .expect("lazy pool builds");
    let client = async_nats::ConnectOptions::new()
        .request_timeout(Some(Duration::from_secs(1)))
        .retry_on_initial_connect()
        .connect("nats://127.0.0.1:1")
        .await
        .expect("client builds");
    // Dead-address coordinator: the control-plane hop fails, so that row is
    // unhealthy — still present with a stable shape, which is all this asserts.
    let control_plane = ControlPlaneClient::new("http://127.0.0.1:1".to_string());

    let repositories =
        tickr_migrations::backend::ReadOnlyRepositoryBundle::from_postgres_pool(pool);
    let report = build_health_report(
        &repositories,
        &client,
        &control_plane,
        Duration::from_secs(1),
    )
    .await;
    let json = serde_json::to_value(&report).expect("serializes");

    assert!(
        json.get("checked_at").and_then(|v| v.as_str()).is_some(),
        "report carries a global checked_at string"
    );
    for row in [
        "api",
        "data_plane_sql",
        "nats_kv",
        "executors",
        "conductor",
        "control_plane",
    ] {
        let obj = json.get(row).unwrap_or_else(|| panic!("row {row} present"));
        let status = obj
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("row {row} has a status string"));
        assert!(
            matches!(status, "healthy" | "degraded" | "unhealthy"),
            "row {row} status must be one of the three bands, got {status}"
        );
        assert!(
            obj.get("detail").and_then(|v| v.as_str()).is_some(),
            "row {row} carries a detail string"
        );
        assert!(
            obj.get("detection_window")
                .and_then(|v| v.as_str())
                .is_some(),
            "row {row} carries a detection_window string"
        );
    }
    assert_eq!(json["data_plane_sql"]["implementation"], "postgres");
    assert!(
        json.get("postgres").is_none(),
        "legacy Postgres-specific row must be absent"
    );
    assert_eq!(
        json["api"]["status"], "healthy",
        "API-self is always healthy"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_redis_capability_projection_is_exposed_verbatim_and_secret_free() {
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(50))
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
        .expect("lazy pool builds");
    let repositories =
        tickr_migrations::backend::ReadOnlyRepositoryBundle::from_postgres_pool(pool);
    let (command_bus, _writer) = CommandBus::local(LocalCommandBusConfig::default());
    let control_plane = ControlPlaneClient::new("http://127.0.0.1:1".to_owned());
    let fleet =
        LocalExecutorCapacity::new(Uuid::new_v4(), NonZeroUsize::new(1).unwrap()).observation();
    let projection = serde_json::json!({
        "capability_fingerprint": "sha256:admitted",
        "profile": "all-redis",
        "redis_implementation": "redis_oss",
        "redis_version": "7.4.2",
        "topology_class": "single_writable_primary",
        "role_protocols": [{"role": "command_bus", "protocol": {"name": "tickr.redis.command-bus", "version": 1}}],
        "operation_manifests": [{"role": "command_bus", "identity": "sha256:manifest"}],
        "durability_class": "one local-primary AOF fsync, zero required replica acknowledgements",
        "normalized_limits": [{"role": "command_bus", "limits": {"max-memory-bytes": 1024}}],
        "capacity": {"configured_memory_bytes": 4096, "used_memory_bytes": 512},
        "quota_state": [{"role": "command_bus", "state": {"used": 1, "soft_threshold": 8, "hard_limit": 10, "accepted_identities": 1, "pressure": "normal"}}],
        "fence": {"state": "open", "generation": 1, "ready": true},
        "last_capability_failure": null
    });

    let report = build_health_report_with_fleet_status(
        &repositories,
        None,
        &command_bus,
        &control_plane,
        &fleet,
        Duration::from_millis(10),
        Some(projection.clone()),
    )
    .await;
    let serialized = serde_json::to_value(report).expect("Health serializes");
    assert_eq!(serialized["redis_capability"], projection);
    let text = serialized["redis_capability"].to_string();
    for forbidden in [
        "endpoint",
        "username",
        "password",
        "query",
        "trust_root",
        "certificate",
    ] {
        assert!(!text.contains(forbidden), "projection leaked {forbidden}");
    }
}

// ---------------------------------------------------------------------------
// Docker-gated: reachable components report healthy, and the wired route serves
// the typed report fresh per request.
// ---------------------------------------------------------------------------

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
    apply_target(MigrationTarget::Conductor, &pool).await.ok()?;
    Some((container, pool))
}

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
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
    let url = format!("nats://127.0.0.1:{}", port);
    let mut client = None;
    for _ in 0..20 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((container, client?))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_postgres_reachable_row_is_healthy() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let repositories = ReadOnlyRepositoryBundle::from_postgres_pool(pool);
    let row = check_data_plane_sql(&repositories).await;
    assert_eq!(row.implementation, DataPlaneSqlImplementation::Postgres);
    assert_eq!(row.status, ComponentStatus::Healthy);
}

async fn migrated_sqlite_repository() -> (tempfile::TempDir, String, ReadOnlyRepositoryBundle) {
    let directory = tempfile::tempdir().expect("temporary SQLite directory");
    let path = directory.path().join("health.db");
    let url = format!("sqlite://{}", path.display());
    let options = sqlite_writer_options(&url, true).expect("SQLite writer options");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open SQLite migration role");
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .expect("migrate SQLite");
    pool.close().await;
    let repositories = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() })
        .open_read_only()
        .await
        .expect("open selected SQLite read-only role");
    (directory, url, repositories)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_sqlite_reports_implementation_and_schema_health() {
    let (_directory, url, repositories) = migrated_sqlite_repository().await;
    let healthy = check_data_plane_sql(&repositories).await;
    assert_eq!(healthy.implementation, DataPlaneSqlImplementation::Sqlite);
    assert_eq!(healthy.status, ComponentStatus::Healthy);

    let options = sqlite_writer_options(&url, false).expect("SQLite writer options");
    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open SQLite writer");
    sqlx::query("DROP TABLE events")
        .execute(&writer)
        .await
        .expect("make logical schema incompatible");
    writer.close().await;

    let unhealthy = check_data_plane_sql(&repositories).await;
    assert_eq!(unhealthy.status, ComponentStatus::Unhealthy);
    assert_eq!(
        unhealthy.detail,
        "repository health check failed: incompatible schema"
    );
    assert!(
        !unhealthy.detail.contains(&url),
        "failure detail must not expose the SQLite URL or path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nats_kv_reachable_row_is_healthy() {
    let Some((_nats, client)) = start_nats().await else {
        return;
    };
    // The probe bucket need not exist: a live JetStream connection with the bucket
    // merely absent still reports the KV substrate reachable.
    assert_eq!(
        check_nats_kv(&client).await.status,
        ComponentStatus::Healthy
    );
}

/// Build the api router around a curated AppState. The coordinator/MinIO stubs point
/// at dead addresses — the health handler only touches the pool and NATS client,
/// so they are never called.
async fn spawn_api(nats: async_nats::Client, pool: Arc<sqlx::PgPool>) -> String {
    let control_plane = Arc::new(
        tickr_api::http::control_plane_client::ControlPlaneClient::new(
            "http://127.0.0.1:1".to_string(),
        ),
    );
    let s3 = opendal::services::S3::default()
        .bucket("ignored")
        .endpoint("http://127.0.0.1:1")
        .access_key_id("x")
        .secret_access_key("x")
        .region("us-east-1");
    let minio = opendal::Operator::new(s3).expect("s3 stub").finish();
    let logs = Arc::new(tickr_api::http::logs_resolver::LogsResolver::new(
        minio,
        Arc::new(tickr_executor::log_stream::AllNatsLogStreamProvider::new(
            Arc::new(nats.clone()),
            Duration::from_secs(5),
        )),
    ));
    let state = tickr_api::http::routes::build_app_state(
        Arc::new(nats),
        Arc::new(
            tickr_migrations::backend::ReadOnlyRepositoryBundle::from_postgres_pool(
                pool.as_ref().clone(),
            ),
        ),
        control_plane,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endpoint_serves_typed_report_fresh_per_request() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    // A handle we can close mid-test to flip Postgres state under the running
    // handler — proving the row is recomputed, not served from a cache.
    let pool_handle = pool.clone();
    let base = spawn_api(nats, pool).await;

    // First request: every reachable row is healthy.
    let first: serde_json::Value = reqwest::get(format!("{}/api/health", base))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(first["api"]["status"], "healthy");
    assert_eq!(first["data_plane_sql"]["implementation"], "postgres");
    assert_eq!(first["data_plane_sql"]["status"], "healthy");
    assert!(first.get("postgres").is_none());
    // No executor observations are present, so the role-backed projection is
    // zero-fleet unhealthy and makes no available-capacity claim.
    assert_eq!(first["executors"]["status"], "unhealthy");
    assert_eq!(
        first["executors"]["detail"],
        "0 observed executors · no guaranteed available capacity"
    );
    // The coordinator stub points at a dead address, so the single control-plane
    // hop fails ⇒ that row is unhealthy — present in the typed report, proving
    // the wired field exists and that coordinator-unreachable ⇒ unhealthy end-to-end.
    assert_eq!(first["control_plane"]["status"], "unhealthy");
    assert!(first.get("checked_at").and_then(|v| v.as_str()).is_some());

    // Immediate re-request with no state change: behavior is identical (a Recheck
    // is byte-for-byte the same work as a normal request).
    let second: serde_json::Value = reqwest::get(format!("{}/api/health", base))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(second["api"]["status"], first["api"]["status"]);
    assert_eq!(
        second["data_plane_sql"]["status"],
        first["data_plane_sql"]["status"]
    );
    assert_eq!(second["nats_kv"]["status"], first["nats_kv"]["status"]);

    // Bracket a state change: close the pool the handler holds, then re-request.
    // No cached table => the selected SQL row now reflects the change.
    pool_handle.close().await;
    let after: serde_json::Value = reqwest::get(format!("{}/api/health", base))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(
        after["data_plane_sql"]["status"], "unhealthy",
        "a state change between requests is reflected — no cached health table"
    );

    // The top-level readiness probe is a separate surface and stays unchanged.
    let readiness: serde_json::Value = reqwest::get(format!("{}/health", base))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(readiness["status"], "ok");
}

// ---------------------------------------------------------------------------
// Executor pool: seed component-liveness keys directly (mirroring the
// task-liveness test pattern) and assert the reported pool row — N alive, summed
// slots, and the key-age-derived band. Docker-gated; skipped when NATS is down.
// ---------------------------------------------------------------------------

/// Start a NATS with per-key KV TTL support. Pins 2.14.2 — per-key TTL + delete
/// markers (`limit_markers`) need the same NATS the dev infra runs; the
/// testcontainers default tag is older.
async fn start_nats_ttl() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default()
        .with_cmd(&cmd)
        .with_tag("2.14.2")
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{}", port);
    let mut client = None;
    for _ in 0..20 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((container, client?))
}

/// Get-or-create the component-liveness bucket with the per-key-TTL config the
/// writer uses (`history: 1`, File storage, `limit_markers` on so the per-message
/// `Nats-TTL` header actually reaps the key).
async fn ensure_component_bucket(nats: &async_nats::Client) -> jetstream::kv::Store {
    let js = jetstream::new(nats.clone());
    if let Ok(store) = js.get_key_value(COMPONENT_LIVENESS_BUCKET).await {
        return store;
    }
    js.create_key_value(jetstream::kv::Config {
        bucket: COMPONENT_LIVENESS_BUCKET.to_string(),
        history: 1,
        storage: jetstream::stream::StorageType::File,
        limit_markers: Some(Duration::from_secs(60)),
        ..Default::default()
    })
    .await
    .expect("create component-liveness bucket")
}

/// Seed one `executor.<uuid>` key by publishing its `{cap, in_flight}` value with
/// a per-message `Nats-TTL` header — the exact self-reaping arm the executor does.
async fn seed_executor_key(nats: &async_nats::Client, cap: usize, in_flight: usize, ttl: Duration) {
    let js = jetstream::new(nats.clone());
    let key = component_liveness_key(Uuid::new_v4());
    let subject = format!("$KV.{COMPONENT_LIVENESS_BUCKET}.{key}");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        async_nats::header::NATS_MESSAGE_TTL,
        ttl.as_secs().to_string().as_str(),
    );
    let value = ComponentLivenessValue { cap, in_flight };
    let bytes = serde_json::to_vec(&value).expect("serialize component value");
    js.publish_with_headers(subject, headers, bytes.into())
        .await
        .expect("publish key")
        .await
        .expect("publish ack");
}

/// Count how many `executor.*` keys the bucket currently lists — used to wait for
/// seeded PUTs to become visible before asserting the row.
async fn executor_key_count(store: &jetstream::kv::Store) -> usize {
    let mut keys = store.keys().await.expect("list keys");
    let mut n = 0;
    while let Some(item) = keys.next().await {
        if let Ok(k) = item {
            if k.starts_with("executor.") {
                n += 1;
            }
        }
    }
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_pool_counts_alive_and_sums_slots() {
    let Some((_nats, nats)) = start_nats_ttl().await else {
        return;
    };
    let store = ensure_component_bucket(&nats).await;

    // A generous TTL so all three keys stay fresh (age < TTL/4) through the read —
    // the band here is exercised by the freshness case; slots are the assertion.
    let ttl = Duration::from_secs(40);
    seed_executor_key(&nats, 4, 1, ttl).await; // cap 4, in_flight 1
    seed_executor_key(&nats, 8, 3, ttl).await; // cap 8, in_flight 3
    seed_executor_key(&nats, 2, 0, ttl).await; // cap 2, in_flight 0

    // Wait for all three PUTs to be listable before asserting.
    for _ in 0..50 {
        if executor_key_count(&store).await == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let row = check_executors(&nats, ttl).await;
    assert_eq!(
        row.status,
        ComponentStatus::Healthy,
        "fresh keys present ⇒ healthy, got detail: {}",
        row.detail
    );
    // N=3 alive; used = 1+3+0 = 4; total = 4+8+2 = 14.
    assert_eq!(
        row.detail,
        "3 observed executors · observed load 4/14 configured slots"
    );
    // The detection window is derived from the liveness knob (40s here).
    assert_eq!(row.detection_window, "liveness window 40s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_pool_keeps_contradictory_capacity_observational() {
    let Some((_nats, nats)) = start_nats_ttl().await else {
        return;
    };
    let store = ensure_component_bucket(&nats).await;
    let ttl = Duration::from_secs(40);
    seed_executor_key(&nats, usize::MAX, usize::MAX, ttl).await;
    seed_executor_key(&nats, 0, usize::MAX, ttl).await;
    for _ in 0..50 {
        if executor_key_count(&store).await == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let row = check_executors(&nats, ttl).await;
    assert_eq!(row.status, ComponentStatus::Healthy);
    assert_eq!(row.observed_executors, Some(2));
    assert_eq!(row.configured_process_slots, Some(usize::MAX));
    assert_eq!(row.in_flight_count, Some(usize::MAX));
    assert_eq!(
        row.detail,
        format!(
            "2 observed executors · observed load {0}/{0} configured slots",
            usize::MAX
        )
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_pool_bands_from_key_age() {
    let Some((_nats, nats)) = start_nats_ttl().await else {
        return;
    };
    let store = ensure_component_bucket(&nats).await;

    // TTL 8s ⇒ TTL/4 = 2s. Seed one key, let it age past 2s into the slack window.
    let ttl = Duration::from_secs(8);
    seed_executor_key(&nats, 4, 0, ttl).await;
    for _ in 0..50 {
        if executor_key_count(&store).await == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Age it into `TTL/4..TTL` with none fresher ⇒ degraded.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let degraded = check_executors(&nats, ttl).await;
    assert_eq!(
        degraded.status,
        ComponentStatus::Degraded,
        "key aged into the slack window (none fresher) ⇒ degraded, got detail: {}",
        degraded.detail
    );

    // Now add a fresh key (`< TTL/4`); the freshest drives the band ⇒ healthy.
    seed_executor_key(&nats, 4, 0, ttl).await;
    for _ in 0..50 {
        if executor_key_count(&store).await == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let healthy = check_executors(&nats, ttl).await;
    assert_eq!(
        healthy.status,
        ComponentStatus::Healthy,
        "a fresh key present ⇒ healthy, got detail: {}",
        healthy.detail
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_pool_zero_fleet_is_unhealthy() {
    let Some((_nats, nats)) = start_nats_ttl().await else {
        return;
    };
    let store = ensure_component_bucket(&nats).await;

    // Empty bucket ⇒ zero fleet ⇒ unhealthy.
    let empty = check_executors(&nats, Duration::from_secs(8)).await;
    assert_eq!(empty.status, ComponentStatus::Unhealthy);
    assert_eq!(
        empty.detail,
        "0 observed executors · observed load 0/0 configured slots"
    );

    // Seed a short-TTL key, then let it expire (self-reaping): the row returns to
    // unhealthy with no reaper, proving an expired key is simply not counted.
    let ttl = Duration::from_secs(2);
    seed_executor_key(&nats, 4, 1, ttl).await;
    for _ in 0..50 {
        if executor_key_count(&store).await == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await; // > TTL: key self-reaps.
    let expired = check_executors(&nats, ttl).await;
    assert_eq!(
        expired.status,
        ComponentStatus::Unhealthy,
        "all executor keys expired ⇒ unhealthy, got detail: {}",
        expired.detail
    );
    assert_eq!(
        expired.detail,
        "0 observed executors · observed load 0/0 configured slots"
    );
}

// ---------------------------------------------------------------------------
// Conductor row: a command-plane-responsive check over the api→conductor
// command bus. Docker-gated (NATS); skipped when unavailable.
//   * real command consumer up          -> Ping answered -> healthy
//   * broker up but no command consumer  -> NoResponders  -> unhealthy
// ---------------------------------------------------------------------------

/// Start the real conductor api-commands subscriber against `nats`. The Ping
/// dispatch arm never touches Postgres, so a lazy pool at a dead port is enough
/// state to construct the subscriber. Returns the cancellation token.
async fn spawn_command_consumer(nats: &async_nats::Client) -> CancellationToken {
    // A lazy pool is never connected — dispatch_ping does no DB work.
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
        .expect("lazy pool builds");
    let pool = Arc::new(pool);
    let definition_repository = Arc::new(
        tickr_migrations::backend::WriterRepositoryBundle::from_postgres_pool(
            pool.as_ref().clone(),
        ),
    );
    let cancel = CancellationToken::new();
    let state = tickr_conductor::api_commands_consumer::ApiCommandsState {
        definition_repository,
        nats: nats.clone(),
        signal_applied_notifications:
            tickr_conductor::signal_applied_notifier::all_nats_signal_applied_notifications(
                nats.clone(),
            )
            .await
            .unwrap()
            .reconciliation(),
        relay_sender: Arc::new(tickr_conductor::wakeup_translator::DefaultRelaySender),
        patch_relay_sender: Arc::new(tickr_conductor::patch_pipeline::DefaultPatchRelaySender),
        gate_index: tickr_conductor::gate_index_lifecycle::gate_index(),
    };
    let token = cancel.clone();
    tokio::spawn(async move {
        let _ = tickr_conductor::api_commands_consumer::start(state, token).await;
    });
    // Give the queue-group subscription time to register before pinging.
    tokio::time::sleep(Duration::from_millis(800)).await;
    cancel
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conductor_row_healthy_when_consumer_answers() {
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let cancel = spawn_command_consumer(&nats).await;

    // Low level: the Ping dispatch arm replies 200 with the Ping payload — proving
    // the new proto variant is genuinely handled (not the unsupported fallthrough).
    let ping = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Ping(api::PingRequest {})),
    };
    let resp = send_command(&nats, ping, Duration::from_secs(5))
        .await
        .expect("consumer replies to Ping");
    assert_eq!(resp.status_code, 200, "Ping acks with 200");
    assert!(
        matches!(
            resp.payload,
            Some(api::api_command_response::Payload::Ping(_))
        ),
        "Ping reply carries the side-effect-free Ping payload"
    );

    // High level: the health row maps the answered Ping to healthy.
    let row = check_conductor(&nats, Duration::from_secs(5)).await;
    assert_eq!(
        row.status,
        ComponentStatus::Healthy,
        "command consumer answered ⇒ healthy, got detail: {}",
        row.detail
    );

    cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conductor_row_unhealthy_when_consumer_absent_but_broker_connected() {
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    // Broker link is up, but no command consumer is bound: NATS reports no
    // responders for the subject. The row must read unhealthy even though the
    // broker connection itself is healthy — the honest command-plane signal.
    assert_eq!(
        nats.connection_state(),
        async_nats::connection::State::Connected,
        "precondition: the broker link is connected"
    );
    let row = check_conductor(&nats, Duration::from_secs(2)).await;
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "no command consumer (NoResponders) while broker up ⇒ unhealthy, got detail: {}",
        row.detail
    );
}

struct UnusedLocalLogs;

#[async_trait::async_trait]
impl LocalTaskLogStore for UnusedLocalLogs {
    async fn fetch_task_logs(
        &self,
        _workflow_id: Uuid,
        _workflow_instance_id: Uuid,
        _task_instance_id: Uuid,
    ) -> Result<TaskLogs, LogsError> {
        Err(LogsError::NotFound)
    }

    async fn fetch_batches_after(
        &self,
        _workflow_id: Uuid,
        _workflow_instance_id: Uuid,
        _task_instance_id: Uuid,
        _after_seq: u64,
    ) -> Result<LogBatchPage, LogsError> {
        Err(LogsError::NotFound)
    }

    async fn fetch_tail(
        &self,
        _workflow_id: Uuid,
        _workflow_instance_id: Uuid,
        _task_instance_id: Uuid,
        _tail: usize,
        _before_seq: Option<u64>,
    ) -> Result<LogBatchPage, LogsError> {
        Err(LogsError::NotFound)
    }
}

fn lite_formation_health() -> ResolvedFormationHealth {
    let roles = [
        (
            HealthCoordinationRole::CommandBus,
            HealthRoleImplementation::LocalRequestReply,
            "tickr.command-bus.local-request-reply",
        ),
        (
            HealthCoordinationRole::TaskDispatch,
            HealthRoleImplementation::LocalSqlite,
            "tickr.task-dispatch.sqlite",
        ),
        (
            HealthCoordinationRole::TaskEvents,
            HealthRoleImplementation::LocalSqlite,
            "tickr.task-events.sqlite",
        ),
        (
            HealthCoordinationRole::TaskCancellation,
            HealthRoleImplementation::LocalSqlite,
            "tickr.task-cancellation.sqlite",
        ),
        (
            HealthCoordinationRole::CompactionStaging,
            HealthRoleImplementation::LocalSqlite,
            "tickr.compaction-staging.sqlite",
        ),
        (
            HealthCoordinationRole::LifecycleWork,
            HealthRoleImplementation::LocalSqlite,
            "tickr.lifecycle-work.sqlite",
        ),
        (
            HealthCoordinationRole::LogStaging,
            HealthRoleImplementation::LocalJournal,
            "tickr.log-staging.local-journal",
        ),
        (
            HealthCoordinationRole::ScopeStore,
            HealthRoleImplementation::LocalSqlite,
            "tickr.scope-store.sqlite",
        ),
        (
            HealthCoordinationRole::IngressIdempotencyStore,
            HealthRoleImplementation::Disabled,
            "tickr.ingress-idempotency.disabled",
        ),
        (
            HealthCoordinationRole::LivenessWatchdog,
            HealthRoleImplementation::LocalSqlite,
            "tickr.liveness-watchdog.sqlite",
        ),
        (
            HealthCoordinationRole::SignalAppliedNotifier,
            HealthRoleImplementation::LocalNotification,
            "tickr.signal-applied.local-notification",
        ),
        (
            HealthCoordinationRole::ExecutorFleetStatus,
            HealthRoleImplementation::LocalObservation,
            "tickr.executor-fleet-status.local-observation",
        ),
        (
            HealthCoordinationRole::EventIngress,
            HealthRoleImplementation::Disabled,
            "tickr.event-ingress.disabled",
        ),
    ]
    .into_iter()
    .map(|(role, implementation, name)| HealthResolvedRole {
        role,
        implementation,
        protocol: HealthProtocolIdentity {
            name: name.to_string(),
            version: 1,
        },
    })
    .collect();
    ResolvedFormationHealth {
        profile: HealthFormationProfile::TickrLite,
        topology: HealthFormationTopology::SingleNode,
        sql: DataPlaneSqlImplementation::Sqlite,
        final_logs: HealthFinalLogStore::LocalFiles,
        writer_topology: HealthWriterTopology::ConductorOwned,
        executor_count: 1,
        substrates: HealthSubstrateSelection {
            sqlite: true,
            postgres: false,
            nats: false,
            redis: false,
            object_store: false,
        },
        roles,
    }
}

async fn spawn_mutable_control_plane() -> (String, Arc<AtomicU8>) {
    let state = Arc::new(AtomicU8::new(0));
    let app = axum::Router::new().route(
        "/api/internal/health",
        axum::routing::get({
            let state = state.clone();
            move || {
                let state = state.clone();
                async move {
                    let status = match state.load(Ordering::Acquire) {
                        0 => "healthy",
                        1 => "degraded",
                        _ => "unhealthy",
                    };
                    axum::Json(serde_json::json!({ "status": status }))
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind mutable control plane");
    let address = listener
        .local_addr()
        .expect("mutable control-plane address");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}"), state)
}
fn test_console_asset(path: &str) -> Option<ConsoleAsset> {
    match path {
        "index.html" => Some(ConsoleAsset::new(
            b"<!doctype html><title>Tickr test Console</title>",
            "text/html; charset=utf-8",
        )),
        "favicon.svg" => Some(ConsoleAsset::new(
            b"<svg><path data-mark=\"concentric\"/></svg>",
            "image/svg+xml",
        )),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_lite_health_reports_ready_degraded_unready_failure_and_reconnect_states() {
    let (directory, _url, repositories) = migrated_sqlite_repository().await;
    let (control_plane_url, control_plane_state) = spawn_mutable_control_plane().await;
    let control_plane = Arc::new(ControlPlaneClient::new(control_plane_url));
    let (command_bus, command_writer) = CommandBus::local(LocalCommandBusConfig::default());
    let command_cancel = CancellationToken::new();
    let command_task = tokio::spawn(command_writer.run(
        command_cancel.clone(),
        |bytes| async move {
            let request = api::ApiCommandRequest::decode(bytes.as_slice()).unwrap();
            assert!(matches!(
                request.body,
                Some(api::api_command_request::Body::Ping(_))
            ));
            api::ApiCommandResponse {
                status_code: 200,
                payload: Some(api::api_command_response::Payload::Ping(
                    api::PingPayload {},
                )),
            }
            .encode_to_vec()
        },
    ));
    let ready = Arc::new(AtomicBool::new(false));
    let fleet = LocalExecutorCapacity::new(
        Uuid::new_v4(),
        NonZeroUsize::new(4).expect("non-zero test capacity"),
    )
    .observation();
    let state = tickr_api::http::routes::build_lite_app_state(
        command_bus,
        Arc::new(repositories),
        control_plane,
        Arc::new(LogsResolver::local(Arc::new(UnusedLocalLogs))),
        ready.clone(),
        test_console_asset,
        fleet,
        lite_formation_health(),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind Lite health API");
    let address = listener.local_addr().unwrap();
    let api_task = tokio::spawn(async move {
        axum::serve(listener, tickr_api::http::routes::build_lite_router(state))
            .await
            .unwrap()
    });
    let client = reqwest::Client::new();
    let base = format!("http://{address}");

    let unready = client.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(unready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let unready_report: serde_json::Value = client
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(unready_report["formation"]["profile"], "tickr_lite");
    assert_eq!(unready_report["formation"]["status"], "unhealthy");
    assert_eq!(unready_report["readiness"]["ready"], false);
    assert_eq!(unready_report["nats_kv"]["status"], "degraded");
    assert_eq!(unready_report["formation"]["substrates"]["nats"], false);
    assert_eq!(unready_report["formation"]["substrates"]["redis"], false);
    assert_eq!(unready_report["formation"]["substrates"]["postgres"], false);
    assert_eq!(
        unready_report["formation"]["substrates"]["object_store"],
        false
    );

    ready.store(true, Ordering::Release);
    let ready_response = client.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(ready_response.status(), reqwest::StatusCode::OK);
    let index = client.get(&base).send().await.unwrap();
    assert_eq!(index.status(), reqwest::StatusCode::OK);
    assert_eq!(
        index.text().await.unwrap(),
        "<!doctype html><title>Tickr test Console</title>"
    );
    let favicon = client
        .get(format!("{base}/favicon.svg"))
        .send()
        .await
        .unwrap();
    assert_eq!(favicon.status(), reqwest::StatusCode::OK);
    assert_eq!(
        favicon.headers()[reqwest::header::CONTENT_TYPE],
        "image/svg+xml"
    );
    let spa = client
        .get(format!("{base}/workflows/example"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        spa.text().await.unwrap(),
        "<!doctype html><title>Tickr test Console</title>"
    );
    let healthy: serde_json::Value = client
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(healthy["formation"]["status"], "healthy");
    assert_eq!(healthy["local_coordination"]["status"], "healthy");
    assert_eq!(
        healthy["command_path"]["implementation"],
        "local_request_reply"
    );
    assert_eq!(
        healthy["command_path"]["protocol"]["name"],
        "tickr.command-bus.local-request-reply"
    );
    assert_eq!(healthy["executors"]["observed_executors"], 1);
    assert_eq!(healthy["executors"]["configured_process_slots"], 4);
    assert_eq!(healthy["executors"]["in_flight_count"], 0);
    assert_eq!(healthy["formation"]["roles"].as_array().unwrap().len(), 13);

    control_plane_state.store(1, Ordering::Release);
    let degraded: serde_json::Value = client
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(degraded["control_plane"]["status"], "degraded");
    assert_eq!(degraded["readiness"]["ready"], true);

    control_plane_state.store(2, Ordering::Release);
    let lost: serde_json::Value = client
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(lost["control_plane"]["status"], "unhealthy");
    assert_eq!(lost["readiness"]["ready"], true);
    assert_eq!(lost["data_plane_sql"]["status"], "healthy");

    control_plane_state.store(0, Ordering::Release);
    let reconnected: serde_json::Value = client
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reconnected["control_plane"]["status"], "healthy");
    assert_eq!(reconnected["readiness"]["ready"], true);

    // This is the same shared transition the critical-child path performs
    // before cancelling siblings; diagnostics remain available during teardown.
    ready.store(false, Ordering::Release);
    let failed: serde_json::Value = client
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(failed["readiness"]["ready"], false);
    assert_eq!(failed["formation"]["status"], "unhealthy");
    let blocked = client
        .get(format!("{base}/api/workflows"))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    command_cancel.cancel();
    command_task.await.unwrap();
    api_task.abort();
}
