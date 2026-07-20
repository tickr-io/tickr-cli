//! Behavioral tests for the operator health surface (`GET /api/health`).
//!
//! The endpoint computes each component row **fresh per request** from a cheap
//! check — no cached health table. Tests drive a component into a known state and
//! assert the *reported row*, never a private field:
//!
//! - deterministic, no-Docker: API-self is always healthy; a closed/unreachable
//!   Postgres pool reports `unhealthy`; an unreachable NATS client reports the KV
//!   row `unhealthy`; the serialized shape carries `checked_at` + lowercase status.
//! - Docker-gated (testcontainers, skipped when unavailable): a reachable Postgres
//!   and NATS report `healthy`, and the wired `/api/health` route serves the typed
//!   report and reflects a mid-flight state change (proving no cached table) while
//!   the top-level `/health` readiness probe stays `{"status":"ok"}`.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use futures::StreamExt as _;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_api::commands::client::send_command;
use tickr_api::http::coordinator_client::CoordinatorClient;
use tickr_api::http::health::{
    api_self, build_health_report, check_conductor, check_control_plane, check_executors,
    check_nats_kv, check_postgres, ComponentStatus,
};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_down_row_is_unhealthy() {
    // Lazy pool at a dead port: no connection is made until the query runs, at
    // which point `SELECT 1` fails within the acquire timeout -> unhealthy.
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/postgres")
        .expect("lazy pool builds");
    let row = check_postgres(&pool).await;
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "unreachable Postgres must report unhealthy, got detail: {}",
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
    // zero-fleet unhealthy, with the "0 alive · 0/0 slots" detail. The read never
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
    assert_eq!(row.detail, "0 alive · 0/0 slots");
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

/// A fake coordinator serving `/api/internal/health` with a fixed rollup body —
/// stands in for the real coordinator so the Control-plane hop is exercised with no
/// control plane behind it. Mirrors the fake-coordinator helpers the other api tests
/// use (e.g. `dashboard_clock_test::spawn_fake_coordinator`).
async fn spawn_fake_control_plane(status: &'static str) -> String {
    let app = axum::Router::new().route(
        "/api/internal/health",
        axum::routing::get(
            move || async move { axum::Json(serde_json::json!({ "status": status })) },
        ),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind fake coordinator");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap_or(()) });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_unreachable_coordinator_row_is_unhealthy() {
    // The coordinator is the only path the UI reaches control plane through, so the
    // control plane is one rollup reached via one HTTP hop — and an unreachable
    // coordinator ⇒ the row is unhealthy, mirroring the coordinator's own
    // live-store-unreachable degrade path. (Prior art: the coordinator-unreachable
    // degrade tests, e.g. dashboard_clock_test::unreachable_coordinator_errors...)
    let coordinator = CoordinatorClient::new("http://127.0.0.1:1".to_string());
    let row = check_control_plane(&coordinator).await;
    assert_eq!(
        row.status,
        ComponentStatus::Unhealthy,
        "unreachable coordinator ⇒ control-plane row unhealthy, got detail: {}",
        row.detail
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_healthy_when_coordinator_rollup_healthy() {
    // A reachable coordinator returning a healthy rollup ⇒ the Control-plane row is
    // healthy. Exactly one HTTP hop to /api/internal/health; the rollup is
    // reported as one row, never split per-component.
    let base = spawn_fake_control_plane("healthy").await;
    let coordinator = CoordinatorClient::new(base);
    let row = check_control_plane(&coordinator).await;
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
    let coordinator = CoordinatorClient::new("http://127.0.0.1:1".to_string());

    let report = build_health_report(&pool, &client, &coordinator, Duration::from_secs(1)).await;
    let json = serde_json::to_value(&report).expect("serializes");

    assert!(
        json.get("checked_at").and_then(|v| v.as_str()).is_some(),
        "report carries a global checked_at string"
    );
    for row in [
        "api",
        "postgres",
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
    assert_eq!(
        json["api"]["status"], "healthy",
        "API-self is always healthy"
    );
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
async fn postgres_reachable_row_is_healthy() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    assert_eq!(check_postgres(&pool).await.status, ComponentStatus::Healthy);
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
    assert_eq!(first["postgres"]["status"], "healthy");
    assert_eq!(first["nats_kv"]["status"], "healthy");
    // No executor keys are seeded here, so the pool row is zero-fleet unhealthy —
    // present in the typed report with its detail, proving the wired field exists.
    assert_eq!(first["executors"]["status"], "unhealthy");
    assert_eq!(first["executors"]["detail"], "0 alive · 0/0 slots");
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
    assert_eq!(second["postgres"]["status"], first["postgres"]["status"]);
    assert_eq!(second["nats_kv"]["status"], first["nats_kv"]["status"]);

    // Bracket a state change: close the pool the handler holds, then re-request.
    // No cached table => the Postgres row now reflects the change.
    pool_handle.close().await;
    let after: serde_json::Value = reqwest::get(format!("{}/api/health", base))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(
        after["postgres"]["status"], "unhealthy",
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
    assert_eq!(row.detail, "3 alive · 4/14 slots");
    // The detection window is derived from the liveness knob (40s here).
    assert_eq!(row.detection_window, "liveness window 40s");
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
    assert_eq!(empty.detail, "0 alive · 0/0 slots");

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
    assert_eq!(expired.detail, "0 alive · 0/0 slots");
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
    let cancel = CancellationToken::new();
    let state = tickr_conductor::api_commands_consumer::ApiCommandsState {
        pg_pool: Arc::new(pool),
        nats: nats.clone(),
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
