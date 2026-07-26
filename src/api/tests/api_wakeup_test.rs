//! End-to-end test for `POST /api/signals/wakeup` over the command bus.
//! Registers a real `waits-on-signal` subscriber into the conductor's
//! subscription index (so the matched-workflow path exercises real
//! translation), drives wakeups through the API, and asserts status + body
//! match the conductor's HTTP handler.
//!
//! Requires Docker (testcontainers Postgres + NATS). Skipped automatically
//! when unavailable.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_proto::workflow as wf;
use tickr_proto::ConductorRelayMessage;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use tickr_conductor::api_commands_consumer::{start, ApiCommandsState};
use tickr_conductor::gate_index_lifecycle::gate_index;
use tickr_conductor::waits_on_signal_lifecycle::{apply_workflow_state, signal_subscription_index};
use tickr_conductor::wakeup_translator::DefaultRelaySender;

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
    let url = format!("nats://127.0.0.1:{}", port);
    let mut client = None;
    for _ in 0..20 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((container, client.expect("nats connect")))
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

async fn spawn_subscriber(nats: NatsClient, pool: Arc<sqlx::PgPool>) -> CancellationToken {
    // Wire a drained relay so the synthesized Trigger fan-out forwards cleanly.
    let (tx, mut rx) = mpsc::channel::<ConductorRelayMessage>(64);
    tickr_conductor::relay::init_relay_tx(tx).await;
    tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let cancel = CancellationToken::new();
    let definition_repository = Arc::new(
        tickr_migrations::backend::WriterRepositoryBundle::from_postgres_pool(
            pool.as_ref().clone(),
        ),
    );
    let state = ApiCommandsState {
        definition_repository,
        nats: nats.clone(),
        signal_applied_notifications:
            tickr_conductor::signal_applied_notifier::all_nats_signal_applied_notifications(
                nats.clone(),
            )
            .await
            .unwrap()
            .reconciliation(),
        relay_sender: Arc::new(DefaultRelaySender),
        patch_relay_sender: Arc::new(tickr_conductor::patch_pipeline::DefaultPatchRelaySender),
        gate_index: gate_index(),
    };
    let token = cancel.clone();
    tokio::spawn(async move {
        let _ = start(state, token).await;
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    cancel
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

async fn harness() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    String,
    CancellationToken,
)> {
    let (pg, pool) = start_postgres().await?;
    let (nats_c, nats) = start_nats().await?;
    let pool = Arc::new(pool);
    let cancel = spawn_subscriber(nats.clone(), Arc::clone(&pool)).await;
    let base = spawn_api(nats, pool).await;
    Some((pg, nats_c, base, cancel))
}

/// Register a no-capture, no-predicate `waits-on-signal` subscriber on
/// `signal_name` into the process-wide subscription index the in-process
/// conductor subscriber reads. Returns the workflow id for cleanup.
fn register_subscriber(signal_name: &str) -> Uuid {
    let id = Uuid::new_v4();
    let workflow = wf::WorkflowDefinition {
        id: id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "sub".to_string(),
        name: "sub".to_string(),
        trigger: Some(wf::Trigger {
            kind: Some(wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
                signal_name: signal_name.to_string(),
                predicate: None,
                captures: vec![],
            })),
        }),
        ..Default::default()
    };
    apply_workflow_state(&workflow).expect("apply state into subscription index");
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_matched_subscriber_returns_200_with_count() {
    let Some((_pg, _nats, base, _cancel)) = harness().await else {
        return;
    };
    let signal_name = format!("user-paid-{}", Uuid::new_v4());
    let sub = register_subscriber(&signal_name);
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/signals/wakeup", base))
        .json(&json!({ "name": signal_name, "payload": { "x": 1 } }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["matched_workflows"].as_u64(), Some(1));
    assert_eq!(body["gates_matched"].as_u64(), Some(0));
    assert!(body["deduplicated"].is_null());
    assert!(body["signal_id"].as_str().is_some());

    signal_subscription_index().unregister(sub);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_with_no_subscribers_returns_200_with_zero_counts() {
    let Some((_pg, _nats, base, _cancel)) = harness().await else {
        return;
    };
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/signals/wakeup", base))
        .json(&json!({ "name": format!("nobody-{}", Uuid::new_v4()) }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["matched_workflows"].as_u64(), Some(0));
    assert_eq!(body["gates_matched"].as_u64(), Some(0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_missing_name_returns_400() {
    let Some((_pg, _nats, base, _cancel)) = harness().await else {
        return;
    };
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/signals/wakeup", base))
        .json(&json!({ "payload": { "x": 1 } }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"].as_str(), Some("missing `name`"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_dedup_and_conflict_on_idempotency_key() {
    let Some((_pg, _nats, base, _cancel)) = harness().await else {
        return;
    };
    let client = reqwest::Client::new();
    let name = format!("evt-{}", Uuid::new_v4());
    let url = format!("{}/api/signals/wakeup", base);
    let key = "wakeup-idem-key";

    // First: Fresh (no subscribers, zero counts).
    let first = client
        .post(&url)
        .header("Idempotency-Key", key)
        .json(&json!({ "name": name, "payload": { "v": 1 } }))
        .send()
        .await
        .expect("first");
    assert_eq!(first.status(), 200);
    let first_body: serde_json::Value = first.json().await.expect("json");
    assert!(first_body["deduplicated"].is_null());
    let first_sid = first_body["signal_id"].as_str().unwrap().to_string();

    // Same key + same payload: Deduplicated.
    let second = client
        .post(&url)
        .header("Idempotency-Key", key)
        .json(&json!({ "name": name, "payload": { "v": 1 } }))
        .send()
        .await
        .expect("second");
    assert_eq!(second.status(), 200);
    let second_body: serde_json::Value = second.json().await.expect("json");
    assert_eq!(second_body["deduplicated"].as_bool(), Some(true));
    assert_eq!(second_body["signal_id"].as_str(), Some(first_sid.as_str()));

    // Same key + different payload: Conflict (409).
    let third = client
        .post(&url)
        .header("Idempotency-Key", key)
        .json(&json!({ "name": name, "payload": { "v": 2 } }))
        .send()
        .await
        .expect("third");
    assert_eq!(third.status(), 409);
    let third_body: serde_json::Value = third.json().await.expect("json");
    assert_eq!(
        third_body["original_signal_id"].as_str(),
        Some(first_sid.as_str())
    );
    assert!(third_body["original_input_hash"].as_str().is_some());
    assert!(third_body["your_input_hash"].as_str().is_some());
}
