//! End-to-end test for `POST /api/workflows/{id}/trigger` over the command
//! bus. Inserts crafted workflow rows directly (no Nickel dependency), drives
//! triggers through the API HTTP server, and asserts status + body match the
//! conductor's HTTP handler across all seven outcomes:
//! Fresh / Deduplicated / Conflict / WorkflowNotFound / InputsProvidedButNoCaptures
//! / CapturesExtractionFailed / relay-unreachable.
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
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use tickr_conductor::api_commands_consumer::{start, ApiCommandsState};
use tickr_conductor::gate_index_lifecycle::gate_index;
use tickr_conductor::wakeup_translator::DefaultRelaySender;

// The in-process conductor exposes one relay sender per process; serialize
// these integration cases so the deliberately uninitialized-relay case cannot
// observe another case's sender.
static RELAY_TEST_LOCK: Mutex<()> = Mutex::const_new(());

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

/// Insert a published workflow-definition fixture at `Ready`.
async fn insert_workflow(pool: &sqlx::PgPool, workflow: &wf::WorkflowDefinition) {
    let definition = serde_json::to_value(workflow).expect("serialize workflow definition");
    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, name, status, definition, nickel_source, namespace, slug, content_hash, cosmetic_hash)
        VALUES ($1, $2, $3, 'Ready', $4, '', $5, $6, 'testhash', 'testcos')
        "#,
    )
    .bind(Uuid::parse_str(&workflow.id).expect("workflow id"))
    .bind(workflow.version)
    .bind(&workflow.name)
    .bind(&definition)
    .bind(&workflow.namespace)
    .bind(&workflow.slug)
    .execute(pool)
    .await
    .expect("insert workflow row");
}

fn bare_workflow() -> wf::WorkflowDefinition {
    wf::WorkflowDefinition {
        id: Uuid::new_v4().to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "bare".to_string(),
        name: "bare".to_string(),
        trigger: Some(wf::Trigger {
            kind: Some(wf::trigger::Kind::FireNow(wf::trigger::FireNow {})),
        }),
        ..Default::default()
    }
}

/// Insert a workflow declaring a single trigger capture with the given
/// JSONPath. The declaration is authored directly on the published definition
/// shape the parser writes. Returns the id.
async fn insert_capture_workflow(pool: &sqlx::PgPool, jsonpath: &str) -> Uuid {
    let mut proto = bare_workflow();
    proto.name = "captured".to_string();
    proto.slug = "captured".to_string();
    let id = Uuid::parse_str(&proto.id).expect("workflow id");
    let version = proto.version;
    proto.captures = vec![tickr_proto::workflow::CaptureDeclaration {
        name: "v".to_string(),
        from: Some(tickr_proto::workflow::CaptureSource {
            source: Some(tickr_proto::workflow::capture_source::Source::Trigger(
                tickr_proto::workflow::capture_source::Trigger {
                    jsonpath: jsonpath.to_string(),
                },
            )),
        }),
    }];
    let definition = serde_json::to_value(&proto).expect("serialize proto definition");
    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, name, status, definition, nickel_source, namespace, slug, content_hash, cosmetic_hash)
        VALUES ($1, $2, $3, 'Ready', $4, '', $5, $6, 'testhash', 'testcos')
        "#,
    )
    .bind(id)
    .bind(&version)
    .bind("captured")
    .bind(&definition)
    .bind(&proto.namespace)
    .bind(&proto.slug)
    .execute(pool)
    .await
    .expect("insert capture workflow row");
    id
}

/// Spawn the conductor subscriber. When `init_relay` is set, the global relay
/// channel is wired to a drained receiver so `Fresh` forwards succeed; when
/// unset, a `Fresh` forward fails with "relay not initialized" -> 503.
async fn spawn_subscriber(
    nats: NatsClient,
    pool: Arc<sqlx::PgPool>,
    init_relay: bool,
) -> CancellationToken {
    if init_relay {
        let (tx, mut rx) = mpsc::channel::<ConductorRelayMessage>(64);
        tickr_conductor::relay::init_relay_tx(tx).await;
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
    }
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
    let control_plane = Arc::new(
        tickr_api::http::control_plane_client::ControlPlaneClient::new(
            "http://127.0.0.1:1".to_string(),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            true,
        )
        .unwrap(),
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

/// Bring up PG + NATS + subscriber + API. Returns None to skip when Docker is
/// unavailable. `_pg` / `_nats` containers are kept alive by the caller.
async fn harness(
    init_relay: bool,
) -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    sqlx::PgPool,
    String,
    CancellationToken,
)> {
    let (pg, pool) = start_postgres().await?;
    let (nats_c, nats) = start_nats().await?;
    let pool = Arc::new(pool);
    let cancel = spawn_subscriber(nats.clone(), Arc::clone(&pool), init_relay).await;
    let base = spawn_api(nats, Arc::clone(&pool)).await;
    Some((pg, nats_c, (*pool).clone(), base, cancel))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_fresh_returns_200() {
    let _relay_lock = RELAY_TEST_LOCK.lock().await;
    let Some((_pg, _nats, pool, base, _cancel)) = harness(true).await else {
        return;
    };
    let wf = bare_workflow();
    insert_workflow(&pool, &wf).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/workflows/{}/trigger", base, wf.id))
        .json(&json!({}))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["deduplicated"].as_bool(), Some(false));
    assert!(body["signal_id"].as_str().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_workflow_not_found_returns_404() {
    let _relay_lock = RELAY_TEST_LOCK.lock().await;
    let Some((_pg, _nats, _pool, base, _cancel)) = harness(true).await else {
        return;
    };
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/workflows/{}/trigger", base, Uuid::new_v4()))
        .json(&json!({}))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 404);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"].as_str(), Some("workflow not found"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_inputs_without_captures_returns_400() {
    let _relay_lock = RELAY_TEST_LOCK.lock().await;
    let Some((_pg, _nats, pool, base, _cancel)) = harness(true).await else {
        return;
    };
    let wf = bare_workflow();
    insert_workflow(&pool, &wf).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/workflows/{}/trigger", base, wf.id))
        .json(&json!({ "inputs": { "anything": 1 } }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert!(body["error"]
        .as_str()
        .unwrap_or_default()
        .contains("declares no captures"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_malformed_capture_jsonpath_returns_400() {
    let _relay_lock = RELAY_TEST_LOCK.lock().await;
    let Some((_pg, _nats, pool, base, _cancel)) = harness(true).await else {
        return;
    };
    // A capture whose JSONPath can't parse — reachable only via a persisted
    // definition the registration validator never blessed, crafted here
    // directly. The extraction step fails with CapturesExtractionFailed.
    let wid = insert_capture_workflow(&pool, "$[").await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/workflows/{}/trigger", base, wid))
        .json(&json!({}))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert!(body["error"]
        .as_str()
        .unwrap_or_default()
        .contains("failed to apply"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_dedup_and_conflict_on_idempotency_key() {
    let _relay_lock = RELAY_TEST_LOCK.lock().await;
    let Some((_pg, _nats, pool, base, _cancel)) = harness(true).await else {
        return;
    };
    let wid = insert_capture_workflow(&pool, "$.k").await;
    let client = reqwest::Client::new();
    let url = format!("{}/api/workflows/{}/trigger", base, wid);
    let key = "trigger-idem-key";

    // First: Fresh.
    let first = client
        .post(&url)
        .header("Idempotency-Key", key)
        .json(&json!({ "inputs": { "k": 1 } }))
        .send()
        .await
        .expect("first");
    assert_eq!(first.status(), 200);
    let first_body: serde_json::Value = first.json().await.expect("json");
    assert_eq!(first_body["deduplicated"].as_bool(), Some(false));
    let first_sid = first_body["signal_id"].as_str().unwrap().to_string();

    // Same key + same payload: Deduplicated, original signal_id, no re-forward.
    let second = client
        .post(&url)
        .header("Idempotency-Key", key)
        .json(&json!({ "inputs": { "k": 1 } }))
        .send()
        .await
        .expect("second");
    assert_eq!(second.status(), 200);
    let second_body: serde_json::Value = second.json().await.expect("json");
    assert_eq!(second_body["deduplicated"].as_bool(), Some(true));
    assert_eq!(second_body["signal_id"].as_str(), Some(first_sid.as_str()));

    // Same key + different payload: Conflict (409) with both hashes.
    let third = client
        .post(&url)
        .header("Idempotency-Key", key)
        .json(&json!({ "inputs": { "k": 2 } }))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_relay_unreachable_returns_503() {
    let _relay_lock = RELAY_TEST_LOCK.lock().await;
    // Relay channel deliberately left uninitialized: the Fresh forward fails.
    let Some((_pg, _nats, pool, base, _cancel)) = harness(false).await else {
        return;
    };
    let wf = bare_workflow();
    insert_workflow(&pool, &wf).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/workflows/{}/trigger", base, wf.id))
        .json(&json!({}))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "internal server error");
}
