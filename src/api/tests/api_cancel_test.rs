//! End-to-end test for the cancel routes over the command bus: the canonical
//! `POST /api/signals/cancel` plus the two path-encoded sugar routes. Stands up
//! a real conductor subscriber, emulates durable server materialization, and
//! asserts status + body match the conductor's HTTP handler across every
//! outcome.
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
use tickr_proto::codec::signal::{cancel_instance_target, decode_signal};
use tickr_proto::signal as sp;
use tickr_proto::{ConductorRelayMessage, EntityType};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use tickr_conductor::api_commands_consumer::{start, ApiCommandsState};
use tickr_conductor::gate_index_lifecycle::gate_index;
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

/// Spawn the conductor subscriber. When `init_relay` is set, the global relay
/// channel is wired and the receiver is returned so the test can read what the
/// conductor forwards and emulate the server's relay-back. When unset, the
/// relay is left uninitialized so a forward fails -> 503.
async fn spawn_subscriber(
    nats: NatsClient,
    pool: Arc<sqlx::PgPool>,
    init_relay: bool,
) -> (
    CancellationToken,
    Option<mpsc::Receiver<ConductorRelayMessage>>,
) {
    let rx = if init_relay {
        let (tx, rx) = mpsc::channel::<ConductorRelayMessage>(64);
        tickr_conductor::relay::init_relay_tx(tx).await;
        Some(rx)
    } else {
        None
    };
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
    (cancel, rx)
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

/// Read the next forwarded `Signal` off the relay and return it decoded.
async fn next_signal(rx: &mut mpsc::Receiver<ConductorRelayMessage>) -> sp::Signal {
    let msg = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("relay receives forwarded signal")
        .expect("relay channel open");
    assert_eq!(msg.entity_type, EntityType::Signal as i32);
    decode_signal(&msg.payload).expect("decode published Signal")
}

fn assert_user_instance_cancel(
    signal: &sp::Signal,
    workflow_instance_id: Uuid,
    node_id: Option<Uuid>,
) {
    let instance = cancel_instance_target(signal).expect("instance Cancel target");
    assert_eq!(
        instance.workflow_instance_id,
        workflow_instance_id.to_string()
    );
    assert_eq!(instance.node_id, node_id.map(|id| id.to_string()));
    let Some(sp::signal::Variant::Cancel(cancel)) = signal.variant.as_ref() else {
        panic!("expected Cancel variant");
    };
    assert!(matches!(
        cancel.reason.as_ref().and_then(|reason| reason.reason.as_ref()),
        Some(sp::cancel_reason::Reason::UserRequested(user)) if user.actor.is_none()
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn instance_cancel_returns_applied_true() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    let (_cancel, mut rx) = spawn_subscriber(nats.clone(), Arc::clone(&pool), true).await;
    let rx = rx.take().expect("relay rx");
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();

    let instance_id = Uuid::new_v4();
    // Instance target has no relay-back; drain the forwarded signal.
    let drain = tokio::spawn(async move {
        let mut rx = rx;
        next_signal(&mut rx).await
    });
    let resp = client
        .post(format!("{}/api/signals/cancel", base))
        .json(&json!({ "target": { "kind": "instance", "workflow_instance_id": instance_id } }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["applied"].as_bool(), Some(true));
    assert!(body["instances_matched"].is_null());
    let signal = drain.await.expect("drain");
    assert_user_instance_cancel(&signal, instance_id, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bytag_cancel_reconciles_durable_state_without_notification() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    let (_cancel, mut rx) = spawn_subscriber(nats.clone(), Arc::clone(&pool), true).await;
    let mut rx = rx.take().expect("relay rx");
    let materialization_repository =
        tickr_migrations::backend::WriterRepositoryBundle::from_postgres_pool(
            pool.as_ref().clone(),
        );
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();

    // Persist server-authored materialization but suppress the transient NATS
    // notification entirely. The API must converge through bounded SQL reads.
    let emulate = tokio::spawn(async move {
        let signal = next_signal(&mut rx).await;
        let signal_id = Uuid::parse_str(&signal.signal_id).expect("published signal id is UUID");
        assert!(tickr_conductor::signal_cancels::materialize(
            &materialization_repository,
            signal_id,
            3,
        )
        .await
        .expect("persist Signal materialization"));
    });
    let resp = client
        .post(format!("{}/api/signals/cancel", base))
        .json(&json!({ "target": { "kind": "by_tag", "filter": { "env": "prod" } } }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["instances_matched"].as_u64(), Some(3));
    assert!(body["applied"].is_null());
    emulate.await.expect("emulator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bytag_cancel_timeout_returns_503_with_signal_id() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    let (_cancel, mut rx) = spawn_subscriber(nats.clone(), Arc::clone(&pool), true).await;
    let mut rx = rx.take().expect("relay rx");
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();

    // No durable Signal materialization is recorded. Draining the forwarded
    // Signal without publishing a notification proves the deadline is owned
    // by bounded durable-state reconciliation rather than the transient path.
    let killer = tokio::spawn(async move {
        let signal = next_signal(&mut rx).await;
        signal.signal_id
    });
    let resp = client
        .post(format!("{}/api/signals/cancel", base))
        .json(&json!({ "target": { "kind": "by_tag", "filter": { "env": "prod" } } }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = resp.json().await.expect("json");
    let sid = killer.await.expect("killer");
    let msg = body["error"].as_str().unwrap_or_default();
    assert_eq!(msg, "internal server error");
    assert!(
        !msg.contains(&sid.to_string()),
        "signal_id leaked in: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_dedup_and_conflict_on_idempotency_key() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    let (_cancel, mut rx) = spawn_subscriber(nats.clone(), Arc::clone(&pool), true).await;
    let mut rx = rx.take().expect("relay rx");
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();
    let instance_id = Uuid::new_v4();
    let key = "cancel-idem-key";
    let target = json!({ "target": { "kind": "instance", "workflow_instance_id": instance_id } });

    // First cancel forwards one signal; drain it.
    let drain = tokio::spawn(async move { next_signal(&mut rx).await });
    let first = client
        .post(format!("{}/api/signals/cancel", base))
        .header("Idempotency-Key", key)
        .json(&target)
        .send()
        .await
        .expect("first");
    assert_eq!(first.status(), 200);
    let first_body: serde_json::Value = first.json().await.expect("json");
    let first_sid = first_body["signal_id"].as_str().unwrap().to_string();
    drain.await.expect("drain");

    // Same key + same body: deduplicated, no re-forward.
    let second = client
        .post(format!("{}/api/signals/cancel", base))
        .header("Idempotency-Key", key)
        .json(&target)
        .send()
        .await
        .expect("second");
    assert_eq!(second.status(), 200);
    let second_body: serde_json::Value = second.json().await.expect("json");
    assert_eq!(second_body["deduplicated"].as_bool(), Some(true));
    assert_eq!(second_body["signal_id"].as_str(), Some(first_sid.as_str()));

    // Same key + different body (note differs): 409 Conflict.
    let third = client
        .post(format!("{}/api/signals/cancel", base))
        .header("Idempotency-Key", key)
        .json(&json!({
            "target": { "kind": "instance", "workflow_instance_id": instance_id },
            "note": "different",
        }))
        .send()
        .await
        .expect("third");
    assert_eq!(third.status(), 409);
    let third_body: serde_json::Value = third.json().await.expect("json");
    assert_eq!(
        third_body["original_signal_id"].as_str(),
        Some(first_sid.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sugar_routes_build_instance_targets() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    let (_cancel, mut rx) = spawn_subscriber(nats.clone(), Arc::clone(&pool), true).await;
    let mut rx = rx.take().expect("relay rx");
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    // Workflow-instance sugar -> Instance { node_id: None }.
    let resp = client
        .post(format!("{}/api/workflows/instances/{}/cancel", base, wi))
        .json(&json!({}))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let signal = next_signal(&mut rx).await;
    assert_user_instance_cancel(&signal, wi, None);

    // Task sugar -> Instance { node_id: Some(ti) }.
    let resp = client
        .post(format!(
            "{}/api/workflows/instances/{}/tasks/{}/cancel",
            base, wi, ti
        ))
        .json(&json!({}))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let signal = next_signal(&mut rx).await;
    assert_user_instance_cancel(&signal, wi, Some(ti));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_relay_unreachable_returns_503() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    // Relay deliberately left uninitialized: the forward fails.
    let (_cancel, _rx) = spawn_subscriber(nats.clone(), Arc::clone(&pool), false).await;
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/signals/cancel", base))
        .json(&json!({
            "target": { "kind": "instance", "workflow_instance_id": Uuid::new_v4() }
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 503);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "internal server error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_target_returns_400() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    // No subscriber needed: the API rejects a target-less cancel with 400
    // from its own request-shape validation, before the proto envelope
    // ever leaves for the command bus.
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/signals/cancel", base))
        .json(&json!({ "note": "no target" }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
}
