//! End-to-end test for `POST /api/workflows/register` over the command bus.
//!
//! Stands up an ephemeral Postgres (conductor migrations) and NATS, a real
//! conductor command-bus subscriber bound to that NATS, and the API HTTP
//! server. Drives register requests through the API on an ephemeral port and
//! asserts the response status + body match what the conductor's own HTTP
//! handler returns today.
//!
//! The Inserted / Conflict cases need a workflow that evaluates through the
//! Nickel parser, so they're guarded on `nickel` being on PATH (mirroring the
//! DSL fixture suite); the parse-failure case runs regardless.
//!
//! Requires Docker (testcontainers). Skipped automatically when Docker or the
//! Nickel-dependent toolchain is unavailable.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tokio_util::sync::CancellationToken;

use tickr_conductor::api_commands_consumer::{start, ApiCommandsState};
use tickr_conductor::gate_index_lifecycle::gate_index;
use tickr_conductor::wakeup_translator::DefaultRelaySender;

fn nickel_available() -> bool {
    Command::new("nickel")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// In-tree Core DSL directory (`tickr/dsl`) relative to this crate, exported as
/// `TICKR_DSL_PATHS` so `nickel export` resolves `import "lib.ncl"`.
fn set_dsl_path() {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("..");
    p.push("..");
    p.push("dsl");
    std::env::set_var("TICKR_DSL_PATHS", p);
}

/// A minimal valid workflow source (`namespace.slug` identity
/// `default.legacy-cron`). Re-registering identical content is an idempotent
/// NoOp: the register pipeline derives the id from `namespace.slug` and detects
/// the unchanged content hash, so there is no `(id, version)` conflict.
const LEGACY_CRON_SOURCE: &str = r#"
let utils = import "lib.ncl" in
let noop = utils.mkTask {
  name = "noop",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ noop ],
} in
utils.mkWorkflow {
  slug = "legacy-cron",
  name = "legacy-cron",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "cron", expr = "0 9 * * *" },
}
"#;

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

/// Spawn the conductor's command-bus subscriber against `nats`. Returns a
/// cancellation token the caller drops at test end. The brief sleep lets the
/// queue subscription propagate before the first request — the same
/// boot-ordering flake class the design accepts for the dev loop.
async fn spawn_subscriber(nats: NatsClient, pool: Arc<sqlx::PgPool>) -> CancellationToken {
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

/// Spawn the API HTTP router on an ephemeral port and return its base URL.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_parse_failure_returns_400() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    let _cancel = spawn_subscriber(nats.clone(), Arc::clone(&pool)).await;
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();

    // A source that cannot evaluate as a workflow surfaces as a parse error.
    let resp = client
        .post(format!("{}/api/workflows/register", base))
        .json(&json!({ "nickel_source": "@@@ this is not nickel @@@" }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["success"].as_bool(), Some(false));
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .starts_with("Failed to parse workflow:"),
        "unexpected message: {:?}",
        body["message"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_inserted_then_noop_on_identical() {
    if !nickel_available() {
        eprintln!("skipping: nickel not on PATH");
        return;
    }
    set_dsl_path();
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    let pool = Arc::new(pool);
    let _cancel = spawn_subscriber(nats.clone(), Arc::clone(&pool)).await;
    let pool_for_assert = Arc::clone(&pool);
    let base = spawn_api(nats, pool).await;
    let client = reqwest::Client::new();

    // First registration: 202 Inserted with the Building status and task count.
    let first = client
        .post(format!("{}/api/workflows/register", base))
        .json(&json!({ "nickel_source": LEGACY_CRON_SOURCE }))
        .send()
        .await
        .expect("first post");
    assert_eq!(first.status(), 202, "expected Inserted");
    let first_body: serde_json::Value = first.json().await.expect("json");
    assert_eq!(first_body["status"].as_str(), Some("Building"));
    assert_eq!(first_body["task_count"].as_u64(), Some(1));
    assert!(first_body["workflow_id"].as_str().is_some());
    assert!(first_body["workflow_version"].as_i64().is_some());

    // The submitted Nickel source is persisted verbatim alongside the parsed
    // definition: read the row back and assert byte-for-byte equality with what
    // was posted. This is the archival fact the workflow detail surface renders.
    let wf_id = first_body["workflow_id"].as_str().expect("workflow_id");
    let (persisted_source,): (String,) =
        sqlx::query_as("SELECT nickel_source FROM workflows WHERE id = $1::uuid")
            .bind(wf_id)
            .fetch_one(pool_for_assert.as_ref())
            .await
            .expect("read nickel_source back");
    assert_eq!(
        persisted_source, LEGACY_CRON_SOURCE,
        "registered Nickel source must round-trip byte-for-byte through persistence"
    );

    // Second registration of the same source: identical content is an
    // idempotent NoOp (200), echoing the same workflow id — not a conflict.
    let second = client
        .post(format!("{}/api/workflows/register", base))
        .json(&json!({ "nickel_source": LEGACY_CRON_SOURCE }))
        .send()
        .await
        .expect("second post");
    assert_eq!(second.status(), 200, "identical re-register is a NoOp");
    let second_body: serde_json::Value = second.json().await.expect("json");
    assert_eq!(second_body["status"].as_str(), Some("NoOp"));
    assert_eq!(
        second_body["workflow_id"].as_str(),
        first_body["workflow_id"].as_str(),
        "NoOp must reference the same workflow id"
    );
    assert!(second_body["workflow_version"].as_i64().is_some());
}
