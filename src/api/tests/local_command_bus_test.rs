//! Real API-to-writer coverage for Tickr Lite's local Command bus.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_api::commands::client::CommandBus;
use tickr_api::commands::local::LocalCommandBusConfig;
use tickr_conductor::api_commands_consumer::{handle_local_request, ApiCommandsState};
use tickr_conductor::gate_index_lifecycle::gate_index;
use tickr_conductor::wakeup_translator::DefaultRelaySender;
use tickr_migrations::backend::{ReadOnlyRepositoryBundle, RepositoryFactory};
use tickr_proto::config::DataPlaneSql;
use tokio_util::sync::CancellationToken;

const WORKFLOW_SOURCE: &str = r#"
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
  slug = "local-command-writer",
  name = "local-command-writer",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "cron", expr = "0 9 * * *" },
}
"#;

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

fn nickel_available() -> bool {
    Command::new("nickel")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn set_dsl_path() {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("..");
    path.push("..");
    path.push("dsl");
    std::env::set_var("TICKR_DSL_PATHS", path);
}

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    NatsClient,
)> {
    let command = NatsServerCmd::default().with_jetstream();
    let container = Nats::default().with_cmd(&command).start().await.ok()?;
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{port}");
    for _ in 0..20 {
        if let Ok(client) = async_nats::connect(&url).await {
            return Some((container, client));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

async fn open_sqlite_roles(
    path: &Path,
) -> (
    tickr_migrations::backend::WriterRepositoryBundle,
    ReadOnlyRepositoryBundle,
) {
    let url = sqlite_url(path);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    tickr_migrations::apply_sqlite(tickr_migrations::MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    pool.close().await;

    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url });
    let writer = factory.open_writer().await.unwrap();
    // Keep the API role open alongside the writer. Its production factory uses
    // read-only SQLite flags and exposes no mutation operations.
    let reader = factory.open_read_only().await.unwrap();
    (writer, reader)
}

async fn spawn_api(
    nats: NatsClient,
    command_bus: CommandBus,
    repositories: ReadOnlyRepositoryBundle,
) -> String {
    let control_plane = Arc::new(
        tickr_api::http::control_plane_client::ControlPlaneClient::new(
            "http://127.0.0.1:1".to_string(),
        ),
    );
    let storage = opendal::services::S3::default()
        .bucket("ignored")
        .endpoint("http://127.0.0.1:1")
        .access_key_id("x")
        .secret_access_key("x")
        .region("us-east-1");
    let logs = Arc::new(tickr_api::http::logs_resolver::LogsResolver::new(
        opendal::Operator::new(storage).unwrap().finish(),
        Arc::new(tickr_executor::log_stream::AllNatsLogStreamProvider::new(
            Arc::new(nats.clone()),
            Duration::from_secs(5),
        )),
    ));
    let state = tickr_api::http::routes::build_app_state_with_command_bus(
        Arc::new(nats),
        command_bus,
        Arc::new(repositories),
        control_plane,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_register_reaches_the_sole_local_sqlite_writer() {
    if !nickel_available() {
        eprintln!("skipping: nickel is unavailable");
        return;
    }
    set_dsl_path();
    let Some((_nats_container, nats)) = start_nats().await else {
        eprintln!("skipping: NATS testcontainer unavailable");
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let (writer_repository, reader_repository) =
        open_sqlite_roles(&directory.path().join("data-plane.db")).await;

    let state = ApiCommandsState {
        definition_repository: Arc::new(writer_repository),
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
    let (command_bus, writer) = CommandBus::local(LocalCommandBusConfig::default());
    let cancel = CancellationToken::new();
    let writer_task = tokio::spawn(writer.run(cancel.clone(), move |payload| {
        let state = state.clone();
        async move { handle_local_request(&state, &payload).await }
    }));
    let base = spawn_api(nats, command_bus, reader_repository).await;
    let client = reqwest::Client::new();

    let malformed = client
        .post(format!("{base}/api/workflows/register"))
        .json(&json!({"nickel_source": "@@@ not nickel @@@"}))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);

    let inserted = client
        .post(format!("{base}/api/workflows/register"))
        .json(&json!({"nickel_source": WORKFLOW_SOURCE}))
        .send()
        .await
        .unwrap();
    assert_eq!(inserted.status(), 202);
    let inserted_body: serde_json::Value = inserted.json().await.unwrap();
    assert_eq!(inserted_body["status"], "Building");

    let repeated = client
        .post(format!("{base}/api/workflows/register"))
        .json(&json!({"nickel_source": WORKFLOW_SOURCE}))
        .send()
        .await
        .unwrap();
    assert_eq!(repeated.status(), 200);

    let listed: serde_json::Value = client
        .get(format!("{base}/api/workflows"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(listed
        .as_array()
        .unwrap()
        .iter()
        .any(|workflow| workflow["slug"] == "local-command-writer"));

    cancel.cancel();
    writer_task.await.unwrap();
}
