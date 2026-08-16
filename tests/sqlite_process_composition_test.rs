#![cfg(not(madsim))]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::{JsonValue, Uuid};
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{ContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{GenericImage, ImageExt};
use tickr_migrations::archive_repository::ArchiveTerminalWorkflowInput;
use tickr_migrations::backend::{RepositoryErrorKind, RepositoryFactory};
use tickr_migrations::event_repository::EventProjectionInput;
use tickr_migrations::patch_repository::{PatchIngressInput, PatchProvenance, PatchSourceFormat};
use tickr_migrations::replay_repository::{ReplayLifecycleInput, STATUS_MATERIALIZING};
use tickr_migrations::signal_repository::{
    SignalCancelInput, SignalCapturesInput, SignalWakeupInput,
};
use tickr_migrations::{apply_sqlite, apply_target, sqlite_writer_options, MigrationTarget};
use tickr_proto::config::DataPlaneSql;
use tickr_proto::tenant::{derive_workflow_id, TenantId};
use tickr_proto::{archive as ap, instance as ip, runnable as rp};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
  slug = "sqlite-process-probe",
  name = "sqlite-process-probe",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "cron", expr = "0 9 * * *" },
}
"#;

struct ManagedChild {
    label: &'static str,
    child: Child,
}

impl ManagedChild {
    async fn terminate(&mut self) {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(status.success(), "failed to signal {}", self.label);
        for _ in 0..400 {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        panic!("{} did not shut down after SIGTERM", self.label);
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

fn object_storage_image() -> GenericImage {
    GenericImage::new("quay.io/minio/minio", "latest")
        .with_exposed_port(ContainerPort::Tcp(9000))
        .with_wait_for(WaitFor::message_on_stdout("API:"))
}

fn configure_common_process(command: &mut Command, nats_url: &str, object_storage_url: &str) {
    command
        .env("TICKR_NATS_URL", nats_url)
        .env("TICKR_TENANT_SLUG", "process-probe")
        .env(
            "TICKR_DSL_PATHS",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dsl"),
        )
        .env("TICKR_CTRL_HTTP_URL", "http://127.0.0.1:1")
        .env("TICKR_CTRL_RELAY_URL", "http://127.0.0.1:1")
        .env("TICKR_LOG_STORAGE_ENDPOINT", object_storage_url)
        .env("TICKR_LOG_STORAGE_BUCKET", "tickr-logs")
        .env("TICKR_LOG_STORAGE_ACCESS_KEY_ID", "minioadmin")
        .env("TICKR_LOG_STORAGE_SECRET_ACCESS_KEY", "minioadmin")
        .env("TICKR_LOG_STORAGE_REGION", "us-east-1")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
}

fn configure_sqlite_process(
    command: &mut Command,
    database: &Path,
    nats_url: &str,
    object_storage_url: &str,
) {
    configure_common_process(command, nats_url, object_storage_url);
    command
        .env_remove("TICKR_CONDUCTOR_POSTGRES_URL")
        .env("TICKR_SQL_BACKEND", "sqlite")
        .env("TICKR_SQL_TOPOLOGY", "single-node")
        .env("TICKR_CONDUCTOR_SQLITE_URL", sqlite_url(database));
}

fn configure_postgres_process(
    command: &mut Command,
    postgres_url: &str,
    nats_url: &str,
    object_storage_url: &str,
) {
    configure_common_process(command, nats_url, object_storage_url);
    command
        .env_remove("TICKR_SQL_BACKEND")
        .env_remove("TICKR_SQL_TOPOLOGY")
        .env_remove("TICKR_CONDUCTOR_SQLITE_URL")
        .env("TICKR_CONDUCTOR_POSTGRES_URL", postgres_url);
}

async fn http_request(
    address: &str,
    method: &str,
    path: &str,
    body: &str,
) -> std::io::Result<String> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(address).await?;
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok(String::from_utf8_lossy(&response).into_owned())
    })
    .await
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::TimedOut, error))?
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}

async fn migrate_sqlite(path: &Path) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&sqlite_url(path), true).unwrap())
        .await
        .unwrap();
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    pool.close().await;
}

async fn wait_for_api(address: &str) {
    for _ in 0..100 {
        if http_request(address, "GET", "/health", "")
            .await
            .map(|response| response.starts_with("HTTP/1.1 200"))
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("API process never opened its HTTP listener");
}

async fn wait_for_degraded_api(address: &str) {
    for _ in 0..100 {
        if http_request(address, "GET", "/health", "")
            .await
            .map(|response| response.starts_with("HTTP/1.1 503"))
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Tickr Lite did not expose degraded health while relay was unavailable");
}

async fn assert_public_read(address: &str, path: &str, expected: &[&str]) -> String {
    let response = http_request(address, "GET", path, "").await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{path}: {response}");
    for value in expected {
        assert!(
            response.contains(value),
            "{path} omitted `{value}`: {response}"
        );
    }
    response
}

struct DurableIds {
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
    event_id: Uuid,
    capture_signal_id: Uuid,
    cancel_signal_id: Uuid,
    wakeup_signal_id: Uuid,
    patch_id: Uuid,
    replay_instance_id: Uuid,
}

fn instant(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

async fn seed_sqlite_durable_concerns(database: &Path) -> DurableIds {
    let workflow_id = derive_workflow_id(
        TenantId::from_slug("process-probe"),
        "default",
        "sqlite-process-probe",
    );
    let workflow_instance_id = Uuid::from_u128(0x1901);
    let task_instance_id = Uuid::from_u128(0x1902);
    let task_id = Uuid::from_u128(0x1903);
    let event_id = Uuid::from_u128(0x1904);
    let capture_signal_id = Uuid::from_u128(0x1905);
    let cancel_signal_id = Uuid::from_u128(0x1906);
    let wakeup_signal_id = Uuid::from_u128(0x1907);
    let patch_key = Uuid::from_u128(0x1908);
    let patch_id = Uuid::from_u128(0x1909);
    let replay_instance_id = Uuid::from_u128(0x1910);

    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite {
        url: sqlite_url(database),
    });
    let writer = factory.open_writer().await.unwrap();
    let archived_at = instant("2026-07-20T10:30:00Z");
    let projection = ap::ArchiveProjection {
        runnable: Some(rp::RunnableProjection {
            graph: Some(rp::RunnableGraph::default()),
            ..Default::default()
        }),
        id: workflow_instance_id.to_string(),
        workflow_id: workflow_id.to_string(),
        workflow_name: "sqlite-process-probe".to_string(),
        workflow_version: 1,
        name: "durable-terminal-run".to_string(),
        state: "Completed".to_string(),
        scheduled_at: Some("2026-07-20T09:00:00Z".to_string()),
        task_instances: vec![ip::SnapshotTaskInstance {
            id: task_instance_id.to_string(),
            task_id: task_id.to_string(),
            name: "durable-task".to_string(),
            task_type: "RegularTask".to_string(),
            state: "Completed".to_string(),
            attempt: 0,
            ..Default::default()
        }],
        ..Default::default()
    };
    let archive = || ArchiveTerminalWorkflowInput {
        projection: &projection,
        ctx_envelope: JsonValue::Array(Vec::new()),
        runtime_params: JsonValue::Object(Default::default()),
        log_uris: JsonValue::Object(Default::default()),
        archived_at,
    };
    writer.archive_terminal_workflow(archive()).await.unwrap();
    writer.archive_terminal_workflow(archive()).await.unwrap();

    let events = [
        EventProjectionInput {
            id: event_id,
            ts: instant("2026-07-20T09:01:00Z"),
            event_type: "WorkflowCompleted".to_string(),
            payload: JsonValue::Object(Default::default()),
            archived_at: instant("2026-07-20T10:31:00Z"),
        },
        EventProjectionInput {
            id: Uuid::from_u128(0x1911),
            ts: instant("2026-07-20T09:02:00Z"),
            event_type: "TaskCompleted".to_string(),
            payload: JsonValue::Object(Default::default()),
            archived_at: instant("2026-07-20T10:31:00Z"),
        },
    ];
    assert_eq!(writer.insert_event_page(&events).await.unwrap(), 2);
    assert_eq!(writer.insert_event_page(&events).await.unwrap(), 0);

    writer
        .insert_signal_captures(&SignalCapturesInput {
            signal_id: capture_signal_id,
            workflow_id,
            workflow_version: Some(1),
            captures: JsonValue::Array(Vec::new()),
        })
        .await
        .unwrap();
    writer
        .link_signal_captures(capture_signal_id, workflow_instance_id)
        .await
        .unwrap();
    writer
        .mark_signal_captures_terminal(workflow_instance_id)
        .await
        .unwrap();
    writer
        .insert_signal_cancel(&SignalCancelInput {
            signal_id: cancel_signal_id,
            applied_count: 1,
            target: JsonValue::Object(Default::default()),
            note: Some("durable cancel".to_string()),
        })
        .await
        .unwrap();
    writer
        .insert_signal_wakeup(&SignalWakeupInput {
            signal_id: wakeup_signal_id,
            name: "durable-wakeup".to_string(),
            matched_workflows: 1,
        })
        .await
        .unwrap();

    let patch_ops = Vec::new();
    let patch_input = PatchIngressInput {
        patch_key,
        patch_id,
        workflow_instance_id,
        ops: &patch_ops,
        operation: None,
        reason: None,
        provenance: PatchProvenance::External,
        source: "{\"ops\":[]}",
        source_format: PatchSourceFormat::Json,
        tasks: Vec::new(),
    };
    writer.ingress_patch(patch_input).await.unwrap();
    writer
        .ingress_patch(PatchIngressInput {
            patch_key,
            patch_id,
            workflow_instance_id,
            ops: &patch_ops,
            operation: None,
            reason: None,
            provenance: PatchProvenance::External,
            source: "{\"ops\":[]}",
            source_format: PatchSourceFormat::Json,
            tasks: Vec::new(),
        })
        .await
        .unwrap();

    let replay = ReplayLifecycleInput {
        replay_instance_id,
        source_instance_id: workflow_instance_id,
        signal_id: Uuid::from_u128(0x1912),
        idempotency_key: Some("durable-replay".to_string()),
        status: STATUS_MATERIALIZING.to_string(),
        resume_from: Vec::new(),
        pre_grounded: Vec::new(),
        name: Some("durable replay".to_string()),
        seed_sha256: None,
        outcome: None,
        shadowed_keys: Vec::new(),
    };
    writer.insert_replay_lifecycle(&replay).await.unwrap();
    writer.insert_replay_lifecycle(&replay).await.unwrap();
    writer.close().await;

    DurableIds {
        workflow_id,
        workflow_instance_id,
        task_instance_id,
        event_id,
        capture_signal_id,
        cancel_signal_id,
        wakeup_signal_id,
        patch_id,
        replay_instance_id,
    }
}

fn spawn_sqlite_components(
    database: &Path,
    nats_url: &str,
    object_storage_url: &str,
    api_address: &str,
) -> (ManagedChild, ManagedChild) {
    let mut conductor_command = Command::new(env!("CARGO_BIN_EXE_tickr-cli"));
    conductor_command.arg("conductor");
    configure_sqlite_process(
        &mut conductor_command,
        database,
        nats_url,
        object_storage_url,
    );
    let conductor = ManagedChild {
        label: "Conductor",
        child: conductor_command.spawn().expect("start Conductor"),
    };

    let mut api_command = Command::new(env!("CARGO_BIN_EXE_tickr-cli"));
    api_command
        .arg("api")
        .env("TICKR_API_BIND_ADDR", api_address);
    configure_sqlite_process(&mut api_command, database, nats_url, object_storage_url);
    let api = ManagedChild {
        label: "API component",
        child: api_command.spawn().expect("start API component"),
    };
    (conductor, api)
}

fn spawn_postgres_components(
    postgres_url: &str,
    nats_url: &str,
    object_storage_url: &str,
    api_address: &str,
) -> (ManagedChild, ManagedChild) {
    let mut conductor_command = Command::new(env!("CARGO_BIN_EXE_tickr-cli"));
    conductor_command.arg("conductor");
    configure_postgres_process(
        &mut conductor_command,
        postgres_url,
        nats_url,
        object_storage_url,
    );
    let conductor = ManagedChild {
        label: "Conductor",
        child: conductor_command.spawn().expect("start Conductor"),
    };

    let mut api_command = Command::new(env!("CARGO_BIN_EXE_tickr-cli"));
    api_command
        .arg("api")
        .env("TICKR_API_BIND_ADDR", api_address);
    configure_postgres_process(&mut api_command, postgres_url, nats_url, object_storage_url);
    let api = ManagedChild {
        label: "API component",
        child: api_command.spawn().expect("start API component"),
    };
    (conductor, api)
}

async fn register_through_public_ingress(api_address: &str) {
    let register_body = format!("{{\"nickel_source\":{}}}", json_string(WORKFLOW_SOURCE));
    for _ in 0..100 {
        let response = http_request(
            api_address,
            "POST",
            "/api/workflows/register",
            &register_body,
        )
        .await
        .unwrap();
        if response.starts_with("HTTP/1.1 202") || response.starts_with("HTTP/1.1 200") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("production registration ingress never committed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_conductor_and_api_share_one_sqlite_data_plane() {
    if !Command::new("nickel")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
    {
        eprintln!("skipping: nickel is unavailable");
        return;
    }
    let command = NatsServerCmd::default().with_jetstream();
    let Ok(nats) = Nats::default().with_cmd(&command).start().await else {
        return;
    };
    let Ok(nats_port) = nats.get_host_port_ipv4(4222).await else {
        return;
    };
    let nats_url = format!("nats://127.0.0.1:{nats_port}");
    let Ok(minio) = object_storage_image()
        .with_cmd(["server", "/data"])
        .start()
        .await
    else {
        return;
    };
    let Ok(minio_port) = minio.get_host_port_ipv4(9000).await else {
        return;
    };
    let object_storage_url = format!("http://127.0.0.1:{minio_port}");

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("data-plane.db");
    migrate_sqlite(&database).await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let api_address = listener.local_addr().unwrap().to_string();
    drop(listener);

    let (mut conductor, mut api) =
        spawn_sqlite_components(&database, &nats_url, &object_storage_url, &api_address);
    wait_for_api(&api_address).await;
    assert_public_read(
        &api_address,
        "/api/health",
        &["\"implementation\":\"sqlite\""],
    )
    .await;
    register_through_public_ingress(&api_address).await;
    assert_public_read(&api_address, "/api/workflows", &["sqlite-process-probe"]).await;

    api.terminate().await;
    conductor.terminate().await;
    let ids = seed_sqlite_durable_concerns(&database).await;

    let (mut restarted_conductor, mut restarted_api) =
        spawn_sqlite_components(&database, &nats_url, &object_storage_url, &api_address);
    wait_for_api(&api_address).await;
    assert_public_read(
        &api_address,
        "/api/health",
        &["\"implementation\":\"sqlite\""],
    )
    .await;
    assert_public_read(
        &api_address,
        "/api/workflows",
        &[
            "sqlite-process-probe",
            &ids.workflow_instance_id.to_string(),
            "\"completed_run_count\":1",
        ],
    )
    .await;
    assert_public_read(
        &api_address,
        &format!("/api/workflows/{}", ids.workflow_id),
        &[&ids.workflow_instance_id.to_string()],
    )
    .await;
    assert_public_read(
        &api_address,
        &format!("/api/workflows/{}/instances", ids.workflow_id),
        &[&ids.workflow_instance_id.to_string()],
    )
    .await;
    assert_public_read(
        &api_address,
        &format!(
            "/api/workflows/{}/calendar?year=2026&tz=UTC",
            ids.workflow_id
        ),
        &["2026-07-20"],
    )
    .await;
    assert_public_read(
        &api_address,
        &format!("/api/workflows/instances/{}", ids.workflow_instance_id),
        &["durable-terminal-run"],
    )
    .await;
    assert_public_read(
        &api_address,
        &format!(
            "/api/workflows/instances/{}/tasks",
            ids.workflow_instance_id
        ),
        &[&ids.task_instance_id.to_string()],
    )
    .await;
    assert_public_read(
        &api_address,
        "/api/dashboard/clock",
        &[&ids.workflow_instance_id.to_string()],
    )
    .await;
    assert_public_read(
        &api_address,
        "/api/events",
        &[&ids.event_id.to_string(), "WorkflowCompleted"],
    )
    .await;
    for signal_id in [
        ids.capture_signal_id,
        ids.cancel_signal_id,
        ids.wakeup_signal_id,
    ] {
        assert_public_read(
            &api_address,
            &format!("/api/signals/{signal_id}"),
            &[&signal_id.to_string()],
        )
        .await;
    }
    assert_public_read(
        &api_address,
        &format!("/api/patches/{}", ids.patch_id),
        &["Validating"],
    )
    .await;
    assert_public_read(
        &api_address,
        &format!("/api/patches/{}/source", ids.patch_id),
        &["{\\\"ops\\\":[]}"],
    )
    .await;
    assert_public_read(
        &api_address,
        &format!(
            "/api/workflows/instances/{}/replays",
            ids.workflow_instance_id
        ),
        &[&ids.replay_instance_id.to_string(), STATUS_MATERIALIZING],
    )
    .await;

    restarted_api.terminate().await;
    restarted_conductor.terminate().await;

    let reopened = RepositoryFactory::new(DataPlaneSql::Sqlite {
        url: sqlite_url(&database),
    })
    .open_read_only()
    .await
    .unwrap();
    assert_eq!(
        reopened
            .completed_run_counts()
            .await
            .unwrap()
            .get(&ids.workflow_id),
        Some(&1)
    );
    assert_eq!(
        reopened
            .event_count(tickr_migrations::event_repository::EventFilter::All)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        reopened
            .replays_for_source(ids.workflow_instance_id)
            .await
            .unwrap()
            .len(),
        1
    );
    reopened.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unset_backend_runs_the_principal_postgres_path_without_topology() {
    if !Command::new("nickel")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
    {
        eprintln!("skipping: nickel is unavailable");
        return;
    }
    let command = NatsServerCmd::default().with_jetstream();
    let Ok(nats) = Nats::default().with_cmd(&command).start().await else {
        return;
    };
    let nats_port = nats.get_host_port_ipv4(4222).await.unwrap();
    let nats_url = format!("nats://127.0.0.1:{nats_port}");
    let Ok(minio) = object_storage_image()
        .with_cmd(["server", "/data"])
        .start()
        .await
    else {
        return;
    };
    let minio_port = minio.get_host_port_ipv4(9000).await.unwrap();
    let object_storage_url = format!("http://127.0.0.1:{minio_port}");
    let Ok(postgres) = Postgres::default().start().await else {
        return;
    };
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let postgres_url = format!("postgres://postgres:postgres@127.0.0.1:{postgres_port}/postgres");
    let pool = PgPoolOptions::new().connect(&postgres_url).await.unwrap();
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    pool.close().await;

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let api_address = listener.local_addr().unwrap().to_string();
    drop(listener);
    let (mut conductor, mut api) =
        spawn_postgres_components(&postgres_url, &nats_url, &object_storage_url, &api_address);
    wait_for_api(&api_address).await;
    assert_public_read(
        &api_address,
        "/api/health",
        &["\"implementation\":\"postgres\""],
    )
    .await;
    register_through_public_ingress(&api_address).await;
    assert_public_read(&api_address, "/api/workflows", &["sqlite-process-probe"]).await;
    api.terminate().await;
    conductor.terminate().await;
}

#[test]
fn inadmissible_sqlite_processes_refuse_before_creating_or_serving() {
    let directory = tempfile::tempdir().unwrap();
    for component in ["conductor", "api"] {
        for topology in [None, Some("distributed")] {
            let database = directory
                .path()
                .join(format!("{component}-{}.db", topology.unwrap_or("missing")));
            let mut command = Command::new(env!("CARGO_BIN_EXE_tickr-cli"));
            command
                .arg(component)
                .env_remove("TICKR_SQL_TOPOLOGY")
                .env_remove("TICKR_CONDUCTOR_POSTGRES_URL")
                .env("TICKR_SQL_BACKEND", "sqlite")
                .env("TICKR_CONDUCTOR_SQLITE_URL", sqlite_url(&database));
            if let Some(topology) = topology {
                command.env("TICKR_SQL_TOPOLOGY", topology);
            }
            let output = command.output().expect("run invalid process configuration");
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("TICKR_SQL_TOPOLOGY"),
                "unexpected stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                !database.exists(),
                "inadmissible process created {}",
                database.display()
            );
        }
    }
}

#[tokio::test]
async fn stale_sqlite_processes_refuse_before_substrate_startup() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("stale.db");
    migrate_sqlite(&database).await;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&sqlite_url(&database), false).unwrap())
        .await
        .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let url = sqlite_url(&database);
    let writer_error = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() })
        .open_writer()
        .await
        .expect_err("stale SQLite must reject the writer role");
    assert_eq!(writer_error.kind(), RepositoryErrorKind::IncompatibleSchema);

    let reader_error = RepositoryFactory::new(DataPlaneSql::Sqlite { url })
        .open_read_only()
        .await
        .expect_err("stale SQLite must reject the read-only role");
    assert_eq!(reader_error.kind(), RepositoryErrorKind::IncompatibleSchema);
}

#[tokio::test]
async fn tickr_lite_withholds_readiness_while_control_plane_relay_is_unavailable() {
    let directory = tempfile::tempdir().unwrap();
    let data_root = directory.path().join("data");
    std::fs::create_dir(&data_root).unwrap();
    std::fs::set_permissions(&data_root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = data_root.join("tickr.db");
    let sqlite_url = sqlite_url(&database);

    let migration = Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .args(["migrate", "--formation", "tickr-lite"])
        .env("TICKR_SQL_BACKEND", "sqlite")
        .env("TICKR_SQL_TOPOLOGY", "single-node")
        .env("TICKR_CONDUCTOR_SQLITE_URL", &sqlite_url)
        .output()
        .expect("migrate Tickr Lite startup fixture");
    assert!(
        migration.status.success(),
        "Tickr Lite migration failed: {}",
        String::from_utf8_lossy(&migration.stderr)
    );

    let port_probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("127.0.0.1:{}", port_probe.local_addr().unwrap().port());
    drop(port_probe);

    let mut command = Command::new(env!("CARGO_BIN_EXE_tickr-lite"));
    command
        .current_dir(directory.path())
        .env_remove("TICKR_CONDUCTOR_POSTGRES_URL")
        .env_remove("TICKR_NATS_URL")
        .env_remove("TICKR_LOG_STORAGE_ENDPOINT")
        .env_remove("TICKR_CONSOLE_DIST")
        .env("TICKR_SQL_BACKEND", "sqlite")
        .env("TICKR_SQL_TOPOLOGY", "single-node")
        .env("TICKR_CONDUCTOR_SQLITE_URL", &sqlite_url)
        .env("TICKR_TENANT_SLUG", "lite-startup-probe")
        .env("TICKR_API_BIND_ADDR", &address)
        .env("TICKR_CTRL_HTTP_URL", "http://127.0.0.1:1")
        .env("TICKR_CTRL_RELAY_URL", "http://127.0.0.1:1")
        .env(
            "TICKR_CONTROL_PLANE_BEARER_TOKEN",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        )
        .env("TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut lite = ManagedChild {
        label: "Tickr Lite",
        child: command.spawn().expect("spawn Tickr Lite"),
    };

    wait_for_degraded_api(&address).await;
    lite.terminate().await;
}
