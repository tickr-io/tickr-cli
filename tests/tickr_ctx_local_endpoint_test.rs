#![cfg(all(unix, not(madsim)))]

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tickr::data_directory::{DataDirectory, RootRelativePath};
use tickr::tickr_ctx_endpoint::{TickrCtxEndpoint, TickrCtxScopeWriter};
use tickr_ctx::envelope::{Envelope, Producer};
use tickr_migrations::backend::RepositoryFactory;
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeValueInput,
};
use tickr_proto::config::DataPlaneSql;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn tickr_ctx_binary() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_tickr-ctx") {
        return PathBuf::from(path);
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("debug")
        .join("tickr-ctx");
    assert!(
        path.is_file(),
        "build the real helper first with `cargo build -p tickr_ctx --bin tickr-ctx`"
    );
    path
}

fn migrate(path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .args(["migrate", "--formation", "tickr-lite"])
        .env("TICKR_SQL_BACKEND", "sqlite")
        .env("TICKR_SQL_TOPOLOGY", "single-node")
        .env(
            "TICKR_CONDUCTOR_SQLITE_URL",
            format!("sqlite://{}", path.display()),
        )
        .output()
        .unwrap()
}

fn ctx_command(
    binary: &Path,
    endpoint_environment: &[(String, String); 2],
    namespace: &str,
    run_id: &str,
    task_id: &str,
) -> Command {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .envs(endpoint_environment.iter().cloned())
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("TICKR_NS", namespace)
        .env("TICKR_RUN_ID", run_id)
        .env("TICKR_TASK_ID", task_id)
        .env("TICKR_TASK_NAME", "local-test")
        .env("TICKR_OUTPUTS", "answer,secret,big,stream")
        .env("TICKR_INPUTS", "collision")
        .env("TICKR_SECRETS", "secret");
    command
}

fn run_ctx(
    binary: &Path,
    endpoint_environment: &[(String, String); 2],
    namespace: &str,
    run_id: &str,
    task_id: &str,
    arguments: &[&str],
) -> Output {
    ctx_command(binary, endpoint_environment, namespace, run_id, task_id)
        .args(arguments)
        .output()
        .unwrap()
}

fn envelope(value: &str, task_id: &str) -> Vec<u8> {
    serde_json::to_vec(&Envelope::new(
        "string",
        serde_json::Value::String(value.to_owned()),
        false,
        Producer::Task {
            task_id: task_id.to_owned(),
            task_name: "seed".to_owned(),
        },
    ))
    .unwrap()
}

async fn create_scope(
    repository: &tickr_migrations::backend::WriterRepositoryBundle,
    scope_id: Uuid,
    namespace: &str,
    run_id: &str,
    values: &[(String, Vec<u8>)],
) {
    let inputs = values
        .iter()
        .map(|(key, envelope)| ScopeValueInput { key, envelope })
        .collect::<Vec<_>>();
    assert_eq!(
        repository
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id,
                namespace,
                run_id,
                claim_id: Uuid::new_v4(),
                values: &inputs,
                now: Utc::now(),
            })
            .await
            .unwrap(),
        ScopeCreationOutcome::Created
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_tickr_ctx_process_uses_the_ready_authenticated_single_writer_endpoint() {
    let helper = tickr_ctx_binary();
    let directory = tempfile::tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("tickr.db");
    let migration = migrate(&database);
    assert!(
        migration.status.success(),
        "Tickr Lite migration failed: {}",
        String::from_utf8_lossy(&migration.stderr)
    );

    let data_directory = Arc::new(DataDirectory::admit(directory.path()).unwrap());
    let url = format!("sqlite://{}", database.display());
    let repository = Arc::new(
        RepositoryFactory::new(DataPlaneSql::Sqlite { url })
            .open_writer()
            .await
            .unwrap(),
    );

    let namespace = "tenant-local";
    let run_id = Uuid::new_v4().to_string();
    let task_id = Uuid::new_v4().to_string();
    let trigger_id = Uuid::new_v4().to_string();
    let scope_id = Uuid::new_v4();
    create_scope(
        &repository,
        scope_id,
        namespace,
        &run_id,
        &[
            (format!("{run_id}/seed"), envelope("seed-value", &task_id)),
            (
                format!("{run_id}/collision"),
                envelope("run-collision", &task_id),
            ),
            (
                format!("{trigger_id}/collision"),
                envelope("trigger-collision", &task_id),
            ),
        ],
    )
    .await;

    let other_run_id = Uuid::new_v4().to_string();
    let other_task_id = Uuid::new_v4().to_string();
    let other_scope_id = Uuid::new_v4();
    create_scope(
        &repository,
        other_scope_id,
        namespace,
        &other_run_id,
        &[(
            format!("{other_run_id}/seed"),
            envelope("other-seed", &other_task_id),
        )],
    )
    .await;

    let (writer_client, writer) = TickrCtxScopeWriter::new(repository.clone());
    let writer_cancel = CancellationToken::new();
    let writer_task = tokio::spawn(writer.run(writer_cancel.clone()));
    let socket = RootRelativePath::new("journals/tickr-ctx.sock").unwrap();
    let (handle, endpoint) = TickrCtxEndpoint::bind_after_recovery(
        data_directory.clone(),
        socket.clone(),
        writer_client.clone(),
    )
    .unwrap();
    let endpoint_environment = handle
        .register_task(&task_id, namespace, &run_id, scope_id)
        .await
        .unwrap()
        .variables();
    let other_environment = handle
        .register_task(&other_task_id, namespace, &other_run_id, other_scope_id)
        .await
        .unwrap()
        .variables();
    let endpoint_cancel = CancellationToken::new();
    let endpoint_task = tokio::spawn(endpoint.run(endpoint_cancel.clone()));

    let unavailable = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["get", "seed"],
    );
    assert_eq!(unavailable.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&unavailable.stderr).contains("unavailable"));

    handle.mark_ready();
    let metadata = fs::symlink_metadata(handle.endpoint()).unwrap();
    assert!(metadata.file_type().is_socket());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

    let capture = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["capture", "answer", "round-trip"],
    );
    assert!(
        capture.status.success(),
        "{}",
        String::from_utf8_lossy(&capture.stderr)
    );
    let get = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["get", "answer"],
    );
    assert!(get.status.success());
    assert_eq!(get.stdout, b"round-trip");

    let collision = ctx_command(&helper, &endpoint_environment, namespace, &run_id, &task_id)
        .env("TICKR_TRIGGER_SIGNAL_ID", &trigger_id)
        .args(["get", "collision"])
        .output()
        .unwrap();
    assert_eq!(collision.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&collision.stderr).contains("multiple scopes"));

    let secret = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["capture", "secret", "sensitive", "--secret"],
    );
    assert!(secret.status.success());
    let listed = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["ls"],
    );
    assert!(listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stdout).contains("secret\t<redacted>"));
    let exported = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["export", "--format", "json"],
    );
    assert!(exported.status.success());
    assert!(!String::from_utf8_lossy(&exported.stdout).contains("sensitive"));

    let oversized = "x".repeat(1024 * 1024);
    let oversized_file = directory.path().join("oversized-value");
    fs::write(&oversized_file, oversized).unwrap();
    let limit = ctx_command(&helper, &endpoint_environment, namespace, &run_id, &task_id)
        .args(["capture", "big", "--file", oversized_file.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(limit.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&limit.stderr).contains("limit"));

    let cross_task = run_ctx(
        &helper,
        &other_environment,
        namespace,
        &run_id,
        &task_id,
        &["get", "answer"],
    );
    assert_eq!(cross_task.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&cross_task.stderr).contains("identity rejected"));

    let mut tail = ctx_command(&helper, &endpoint_environment, namespace, &run_id, &task_id)
        .args(["tail", "--prefix", "stream"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let streamed = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["capture", "stream", "event"],
    );
    assert!(streamed.status.success());
    let mut line = String::new();
    BufReader::new(tail.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert!(
        line.contains("Put\tstream\tstring"),
        "unexpected tail line: {line}"
    );
    tail.kill().unwrap();
    tail.wait().unwrap();

    let contender = migrate(&database);
    assert!(!contender.status.success());
    assert!(String::from_utf8_lossy(&contender.stderr).contains("already exclusively locked"));

    handle.clear_ready();
    let cleared = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["get", "answer"],
    );
    assert_eq!(cleared.status.code(), Some(5));

    endpoint_cancel.cancel();
    endpoint_task.await.unwrap().unwrap();
    assert!(!handle.endpoint().exists());
    let lost = run_ctx(
        &helper,
        &endpoint_environment,
        namespace,
        &run_id,
        &task_id,
        &["get", "answer"],
    );
    assert_eq!(lost.status.code(), Some(5));

    let (restarted_handle, restarted_endpoint) = TickrCtxEndpoint::bind_after_recovery(
        data_directory.clone(),
        socket,
        writer_client.clone(),
    )
    .unwrap();
    let restarted_environment = restarted_handle
        .register_task(&task_id, namespace, &run_id, scope_id)
        .await
        .unwrap()
        .variables();
    let restarted_cancel = CancellationToken::new();
    let restarted_task = tokio::spawn(restarted_endpoint.run(restarted_cancel.clone()));
    restarted_handle.mark_ready();
    let after_restart = run_ctx(
        &helper,
        &restarted_environment,
        namespace,
        &run_id,
        &task_id,
        &["get", "answer"],
    );
    assert!(after_restart.status.success());
    assert_eq!(after_restart.stdout, b"round-trip");

    let removed = run_ctx(
        &helper,
        &restarted_environment,
        namespace,
        &run_id,
        &task_id,
        &["rm", "answer"],
    );
    assert!(removed.status.success());
    let missing = run_ctx(
        &helper,
        &restarted_environment,
        namespace,
        &run_id,
        &task_id,
        &["get", "answer", "--default", "missing"],
    );
    assert!(missing.status.success());
    assert_eq!(missing.stdout, b"missing");

    restarted_cancel.cancel();
    restarted_task.await.unwrap().unwrap();
    writer_cancel.cancel();
    writer_task.await.unwrap().unwrap();
    repository.close().await;
}
