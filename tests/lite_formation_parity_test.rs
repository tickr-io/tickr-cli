use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use prost::Message;
use sqlx::sqlite::SqlitePoolOptions;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child as ProcessChild, Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tickr::data_directory::DataDirectory;
use tickr::formation::{
    resolve_formation, CoordinationRole, ExecutorTopology, FormationSelection, RoleImplementation,
};
use tickr::local_compaction::{LocalCompactionDrain, LocalCompactionStager};
use tickr::local_log_staging::{
    read_final_log, AcceptOutcome, FinalLogReference, LocalLogStagingStream, LogExit,
    LogRecordIdentity, LogRecordSubmission, LogStreamIdentity, LogTerminal, ReplayedLogRecord,
};
use tickr::local_task_pickup_writer::{LocalTaskPickupWriter, LocalTaskPickupWriterClient};
use tickr::migrate_cmd::{self, MigrationFormation};
use tickr_conductor::build_pipeline::{
    definition_build_notifications, start_local_definition_build_worker,
    LocalDefinitionBuildWorkerConfig, TestBuildExecutor,
};
use tickr_conductor::register_pipeline::{
    process_register_local, RegisterOutcome, RegisterRequest,
};
use tickr_conductor::relay::init_relay_tx;
use tickr_conductor::submission_consumer::{
    definition_submission_notifications, start_local_definition_submission_worker,
    LocalDefinitionSubmissionWorkerConfig,
};
use tickr_executor::local_pickup::{
    LocalAttemptOutcome, LocalExecutorCapacity, PickupBoundary, PickupCheckpoint, PickupOutcome,
    SafeAttemptOutcomeHandoff, SafePickupExecutor, TaskProcessLauncher, TerminalElection,
};
use tickr_executor::task_log_shipper::{ShipperConfig, TaskLogShipper};
use tickr_executor::wire::{encode_dispatch, encode_task_event, DispatchedTask, EmitKind};
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeValueInput,
};
use tickr_migrations::sqlite_writer_options;
use tickr_migrations::task_pickup_repository::TaskPickupTerminalOutcome;
use tickr_proto::archive::{ArchiveProjection, CompactionEnvelope};
use tickr_proto::config::DataPlaneSql;
use tickr_proto::instance::SnapshotTaskInstance;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const NORMAL_LOG: &[u8] = b"normal output\n";
const CRASH_LOG: &[u8] = b"accepted before Log-owner crash\n";
const PRESSURE_LOG: &[u8] = b"accepted before local filesystem pressure\n";
const SCOPE_ENVELOPE: &[u8] = br#"{"v":2,"type":"string","value":"lite-parity","secret":false,"producer":{"kind":"task","task_id":"lite-parity","task_name":"parity-task"},"created_at":"2026-07-23T00:00:00Z","sha256":"lite-parity-scope"}"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiteRoleLawScenario {
    CommandBus,
    TaskDispatch,
    TaskEvents,
    TaskCancellation,
    CompactionStaging,
    LifecycleWork,
    LogStaging,
    ScopeStore,
    LivenessWatchdog,
    SignalAppliedNotifier,
    ExecutorFleetStatus,
}

fn enabled_lite_role_law(role: CoordinationRole) -> Option<LiteRoleLawScenario> {
    match role {
        CoordinationRole::CommandBus => Some(LiteRoleLawScenario::CommandBus),
        CoordinationRole::TaskDispatch => Some(LiteRoleLawScenario::TaskDispatch),
        CoordinationRole::TaskEvents => Some(LiteRoleLawScenario::TaskEvents),
        CoordinationRole::TaskCancellation => Some(LiteRoleLawScenario::TaskCancellation),
        CoordinationRole::CompactionStaging => Some(LiteRoleLawScenario::CompactionStaging),
        CoordinationRole::LifecycleWork => Some(LiteRoleLawScenario::LifecycleWork),
        CoordinationRole::LogStaging => Some(LiteRoleLawScenario::LogStaging),
        CoordinationRole::ScopeStore => Some(LiteRoleLawScenario::ScopeStore),
        CoordinationRole::LivenessWatchdog => Some(LiteRoleLawScenario::LivenessWatchdog),
        CoordinationRole::SignalAppliedNotifier => Some(LiteRoleLawScenario::SignalAppliedNotifier),
        CoordinationRole::ExecutorFleetStatus => Some(LiteRoleLawScenario::ExecutorFleetStatus),
        CoordinationRole::IngressIdempotencyStore | CoordinationRole::EventIngress => None,
    }
}

fn workflow_source() -> String {
    r#"let utils = import "lib.ncl" in
utils.mkWorkflow {
  slug = "tickr-lite-role-parity",
  name = "tickr-lite-role-parity",
  args = [],
  outputs = [],
  tasks = [ utils.mkTaskGroup {
    name = "parity",
    args = [],
    outputs = [],
    tasks = [ utils.mkTask {
      name = "parity-task",
      args = [],
      nix_expression_path = "unused-by-parity-launcher",
      outputs = [],
    } ],
  } ],
}"#
    .to_owned()
}

fn configure_lite_sql(url: &str) {
    std::env::set_var("TICKR_SQL_BACKEND", "sqlite");
    std::env::set_var("TICKR_SQL_TOPOLOGY", "single-node");
    std::env::set_var("TICKR_CONDUCTOR_SQLITE_URL", url);
}

async fn migrate_lite(url: &str) -> Result<()> {
    configure_lite_sql(url);
    migrate_cmd::run(MigrationFormation::TickrLite).await
}

async fn open_writer(url: &str) -> Result<WriterRepositoryBundle> {
    Ok(RepositoryFactory::new(DataPlaneSql::Sqlite {
        url: url.to_owned(),
    })
    .open_writer()
    .await?)
}

async fn open_read_pool(url: &str) -> Result<sqlx::SqlitePool> {
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(url, false)?)
        .await?)
}

async fn wait_for_workflow_status(url: &str, workflow_id: Uuid, expected: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let pool = open_read_pool(url).await.expect("open status reader");
            let actual: String =
                sqlx::query_scalar("SELECT status FROM workflows WHERE id = ?1 AND version = 1")
                    .bind(workflow_id.to_string())
                    .fetch_one(&pool)
                    .await
                    .expect("read Workflow status");
            pool.close().await;
            if actual == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Workflow did not reach {expected}"));
}

fn task_from_env() -> DispatchedTask {
    let parse = |name: &str| {
        std::env::var(name)
            .unwrap_or_else(|_| panic!("missing {name}"))
            .parse()
            .unwrap_or_else(|_| panic!("invalid UUID in {name}"))
    };
    DispatchedTask {
        task_instance_id: parse("TICKR_LITE_PARITY_TASK_INSTANCE_ID"),
        task_id: parse("TICKR_LITE_PARITY_TASK_ID"),
        workflow_instance_id: parse("TICKR_LITE_PARITY_WORKFLOW_INSTANCE_ID"),
        workflow_id: parse("TICKR_LITE_PARITY_WORKFLOW_ID"),
        name: "parity-task".to_owned(),
        nix_expression_path: "unused-by-parity-launcher".to_owned(),
        nix_args: vec![],
        outputs: vec![],
        inputs: vec![],
        secrets: vec![],
        originating_signal_id: None,
        gate_signal_ids: HashMap::new(),
        gate_signal_ids_ambient: HashSet::new(),
    }
}

struct WriterRuntime {
    client: LocalTaskPickupWriterClient,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl WriterRuntime {
    fn start(repository: WriterRepositoryBundle) -> Self {
        let (client, writer) = LocalTaskPickupWriter::new(repository);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(writer.run(cancel.clone()));
        Self {
            client,
            cancel,
            handle,
        }
    }

    async fn stop(self) {
        self.cancel.cancel();
        self.handle.await.expect("join local pickup writer");
    }
}

#[derive(Clone)]
struct ShippedTaskLauncher {
    data_directory: Arc<DataDirectory>,
    launch_log: PathBuf,
    shippers: Arc<Mutex<HashMap<Uuid, TaskLogShipper>>>,
}

impl ShippedTaskLauncher {
    async fn spawn_process(&self) -> Result<Child, String> {
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "printf 'launch\\n' >> \"$1\"; printf 'normal output\\n'",
                "tickr-lite-parity-task",
            ])
            .arg(&self.launch_log)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        command
            .spawn()
            .map_err(|error| format!("spawn real parity Task process: {error}"))
    }
}

impl TaskProcessLauncher for ShippedTaskLauncher {
    async fn spawn(&self, _task: &DispatchedTask) -> Result<Child, String> {
        self.spawn_process().await
    }

    async fn spawn_claimed(
        &self,
        task: &DispatchedTask,
        claim: &tickr_executor::local_pickup::LocalPickupClaim,
    ) -> Result<Child, String> {
        let mut child = self.spawn_process().await?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "real parity Task stdout was not piped".to_owned())?;
        let stream = LocalLogStagingStream::open(
            self.data_directory.as_ref(),
            LogStreamIdentity {
                task_instance_id: task.task_instance_id,
                pickup_generation: claim
                    .pickup_generation
                    .try_into()
                    .map_err(|_| "pickup generation is negative".to_owned())?,
            },
        )
        .map_err(|error| error.to_string())?;
        let shipper = TaskLogShipper::start(
            Box::new(stream),
            &ShipperConfig {
                flush_deadline: Duration::from_secs(5),
                ..ShipperConfig::default()
            },
            stdout,
        );
        self.shippers
            .lock()
            .await
            .insert(task.task_instance_id, shipper);
        Ok(child)
    }

    async fn process_exited(
        &self,
        task: &DispatchedTask,
        _claim: &tickr_executor::local_pickup::LocalPickupClaim,
        status: &std::process::ExitStatus,
    ) -> Result<(), String> {
        let shipper = self
            .shippers
            .lock()
            .await
            .remove(&task.task_instance_id)
            .ok_or_else(|| "real parity Task Log shipper was not registered".to_owned())?;
        let exit = status.code().map_or(LogExit::NoStatus, LogExit::Status);
        shipper.finish(exit, &CancellationToken::new()).await;
        Ok(())
    }
}

#[derive(Clone)]
struct BlockAfterClaimProof {
    marker: PathBuf,
}

impl PickupCheckpoint for BlockAfterClaimProof {
    fn reached(&self, boundary: PickupBoundary) -> Result<(), String> {
        if boundary == PickupBoundary::AfterClaimProof {
            std::fs::write(&self.marker, b"claim-proved").map_err(|error| error.to_string())?;
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        Ok(())
    }
}

fn launch_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|contents| contents.lines().count())
        .unwrap_or(0)
}

async fn helper_context() -> Result<(Arc<DataDirectory>, WriterRepositoryBundle)> {
    let url = std::env::var("TICKR_LITE_PARITY_SQLITE_URL")?;
    migrate_lite(&url).await?;
    let root = PathBuf::from(std::env::var("TICKR_LITE_PARITY_DATA_ROOT")?);
    let data_directory = Arc::new(DataDirectory::admit(&root)?);
    let writer = open_writer(&url).await?;
    Ok((data_directory, writer))
}

async fn run_pickup_helper(mode: &str) -> Result<()> {
    let (data_directory, writer) = helper_context().await?;
    let runtime = WriterRuntime::start(writer.clone());
    let task = task_from_env();
    let payload = encode_dispatch(&task);
    let (dispatch_key, _) = runtime
        .client
        .stage_dispatch(&payload)
        .await
        .map_err(anyhow::Error::msg)?;
    let launch_log = PathBuf::from(std::env::var("TICKR_LITE_PARITY_LAUNCH_LOG")?);
    let capacity = LocalExecutorCapacity::new(Uuid::new_v4(), NonZeroUsize::new(1).unwrap());
    let observation = capacity.observation().snapshot();
    assert_eq!(observation.configured_process_slots, 1);
    assert_eq!(observation.in_flight_count, 0);
    let launcher = ShippedTaskLauncher {
        data_directory,
        launch_log: launch_log.clone(),
        shippers: Arc::new(Mutex::new(HashMap::new())),
    };

    match mode {
        "normal" => {
            let outcome = SafePickupExecutor::new(
                runtime.client.clone(),
                launcher,
                capacity,
                "executor-one",
                Duration::from_millis(200),
            )
            .run_one()
            .await?;
            let PickupOutcome::Launched {
                claim,
                exit_success,
                election,
            } = outcome
            else {
                anyhow::bail!("normal Tickr Lite pickup did not launch: {outcome:?}");
            };
            assert!(exit_success);
            assert_eq!(claim.pickup_generation, 1);
            assert_eq!(claim.owner, "executor-one");
            assert_eq!(election, TerminalElection::Won);
            assert_eq!(launch_count(&launch_log), 1);
        }
        "crash" => {
            let marker = PathBuf::from(std::env::var("TICKR_LITE_PARITY_BOUNDARY")?);
            let _ = SafePickupExecutor::with_checkpoint(
                runtime.client.clone(),
                launcher,
                BlockAfterClaimProof { marker },
                capacity,
                "executor-one",
                Duration::from_millis(200),
            )
            .run_one()
            .await;
            unreachable!("pickup crash helper must be killed after durable claim proof");
        }
        "recover" => {
            let executor = SafePickupExecutor::new(
                runtime.client.clone(),
                launcher,
                capacity,
                "executor-one",
                Duration::from_millis(200),
            );
            assert_eq!(executor.run_one().await?, PickupOutcome::NoWork);
            let (claim, election) = executor
                .reconcile_one_due_liveness(Utc::now() + ChronoDuration::seconds(10))
                .await?
                .context("restart did not recover the due pickup generation")?;
            assert_eq!(claim.dispatch_key, dispatch_key);
            assert_eq!(claim.pickup_generation, 1);
            assert_eq!(claim.owner, "executor-one");
            assert_eq!(election, TerminalElection::Won);

            let completed = encode_task_event(&task, Uuid::new_v4(), EmitKind::Completed);
            assert_eq!(
                runtime
                    .client
                    .elect_terminal(
                        &claim,
                        LocalAttemptOutcome::ProcessExitedSuccess,
                        &completed,
                        Utc::now(),
                    )
                    .await
                    .map_err(anyhow::Error::msg)?,
                TerminalElection::Settled(LocalAttemptOutcome::LivenessExpired)
            );
            let snapshot = writer
                .task_pickup_snapshot(&dispatch_key)
                .await?
                .context("recovered pickup snapshot disappeared")?;
            assert_eq!(snapshot.pickup_generation, 1);
            assert_eq!(snapshot.owner.as_deref(), Some("executor-one"));
            assert_eq!(
                snapshot.terminal_outcome,
                Some(TaskPickupTerminalOutcome::LivenessExpired)
            );
            assert_eq!(
                snapshot.staged_event_kinds,
                ["Assigned".to_owned(), "Unhealthy".to_owned()]
            );
            assert_eq!(launch_count(&launch_log), 1);
            std::fs::write(
                std::env::var("TICKR_LITE_PARITY_BOUNDARY")?,
                b"pickup-recovered",
            )?;
        }
        other => anyhow::bail!("unknown pickup helper mode {other}"),
    }

    runtime.stop().await;
    writer.close().await;
    Ok(())
}

async fn run_log_helper(mode: &str) -> Result<()> {
    let url = std::env::var("TICKR_LITE_PARITY_SQLITE_URL")?;
    migrate_lite(&url).await?;
    let root = PathBuf::from(std::env::var("TICKR_LITE_PARITY_DATA_ROOT")?);
    let data_directory = DataDirectory::admit(&root)?;
    let task = task_from_env();
    let stream_identity = LogStreamIdentity {
        task_instance_id: task.task_instance_id,
        pickup_generation: 1,
    };
    let submission = LogRecordSubmission::new(
        LogRecordIdentity {
            stream: stream_identity.clone(),
            sequence: 0,
        },
        CRASH_LOG.to_vec(),
    );

    match mode {
        "log-crash" => {
            let mut stream = LocalLogStagingStream::open(&data_directory, stream_identity)?;
            assert_eq!(stream.accept(submission)?, AcceptOutcome::Accepted);
            assert_eq!(stream.committed_frontier(), Some(0));
            std::fs::write(
                std::env::var("TICKR_LITE_PARITY_BOUNDARY")?,
                b"log-accepted",
            )?;
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        "log-recover" => {
            let mut stream =
                LocalLogStagingStream::open_existing(&data_directory, stream_identity)?;
            assert_eq!(stream.committed_frontier(), Some(0));
            assert_eq!(
                stream.replay(),
                [ReplayedLogRecord::Accepted {
                    identity: submission.identity.clone(),
                    content_digest: submission.content_digest.clone(),
                    bytes: CRASH_LOG.to_vec(),
                }]
            );
            assert_eq!(
                stream.accept(submission.clone())?,
                AcceptOutcome::AlreadyAccepted
            );
            let conflicting = LogRecordSubmission::new(submission.identity, b"conflict".to_vec());
            assert!(stream.accept(conflicting).is_err());
            assert_eq!(stream.committed_frontier(), Some(0));
            stream.recover_abnormal_closure()?;
            let first_seal = stream.seal()?;
            let replayed_seal = stream.seal()?;
            assert_eq!(first_seal, replayed_seal);
            std::fs::write(
                std::env::var("TICKR_LITE_PARITY_BOUNDARY")?,
                b"log-recovered",
            )?;
        }
        other => anyhow::bail!("unknown Log helper mode {other}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawned by the Tickr Lite real-process parity scenario"]
async fn lite_parity_process_helper() {
    let mode = std::env::var("TICKR_LITE_PARITY_MODE").expect("parity helper mode");
    let result = if matches!(mode.as_str(), "normal" | "crash" | "recover") {
        run_pickup_helper(&mode).await
    } else {
        run_log_helper(&mode).await
    };
    result.expect("Tickr Lite parity helper failed");
}

fn spawn_helper(mode: &str, env: &[(&str, String)]) -> ProcessChild {
    let mut command =
        ProcessCommand::new(std::env::current_exe().expect("current test executable"));
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("lite_parity_process_helper")
        .arg("--nocapture")
        .env("TICKR_LITE_PARITY_MODE", mode)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (name, value) in env {
        command.env(name, value);
    }
    command.spawn().expect("spawn Tickr Lite parity helper")
}

async fn wait_for_path(path: &Path) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("process boundary marker missing: {}", path.display()));
}

fn helper_env(
    root: &Path,
    url: &str,
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_id: Uuid,
    task_instance_id: Uuid,
    launch_log: &Path,
    boundary: &Path,
) -> Vec<(&'static str, String)> {
    vec![
        ("TICKR_SQL_BACKEND", "sqlite".to_owned()),
        ("TICKR_SQL_TOPOLOGY", "single-node".to_owned()),
        ("TICKR_CONDUCTOR_SQLITE_URL", url.to_owned()),
        ("TICKR_LITE_PARITY_SQLITE_URL", url.to_owned()),
        (
            "TICKR_LITE_PARITY_DATA_ROOT",
            root.to_string_lossy().into_owned(),
        ),
        ("TICKR_LITE_PARITY_WORKFLOW_ID", workflow_id.to_string()),
        (
            "TICKR_LITE_PARITY_WORKFLOW_INSTANCE_ID",
            workflow_instance_id.to_string(),
        ),
        ("TICKR_LITE_PARITY_TASK_ID", task_id.to_string()),
        (
            "TICKR_LITE_PARITY_TASK_INSTANCE_ID",
            task_instance_id.to_string(),
        ),
        (
            "TICKR_LITE_PARITY_LAUNCH_LOG",
            launch_log.to_string_lossy().into_owned(),
        ),
        (
            "TICKR_LITE_PARITY_BOUNDARY",
            boundary.to_string_lossy().into_owned(),
        ),
    ]
}

fn compaction_payload(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_id: Uuid,
    normal_task: Uuid,
    crashed_task: Uuid,
) -> Vec<u8> {
    let task = |id: Uuid, state: &str| SnapshotTaskInstance {
        id: id.to_string(),
        task_id: task_id.to_string(),
        name: "parity-task".to_owned(),
        task_type: "Regular".to_owned(),
        state: state.to_owned(),
        executor_id: Some("executor-one".to_owned()),
        attempt: 0,
        ..Default::default()
    };
    CompactionEnvelope {
        projection: Some(ArchiveProjection {
            id: workflow_instance_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "Tickr Lite parity instance".to_owned(),
            state: "Failed".to_owned(),
            scheduled_at: Some(Utc::now().to_rfc3339()),
            task_instances: vec![
                task(normal_task, "Completed"),
                task(crashed_task, "Unhealthy"),
            ],
            ..Default::default()
        }),
        correlation: "tickr-lite-role-parity".to_owned(),
        shipped_at: Some(Utc::now().to_rfc3339()),
    }
    .encode_to_vec()
}

fn assert_local_filesystem_pressure_fences_and_recovers(
    data_directory: &DataDirectory,
    root: &Path,
) -> Result<()> {
    let identity = LogStreamIdentity {
        task_instance_id: Uuid::new_v4(),
        pickup_generation: 1,
    };
    let submission = LogRecordSubmission::new(
        LogRecordIdentity {
            stream: identity.clone(),
            sequence: 0,
        },
        PRESSURE_LOG.to_vec(),
    );
    let mut stream = LocalLogStagingStream::open(data_directory, identity.clone())?;
    assert_eq!(stream.accept(submission.clone())?, AcceptOutcome::Accepted);
    drop(stream);

    let staged_directory = root.join("logs/staged");
    std::fs::set_permissions(&staged_directory, std::fs::Permissions::from_mode(0o500))?;
    let fenced = LocalLogStagingStream::open_existing(data_directory, identity.clone());
    std::fs::set_permissions(&staged_directory, std::fs::Permissions::from_mode(0o700))?;
    assert!(
        fenced.is_err(),
        "local filesystem pressure must fence acceptance"
    );

    let mut recovered = LocalLogStagingStream::open_existing(data_directory, identity)?;
    assert_eq!(recovered.committed_frontier(), Some(0));
    assert_eq!(
        recovered.accept(submission)?,
        AcceptOutcome::AlreadyAccepted
    );
    recovered.finish_cleanly(LogExit::Status(0))?;
    let seal = recovered.seal()?;
    let reference = LocalLogStagingStream::install_final(data_directory, &seal)?;
    LocalLogStagingStream::verify_final(data_directory, &reference)?;
    LocalLogStagingStream::purge_staged(data_directory, &reference)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tickr_lite_runs_enabled_role_laws_and_real_process_crash_parity() -> Result<()> {
    let descriptor = resolve_formation(&FormationSelection::lite_local())?;
    assert_eq!(descriptor.executors, ExecutorTopology::Exactly(1));
    let mut enabled = 0;
    let mut disabled = Vec::new();
    for resolved in descriptor.roles.iter() {
        match resolved.implementation {
            RoleImplementation::Disabled => {
                assert!(enabled_lite_role_law(resolved.role).is_none());
                disabled.push(resolved.role);
            }
            _ => {
                enabled += 1;
                assert!(
                    enabled_lite_role_law(resolved.role).is_some(),
                    "enabled role {:?} has no Tickr Lite law scenario",
                    resolved.role
                );
            }
        }
    }
    assert_eq!(enabled, 11);
    assert_eq!(
        disabled,
        [
            CoordinationRole::IngressIdempotencyStore,
            CoordinationRole::EventIngress,
        ]
    );

    assert!(
        ProcessCommand::new("nickel")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false),
        "Nickel is required for the real registration path"
    );
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("data");
    std::fs::create_dir(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let url = format!("sqlite://{}", root.join("tickr.db").display());
    configure_lite_sql(&url);
    migrate_lite(&url).await?;
    std::env::set_var(
        tickr_conductor::parser::nickel::DSL_PATHS_ENV,
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dsl"),
    );

    let data_directory = DataDirectory::admit(&root)?;
    let writer = Arc::new(open_writer(&url).await?);
    let registration = process_register_local(
        writer.as_ref(),
        RegisterRequest {
            nickel_source: workflow_source(),
            namespace: "default".to_owned(),
        },
    )
    .await?;
    let workflow_id = match registration {
        RegisterOutcome::Inserted {
            workflow_id,
            workflow_version: 1,
            task_count: 1,
            ..
        } => workflow_id,
        _ => anyhow::bail!("fresh Tickr Lite registration was not inserted"),
    };

    let (_build_notifier, build_notifications) =
        definition_build_notifications(NonZeroUsize::new(1).unwrap());
    let build_cancel = CancellationToken::new();
    let build_handle = tokio::spawn(start_local_definition_build_worker(
        writer.clone(),
        Arc::new(TestBuildExecutor::new()),
        "tickr-lite-parity-build".to_owned(),
        build_notifications,
        LocalDefinitionBuildWorkerConfig {
            scan_interval: Duration::from_millis(50),
            lease_duration: Duration::from_secs(2),
            batch_size: NonZeroUsize::new(4).unwrap(),
        },
        build_cancel.clone(),
    ));
    wait_for_workflow_status(&url, workflow_id, "Ready").await;
    build_cancel.cancel();
    build_handle.await??;

    let (relay_tx, mut relay_rx) = mpsc::channel(4);
    init_relay_tx(relay_tx).await;
    let (_submission_notifier, submission_notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).unwrap());
    let submission_cancel = CancellationToken::new();
    let submission_handle = tokio::spawn(start_local_definition_submission_worker(
        writer.clone(),
        "tickr-lite-parity-submission".to_owned(),
        submission_notifications,
        LocalDefinitionSubmissionWorkerConfig {
            scan_interval: Duration::from_millis(50),
            lease_duration: Duration::from_secs(2),
            batch_size: NonZeroUsize::new(4).unwrap(),
        },
        submission_cancel.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(10), relay_rx.recv())
        .await
        .context("definition submission was not forwarded")?
        .context("definition relay closed")?;
    wait_for_workflow_status(&url, workflow_id, "Submitted").await;
    submission_cancel.cancel();
    submission_handle.await??;

    let pool = open_read_pool(&url).await?;
    let task_id: String = sqlx::query_scalar(
        "SELECT task_id FROM workflow_task_builds WHERE workflow_id = ?1 AND workflow_version = 1 ORDER BY task_id LIMIT 1",
    )
    .bind(workflow_id.to_string())
    .fetch_one(&pool)
    .await?;
    pool.close().await;
    let task_id: Uuid = task_id.parse()?;
    let workflow_instance_id = Uuid::new_v4();
    let run_id = workflow_instance_id.to_string();
    assert!(matches!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: Uuid::new_v4(),
                namespace: "default",
                run_id: &run_id,
                claim_id: Uuid::new_v4(),
                values: &[ScopeValueInput {
                    key: "parity/result",
                    envelope: SCOPE_ENVELOPE,
                }],
                now: Utc::now(),
            })
            .await?,
        ScopeCreationOutcome::Created
    ));
    Arc::try_unwrap(writer)
        .map_err(|_| anyhow::anyhow!("Tickr Lite writer still has an owner"))?
        .close()
        .await;
    drop(data_directory);

    let normal_task = Uuid::new_v4();
    let crashed_task = Uuid::new_v4();
    let launch_log = temporary.path().join("launches");
    let unused_boundary = temporary.path().join("unused-boundary");
    let normal_env = helper_env(
        &root,
        &url,
        workflow_id,
        workflow_instance_id,
        task_id,
        normal_task,
        &launch_log,
        &unused_boundary,
    );
    assert!(
        spawn_helper("normal", &normal_env)
            .wait()
            .context("wait for normal Tickr Lite Executor")?
            .success(),
        "normal Tickr Lite Executor helper failed"
    );
    assert_eq!(launch_count(&launch_log), 1);

    let pickup_boundary = temporary.path().join("pickup-boundary");
    let crash_env = helper_env(
        &root,
        &url,
        workflow_id,
        workflow_instance_id,
        task_id,
        crashed_task,
        &launch_log,
        &pickup_boundary,
    );
    let mut crashing_executor = spawn_helper("crash", &crash_env);
    wait_for_path(&pickup_boundary).await;
    crashing_executor.kill()?;
    crashing_executor.wait()?;
    assert_eq!(launch_count(&launch_log), 1);

    let pickup_recovered = temporary.path().join("pickup-recovered");
    let recovery_env = helper_env(
        &root,
        &url,
        workflow_id,
        workflow_instance_id,
        task_id,
        crashed_task,
        &launch_log,
        &pickup_recovered,
    );
    assert!(
        spawn_helper("recover", &recovery_env)
            .wait()
            .context("wait for replacement Tickr Lite Executor")?
            .success(),
        "replacement Tickr Lite Executor helper failed"
    );
    wait_for_path(&pickup_recovered).await;
    assert_eq!(launch_count(&launch_log), 1);

    let log_boundary = temporary.path().join("log-boundary");
    let log_crash_env = helper_env(
        &root,
        &url,
        workflow_id,
        workflow_instance_id,
        task_id,
        crashed_task,
        &launch_log,
        &log_boundary,
    );
    let mut crashing_log_owner = spawn_helper("log-crash", &log_crash_env);
    wait_for_path(&log_boundary).await;
    crashing_log_owner.kill()?;
    crashing_log_owner.wait()?;

    let log_recovered = temporary.path().join("log-recovered");
    let log_recovery_env = helper_env(
        &root,
        &url,
        workflow_id,
        workflow_instance_id,
        task_id,
        crashed_task,
        &launch_log,
        &log_recovered,
    );
    assert!(
        spawn_helper("log-recover", &log_recovery_env)
            .wait()
            .context("wait for replacement Log owner")?
            .success(),
        "replacement Log owner helper failed"
    );
    wait_for_path(&log_recovered).await;

    let data_directory = DataDirectory::admit(&root)?;
    assert_local_filesystem_pressure_fences_and_recovers(&data_directory, &root)?;
    let writer = open_writer(&url).await?;
    let payload = compaction_payload(
        workflow_id,
        workflow_instance_id,
        task_id,
        normal_task,
        crashed_task,
    );
    let acknowledgement = LocalCompactionStager::new(&writer)
        .stage_for_relay(&payload)
        .await?;
    assert!(!acknowledgement.payload.is_empty());
    writer.close().await;
    drop(data_directory);

    let data_directory = DataDirectory::admit(&root)?;
    let writer = open_writer(&url).await?;
    let drain = LocalCompactionDrain::new(&writer, &data_directory);
    assert!(drain.drain_next().await?);
    assert!(!drain.drain_next().await?);
    writer.close().await;

    let pool = open_read_pool(&url).await?;
    let archive_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ?1")
            .bind(workflow_instance_id.to_string())
            .fetch_one(&pool)
            .await?;
    assert_eq!(archive_count, 1);
    let archived_tasks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_instances WHERE workflow_instance_id = ?1")
            .bind(workflow_instance_id.to_string())
            .fetch_one(&pool)
            .await?;
    assert_eq!(archived_tasks, 2);
    let staging: (String, Option<Vec<u8>>) = sqlx::query_as(
        "SELECT state, payload FROM local_compaction_staging WHERE workflow_instance_id = ?1",
    )
    .bind(workflow_instance_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(staging, ("purged".to_owned(), None));
    let final_references: Vec<FinalLogReference> = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT log_uris FROM workflow_run_info WHERE workflow_instance_id = ?1",
        )
        .bind(workflow_instance_id.to_string())
        .fetch_one(&pool)
        .await?,
    )?;
    pool.close().await;
    assert_eq!(final_references.len(), 2);

    let normal_identity = LogStreamIdentity {
        task_instance_id: normal_task,
        pickup_generation: 1,
    };
    let crash_identity = LogStreamIdentity {
        task_instance_id: crashed_task,
        pickup_generation: 1,
    };
    let normal_final = read_final_log(&data_directory, &normal_identity)?
        .context("normal Task final Log is missing")?;
    assert_eq!(
        normal_final
            .records
            .iter()
            .flat_map(|record| record.bytes.iter().copied())
            .collect::<Vec<_>>(),
        NORMAL_LOG
    );
    assert!(matches!(
        normal_final.terminal,
        LogTerminal::EndOfStream {
            exit: LogExit::Status(0)
        }
    ));
    let crash_final = read_final_log(&data_directory, &crash_identity)?
        .context("crashed Task final Log is missing")?;
    assert_eq!(
        crash_final
            .records
            .iter()
            .flat_map(|record| record.bytes.iter().copied())
            .collect::<Vec<_>>(),
        CRASH_LOG
    );
    assert!(matches!(
        crash_final.terminal,
        LogTerminal::AbnormalClosure {
            committed_frontier: Some(0)
        }
    ));
    for reference in &final_references {
        LocalLogStagingStream::verify_final(&data_directory, reference)?;
    }
    assert!(LocalLogStagingStream::open_existing(&data_directory, normal_identity).is_err());
    assert!(LocalLogStagingStream::open_existing(&data_directory, crash_identity).is_err());

    let writer = open_writer(&url).await?;
    LocalCompactionStager::new(&writer)
        .stage_for_relay(&payload)
        .await?;
    assert!(
        !LocalCompactionDrain::new(&writer, &data_directory)
            .drain_next()
            .await?
    );
    writer.close().await;
    assert_eq!(launch_count(&launch_log), 1);

    for name in [
        "TICKR_SQL_BACKEND",
        "TICKR_SQL_TOPOLOGY",
        "TICKR_CONDUCTOR_SQLITE_URL",
        tickr_conductor::parser::nickel::DSL_PATHS_ENV,
    ] {
        std::env::remove_var(name);
    }
    Ok(())
}
