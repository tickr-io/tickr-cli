use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use tickr_api::commands::local::LocalCommandBusConfig;
use tickr_api::http::coordinator_client::CoordinatorClient;
use tickr_api::http::health::{
    HealthCoordinationRole, HealthFinalLogStore, HealthFormationProfile, HealthFormationTopology,
    HealthProtocolIdentity, HealthResolvedRole, HealthRoleImplementation, HealthSubstrateSelection,
    HealthWriterTopology, ResolvedFormationHealth,
};
use tickr_api::http::logs_resolver::{
    EndOfStreamMarker, LocalTaskLogStore, LogBatch, LogBatchPage, LogsError, LogsResolver, TaskLogs,
};
use tickr_conductor::api_commands_consumer::LiteApiCommandsState;
use tickr_conductor::build_pipeline::{
    definition_build_notifications, start_local_definition_build_worker,
    LocalDefinitionBuildWorkerConfig, NixBuildExecutor,
};
use tickr_conductor::patch_pipeline::{
    local::{patch_work_notifications, start_local_patch_worker, LocalPatchWorkerConfig},
    DefaultPatchRelaySender,
};
use tickr_conductor::relay::{run_streaming_lite, LiteRelayRoles};
use tickr_conductor::replay_pipeline::{
    local::{replay_work_notifications, start_local_replay_worker, LocalReplayWorkerConfig},
    ReplayRelaySender,
};
use tickr_conductor::replay_rehydration::{local_rehydration_values, RehydrationPlan};
use tickr_conductor::signal_applied_notifier::{
    signal_applied_notifications, LocalSignalAppliedNotifier, SignalAppliedNotifier,
};
use tickr_conductor::submission_consumer::local::{
    definition_submission_notifications, start_local_definition_submission_worker,
    LocalDefinitionSubmissionWorkerConfig,
};
use tickr_conductor::wakeup_translator::DefaultRelaySender;
use tickr_executor::local_pickup::{
    LocalExecutorFleetStatus, LocalPickupClaim, NixTaskProcessLauncher, PickupOutcome,
    SafeCancellationCoordinator, SafePickupExecutor, TaskProcessLauncher,
};
use tickr_executor::task_handler::build_task_environment;
use tickr_executor::wire::DispatchedTask;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, ScopeCreationOutcome, ScopeValueInput,
};
use tickr_proto::config::DataPlaneSql;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::data_directory::{sqlite_path_from_url, DataDirectory, RootRelativePath};
use crate::formation::{
    resolve_formation, CoordinationRole, ExecutorTopology, FinalLogStore, FormationProfile,
    FormationSelection, ResolvedFormationDescriptor, RoleImplementation, SqlImplementation,
    Topology, WriterTopology,
};
use crate::formation_manifest::{install_or_verify_formation_manifest, ManifestAdmission};
use crate::local_command_writer::LocalCommandWriter;
use crate::local_compaction::{LocalCompactionDrain, LocalCompactionStager};
use crate::local_log_staging::{
    read_final_log, FinalLogTerminal, LocalLogStagingStream, LogExit, LogRecordIdentity,
    LogStreamIdentity, ReplayedLogRecord,
};
use crate::local_task_pickup_writer::{LocalTaskPickupWriter, LocalTaskPickupWriterClient};
use crate::tickr_ctx_endpoint::{TickrCtxEndpoint, TickrCtxEndpointHandle, TickrCtxScopeWriter};

const LIVENESS_TIMEOUT: Duration = Duration::from_secs(20);
const IDLE_SCAN: Duration = Duration::from_millis(100);
const ROLE_NOTIFICATION_CAPACITY: usize = 32;
const EXECUTOR_PROCESS_SLOTS: usize = 10;
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(15);
const GUARDIAN_TERMINATION_GRACE: Duration = Duration::from_secs(1);

/// Formation-wide ownership root for the single-process Tickr Lite profile.
pub struct LiteSupervisor {
    ready: Arc<AtomicBool>,
    cancel: CancellationToken,
}

fn lite_health_descriptor(
    descriptor: &ResolvedFormationDescriptor,
) -> Result<ResolvedFormationHealth> {
    if descriptor.profile != FormationProfile::TickrLite
        || descriptor.topology != Topology::SingleNode
        || descriptor.sql != SqlImplementation::Sqlite
        || descriptor.final_logs != FinalLogStore::LocalFiles
        || descriptor.writer_topology != WriterTopology::ConductorOwned
    {
        bail!("resolved formation is not Tickr Lite");
    }
    let ExecutorTopology::Exactly(executor_count) = descriptor.executors else {
        bail!("Tickr Lite health requires an exact Executor count");
    };
    let substrates = HealthSubstrateSelection {
        sqlite: descriptor.sql == SqlImplementation::Sqlite,
        postgres: descriptor.sql == SqlImplementation::Postgres,
        nats: descriptor
            .roles
            .iter()
            .any(|role| role.implementation == RoleImplementation::NatsJetStream),
        redis: descriptor
            .roles
            .iter()
            .any(|role| role.implementation == RoleImplementation::Redis),
        object_store: descriptor.final_logs == FinalLogStore::ObjectStore,
    };
    let roles = descriptor
        .roles
        .iter()
        .map(|resolved| {
            let role = match resolved.role {
                CoordinationRole::CommandBus => HealthCoordinationRole::CommandBus,
                CoordinationRole::TaskDispatch => HealthCoordinationRole::TaskDispatch,
                CoordinationRole::TaskEvents => HealthCoordinationRole::TaskEvents,
                CoordinationRole::TaskCancellation => HealthCoordinationRole::TaskCancellation,
                CoordinationRole::CompactionStaging => HealthCoordinationRole::CompactionStaging,
                CoordinationRole::LifecycleWork => HealthCoordinationRole::LifecycleWork,
                CoordinationRole::LogStaging => HealthCoordinationRole::LogStaging,
                CoordinationRole::ScopeStore => HealthCoordinationRole::ScopeStore,
                CoordinationRole::IngressIdempotencyStore => {
                    HealthCoordinationRole::IngressIdempotencyStore
                }
                CoordinationRole::LivenessWatchdog => HealthCoordinationRole::LivenessWatchdog,
                CoordinationRole::SignalAppliedNotifier => {
                    HealthCoordinationRole::SignalAppliedNotifier
                }
                CoordinationRole::ExecutorFleetStatus => {
                    HealthCoordinationRole::ExecutorFleetStatus
                }
                CoordinationRole::EventIngress => HealthCoordinationRole::EventIngress,
            };
            let implementation = match resolved.implementation {
                RoleImplementation::LocalRequestReply => {
                    HealthRoleImplementation::LocalRequestReply
                }
                RoleImplementation::LocalSqlite => HealthRoleImplementation::LocalSqlite,
                RoleImplementation::LocalJournal => HealthRoleImplementation::LocalJournal,
                RoleImplementation::LocalNotification => {
                    HealthRoleImplementation::LocalNotification
                }
                RoleImplementation::LocalObservation => HealthRoleImplementation::LocalObservation,
                RoleImplementation::Disabled => HealthRoleImplementation::Disabled,
                RoleImplementation::NatsJetStream | RoleImplementation::Redis => {
                    bail!("Tickr Lite health cannot contain a distributed role")
                }
            };
            Ok(HealthResolvedRole {
                role,
                implementation,
                protocol: HealthProtocolIdentity {
                    name: resolved.protocol.name.to_owned(),
                    version: resolved.protocol.version,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ResolvedFormationHealth {
        profile: HealthFormationProfile::TickrLite,
        topology: HealthFormationTopology::SingleNode,
        sql: tickr_api::http::health::DataPlaneSqlImplementation::Sqlite,
        final_logs: HealthFinalLogStore::LocalFiles,
        writer_topology: HealthWriterTopology::ConductorOwned,
        executor_count,
        substrates,
        roles,
    })
}

impl LiteSupervisor {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
            cancel,
        }
    }

    pub async fn run(self) -> Result<()> {
        let selection =
            tickr_proto::config::data_plane_sql().context("resolving Tickr Lite data-plane SQL")?;
        let DataPlaneSql::Sqlite { url } = &selection else {
            bail!("Tickr Lite requires SQLite");
        };
        let descriptor = resolve_formation(&FormationSelection::tickr_lite())
            .context("resolving Tickr Lite formation")?;
        let health_descriptor = lite_health_descriptor(&descriptor)?;
        let sqlite_path = sqlite_path_from_url(url)
            .context("resolving SQLite path beneath the Tickr Lite data directory")?;
        let root = sqlite_path
            .parent()
            .ok_or_else(|| anyhow!("configured SQLite path has no data-directory parent"))?;
        let sqlite_name = sqlite_path
            .file_name()
            .ok_or_else(|| anyhow!("configured SQLite path has no database file name"))?;
        let sqlite_relative = RootRelativePath::new(Path::new(sqlite_name))
            .context("validating root-relative SQLite path")?;

        // No listener, relay, or claim loop exists before directory, schema, and
        // formation-manifest admission has completed.
        let data_directory =
            Arc::new(DataDirectory::admit(root).context("admitting Tickr Lite data directory")?);
        let spec =
            crate::migrate_cmd::tickr_lite_manifest_spec(&descriptor, url, &sqlite_relative)?;
        install_or_verify_formation_manifest(
            data_directory.as_ref(),
            &spec,
            ManifestAdmission::Runtime,
        )
        .context("verifying Tickr Lite formation manifest")?;

        let writer = Arc::new(
            tickr_conductor::repository::configure_writer(selection.clone())
                .await
                .context("opening Tickr Lite writer role")?,
        );
        let read_only = Arc::new(
            tickr_api::repository::configure_read_only(selection)
                .await
                .context("opening Tickr Lite read-only role")?,
        );

        self.recover_local_journals(writer.as_ref(), data_directory.as_ref())
            .await?;
        while LocalCompactionDrain::new(writer.as_ref(), data_directory.as_ref())
            .drain_next()
            .await
            .context("recovering local Compaction drain")?
        {}

        let (pickup_client, pickup_writer) = LocalTaskPickupWriter::new(writer.as_ref().clone());

        let (scope_writer_client, scope_writer) = TickrCtxScopeWriter::new(writer.clone());
        let (ctx_handle, ctx_endpoint) = TickrCtxEndpoint::bind_after_recovery(
            data_directory.clone(),
            RootRelativePath::new("run/tickr-ctx.sock")?,
            scope_writer_client,
        )
        .context("binding Tickr Lite tickr-ctx endpoint")?;

        let slots = NonZeroUsize::new(EXECUTOR_PROCESS_SLOTS).expect("slots are non-zero");
        let fleet = LocalExecutorFleetStatus::new(Uuid::new_v4(), slots);
        let executor = Arc::new(SafePickupExecutor::new(
            pickup_client.clone(),
            LiteTaskProcessLauncher {
                nix: NixTaskProcessLauncher::default(),
                ctx: ctx_handle.clone(),
                namespace: std::env::var("TICKR_NS").unwrap_or_else(|_| "default".to_owned()),
                data_directory: data_directory.clone(),
                writer: writer.clone(),
                logs: Arc::new(Mutex::new(HashMap::new())),
            },
            fleet.clone(),
            format!("tickr-lite-{}", Uuid::new_v4()),
            LIVENESS_TIMEOUT,
        ));
        while executor
            .reconcile_one_due_liveness(Utc::now())
            .await
            .context("reconciling overdue local pickup")?
            .is_some()
        {}
        let cancellation = Arc::new(SafeCancellationCoordinator::new(
            pickup_client.clone(),
            executor.task_handler(),
        ));

        let notification_capacity = NonZeroUsize::new(ROLE_NOTIFICATION_CAPACITY)
            .expect("notification capacity is non-zero");
        let (_build_notifier, build_notifications) =
            definition_build_notifications(notification_capacity);
        let (_submission_notifier, submission_notifications) =
            definition_submission_notifications(notification_capacity);
        let (_patch_notifier, patch_notifications) =
            patch_work_notifications(notification_capacity);
        let (_replay_notifier, replay_notifications) =
            replay_work_notifications(notification_capacity);
        let (signal_notifier, signal_notifications) =
            signal_applied_notifications(notification_capacity);

        let patch_sender = Arc::new(DefaultPatchRelaySender);
        let replay_sender = Arc::new(LiteReplayRelaySender {
            writer: writer.clone(),
        });
        let command_state = LiteApiCommandsState {
            definition_repository: writer.clone(),
            relay_sender: Arc::new(DefaultRelaySender),
            patch_relay_sender: patch_sender.clone(),
            replay_relay_sender: replay_sender.clone(),
            signal_applied_notifications: Arc::new(tokio::sync::Mutex::new(signal_notifications)),
            gate_index: tickr_conductor::gate_index_lifecycle::gate_index().clone(),
        };
        let (command_bus, command_writer) =
            LocalCommandWriter::new(command_state, LocalCommandBusConfig::default());

        let relay_roles = Arc::new(LiteRelayRoleSet {
            pickup: pickup_client.clone(),
            writer: writer.clone(),
            cancellation,
            signal_notifier,
        });

        let coordinator = Arc::new(
            CoordinatorClient::try_new(tickr_proto::config::coordinator_http_url())
                .context("validating Control-plane query client")?,
        );
        let logs = Arc::new(LogsResolver::local(Arc::new(LiteLogStore {
            writer: writer.clone(),
            data_directory: data_directory.clone(),
        })));
        let console_root = console_root()?;
        if !console_root.join("index.html").is_file() {
            bail!(
                "embedded Console assets are missing at {}; run the Console build first",
                console_root.display()
            );
        }
        let api_state = tickr_api::http::routes::build_lite_app_state(
            command_bus,
            read_only,
            coordinator,
            logs,
            self.ready.clone(),
            console_root,
            fleet,
            health_descriptor,
        );
        let api_router = tickr_api::http::routes::build_lite_router(api_state);
        let api_listener = tokio::net::TcpListener::bind(tickr_api::config::api_bind_addr()?)
            .await
            .context("binding Tickr Lite API listener")?;

        let (work_admission_tx, work_admission_rx) = watch::channel(false);
        let mut children: JoinSet<(&'static str, Result<()>)> = JoinSet::new();
        let pickup_cancel = self.cancel.child_token();
        spawn_child(&mut children, "task-pickup-writer", async move {
            pickup_writer.run(pickup_cancel).await;
            Ok(())
        });
        spawn_child(
            &mut children,
            "command-writer",
            command_writer.run(self.cancel.child_token()),
        );
        spawn_child(
            &mut children,
            "scope-writer",
            scope_writer.run(self.cancel.child_token()),
        );
        spawn_child(
            &mut children,
            "tickr-ctx",
            ctx_endpoint.run(self.cancel.child_token()),
        );
        spawn_child(
            &mut children,
            "definition-build",
            after_admission(
                work_admission_rx.clone(),
                self.cancel.child_token(),
                start_local_definition_build_worker(
                    writer.clone(),
                    Arc::new(NixBuildExecutor),
                    format!("tickr-lite-build-{}", Uuid::new_v4()),
                    build_notifications,
                    LocalDefinitionBuildWorkerConfig::default(),
                    self.cancel.child_token(),
                ),
            ),
        );
        spawn_child(
            &mut children,
            "definition-submission",
            after_admission(
                work_admission_rx.clone(),
                self.cancel.child_token(),
                start_local_definition_submission_worker(
                    writer.clone(),
                    format!("tickr-lite-submission-{}", Uuid::new_v4()),
                    submission_notifications,
                    LocalDefinitionSubmissionWorkerConfig::default(),
                    self.cancel.child_token(),
                ),
            ),
        );
        spawn_child(
            &mut children,
            "patch-lifecycle",
            after_admission(
                work_admission_rx.clone(),
                self.cancel.child_token(),
                start_local_patch_worker(
                    writer.clone(),
                    Arc::new(NixBuildExecutor),
                    patch_sender,
                    format!("tickr-lite-patch-{}", Uuid::new_v4()),
                    patch_notifications,
                    LocalPatchWorkerConfig::default(),
                    self.cancel.child_token(),
                ),
            ),
        );
        spawn_child(
            &mut children,
            "replay-lifecycle",
            after_admission(
                work_admission_rx.clone(),
                self.cancel.child_token(),
                start_local_replay_worker(
                    writer.clone(),
                    replay_sender,
                    format!("tickr-lite-replay-{}", Uuid::new_v4()),
                    replay_notifications,
                    LocalReplayWorkerConfig::default(),
                    self.cancel.child_token(),
                ),
            ),
        );
        spawn_child(
            &mut children,
            "relay",
            after_admission(
                work_admission_rx.clone(),
                self.cancel.child_token(),
                run_streaming_lite(self.cancel.child_token(), writer.clone(), relay_roles),
            ),
        );
        spawn_child(
            &mut children,
            "executor",
            after_admission(
                work_admission_rx.clone(),
                self.cancel.child_token(),
                run_executor(executor, self.cancel.child_token()),
            ),
        );
        spawn_child(
            &mut children,
            "compaction-drain",
            after_admission(
                work_admission_rx,
                self.cancel.child_token(),
                run_compaction_drain(writer, data_directory, self.cancel.child_token()),
            ),
        );

        let api_cancel = self.cancel.child_token();
        children.spawn(async move {
            let result = axum::serve(api_listener, api_router)
                .with_graceful_shutdown(api_cancel.cancelled_owned())
                .await
                .context("serving Tickr Lite API");
            ("api", result)
        });

        tokio::task::yield_now().await;
        if self.cancel.is_cancelled() {
            self.ready.store(false, Ordering::Release);
            ctx_handle.clear_ready();
            self.cancel.cancel();
            return shutdown_children(&mut children).await;
        }
        if let Err(startup_failure) = ensure_no_child_exited_before_admission(&mut children) {
            self.ready.store(false, Ordering::Release);
            ctx_handle.clear_ready();
            self.cancel.cancel();
            if let Err(shutdown_failure) = shutdown_children(&mut children).await {
                return Err(startup_failure).context(format!(
                    "bounded startup teardown failed: {shutdown_failure}"
                ));
            }
            return Err(startup_failure);
        }

        ctx_handle.mark_ready();
        self.ready.store(true, Ordering::Release);
        if work_admission_tx.send(true).is_err() {
            self.ready.store(false, Ordering::Release);
            ctx_handle.clear_ready();
            self.cancel.cancel();
            shutdown_children(&mut children)
                .await
                .context("tearing down after work-admission failure")?;
            bail!("Tickr Lite work-admission gate has no critical children");
        }
        println!("Tickr Lite ready");

        let failure = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => None,
            joined = children.join_next() => Some(critical_child_failure(joined)),
        };

        // Availability is withdrawn before any sibling observes cancellation.
        self.ready.store(false, Ordering::Release);
        ctx_handle.clear_ready();
        self.cancel.cancel();
        let shutdown = shutdown_children(&mut children).await;
        match (failure, shutdown) {
            (Some(failure), Ok(())) => Err(failure),
            (Some(failure), Err(shutdown)) => {
                Err(failure).context(format!("bounded Tickr Lite teardown failed: {shutdown}"))
            }
            (None, Err(shutdown)) => Err(shutdown),
            (None, Ok(())) => Ok(()),
        }
    }

    async fn recover_local_journals(
        &self,
        writer: &WriterRepositoryBundle,
        data_directory: &DataDirectory,
    ) -> Result<()> {
        for stream in writer
            .all_local_task_log_streams()
            .await
            .context("inventorying local Log staging streams")?
        {
            let identity = LogStreamIdentity {
                task_instance_id: stream.task_instance_id,
                pickup_generation: stream.pickup_generation,
            };
            if read_final_log(data_directory, &identity)?.is_some() {
                continue;
            }
            let mut staging = LocalLogStagingStream::open_existing(data_directory, identity)
                .context("recovering local Log staging journal")?;
            staging.recover_abnormal_closure()?;
        }
        Ok(())
    }
}

struct CapturedLogStream {
    stream: Arc<Mutex<LocalLogStagingStream>>,
    readers: Vec<tokio::task::JoinHandle<Result<(), String>>>,
    _parent_liveness: tokio::process::ChildStdin,
}

#[derive(Clone)]
struct LiteTaskProcessLauncher {
    nix: NixTaskProcessLauncher,
    ctx: TickrCtxEndpointHandle,
    namespace: String,
    data_directory: Arc<DataDirectory>,
    logs: Arc<Mutex<HashMap<String, CapturedLogStream>>>,
    writer: Arc<WriterRepositoryBundle>,
}

impl LiteTaskProcessLauncher {
    async fn finish_logs(&self, task_id: &str, exit: LogExit) -> Result<(), String> {
        let Some(capture) = self.logs.lock().await.remove(task_id) else {
            return Ok(());
        };
        let mut capture_error = None;
        for reader in capture.readers {
            match reader.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => capture_error = Some(error),
                Err(error) => capture_error = Some(format!("log capture task failed: {error}")),
            }
        }
        let terminal = capture_error.map(LogExit::Error).unwrap_or(exit);
        let result = capture
            .stream
            .lock()
            .await
            .finish_cleanly(terminal)
            .map_err(|error| format!("finish local task log: {error}"));
        result
    }
}

impl TaskProcessLauncher for LiteTaskProcessLauncher {
    async fn spawn(&self, task: &DispatchedTask) -> Result<tokio::process::Child, String> {
        self.nix.spawn(task).await
    }

    async fn spawn_claimed(
        &self,
        task: &DispatchedTask,
        claim: &LocalPickupClaim,
    ) -> Result<tokio::process::Child, String> {
        let requested_scope_id = task.workflow_instance_id;
        let run_id = requested_scope_id.to_string();
        let scope_claim_id = Uuid::new_v5(&requested_scope_id, b"tickr-lite-ctx-scope");
        let scope_id = match self
            .writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: requested_scope_id,
                namespace: &self.namespace,
                run_id: &run_id,
                claim_id: scope_claim_id,
                values: &[],
                now: Utc::now(),
            })
            .await
            .map_err(|error| format!("create tickr-ctx task scope: {error}"))?
        {
            ScopeCreationOutcome::Created | ScopeCreationOutcome::Idempotent => requested_scope_id,
            ScopeCreationOutcome::Collision { existing_scope_id } => existing_scope_id,
            outcome => return Err(format!("create tickr-ctx task scope: {outcome:?}")),
        };
        let task_id = task.task_instance_id.to_string();
        if self.logs.lock().await.contains_key(&task_id) {
            self.ctx.revoke_task(&task_id).await;
            return Err("Task log capture is already registered".to_owned());
        }
        let ctx_environment = self
            .ctx
            .register_task(
                task_id.clone(),
                self.namespace.clone(),
                task.workflow_instance_id.to_string(),
                scope_id,
            )
            .await
            .map_err(|error| format!("register tickr-ctx task grant: {error}"))?;
        let mut process_environment = build_task_environment(
            task,
            &self.namespace,
            task.originating_signal_id,
            &task.gate_signal_ids,
            &task.gate_signal_ids_ambient,
        );
        process_environment.extend(ctx_environment.variables());
        let generation = u64::try_from(claim.pickup_generation)
            .map_err(|_| "pickup generation must be non-negative".to_owned())?;
        let stream_identity = LogStreamIdentity {
            task_instance_id: task.task_instance_id,
            pickup_generation: generation,
        };
        let stream =
            match LocalLogStagingStream::open(self.data_directory.as_ref(), stream_identity) {
                Ok(stream) => Arc::new(Mutex::new(stream)),
                Err(error) => {
                    self.ctx.revoke_task(&task_id).await;
                    return Err(format!("open local task log: {error}"));
                }
            };
        let guardian_executable = std::env::current_exe()
            .map_err(|error| format!("resolve Task process guardian executable: {error}"))?;
        let mut guardian_command = Command::new(guardian_executable);
        guardian_command
            .arg("__task-guardian")
            .arg("--")
            .arg("nix")
            .arg("run")
            .arg(&task.nix_expression_path)
            .args(&task.nix_args)
            .envs(process_environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        #[cfg(unix)]
        guardian_command.process_group(0);
        let mut child = match guardian_command.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.ctx.revoke_task(&task_id).await;
                return Err(format!("spawn Task process guardian: {error}"));
            }
        };
        let Some(parent_liveness) = child.stdin.take() else {
            self.ctx.revoke_task(&task_id).await;
            stop_guardian_after_setup_failure(&mut child).await;
            return Err("Task guardian parent-liveness pipe was not configured".to_owned());
        };
        let Some(stdout) = child.stdout.take() else {
            drop(parent_liveness);
            self.ctx.revoke_task(&task_id).await;
            stop_guardian_after_setup_failure(&mut child).await;
            return Err("Task stdout capture was not configured".to_owned());
        };
        let Some(stderr) = child.stderr.take() else {
            drop(parent_liveness);
            self.ctx.revoke_task(&task_id).await;
            stop_guardian_after_setup_failure(&mut child).await;
            return Err("Task stderr capture was not configured".to_owned());
        };
        let readers = vec![
            tokio::spawn(capture_task_output(stdout, stream.clone())),
            tokio::spawn(capture_task_output(stderr, stream.clone())),
        ];
        self.logs.lock().await.insert(
            task_id,
            CapturedLogStream {
                stream,
                readers,
                _parent_liveness: parent_liveness,
            },
        );
        Ok(child)
    }

    async fn process_exited(
        &self,
        task: &DispatchedTask,
        _claim: &LocalPickupClaim,
        status: &std::process::ExitStatus,
    ) -> Result<(), String> {
        let task_id = task.task_instance_id.to_string();
        self.ctx.revoke_task(&task_id).await;
        self.finish_logs(
            &task_id,
            status
                .code()
                .map(LogExit::Status)
                .unwrap_or(LogExit::NoStatus),
        )
        .await
    }

    async fn process_stopped(
        &self,
        task: &DispatchedTask,
        _claim: &LocalPickupClaim,
    ) -> Result<(), String> {
        let task_id = task.task_instance_id.to_string();
        self.ctx.revoke_task(&task_id).await;
        self.finish_logs(&task_id, LogExit::NoStatus).await
    }
}

async fn capture_task_output<R>(
    mut output: R,
    stream: Arc<Mutex<LocalLogStagingStream>>,
) -> Result<(), String>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = vec![0_u8; 8 * 1024];
    loop {
        let read = output
            .read(&mut bytes)
            .await
            .map_err(|error| format!("read Task process output: {error}"))?;
        if read == 0 {
            return Ok(());
        }
        let mut stream = stream.lock().await;
        let sequence = stream
            .committed_frontier()
            .map_or(0, |frontier| frontier.saturating_add(1));
        let identity = LogRecordIdentity {
            stream: stream.identity().clone(),
            sequence,
        };
        stream
            .accept(identity, bytes[..read].to_vec())
            .map_err(|error| format!("stage Task process output: {error}"))?;
    }
}

async fn stop_guardian_after_setup_failure(child: &mut Child) {
    if tokio::time::timeout(GUARDIAN_TERMINATION_GRACE * 2, child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
    }
}

/// Run the per-Task guardian used by the hidden executable subcommand.
///
/// The workload receives its own process group. The guardian is its parent,
/// forwards handler signals, and independently tears the group down when the
/// formation-owned liveness pipe reaches EOF.
pub async fn run_task_guardian(command: Vec<String>) -> Result<i32> {
    let (program, arguments) = command
        .split_first()
        .ok_or_else(|| anyhow!("Task guardian requires a command"))?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("installing Task guardian SIGTERM handler")?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("installing Task guardian SIGINT handler")?;
    let mut task_command = Command::new(program);
    task_command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(false);
    #[cfg(unix)]
    task_command.process_group(0);
    let mut task = task_command
        .spawn()
        .with_context(|| format!("spawning guarded Task command `{program}`"))?;
    let task_group = task
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .ok_or_else(|| anyhow!("guarded Task command has no valid process-group id"))?;
    let mut parent_liveness = tokio::io::stdin();
    let status = tokio::select! {
        status = task.wait() => {
            let status = status.context("reaping guarded Task process")?;
            signal_guarded_group(task_group, 9)?;
            return Ok(status.code().unwrap_or(1));
        }
        eof = wait_parent_liveness_eof(&mut parent_liveness) => {
            eof.context("observing Task guardian parent-liveness EOF")?;
            terminate_guarded_task(task_group, &mut task).await?
        }
        signal = sigterm.recv() => {
            signal.ok_or_else(|| anyhow!("Task guardian SIGTERM stream closed"))?;
            terminate_guarded_task(task_group, &mut task).await?
        }
        signal = sigint.recv() => {
            signal.ok_or_else(|| anyhow!("Task guardian SIGINT stream closed"))?;
            terminate_guarded_task(task_group, &mut task).await?
        }
    };
    Ok(status.code().unwrap_or(1))
}

async fn wait_parent_liveness_eof<R>(reader: &mut R) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut byte = [0_u8; 1];
    loop {
        if reader.read(&mut byte).await? == 0 {
            return Ok(());
        }
    }
}

async fn terminate_guarded_task(
    task_group: i32,
    task: &mut Child,
) -> Result<std::process::ExitStatus> {
    signal_guarded_group(task_group, 15)?;
    tokio::time::sleep(GUARDIAN_TERMINATION_GRACE).await;
    // Escalate before reaping the leader, so its process-group id cannot be
    // recycled onto an unrelated process between signal and wait.
    signal_guarded_group(task_group, 9)?;
    task.wait()
        .await
        .context("reaping guarded Task process after process-group teardown")
}

fn signal_guarded_group(process_group: i32, signal: i32) -> io::Result<()> {
    if process_group <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Task process-group id must be positive",
        ));
    }
    unsafe extern "C" {
        #[link_name = "kill"]
        fn c_kill(process: i32, signal: i32) -> i32;
    }
    if unsafe { c_kill(-process_group, signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(3) {
        Ok(())
    } else {
        Err(error)
    }
}

async fn after_admission<F>(
    mut admission: watch::Receiver<bool>,
    cancel: CancellationToken,
    future: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    while !*admission.borrow() {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            changed = admission.changed() => {
                changed.map_err(|_| anyhow!("Tickr Lite work-admission gate closed"))?;
            }
        }
    }
    future.await
}

fn ensure_no_child_exited_before_admission(
    children: &mut JoinSet<(&'static str, Result<()>)>,
) -> Result<()> {
    let Some(joined) = children.try_join_next() else {
        return Ok(());
    };
    match joined {
        Ok((name, Ok(()))) => bail!("critical Tickr Lite child `{name}` exited"),
        Ok((name, Err(error))) => {
            Err(error).with_context(|| format!("critical Tickr Lite child `{name}` failed"))
        }
        Err(error) => bail!("critical Tickr Lite child panicked: {error}"),
    }
}

fn critical_child_failure(
    joined: Option<std::result::Result<(&'static str, Result<()>), tokio::task::JoinError>>,
) -> anyhow::Error {
    match joined {
        Some(Ok((name, Ok(())))) => anyhow!("critical Tickr Lite child `{name}` exited"),
        Some(Ok((name, Err(error)))) => {
            error.context(format!("critical Tickr Lite child `{name}` failed"))
        }
        Some(Err(error)) => anyhow!("critical Tickr Lite child panicked: {error}"),
        None => anyhow!("Tickr Lite has no registered critical children"),
    }
}

async fn shutdown_children(children: &mut JoinSet<(&'static str, Result<()>)>) -> Result<()> {
    let settled = tokio::time::timeout(SHUTDOWN_DEADLINE, async {
        let mut first_failure = None;
        while let Some(joined) = children.join_next().await {
            let failure = match joined {
                Ok((_name, Ok(()))) => None,
                Ok((name, Err(error))) => {
                    Some(error.context(format!("Tickr Lite child `{name}` failed during shutdown")))
                }
                Err(error) => Some(anyhow!(
                    "Tickr Lite child panicked during shutdown: {error}"
                )),
            };
            if first_failure.is_none() {
                first_failure = failure;
            }
        }
        first_failure.map_or(Ok(()), Err)
    })
    .await;
    match settled {
        Ok(result) => result,
        Err(_) => {
            children.abort_all();
            while children.join_next().await.is_some() {}
            bail!(
                "critical Tickr Lite children exceeded the {:?} shutdown deadline",
                SHUTDOWN_DEADLINE
            )
        }
    }
}

fn spawn_child<F>(children: &mut JoinSet<(&'static str, Result<()>)>, name: &'static str, future: F)
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    children.spawn(async move { (name, future.await) });
}

async fn run_executor(
    executor: Arc<SafePickupExecutor<LocalTaskPickupWriterClient, LiteTaskProcessLauncher>>,
    cancel: CancellationToken,
) -> Result<()> {
    loop {
        let outcome = executor.run_one();
        tokio::pin!(outcome);
        let outcome = tokio::select! {
            _ = cancel.cancelled() => {
                let task_handler = executor.task_handler();
                let stop_all = task_handler.stop_all();
                let _ = tokio::join!(stop_all, &mut outcome);
                return Ok(());
            }
            outcome = &mut outcome => outcome?,
        };
        if matches!(outcome, PickupOutcome::NoWork) {
            tokio::time::sleep(IDLE_SCAN).await;
        }
    }
}

async fn run_compaction_drain(
    writer: Arc<WriterRepositoryBundle>,
    data_directory: Arc<DataDirectory>,
    cancel: CancellationToken,
) -> Result<()> {
    loop {
        let drain = LocalCompactionDrain::new(writer.as_ref(), data_directory.as_ref());
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            drained = drain.drain_next() => {
                if !drained? {
                    tokio::time::sleep(IDLE_SCAN).await;
                }
            }
        }
    }
}
struct LiteReplayRelaySender {
    writer: Arc<WriterRepositoryBundle>,
}

#[async_trait]
impl ReplayRelaySender for LiteReplayRelaySender {
    async fn send(&self, signal: &tickr_proto::signal::Signal) -> Result<()> {
        tickr_conductor::relay::send_signal(signal).await
    }

    async fn rehydrate(&self, replay_run_id: Uuid, plan: &RehydrationPlan) -> Result<()> {
        let values = local_rehydration_values(plan)?;
        let inputs = values
            .iter()
            .map(|value| ScopeValueInput {
                key: &value.name,
                envelope: &value.bytes,
            })
            .collect::<Vec<_>>();
        let run_id = replay_run_id.to_string();
        let claim_id = Uuid::new_v5(&replay_run_id, b"tickr-lite-rehydration");
        self.writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: replay_run_id,
                namespace: "replay",
                run_id: &run_id,
                claim_id,
                values: &inputs,
                now: Utc::now(),
            })
            .await
            .context("committing local replay rehydration")?;
        Ok(())
    }
}

struct LiteRelayRoleSet {
    pickup: LocalTaskPickupWriterClient,
    writer: Arc<WriterRepositoryBundle>,
    cancellation:
        Arc<SafeCancellationCoordinator<LocalTaskPickupWriterClient, LiteTaskProcessLauncher>>,
    signal_notifier: LocalSignalAppliedNotifier,
}

#[async_trait]
impl LiteRelayRoles for LiteRelayRoleSet {
    async fn relay_connected(
        &self,
        relay_tx: mpsc::Sender<tickr_conductor::proto::ConductorRelayMessage>,
        cycle: CancellationToken,
    ) {
        let pickup = self.pickup.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cycle.cancelled() => return,
                    result = pickup.forward_next_task_event(&relay_tx) => match result {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(error) => {
                            eprintln!("local TaskEvent relay drain failed: {error}");
                            return;
                        }
                    }
                }
                match pickup.forward_next_cancellation_ack(&relay_tx).await {
                    Ok(true) => continue,
                    Ok(false) => tokio::time::sleep(IDLE_SCAN).await,
                    Err(error) => {
                        eprintln!("local cancellation acknowledgement relay drain failed: {error}");
                        return;
                    }
                }
            }
        });
    }

    async fn stage_task_dispatch(&self, payload: &[u8]) -> Result<()> {
        self.pickup
            .stage_dispatch(payload)
            .await
            .map(|_| ())
            .map_err(|error| anyhow!(error))
    }

    async fn stage_task_cancellation(&self, payload: &[u8]) -> Result<()> {
        self.cancellation
            .cancel(payload)
            .await
            .map(|_| ())
            .map_err(|error| anyhow!(error))
    }

    async fn stage_compaction(
        &self,
        payload: &[u8],
    ) -> Result<tickr_conductor::proto::ConductorRelayMessage> {
        LocalCompactionStager::new(self.writer.as_ref())
            .stage_for_relay(payload)
            .await
    }

    fn signal_applied(&self, signal_id: Uuid) {
        self.signal_notifier
            .notify_bytag_cancel_materialized(signal_id);
    }
}

struct LiteLogStore {
    writer: Arc<WriterRepositoryBundle>,
    data_directory: Arc<DataDirectory>,
}

impl LiteLogStore {
    async fn read(
        &self,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
    ) -> Result<(Vec<LogBatch>, Option<EndOfStreamMarker>), LogsError> {
        let stream = self
            .writer
            .local_task_log_streams_for_workflow_instance(workflow_instance_id)
            .await
            .map_err(|error| LogsError::Local(error.to_string()))?
            .into_iter()
            .find(|stream| stream.task_instance_id == task_instance_id)
            .ok_or(LogsError::NotFound)?;
        let identity = LogStreamIdentity {
            task_instance_id,
            pickup_generation: stream.pickup_generation,
        };
        if let Some(final_log) = read_final_log(self.data_directory.as_ref(), &identity)
            .map_err(|error| LogsError::Local(error.to_string()))?
        {
            let batches = final_log
                .records
                .into_iter()
                .map(|record| LogBatch {
                    seq: record.identity.sequence,
                    bytes: record.bytes,
                })
                .collect();
            return Ok((batches, marker(final_log.terminal)));
        }
        let staging = LocalLogStagingStream::open_existing(self.data_directory.as_ref(), identity)
            .map_err(|error| LogsError::Local(error.to_string()))?;
        let mut batches = Vec::new();
        let mut end = None;
        for record in staging.replay() {
            match record {
                ReplayedLogRecord::Accepted { identity, bytes } => batches.push(LogBatch {
                    seq: identity.sequence,
                    bytes,
                }),
                ReplayedLogRecord::EndOfStream { exit } => {
                    end = marker(FinalLogTerminal::EndOfStream { exit })
                }
                ReplayedLogRecord::PreAcceptanceGap { .. }
                | ReplayedLogRecord::AbnormalClosure { .. } => {}
            }
        }
        Ok((batches, end))
    }
}

#[async_trait]
impl LocalTaskLogStore for LiteLogStore {
    async fn fetch_task_logs(
        &self,
        _workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
    ) -> Result<TaskLogs, LogsError> {
        let (batches, marker) = self.read(workflow_instance_id, task_instance_id).await?;
        if batches.is_empty() && marker.is_none() {
            return Err(LogsError::NotFound);
        }
        Ok(TaskLogs {
            content: batches.into_iter().flat_map(|batch| batch.bytes).collect(),
            marker,
        })
    }

    async fn fetch_batches_after(
        &self,
        _workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        after_seq: u64,
    ) -> Result<LogBatchPage, LogsError> {
        let (mut batches, marker) = self.read(workflow_instance_id, task_instance_id).await?;
        batches.retain(|batch| batch.seq > after_seq);
        Ok(LogBatchPage {
            batches,
            marker,
            has_earlier: false,
        })
    }

    async fn fetch_tail(
        &self,
        _workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        tail: usize,
        before_seq: Option<u64>,
    ) -> Result<LogBatchPage, LogsError> {
        let (mut batches, marker) = self.read(workflow_instance_id, task_instance_id).await?;
        if let Some(before) = before_seq {
            batches.retain(|batch| batch.seq < before);
        }
        let has_earlier = batches.len() > tail;
        if has_earlier {
            batches.drain(..batches.len() - tail);
        }
        Ok(LogBatchPage {
            batches,
            marker,
            has_earlier,
        })
    }
}

fn marker(terminal: FinalLogTerminal) -> Option<EndOfStreamMarker> {
    let FinalLogTerminal::EndOfStream { exit } = terminal else {
        return None;
    };
    Some(match exit {
        LogExit::Status(status) => EndOfStreamMarker {
            exit_status: i64::from(status),
            reason: None,
        },
        LogExit::NoStatus => EndOfStreamMarker {
            exit_status: -1,
            reason: None,
        },
        LogExit::Error(reason) => EndOfStreamMarker {
            exit_status: -1,
            reason: Some(reason),
        },
    })
}

fn console_root() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("TICKR_CONSOLE_DIST") {
        return Ok(PathBuf::from(path));
    }
    Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("console/dist"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn work_future_is_not_polled_before_admission() {
        let (admit, admitted) = watch::channel(false);
        let polled = Arc::new(AtomicBool::new(false));
        let observed = polled.clone();
        let child = tokio::spawn(after_admission(
            admitted,
            CancellationToken::new(),
            async move {
                observed.store(true, Ordering::Release);
                Ok(())
            },
        ));

        tokio::task::yield_now().await;
        assert!(!polled.load(Ordering::Acquire));
        admit.send(true).unwrap();
        child.await.unwrap().unwrap();
        assert!(polled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn child_failure_prevents_readiness_transition() {
        let ready = AtomicBool::new(false);
        let mut children = JoinSet::new();
        spawn_child(&mut children, "failed-child", async {
            Err(anyhow!("startup failure"))
        });
        tokio::task::yield_now().await;

        let error = ensure_no_child_exited_before_admission(&mut children).unwrap_err();

        assert!(error.to_string().contains("failed-child"));
        assert!(!ready.load(Ordering::Acquire));
    }

    #[test]
    fn health_projection_comes_from_the_resolved_lite_descriptor() {
        let descriptor = resolve_formation(&FormationSelection::tickr_lite()).unwrap();
        let health = lite_health_descriptor(&descriptor).unwrap();

        assert_eq!(health.profile, HealthFormationProfile::TickrLite);
        assert_eq!(health.topology, HealthFormationTopology::SingleNode);
        assert_eq!(health.executor_count, 1);
        assert!(health.substrates.sqlite);
        assert!(!health.substrates.postgres);
        assert!(!health.substrates.nats);
        assert!(!health.substrates.redis);
        assert!(!health.substrates.object_store);
        assert_eq!(health.roles.len(), descriptor.roles.iter().len());
        let command = health
            .roles
            .iter()
            .find(|role| role.role == HealthCoordinationRole::CommandBus)
            .unwrap();
        assert_eq!(
            command.implementation,
            HealthRoleImplementation::LocalRequestReply
        );
        assert_eq!(
            command.protocol.name,
            "tickr.command-bus.local-request-reply"
        );
    }
}
