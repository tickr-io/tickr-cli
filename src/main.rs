mod tenant_cmd;

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use tickr_api::app::{open_api_repositories, run_api, run_api_with_runtime_readiness};
use tickr_conductor::app::{open_conductor_repositories, run_conductor, run_conductor_with_roles};
use tickr_executor::app::{run_executor, run_executor_with_formation_roles};
use tokio_util::sync::CancellationToken;

use tickr::all_redis_formation::AllRedisProcessAdmission;
use tickr::lite_supervisor::LiteSupervisor;
use tickr::migrate_cmd::{self, MigrationFormation};
use tickr::redis_acl_admission::{
    compose_and_reconstruct_all_redis, DistributedCoordinationBundle,
};
use tickr::tickr_ctx_endpoint::{DistributedTickrCtx, TickrCtxEndpoint, TickrCtxScopeWriter};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum DistributedFormation {
    #[default]
    AllNats,
    AllRedis,
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    /// Select the complete distributed Data-plane formation.
    #[arg(long = "formation", value_enum, default_value_t)]
    distributed_formation: DistributedFormation,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Conductor,
    Api,
    Executor,
    /// Administer Tenants through the loopback-only Frontend API.
    Tenant {
        #[command(subcommand)]
        command: tenant_cmd::TenantCommand,
    },
    /// Run the admitted single-process Tickr Lite formation.
    TickrLite,
    /// Apply and verify the selected Data-plane SQL migrations.
    Migrate {
        /// Install or update Tickr Lite formation metadata after SQLite verification.
        #[arg(long, value_enum, default_value_t = MigrationFormation::Distributed)]
        formation: MigrationFormation,
    },
    /// Internal per-Task process-group guardian.
    #[command(name = "__task-guardian", hide = true)]
    TaskGuardian {
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
}

async fn admitted_all_redis_bundle(
    shutdown: CancellationToken,
    lifecycle_repositories: Option<Arc<tickr_migrations::backend::WriterRepositoryBundle>>,
) -> Result<DistributedCoordinationBundle> {
    let admitted = AllRedisProcessAdmission::from_environment()?
        .admit()
        .await?;
    compose_and_reconstruct_all_redis(
        admitted.formation,
        admitted.monitor,
        lifecycle_repositories,
        shutdown,
    )
    .await
    .context("composing and reconstructing the all-Redis role bundle")
}

async fn supervise_capability_monitor<F>(
    shutdown: CancellationToken,
    mut monitor: tokio::task::JoinHandle<anyhow::Result<()>>,
    component: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    tokio::pin!(component);
    tokio::select! {
        result = &mut component => {
            shutdown.cancel();
            monitor
                .await
                .context("joining the all-Redis capability monitor")??;
            result
        }
        monitored = &mut monitor => {
            shutdown.cancel();
            let monitor_result = monitored
                .context("joining the all-Redis capability monitor")?;
            let component_result = component.await;
            monitor_result?;
            component_result
        }
    }
}

async fn run_all_redis_api(shutdown: CancellationToken) -> Result<()> {
    // Admission must complete before Postgres, object storage, or HTTP exists.
    let admitted = AllRedisProcessAdmission::from_environment()?
        .admit()
        .await?;
    let repositories = open_api_repositories().await?;
    let mut bundle = compose_and_reconstruct_all_redis(
        admitted.formation,
        admitted.monitor,
        None,
        shutdown.clone(),
    )
    .await?;
    let command_bus = bundle
        .command_bus_client()
        .context("all-Redis API requires the CommandBus role")?;
    let log_streams = bundle
        .log_stream_provider()
        .context("all-Redis API requires the LogStaging role")?;
    let fleet_status = bundle
        .executor_fleet_status()
        .context("all-Redis API requires the ExecutorFleetStatus role")?;
    let readiness = bundle.readiness_probe();
    let diagnostics_probe = bundle.diagnostics_probe();
    let diagnostics: tickr_api::http::routes::FormationDiagnostics = Arc::new(move || {
        serde_json::to_value(diagnostics_probe()).expect("Redis diagnostics serialize")
    });
    let monitor = bundle.start_capability_monitor(Duration::from_secs(5), shutdown.clone());
    let result = supervise_capability_monitor(
        shutdown.clone(),
        monitor,
        run_api_with_runtime_readiness(
            shutdown,
            repositories,
            command_bus,
            log_streams,
            None,
            fleet_status,
            Some(readiness),
            Some(diagnostics),
        ),
    )
    .await;
    bundle
        .shutdown_critical_children()
        .await
        .context("joining all-Redis API role children")?;
    result
}

async fn run_all_redis_conductor(shutdown: CancellationToken) -> Result<()> {
    // The external Redis class is proved before the SQL writer is constructed.
    let admitted = AllRedisProcessAdmission::from_environment()?
        .admit()
        .await?;
    let repositories = open_conductor_repositories().await?;
    let mut bundle = compose_and_reconstruct_all_redis(
        admitted.formation,
        admitted.monitor,
        Some(Arc::clone(&repositories)),
        shutdown.clone(),
    )
    .await?;
    let command_bus = bundle
        .command_bus_consumer()
        .context("all-Redis Conductor requires the CommandBus role")?;
    let task_events = bundle
        .task_event_consumer()
        .context("all-Redis Conductor requires the TaskEvents consumer")?;
    let task_event_writer = bundle
        .task_event_writer()
        .context("all-Redis Conductor requires the TaskEvents writer")?;
    let task_dispatch = bundle
        .task_dispatch_publisher()
        .context("all-Redis Conductor requires the TaskDispatch role")?;
    let cancellation = bundle
        .task_cancellation_publisher()
        .context("all-Redis Conductor requires the TaskCancellation publisher")?;
    let cancellation_acks = bundle
        .task_cancellation_ack_consumer()
        .context("all-Redis Conductor requires TaskCancellation acknowledgements")?;
    let compaction = bundle
        .compaction_staging()
        .context("all-Redis Conductor requires the CompactionStaging role")?;
    let compaction_logs = bundle
        .compaction_log_staging()
        .context("all-Redis Conductor requires the LogStaging role")?;
    let scopes = bundle
        .scope_store()
        .context("all-Redis Conductor requires the ScopeStore role")?;
    let event_ingress = bundle
        .event_ingress()
        .context("all-Redis Conductor requires the EventIngress role")?;
    let ingress = bundle
        .ingress_coordinator()
        .context("all-Redis Conductor requires IngressIdempotencyStore")?;
    let signal_applied = bundle
        .signal_applied_roles()
        .context("all-Redis Conductor requires SignalAppliedNotifier")?;
    let lifecycle_work = bundle
        .lifecycle_work()
        .context("all-Redis Conductor requires LifecycleWork")?;
    let sweeper = bundle
        .conductor_liveness_sweeper()
        .context("all-Redis Conductor requires LivenessWatchdog")?;

    let monitor = bundle.start_capability_monitor(Duration::from_secs(5), shutdown.clone());
    let sweeper_shutdown = shutdown.clone();
    let sweeper_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = sweeper_shutdown.cancelled() => return,
                _ = interval.tick() => {
                    if let Err(error) = sweeper.sweep_one_due().await {
                        eprintln!("LivenessWatchdog sweep failed: {error}");
                    }
                }
            }
        }
    });
    let result = supervise_capability_monitor(
        shutdown.clone(),
        monitor,
        run_conductor_with_roles(
            shutdown.clone(),
            repositories,
            command_bus,
            task_events,
            task_event_writer,
            task_dispatch,
            cancellation,
            cancellation_acks,
            compaction,
            compaction_logs,
            scopes,
            event_ingress,
            ingress,
            signal_applied,
            lifecycle_work,
        ),
    )
    .await;
    shutdown.cancel();
    sweeper_handle
        .await
        .context("joining the all-Redis LivenessWatchdog sweeper")?;
    bundle
        .shutdown_critical_children()
        .await
        .context("joining all-Redis Conductor role children")?;
    result
}

async fn run_all_redis_executor(shutdown: CancellationToken) -> Result<()> {
    let mut bundle = admitted_all_redis_bundle(shutdown.clone(), None).await?;
    let handoff = bundle
        .executor_task_handoff()
        .context("all-Redis Executor requires TaskDispatch and LivenessWatchdog")?;
    let task_events = bundle
        .task_event_writer()
        .context("all-Redis Executor requires the TaskEvents role")?;
    let cancellation = bundle
        .executor_task_cancellation()
        .context("all-Redis Executor requires the TaskCancellation role")?;
    let log_streams = bundle
        .log_stream_provider()
        .context("all-Redis Executor requires the LogStaging role")?;
    let fleet_status = bundle
        .executor_fleet_status()
        .context("all-Redis Executor requires ExecutorFleetStatus")?;
    let scope_store = bundle
        .scope_store()
        .context("all-Redis Executor requires the ScopeStore role")?;
    let (scope_writer_client, scope_writer) = TickrCtxScopeWriter::new(Arc::clone(&scope_store));
    let (ctx_handle, ctx_endpoint) =
        TickrCtxEndpoint::bind_distributed_after_recovery(scope_writer_client)?;
    let writer_shutdown = shutdown.child_token();
    let writer_handle = tokio::spawn(scope_writer.run(writer_shutdown));
    let endpoint_shutdown = shutdown.child_token();
    let endpoint_handle = tokio::spawn(ctx_endpoint.run(endpoint_shutdown));
    ctx_handle.mark_ready();
    let task_context = Arc::new(DistributedTickrCtx::new(
        ctx_handle,
        scope_store,
        std::env::var("TICKR_NS").unwrap_or_else(|_| "default".to_owned()),
    ));
    let monitor = bundle.start_capability_monitor(Duration::from_secs(5), shutdown.clone());
    let children_shutdown = shutdown.clone();
    let children = async move {
        let executor = run_executor_with_formation_roles(
            handoff,
            task_events,
            cancellation,
            log_streams,
            fleet_status,
            task_context,
            children_shutdown.clone(),
        );
        tokio::pin!(executor);
        let mut endpoint_handle = endpoint_handle;
        let mut writer_handle = writer_handle;
        tokio::select! {
            result = &mut executor => {
                children_shutdown.cancel();
                endpoint_handle
                    .await
                    .context("joining the distributed tickr-ctx endpoint")??;
                writer_handle
                    .await
                    .context("joining the distributed tickr-ctx writer")??;
                result
            }
            endpoint = &mut endpoint_handle => {
                children_shutdown.cancel();
                let endpoint_result = endpoint
                    .context("joining the distributed tickr-ctx endpoint")?;
                let executor_result = executor.await;
                writer_handle
                    .await
                    .context("joining the distributed tickr-ctx writer")??;
                endpoint_result?;
                executor_result
            }
            writer = &mut writer_handle => {
                children_shutdown.cancel();
                let writer_result = writer
                    .context("joining the distributed tickr-ctx writer")?;
                let executor_result = executor.await;
                endpoint_handle
                    .await
                    .context("joining the distributed tickr-ctx endpoint")??;
                writer_result?;
                executor_result
            }
        }
    };
    let result = supervise_capability_monitor(shutdown, monitor, children).await;
    bundle
        .shutdown_critical_children()
        .await
        .context("joining all-Redis Executor role children")?;
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Commands::TaskGuardian { command } = &cli.command {
        let code = tickr::lite_supervisor::run_task_guardian(command.clone()).await?;
        std::process::exit(code);
    }
    if !matches!(
        &cli.command,
        Commands::Conductor | Commands::Api | Commands::Executor
    ) && cli.distributed_formation != DistributedFormation::AllNats
    {
        bail!("--formation applies only to distributed API, Conductor, and Executor roots");
    }
    let selection = match cli.distributed_formation {
        DistributedFormation::AllNats => tickr::formation::FormationSelection::all_nats(),
        DistributedFormation::AllRedis => tickr::formation::FormationSelection::all_redis(),
    };
    let distributed_root = matches!(
        &cli.command,
        Commands::Conductor | Commands::Api | Commands::Executor
    );
    if distributed_root {
        tickr::formation::resolve_formation(&selection)?;
        if matches!(&cli.command, Commands::Conductor | Commands::Api) {
            tickr_proto::config::data_plane_sql()
                .context("validating data-plane SQL configuration before substrate admission")?;
        }
        if cli.distributed_formation == DistributedFormation::AllNats {
            tickr_conductor::all_nats_formation::connect_and_admit(
                &tickr_proto::config::nats_url(),
            )
            .await
            .context("admitting all-NATS formation before component startup")?;
        }
    }

    let shutdown = CancellationToken::new();
    let future: Pin<Box<dyn Future<Output = Result<()>>>> = match cli.command {
        Commands::Conductor if cli.distributed_formation == DistributedFormation::AllRedis => {
            Box::pin(run_all_redis_conductor(shutdown.clone()))
        }
        Commands::Api if cli.distributed_formation == DistributedFormation::AllRedis => {
            Box::pin(run_all_redis_api(shutdown.clone()))
        }
        Commands::Executor if cli.distributed_formation == DistributedFormation::AllRedis => {
            Box::pin(run_all_redis_executor(shutdown.clone()))
        }
        Commands::Conductor => Box::pin(run_conductor(shutdown.clone())),
        Commands::Api => Box::pin(run_api(shutdown.clone())),
        Commands::Executor => Box::pin(run_executor()),
        Commands::Tenant { command } => Box::pin(tenant_cmd::run(command)),
        Commands::TickrLite => Box::pin(LiteSupervisor::new(shutdown.clone()).run()),
        Commands::Migrate { formation } => Box::pin(migrate_cmd::run(formation)),
        Commands::TaskGuardian { .. } => unreachable!("Task guardian exits before composition"),
    };

    #[cfg(not(madsim))]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::pin!(future);
        tokio::select! {
            result = &mut future => result?,
            _ = sigint.recv() => {
                shutdown.cancel();
                future.await?;
            },
            _ = sigterm.recv() => {
                shutdown.cancel();
                future.await?;
            },
        }
    }

    #[cfg(madsim)]
    future.await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Cli, Commands, DistributedFormation};
    use clap::Parser as _;

    #[test]
    fn distributed_roots_accept_explicit_all_redis_selection() {
        for component in ["api", "conductor", "executor"] {
            let cli = Cli::try_parse_from(["tickr", "--formation", "all-redis", component])
                .expect("all-Redis is a valid distributed formation");
            assert_eq!(cli.distributed_formation, DistributedFormation::AllRedis);
            assert!(matches!(
                cli.command,
                Commands::Api | Commands::Conductor | Commands::Executor
            ));
        }
    }

    #[test]
    fn omitted_distributed_selection_remains_all_nats() {
        let cli = Cli::try_parse_from(["tickr", "api"]).expect("default API command");
        assert_eq!(cli.distributed_formation, DistributedFormation::AllNats);
    }
}
