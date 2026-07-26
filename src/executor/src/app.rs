use crate::component_liveness::NatsExecutorFleetStatus;
use crate::local_pickup::{
    ExecutorFleetStatus, SafeAttemptOutcomeHandoff, SafeCancellationRole, SafeLivenessWatchdog,
    SafePickupWriter,
};
use crate::log_stream::LogStreamProvider;
use crate::nats_pickup::NatsPickupHandoff;
use crate::task_handler::{TaskContextProvider, TaskHandler};
use crate::task_liveness::LivenessConfig;
use async_nats;
use std::sync::Arc;
use tickr_proto::coord::TaskEventWriter;
use tokio;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub async fn run_executor() -> anyhow::Result<()> {
    let (nats, executor_id) = open_executor_root().await?;
    let fleet_status = open_all_nats_fleet_status(nats.as_ref()).await?;
    run_task_handler(TaskHandler::new(nats, executor_id), fleet_status).await
}

/// Run the production Executor renewal path with an admitted liveness role.
///
/// The component receives only the formation-neutral watchdog contract; its
/// substrate client remains owned by the adapter.
pub async fn run_executor_with_liveness<L>(liveness: L) -> anyhow::Result<()>
where
    L: SafeLivenessWatchdog + Clone,
{
    let (nats, executor_id) = open_executor_root().await?;
    let fleet_status = open_all_nats_fleet_status(nats.as_ref()).await?;
    run_task_handler(
        TaskHandler::with_liveness_watchdog(nats, executor_id, liveness),
        fleet_status,
    )
    .await
}

/// Run the production Executor with admitted liveness and TaskEvents roles.
pub async fn run_executor_with_roles<L>(
    liveness: L,
    task_events: Arc<dyn TaskEventWriter>,
) -> anyhow::Result<()>
where
    L: SafeLivenessWatchdog + Clone,
{
    let (nats, executor_id) = open_executor_root().await?;
    let fleet_status = open_all_nats_fleet_status(nats.as_ref()).await?;
    run_task_handler(
        TaskHandler::with_task_events(nats, executor_id, liveness, task_events),
        fleet_status,
    )
    .await
}
/// Run the production Executor pickup loop from an admitted TaskDispatch
/// safe-handoff contract. The component never receives a substrate client.
pub async fn run_executor_with_dispatch_roles<H, C>(
    handoff: H,
    task_events: Arc<dyn TaskEventWriter>,
    cancellation: C,
    log_streams: Arc<dyn LogStreamProvider>,
    fleet_status: Arc<dyn ExecutorFleetStatus>,
) -> anyhow::Result<()>
where
    H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    C: SafeCancellationRole + Clone,
{
    let (nats, executor_id) = open_executor_root().await?;
    run_task_handler_with_dispatch(
        TaskHandler::with_selected_task_roles(nats, executor_id, task_events, cancellation)
            .with_log_streams(log_streams),
        handoff,
        fleet_status,
    )
    .await
}

/// Run the distributed Executor from substrate-neutral role interfaces only.
/// No NATS connection or resource is created on this path.
pub async fn run_executor_with_formation_roles<H, C>(
    handoff: H,
    task_events: Arc<dyn TaskEventWriter>,
    cancellation: C,
    log_streams: Arc<dyn LogStreamProvider>,
    fleet_status: Arc<dyn ExecutorFleetStatus>,
    task_context: Arc<dyn TaskContextProvider>,
    shutdown: CancellationToken,
) -> anyhow::Result<()>
where
    H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    C: SafeCancellationRole + Clone,
{
    println!("Starting executor...");
    let _ = env_logger::try_init();
    let executor_id = Uuid::new_v4();
    println!("Executor ID: {executor_id}");
    run_task_handler_with_dispatch_and_shutdown(
        TaskHandler::with_substrate_neutral_roles(
            executor_id,
            task_events,
            cancellation,
            log_streams,
            task_context,
        ),
        handoff,
        fleet_status,
        shutdown,
    )
    .await
}

async fn open_executor_root() -> anyhow::Result<(Arc<async_nats::Client>, Uuid)> {
    println!("Starting executor...");
    env_logger::init();
    let nats = Arc::new(async_nats::connect(tickr_proto::config::nats_url()).await?);
    let executor_id = Uuid::new_v4();
    println!("Executor ID: {executor_id}");
    Ok((nats, executor_id))
}

async fn run_task_handler<L>(
    mut handler: TaskHandler<L>,
    fleet_status: Arc<dyn ExecutorFleetStatus>,
) -> anyhow::Result<()>
where
    L: SafeLivenessWatchdog + Clone,
{
    // Accepted Log durability is admitted before task pickup; there is no
    // discard fallback that could make an unaccepted stream appear complete.
    handler.init_log_stream().await?;

    let shutdown = install_shutdown_handler();

    handler
        .poll_and_handle_tasks(shutdown, fleet_status)
        .await?;
    handler.shutdown_running_tasks().await;

    println!("Executor stopped gracefully.");
    Ok(())
}
async fn open_all_nats_fleet_status(
    nats: &async_nats::Client,
) -> anyhow::Result<Arc<dyn ExecutorFleetStatus>> {
    let fleet_status =
        NatsExecutorFleetStatus::new(nats.clone(), LivenessConfig::from_env().timeout);
    fleet_status.prepare().await?;
    Ok(Arc::new(fleet_status))
}

async fn run_task_handler_with_dispatch<H, C>(
    handler: TaskHandler<NatsPickupHandoff, C>,
    handoff: H,
    fleet_status: Arc<dyn ExecutorFleetStatus>,
) -> anyhow::Result<()>
where
    H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    C: SafeCancellationRole + Clone,
{
    let shutdown = install_shutdown_handler();
    run_task_handler_with_dispatch_and_shutdown(handler, handoff, fleet_status, shutdown).await
}

async fn run_task_handler_with_dispatch_and_shutdown<H, C>(
    mut handler: TaskHandler<NatsPickupHandoff, C>,
    handoff: H,
    fleet_status: Arc<dyn ExecutorFleetStatus>,
    shutdown: CancellationToken,
) -> anyhow::Result<()>
where
    H: SafePickupWriter + SafeAttemptOutcomeHandoff,
    C: SafeCancellationRole + Clone,
{
    handler.init_log_stream().await?;
    handler
        .poll_and_handle_selected_dispatch(handoff, fleet_status, shutdown)
        .await?;
    handler.shutdown_running_tasks().await;
    println!("Executor stopped gracefully.");
    Ok(())
}

fn install_shutdown_handler() -> CancellationToken {
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => println!("Received SIGTERM, shutting down..."),
            _ = sigint.recv() => println!("Received SIGINT, shutting down..."),
        }
        shutdown_clone.cancel();
    });
    #[cfg(not(unix))]
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("Received Ctrl+C, shutting down...");
        shutdown_clone.cancel();
    });
    shutdown
}
