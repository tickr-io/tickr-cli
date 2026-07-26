//! API component boot path. It verifies the selected read-only Data-plane SQL
//! role before opening NATS or the HTTP listener. Migrations remain an
//! out-of-band operator step.

use crate::commands::client::CommandBus;
use crate::http;
use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tickr_executor::local_pickup::ExecutorFleetStatus;
use tickr_executor::log_stream::{AllNatsLogStreamProvider, LogStreamProvider};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub async fn open_api_repositories(
) -> Result<Arc<tickr_migrations::backend::ReadOnlyRepositoryBundle>> {
    let selection =
        tickr_proto::config::data_plane_sql().context("resolving data-plane SQL configuration")?;
    println!(
        "Opening {} data-plane SQL read-only role...",
        selection.implementation()
    );
    let repositories = Arc::new(
        crate::repository::configure_read_only(selection)
            .await
            .context("opening selected data-plane SQL read-only role")?,
    );
    println!("Data-plane SQL read-only schema verified.");
    Ok(repositories)
}

pub async fn run_api(shutdown_token: CancellationToken) -> Result<()> {
    let repositories = open_api_repositories().await?;
    let command_bus = CommandBus::connect_nats(&tickr_proto::config::nats_url()).await?;
    let log_nats = Arc::new(async_nats::connect(tickr_proto::config::nats_url()).await?);
    let log_streams = Arc::new(AllNatsLogStreamProvider::new(
        log_nats,
        Duration::from_secs(5),
    ));
    run_api_with_repositories(shutdown_token, repositories, command_bus, log_streams).await
}

pub async fn run_api_with_repositories(
    shutdown_token: CancellationToken,
    repositories: Arc<tickr_migrations::backend::ReadOnlyRepositoryBundle>,
    command_bus: CommandBus,
    log_streams: Arc<dyn LogStreamProvider>,
) -> Result<()> {
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;
    let fleet_status = Arc::new(
        tickr_executor::component_liveness::NatsExecutorFleetStatus::new(
            nats.clone(),
            tickr_executor::task_liveness::LivenessConfig::from_env().timeout,
        ),
    );
    run_api_with_selected_fleet_status(
        shutdown_token,
        repositories,
        command_bus,
        log_streams,
        Some(nats),
        fleet_status,
    )
    .await
}

/// Run the API with a formation-selected observational fleet role.
pub async fn run_api_with_selected_fleet_status(
    shutdown_token: CancellationToken,
    repositories: Arc<tickr_migrations::backend::ReadOnlyRepositoryBundle>,
    command_bus: CommandBus,
    log_streams: Arc<dyn LogStreamProvider>,
    nats: Option<async_nats::Client>,
    executor_fleet: Arc<dyn ExecutorFleetStatus>,
) -> Result<()> {
    run_api_with_runtime_readiness(
        shutdown_token,
        repositories,
        command_bus,
        log_streams,
        nats,
        executor_fleet,
        None,
        None,
    )
    .await
}

pub async fn run_api_with_runtime_readiness(
    shutdown_token: CancellationToken,
    repositories: Arc<tickr_migrations::backend::ReadOnlyRepositoryBundle>,
    command_bus: CommandBus,
    log_streams: Arc<dyn LogStreamProvider>,
    nats: Option<async_nats::Client>,
    executor_fleet: Arc<dyn ExecutorFleetStatus>,
    readiness: Option<http::routes::FormationReadiness>,
    diagnostics: Option<http::routes::FormationDiagnostics>,
) -> Result<()> {
    println!("Starting tickr API component...");
    log_streams.prepare().await?;

    let (http_shutdown_tx, http_shutdown_rx) = watch::channel(false);
    let http_repositories = Arc::clone(&repositories);
    let http_server_handle = tokio::spawn(async move {
        if let Err(e) = http::start_http_server_with_runtime_readiness(
            http_shutdown_rx,
            nats,
            log_streams,
            http_repositories,
            command_bus,
            executor_fleet,
            readiness,
            diagnostics,
        )
        .await
        {
            eprintln!("API HTTP server error: {}", e);
        }
    });

    shutdown_token.cancelled().await;
    println!("Shutdown signal received, stopping API component...");
    let _ = http_shutdown_tx.send(true);
    if let Err(e) = http_server_handle.await {
        eprintln!("Error waiting for API HTTP server task: {}", e);
    }
    repositories.close().await;
    println!("API component stopped gracefully.");
    Ok(())
}
