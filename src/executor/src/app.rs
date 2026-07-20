use crate::task_handler::TaskHandler;
use async_nats;
use std::sync::Arc;
use tokio;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub async fn run_executor() -> anyhow::Result<()> {
    println!("Starting executor...");

    // Initialize logging
    env_logger::init();

    // Connect to NATS
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;
    let nats = Arc::new(nats);

    let executor_id = Uuid::new_v4();
    println!("Executor ID: {}", executor_id);

    // Initialize task handler
    let mut handler = TaskHandler::new(Arc::clone(&nats), executor_id);

    // Sweep any task-log spill files orphaned by a prior crash before taking
    // work — orphans are never read, only cleaned, so first disk use doesn't
    // accrete across crashes.
    handler.sweep_orphaned_spills();

    // Ensure the Log staging stream exists for task-log batches
    if let Err(e) = handler.init_log_stream().await {
        eprintln!("Failed to initialize log stream: {}", e);
        eprintln!("Continuing anyway, logs will not be stored");
    }

    // Create shutdown token
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    // Trip the shutdown token on either SIGTERM or SIGINT. The formation is
    // brought down with `overmind quit`, which sends SIGTERM — catching only
    // Ctrl-C (SIGINT) would miss the real teardown path and leak task trees.
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

    // Poll and handle tasks in a loop
    handler.poll_and_handle_tasks(shutdown).await?;

    // Tear down any task subprocess groups still in flight before exiting so
    // `overmind quit` doesn't leave the nix task trees reparented to init.
    handler.shutdown_running_tasks().await;

    println!("Executor stopped gracefully.");
    Ok(())
}
