//! API component boot path. It verifies the selected read-only Data-plane SQL
//! role before opening NATS or the HTTP listener. Migrations remain an
//! out-of-band operator step.

use crate::http;
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub async fn run_api(shutdown_token: CancellationToken) -> Result<()> {
    println!("Starting tickr API component...");

    // Resolve the complete selection and verify the read-only role before any
    // HTTP listener or NATS-backed request path can start.
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

    // Connect to NATS only after SQL selection and verification succeed.
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;

    // Watch channel for graceful HTTP shutdown, fed by the process-wide
    // cancellation token.
    let (http_shutdown_tx, http_shutdown_rx) = watch::channel(false);
    let http_nats = nats.clone();
    let http_repositories = Arc::clone(&repositories);
    let http_server_handle = tokio::spawn(async move {
        if let Err(e) =
            http::start_http_server(http_shutdown_rx, http_nats, http_repositories).await
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
