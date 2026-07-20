//! API component boot path. Connects the same data substrate the conductor
//! uses — NATS, the conductor-side Postgres archive — and starts the HTTP
//! server that serves the UI's read surface.
//!
//! The API never runs migrations: the schema is applied out-of-band by the
//! `tickr migrate` operator step. The API does, however, *verify* the
//! conductor-side schema it reads is current at boot and refuses to start if it
//! is stale. That closes the fresh-init race by construction: rather than
//! racing the conductor's old startup migrations and serving "relation does not
//! exist" until they finished, the API cannot come up until `just migrate` has
//! brought the schema current.

use crate::http;
use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub async fn run_api(shutdown_token: CancellationToken) -> Result<()> {
    println!("Starting tickr API component...");

    // Connect to NATS (live log batches; future write-forwarding slices).
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;

    // Connect to the conductor-side Postgres — the per-tenant archive the read
    // endpoints query. The API is a second reader against the same database the
    // conductor writes; it does not own or migrate the schema.
    let pg_url = tickr_proto::config::conductor_postgres_url();
    println!("Connecting to configured conductor Postgres...");
    let pg_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&pg_url)
        .await?;

    // Verify the conductor schema is current before serving. The API reads the
    // schema but never migrates it; refusing to boot on drift removes the
    // fresh-init race against the migrate step and turns a stale schema into a
    // clear boot error (naming `just migrate`) instead of deep-runtime 500s.
    tickr_migrations::verify_current(tickr_migrations::MigrationTarget::Conductor, &pg_pool)
        .await?;
    println!("Conductor schema verified.");
    let pg_pool = Arc::new(pg_pool);

    // Watch channel for graceful HTTP shutdown, fed by the process-wide
    // cancellation token.
    let (http_shutdown_tx, http_shutdown_rx) = watch::channel(false);
    let http_nats = nats.clone();
    let http_pool = Arc::clone(&pg_pool);
    let http_server_handle = tokio::spawn(async move {
        if let Err(e) = http::start_http_server(http_shutdown_rx, http_nats, http_pool).await {
            eprintln!("API HTTP server error: {}", e);
        }
    });

    shutdown_token.cancelled().await;
    println!("Shutdown signal received, stopping API component...");

    let _ = http_shutdown_tx.send(true);
    if let Err(e) = http_server_handle.await {
        eprintln!("Error waiting for API HTTP server task: {}", e);
    }

    println!("API component stopped gracefully.");
    Ok(())
}
