use std::{future::Future, pin::Pin};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tickr_api::app::run_api;
use tickr_conductor::app::run_conductor;
use tickr_executor::app::run_executor;
use tokio_util::sync::CancellationToken;

mod migrate_cmd;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Conductor,
    Api,
    Executor,
    /// Apply the conductor-owned Postgres migrations.
    Migrate,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let shutdown = CancellationToken::new();
    let future: Pin<Box<dyn Future<Output = Result<()>>>> = match cli.command {
        Commands::Conductor => Box::pin(run_conductor(shutdown.clone())),
        Commands::Api => Box::pin(run_api(shutdown.clone())),
        Commands::Executor => Box::pin(run_executor()),
        Commands::Migrate => Box::pin(migrate_cmd::run()),
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
