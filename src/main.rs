use std::{future::Future, pin::Pin};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tickr_api::app::run_api;
use tickr_conductor::app::run_conductor;
use tickr_executor::app::run_executor;
use tokio_util::sync::CancellationToken;

use tickr::lite_supervisor::LiteSupervisor;
use tickr::migrate_cmd::{self, MigrationFormation};

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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Commands::TaskGuardian { command } = &cli.command {
        let code = tickr::lite_supervisor::run_task_guardian(command.clone()).await?;
        std::process::exit(code);
    }
    let shutdown = CancellationToken::new();
    let future: Pin<Box<dyn Future<Output = Result<()>>>> = match cli.command {
        Commands::Conductor => Box::pin(run_conductor(shutdown.clone())),
        Commands::Api => Box::pin(run_api(shutdown.clone())),
        Commands::Executor => Box::pin(run_executor()),
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
