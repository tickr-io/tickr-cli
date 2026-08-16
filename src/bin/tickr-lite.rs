use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tickr::lite_supervisor::LiteSupervisor;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(author, version, about = "Run the single-process Tickr Lite formation")]
struct Cli {
    #[command(subcommand)]
    command: Option<InternalCommand>,
}

#[derive(Debug, Subcommand)]
enum InternalCommand {
    /// Internal per-Task process-group guardian.
    #[command(name = "__task-guardian", hide = true)]
    TaskGuardian {
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Rustls crypto provider was already installed"))?;
    let cli = Cli::parse();
    if let Some(InternalCommand::TaskGuardian { command }) = cli.command {
        let code = tickr::lite_supervisor::run_task_guardian(command).await?;
        std::process::exit(code);
    }

    let profile = tickr::setup_cmd::load_and_apply_profile()?;
    tickr::setup_cmd::change_to_release_home(profile.as_ref())?;

    let shutdown = CancellationToken::new();
    let future = LiteSupervisor::new(shutdown.clone()).run();
    tokio::pin!(future);

    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt()).context("installing SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("installing SIGTERM handler")?;
    tokio::select! {
        result = &mut future => result?,
        _ = sigint.recv() => {
            shutdown.cancel();
            future.await?;
        }
        _ = sigterm.recv() => {
            shutdown.cancel();
            future.await?;
        }
    }

    Ok(())
}
