//! `tickr-ctx` — inter-task context CLI.
//!
//! Tasks running inside the tickr engine call this binary to publish and
//! consume typed values through NATS JetStream KV. Scoping (namespace, run id,
//! task id) is read from environment variables injected by the executor at
//! task spawn time, so the common case is `tickr-ctx capture <k> <v>` /
//! `tickr-ctx get <k>` with no flags.
//!
//! See `research/inter-task-comms/synthesis.md` and `notes/secrets-handling-idea.md`.

mod ambient;
mod cli;
mod envelope;
mod scope;
mod store;

use clap::Parser;

#[tokio::main]
async fn main() {
    let args = cli::Cli::parse();
    let exit = match cli::run(args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("tickr-ctx: {:#}", e);
            // Default error mapping: anyhow chain whose root mentions NATS
            // gets mapped to 5; everything else to 2. Subcommands return
            // their own specific code via Ok(code) when they want precision.
            if format!("{:#}", e).to_lowercase().contains("nats") {
                5
            } else {
                2
            }
        }
    };
    std::process::exit(exit);
}
