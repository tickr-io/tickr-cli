//! `tickr-ctx` — inter-task context CLI.
//!
//! Tasks running inside the tickr engine call this binary to publish and
//! consume typed values through NATS JetStream KV. Scoping (namespace, run id,
//! task id) is read from environment variables injected by the executor at
//! task spawn time, so the common case is `tickr-ctx capture <k> <v>` /
//! `tickr-ctx get <k>` with no flags.
//!
//! See `research/inter-task-comms/synthesis.md` and `notes/secrets-handling-idea.md`.

mod cli;

pub use tickr_ctx::{ambient, envelope, local, nats_scope, scope, store};

use clap::Parser;

#[tokio::main]
async fn main() {
    let args = cli::Cli::parse();
    let exit = match cli::run(args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("tickr-ctx: {:#}", e);
            // Transport loss remains the existing transient exit 5. Typed
            // local identity and bound refusals use the contract-failure exit.
            let message = format!("{:#}", e).to_lowercase();
            if message.contains("nats") || message.contains("endpoint unavailable") {
                5
            } else if message.contains("identity rejected") || message.contains("bound exceeded") {
                4
            } else {
                2
            }
        }
    };
    std::process::exit(exit);
}
