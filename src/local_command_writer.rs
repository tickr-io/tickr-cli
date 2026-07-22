//! Tickr Lite's Conductor-owned local Command writer.
//!
//! This is the composition seam between the API component's bounded local
//! request/reply transport and the existing production Command dispatcher. One
//! instance owns the only receiver and processes requests serially against one
//! writer repository bundle held by `ApiCommandsState`.

use tickr_api::commands::client::CommandBus;
use tickr_api::commands::local::{LocalCommandBusConfig, LocalCommandWriter as RequestReceiver};
use tickr_conductor::api_commands_consumer::{handle_local_request, ApiCommandDispatchState};
use tokio_util::sync::CancellationToken;

/// Sole local Command receiver for a Tickr Lite formation.
pub struct LocalCommandWriter<S> {
    state: S,
    receiver: RequestReceiver,
}

impl<S> LocalCommandWriter<S>
where
    S: ApiCommandDispatchState + Clone + Send + Sync + 'static,
{
    /// Construct one API-side bus handle and its sole Conductor-owned writer.
    pub fn new(state: S, config: LocalCommandBusConfig) -> (CommandBus, Self) {
        let (command_bus, receiver) = CommandBus::local(config);
        (command_bus, Self { state, receiver })
    }

    /// Run the writer until formation cancellation or API shutdown.
    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let state = self.state;
        self.receiver
            .run(cancel, move |payload| {
                let state = state.clone();
                async move { handle_local_request(&state, &payload).await }
            })
            .await;
        Ok(())
    }
}
