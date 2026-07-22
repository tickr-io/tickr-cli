//! Durable definition-build processing for Tickr Lite.
//!
//! SQLite lifecycle rows are the source of truth. The bounded channel only
//! shortens scan latency; startup and periodic scans recover every committed
//! eligible row after notification loss or process restart.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::try_join_all;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::definition_repository::{
    DefinitionBuildLeaseRequest, DefinitionBuildSettlementOutcome, DefinitionTaskBuildResult,
    LeasedDefinitionBuildSettlementOutcome, LeasedDefinitionBuildTask,
};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::{BuildExecutor, BuildOutcome, TaskBuildJob};
use crate::waits_on_signal_lifecycle::apply_workflow_state;

/// Bounded best-effort wakeup for newly committed definition-build work.
#[derive(Clone)]
pub struct DefinitionBuildNotifier {
    sender: mpsc::Sender<()>,
}

/// Consumer half of the notification hint, kept opaque so callers cannot use
/// channel state as lifecycle state.
pub struct DefinitionBuildNotificationStream {
    receiver: mpsc::Receiver<()>,
}

/// Construct one bounded notification hint.
pub fn definition_build_notifications(
    capacity: NonZeroUsize,
) -> (DefinitionBuildNotifier, DefinitionBuildNotificationStream) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        DefinitionBuildNotifier { sender },
        DefinitionBuildNotificationStream { receiver },
    )
}

impl DefinitionBuildNotifier {
    /// Request an immediate scan. Full or closed channels deliberately lose
    /// only this hint; the committed SQLite row remains authoritative.
    pub fn notify(&self) {
        let _ = self.sender.try_send(());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LocalDefinitionBuildWorkerConfig {
    pub scan_interval: Duration,
    pub lease_duration: Duration,
    pub batch_size: NonZeroUsize,
}

impl Default for LocalDefinitionBuildWorkerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(5),
            lease_duration: Duration::from_secs(10 * 60),
            batch_size: NonZeroUsize::new(8).expect("definition build batch is non-zero"),
        }
    }
}

/// Run startup reconciliation followed by bounded notification- and timer-led
/// scans until formation cancellation.
pub async fn start_local_definition_build_worker(
    repositories: Arc<WriterRepositoryBundle>,
    executor: Arc<dyn BuildExecutor>,
    lease_owner: String,
    mut notifications: DefinitionBuildNotificationStream,
    config: LocalDefinitionBuildWorkerConfig,
    cancel: CancellationToken,
) -> Result<()> {
    if config.scan_interval.is_zero() {
        anyhow::bail!("definition build scan interval must be non-zero");
    }
    let lease_duration = chrono::Duration::from_std(config.lease_duration)
        .context("definition build lease duration is out of range")?;
    if lease_duration <= chrono::Duration::zero() {
        anyhow::bail!("definition build lease duration must be positive");
    }

    let mut ticker = tokio::time::interval(config.scan_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    scan_once(
        repositories.as_ref(),
        executor.as_ref(),
        &lease_owner,
        lease_duration,
        config.batch_size,
    )
    .await?;

    let mut notifications_open = true;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {
                scan_once(
                    repositories.as_ref(),
                    executor.as_ref(),
                    &lease_owner,
                    lease_duration,
                    config.batch_size,
                ).await?;
            }
            notification = notifications.receiver.recv(), if notifications_open => {
                notifications_open = notification.is_some();
                scan_once(
                    repositories.as_ref(),
                    executor.as_ref(),
                    &lease_owner,
                    lease_duration,
                    config.batch_size,
                ).await?;
            }
        }
    }
    Ok(())
}

async fn scan_once(
    repositories: &WriterRepositoryBundle,
    executor: &dyn BuildExecutor,
    lease_owner: &str,
    lease_duration: chrono::Duration,
    batch_size: NonZeroUsize,
) -> Result<()> {
    let now = Utc::now();
    let leases = repositories
        .lease_definition_build_tasks(DefinitionBuildLeaseRequest {
            owner: lease_owner,
            now,
            expires_at: now + lease_duration,
            limit: batch_size.get(),
        })
        .await?;

    try_join_all(
        leases
            .into_iter()
            .map(|lease| process_leased_task(repositories, executor, lease)),
    )
    .await?;
    Ok(())
}

async fn process_leased_task(
    repositories: &WriterRepositoryBundle,
    executor: &dyn BuildExecutor,
    lease: LeasedDefinitionBuildTask,
) -> Result<()> {
    // Nix realization is idempotent; lease expiry may therefore retry it, while
    // the guarded repository finalizer still permits exactly one winning row.
    let job = TaskBuildJob {
        workflow_id: lease.task.workflow_id,
        workflow_version: lease.task.workflow_version,
        task_id: lease.task.task_id,
        nix_expression_path: lease.task.nix_expression_path.clone(),
    };
    let outcome = executor.build(&job).await;
    let result = match &outcome {
        BuildOutcome::Success => DefinitionTaskBuildResult::Success,
        BuildOutcome::Failure { error } => DefinitionTaskBuildResult::Failure { error },
    };
    let settlement = repositories
        .settle_leased_definition_task_build(&lease, result, Utc::now())
        .await?;

    match settlement {
        LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::Ready(intent),
        ) => {
            if let Err(error) = apply_workflow_state(&intent.definition) {
                eprintln!(
                    "local build worker: waits-on-signal refresh failed for {}: {error}",
                    intent.workflow_id
                );
            }
        }
        LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::BuildFailed,
        ) => {
            if let BuildOutcome::Failure { error } = outcome {
                eprintln!(
                    "local build worker: workflow {} v{} task {} failed: {error}",
                    lease.task.workflow_id, lease.task.workflow_version, lease.task.task_id
                );
            }
        }
        LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::AwaitingTasks
            | DefinitionBuildSettlementOutcome::AlreadySettled(_)
            | DefinitionBuildSettlementOutcome::TaskAlreadySettled,
        )
        | LeasedDefinitionBuildSettlementOutcome::LeaseLost => {}
        LeasedDefinitionBuildSettlementOutcome::Settled(
            DefinitionBuildSettlementOutcome::Absent,
        ) => {
            eprintln!(
                "local build worker: definition build task disappeared for {} v{} task {}",
                lease.task.workflow_id, lease.task.workflow_version, lease.task.task_id
            );
        }
    }
    Ok(())
}
