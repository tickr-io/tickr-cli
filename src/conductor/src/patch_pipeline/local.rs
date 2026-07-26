//! Durable Patch build and lifecycle reconciliation over authoritative rows.
//!
//! The bounded local channel only shortens scan latency; startup and periodic
//! scans recover work after notification loss or process restart. Distributed
//! formation notifications use the same scan law.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::try_join_all;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::patch_repository::{
    LeasedPatchBuildSettlementOutcome, LeasedPatchBuildTask, LeasedPatchLifecycle,
    LeasedPatchSubmissionOutcome, PatchBuildLeaseRequest, PatchBuildSettlementOutcome,
    PatchLifecycleLeaseRequest, PatchTaskBuildResult,
};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::{envelope_from_repository_row, PatchRelaySender};
use crate::build_pipeline::{BuildExecutor, BuildOutcome, TaskBuildJob};
use crate::lifecycle_work::{LifecycleClaimAdmission, LifecyclePipeline, OpenLifecycleClaims};

/// Bounded best-effort wakeup for newly committed Patch work.
#[derive(Clone)]
pub struct PatchWorkNotifier {
    sender: mpsc::Sender<()>,
}

/// Consumer half of the notification hint, opaque to lifecycle callers.
pub struct PatchWorkNotificationStream {
    receiver: mpsc::Receiver<()>,
}

/// Construct one bounded notification hint.
pub fn patch_work_notifications(
    capacity: NonZeroUsize,
) -> (PatchWorkNotifier, PatchWorkNotificationStream) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        PatchWorkNotifier { sender },
        PatchWorkNotificationStream { receiver },
    )
}

impl PatchWorkNotifier {
    /// Request an immediate durable scan. A full or closed channel loses only
    /// this hint; no committed Patch row is acknowledged by the channel.
    pub fn notify(&self) -> bool {
        self.sender.try_send(()).is_ok()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PatchReconcilerConfig {
    pub scan_interval: Duration,
    pub build_lease_duration: Duration,
    pub lifecycle_lease_duration: Duration,
    pub lifecycle_min_age: Duration,
    pub batch_size: NonZeroUsize,
}

impl Default for PatchReconcilerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(5),
            build_lease_duration: Duration::from_secs(10 * 60),
            lifecycle_lease_duration: Duration::from_secs(30),
            lifecycle_min_age: Duration::from_secs(10),
            batch_size: NonZeroUsize::new(8).expect("Patch batch is non-zero"),
        }
    }
}

pub async fn start_local_patch_worker(
    repositories: Arc<WriterRepositoryBundle>,
    executor: Arc<dyn BuildExecutor>,
    sender: Arc<dyn PatchRelaySender>,
    lease_owner: String,
    notifications: PatchWorkNotificationStream,
    config: PatchReconcilerConfig,
    cancel: CancellationToken,
) -> Result<()> {
    start_local_patch_worker_with_claim_admission(
        repositories,
        executor,
        sender,
        lease_owner,
        notifications,
        Arc::new(OpenLifecycleClaims),
        config,
        cancel,
    )
    .await
}

/// Run startup reconciliation followed by bounded notification- and timer-led
/// scans until formation cancellation.
pub async fn start_local_patch_worker_with_claim_admission(
    repositories: Arc<WriterRepositoryBundle>,
    executor: Arc<dyn BuildExecutor>,
    sender: Arc<dyn PatchRelaySender>,
    lease_owner: String,
    mut notifications: PatchWorkNotificationStream,
    claim_admission: Arc<dyn LifecycleClaimAdmission>,
    config: PatchReconcilerConfig,
    cancel: CancellationToken,
) -> Result<()> {
    if config.scan_interval.is_zero() {
        anyhow::bail!("Patch scan interval must be non-zero");
    }
    let build_lease_duration = chrono::Duration::from_std(config.build_lease_duration)
        .context("Patch build lease duration is out of range")?;
    let lifecycle_lease_duration = chrono::Duration::from_std(config.lifecycle_lease_duration)
        .context("Patch lifecycle lease duration is out of range")?;
    if build_lease_duration <= chrono::Duration::zero() {
        anyhow::bail!("Patch build lease duration must be positive");
    }
    if lifecycle_lease_duration <= chrono::Duration::zero() {
        anyhow::bail!("Patch lifecycle lease duration must be positive");
    }
    let lifecycle_min_age = chrono::Duration::from_std(config.lifecycle_min_age)
        .context("Patch lifecycle minimum age is out of range")?;

    let mut ticker = tokio::time::interval(config.scan_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    scan_once_with_claim_admission(
        repositories.as_ref(),
        executor.as_ref(),
        sender.as_ref(),
        &lease_owner,
        build_lease_duration,
        lifecycle_lease_duration,
        chrono::Duration::zero(),
        config.batch_size,
        claim_admission.as_ref(),
    )
    .await?;

    let mut notifications_open = true;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {
                scan_once_with_claim_admission(
                    repositories.as_ref(),
                    executor.as_ref(),
                    sender.as_ref(),
                    &lease_owner,
                    build_lease_duration,
                    lifecycle_lease_duration,
                    lifecycle_min_age,
                    config.batch_size,
                    claim_admission.as_ref(),
                ).await?;
            }
            notification = notifications.receiver.recv(), if notifications_open => {
                if notification.is_some() {
                    scan_once_with_claim_admission(
                        repositories.as_ref(),
                        executor.as_ref(),
                        sender.as_ref(),
                        &lease_owner,
                        build_lease_duration,
                        lifecycle_lease_duration,
                        chrono::Duration::zero(),
                        config.batch_size,
                        claim_admission.as_ref(),
                    ).await?;
                } else {
                    notifications_open = false;
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn scan_once_with_claim_admission(
    repositories: &WriterRepositoryBundle,
    executor: &dyn BuildExecutor,
    sender: &dyn PatchRelaySender,
    lease_owner: &str,
    build_lease_duration: chrono::Duration,
    lifecycle_lease_duration: chrono::Duration,
    lifecycle_min_age: chrono::Duration,
    batch_size: NonZeroUsize,
    claim_admission: &dyn LifecycleClaimAdmission,
) -> Result<()> {
    // Re-drive already committed apply intents before opening fresh build work.
    // This avoids a startup build finalizer and startup reconciliation racing to
    // publish the same newly-Submitted row in one scan.
    scan_lifecycle(
        repositories,
        sender,
        lease_owner,
        lifecycle_lease_duration,
        lifecycle_min_age,
        batch_size,
        claim_admission,
    )
    .await?;
    scan_builds(
        repositories,
        executor,
        sender,
        lease_owner,
        build_lease_duration,
        batch_size,
        claim_admission,
    )
    .await
}

async fn scan_builds(
    repositories: &WriterRepositoryBundle,
    executor: &dyn BuildExecutor,
    sender: &dyn PatchRelaySender,
    lease_owner: &str,
    lease_duration: chrono::Duration,
    batch_size: NonZeroUsize,
    claim_admission: &dyn LifecycleClaimAdmission,
) -> Result<()> {
    let now = Utc::now();
    if !repositories.has_reclaimable_patch_build(now).await?
        || !claim_admission.claims_open(LifecyclePipeline::PatchBuild)
    {
        return Ok(());
    }
    let leases = repositories
        .lease_patch_build_tasks(PatchBuildLeaseRequest {
            owner: lease_owner,
            now,
            expires_at: now + lease_duration,
            limit: batch_size.get(),
        })
        .await?;
    try_join_all(
        leases
            .into_iter()
            .map(|lease| process_leased_build(repositories, executor, sender, lease)),
    )
    .await?;
    Ok(())
}

async fn process_leased_build(
    repositories: &WriterRepositoryBundle,
    executor: &dyn BuildExecutor,
    sender: &dyn PatchRelaySender,
    lease: LeasedPatchBuildTask,
) -> Result<()> {
    // Nix realization is idempotent. Lease expiry may retry it, while guarded
    // settlement leaves exactly one winning Patch finalizer transition.
    let input = TaskBuildJob {
        workflow_id: lease.task.patch_key,
        workflow_version: 0,
        task_id: lease.task.task_id,
        nix_expression_path: lease.task.nix_expression_path.clone(),
    };
    let outcome = executor.build(&input).await;
    let result = match &outcome {
        BuildOutcome::Success => PatchTaskBuildResult::Success,
        BuildOutcome::Failure { error } => PatchTaskBuildResult::Failure { error },
    };
    let settlement = repositories
        .settle_leased_patch_task_build(&lease, result, Utc::now())
        .await?;

    match settlement {
        LeasedPatchBuildSettlementOutcome::Settled(PatchBuildSettlementOutcome::Submitted(row)) => {
            let envelope = envelope_from_repository_row(&row);
            if let Err(error) = sender.send(&envelope).await {
                eprintln!(
                    "Patch reconciler: apply relay failed for {} (will re-drive): {error}",
                    lease.task.patch_key
                );
            }
        }
        LeasedPatchBuildSettlementOutcome::Settled(PatchBuildSettlementOutcome::BuildFailed) => {
            if let BuildOutcome::Failure { error } = outcome {
                eprintln!(
                    "Patch reconciler: Patch {} task {} failed: {error}",
                    lease.task.patch_key, lease.task.task_id
                );
            }
        }
        LeasedPatchBuildSettlementOutcome::Settled(
            PatchBuildSettlementOutcome::AwaitingTasks
            | PatchBuildSettlementOutcome::AlreadySettled(_)
            | PatchBuildSettlementOutcome::TaskAlreadySettled,
        )
        | LeasedPatchBuildSettlementOutcome::LeaseLost => {}
        LeasedPatchBuildSettlementOutcome::Settled(PatchBuildSettlementOutcome::Absent) => {
            eprintln!(
                "Patch reconciler: build task disappeared for {}/{}",
                lease.task.patch_key, lease.task.task_id
            );
        }
    }
    Ok(())
}

async fn scan_lifecycle(
    repositories: &WriterRepositoryBundle,
    sender: &dyn PatchRelaySender,
    lease_owner: &str,
    lease_duration: chrono::Duration,
    min_age: chrono::Duration,
    batch_size: NonZeroUsize,
    claim_admission: &dyn LifecycleClaimAdmission,
) -> Result<()> {
    let now = Utc::now();
    if !repositories
        .has_reclaimable_patch_lifecycle(now, now - min_age)
        .await?
        || !claim_admission.claims_open(LifecyclePipeline::PatchBuild)
    {
        return Ok(());
    }
    let leases = repositories
        .lease_patch_lifecycle(PatchLifecycleLeaseRequest {
            owner: lease_owner,
            now,
            expires_at: now + lease_duration,
            eligible_before: now - min_age,
            limit: batch_size.get(),
        })
        .await?;
    for lease in leases {
        process_leased_lifecycle(repositories, sender, lease).await?;
    }
    Ok(())
}

async fn process_leased_lifecycle(
    repositories: &WriterRepositoryBundle,
    sender: &dyn PatchRelaySender,
    lease: LeasedPatchLifecycle,
) -> Result<()> {
    let envelope = envelope_from_repository_row(&lease.row);
    match sender.send(&envelope).await {
        Ok(()) => {
            match repositories
                .settle_leased_patch_submission(&lease, Utc::now())
                .await?
            {
                LeasedPatchSubmissionOutcome::Settled(_)
                | LeasedPatchSubmissionOutcome::LeaseLost => {}
            }
        }
        Err(error) => {
            eprintln!(
                "Patch reconciler: lifecycle relay failed for {} (will re-drive): {error}",
                lease.row.patch_key
            );
            repositories.release_patch_lifecycle_lease(&lease).await?;
        }
    }
    Ok(())
}
