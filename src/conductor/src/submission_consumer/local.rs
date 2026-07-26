//! Durable definition-submission reconciliation.
//!
//! Committed `Ready` SQL rows are authoritative. Notifications only shorten
//! scan latency; startup and periodic scans recover work after notification
//! loss, process restart, or relay disconnection.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use prost::Message;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::definition_repository::{
    DefinitionSubmissionLeaseRequest, DefinitionSubmissionSettlementOutcome,
    LeasedDefinitionSubmission, LeasedDefinitionSubmissionSettlementOutcome,
};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::lifecycle_work::{LifecycleClaimAdmission, LifecyclePipeline, OpenLifecycleClaims};
use crate::relay::forward_workflow_registration_bytes;

/// Bounded best-effort wakeup for newly committed definition-submission work.
#[derive(Clone)]
pub struct DefinitionSubmissionNotifier {
    sender: mpsc::Sender<()>,
}

/// Consumer half of the notification hint, kept opaque so channel state cannot
/// be mistaken for submission lifecycle state.
pub struct DefinitionSubmissionNotificationStream {
    receiver: mpsc::Receiver<()>,
}

/// Construct one bounded submission notification hint.
pub fn definition_submission_notifications(
    capacity: NonZeroUsize,
) -> (
    DefinitionSubmissionNotifier,
    DefinitionSubmissionNotificationStream,
) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        DefinitionSubmissionNotifier { sender },
        DefinitionSubmissionNotificationStream { receiver },
    )
}

impl DefinitionSubmissionNotifier {
    /// Request an immediate scan. A full or closed channel deliberately loses
    /// only this hint; the committed `Ready` row remains authoritative.
    pub fn notify(&self) -> bool {
        self.sender.try_send(()).is_ok()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LocalDefinitionSubmissionWorkerConfig {
    pub scan_interval: Duration,
    pub lease_duration: Duration,
    pub batch_size: NonZeroUsize,
}

impl Default for LocalDefinitionSubmissionWorkerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(5),
            lease_duration: Duration::from_secs(30),
            batch_size: NonZeroUsize::new(16).expect("definition submission batch is non-zero"),
        }
    }
}

pub async fn start_local_definition_submission_worker(
    repositories: Arc<WriterRepositoryBundle>,
    lease_owner: String,
    notifications: DefinitionSubmissionNotificationStream,
    config: LocalDefinitionSubmissionWorkerConfig,
    cancel: CancellationToken,
) -> Result<()> {
    start_local_definition_submission_worker_with_claim_admission(
        repositories,
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
pub async fn start_local_definition_submission_worker_with_claim_admission(
    repositories: Arc<WriterRepositoryBundle>,
    lease_owner: String,
    mut notifications: DefinitionSubmissionNotificationStream,
    claim_admission: Arc<dyn LifecycleClaimAdmission>,
    config: LocalDefinitionSubmissionWorkerConfig,
    cancel: CancellationToken,
) -> Result<()> {
    if config.scan_interval.is_zero() {
        anyhow::bail!("definition submission scan interval must be non-zero");
    }
    let lease_duration = chrono::Duration::from_std(config.lease_duration)
        .context("definition submission lease duration is out of range")?;
    if lease_duration <= chrono::Duration::zero() {
        anyhow::bail!("definition submission lease duration must be positive");
    }

    let mut ticker = tokio::time::interval(config.scan_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    scan_once_with_claim_admission(
        repositories.as_ref(),
        &lease_owner,
        lease_duration,
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
                    &lease_owner,
                    lease_duration,
                    config.batch_size,
                    claim_admission.as_ref(),
                ).await?;
            }
            notification = notifications.receiver.recv(), if notifications_open => {
                notifications_open = notification.is_some();
                scan_once_with_claim_admission(
                    repositories.as_ref(),
                    &lease_owner,
                    lease_duration,
                    config.batch_size,
                    claim_admission.as_ref(),
                ).await?;
            }
        }
    }
    Ok(())
}

async fn scan_once_with_claim_admission(
    repositories: &WriterRepositoryBundle,
    lease_owner: &str,
    lease_duration: chrono::Duration,
    batch_size: NonZeroUsize,
    claim_admission: &dyn LifecycleClaimAdmission,
) -> Result<()> {
    let now = Utc::now();
    if !repositories
        .has_reclaimable_definition_submission(now)
        .await?
        || !claim_admission.claims_open(LifecyclePipeline::Submission)
    {
        return Ok(());
    }
    let leases = repositories
        .lease_definition_submissions(DefinitionSubmissionLeaseRequest {
            owner: lease_owner,
            now,
            expires_at: now + lease_duration,
            limit: batch_size.get(),
        })
        .await?;

    // Relay sequentially so the stable repository selection order remains the
    // observable forward order for one reconciler.
    for lease in leases {
        process_leased_submission(repositories, lease).await?;
    }
    Ok(())
}

async fn process_leased_submission(
    repositories: &WriterRepositoryBundle,
    lease: LeasedDefinitionSubmission,
) -> Result<()> {
    let payload = lease.intent.definition.encode_to_vec();
    if let Err(error) = forward_workflow_registration_bytes(payload).await {
        eprintln!(
            "definition submission worker: relay unavailable for ({}, {}): {error}",
            lease.intent.workflow_id, lease.intent.workflow_version
        );
        return Ok(());
    }

    match repositories
        .settle_leased_definition_submission(&lease, Utc::now())
        .await
        .context("settle leased definition submission")?
    {
        LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::Submitted,
        ) => {}
        LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::AlreadySettled(status),
        ) => {
            eprintln!(
                "definition submission worker: ({}, {}) already settled as {status:?}",
                lease.intent.workflow_id, lease.intent.workflow_version
            );
        }
        LeasedDefinitionSubmissionSettlementOutcome::Settled(
            DefinitionSubmissionSettlementOutcome::Absent,
        ) => {
            eprintln!(
                "definition submission worker: ({}, {}) disappeared after relay forward",
                lease.intent.workflow_id, lease.intent.workflow_version
            );
        }
        LeasedDefinitionSubmissionSettlementOutcome::LeaseLost => {
            eprintln!(
                "definition submission worker: lease expired for ({}, {}) after relay forward; leaving Ready for re-drive",
                lease.intent.workflow_id, lease.intent.workflow_version
            );
        }
    }
    Ok(())
}
