//! Durable replay lifecycle processing for Tickr Lite.
//!
//! Committed SQLite replay rows are authoritative. Bounded notifications only
//! shorten scan latency; startup and periodic scans recover rows after dropped
//! wakeups, lease expiry, or process restart.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::replay_repository::{
    LeasedReplay, LeasedReplaySettlementOutcome, ReplayDriveLoadOutcome, ReplayLeaseCandidate,
    ReplayLeaseRequest, ReplayLifecycleStatus, ReplaySettlementOutcome,
};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use super::ReplayRelaySender;

/// Bounded best-effort wakeup for newly committed replay work.
#[derive(Clone)]
pub struct ReplayWorkNotifier {
    sender: mpsc::Sender<()>,
}

/// Consumer half of the notification hint, opaque to lifecycle callers.
pub struct ReplayWorkNotificationStream {
    receiver: mpsc::Receiver<()>,
}

/// Construct one bounded replay notification hint.
pub fn replay_work_notifications(
    capacity: NonZeroUsize,
) -> (ReplayWorkNotifier, ReplayWorkNotificationStream) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        ReplayWorkNotifier { sender },
        ReplayWorkNotificationStream { receiver },
    )
}

impl ReplayWorkNotifier {
    /// Request an immediate durable scan. Full and closed channels lose only
    /// this hint; no committed replay row is acknowledged by the channel.
    pub fn notify(&self) {
        let _ = self.sender.try_send(());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LocalReplayWorkerConfig {
    pub scan_interval: Duration,
    pub lease_duration: Duration,
    pub min_age: Duration,
    pub batch_size: NonZeroUsize,
}

impl Default for LocalReplayWorkerConfig {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(5),
            lease_duration: Duration::from_secs(30),
            min_age: Duration::from_secs(10),
            batch_size: NonZeroUsize::new(8).expect("replay batch is non-zero"),
        }
    }
}

/// Run startup reconciliation followed by bounded notification- and timer-led
/// scans until formation cancellation.
pub async fn start_local_replay_worker(
    repositories: Arc<WriterRepositoryBundle>,
    sender: Arc<dyn ReplayRelaySender>,
    lease_owner: String,
    mut notifications: ReplayWorkNotificationStream,
    config: LocalReplayWorkerConfig,
    cancel: CancellationToken,
) -> Result<()> {
    if config.scan_interval.is_zero() {
        anyhow::bail!("replay scan interval must be non-zero");
    }
    let lease_duration = chrono::Duration::from_std(config.lease_duration)
        .context("replay lease duration is out of range")?;
    if lease_duration <= chrono::Duration::zero() {
        anyhow::bail!("replay lease duration must be positive");
    }
    let min_age =
        chrono::Duration::from_std(config.min_age).context("replay minimum age is out of range")?;

    let mut ticker = tokio::time::interval(config.scan_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    scan_once(
        repositories.as_ref(),
        sender.as_ref(),
        &lease_owner,
        lease_duration,
        chrono::Duration::zero(),
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
                    sender.as_ref(),
                    &lease_owner,
                    lease_duration,
                    min_age,
                    config.batch_size,
                ).await?;
            }
            notification = notifications.receiver.recv(), if notifications_open => {
                if notification.is_some() {
                    scan_once(
                        repositories.as_ref(),
                        sender.as_ref(),
                        &lease_owner,
                        lease_duration,
                        chrono::Duration::zero(),
                        config.batch_size,
                    ).await?;
                } else {
                    notifications_open = false;
                }
            }
        }
    }
    Ok(())
}

async fn scan_once(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
    lease_owner: &str,
    lease_duration: chrono::Duration,
    min_age: chrono::Duration,
    batch_size: NonZeroUsize,
) -> Result<usize> {
    let now = Utc::now();
    let candidates = repositories
        .lease_replays(ReplayLeaseRequest {
            owner: lease_owner,
            now,
            expires_at: now + lease_duration,
            eligible_before: now - min_age,
            limit: batch_size.get(),
        })
        .await?;

    let mut settled = 0;
    for candidate in candidates {
        let lease = match candidate {
            ReplayLeaseCandidate::Ready(lease) => lease,
            ReplayLeaseCandidate::Corrupt { identity, error } => {
                eprintln!(
                    "local replay worker: stored lifecycle for {identity} is corrupt and was skipped: {error}"
                );
                continue;
            }
        };
        match drive_leased_replay(repositories, sender, &lease).await {
            Ok(DriveLeaseOutcome::Settled) => settled += 1,
            Ok(DriveLeaseOutcome::LeaseLost | DriveLeaseOutcome::AlreadySettled) => {}
            Err(error) => {
                eprintln!(
                    "local replay worker: drive failed for {} (will retry): {error}",
                    lease.row.replay_instance_id
                );
                repositories.release_replay_lease(&lease).await?;
            }
        }
    }
    Ok(settled)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriveLeaseOutcome {
    Settled,
    LeaseLost,
    AlreadySettled,
}

async fn drive_leased_replay(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
    lease: &LeasedReplay,
) -> Result<DriveLeaseOutcome> {
    let replay = match repositories
        .load_replay_drive(lease.row.replay_instance_id)
        .await?
    {
        ReplayDriveLoadOutcome::Ready(replay) => replay,
        ReplayDriveLoadOutcome::AlreadySettled(ReplayLifecycleStatus::Released) => {
            return Ok(DriveLeaseOutcome::AlreadySettled)
        }
        ReplayDriveLoadOutcome::AlreadySettled(status) => {
            anyhow::bail!(
                "replay {} settled as {} during leased drive",
                lease.row.replay_instance_id,
                status.as_str()
            )
        }
        ReplayDriveLoadOutcome::SourceUnavailable(row) => {
            anyhow::bail!(
                "archive blob for source {} is unavailable while replay {} remains Materializing",
                row.source_instance_id,
                row.replay_instance_id
            )
        }
        ReplayDriveLoadOutcome::Absent => {
            anyhow::bail!(
                "replay {} disappeared before leased drive",
                lease.row.replay_instance_id
            )
        }
    };
    let inputs =
        super::build_drive_inputs(repositories, &replay.lifecycle, &replay.source, Vec::new())
            .await?;
    super::perform_drive_effects(sender, &inputs).await?;

    Ok(
        match repositories
            .settle_leased_replay_released(lease, Utc::now())
            .await?
        {
            LeasedReplaySettlementOutcome::Settled(ReplaySettlementOutcome::Released)
            | LeasedReplaySettlementOutcome::Settled(ReplaySettlementOutcome::AlreadySettled(
                ReplayLifecycleStatus::Released,
            )) => DriveLeaseOutcome::Settled,
            LeasedReplaySettlementOutcome::LeaseLost => DriveLeaseOutcome::LeaseLost,
            LeasedReplaySettlementOutcome::Settled(ReplaySettlementOutcome::AlreadySettled(
                status,
            )) => {
                anyhow::bail!(
                    "replay {} settled as {} during leased drive",
                    lease.row.replay_instance_id,
                    status.as_str()
                )
            }
            LeasedReplaySettlementOutcome::Settled(ReplaySettlementOutcome::Absent) => {
                anyhow::bail!(
                    "replay {} disappeared before leased settlement",
                    lease.row.replay_instance_id
                )
            }
        },
    )
}
