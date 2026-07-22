//! Bounded Signal-applied notification for Tickr Lite ByTag cancellation.
//!
//! The channel carries only a reconciliation hint keyed by `signal_id`. Durable
//! Signal state and the existing relay response own materialization results;
//! notification loss, duplication, delay, or closure cannot mutate either.

use std::num::NonZeroUsize;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

/// Interface for the sole transient Signal-applied role.
///
/// Returning `()` is deliberate: notification delivery cannot participate in
/// relay acknowledgement, audit mutation, or materialization settlement.
pub trait SignalAppliedNotifier: Clone + Send + Sync + 'static {
    fn notify_bytag_cancel_materialized(&self, signal_id: Uuid);
}

/// Best-effort local implementation backed by one bounded channel.
#[derive(Clone)]
pub struct LocalSignalAppliedNotifier {
    sender: mpsc::Sender<ByTagCancelMaterialization>,
}

/// Opaque consumer half. Callers receive a hint or a bounded reconciliation
/// deadline, then consult the durable Signal state rather than this channel.
pub struct SignalAppliedNotificationStream {
    receiver: mpsc::Receiver<ByTagCancelMaterialization>,
    closed: bool,
}

/// The only notification shape admitted by this role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByTagCancelMaterialization {
    pub signal_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalAppliedReconciliationWake {
    Notification(ByTagCancelMaterialization),
    Deadline,
}

pub fn signal_applied_notifications(
    capacity: NonZeroUsize,
) -> (LocalSignalAppliedNotifier, SignalAppliedNotificationStream) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (
        LocalSignalAppliedNotifier { sender },
        SignalAppliedNotificationStream {
            receiver,
            closed: false,
        },
    )
}

impl SignalAppliedNotifier for LocalSignalAppliedNotifier {
    fn notify_bytag_cancel_materialized(&self, signal_id: Uuid) {
        let _ = self
            .sender
            .try_send(ByTagCancelMaterialization { signal_id });
    }
}

impl SignalAppliedNotificationStream {
    /// Wait for a latency hint or for the next mandatory durable-state scan.
    ///
    /// Once the producer closes, subsequent waits retain the scan cadence
    /// instead of spinning. Full and dropped channels reach the same deadline.
    pub async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake {
        if self.closed {
            sleep(maximum_delay).await;
            return SignalAppliedReconciliationWake::Deadline;
        }

        match timeout(maximum_delay, self.receiver.recv()).await {
            Ok(Some(notification)) => SignalAppliedReconciliationWake::Notification(notification),
            Ok(None) => {
                self.closed = true;
                SignalAppliedReconciliationWake::Deadline
            }
            Err(_) => SignalAppliedReconciliationWake::Deadline,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use tokio::time::{sleep, Instant};
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn full_dropped_and_closed_notifications_still_reach_reconciliation() {
        let (notifier, mut notifications) =
            signal_applied_notifications(NonZeroUsize::new(1).unwrap());
        let retained = Uuid::new_v4();
        let dropped = Uuid::new_v4();

        notifier.notify_bytag_cancel_materialized(retained);
        notifier.notify_bytag_cancel_materialized(dropped);

        assert_eq!(
            notifications
                .next_reconciliation(Duration::from_millis(20))
                .await,
            SignalAppliedReconciliationWake::Notification(ByTagCancelMaterialization {
                signal_id: retained,
            })
        );
        assert_eq!(
            notifications
                .next_reconciliation(Duration::from_millis(10))
                .await,
            SignalAppliedReconciliationWake::Deadline
        );

        drop(notifier);
        assert_eq!(
            notifications
                .next_reconciliation(Duration::from_millis(10))
                .await,
            SignalAppliedReconciliationWake::Deadline
        );
        let started = Instant::now();
        assert_eq!(
            notifications
                .next_reconciliation(Duration::from_millis(10))
                .await,
            SignalAppliedReconciliationWake::Deadline
        );
        assert!(started.elapsed() >= Duration::from_millis(10));
    }

    #[tokio::test]
    async fn duplicated_and_delayed_notifications_are_only_reconciliation_hints() {
        let (notifier, mut notifications) =
            signal_applied_notifications(NonZeroUsize::new(3).unwrap());
        let signal_id = Uuid::new_v4();

        notifier.notify_bytag_cancel_materialized(signal_id);
        notifier.notify_bytag_cancel_materialized(signal_id);
        for _ in 0..2 {
            assert_eq!(
                notifications
                    .next_reconciliation(Duration::from_millis(20))
                    .await,
                SignalAppliedReconciliationWake::Notification(ByTagCancelMaterialization {
                    signal_id,
                })
            );
        }

        let delayed_notifier = notifier.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(5)).await;
            delayed_notifier.notify_bytag_cancel_materialized(signal_id);
        });
        assert_eq!(
            notifications
                .next_reconciliation(Duration::from_millis(50))
                .await,
            SignalAppliedReconciliationWake::Notification(ByTagCancelMaterialization { signal_id })
        );
    }
}
