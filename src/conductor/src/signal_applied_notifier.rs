//! Bounded Signal-applied notification for Tickr Lite ByTag cancellation.
//!
//! The channel carries only a reconciliation hint keyed by `signal_id`. Durable
//! Signal state and the existing relay response own materialization results;
//! notification loss, duplication, delay, or closure cannot mutate either.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

/// Interface for the sole transient Signal-applied role.
///
/// Returning `()` is deliberate: notification delivery cannot participate in
/// relay acknowledgement, audit mutation, or materialization settlement.
pub trait SignalAppliedNotifier: Send + Sync + 'static {
    fn notify_bytag_cancel_materialized(&self, signal_id: Uuid);
}

/// Backend-neutral wake source for bounded durable Signal-state reconciliation.
#[async_trait]
pub trait SignalAppliedReconciliationStream: Send {
    async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake;
}

#[async_trait]
impl<T> SignalAppliedReconciliationStream for Box<T>
where
    T: SignalAppliedReconciliationStream + ?Sized,
{
    async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake {
        (**self).next_reconciliation(maximum_delay).await
    }
}

pub type SharedSignalAppliedReconciliationStream =
    Arc<Mutex<Box<dyn SignalAppliedReconciliationStream>>>;

/// Formation-selected publication and observation surfaces with substrate
/// details erased before either production caller receives them.
#[derive(Clone)]
pub struct SignalAppliedNotificationRoles {
    notifier: Arc<dyn SignalAppliedNotifier>,
    reconciliation: SharedSignalAppliedReconciliationStream,
}

impl SignalAppliedNotificationRoles {
    pub fn new<N, S>(notifier: N, reconciliation: S) -> Self
    where
        N: SignalAppliedNotifier,
        S: SignalAppliedReconciliationStream + 'static,
    {
        Self {
            notifier: Arc::new(notifier),
            reconciliation: Arc::new(Mutex::new(Box::new(reconciliation))),
        }
    }

    pub fn notifier(&self) -> Arc<dyn SignalAppliedNotifier> {
        Arc::clone(&self.notifier)
    }

    pub fn reconciliation(&self) -> SharedSignalAppliedReconciliationStream {
        Arc::clone(&self.reconciliation)
    }
}

const ALL_NATS_SIGNAL_APPLIED_SUBJECT_PREFIX: &str =
    tickr_proto::coord::all_nats::SIGNAL_APPLIED_SUBJECT_PREFIX;

fn all_nats_signal_applied_subject(signal_id: Uuid) -> String {
    format!("{ALL_NATS_SIGNAL_APPLIED_SUBJECT_PREFIX}.{signal_id}")
}

/// Fresh all-NATS transient notifier. The bounded local queue deliberately
/// drops pressure rather than turning an advisory wake into acknowledgement.
#[derive(Clone)]
pub struct NatsSignalAppliedNotifier {
    sender: mpsc::Sender<Uuid>,
}

impl SignalAppliedNotifier for NatsSignalAppliedNotifier {
    fn notify_bytag_cancel_materialized(&self, signal_id: Uuid) {
        let _ = self.sender.try_send(signal_id);
    }
}

pub struct NatsSignalAppliedNotificationStream {
    subscriber: async_nats::Subscriber,
    closed: bool,
}

/// Open the fresh all-NATS advisory resource behind the same role interfaces
/// used by Redis and Tickr Lite.
pub async fn all_nats_signal_applied_notifications(
    nats: async_nats::Client,
) -> anyhow::Result<SignalAppliedNotificationRoles> {
    let subscriber = nats
        .subscribe(format!("{ALL_NATS_SIGNAL_APPLIED_SUBJECT_PREFIX}.*"))
        .await?;
    nats.flush().await?;
    let (sender, mut receiver) = mpsc::channel::<Uuid>(64);
    tokio::spawn(async move {
        while let Some(signal_id) = receiver.recv().await {
            if nats
                .publish(
                    all_nats_signal_applied_subject(signal_id),
                    Vec::<u8>::new().into(),
                )
                .await
                .is_ok()
            {
                let _ = nats.flush().await;
            }
        }
    });
    Ok(SignalAppliedNotificationRoles::new(
        NatsSignalAppliedNotifier { sender },
        NatsSignalAppliedNotificationStream {
            subscriber,
            closed: false,
        },
    ))
}

#[async_trait]
impl SignalAppliedReconciliationStream for NatsSignalAppliedNotificationStream {
    async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake {
        let deadline = tokio::time::Instant::now() + maximum_delay;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return SignalAppliedReconciliationWake::Deadline;
            }
            if self.closed {
                sleep(remaining).await;
                return SignalAppliedReconciliationWake::Deadline;
            }
            match timeout(remaining, self.subscriber.next()).await {
                Err(_) => return SignalAppliedReconciliationWake::Deadline,
                Ok(None) => self.closed = true,
                Ok(Some(message)) => {
                    let Some(signal_id) = message
                        .subject
                        .as_str()
                        .strip_prefix(ALL_NATS_SIGNAL_APPLIED_SUBJECT_PREFIX)
                        .and_then(|suffix| suffix.strip_prefix('.'))
                        .and_then(|value| Uuid::parse_str(value).ok())
                    else {
                        continue;
                    };
                    return SignalAppliedReconciliationWake::Notification(
                        ByTagCancelMaterialization { signal_id },
                    );
                }
            }
        }
    }
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

#[async_trait]
impl SignalAppliedReconciliationStream for SignalAppliedNotificationStream {
    async fn next_reconciliation(
        &mut self,
        maximum_delay: Duration,
    ) -> SignalAppliedReconciliationWake {
        SignalAppliedNotificationStream::next_reconciliation(self, maximum_delay).await
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
