//! The executor's **component-liveness key** — a process-lifetime self-reaping
//! KV key that lets the fleet be counted and its saturation read without a
//! durable registry and without scanning per-task liveness keys.
//!
//! Distinct from the per-*task* liveness heartbeat ([`crate::task_liveness`]):
//! that key is task-scoped (armed at pickup, deleted on terminal), this one is
//! scoped to the executor *process* and lives from boot to shutdown. At boot the
//! executor PUTs `executor.<uuid>` into the dedicated `tickr_component_liveness`
//! bucket with a per-message TTL, re-arms it on the `TTL/4` cadence via the
//! shared [`crate::self_reaping_key::arm`] primitive, and cancels the loop on
//! shutdown so a clean stop stops re-arming and the key self-reaps by TTL.
//!
//! The bucket is separate from the task-liveness bucket precisely so a component
//! key's expiry never fires a marker into the conductor's task-death verdict path
//! (nothing binds this bucket's wildcard). Writes are **cadence-only** — the
//! `{cap, in_flight}` value is a coarse ~TTL-stale saturation gauge, deliberately
//! not coupled to dispatch/completion, so there is no real-time write churn.

use anyhow::Result;
use async_nats::jetstream::{
    self,
    kv::{self, Operation},
};
use async_nats::Client as NatsClient;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tickr_proto::coord::{
    component_liveness_key, ComponentLivenessValue, COMPONENT_LIVENESS_BUCKET,
};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::local_pickup::{
    ExecutorCapacityObservation, ExecutorFleetSnapshot, ExecutorFleetStatus, LocalExecutorCapacity,
};
use crate::self_reaping_key;
use crate::task_liveness::LivenessConfig;

/// Delete-marker TTL for the component bucket. `limit_markers` couples the
/// per-key TTL (which is what actually reaps the key) with subject-delete-marker
/// emission, so it must be set to make the key self-reap. Those markers are never
/// consumed — nothing binds this bucket's wildcard — so this value only bounds
/// how long a spent tombstone lingers in the backing stream; keep it short.
const COMPONENT_MARKER_TTL: Duration = Duration::from_secs(60);

/// Get-or-create the dedicated component-liveness KV bucket. `history: 1` so a
/// re-arm supersede never leaves two live revisions, File storage, and
/// `limit_markers` to turn on the per-key TTL each arm carries. Idempotent — an
/// existing bucket is reused. Mirrors `ensure_liveness_bucket` on the task side;
/// unlike that bucket, no consumer ever binds this one's wildcard.
pub async fn ensure_component_liveness_bucket(nats: &NatsClient) -> Result<kv::Store> {
    let js = jetstream::new(nats.clone());
    if let Ok(store) = js.get_key_value(COMPONENT_LIVENESS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: COMPONENT_LIVENESS_BUCKET.to_string(),
        history: 1,
        storage: jetstream::stream::StorageType::File,
        limit_markers: Some(COMPONENT_MARKER_TTL),
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("create component-liveness KV bucket: {}", e))
}

/// All-NATS implementation of the observational ExecutorFleetStatus role.
///
/// NATS keys remain private to this adapter. Callers receive only the common
/// report/snapshot interface.
#[derive(Clone)]
pub struct NatsExecutorFleetStatus {
    nats: NatsClient,
    observation_ttl: Duration,
}

impl NatsExecutorFleetStatus {
    pub fn new(nats: NatsClient, observation_ttl: Duration) -> Self {
        Self {
            nats,
            observation_ttl,
        }
    }

    pub async fn prepare(&self) -> Result<()> {
        ensure_component_liveness_bucket(&self.nats).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExecutorFleetStatus for NatsExecutorFleetStatus {
    fn observation_ttl(&self) -> Duration {
        self.observation_ttl
    }

    async fn report(&self, observation: ExecutorCapacityObservation) -> Result<(), String> {
        let value = ComponentLivenessValue {
            cap: observation.configured_process_slots,
            in_flight: observation.in_flight_count,
        };
        let bytes =
            serde_json::to_vec(&value).map_err(|_| "invalid fleet observation".to_string())?;
        let js = jetstream::new(self.nats.clone());
        self_reaping_key::arm(
            &js,
            COMPONENT_LIVENESS_BUCKET,
            &component_liveness_key(observation.executor_id),
            &bytes,
            self.observation_ttl,
        )
        .await;
        Ok(())
    }

    async fn fleet_snapshot(&self) -> Result<ExecutorFleetSnapshot, String> {
        let server_time_millis = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let observation_ttl_millis =
            u64::try_from(self.observation_ttl.as_millis()).unwrap_or(u64::MAX);
        let empty = || ExecutorFleetSnapshot {
            server_time_millis,
            observation_ttl_millis,
            observations: Vec::new(),
        };
        let js = jetstream::new(self.nats.clone());
        let store = match js.get_key_value(COMPONENT_LIVENESS_BUCKET).await {
            Ok(store) => store,
            Err(_) => return Ok(empty()),
        };
        let mut keys = match store.keys().await {
            Ok(keys) => keys,
            Err(_) => return Ok(empty()),
        };
        let mut observations = Vec::new();

        while let Some(item) = keys.next().await {
            let Ok(key) = item else { continue };
            let Some(executor_id) = key
                .strip_prefix("executor.")
                .and_then(|value| Uuid::parse_str(value).ok())
            else {
                continue;
            };
            let entry = match store.entry(&key).await {
                Ok(Some(entry)) if entry.operation == Operation::Put => entry,
                _ => continue,
            };
            let Ok(value) = serde_json::from_slice::<ComponentLivenessValue>(&entry.value) else {
                continue;
            };
            let observed_at_server_millis =
                u64::try_from(entry.created.unix_timestamp_nanos().max(0) / 1_000_000)
                    .unwrap_or(u64::MAX);
            let expires_at_server_millis =
                observed_at_server_millis.saturating_add(observation_ttl_millis);
            if expires_at_server_millis <= server_time_millis {
                continue;
            }
            observations.push(ExecutorCapacityObservation {
                executor_id,
                reporter_id: executor_id,
                sequence: entry.revision,
                configured_process_slots: value.cap,
                in_flight_count: value.in_flight,
                observed_at_server_millis,
                expires_at_server_millis,
            });
        }
        observations.sort_unstable_by_key(|observation| observation.executor_id);
        Ok(ExecutorFleetSnapshot {
            server_time_millis,
            observation_ttl_millis,
            observations,
        })
    }
}

/// Spawn one process-incarnation reporter over an observational role interface.
///
/// The loop owns one reporter identity and a monotonic sequence. Report
/// outcomes never feed dispatch admission or local semaphore ownership.
pub fn spawn_executor_fleet_reporting(
    fleet_status: Arc<dyn ExecutorFleetStatus>,
    capacity: LocalExecutorCapacity,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let reporter_id = Uuid::new_v4();
        let cadence = fleet_status
            .observation_ttl()
            .checked_div(4)
            .unwrap_or(Duration::ZERO);
        let cadence = cadence.max(Duration::from_millis(1));
        let mut sequence = 0u64;

        loop {
            sequence = sequence.saturating_add(1);
            let snapshot = capacity.snapshot();
            let observation = ExecutorCapacityObservation {
                executor_id: snapshot.executor_id,
                reporter_id,
                sequence,
                configured_process_slots: snapshot.configured_process_slots,
                in_flight_count: snapshot.in_flight_count,
                observed_at_server_millis: 0,
                expires_at_server_millis: 0,
            };
            if let Err(error) = fleet_status.report(observation).await {
                eprintln!("executor fleet observation failed: {error}");
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(cadence) => {}
            }
        }
    })
}
/// Compatibility wrapper for the existing all-NATS fleet-observation suite.
pub fn spawn_component_liveness(
    nats: Arc<NatsClient>,
    config: LivenessConfig,
    semaphore: Arc<Semaphore>,
    cap: usize,
    executor_id: Uuid,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let cap = std::num::NonZeroUsize::new(cap).expect("component capacity is positive");
    let capacity = LocalExecutorCapacity::from_process_slots(executor_id, cap, semaphore);
    spawn_executor_fleet_reporting(
        Arc::new(NatsExecutorFleetStatus::new(
            (*nats).clone(),
            config.timeout,
        )),
        capacity,
        shutdown,
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::sync::mpsc;

    use super::*;

    struct CapturingFleetStatus {
        reports: mpsc::UnboundedSender<ExecutorCapacityObservation>,
    }

    #[async_trait::async_trait]
    impl ExecutorFleetStatus for CapturingFleetStatus {
        fn observation_ttl(&self) -> Duration {
            Duration::from_millis(4)
        }

        async fn report(&self, observation: ExecutorCapacityObservation) -> Result<(), String> {
            self.reports
                .send(observation)
                .map_err(|_| "capture closed".to_string())
        }

        async fn fleet_snapshot(&self) -> Result<ExecutorFleetSnapshot, String> {
            Err("report-only test role".to_string())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reporter_incarnation_is_monotonic_and_observes_local_capacity() {
        let executor_id = Uuid::new_v4();
        let capacity =
            LocalExecutorCapacity::new(executor_id, NonZeroUsize::new(2).expect("non-zero"));
        let (reports, mut received) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let handle = spawn_executor_fleet_reporting(
            Arc::new(CapturingFleetStatus { reports }),
            capacity.clone(),
            shutdown.clone(),
        );

        let first = received.recv().await.expect("initial observation");
        let permit = capacity
            .acquire_process_slot()
            .await
            .expect("local slot remains available");
        tokio::time::advance(Duration::from_millis(1)).await;
        let second = received.recv().await.expect("replacement observation");

        assert_eq!(first.executor_id, executor_id);
        assert_eq!(first.reporter_id, second.reporter_id);
        assert!(first.sequence < second.sequence);
        assert_eq!(first.in_flight_count, 0);
        assert_eq!(second.in_flight_count, 1);

        drop(permit);
        shutdown.cancel();
        handle.await.expect("reporter exits");
    }
}
