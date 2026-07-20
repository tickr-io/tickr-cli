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
use async_nats::jetstream::{self, kv};
use async_nats::Client as NatsClient;
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

/// Spawn the process-lifetime component-liveness re-arm loop. Arms the key once
/// immediately (so the executor is counted without waiting a full cadence), then
/// re-arms every `TTL/4`, reading `in_flight` off the shared dispatch `semaphore`
/// at each beat. Cancelled by `shutdown`, at which point re-arming stops and the
/// key self-reaps by TTL. The caller must ensure the bucket exists first — a
/// missing bucket makes every arm's publish fail (logged, non-fatal).
pub fn spawn_component_liveness(
    nats: Arc<NatsClient>,
    config: LivenessConfig,
    semaphore: Arc<Semaphore>,
    cap: usize,
    executor_id: Uuid,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let js = jetstream::new((*nats).clone());
        let key = component_liveness_key(executor_id);
        let timeout = config.timeout;
        let cadence = config.cadence();

        // First arm at boot — before the first cadence tick — so a just-booted
        // executor is counted immediately.
        arm_once(&js, &key, &semaphore, cap, timeout).await;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = sleep(cadence) => arm_once(&js, &key, &semaphore, cap, timeout).await,
            }
        }
    })
}

/// Compute the current `{cap, in_flight}` and arm the key once.
async fn arm_once(
    js: &jetstream::Context,
    key: &str,
    semaphore: &Semaphore,
    cap: usize,
    timeout: Duration,
) {
    // in_flight = cap − available_permits, read straight off the shared dispatch
    // semaphore — no separate counter to drift. Known coarse-gauge bias (document,
    // don't fight): the drain acquires a permit *before* each speculative pull and
    // holds it through the batch wait, so a fully idle executor reads
    // in_flight = 1, not 0 — acceptable for a ~30s saturation gauge.
    let in_flight = cap.saturating_sub(semaphore.available_permits());
    let value = ComponentLivenessValue { cap, in_flight };
    match serde_json::to_vec(&value) {
        Ok(bytes) => {
            self_reaping_key::arm(js, COMPONENT_LIVENESS_BUCKET, key, &bytes, timeout).await
        }
        Err(e) => eprintln!("component-liveness value serialize failed: {e}"),
    }
}
