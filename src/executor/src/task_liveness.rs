//! The executor's **liveness heartbeat** — the producer half of the task-
//! instance liveness watchdog (sibling of the Task log shipper).
//!
//! While a task runs, the executor holds a *liveness key* alive in a dedicated
//! NATS KV bucket: written the instant the task is picked up, re-armed every
//! `TTL/4`, deleted when the task reaches a terminal state. If the executor
//! goes dark, the re-arms stop, the key's per-key TTL elapses, and NATS appends
//! a delete marker the conductor's marker-consumer drains into an `Unhealthy`
//! verdict. The whole key lifecycle sits behind [`LivenessHeartbeat::start`] /
//! [`LivenessHeartbeat::stop`] — callers never touch the bucket.

use anyhow::Result;
use async_nats::jetstream::{self, kv};
use async_nats::Client as NatsClient;
use std::time::Duration;
use tickr_proto::coord::{liveness_key, LIVENESS_BUCKET, LIVENESS_MARKER_TTL};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::self_reaping_key;
use crate::task_log_shipper::LogIdentity;

/// Env var name and default for the liveness timeout, read off the published
/// contract so the producer and the constants stay in one place.
use tickr_proto::coord::{DEFAULT_LIVENESS_TIMEOUT_SECS, LIVENESS_TIMEOUT_ENV};

/// The opaque placeholder value of a liveness key. All signal is in key-
/// presence + TTL; the value is never read.
const LIVENESS_VALUE: &[u8] = b"alive";

/// System-internal liveness configuration: one knob, the **liveness timeout =
/// the per-key TTL**. The refresh cadence is *derived* as `TTL/4` (three missed
/// beats of slack before expiry) — not independently configurable, so cadence
/// can never be misconfigured larger than the TTL.
#[derive(Clone, Debug)]
pub struct LivenessConfig {
    /// The liveness timeout = the per-key TTL.
    pub timeout: Duration,
}

impl LivenessConfig {
    /// Read the liveness timeout from the environment (`TICKR_LIVENESS_TIMEOUT_SECS`),
    /// defaulting to 2 minutes. Whole seconds (NATS per-key TTL granularity); a
    /// zero or unparseable value falls back to the default.
    pub fn from_env() -> Self {
        let secs = std::env::var(LIVENESS_TIMEOUT_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_LIVENESS_TIMEOUT_SECS);
        Self {
            timeout: Duration::from_secs(secs),
        }
    }

    /// The derived refresh cadence: one quarter of the liveness timeout.
    pub fn cadence(&self) -> Duration {
        self.timeout / 4
    }
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(DEFAULT_LIVENESS_TIMEOUT_SECS),
        }
    }
}

/// Get-or-create the dedicated liveness KV bucket with the spike-pinned config:
/// `history: 1` (`MaxMsgsPerSubject = 1`, so a re-arm supersede never leaves
/// two live revisions and expiry yields exactly one marker per subject), File
/// storage, and `limit_markers = Some(TTL)` which flips on BOTH per-key TTL
/// (via the per-message `Nats-TTL` header each arm carries) and the subject-
/// delete-marker emission. The marker TTL is the generous verdict-durability
/// window. Idempotent — an existing bucket is reused. Mirrors the conductor's
/// own `ensure_liveness_bucket` exactly (the `ensure_task_event_stream` /
/// `task_event_consumer` precedent), since neither data-plane side can depend
/// on the other.
pub async fn ensure_liveness_bucket(nats: &NatsClient) -> Result<kv::Store> {
    let js = jetstream::new(nats.clone());
    if let Ok(store) = js.get_key_value(LIVENESS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: LIVENESS_BUCKET.to_string(),
        history: 1,
        storage: jetstream::stream::StorageType::File,
        limit_markers: Some(LIVENESS_MARKER_TTL),
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("create liveness KV bucket: {}", e))
}

/// The deep liveness module. Its entire surface is `start` / `stop`; behind it
/// sit the first PUT at pickup, the `TTL/4` re-arm loop, and the delete-on-stop.
pub struct LivenessHeartbeat {
    kv: kv::Store,
    key: String,
    cancel: CancellationToken,
    refresh: Option<JoinHandle<()>>,
}

impl LivenessHeartbeat {
    /// Arm the liveness key for a task instance and spawn the re-arm loop.
    ///
    /// The first PUT lands **here, at pickup, before the task process is
    /// spawned** — a lazy first write (on the first refresh tick) would leave a
    /// sub-cadence crash window invisible: an executor that died between pickup
    /// and the first beat would never have armed the switch. The re-arm re-PUTs
    /// the same key every `TTL/4`; a re-PUT is **marker-silent** because the
    /// key's subject already holds a live message, so a supersede never empties
    /// the subject (markers fire only on true expiry or explicit delete).
    pub async fn start(
        identity: LogIdentity,
        kv: kv::Store,
        nats: &NatsClient,
        config: &LivenessConfig,
    ) -> Self {
        let js = jetstream::new(nats.clone());
        let key = liveness_key(
            identity.workflow_id,
            identity.workflow_instance_id,
            identity.task_instance_id,
        );
        let timeout = config.timeout;

        // First arm at pickup — before the process spawns.
        self_reaping_key::arm(&js, LIVENESS_BUCKET, &key, LIVENESS_VALUE, timeout).await;

        let cancel = CancellationToken::new();
        let refresh = {
            let js = js.clone();
            let key = key.clone();
            let cadence = config.cadence();
            let token = cancel.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = sleep(cadence) => {
                            self_reaping_key::arm(&js, LIVENESS_BUCKET, &key, LIVENESS_VALUE, timeout).await
                        }
                    }
                }
            })
        };

        Self {
            kv,
            key,
            cancel,
            refresh: Some(refresh),
        }
    }

    /// Stop the heartbeat: cancel the re-arm loop and delete the key. Called in
    /// the executor's terminal sequence after the terminal `TaskEvent` is
    /// durably sent (terminal-update → **delete-key** → finish-logs). The one
    /// hard invariant of the whole feature: **the refresh stops on terminal** —
    /// everything else downstream (a stray marker, a duplicate verdict) is
    /// absorbed by the server's idempotency guard; only a refresh that *doesn't*
    /// stop can manufacture a false "alive". The explicit delete emits a
    /// `KV-Operation: DEL` tombstone the conductor filters out (noise); if the
    /// delete fails, the key's TTL reaps it within one timeout anyway.
    pub async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.refresh.take() {
            let _ = handle.await;
        }
        if let Err(e) = self.kv.delete(&self.key).await {
            eprintln!(
                "liveness key delete failed for {}: {} (TTL reaps within one timeout)",
                self.key, e
            );
        }
    }
}

impl Drop for LivenessHeartbeat {
    fn drop(&mut self) {
        // Belt-and-braces: a heartbeat dropped without `stop()` (an error path
        // that skips the terminal sequence) must not leak its detached re-arm
        // loop. Cancel and abort it; the key then expires by TTL. A tokio
        // JoinHandle does NOT cancel its task on drop, so this is load-bearing.
        self.cancel.cancel();
        if let Some(handle) = self.refresh.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cadence_is_a_quarter_of_the_timeout() {
        let cfg = LivenessConfig {
            timeout: Duration::from_secs(120),
        };
        assert_eq!(cfg.cadence(), Duration::from_secs(30));
    }

    #[test]
    fn from_env_defaults_to_two_minutes() {
        // No env override in the default test environment.
        std::env::remove_var(LIVENESS_TIMEOUT_ENV);
        let cfg = LivenessConfig::from_env();
        assert_eq!(cfg.timeout, Duration::from_secs(120));
    }
}
