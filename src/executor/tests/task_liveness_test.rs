//! Real-NATS integration test for the executor's liveness heartbeat
//! (`task_liveness::LivenessHeartbeat` / `ensure_liveness_bucket`).
//!
//! Asserts the externally-observable key lifecycle the watchdog rides:
//!   1. **PUT-at-start** — `start` writes the liveness key immediately, at
//!      pickup, before any refresh tick;
//!   2. **marker-silent re-PUT** — a refresh re-PUT supersedes the live key
//!      without emitting a delete marker (the subject never empties);
//!   3. **delete + stop on `stop`** — `stop` deletes the key and cancels the
//!      refresh, so the key stays gone (no resurrection).
//!
//! Requires Docker (testcontainers). Skipped automatically when the NATS
//! container is unavailable — the startup failure is the skip marker, matching
//! the other executor/conductor integration tests.

use async_nats::jetstream;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_executor::task_liveness::{ensure_liveness_bucket, LivenessConfig, LivenessHeartbeat};
use tickr_executor::task_log_shipper::LogIdentity;
use tickr_proto::coord::{liveness_key, LIVENESS_BUCKET};
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    // Pin 2.14.2 — per-key KV TTL + delete markers (`limit_markers`) need the
    // same NATS the dev infra runs; the testcontainers default tag is older.
    let container = match Nats::default()
        .with_cmd(&cmd)
        .with_tag("2.14.2")
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{}", port);
    let mut client = None;
    for _ in 0..20 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((container, client.expect("nats connect")))
}

/// Message count of the KV bucket's backing stream. While a single key is alive
/// and `history: 1` keeps one revision per subject, this is exactly 1 — an
/// expiry/delete marker would be an *additional* message, so `== 1` after a run
/// of re-arms is the marker-silence assertion.
async fn backing_stream_messages(nats: &async_nats::Client) -> u64 {
    let js = jetstream::new(nats.clone());
    let mut stream = js
        .get_stream(format!("KV_{}", LIVENESS_BUCKET))
        .await
        .expect("get liveness backing stream");
    stream.info().await.expect("stream info").state.messages
}

#[tokio::test]
async fn heartbeat_arms_at_start_is_marker_silent_and_deletes_on_stop() {
    let Some((_container, nats)) = start_nats().await else {
        return; // Docker/NATS unavailable — skip.
    };

    let kv = ensure_liveness_bucket(&nats)
        .await
        .expect("ensure liveness bucket");

    let workflow_id = Uuid::new_v4();
    let workflow_instance_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    let identity = LogIdentity {
        workflow_id,
        workflow_instance_id,
        task_instance_id,
    };
    let key = liveness_key(workflow_id, workflow_instance_id, task_instance_id);

    // Timeout 8s → derived cadence 2s. Long enough that the key never expires
    // during the ~5s alive window, short enough to drive a couple of re-arms.
    let config = LivenessConfig {
        timeout: Duration::from_secs(8),
    };

    let heartbeat = LivenessHeartbeat::start(identity, kv.clone(), &nats, &config).await;

    // (1) PUT-at-start: the key exists the moment start returns, before any tick.
    assert!(
        kv.get(&key).await.expect("kv get").is_some(),
        "liveness key must be PUT at start (pickup), before any refresh tick"
    );

    // (2) marker-silence: let the re-arm loop fire ~2 times (at 2s and 4s).
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        kv.get(&key).await.expect("kv get").is_some(),
        "live key must still be present after re-arms (TTL not elapsed)"
    );
    assert_eq!(
        backing_stream_messages(&nats).await,
        1,
        "a re-PUT must be marker-silent: exactly one live revision, no marker"
    );

    // (3) delete + stop: stop deletes the key and cancels the refresh.
    heartbeat.stop().await;
    assert!(
        kv.get(&key).await.expect("kv get").is_none(),
        "stop must delete the liveness key"
    );

    // The refresh stopped: past one full cadence the key is not resurrected.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        kv.get(&key).await.expect("kv get").is_none(),
        "stop must cancel the refresh — no re-arm resurrects the key"
    );
}
