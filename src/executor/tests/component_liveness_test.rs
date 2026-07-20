//! Real-NATS integration test for the executor's **component-liveness key**
//! (`component_liveness::ensure_component_liveness_bucket` /
//! `spawn_component_liveness`).
//!
//! Asserts the externally-observable lifecycle the fleet-count / saturation
//! gauge rides on:
//!   1. **PUT-at-boot with the `{cap, in_flight}` schema** — `executor.<uuid>`
//!      appears in `tickr_component_liveness` with `cap` = the dispatch cap and
//!      `in_flight` derived from the shared semaphore (`cap − available_permits`);
//!   2. **re-arm on the `TTL/4` cadence** — the key survives past a single TTL
//!      window, which it could only do by being re-armed (a lone arm would have
//!      reaped at the TTL);
//!   3. **self-reap on stop** — once the loop is cancelled, re-arming stops and
//!      the key self-reaps by TTL (stays gone, no resurrection).
//!
//! Requires Docker (testcontainers). Skipped automatically when the NATS
//! container is unavailable — the startup failure is the skip marker, matching
//! the other executor/conductor integration tests.

use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_executor::component_liveness::{
    ensure_component_liveness_bucket, spawn_component_liveness,
};
use tickr_executor::task_liveness::LivenessConfig;
use tickr_proto::coord::{component_liveness_key, ComponentLivenessValue};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
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

/// Poll the bucket until the key is present (returns its decoded value) or the
/// budget runs out.
async fn await_value(
    kv: &async_nats::jetstream::kv::Store,
    key: &str,
) -> Option<ComponentLivenessValue> {
    for _ in 0..50 {
        if let Some(bytes) = kv.get(key).await.expect("kv get") {
            return Some(serde_json::from_slice(&bytes).expect("decode component value"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Poll the bucket until the key is gone or the budget runs out.
async fn await_gone(kv: &async_nats::jetstream::kv::Store, key: &str) -> bool {
    for _ in 0..120 {
        if kv.get(key).await.expect("kv get").is_none() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn component_key_arms_with_schema_re_arms_and_self_reaps() {
    let Some((_container, nats)) = start_nats().await else {
        return; // Docker/NATS unavailable — skip.
    };

    let kv = ensure_component_liveness_bucket(&nats)
        .await
        .expect("ensure component-liveness bucket");

    const CAP: usize = 4;
    let semaphore = Arc::new(Semaphore::new(CAP));
    // Hold two permits so in_flight (= cap − available_permits) reads exactly 2,
    // proving it is derived from the shared semaphore, not a separate counter.
    let held: Vec<OwnedSemaphorePermit> = vec![
        semaphore.clone().acquire_owned().await.unwrap(),
        semaphore.clone().acquire_owned().await.unwrap(),
    ];

    // Timeout 4s → derived cadence 1s. Long enough that the key never expires
    // inside a cadence, short enough to drive several re-arms and a quick reap.
    let config = LivenessConfig {
        timeout: Duration::from_secs(4),
    };
    let executor_id = Uuid::new_v4();
    let key = component_liveness_key(executor_id);
    let shutdown = CancellationToken::new();

    let handle = spawn_component_liveness(
        Arc::new(nats.clone()),
        config,
        Arc::clone(&semaphore),
        CAP,
        executor_id,
        shutdown.clone(),
    );

    // (1) PUT-at-boot with the {cap, in_flight} schema.
    let value = await_value(&kv, &key)
        .await
        .expect("component key must appear at boot");
    assert_eq!(
        value.cap, CAP,
        "cap must equal the dispatch concurrency cap"
    );
    assert_eq!(
        value.in_flight, 2,
        "in_flight must be cap − available_permits off the shared semaphore"
    );

    // (2) re-arm on the TTL/4 cadence: after > one full TTL the key is still
    // present — only possible because the loop re-armed it (a lone arm reaps).
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(
        kv.get(&key).await.expect("kv get").is_some(),
        "key must survive past one TTL, proving TTL/4 re-arm"
    );

    // (3) self-reap on stop: cancel the loop, wait for it to observe the cancel,
    // then confirm the key reaps by TTL and stays gone (no re-arm resurrects it).
    shutdown.cancel();
    let _ = handle.await;
    drop(held);
    assert!(
        await_gone(&kv, &key).await,
        "after the re-arm loop stops, the component key must self-reap by TTL"
    );
}
