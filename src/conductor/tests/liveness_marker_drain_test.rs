//! Real-NATS integration test for the conductor's liveness marker-consumer
//! (`relay::liveness_marker_consumer` / `relay::drain_liveness_markers`).
//!
//! Asserts the watchdog's consumer-side guarantees:
//!   1. **expiry → Unhealthy, acked on forward** — a true per-key-TTL expiry
//!      marker becomes exactly one conductor-origin `TaskEvent{Unhealthy}`
//!      carrying the parsed identity, forwarded onto the relay;
//!   2. **explicit-delete filtered** — the executor's terminal `delete` (a
//!      `KV-Operation: DEL` tombstone) is NOT forwarded;
//!   3. **redelivery on un-ack** — a marker whose forward fails (relay outbound
//!      down) is NAK'd and redelivered once the path recovers, not lost.
//!
//! Requires Docker (testcontainers). Skipped automatically when the NATS
//! container is unavailable — the startup failure is the skip marker.

#![cfg(not(madsim))]

use async_nats::jetstream;
use prost::Message;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::proto::{ConductorRelayMessage, EntityType};
use tickr_conductor::relay::{
    drain_liveness_markers, ensure_liveness_bucket, liveness_marker_consumer,
};
use tickr_proto::coord::{liveness_key, LIVENESS_BUCKET};
use tickr_proto::task as tc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    // Pin 2.14.2 — per-key KV TTL + delete markers need the same NATS the dev
    // infra runs; the testcontainers default tag is older.
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
    for _ in 0..50 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Some((container, client.expect("nats connect")))
}

/// Arm a liveness key with a per-message TTL (whole seconds), the executor's
/// arm primitive: a direct publish to the KV subject carrying `Nats-TTL`.
async fn arm_key(nats: &async_nats::Client, key: &str, ttl_secs: u64) {
    let js = jetstream::new(nats.clone());
    let subject = format!("$KV.{LIVENESS_BUCKET}.{key}");
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        async_nats::header::NATS_MESSAGE_TTL,
        ttl_secs.to_string().as_str(),
    );
    js.publish_with_headers(subject, headers, "alive".into())
        .await
        .expect("arm publish")
        .await
        .expect("arm publish ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expiry_forwards_unhealthy_and_explicit_delete_is_filtered() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let kv = ensure_liveness_bucket(&nats)
        .await
        .expect("ensure liveness bucket");

    // Key A: armed with a 1s TTL, then left to expire → true expiry marker.
    let (wf_a, wi_a, ti_a) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let key_a = liveness_key(wf_a, wi_a, ti_a);
    arm_key(&nats, &key_a, 1).await;

    // Key B: armed long, then explicitly deleted → DEL tombstone (terminal
    // teardown), which must be filtered out (not forwarded as Unhealthy).
    let (wf_b, wi_b, ti_b) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let key_b = liveness_key(wf_b, wi_b, ti_b);
    arm_key(&nats, &key_b, 60).await;
    kv.delete(&key_b).await.expect("explicit delete");

    // Let key A's TTL elapse so its expiry marker is appended.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let consumer = liveness_marker_consumer(&nats)
        .await
        .expect("marker consumer");
    let (tx, mut rx) = mpsc::channel::<ConductorRelayMessage>(8);
    let token = CancellationToken::new();
    let drain_token = token.clone();
    let handle = tokio::spawn(async move {
        drain_liveness_markers(consumer, tx, drain_token).await;
    });

    // Exactly one forward — key A's expiry as an Unhealthy TaskEvent.
    let msg = tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("Unhealthy forward did not arrive")
        .expect("relay channel closed");
    assert_eq!(msg.entity_type, EntityType::TaskEvent as i32);
    let ev = tc::TaskEvent::decode(&msg.payload[..]).expect("decode forwarded event");
    assert!(
        matches!(ev.kind, Some(tc::task_event::Kind::Unhealthy(_))),
        "the forwarded marker must be an Unhealthy task event"
    );
    assert_eq!(
        ev.task_instance_id,
        ti_a.to_string(),
        "identity parsed off the key"
    );
    assert_eq!(ev.workflow_id, wf_a.to_string());
    assert_eq!(ev.workflow_instance_id, wi_a.to_string());
    assert_eq!(ev.executor_id, None, "executor is gone — identity-only");

    // No second forward: key B's explicit-delete tombstone is filtered out.
    let second = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    assert!(
        second.is_err(),
        "explicit-delete marker must be filtered (no Unhealthy forward)"
    );

    token.cancel();
    let _ = handle.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unacked_marker_is_redelivered() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let _kv = ensure_liveness_bucket(&nats)
        .await
        .expect("ensure liveness bucket");

    let (wf, wi, ti) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let key = liveness_key(wf, wi, ti);
    arm_key(&nats, &key, 1).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // let it expire

    // --- 1. Outage: drain with a CLOSED relay channel. The forward fails, the
    // drain NAKs and stops — the marker is NOT acked, so it stays pending.
    {
        let (closed_tx, closed_rx) = mpsc::channel::<ConductorRelayMessage>(1);
        drop(closed_rx);
        let consumer = liveness_marker_consumer(&nats)
            .await
            .expect("marker consumer");
        let token = CancellationToken::new();
        drain_liveness_markers(consumer, closed_tx, token).await;
    }

    // --- 2. Recovery: drain with a WORKING relay channel. The marker redelivers
    // and is forwarded as Unhealthy.
    let consumer = liveness_marker_consumer(&nats)
        .await
        .expect("marker consumer");
    let (open_tx, mut open_rx) = mpsc::channel::<ConductorRelayMessage>(4);
    let token = CancellationToken::new();
    let drain_token = token.clone();
    let handle = tokio::spawn(async move {
        drain_liveness_markers(consumer, open_tx, drain_token).await;
    });

    let msg = tokio::time::timeout(Duration::from_secs(15), open_rx.recv())
        .await
        .expect("forward did not arrive — redelivery failed")
        .expect("relay channel closed");
    let ev = tc::TaskEvent::decode(&msg.payload[..]).expect("decode forwarded event");
    assert!(
        matches!(ev.kind, Some(tc::task_event::Kind::Unhealthy(_))),
        "the redelivered marker must forward as an Unhealthy task event"
    );
    assert_eq!(ev.task_instance_id, ti.to_string());

    token.cancel();
    let _ = handle.await;
}
