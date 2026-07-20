//! Real-NATS integration test for the executor's durable task-dispatch
//! consumer (`task_handler::dispatch_consumer` / `drain_dispatch_to_capacity`).
//!
//! Asserts the two guarantees of the dispatch-side durability:
//!   1. **pull-to-capacity** — the executor pulls at most `cap` tasks
//!      concurrently; the remainder wait durably in the work queue until a slot
//!      frees, then are pulled and run (no available executor ≠ dropped);
//!   2. **ack-on-pickup** — a pulled message is acked (removed from the queue)
//!      the moment it is picked up (at-most-once execution).
//!
//! Requires Docker (testcontainers). Skipped automatically when the NATS
//! container is unavailable — the startup failure is the skip marker, matching
//! the conductor integration tests.

use async_nats::jetstream;
use prost::Message;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_executor::task_handler::{dispatch_consumer, drain_dispatch_to_capacity};
use tickr_proto::coord::{TASK_DISPATCH_STREAM, TASK_DISPATCH_SUBJECT};
use tickr_proto::task as tc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default().with_cmd(&cmd).start().await {
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

/// A minimal dispatch on the published `TaskDispatch` contract — enough valid
/// identity for the executor's decode to reconstruct the execution slice. The
/// drain handler under test ignores the payload, so only decode-validity matters.
fn fresh_dispatch_item() -> tc::TaskDispatch {
    let workflow_id = Uuid::new_v4();
    tc::TaskDispatch {
        task_instance_id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: workflow_id.to_string(),
        name: "dispatch-task".to_string(),
        // The executor never reads task_type; the proto default (RegularTask) is fine.
        task_type: 0,
        nix_expression_path: "/p".to_string(),
        nix_args: vec![],
        outputs: vec![],
        inputs: vec![],
        secrets: vec![],
        tenant_id: "test".to_string(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: vec![],
    }
}

async fn publish_dispatch(nats: &async_nats::Client, item: &tc::TaskDispatch) {
    let js = jetstream::new(nats.clone());
    js.publish(TASK_DISPATCH_SUBJECT, item.encode_to_vec().into())
        .await
        .expect("publish dispatch")
        .await
        .expect("publish ack");
}

async fn queue_depth(nats: &async_nats::Client) -> u64 {
    let js = jetstream::new(nats.clone());
    let mut stream = js
        .get_stream(TASK_DISPATCH_STREAM)
        .await
        .expect("get dispatch stream");
    stream.info().await.expect("stream info").state.messages
}

/// Poll `queue_depth` until it equals `want` or the budget runs out.
async fn await_queue_depth(nats: &async_nats::Client, want: u64) -> bool {
    for _ in 0..100 {
        if queue_depth(nats).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn pull_to_capacity_bounds_concurrent_pulls_and_acks_on_pickup() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };

    const CAP: usize = 2;
    const TOTAL: usize = 5;

    // Create the work queue + shared durable consumer, then publish more
    // dispatches than the cap.
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    for _ in 0..TOTAL {
        publish_dispatch(&nats, &fresh_dispatch_item()).await;
    }
    assert!(
        await_queue_depth(&nats, TOTAL as u64).await,
        "all dispatched tasks must be durably staged in the work queue"
    );

    // Drain pull-to-capacity with a handler that blocks (holds its slot) until
    // `release` fires — so the cap genuinely bounds concurrent in-flight pulls.
    let pulled = Arc::new(AtomicUsize::new(0));
    let release = CancellationToken::new();
    let shutdown = CancellationToken::new();
    let tracker = TaskTracker::new();

    let drain_pulled = Arc::clone(&pulled);
    let drain_release = release.clone();
    let handle = tokio::spawn(drain_dispatch_to_capacity(
        consumer,
        Arc::new(Semaphore::new(CAP)),
        tracker.clone(),
        shutdown.clone(),
        move |_item| {
            let pulled = Arc::clone(&drain_pulled);
            let release = drain_release.clone();
            async move {
                pulled.fetch_add(1, Ordering::SeqCst);
                // Hold the slot until released — simulates a long-running task.
                release.cancelled().await;
            }
        },
    ));

    // Exactly CAP tasks get pulled; ack-on-pickup removes them, leaving the
    // remainder durably queued. Wait for the cap to fill, then confirm it does
    // not creep past the cap while the slots stay held.
    for _ in 0..100 {
        if pulled.load(Ordering::SeqCst) == CAP {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        pulled.load(Ordering::SeqCst),
        CAP,
        "pull-to-capacity must pull at most `cap` tasks while slots are held"
    );
    assert!(
        await_queue_depth(&nats, (TOTAL - CAP) as u64).await,
        "ack-on-pickup removes the pulled tasks; the remainder waits in the queue"
    );

    // Release the held slots: the freed capacity lets the executor pull the
    // remainder — an unpicked dispatch waits, then is pulled and run.
    release.cancel();
    assert!(
        await_queue_depth(&nats, 0).await,
        "once slots free, the remaining dispatches are pulled and acked"
    );
    for _ in 0..100 {
        if pulled.load(Ordering::SeqCst) == TOTAL {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        pulled.load(Ordering::SeqCst),
        TOTAL,
        "every durably-staged dispatch is eventually pulled and run"
    );

    shutdown.cancel();
    tracker.close();
    tracker.wait().await;
    let _ = handle.await;
}
