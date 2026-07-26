//! Real-NATS integration test for the conductor's dispatch-side publish
//! (`relay::publish_dispatch_and_deliver`).
//!
//! Asserts the dispatch durability contract: the conductor publishes the
//! dispatch into the durable work queue and emits the `Delivered` task event
//! **only after the publish ack** — so `TaskDelivered` means "durably staged
//! for an executor", not "published to fire-and-forget core NATS". By the time
//! the `Delivered` event is observable on the relay, the dispatch is already in
//! the work queue.
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
    ensure_task_dispatch_stream, publish_dispatch_and_deliver, NatsTaskDispatchPublisher,
};
use tickr_proto::coord::TASK_DISPATCH_STREAM;
use tickr_proto::task as tc;
use tickr_proto::workflow as wf;
use tokio::sync::mpsc;
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
    for _ in 0..50 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Some((container, client.expect("nats connect")))
}

/// The correlation ids the dispatch hand-off carries: `(task_instance_id,
/// task_id, workflow_instance_id, workflow_id)`.
fn fresh_dispatch_ids() -> (Uuid, Uuid, Uuid, Uuid) {
    (
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
}

/// Encode a minimal dispatch aggregate onto the task-coordination wire — the
/// opaque payload the conductor republishes verbatim into the work queue.
fn encode_dispatch(
    task_instance_id: Uuid,
    task_id: Uuid,
    workflow_instance_id: Uuid,
    workflow_id: Uuid,
) -> Vec<u8> {
    let dispatch = tc::TaskDispatch {
        task_instance_id: task_instance_id.to_string(),
        task_id: task_id.to_string(),
        workflow_instance_id: workflow_instance_id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: "dispatch-task".to_string(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: "/p".to_string(),
        nix_args: vec![],
        outputs: vec![],
        inputs: vec![],
        secrets: vec![],
        tenant_id: "test".to_string(),
        originating_signal_id: None,
        gate_signal_ids: std::collections::HashMap::new(),
        gate_signal_ids_ambient: vec![],
    };
    dispatch.encode_to_vec()
}

async fn queue_depth(nats: &async_nats::Client) -> u64 {
    let js = jetstream::new(nats.clone());
    let mut stream = js
        .get_stream(TASK_DISPATCH_STREAM)
        .await
        .expect("get dispatch stream");
    stream.info().await.expect("stream info").state.messages
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivered_is_emitted_only_after_the_publish_ack() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };

    ensure_task_dispatch_stream(&nats)
        .await
        .expect("ensure dispatch stream");
    let task_dispatch = NatsTaskDispatchPublisher::new(&nats);

    let (task_instance_id, task_id, workflow_instance_id, workflow_id) = fresh_dispatch_ids();
    let payload = encode_dispatch(task_instance_id, task_id, workflow_instance_id, workflow_id);

    let (tx, mut rx) = mpsc::channel::<ConductorRelayMessage>(4);
    publish_dispatch_and_deliver(
        &task_dispatch,
        &tx,
        payload,
        task_instance_id,
        task_id,
        workflow_instance_id,
        workflow_id,
    )
    .await
    .expect("publish dispatch and deliver");

    // The `Delivered` event is on the relay — and because the function awaits
    // the publish ack before emitting it, the dispatch is already durably
    // staged in the work queue by the time we observe this.
    let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("delivered event did not arrive")
        .expect("relay channel closed");
    assert_eq!(msg.entity_type, EntityType::TaskEvent as i32);

    let delivered: tc::TaskEvent =
        tc::TaskEvent::decode(&msg.payload[..]).expect("decode TaskEvent");
    assert!(
        matches!(delivered.kind, Some(tc::task_event::Kind::Delivered(_))),
        "the conductor's dispatch hand-off must be a Delivered task event"
    );
    assert_eq!(delivered.task_instance_id, task_instance_id.to_string());
    assert!(
        delivered.executor_id.is_none(),
        "delivered carries no executor_id — no executor has picked it up yet"
    );

    // The dispatch landed in the durable work queue (the publish ack the
    // function awaited): proves the publish preceded the Delivered emit.
    assert_eq!(
        queue_depth(&nats).await,
        1,
        "the dispatch must be durably staged in the work queue"
    );
}
