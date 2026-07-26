//! Real-NATS coverage for durable all-NATS attempt-outcome reconciliation.

#![cfg(not(madsim))]

use async_nats::jetstream;
use futures::StreamExt;
use prost::Message;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::relay::{drain_attempt_outcomes, task_event_consumer};
use tickr_proto::coord::all_nats::{
    AttemptOutcome, ElectionDecision, TaskPickupRecord, TASK_PICKUP_BUCKET,
};
use tickr_proto::task as tc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default()
        .with_cmd(&cmd)
        .with_tag("2.14.2")
        .start()
        .await
    {
        Ok(container) => container,
        Err(error) => {
            eprintln!("skipping: NATS testcontainer unavailable: {error}");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{port}");
    let mut client = None;
    for _ in 0..50 {
        if let Ok(connected) = async_nats::connect(&url).await {
            client = Some(connected);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Some((container, client.expect("nats connect")))
}

fn dispatch() -> tc::TaskDispatch {
    tc::TaskDispatch {
        task_instance_id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::new_v4().to_string(),
        name: "outcome-test".to_owned(),
        task_type: 0,
        nix_expression_path: "/p".to_owned(),
        nix_args: vec![],
        outputs: vec![],
        inputs: vec![],
        secrets: vec![],
        tenant_id: "test".to_owned(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: vec![],
    }
}

async fn pull_one(consumer: &jetstream::consumer::PullConsumer) -> jetstream::Message {
    let mut messages = consumer
        .batch()
        .max_messages(1)
        .expires(Duration::from_secs(5))
        .messages()
        .await
        .expect("open TaskEvent pull");
    messages
        .next()
        .await
        .expect("TaskEvent delivery")
        .expect("valid TaskEvent delivery")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn due_deadline_elects_once_and_redelivers_until_forward_ack() {
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };
    let consumer = task_event_consumer(&nats)
        .await
        .expect("TaskEvent consumer");
    let js = jetstream::new(nats.clone());
    let pickup = js
        .create_key_value(jetstream::kv::Config {
            bucket: TASK_PICKUP_BUCKET.to_owned(),
            history: 1,
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await
        .expect("pickup outcome bucket");

    let task = dispatch();
    let key = "dispatch.1";
    let mut record = TaskPickupRecord {
        dispatch_key: key.to_owned(),
        payload: task.encode_to_vec(),
        pickup_generation: 1,
        owner: "executor-one".to_owned(),
        liveness_deadline_ms: 0,
        assigned_event: vec![1],
        assigned_staged: true,
        liveness_armed: true,
        source_completed: true,
        started_event: Some(vec![2]),
        terminal: None,
        rejected_reason: None,
    };
    pickup
        .create(
            key,
            serde_json::to_vec(&record).expect("encode record").into(),
        )
        .await
        .expect("stage due pickup while Conductor is down");

    let first_cycle = CancellationToken::new();
    let first_handle = tokio::spawn(drain_attempt_outcomes(
        nats.clone(),
        None,
        first_cycle.clone(),
    ));
    let first = pull_one(&consumer).await;
    let first_payload = first.payload.to_vec();
    let event = tc::TaskEvent::decode(&first_payload[..]).expect("decode elected TaskEvent");
    assert!(matches!(
        event.kind,
        Some(tc::task_event::Kind::Unhealthy(_))
    ));

    first_cycle.cancel();
    first_handle.await.expect("first Conductor cycle stops");
    first
        .ack_with(jetstream::AckKind::Nak(None))
        .await
        .expect("simulate restart before relay-forward acknowledgement");

    let stored = pickup
        .get(key)
        .await
        .expect("read elected record")
        .expect("elected record remains durable");
    record = serde_json::from_slice(&stored).expect("decode elected record");
    let terminal = record
        .terminal
        .as_ref()
        .expect("terminal election committed");
    assert_eq!(terminal.outcome, AttemptOutcome::LivenessExpired);
    assert!(terminal.event_enqueued);
    assert_eq!(terminal.event, first_payload);

    let duplicate = record.elect(
        key,
        1,
        "executor-one",
        AttemptOutcome::ProcessExitedFailure,
        &[9],
    );
    assert_eq!(
        duplicate,
        ElectionDecision::Settled(AttemptOutcome::LivenessExpired),
        "late process evidence observes the durable winner"
    );

    let second_cycle = CancellationToken::new();
    let second_handle = tokio::spawn(drain_attempt_outcomes(
        nats.clone(),
        None,
        second_cycle.clone(),
    ));
    let redelivery = pull_one(&consumer).await;
    assert_eq!(redelivery.payload.as_ref(), first_payload.as_slice());
    redelivery
        .ack()
        .await
        .expect("relay-forward boundary completes TaskEvent");
    second_cycle.cancel();
    second_handle.await.expect("second Conductor cycle stops");
}
