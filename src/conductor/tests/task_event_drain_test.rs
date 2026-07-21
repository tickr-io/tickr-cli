//! Real-NATS + real-Postgres integration test for the conductor's durable
//! task-event update consumer (`relay::task_event_consumer` /
//! `relay::drain_task_events`).
//!
//! Asserts the three guarantees of the update-leg durability:
//!   1. **redelivery on un-ack** — an event whose forward fails (the relay
//!      outbound is down) is NAK'd and redelivered once the path recovers,
//!      not dropped (the outage-survival path);
//!   2. **enrichment preserved** — the completing task's declared routing
//!      variable is stamped onto the forwarded event;
//!   3. **ack-on-forward** — once forwarded, the work-queue message is acked
//!      and removed from the stream.
//!
//! Requires Docker (testcontainers). Skipped automatically when NATS or
//! Postgres containers are unavailable — the startup failure is the skip
//! marker, matching the other conductor integration tests.

#![cfg(not(madsim))]

mod common;

use async_nats::jetstream;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::proto::{ConductorRelayMessage, EntityType};
use tickr_conductor::relay::{drain_task_events, task_event_consumer};
use tickr_proto::coord::{TASK_EVENT_CONSUMER, TASK_EVENT_STREAM, TASK_EVENT_SUBJECT};
use tickr_proto::task as tc;
use tickr_proto::workflow as wf;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
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

async fn start_postgres() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

/// The producer task's ids plus its declared `decision` routing variable, so
/// the enrichment has a spec to match the emitted output against.
fn workflow_with_routing_var() -> (Uuid, Uuid, Vec<wf::RoutingVarDecl>) {
    let workflow_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let specs = vec![wf::RoutingVarDecl {
        name: "decision".to_string(),
        var_type: None,
    }];
    (workflow_id, task_id, specs)
}

/// Seed the declared routing-variable spec into the unified `task_id`-keyed
/// `task_specs` store — the ONLY source enrichment reads (it queries neither
/// the `workflows` table nor the definition JSONB). This test writes the store
/// directly, bypassing the register pipeline that would normally populate it;
/// otherwise every completion fails closed on a zero-row `LookupIntegrity` and
/// the drain NAK-redelivers it forever.
async fn seed_task_spec(pool: &sqlx::PgPool, task_id: Uuid, specs: &[wf::RoutingVarDecl]) {
    let routing_vars = serde_json::to_value(specs).expect("serialize specs");
    sqlx::query(
        "INSERT INTO task_specs (task_id, routing_vars) VALUES ($1, $2) ON CONFLICT (task_id) DO NOTHING",
    )
    .bind(task_id)
    .bind(&routing_vars)
    .execute(pool)
    .await
    .expect("insert task_specs row");
}

/// Write the task's emitted `decision` output into the `ctx-default` KV bucket
/// under `<run_id>/decision`, shaped as the v=2 ctx envelope the enrichment
/// read path expects (producer is this task instance). UUIDs are
/// sanitize-identity so the raw key matches the read-side prefix.
async fn seed_ctx_output(nats: &async_nats::Client, run_id: Uuid, task_instance_id: Uuid) {
    let js = jetstream::new(nats.clone());
    let kv = js
        .create_key_value(jetstream::kv::Config {
            bucket: "ctx-default".to_string(),
            ..Default::default()
        })
        .await
        .expect("create ctx-default bucket");
    let envelope = serde_json::json!({
        "v": 2,
        "type": "string",
        "value": "approve",
        "secret": false,
        "producer": {
            "kind": "task",
            "task_id": task_instance_id.to_string(),
            "task_name": "producer",
        },
        "created_at": "2026-01-01T00:00:00Z",
        "sha256": "0",
    });
    let key = format!("{}/decision", run_id);
    kv.put(key, serde_json::to_vec(&envelope).unwrap().into())
        .await
        .expect("put ctx output");
}

/// Pre-create the durable task-event consumer with a short `ack_wait` so the
/// test's redelivery path is fast and deterministic. Production's
/// `task_event_consumer` then `get_or_create`s the same durable name and
/// reuses this consumer. (With the default 30s ack_wait, a NAK'd message that
/// races into a dropped puller's in-flight pull only redelivers after that
/// pull expires — too slow for a test, though correct.)
async fn precreate_fast_consumer(nats: &async_nats::Client) {
    let js = jetstream::new(nats.clone());
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: TASK_EVENT_STREAM.to_string(),
            subjects: vec![TASK_EVENT_SUBJECT.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .expect("create stream");
    stream
        .get_or_create_consumer(
            TASK_EVENT_CONSUMER,
            jetstream::consumer::pull::Config {
                durable_name: Some(TASK_EVENT_CONSUMER.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .await
        .expect("create consumer");
}

async fn stream_message_count(nats: &async_nats::Client) -> u64 {
    let js = jetstream::new(nats.clone());
    let mut stream = js.get_stream(TASK_EVENT_STREAM).await.expect("get stream");
    stream.info().await.expect("stream info").state.messages
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_consumer_redelivers_on_unack_then_acks_on_forward_with_enrichment() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    // Seed the declared routing-var spec (PG) and the emitted output (NATS KV).
    let (workflow_id, task_id, specs) = workflow_with_routing_var();
    seed_task_spec(&pool, task_id, &specs).await;
    let run_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    seed_ctx_output(&nats, run_id, task_instance_id).await;

    // Ensure the stream/consumer exist (short ack_wait so redelivery is fast),
    // then publish one bare `completed` TaskEvent (empty routing vars — the
    // conductor enriches).
    precreate_fast_consumer(&nats).await;
    let _consumer = task_event_consumer(&nats).await.expect("consumer");
    let event = tc::TaskEvent {
        task_instance_id: task_instance_id.to_string(),
        task_id: task_id.to_string(),
        workflow_instance_id: run_id.to_string(),
        workflow_id: workflow_id.to_string(),
        executor_id: Some(Uuid::new_v4().to_string()),
        kind: Some(tc::task_event::Kind::Completed(tc::task_event::Completed {
            routing_variables: std::collections::HashMap::new(),
            self_patch: None,
            self_patch_stall_ttl: None,
        })),
    };
    let js = jetstream::new(nats.clone());
    js.publish(TASK_EVENT_SUBJECT, event.encode_to_vec().into())
        .await
        .expect("publish")
        .await
        .expect("publish ack");
    let definitions = Arc::new(
        tickr_migrations::backend::WriterRepositoryBundle::from_postgres_pool(
            pool.as_ref().clone(),
        ),
    );

    // --- 1. Simulated outage: drain with a CLOSED relay channel. The forward
    // fails, the drain NAKs and stops — the event is NOT acked, so it stays in
    // the durable queue.
    {
        let (closed_tx, closed_rx) = mpsc::channel::<ConductorRelayMessage>(1);
        drop(closed_rx); // relay outbound is "down"
        let consumer = task_event_consumer(&nats).await.expect("consumer");
        let token = CancellationToken::new();
        drain_task_events(
            consumer,
            closed_tx,
            Arc::clone(&definitions),
            nats.clone(),
            token,
        )
        .await;
    }
    // Still parked in the queue — redelivery must keep it.
    assert!(
        stream_message_count(&nats).await >= 1,
        "un-acked event must remain in the durable queue"
    );

    // --- 2. Recovery: drain with a WORKING relay channel. The event redelivers,
    // is enriched, forwarded, and acked.
    let (open_tx, mut open_rx) = mpsc::channel::<ConductorRelayMessage>(4);
    let consumer = task_event_consumer(&nats).await.expect("consumer");
    let token = CancellationToken::new();
    let drain_token = token.clone();
    let drain_nats = nats.clone();
    let drain_definitions = Arc::clone(&definitions);
    let handle = tokio::spawn(async move {
        drain_task_events(
            consumer,
            open_tx,
            drain_definitions,
            drain_nats,
            drain_token,
        )
        .await;
    });

    let msg = tokio::time::timeout(Duration::from_secs(15), open_rx.recv())
        .await
        .expect("forward did not arrive — redelivery failed")
        .expect("relay channel closed");
    assert_eq!(msg.entity_type, EntityType::TaskEvent as i32);

    let forwarded = tc::TaskEvent::decode(&msg.payload[..]).expect("decode forwarded event");
    // --- enrichment preserved: the declared `decision` rides the completed.
    match &forwarded.kind {
        Some(tc::task_event::Kind::Completed(completed)) => {
            assert_eq!(
                completed
                    .routing_variables
                    .get("decision")
                    .and_then(|v| match &v.value {
                        Some(wf::routing_value::Value::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    }),
                Some("approve"),
                "conductor enrichment must stamp the declared routing variable"
            );
        }
        other => panic!("expected Completed, got {:?}", other),
    }

    // --- ack-on-forward: the work-queue message is removed once forwarded.
    let mut acked = false;
    for _ in 0..40 {
        if stream_message_count(&nats).await == 0 {
            acked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        acked,
        "forwarded event must be acked and removed from the queue"
    );

    token.cancel();
    let _ = handle.await;
}
