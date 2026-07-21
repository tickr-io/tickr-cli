//! Real-NATS integration test for `nats_ingress`.
//!
//! Spins up an ephemeral Postgres (for the conductor's `workflows` table)
//! and an ephemeral NATS-with-JetStream (for the ingress subject + the
//! idempotency cache + the ctx-scope captures bucket). Publishes a v=1
//! Trigger envelope onto `tickr.external.signals`, runs the translator,
//! and asserts the wire `Signal::Trigger` arrives on a test relay channel.
//!
//! Requires Docker running (testcontainers). Skipped automatically when
//! Docker isn't available — the connection failure is the skip marker.

#![cfg(not(madsim))]

mod common;

use async_nats::jetstream;
use async_trait::async_trait;
use prost::Message;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::nats_ingress::{self, RelaySendOutcome, RelaySender};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_proto::codec::definition::definition_proto_to_json;
use tickr_proto::signal as sp;
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

async fn start_postgres_with_migrations() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

async fn insert_workflow(pool: &sqlx::PgPool, workflow: &wf::WorkflowDefinition) {
    let definition = definition_proto_to_json(workflow).expect("encode proto definition");
    sqlx::query(
        "INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
         VALUES ($1, $2, 'default', 'wf', $3, 'Ready', 'testhash', 'testcos', $4, '')",
    )
    .bind(Uuid::parse_str(&workflow.id).expect("workflow id"))
    .bind(workflow.version)
    .bind(&workflow.name)
    .bind(definition)
    .execute(pool)
    .await
    .expect("insert workflow row");
}

fn empty_workflow(name: &str) -> wf::WorkflowDefinition {
    wf::WorkflowDefinition {
        id: Uuid::new_v4().to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: name.to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

/// Capturing relay sender for tests. Each test instantiates its own and
/// passes it into `run_translator_with_sender`, so parallel tests don't
/// race on a shared global.
struct CapturingRelaySender {
    tx: mpsc::Sender<sp::Signal>,
}

#[async_trait]
impl RelaySender for CapturingRelaySender {
    async fn try_send(&self, signal: &sp::Signal) -> RelaySendOutcome {
        match self.tx.try_send(signal.clone()) {
            Ok(()) => RelaySendOutcome::Sent,
            Err(mpsc::error::TrySendError::Full(_)) => RelaySendOutcome::Saturated,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                RelaySendOutcome::Error(anyhow::anyhow!("test channel closed"))
            }
        }
    }
}

/// Always-saturated relay sender for the buffer-saturation NAK test. Returns
/// `Saturated` every time so the translator must NAK rather than ack.
struct AlwaysSaturatedSender;

#[async_trait]
impl RelaySender for AlwaysSaturatedSender {
    async fn try_send(&self, _signal: &sp::Signal) -> RelaySendOutcome {
        RelaySendOutcome::Saturated
    }
}

/// Slice 01 acceptance: a v=1 Trigger envelope published to the subject
/// flows through the translator into a forwarded `Signal::Trigger` on the
/// relay outbound channel. No captures declared, no inputs sent — the
/// minimal end-to-end happy path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_envelope_flows_end_to_end() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    // Register a workflow row so the translator's lookup succeeds.
    let workflow = empty_workflow("nats-ingress-test-trigger");
    let workflow_id = Uuid::parse_str(&workflow.id).expect("workflow id");
    insert_workflow(&pool, &workflow).await;

    // Capturing relay sender so the translator's forwarded Signal is
    // observable without the global relay channel and without standing up
    // the full gRPC streaming connection. Per-test instance avoids the
    // race other parallel tests would otherwise create on a process-wide
    // sender slot.
    let (relay_tx, mut relay_rx) = mpsc::channel::<sp::Signal>(32);
    let sender = Arc::new(CapturingRelaySender { tx: relay_tx });

    // Spawn the translator. It creates the stream + consumer if absent on
    // its first iteration, then waits for messages.
    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    // Wait for the translator to create the stream + consumer before
    // publishing. A short poll is reliable: `init_stream_and_consumer`
    // returns on the first translator-loop iteration, well under 1s.
    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready, "translator must create the JetStream stream");

    // Publish a v=1 Trigger envelope.
    let idempotency_key = format!("ext-test-{}", Uuid::new_v4());
    let envelope = json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": idempotency_key,
        "workflow_id": workflow_id.to_string(),
    });
    let bytes = serde_json::to_vec(&envelope).expect("encode envelope");

    js.publish(nats_ingress::SUBJECT, bytes.into())
        .await
        .expect("publish envelope")
        .await
        .expect("publish ack");

    // Receive the forwarded signal. The translator's processing chain is
    // non-trivial (idempotency cache, captures extraction, Postgres + NATS
    // writes), so a generous deadline absorbs container startup jitter
    // without giving false confidence on a hang.
    let signal = tokio::time::timeout(Duration::from_secs(10), relay_rx.recv())
        .await
        .expect("relay receives the forwarded signal within deadline")
        .expect("channel still open");

    assert_eq!(
        signal.idempotency_key.as_deref(),
        Some(idempotency_key.as_str())
    );
    match signal.variant {
        Some(sp::signal::Variant::Trigger(t)) => {
            assert_eq!(t.workflow_id, workflow_id.to_string());
            assert!(t.scheduled_at.is_none());
            // External NATS-ingress path stamps the wire source so the
            // server records `External` provenance on the resulting instance.
            match t.source.and_then(|s| s.source) {
                Some(sp::trigger_source::Source::External(e)) => {
                    assert_eq!(e.subject, "tickr.external.signals");
                }
                other => panic!("expected TriggerSource::External, got {:?}", other),
            }
        }
        other => panic!("expected Signal::Trigger, got {:?}", other),
    }

    shutdown.cancel();
    let _ = translator.await;
}

/// Stream + consumer creation is create-if-absent: a second translator
/// instance attaching to the same NATS server picks up the existing
/// stream and consumer rather than failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_and_consumer_creation_is_idempotent() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    // First init: should create the stream + consumer.
    let _ = nats_ingress::init_stream_and_consumer(&nats)
        .await
        .expect("first init creates");

    // Second init: must succeed against the existing stream + consumer.
    let _ = nats_ingress::init_stream_and_consumer(&nats)
        .await
        .expect("second init reuses");

    // Sanity: the stream + consumer exist.
    let js = jetstream::new(nats.clone());
    js.get_stream(nats_ingress::STREAM_NAME)
        .await
        .expect("stream exists");

    drop(pool);
}

/// Slice 02 acceptance: a v=1 Cancel envelope flows end-to-end producing
/// a `Signal::Cancel` on the relay outbound channel with the expected
/// target / reason / note fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_envelope_flows_end_to_end() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let (relay_tx, mut relay_rx) = mpsc::channel::<sp::Signal>(32);
    let sender = Arc::new(CapturingRelaySender { tx: relay_tx });

    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    let instance_id = Uuid::new_v4();
    let idempotency_key = format!("ext-cancel-{}", Uuid::new_v4());
    let envelope = json!({
        "version": 1,
        "variant": "Cancel",
        "idempotency_key": idempotency_key,
        "target": { "Instance": { "workflow_instance_id": instance_id.to_string(), "node_id": null } },
        "reason": { "UserRequested": { "actor": "alice" } },
        "note": "operator cancel",
    });
    let bytes = serde_json::to_vec(&envelope).expect("encode envelope");

    js.publish(nats_ingress::SUBJECT, bytes.into())
        .await
        .expect("publish envelope")
        .await
        .expect("publish ack");

    let signal = tokio::time::timeout(Duration::from_secs(10), relay_rx.recv())
        .await
        .expect("relay receives the forwarded signal within deadline")
        .expect("channel still open");

    assert_eq!(
        signal.idempotency_key.as_deref(),
        Some(idempotency_key.as_str())
    );
    match signal.variant {
        Some(sp::signal::Variant::Cancel(c)) => {
            match c.target.and_then(|t| t.addressing) {
                Some(sp::target::Addressing::Instance(i)) => {
                    assert_eq!(i.workflow_instance_id, instance_id.to_string());
                    assert_eq!(i.node_id, None);
                }
                other => panic!("expected Target::Instance, got {:?}", other),
            }
            match c.reason.and_then(|r| r.reason) {
                Some(sp::cancel_reason::Reason::UserRequested(u)) => {
                    assert_eq!(u.actor.as_deref(), Some("alice"));
                }
                other => panic!("expected UserRequested, got {:?}", other),
            }
            assert_eq!(c.note.as_deref(), Some("operator cancel"));
        }
        other => panic!("expected Signal::Cancel, got {:?}", other),
    }

    shutdown.cancel();
    let _ = translator.await;
}

/// External Cancel redelivery is absorbed by the NATS idempotency bucket.
/// The duplicate performs no SQL-backed Signal audit write and produces no
/// second relay effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_redelivery_uses_nats_idempotency_without_sql_or_second_relay() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let (relay_tx, mut relay_rx) = mpsc::channel::<sp::Signal>(32);
    let sender = Arc::new(CapturingRelaySender { tx: relay_tx });

    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    let instance_id = Uuid::new_v4();
    let idempotency_key = format!("ext-cancel-redelivery-{}", Uuid::new_v4());
    let envelope = json!({
        "version": 1,
        "variant": "Cancel",
        "idempotency_key": idempotency_key,
        "target": { "Instance": { "workflow_instance_id": instance_id.to_string(), "node_id": null } },
        "reason": { "UserRequested": { "actor": "alice" } },
        "note": "operator cancel",
    });
    let bytes = serde_json::to_vec(&envelope).expect("encode envelope");

    // First publish → Fresh → forward.
    js.publish(nats_ingress::SUBJECT, bytes.clone().into())
        .await
        .expect("publish 1")
        .await
        .expect("publish 1 ack");
    let first = tokio::time::timeout(Duration::from_secs(10), relay_rx.recv())
        .await
        .expect("first arrival forwarded")
        .expect("channel still open");
    let first_signal_id = first.signal_id;

    let dedup_baseline = nats_ingress::signals_deduplicated();

    // Second publish with byte-identical payload → Deduplicated → no forward.
    js.publish(nats_ingress::SUBJECT, bytes.into())
        .await
        .expect("publish 2")
        .await
        .expect("publish 2 ack");

    let second = tokio::time::timeout(Duration::from_millis(750), relay_rx.recv()).await;
    assert!(
        second.is_err(),
        "second publish must NOT produce a second relay forward (got {:?}, first was {})",
        second,
        first_signal_id
    );

    // Dedup counter increments past baseline.
    let mut dedup_bumped = false;
    for _ in 0..50 {
        if nats_ingress::signals_deduplicated() > dedup_baseline {
            dedup_bumped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        dedup_bumped,
        "signals_deduplicated counter must increment for duplicate envelope"
    );
    let sql_signal_rows: i64 = sqlx::query_scalar(
        "SELECT
            (SELECT COUNT(*) FROM signal_cancels)
          + (SELECT COUNT(*) FROM signal_captures)
          + (SELECT COUNT(*) FROM signal_wakeups)",
    )
    .fetch_one(pool_arc.as_ref())
    .await
    .expect("read SQL-backed Signal audit counts");
    assert_eq!(
        sql_signal_rows, 0,
        "external Cancel delivery and redelivery must not touch SQL-backed Signal state"
    );

    shutdown.cancel();
    let _ = translator.await;
}

/// Slice 02 acceptance: same idempotency_key + different payload returns
/// the Collision outcome — the second arrival is dropped, the matching
/// counter increments, no relay forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collision_envelope_drops_with_counter_and_no_forward() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let workflow_a = empty_workflow("nats-ingress-test-collision-a");
    let workflow_b = empty_workflow("nats-ingress-test-collision-b");
    let wid_a = Uuid::parse_str(&workflow_a.id).expect("workflow id");
    let wid_b = Uuid::parse_str(&workflow_b.id).expect("workflow id");
    insert_workflow(&pool, &workflow_a).await;
    insert_workflow(&pool, &workflow_b).await;

    let (relay_tx, mut relay_rx) = mpsc::channel::<sp::Signal>(32);
    let sender = Arc::new(CapturingRelaySender { tx: relay_tx });

    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    let idempotency_key = format!("ext-collision-{}", Uuid::new_v4());

    // First publish targeting workflow A.
    let env_a = json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": idempotency_key,
        "workflow_id": wid_a.to_string(),
    });
    js.publish(
        nats_ingress::SUBJECT,
        serde_json::to_vec(&env_a).unwrap().into(),
    )
    .await
    .expect("publish A")
    .await
    .expect("publish A ack");
    let _first = tokio::time::timeout(Duration::from_secs(10), relay_rx.recv())
        .await
        .expect("first arrival forwarded")
        .expect("channel still open");

    let collision_baseline = nats_ingress::signals_dropped_idempotency_collision();

    // Second publish: same key, different payload (targets workflow B).
    let env_b = json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": idempotency_key,
        "workflow_id": wid_b.to_string(),
    });
    js.publish(
        nats_ingress::SUBJECT,
        serde_json::to_vec(&env_b).unwrap().into(),
    )
    .await
    .expect("publish B")
    .await
    .expect("publish B ack");

    let mut collision_bumped = false;
    for _ in 0..50 {
        if nats_ingress::signals_dropped_idempotency_collision() > collision_baseline {
            collision_bumped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        collision_bumped,
        "signals_dropped_idempotency_collision counter must increment for collision"
    );

    let forwarded = tokio::time::timeout(Duration::from_millis(750), relay_rx.recv()).await;
    assert!(
        forwarded.is_err(),
        "collision must NOT produce a relay forward (got {:?})",
        forwarded
    );

    shutdown.cancel();
    let _ = translator.await;
}

/// A v=1 Wakeup envelope with no waits-on-signal subscribers parses,
/// runs the idempotency check, writes the audit row, and acks without
/// forwarding any wire signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_envelope_with_no_subscribers_acks_without_forwarding() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    // Wire the global relay channel — the translator's
    // `DefaultRelaySender` forwards through it. The capturing sender is
    // for the trigger/cancel paths and stays unused here.
    let (relay_global_tx, mut relay_global_rx) =
        mpsc::channel::<tickr_proto::ConductorRelayMessage>(32);
    tickr_conductor::relay::init_relay_tx(relay_global_tx).await;

    let (relay_tx, _relay_rx) = mpsc::channel::<sp::Signal>(32);
    let sender = Arc::new(CapturingRelaySender { tx: relay_tx });

    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    let instance_id = Uuid::new_v4();
    let idempotency_key = format!("ext-wakeup-{}", Uuid::new_v4());
    let envelope = json!({
        "version": 1,
        "variant": "Wakeup",
        "idempotency_key": idempotency_key,
        "name": format!("no-subscribers-{}", Uuid::new_v4()),
        "target": { "Instance": { "workflow_instance_id": instance_id.to_string(), "node_id": null } },
        "payload": { "order_id": "C-123" },
    });

    js.publish(
        nats_ingress::SUBJECT,
        serde_json::to_vec(&envelope).unwrap().into(),
    )
    .await
    .expect("publish wakeup")
    .await
    .expect("publish wakeup ack");

    // No subscribers → no Trigger forwarded → relay channel stays empty.
    let nothing_forwarded =
        tokio::time::timeout(Duration::from_secs(2), relay_global_rx.recv()).await;
    assert!(
        matches!(&nothing_forwarded, Err(_) | Ok(None)),
        "no subscribers must not surface any wire signal (got {:?})",
        nothing_forwarded
    );

    shutdown.cancel();
    let _ = translator.await;
}

/// A v=1 Wakeup envelope WITH a registered waits-on-signal subscriber
/// produces exactly one `Signal::Trigger { source: TriggerSource::Wakeup }`
/// on the relay outbound, addressed at the subscribing workflow_id.
/// Mirrors the HTTP route's behaviour over the NATS transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_envelope_with_subscriber_forwards_trigger() {
    use tickr_conductor::waits_on_signal_lifecycle::{
        apply_workflow_state, signal_subscription_index,
    };
    use tickr_proto::workflow as wf;

    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let (relay_global_tx, mut relay_global_rx) =
        mpsc::channel::<tickr_proto::ConductorRelayMessage>(32);
    tickr_conductor::relay::init_relay_tx(relay_global_tx).await;

    let (relay_tx, _relay_rx) = mpsc::channel::<sp::Signal>(32);
    let sender = Arc::new(CapturingRelaySender { tx: relay_tx });

    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    // Register a subscriber into the conductor's in-process index.
    let signal_name = format!("nats-paid-{}", Uuid::new_v4());
    let wf = wf::WorkflowDefinition {
        id: Uuid::new_v4().to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "nats-sub".to_string(),
        name: "nats-sub".to_string(),
        trigger: Some(wf::Trigger {
            kind: Some(wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
                signal_name: signal_name.clone(),
                predicate: None,
                captures: vec![],
            })),
        }),
        ..Default::default()
    };
    apply_workflow_state(&wf).expect("register subscriber");
    let workflow_id = Uuid::parse_str(&wf.id).expect("workflow id");

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    let idempotency_key = format!("ext-wakeup-fanout-{}", Uuid::new_v4());
    let envelope = json!({
        "version": 1,
        "variant": "Wakeup",
        "idempotency_key": idempotency_key,
        "name": signal_name,
        "target": null,
        "payload": { "x": 1 },
    });

    js.publish(
        nats_ingress::SUBJECT,
        serde_json::to_vec(&envelope).unwrap().into(),
    )
    .await
    .expect("publish wakeup")
    .await
    .expect("publish wakeup ack");

    let msg = tokio::time::timeout(Duration::from_secs(10), relay_global_rx.recv())
        .await
        .expect("NATS path forwards exactly one trigger")
        .expect("relay channel still open");
    assert_eq!(msg.entity_type, tickr_proto::EntityType::Signal as i32);
    let signal = sp::Signal::decode(&msg.payload[..]).expect("decode forwarded signal");
    match &signal.variant {
        Some(sp::signal::Variant::Trigger(t)) => {
            assert_eq!(t.workflow_id, workflow_id.to_string());
            match t.source.as_ref().and_then(|s| s.source.as_ref()) {
                Some(sp::trigger_source::Source::Wakeup(w)) => {
                    assert_eq!(
                        w.name, signal_name,
                        "wakeup name must thread to TriggerSource"
                    );
                }
                other => panic!("expected Wakeup source, got {:?}", other),
            }
        }
        other => panic!("expected Trigger variant, got {:?}", other),
    }

    signal_subscription_index().unregister(workflow_id);
    shutdown.cancel();
    let _ = translator.await;
}

/// Slice 03 acceptance: when the relay outbound buffer is saturated, the
/// translator NAKs the NATS message (so NATS holds + redelivers) and
/// increments `signals_relay_outbound_saturation`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_relay_buffer_results_in_nak_with_counter() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    // Use a workflow with no captures so the trigger path doesn't fail on
    // a Postgres write before reaching the relay-send step.
    let workflow = empty_workflow("nats-ingress-test-saturation");
    let workflow_id = Uuid::parse_str(&workflow.id).expect("workflow id");
    insert_workflow(&pool, &workflow).await;

    // Always-saturated sender so every relay forward returns `Saturated`.
    let sender = Arc::new(AlwaysSaturatedSender);

    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    let idempotency_key = format!("ext-saturate-{}", Uuid::new_v4());
    let envelope = json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": idempotency_key,
        "workflow_id": workflow_id.to_string(),
    });

    let saturation_baseline = nats_ingress::signals_relay_outbound_saturation();

    js.publish(
        nats_ingress::SUBJECT,
        serde_json::to_vec(&envelope).unwrap().into(),
    )
    .await
    .expect("publish trigger")
    .await
    .expect("publish ack");

    // The translator should detect saturation, NAK, and increment the
    // counter. Wait for the bump.
    let mut counter_bumped = false;
    for _ in 0..50 {
        if nats_ingress::signals_relay_outbound_saturation() > saturation_baseline {
            counter_bumped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        counter_bumped,
        "signals_relay_outbound_saturation counter must increment on saturation"
    );

    shutdown.cancel();
    let _ = translator.await;
}

/// Malformed envelope is rejected (log + ack) and the matching counter
/// increments. The translator does not forward anything to the relay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_envelope_increments_rejection_counter_and_does_not_forward() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let (relay_tx, mut relay_rx) = mpsc::channel::<sp::Signal>(32);
    let sender = Arc::new(CapturingRelaySender { tx: relay_tx });

    let shutdown = CancellationToken::new();
    let pool_arc = Arc::new(pool);
    let translator_nats = nats.clone();
    let translator_shutdown = shutdown.clone();
    let translator_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let translator = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            translator_nats,
            translator_repositories,
            sender,
            translator_shutdown,
        )
        .await
    });

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    let baseline = nats_ingress::rejected_malformed_json();
    js.publish(nats_ingress::SUBJECT, b"{not json".to_vec().into())
        .await
        .expect("publish raw bytes")
        .await
        .expect("publish ack");

    // The translator should process and reject without forwarding. We can't
    // race-free assert against the process-global counter (other tests may
    // be incrementing it concurrently), but we can wait until the counter
    // has bumped past the baseline value we captured before publishing —
    // the absolute counter is per-process but the delta from a captured
    // baseline is monotone, so any bump signals the rejection happened.
    let mut counter_bumped = false;
    for _ in 0..50 {
        if nats_ingress::rejected_malformed_json() > baseline {
            counter_bumped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        counter_bumped,
        "rejected_malformed_json counter must increment past baseline"
    );

    // No forward to this test's capturing sender should have happened —
    // the translator only sees messages on this test's NATS server, and
    // the only message published was malformed.
    let forwarded = tokio::time::timeout(Duration::from_millis(500), relay_rx.recv()).await;
    assert!(
        forwarded.is_err(),
        "no relay forward expected for malformed envelope (got {:?})",
        forwarded
    );

    shutdown.cancel();
    let _ = translator.await;
}

/// Slice 01 AC: the translator-loop survives a conductor restart cleanly
/// — pending messages on the subject are picked up on next startup. The
/// durable consumer (`tickr-conductor-external`) persists across translator
/// restarts; messages published while the first translator is down land in
/// the JetStream stream (WorkQueue retention) and the second translator's
/// pull subscription consumes them on next startup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn translator_restart_picks_up_pending_messages() {
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let workflow = empty_workflow("nats-restart");
    let workflow_id = Uuid::parse_str(&workflow.id).expect("workflow id");
    insert_workflow(&pool, &workflow).await;
    let pool_arc = Arc::new(pool);

    // Round 1: T1 starts, creates the durable consumer + stream, drains
    // anything pending, then shuts down. The consumer persists in NATS.
    let (t1_tx, mut t1_rx) = mpsc::channel::<sp::Signal>(32);
    let t1_sender = Arc::new(CapturingRelaySender { tx: t1_tx });
    let t1_shutdown = CancellationToken::new();
    let t1_nats = nats.clone();
    let t1_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let t1_shutdown_clone = t1_shutdown.clone();
    let t1 = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            t1_nats,
            t1_repositories,
            t1_sender,
            t1_shutdown_clone,
        )
        .await
    });

    let js = jetstream::new(nats.clone());
    let mut stream_ready = false;
    for _ in 0..50 {
        if js.get_stream(nats_ingress::STREAM_NAME).await.is_ok() {
            stream_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stream_ready);

    // Confirm T1 isn't seeing anything yet (clean state).
    let nothing_t1 = tokio::time::timeout(Duration::from_millis(500), t1_rx.recv()).await;
    assert!(nothing_t1.is_err(), "T1 should see no messages yet");

    // Tear T1 down. The durable consumer persists in NATS; the stream
    // retains anything published after this point until a consumer drains.
    t1_shutdown.cancel();
    let _ = t1.await;

    // Publish AFTER T1 is down. The message lands in the stream and waits
    // for the next consumer pull — proving the stream + consumer survive a
    // translator restart and pending work is held in flight.
    let idempotency_key = format!("ext-restart-{}", Uuid::new_v4());
    let envelope = json!({
        "version": 1,
        "variant": "Trigger",
        "idempotency_key": idempotency_key,
        "workflow_id": workflow_id.to_string(),
    });
    js.publish(
        nats_ingress::SUBJECT,
        serde_json::to_vec(&envelope).unwrap().into(),
    )
    .await
    .expect("publish trigger to dormant subject")
    .await
    .expect("publish trigger ack");

    // Round 2: T2 starts and attaches to the same durable consumer. The
    // pending message is delivered on the first pull.
    let (t2_tx, mut t2_rx) = mpsc::channel::<sp::Signal>(32);
    let t2_sender = Arc::new(CapturingRelaySender { tx: t2_tx });
    let t2_shutdown = CancellationToken::new();
    let t2_nats = nats.clone();
    let t2_repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool_arc.as_ref().clone(),
    ));
    let t2_shutdown_clone = t2_shutdown.clone();
    let t2 = tokio::spawn(async move {
        nats_ingress::run_translator_with_sender(
            t2_nats,
            t2_repositories,
            t2_sender,
            t2_shutdown_clone,
        )
        .await
    });

    let signal = tokio::time::timeout(Duration::from_secs(10), t2_rx.recv())
        .await
        .expect("T2 picks up the pending envelope within deadline")
        .expect("channel still open");
    assert_eq!(
        signal.idempotency_key.as_deref(),
        Some(idempotency_key.as_str())
    );
    match signal.variant {
        Some(sp::signal::Variant::Trigger(t)) => {
            assert_eq!(t.workflow_id, workflow_id.to_string());
        }
        other => panic!("expected Trigger variant on restart, got {:?}", other),
    }

    t2_shutdown.cancel();
    let _ = t2.await;
}
