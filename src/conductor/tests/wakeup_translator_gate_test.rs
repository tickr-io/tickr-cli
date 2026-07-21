//! Wakeup-translator gate behaviour, driven directly against
//! `process_wakeup` with a recording relay sender — no HTTP wrapper
//! between the test and the pipeline function. Covers the two
//! hyperedge-gate arms (predicate satisfied → `GateOutcome` emitted +
//! gate dropped; predicate failed → gate retained + nothing emitted)
//! and the `waits-on-signal` subscriber predicate filter (only the
//! matching subscriber fires).
//!
//! The gate arms use an isolated `GateIndex::new()` per test (as
//! `process_wakeup` documents for tests); the subscriber arm uses the
//! process-wide subscription index and unregisters on the way out.
//!
//! Requires Docker (testcontainers Postgres + NATS) for the audit /
//! capture writes `process_wakeup` performs. Skipped automatically
//! when unavailable.

#![cfg(not(madsim))]

mod common;

use std::time::Duration;

use async_nats::Client as NatsClient;
use serde_json::json;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::gate_index::GateIndex;
use tickr_conductor::waits_on_signal_lifecycle::{apply_workflow_state, signal_subscription_index};
use tickr_conductor::wakeup_translator::{
    process_wakeup, WakeupOutcome, WakeupRelaySender, WakeupRequest,
};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_proto::signal as sp;
use tickr_proto::workflow as wf;
use tokio::sync::Mutex;
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    NatsClient,
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
fn repositories(pool: &sqlx::PgPool) -> WriterRepositoryBundle {
    WriterRepositoryBundle::from_postgres_pool(pool.clone())
}

/// In-process relay sender that buffers what the translator forwards,
/// so the test can assert on emitted `Signal`s and `GateOutcome`s
/// without standing up the relay client.
#[derive(Default)]
struct RecordingSender {
    signals: Mutex<Vec<sp::Signal>>,
    gate_outcomes: Mutex<Vec<sp::GateOutcome>>,
}

#[async_trait::async_trait]
impl WakeupRelaySender for RecordingSender {
    async fn send(&self, signal: &sp::Signal) -> anyhow::Result<()> {
        self.signals.lock().await.push(signal.clone());
        Ok(())
    }
    async fn send_gate_outcome(&self, outcome: &sp::GateOutcome) -> anyhow::Result<()> {
        self.gate_outcomes.lock().await.push(outcome.clone());
        Ok(())
    }
}

fn cap(name: &str, jsonpath: &str) -> wf::CaptureDeclaration {
    wf::CaptureDeclaration {
        name: name.to_string(),
        from: Some(wf::CaptureSource {
            source: Some(wf::capture_source::Source::Trigger(
                wf::capture_source::Trigger {
                    jsonpath: jsonpath.to_string(),
                },
            )),
        }),
    }
}

fn register_subscriber(
    name: &str,
    signal_name: &str,
    predicate: Option<&str>,
    captures: Vec<wf::CaptureDeclaration>,
) -> wf::WorkflowDefinition {
    let definition = wf::WorkflowDefinition {
        id: Uuid::new_v4().to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: name.to_string(),
        name: name.to_string(),
        trigger: Some(wf::Trigger {
            kind: Some(wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
                signal_name: signal_name.to_string(),
                predicate: predicate.map(str::to_string),
                captures,
            })),
        }),
        ..Default::default()
    };
    apply_workflow_state(&definition).expect("apply state into singleton");
    definition
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_with_matching_dispatched_gate_emits_gate_outcome() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };

    let signal_name = format!("payment-cleared-{}", Uuid::new_v4());
    let wi_id = Uuid::new_v4();
    let edge_id = Uuid::new_v4();
    let gate_index = GateIndex::new();
    gate_index
        .register(
            wi_id,
            edge_id,
            &signal_name,
            Some("$[?@.amount > 100]"),
            vec![cap("receipt_url", "$.receipt")],
        )
        .expect("register dispatched gate");

    let sender = RecordingSender::default();
    let outcome = process_wakeup(
        &repositories(&pool),
        &nats,
        &sender,
        &gate_index,
        WakeupRequest {
            name: signal_name.clone(),
            payload: Some(json!({"amount": 250, "receipt": "https://r/123"})),
            idempotency_key: None,
        },
    )
    .await
    .expect("process_wakeup");

    match outcome {
        WakeupOutcome::Fresh {
            matched_workflows,
            gates_matched,
            ..
        } => {
            assert_eq!(gates_matched, 1, "satisfied gate must count once");
            assert_eq!(matched_workflows, 0, "no waits-on-signal subscribers");
        }
        other => panic!("expected Fresh, got {:?}", outcome_kind(&other)),
    }

    // One GateOutcome was emitted, carrying the gate's identity.
    let outcomes = sender.gate_outcomes.lock().await;
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].workflow_instance_id, wi_id.to_string());
    assert_eq!(outcomes[0].edge_id, edge_id.to_string());
    assert!(Uuid::parse_str(&outcomes[0].signal_id).is_ok_and(|u| !u.is_nil()));
    assert!(
        sender.signals.lock().await.is_empty(),
        "no subscriber Trigger expected"
    );

    // Gate was dropped on satisfaction so a follow-up wakeup doesn't
    // re-fire against the same edge.
    assert!(gate_index.lookup_by_signal_name(&signal_name).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_with_failed_gate_predicate_keeps_entry_and_emits_nothing() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };

    let signal_name = format!("payment-cleared-{}", Uuid::new_v4());
    let wi_id = Uuid::new_v4();
    let edge_id = Uuid::new_v4();
    let gate_index = GateIndex::new();
    gate_index
        .register(
            wi_id,
            edge_id,
            &signal_name,
            Some("$[?@.amount > 100]"),
            vec![],
        )
        .expect("register dispatched gate");

    let sender = RecordingSender::default();
    let outcome = process_wakeup(
        &repositories(&pool),
        &nats,
        &sender,
        &gate_index,
        WakeupRequest {
            name: signal_name.clone(),
            payload: Some(json!({"amount": 5})),
            idempotency_key: None,
        },
    )
    .await
    .expect("process_wakeup");

    match outcome {
        WakeupOutcome::Fresh { gates_matched, .. } => {
            assert_eq!(gates_matched, 0, "unsatisfied gate must not count");
        }
        other => panic!("expected Fresh, got {:?}", outcome_kind(&other)),
    }

    assert!(
        sender.gate_outcomes.lock().await.is_empty(),
        "no GateOutcome must fire when predicate is false"
    );
    // Entry stays so a later wakeup with a passing payload still
    // satisfies this gate.
    assert_eq!(
        gate_index.lookup_by_signal_name(&signal_name).len(),
        1,
        "unsatisfied gate must remain dispatched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_with_predicate_filter_only_fires_matching_subscriber() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };

    let signal_name = format!("order-paid-{}", Uuid::new_v4());
    // Subscriber-a fires only when amount > 100; subscriber-b has no
    // predicate and fires unconditionally.
    let a = register_subscriber("filter-a", &signal_name, Some("$[?@.amount > 100]"), vec![]);
    let b = register_subscriber("filter-b", &signal_name, None, vec![]);

    // Empty gate index — this scenario exercises the subscriber arm only.
    let gate_index = GateIndex::new();
    let sender = RecordingSender::default();
    let outcome = process_wakeup(
        &repositories(&pool),
        &nats,
        &sender,
        &gate_index,
        WakeupRequest {
            name: signal_name.clone(),
            payload: Some(json!({"amount": 50})),
            idempotency_key: None,
        },
    )
    .await
    .expect("process_wakeup");

    match outcome {
        WakeupOutcome::Fresh {
            matched_workflows, ..
        } => {
            assert_eq!(
                matched_workflows, 1,
                "only the no-predicate subscriber should fire"
            );
        }
        other => panic!("expected Fresh, got {:?}", outcome_kind(&other)),
    }

    let signals = sender.signals.lock().await;
    assert_eq!(
        signals.len(),
        1,
        "predicate-filtered subscriber must not fire"
    );
    match &signals[0].variant {
        Some(sp::signal::Variant::Trigger(t)) => {
            assert_eq!(
                t.workflow_id, b.id,
                "the no-predicate subscriber must be the one that fired"
            );
        }
        other => panic!("expected Trigger variant, got {:?}", other),
    }
    drop(signals);

    signal_subscription_index().unregister(Uuid::parse_str(&a.id).expect("workflow id"));
    signal_subscription_index().unregister(Uuid::parse_str(&b.id).expect("workflow id"));
}

/// Stable label for the non-`Fresh` outcome variants, for panic
/// messages (`WakeupOutcome` is not `Debug`).
fn outcome_kind(outcome: &WakeupOutcome) -> &'static str {
    match outcome {
        WakeupOutcome::Fresh { .. } => "Fresh",
        WakeupOutcome::Deduplicated { .. } => "Deduplicated",
        WakeupOutcome::Conflict { .. } => "Conflict",
    }
}
