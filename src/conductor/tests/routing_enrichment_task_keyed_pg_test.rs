//! Real-NATS + real-Postgres tests for the task-keyed declared-spec lookup
//! in the conductor's routing-variable enrichment, now served by the unified
//! `task_id`-keyed `task_specs` store.
//!
//! Workflow identity keeps one `workflows.id` across every registration: each
//! re-registration inserts a new row at a new version, and each build mints
//! fresh task ids. Enrichment keys its lookup on the completing event's
//! **definition task id**, which — ids being minted fresh per build (or per
//! patch `AddNode`) — holds exactly one `task_specs` row. Registration and
//! patch ingress both write the store, so a patched-in task resolves exactly
//! like a registered one: one lookup, one fail-closed rule, no
//! registered-vs-patched branch.
//!
//! Covered here:
//!   1. **twice-registered loop terminates** — two builds' task ids each
//!      resolve their own spec row; the running instance's producer gets
//!      `loop_control=done` stamped, the loop-continue gate rejects, and the
//!      post-loop (`exitTo`) node runs to completion (End grounds `Success`).
//!   2. **no declared specs forwards unchanged** — a completing task with no
//!      `mkRoutingVar` declarations rides through bare (no failure surface
//!      for ordinary tasks).
//!   3. **zero-row lookup fails closed** — a completing task id in no spec
//!      set is an integrity fault: the event is NOT forwarded un-enriched
//!      (it stays in the durable queue for redelivery).
//!   4. **same build re-registered resolves from one store row** — the shape
//!      that used to be the multi-row integrity fault dedups to a single
//!      `task_specs` row (PK + DO NOTHING) and resolves normally.
//!   5. **split-stage drop escalates** — a declared routing variable present
//!      in the emitted bag but dropped by the split (declared/emitted type
//!      mismatch) escalates the completion to the conductor-minted `Failed`
//!      verdict instead of forwarding un-enriched.
//!   6. **continue-iteration keeps turning** — bare absence of the
//!      default-bearing `loop_control` is a legitimate continue, forwarded
//!      as `Completed` with nothing stamped.
//!   7. **patched-in task resolves from the unified store** — a Patch whose
//!      `AddNode` carries a full task spec (build-at-patch) writes the store
//!      at ingress; the patched task's completion enriches exactly like a
//!      registered one, and a gate over the patched variable evaluates
//!      instead of parking.
//!
//! Requires Docker (testcontainers). Skipped automatically when NATS or
//! Postgres containers are unavailable — the startup failure is the skip
//! marker, matching the other conductor integration tests.

#![cfg(not(madsim))]

mod common;

use async_nats::jetstream;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::patch_pipeline::PatchProvenance;
use tickr_conductor::proto::{ConductorRelayMessage, EntityType};
use tickr_conductor::relay::{drain_task_events, task_event_consumer};
use tickr_proto::coord::{TASK_EVENT_CONSUMER, TASK_EVENT_STREAM, TASK_EVENT_SUBJECT};
use tickr_proto::patch as pp;
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

/// One build of the loop workflow: a self-loop producer declaring
/// `loop_control` plus a post-loop (`exitTo`) task behind a
/// `loop_control == done` exit gate. The workflow id is derived from the name
/// (UUIDv5) so every build shares it, while each build mints fresh task ids
/// per build — exactly the multi-registration shape that broke the
/// workflow-id-keyed lookup.
fn build_loop_workflow(name: &str, version: i64) -> (wf::WorkflowDefinition, Uuid, Uuid) {
    let workflow_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes());

    let looper_id = Uuid::new_v4();
    let looper = wf::TaskDefinition {
        id: looper_id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: "looper".to_string(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: String::new(),
        nix_args: Vec::new(),
        outputs: Vec::new(),
        inputs: Vec::new(),
        secrets: Vec::new(),
        max_attempts: 3,
        input_sources: None,
        timeout_secs: None,
        emits: Vec::new(),
        routing_vars: vec![wf::RoutingVarDecl {
            name: "loop_control".to_string(),
            var_type: Some("string".to_string()),
        }],
        loop_participant: true,
    };

    let exit_id = Uuid::new_v4();
    let exit_task = wf::TaskDefinition {
        id: exit_id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: "post-loop".to_string(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: String::new(),
        nix_args: Vec::new(),
        outputs: Vec::new(),
        inputs: Vec::new(),
        secrets: Vec::new(),
        max_attempts: 3,
        input_sources: None,
        timeout_secs: None,
        emits: Vec::new(),
        routing_vars: Vec::new(),
        loop_participant: false,
    };

    let start = Uuid::new_v4().to_string();
    let end = Uuid::new_v4().to_string();
    let nodes = vec![
        wf::GraphNode {
            id: start.clone(),
            node_type: wf::NodeType::Start as i32,
        },
        wf::GraphNode {
            id: looper_id.to_string(),
            node_type: wf::NodeType::Task as i32,
        },
        wf::GraphNode {
            id: exit_id.to_string(),
            node_type: wf::NodeType::Task as i32,
        },
        wf::GraphNode {
            id: end.clone(),
            node_type: wf::NodeType::End as i32,
        },
    ];
    let edges = vec![
        wf::Edge {
            id: Uuid::new_v4().to_string(),
            sources: vec![start.clone()],
            targets: vec![looper_id.to_string()],
            kind: wf::EdgeKind::Control as i32,
            gates: Vec::new(),
        },
        // Loop self-edge, gated on the continue verdict.
        wf::Edge {
            id: Uuid::new_v4().to_string(),
            sources: vec![looper_id.to_string()],
            targets: vec![looper_id.to_string()],
            kind: wf::EdgeKind::Loop as i32,
            gates: vec![predicate_gate("loop_control", "continue")],
        },
        // Exit edge to the post-loop node, gated on the terminal verdict.
        wf::Edge {
            id: Uuid::new_v4().to_string(),
            sources: vec![looper_id.to_string()],
            targets: vec![exit_id.to_string()],
            kind: wf::EdgeKind::Data as i32,
            gates: vec![predicate_gate("loop_control", "done")],
        },
        wf::Edge {
            id: Uuid::new_v4().to_string(),
            sources: vec![exit_id.to_string()],
            targets: vec![end.clone()],
            kind: wf::EdgeKind::Control as i32,
            gates: Vec::new(),
        },
    ];

    let definition = wf::WorkflowDefinition {
        id: workflow_id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: name.to_string(),
        version,
        tasks: vec![looper, exit_task],
        task_graph: Some(wf::TaskGraph {
            nodes,
            edges,
            start,
            end,
        }),
        trigger: None,
        status: wf::WorkflowStatus::Inactive as i32,
        captures: Vec::new(),
        timeout_secs: None,
        tags: HashMap::new(),
    };
    (definition, looper_id, exit_id)
}

fn predicate_gate(routing_var: &str, value: &str) -> wf::Gate {
    wf::Gate {
        kind: Some(wf::gate::Kind::PredicateHolds(wf::gate::PredicateHolds {
            routing_var: routing_var.to_string(),
            op: wf::ComparisonOp::Eq as i32,
            value: Some(wf::RoutingValue {
                value: Some(wf::routing_value::Value::StringValue(value.to_string())),
            }),
            timeout: None,
        })),
    }
}

/// The workflow definition's id as a `Uuid` (the wire form carries it as a
/// string).
fn wf_id(def: &wf::WorkflowDefinition) -> Uuid {
    Uuid::parse_str(&def.id).expect("workflow id")
}

async fn insert_workflow(pool: &sqlx::PgPool, workflow: &wf::WorkflowDefinition) {
    let definition = serde_json::to_value(workflow).expect("serialize workflow definition");
    sqlx::query(
        "INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
         VALUES ($1, $2, 'default', 'wf', $3, 'Ready', $4, 'testcos', $5, '')",
    )
    .bind(Uuid::parse_str(&workflow.id).expect("workflow id"))
    .bind(workflow.version)
    .bind(&workflow.name)
    .bind(format!("hash-v{}", workflow.version))
    .bind(definition)
    .execute(pool)
    .await
    .expect("insert workflow row");

    // Mirror registration's unified-store write: one `task_specs` row per
    // task, keyed by the minted task id — the row enrichment reads.
    for task in &workflow.tasks {
        let routing_vars =
            serde_json::to_value(&task.routing_vars).expect("serialize routing vars");
        sqlx::query(
            "INSERT INTO task_specs (task_id, routing_vars)
             VALUES ($1, $2) ON CONFLICT (task_id) DO NOTHING",
        )
        .bind(Uuid::parse_str(&task.id).expect("task id"))
        .bind(&routing_vars)
        .execute(pool)
        .await
        .expect("insert task_specs row");
    }
}

/// Write an emitted task output into the `ctx-default` KV bucket under
/// `<run_id>/<name>`, shaped as the v=2 ctx envelope the enrichment read path
/// expects (producer is this task instance).
async fn seed_ctx_output(
    nats: &async_nats::Client,
    run_id: Uuid,
    task_instance_id: Uuid,
    name: &str,
    value: &str,
) {
    seed_ctx_output_typed(
        nats,
        run_id,
        task_instance_id,
        name,
        "string",
        serde_json::json!(value),
    )
    .await;
}

/// `seed_ctx_output` with an explicit type tag and JSON value, for emissions
/// whose shape must disagree with the declared routing-variable type.
async fn seed_ctx_output_typed(
    nats: &async_nats::Client,
    run_id: Uuid,
    task_instance_id: Uuid,
    name: &str,
    type_tag: &str,
    value: serde_json::Value,
) {
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
        "type": type_tag,
        "value": value,
        "secret": false,
        "producer": {
            "kind": "task",
            "task_id": task_instance_id.to_string(),
            "task_name": "producer",
        },
        "created_at": "2026-01-01T00:00:00Z",
        "sha256": "0",
    });
    let key = format!("{}/{}", run_id, name);
    kv.put(key, serde_json::to_vec(&envelope).unwrap().into())
        .await
        .expect("put ctx output");
}

/// Pre-create the durable task-event consumer with a short `ack_wait` so
/// NAK/redelivery paths are fast and deterministic in tests.
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

async fn publish_completed_event(
    nats: &async_nats::Client,
    task_id: Uuid,
    workflow_id: Uuid,
    run_id: Uuid,
    task_instance_id: Uuid,
) {
    let event = tc::TaskEvent {
        task_instance_id: task_instance_id.to_string(),
        task_id: task_id.to_string(),
        workflow_instance_id: run_id.to_string(),
        workflow_id: workflow_id.to_string(),
        executor_id: Some(Uuid::new_v4().to_string()),
        kind: Some(tc::task_event::Kind::Completed(tc::task_event::Completed {
            routing_variables: HashMap::new(),
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
}

/// Spawn `drain_task_events` on a fresh consumer with an open relay channel.
fn spawn_drain(
    nats: async_nats::Client,
    pool: Arc<sqlx::PgPool>,
) -> (
    mpsc::Receiver<ConductorRelayMessage>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel::<ConductorRelayMessage>(16);
    let token = CancellationToken::new();
    let drain_token = token.clone();
    let handle = tokio::spawn(async move {
        let consumer = task_event_consumer(&nats).await.expect("consumer");
        drain_task_events(consumer, tx, pool, nats.clone(), drain_token).await;
    });
    (rx, token, handle)
}

fn completed_routing_vars(msg: &ConductorRelayMessage) -> HashMap<String, wf::RoutingValue> {
    assert_eq!(msg.entity_type, EntityType::TaskEvent as i32);
    let forwarded = tc::TaskEvent::decode(&msg.payload[..]).expect("decode forwarded event");
    match forwarded.kind {
        Some(tc::task_event::Kind::Completed(completed)) => completed.routing_variables,
        other => panic!("expected Completed, got {:?}", other),
    }
}

fn routing_string(value: Option<&wf::RoutingValue>) -> Option<&str> {
    match value.and_then(|value| value.value.as_ref()) {
        Some(wf::routing_value::Value::StringValue(value)) => Some(value),
        _ => None,
    }
}

/// The headline regression: a loop workflow registered twice (two definition
/// rows sharing one workflow id, distinct task ids per build) still resolves
/// the running instance's declared routing variables, so the producer's
/// `loop_control=done` is stamped, the loop reaps, and the post-loop
/// (`exitTo`) node runs to completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_registered_twice_terminates_via_task_keyed_lookup() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    // Two registrations of the same workflow: one `workflows.id`, two rows,
    // fresh task ids per build.
    let (wf_v1, looper_v1, _) = build_loop_workflow("loop-rereg", 1);
    let (wf_v2, looper_v2, exit_v2) = build_loop_workflow("loop-rereg", 2);
    assert_eq!(wf_v1.id, wf_v2.id);
    assert_ne!(looper_v1, looper_v2);
    insert_workflow(&pool, &wf_v1).await;
    insert_workflow(&pool, &wf_v2).await;

    // Running instances: one built from each registration. Each producer
    // emits `loop_control=done` into the ctx KV scope.
    let run_v2 = Uuid::new_v4();
    let looper_ti_v2 = Uuid::new_v4();
    seed_ctx_output(&nats, run_v2, looper_ti_v2, "loop_control", "done").await;
    let run_v1 = Uuid::new_v4();
    let looper_ti_v1 = Uuid::new_v4();
    seed_ctx_output(&nats, run_v1, looper_ti_v1, "loop_control", "done").await;

    precreate_fast_consumer(&nats).await;
    // Three completions through the real drain: the v2 producer, the v1
    // producer (the lookup must resolve per-build regardless of which row a
    // version-blind read would have picked), and the v2 post-loop task
    // (declares nothing → forwards unchanged).
    publish_completed_event(&nats, looper_v2, wf_id(&wf_v2), run_v2, looper_ti_v2).await;
    publish_completed_event(&nats, looper_v1, wf_id(&wf_v1), run_v1, looper_ti_v1).await;
    publish_completed_event(&nats, exit_v2, wf_id(&wf_v2), run_v2, Uuid::new_v4()).await;

    let (mut rx, token, handle) = spawn_drain(nats.clone(), Arc::clone(&pool));

    async fn recv(rx: &mut mpsc::Receiver<ConductorRelayMessage>) -> ConductorRelayMessage {
        tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("forward did not arrive in time")
            .expect("relay channel closed")
    }
    let vars_v2 = completed_routing_vars(&recv(&mut rx).await);
    assert_eq!(
        routing_string(vars_v2.get("loop_control")),
        Some("done"),
        "the running instance's producer must get its declared routing variable stamped \
         even though the workflow has two definition rows"
    );
    let vars_v1 = completed_routing_vars(&recv(&mut rx).await);
    assert_eq!(
        routing_string(vars_v1.get("loop_control")),
        Some("done"),
        "a completion from the other build must resolve its own row too"
    );
    let vars_exit = completed_routing_vars(&recv(&mut rx).await);
    assert!(
        vars_exit.is_empty(),
        "a task with no declared routing variables forwards unchanged"
    );

    token.cancel();
    let _ = handle.await;

    // Server-side loop termination given these enriched routing variables is verified by the server-side gate-lifecycle and sim suites; the conductor's responsibility — stamping the declared routing variables via the task-keyed lookup — is asserted above.
}

/// Zero-row lookup: the completing task's id is in no registered definition.
/// For a legitimately-built instance that is an integrity fault — the event
/// must NOT forward un-enriched (that silently drops the routing variable the
/// task emitted and re-creates the park-forever hang); it stays in the
/// durable queue instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_row_lookup_fails_closed_instead_of_forwarding_unenriched() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    // A registered workflow exists, but the completing task id is from no
    // definition at all.
    let (wf, _, _) = build_loop_workflow("loop-zero-row", 1);
    insert_workflow(&pool, &wf).await;

    let orphan_task_id = Uuid::new_v4();
    let run_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    // The task did emit a routing variable — forwarding un-enriched would
    // silently drop it.
    seed_ctx_output(&nats, run_id, task_instance_id, "loop_control", "done").await;

    precreate_fast_consumer(&nats).await;
    publish_completed_event(&nats, orphan_task_id, wf_id(&wf), run_id, task_instance_id).await;

    let (mut rx, token, handle) = spawn_drain(nats.clone(), Arc::clone(&pool));

    // Not forwarded within the observation window...
    let forwarded = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(
        forwarded.is_err(),
        "a zero-row lookup must fail closed, not forward un-enriched: got {:?}",
        forwarded
    );
    // ...and not acked away: the completion stays in the durable queue.
    assert!(
        stream_message_count(&nats).await >= 1,
        "the fail-closed event must remain in the durable queue for redelivery"
    );

    token.cancel();
    let _ = handle.await;
}

/// The same build inserted at two versions — the shape that used to be the
/// multi-row integrity fault against the `workflows.definition` read —
/// dedups to a single `task_specs` row (primary key + `DO NOTHING`), so the
/// lookup resolves normally: the store's uniqueness is structural, not an
/// invariant a query must re-check. The completion forwards enriched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_build_reregistered_resolves_from_single_store_row() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    // Insert the SAME build (same task ids) at two versions.
    let (mut wf, looper_id, _) = build_loop_workflow("loop-multi-row", 1);
    insert_workflow(&pool, &wf).await;
    wf.version = 2;
    insert_workflow(&pool, &wf).await;

    // Exactly one spec row survives the double write.
    let (spec_rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM task_specs WHERE task_id = $1")
        .bind(looper_id)
        .fetch_one(pool.as_ref())
        .await
        .expect("count spec rows");
    assert_eq!(spec_rows, 1, "the unified store dedups on task_id");

    let run_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    seed_ctx_output(&nats, run_id, task_instance_id, "loop_control", "done").await;

    precreate_fast_consumer(&nats).await;
    publish_completed_event(&nats, looper_id, wf_id(&wf), run_id, task_instance_id).await;

    let (mut rx, token, handle) = spawn_drain(nats.clone(), Arc::clone(&pool));

    let msg = tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("forward did not arrive in time")
        .expect("relay channel closed");
    let vars = completed_routing_vars(&msg);
    assert_eq!(
        routing_string(vars.get("loop_control")),
        Some("done"),
        "the single unified-store row resolves the declared variable"
    );

    token.cancel();
    let _ = handle.await;
}

/// Poll until the durable task-event queue drains to empty (the drain acked
/// the message after forwarding). `rx.recv()` resolves when the forward hits
/// the relay channel, which races the ack — so the count is awaited, not
/// asserted immediately.
async fn await_stream_drained(nats: &async_nats::Client) {
    for _ in 0..50 {
        if stream_message_count(nats).await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "task-event queue did not drain: expected the forwarded event to be acked, \
         {} message(s) remain",
        stream_message_count(nats).await
    );
}

/// Split-stage drop: the producer *did* emit its declared routing variable,
/// but a declared/emitted type mismatch makes the all-or-nothing split drop
/// the whole routing-variable map. Forwarding the completion un-enriched
/// (fail-open) would re-strand the loop and read as a slow run; instead the
/// completion is escalated to the conductor-minted failure verdict — kind
/// `Failed` on the same relay channel the liveness verdict rides — forwarded
/// and acked, so the server grounds a terminal task failure that cascades
/// through the existing loop machinery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emitted_but_dropped_routing_variable_escalates_to_task_failure() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    let (wf, looper_id, _) = build_loop_workflow("loop-split-drop", 1);
    insert_workflow(&pool, &wf).await;

    let run_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    // `loop_control` is declared `string`; emit an int so the split drops the
    // emitted value (present in bag, absent from stamped result).
    seed_ctx_output_typed(
        &nats,
        run_id,
        task_instance_id,
        "loop_control",
        "int",
        serde_json::json!(1),
    )
    .await;

    precreate_fast_consumer(&nats).await;
    publish_completed_event(&nats, looper_id, wf_id(&wf), run_id, task_instance_id).await;

    let (mut rx, token, handle) = spawn_drain(nats.clone(), Arc::clone(&pool));

    let msg = tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("escalated failure did not arrive in time")
        .expect("relay channel closed");
    assert_eq!(msg.entity_type, EntityType::TaskEvent as i32);
    let forwarded = tc::TaskEvent::decode(&msg.payload[..]).expect("decode forwarded event");
    assert_eq!(forwarded.task_instance_id, task_instance_id.to_string());
    assert!(
        matches!(forwarded.kind, Some(tc::task_event::Kind::Failed(_))),
        "an emitted-but-dropped declared routing variable must escalate the completion \
         to a terminal task failure, not forward un-enriched; got {:?}",
        forwarded.kind
    );
    // Escalation is terminal, not a retry loop: the completion was consumed
    // (acked), not left in the durable queue for redelivery.
    await_stream_drained(&nats).await;

    token.cancel();
    let _ = handle.await;
}

/// Recording relay sender for the patch pipeline (the trait injection point,
/// no gRPC), used by the patched-task enrichment test below.
struct RecordingPatchSender {
    sent: tokio::sync::Mutex<Vec<pp::PatchEnvelope>>,
}

#[async_trait::async_trait]
impl tickr_conductor::patch_pipeline::PatchRelaySender for RecordingPatchSender {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> anyhow::Result<()> {
        self.sent.lock().await.push(envelope.clone());
        Ok(())
    }
}

/// AC: a patched-in task that declares a routing variable has it stamped
/// from the unified `task_id`-keyed store like a registered task — one
/// lookup, one fail-closed rule, no registered-vs-patched branch — and a
/// gate over the patched variable evaluates instead of parking.
///
/// End-to-end across the seam: the Patch document's `AddNode` carries a full
/// task body; ingress mints the node id, writes the spec into `task_specs`,
/// and opens the row at `Building`; the build finalizer ships the apply; the
/// instance applies the ops (installing the task spec); the patched task's
/// completion drains through the REAL enrichment path and comes out stamped;
/// feeding it into the graph machinery satisfies the gate and grounds `End`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patched_in_task_resolves_from_unified_store_and_gate_evaluates() {
    use tickr_conductor::patch_pipeline::{
        finalize_patch_after_build, parse_patch_document_json, patch_key, process_patch,
        PatchBuildFinalize, PatchIngress,
    };

    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    // Base workflow: start → r → end, r declaring nothing.
    let workflow_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"patched-enrich");
    let r = Uuid::new_v4();
    let start = Uuid::new_v4();
    let end = Uuid::new_v4();
    let r_end_edge_id = Uuid::new_v4().to_string();
    let wf = wf::WorkflowDefinition {
        id: workflow_id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: "patched-enrich".to_string(),
        version: 1,
        tasks: vec![wf::TaskDefinition {
            id: r.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "r".to_string(),
            task_type: wf::TaskType::Regular as i32,
            nix_expression_path: String::new(),
            nix_args: Vec::new(),
            outputs: Vec::new(),
            inputs: Vec::new(),
            secrets: Vec::new(),
            max_attempts: 3,
            input_sources: None,
            timeout_secs: None,
            emits: Vec::new(),
            routing_vars: Vec::new(),
            loop_participant: false,
        }],
        task_graph: Some(wf::TaskGraph {
            nodes: vec![
                wf::GraphNode {
                    id: start.to_string(),
                    node_type: wf::NodeType::Start as i32,
                },
                wf::GraphNode {
                    id: r.to_string(),
                    node_type: wf::NodeType::Task as i32,
                },
                wf::GraphNode {
                    id: end.to_string(),
                    node_type: wf::NodeType::End as i32,
                },
            ],
            edges: vec![
                wf::Edge {
                    id: Uuid::new_v4().to_string(),
                    sources: vec![start.to_string()],
                    targets: vec![r.to_string()],
                    kind: wf::EdgeKind::Control as i32,
                    gates: Vec::new(),
                },
                wf::Edge {
                    id: r_end_edge_id.clone(),
                    sources: vec![r.to_string()],
                    targets: vec![end.to_string()],
                    kind: wf::EdgeKind::Control as i32,
                    gates: Vec::new(),
                },
            ],
            start: start.to_string(),
            end: end.to_string(),
        }),
        trigger: None,
        status: wf::WorkflowStatus::Inactive as i32,
        captures: Vec::new(),
        timeout_secs: None,
        tags: HashMap::new(),
    };
    insert_workflow(&pool, &wf).await;

    // The Patch document: splice task `enrich` (declares routing var
    // `verdict`) between r and end, with the enrich → end edge gated on
    // `verdict == go`. The document's AddNode id is a placeholder — lowering
    // mints the real node id and rewrites the edge references.
    let placeholder = Uuid::new_v4();
    let r_end_edge = wf
        .task_graph
        .as_ref()
        .expect("graph")
        .edges
        .iter()
        .find(|e| e.sources.contains(&r.to_string()) && e.targets.contains(&end.to_string()))
        .map(|e| e.id.clone())
        .expect("r → end edge");
    let document = serde_json::json!({
        "ops": [
            { "AddNode": { "node_id": placeholder, "task": {
                "name": "enrich", "command": "shell", "args": [], "outputs": [],
                "nix_expression_path": "/patch/enrich.nix",
                "routing_vars": [ { "name": "verdict", "kind": "routing-var", "type": "string" } ]
            } } },
            { "AddEdge": { "sources": [r], "targets": [placeholder],
                           "kind": "Control", "gates": [] } },
            { "AddEdge": { "sources": [placeholder], "targets": [end],
                           "kind": "Data",
                           "gates": [ { "kind": "predicate-gate",
                               "routing_var": "verdict", "op": "Eq",
                               "value": "go", "timeout": null } ] } },
            { "RemoveEdge": { "edge_id": r_end_edge } }
        ],
        "reason": "runtime enrich splice"
    });
    let parsed = parse_patch_document_json(&document.to_string()).expect("parse document");
    let minted = parsed
        .new_tasks()
        .first()
        .map(|(id, _)| *id)
        .expect("one new task");
    assert_ne!(minted, placeholder, "lowering mints a fresh node id");

    // Ingress: row at Building, spec row written, one build job handed back.
    let sender = RecordingPatchSender {
        sent: tokio::sync::Mutex::new(Vec::new()),
    };
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let ingress = process_patch(
        &pool,
        &sender,
        wi,
        patch_id,
        parsed.clone(),
        PatchProvenance::External,
    )
    .await
    .expect("ingress");
    let key = patch_key(wi, patch_id);
    let build_jobs = match ingress {
        PatchIngress::Accepted { build_jobs, .. } => build_jobs,
        other => panic!("expected Accepted, got {:?}", other),
    };
    assert_eq!(build_jobs.len(), 1);
    assert_eq!(build_jobs[0].task_id, minted);

    // Build success → the finalizer flips Building → Submitted and ships the
    // single validate+apply envelope carrying the lowered ops.
    tickr_conductor::patch_pipeline::record_patch_task_outcome(
        &pool,
        key,
        minted,
        &tickr_conductor::build_pipeline::BuildOutcome::Success,
    )
    .await
    .expect("record outcome");
    assert_eq!(
        finalize_patch_after_build(
            &pool,
            &sender,
            key,
            &tickr_conductor::build_pipeline::BuildOutcome::Success
        )
        .await
        .expect("finalize"),
        PatchBuildFinalize::FlippedToSubmitted
    );
    let sent = sender.sent.lock().await.clone();
    assert_eq!(
        sent.len(),
        1,
        "nothing at ingress; one validate+apply envelope at finalize"
    );

    // The patched task completes, emitting its declared `verdict` output.
    // Drain it through the REAL enrichment path: the spec must resolve from
    // the unified store (written at patch ingress, not registration).
    let ti = Uuid::new_v4();
    seed_ctx_output(&nats, wi, ti, "verdict", "go").await;
    precreate_fast_consumer(&nats).await;
    publish_completed_event(&nats, minted, workflow_id, wi, ti).await;
    let (mut rx, token, handle) = spawn_drain(nats.clone(), Arc::clone(&pool));
    let msg = tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("forward did not arrive in time")
        .expect("relay channel closed");
    let vars = completed_routing_vars(&msg);
    assert_eq!(
        routing_string(vars.get("verdict")),
        Some("go"),
        "a patched-in task's declared routing variable is stamped from the unified store"
    );
    token.cancel();
    let _ = handle.await;

    // Server-side loop termination given these enriched routing variables is verified by the server-side gate-lifecycle and sim suites; the conductor's responsibility — stamping the declared routing variables via the task-keyed lookup — is asserted above.
}

/// Continue-iteration: the producer completes WITHOUT emitting its
/// default-bearing `loop_control` — a legitimate "keep looping" turn. Bare
/// absence must not trip the fail-closed check: the completion forwards as
/// `Completed` with nothing stamped and the loop keeps turning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn continue_iteration_bare_absence_forwards_completed_unchanged() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    let (wf, looper_id, _) = build_loop_workflow("loop-continue", 1);
    insert_workflow(&pool, &wf).await;

    let run_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    // The iteration emitted only undeclared inter-task data — no
    // `loop_control` verdict this turn.
    seed_ctx_output(&nats, run_id, task_instance_id, "scratch", "inter-task").await;

    precreate_fast_consumer(&nats).await;
    publish_completed_event(&nats, looper_id, wf_id(&wf), run_id, task_instance_id).await;

    let (mut rx, token, handle) = spawn_drain(nats.clone(), Arc::clone(&pool));

    let msg = tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("completion did not arrive in time")
        .expect("relay channel closed");
    // `completed_routing_vars` panics on any non-`Completed` kind — the
    // completion must NOT have been escalated to a failure.
    let vars = completed_routing_vars(&msg);
    assert!(
        vars.is_empty(),
        "a continue-iteration stamps nothing; got {:?}",
        vars
    );
    await_stream_drained(&nats).await;

    token.cancel();
    let _ = handle.await;
}

/// AC: self-patch ingress on the completion drain. A completing task carries
/// its raw Patch document on the reserved `tickr_patch` ctx output; the drain
/// detects it, stamps the attempt-invariant `patch_key` onto the forwarded
/// completion (so the server arms the Stall on presence, pre-cascade), and —
/// only AFTER the completion forwards (FIFO: the Stall always arms before the
/// apply can ask) — forks the parsed document into the patch pipeline, which
/// opens the lifecycle row and relays the patch envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_patch_output_is_detected_stamped_and_forked_on_the_drain() {
    use tickr_conductor::patch_pipeline::{fetch_row, patch_key};

    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let Some((_pg_c, pool)) = start_postgres().await else {
        return;
    };
    let pool = Arc::new(pool);

    // A registered workflow whose task declares nothing — enrichment forwards
    // the completion bare; the drain's self-patch detection is what's under
    // test.
    let workflow_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"self-patch-drain");
    let emitter_id = Uuid::new_v4();
    let start = Uuid::new_v4();
    let end = Uuid::new_v4();
    let wf = wf::WorkflowDefinition {
        id: workflow_id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: "self-patch-drain".to_string(),
        version: 1,
        tasks: vec![wf::TaskDefinition {
            id: emitter_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "emitter".to_string(),
            task_type: wf::TaskType::Regular as i32,
            nix_expression_path: String::new(),
            nix_args: Vec::new(),
            outputs: Vec::new(),
            inputs: Vec::new(),
            secrets: Vec::new(),
            max_attempts: 3,
            input_sources: None,
            timeout_secs: None,
            emits: Vec::new(),
            routing_vars: Vec::new(),
            loop_participant: false,
        }],
        task_graph: Some(wf::TaskGraph {
            nodes: vec![
                wf::GraphNode {
                    id: start.to_string(),
                    node_type: wf::NodeType::Start as i32,
                },
                wf::GraphNode {
                    id: emitter_id.to_string(),
                    node_type: wf::NodeType::Task as i32,
                },
                wf::GraphNode {
                    id: end.to_string(),
                    node_type: wf::NodeType::End as i32,
                },
            ],
            edges: vec![
                wf::Edge {
                    id: Uuid::new_v4().to_string(),
                    sources: vec![start.to_string()],
                    targets: vec![emitter_id.to_string()],
                    kind: wf::EdgeKind::Control as i32,
                    gates: Vec::new(),
                },
                wf::Edge {
                    id: Uuid::new_v4().to_string(),
                    sources: vec![emitter_id.to_string()],
                    targets: vec![end.to_string()],
                    kind: wf::EdgeKind::Control as i32,
                    gates: Vec::new(),
                },
            ],
            start: start.to_string(),
            end: end.to_string(),
        }),
        trigger: None,
        status: wf::WorkflowStatus::Inactive as i32,
        captures: Vec::new(),
        timeout_secs: None,
        tags: HashMap::new(),
    };
    insert_workflow(&pool, &wf).await;

    // The task published its Patch document under the reserved output name
    // before exiting (emit-and-exit): a JSON document, no Nickel round-trip.
    let run_id = Uuid::new_v4();
    let task_instance_id = Uuid::new_v4();
    let spliced = Uuid::new_v4();
    let document = serde_json::json!({
        "ops": [
            { "AddNode": { "node_id": spliced } },
            { "AddEdge": { "sources": [emitter_id], "targets": [spliced],
                           "kind": "Control", "gates": [] } },
            { "AddEdge": { "sources": [spliced], "targets": [end],
                           "kind": "Control", "gates": [] } }
        ],
        "reason": "self splice"
    });
    seed_ctx_output_typed(
        &nats,
        run_id,
        task_instance_id,
        "tickr_patch",
        "string",
        document,
    )
    .await;

    precreate_fast_consumer(&nats).await;
    publish_completed_event(&nats, emitter_id, workflow_id, run_id, task_instance_id).await;

    // The drain's pipeline fork relays through the process-global sender;
    // point it at the same channel the drain forwards completions on, so the
    // test observes the exact wire ordering the server would.
    let (tx, mut rx) = mpsc::channel::<ConductorRelayMessage>(16);
    tickr_conductor::relay::init_relay_tx(tx.clone()).await;
    let token = CancellationToken::new();
    let drain_token = token.clone();
    let drain_nats = nats.clone();
    let drain_pool = Arc::clone(&pool);
    let handle = tokio::spawn(async move {
        let consumer = task_event_consumer(&drain_nats).await.expect("consumer");
        drain_task_events(consumer, tx, drain_pool, drain_nats.clone(), drain_token).await;
    });

    async fn recv(rx: &mut mpsc::Receiver<ConductorRelayMessage>) -> ConductorRelayMessage {
        tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("message did not arrive in time")
            .expect("relay channel closed")
    }

    // First on the wire: the completion, stamped with the attempt-invariant
    // patch_key — the server arms the Stall on this, atomically with the
    // completion, before its cascade walk.
    let expected_key = patch_key(run_id, emitter_id);
    let first = recv(&mut rx).await;
    assert_eq!(first.entity_type, EntityType::TaskEvent as i32);
    let forwarded = tc::TaskEvent::decode(&first.payload[..]).expect("decode completion");
    assert_eq!(forwarded.task_instance_id, task_instance_id.to_string());
    match forwarded.kind {
        Some(tc::task_event::Kind::Completed(completed)) => {
            assert_eq!(
                completed.self_patch,
                Some(expected_key.to_string()),
                "the drain stamps the self-patch key on the forwarded completion"
            );
        }
        other => panic!("expected Completed, got {:?}", other),
    }

    // Second on the wire (FIFO — after the Stall-arming completion): the
    // forked patch envelope for the same key.
    let second = recv(&mut rx).await;
    assert_eq!(
        second.entity_type,
        EntityType::PatchWorkflowInstance as i32,
        "the pipeline fork relays only after the completion forwarded"
    );
    let envelope = <pp::PatchEnvelope as prost::Message>::decode(&second.payload[..])
        .expect("decode patch envelope");
    assert_eq!(envelope.patch_key, expected_key.to_string());
    assert_eq!(envelope.workflow_instance_id, run_id.to_string());
    assert_eq!(envelope.ops.len(), 3);

    // The fork opened the lifecycle row keyed UUIDv5(instance, node_id) and
    // the successful relay flipped it to Submitted.
    // The relay flips the row to Submitted on a background drain task, which
    // can lag the wire-order assertions above; poll rather than fetch-once so
    // the assertion doesn't race the async flip (~5s ceiling).
    let mut row = fetch_row(&pool, expected_key)
        .await
        .expect("fetch")
        .expect("the fork opens a lifecycle row");
    for _ in 0..100 {
        if row.status == "Submitted" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        row = fetch_row(&pool, expected_key)
            .await
            .expect("fetch")
            .expect("the fork opens a lifecycle row");
    }
    assert_eq!(row.workflow_instance_id, run_id);
    assert_eq!(row.status, "Submitted");

    token.cancel();
    let _ = handle.await;
}
