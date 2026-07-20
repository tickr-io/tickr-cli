//! Real-NATS + real-Postgres integration test for the `tickr_graph` ctx
//! mirror the conductor writes at materialization.
//!
//! Spins an ephemeral Postgres (for the conductor's `workflows` table) and an
//! ephemeral NATS-with-JetStream (for the run-scoped ctx KV). Registers a
//! workflow definition, invokes `mirror_ctx_graph`, then reads the run-scoped
//! `tickr_graph` key back through the exact envelope/render path a task uses
//! for `tickr-ctx get` — asserting every HyperNode and HyperEdge carries the
//! same identity code the HTTP instance view's projection produces, plus the
//! Instance-version header.
//!
//! Requires Docker running (testcontainers). Skipped automatically when Docker
//! isn't available — the connection failure is the skip marker.

#![cfg(not(madsim))]

mod common;

use std::time::Duration;

use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uuid::Uuid;

use async_nats::jetstream;
use tickr_conductor::ctx_graph_mirror::{
    ctx_graph_projection_from_definition, mirror_ctx_graph, mirror_reshaped_ctx_graph,
    CTX_GRAPH_KEY,
};
use tickr_conductor::parser::Parser;
use tickr_ctx::envelope::{Envelope, Producer};
use tickr_proto::identity_code;
use tickr_proto::instance::CtxGraphProjection;
use tickr_proto::workflow::WorkflowDefinition;

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

/// Register a workflow definition row the mirror reads back by workflow id.
async fn insert_workflow(pool: &sqlx::PgPool, workflow: &WorkflowDefinition) {
    let definition = serde_json::to_value(workflow).expect("serialize workflow definition");
    sqlx::query(
        "INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
         VALUES ($1, $2, $3, $4, $5, 'Ready', 'testhash', 'testcos', $6, '')",
    )
    .bind(uuid::Uuid::parse_str(&workflow.id).expect("workflow id is UUID"))
    .bind(workflow.version)
    .bind(&workflow.namespace)
    .bind(&workflow.slug)
    .bind(&workflow.name)
    .bind(definition)
    .execute(pool)
    .await
    .expect("insert workflow row");
}

/// A sealed two-task workflow `start → a → b → end`, built through the same
/// parser and decoder path used by registration.
async fn two_task_workflow() -> WorkflowDefinition {
    const JSON: &str = r#"{
        "slug": "ctx-graph-mirror-test",
        "name": "ctx-graph-mirror-test",
        "args": [],
        "outputs": [],
        "tasks": [
            {
                "name": "g",
                "args": [],
                "outputs": [],
                "tasks": [
                    {
                        "name": "a", "args": [], "outputs": [], "nix_expression_path": "x",
                        "routing_vars": [{ "name": "loop_control", "kind": "routing-var", "type": "string" }]
                    },
                    { "name": "b", "args": [], "outputs": [], "nix_expression_path": "x" }
                ],
                "edges": [
                    {
                        "sources": ["a"], "targets": ["a"], "kind": "loop",
                        "gate": {
                            "kind": "predicate-gate", "routing_var": "loop_control",
                            "op": "Eq", "value": "continue"
                        }
                    },
                    {
                        "sources": ["a"], "targets": ["b"], "kind": "data",
                        "gate": {
                            "kind": "predicate-gate", "routing_var": "loop_control",
                            "op": "Eq", "value": "done"
                        }
                    }
                ]
            }
        ]
    }"#;
    Parser::parse_workflow_from_json(JSON, "default")
        .await
        .expect("ctx-graph-mirror workflow parses")
}

/// Read the run-scoped `tickr_graph` key exactly as a task would: fetch the
/// bytes, deserialize the `Envelope`, and `render()` it back to the wrapped
/// JSON document.
async fn read_ctx_graph(
    nats: &async_nats::Client,
    run_id: Uuid,
) -> Option<(Envelope, CtxGraphProjection)> {
    let js = jetstream::new(nats.clone());
    let kv = js.get_key_value("ctx-default").await.ok()?;
    let key = format!("{}/{}", run_id, CTX_GRAPH_KEY);
    let bytes = kv.get(&key).await.ok()??;
    let envelope: Envelope = serde_json::from_slice(&bytes).expect("value is a tickr-ctx envelope");
    let rendered = envelope.render().expect("render json envelope");
    let ctx_graph: CtxGraphProjection =
        serde_json::from_slice(&rendered).expect("rendered value is a CtxGraphProjection");
    Some((envelope, ctx_graph))
}

/// Acceptance: on materialization the reserved `tickr_graph` key holds the full
/// task graph with identity codes and UUIDs on every HyperNode and HyperEdge,
/// plus the Instance-version header — at parity with the published instance
/// graph projection. The read never touches the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialization_writes_the_ctx_graph_with_codes_and_version() {
    let Some((_pg, pool)) = common::test_db().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let workflow = two_task_workflow().await;
    let workflow_id = Uuid::parse_str(&workflow.id).expect("workflow id is UUID");
    insert_workflow(&pool, &workflow).await;

    let run_id = Uuid::new_v4();
    mirror_ctx_graph(&pool, &nats, run_id, workflow_id)
        .await
        .expect("mirror the ctx graph");

    let (envelope, ctx_graph) = read_ctx_graph(&nats, run_id)
        .await
        .expect("tickr_graph key present after materialization");

    // A conductor system write, rendered as JSON.
    assert_eq!(envelope.kind, "json");
    match envelope.producer {
        Producer::System { component } => assert_eq!(component, "conductor"),
        other => panic!("expected a System producer, got {other:?}"),
    }

    // A freshly materialized instance has never been patched.
    assert_eq!(ctx_graph.version, 0);

    // Every node and edge carries its identity code alongside its full UUID.
    let graph = ctx_graph
        .graph
        .as_ref()
        .expect("projection carries a graph");
    assert!(
        graph.nodes.len() >= 4,
        "start + a + b + end are all present"
    );
    for node in &graph.nodes {
        let uuid = Uuid::parse_str(&node.id).expect("node id is the full UUID");
        assert_eq!(node.code, identity_code(&uuid));
        assert_eq!(node.code.len(), 4);
    }
    assert!(!graph.edges.is_empty(), "the sealed graph has edges");
    for edge in &graph.edges {
        let uuid = Uuid::parse_str(&edge.id).expect("edge id is the full UUID");
        assert_eq!(edge.code, identity_code(&uuid));
        assert_eq!(edge.code.len(), 4);
    }
    let loop_edge = graph
        .edges
        .iter()
        .find(|edge| edge.kind == "loop")
        .expect("loop edge is mirrored");
    let gate = loop_edge.gates.first().expect("loop gate is mirrored");
    assert_eq!(gate.kind, "predicate");
    assert_eq!(gate.state, "Idle");
    assert!(gate.transitions.is_empty());

    // Parity: the mirror is the published projection of this definition's
    // graph — same structures, gate views, and identity codes.
    assert_eq!(
        ctx_graph,
        ctx_graph_projection_from_definition(
            workflow.task_graph.as_ref().expect("workflow graph"),
            0,
        ),
    );
}

/// Acceptance: the mirror is written once at materialization; a second sighting
/// of the run (another task dispatch) is a present-key no-op that leaves the
/// stored value byte-for-byte unchanged — no rewrite churn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mirror_is_idempotent_across_repeated_dispatches() {
    let Some((_pg, pool)) = common::test_db().await else {
        return;
    };
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };

    let workflow = two_task_workflow().await;
    let workflow_id = Uuid::parse_str(&workflow.id).expect("workflow id is UUID");
    insert_workflow(&pool, &workflow).await;
    let run_id = Uuid::new_v4();

    mirror_ctx_graph(&pool, &nats, run_id, workflow_id)
        .await
        .expect("first mirror");
    let (first, _) = read_ctx_graph(&nats, run_id)
        .await
        .expect("present after first");

    mirror_ctx_graph(&pool, &nats, run_id, workflow_id)
        .await
        .expect("second mirror is a no-op");
    let (second, _) = read_ctx_graph(&nats, run_id).await.expect("still present");

    // The present-key short-circuit means the first write is retained verbatim.
    assert_eq!(first.created_at, second.created_at);
    assert_eq!(first.sha256, second.sha256);
}

/// Acceptance: after a successful apply, the `tickr_graph` ctx key reflects the
/// *reshaped* graph. The re-mirror overwrites the materialization mirror with
/// the graph the server relayed on the successful `PatchOutcome` — no PG read
/// and no present-key short-circuit, because a patch is exactly the rewrite the
/// mirror must now show. A self-patching task re-reads the new structures (and
/// their fresh identity codes) straight from NATS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn re_mirror_on_apply_overwrites_with_the_reshaped_graph() {
    let Some((_nats_container, nats)) = start_nats().await else {
        return;
    };
    let workflow = two_task_workflow().await;
    let run_id = Uuid::new_v4();

    // Seed the key with the base (version 0) graph, standing in for the
    // materialization mirror.
    let base = ctx_graph_projection_from_definition(
        workflow.task_graph.as_ref().expect("workflow graph"),
        0,
    );
    mirror_reshaped_ctx_graph(&nats, run_id, &serde_json::to_string(&base).unwrap())
        .await
        .expect("seed base mirror");

    // Reshape as a patch would: drop one edge from the live graph and bump the
    // Instance version. This stands in for the server's post-apply graph.
    let mut reshaped_graph = workflow.task_graph.clone().expect("workflow graph");
    let victim = reshaped_graph
        .edges
        .first()
        .map(|edge| edge.id.clone())
        .expect("the sealed graph has edges");
    reshaped_graph.edges.retain(|edge| edge.id != victim);
    let reshaped = ctx_graph_projection_from_definition(&reshaped_graph, 1);

    mirror_reshaped_ctx_graph(&nats, run_id, &serde_json::to_string(&reshaped).unwrap())
        .await
        .expect("re-mirror the reshaped graph");

    let (_, stored) = read_ctx_graph(&nats, run_id)
        .await
        .expect("key present after re-mirror");

    // The key now reflects the reshaped graph, not the base: the Instance
    // version bumped and the patched-out edge is gone.
    assert_eq!(stored.version, 1, "re-mirror carries the bumped version");
    assert_eq!(stored.graph, reshaped.graph);
    assert!(
        stored.graph.as_ref().expect("stored graph").edges.len()
            < base.graph.as_ref().expect("base graph").edges.len(),
        "the patched-out edge is gone from the mirror"
    );
    assert!(
        !stored
            .graph
            .as_ref()
            .expect("stored graph")
            .edges
            .iter()
            .any(|e| e.id == victim),
        "the removed edge is absent"
    );
}
