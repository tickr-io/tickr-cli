//! Mirror the live task graph into the run-scoped tickr-ctx KV so a task can
//! author a self-patch by reading the graph directly from NATS. Every HyperNode
//! and HyperEdge includes a short identity code, and graph reads remain local to
//! the runtime.
//!
//! The mirror is written under the reserved run-scoped key `tickr_graph` when
//! an instance materializes. At materialization the instance's live graph is
//! exactly the registered definition's sealed graph (the instance clones it and
//! has never been patched), so the conductor reconstructs the graph from its
//! own `workflows` store and serializes it through the same projection the HTTP
//! instance view uses — the codes exposed here match the view's for that
//! instance. The write is advisory: it reflects the graph at materialization
//! time, with no freshness claim beyond that.

use anyhow::{Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use tickr_ctx::envelope::{Envelope, Producer};
use tickr_ctx::scope::sanitize_segment;
use tickr_proto::instance as ip;
use tickr_proto::workflow as wf;

/// Reserved run-scoped key holding the published graph projection.
pub const CTX_GRAPH_KEY: &str = "tickr_graph";

/// Project a stored workflow definition onto the published task-visible graph.
/// A newly materialized run has no runtime transitions: every node is pending
/// and every declared gate is idle. Patch refreshes instead receive this same
/// projection from the server with its live runtime overlays.
pub fn ctx_graph_projection_from_definition(
    graph: &wf::TaskGraph,
    version: u32,
) -> ip::CtxGraphProjection {
    let mut nodes: Vec<_> = graph
        .nodes
        .iter()
        .map(|node| ip::SnapshotNode {
            code: node_code(&node.id),
            id: node.id.clone(),
            kind: node_kind(node.node_type).to_string(),
            ground: "pending".to_string(),
            grounded_at: None,
            ghost: false,
            pre_grounded: false,
        })
        .collect();
    nodes.sort_by(|a, b| a.id.cmp(&b.id));

    let mut edges: Vec<_> = graph
        .edges
        .iter()
        .map(|edge| {
            let mut sources = edge.sources.clone();
            let mut targets = edge.targets.clone();
            sources.sort();
            targets.sort();
            ip::SnapshotEdge {
                code: node_code(&edge.id),
                id: edge.id.clone(),
                sources,
                targets,
                kind: edge_kind(edge.kind).to_string(),
                gates: edge.gates.iter().map(gate_view).collect(),
            }
        })
        .collect();
    edges.sort_by(|a, b| a.id.cmp(&b.id));

    ip::CtxGraphProjection {
        version,
        graph: Some(ip::SnapshotGraph {
            start: graph.start.clone(),
            end: graph.end.clone(),
            nodes,
            edges,
        }),
    }
}

fn node_code(id: &str) -> String {
    uuid::Uuid::parse_str(id)
        .map(|id| tickr_proto::identity_code(&id))
        .unwrap_or_default()
}

fn node_kind(kind: i32) -> &'static str {
    match wf::NodeType::try_from(kind).unwrap_or(wf::NodeType::Task) {
        wf::NodeType::Start => "start",
        wf::NodeType::End => "end",
        wf::NodeType::Task => "task",
    }
}

fn edge_kind(kind: i32) -> &'static str {
    match wf::EdgeKind::try_from(kind).unwrap_or(wf::EdgeKind::Control) {
        wf::EdgeKind::Control => "control",
        wf::EdgeKind::Data => "data",
        wf::EdgeKind::Loop => "loop",
    }
}

fn gate_view(gate: &wf::Gate) -> ip::GateView {
    let mut view = ip::GateView {
        kind: String::new(),
        state: "Idle".to_string(),
        signal_id: None,
        signal_name: None,
        predicate: None,
        captures: Vec::new(),
        routing_var: None,
        op: None,
        value: None,
        timeout_secs: None,
        duration_secs: None,
        transitions: Vec::new(),
    };
    match gate.kind.as_ref() {
        Some(wf::gate::Kind::SignalReceived(gate)) => {
            view.kind = "signal".to_string();
            view.signal_name = Some(gate.signal_name.clone());
            view.predicate = gate.predicate.clone();
            view.captures = gate
                .captures_spec
                .iter()
                .map(|capture| capture.name.clone())
                .collect();
            view.timeout_secs = gate.timeout.as_ref().map(|timeout| timeout.secs);
        }
        Some(wf::gate::Kind::PredicateHolds(gate)) => {
            view.kind = "predicate".to_string();
            view.routing_var = Some(gate.routing_var.clone());
            view.op = Some(comparison_symbol(gate.op).to_string());
            view.value = gate.value.as_ref().and_then(routing_value_view);
            view.timeout_secs = gate.timeout.as_ref().map(|timeout| timeout.secs);
        }
        Some(wf::gate::Kind::TimerElapsed(gate)) => {
            view.kind = "timer".to_string();
            view.duration_secs = gate.duration.as_ref().map(|duration| duration.secs);
        }
        None => {}
    }
    view
}

fn comparison_symbol(op: i32) -> &'static str {
    match wf::ComparisonOp::try_from(op).unwrap_or(wf::ComparisonOp::Eq) {
        wf::ComparisonOp::Eq => "==",
        wf::ComparisonOp::NotEq => "!=",
        wf::ComparisonOp::Lt => "<",
        wf::ComparisonOp::Le => "<=",
        wf::ComparisonOp::Gt => ">",
        wf::ComparisonOp::Ge => ">=",
    }
}

fn routing_value_view(value: &wf::RoutingValue) -> Option<ip::RoutingValueView> {
    use ip::routing_value_view::Value as ProjectionValue;
    use wf::routing_value::Value;

    let value = match value.value.as_ref()? {
        Value::StringValue(value) => ProjectionValue::StringValue(value.clone()),
        Value::IntValue(value) => ProjectionValue::IntValue(*value),
        Value::BoolValue(value) => ProjectionValue::BoolValue(*value),
        Value::BytesValue(value) => ProjectionValue::BytesValue(hex::encode(value)),
    };
    let kind = match &value {
        ProjectionValue::StringValue(_) => "string",
        ProjectionValue::IntValue(_) => "int",
        ProjectionValue::BoolValue(_) => "bool",
        ProjectionValue::BytesValue(_) => "bytes",
    };
    Some(ip::RoutingValueView {
        kind: kind.to_string(),
        value: Some(value),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_projection_preserves_loop_gate_and_task_identity() {
        let start = uuid::Uuid::new_v4();
        let end = uuid::Uuid::new_v4();
        let task = uuid::Uuid::new_v4();
        let edge = uuid::Uuid::new_v4();
        let graph = wf::TaskGraph {
            start: start.to_string(),
            end: end.to_string(),
            nodes: vec![
                wf::GraphNode {
                    id: start.to_string(),
                    node_type: wf::NodeType::Start as i32,
                },
                wf::GraphNode {
                    id: task.to_string(),
                    node_type: wf::NodeType::Task as i32,
                },
                wf::GraphNode {
                    id: end.to_string(),
                    node_type: wf::NodeType::End as i32,
                },
            ],
            edges: vec![wf::Edge {
                id: edge.to_string(),
                sources: vec![task.to_string()],
                targets: vec![task.to_string()],
                kind: wf::EdgeKind::Loop as i32,
                gates: vec![wf::Gate {
                    kind: Some(wf::gate::Kind::SignalReceived(wf::gate::SignalReceived {
                        signal_name: "continue".to_string(),
                        predicate: Some("$.continue".to_string()),
                        captures_spec: Vec::new(),
                        timeout: Some(wf::Duration { secs: 30, nanos: 0 }),
                    })),
                }],
            }],
        };

        let projection = ctx_graph_projection_from_definition(&graph, 0);
        let projected = projection.graph.expect("projection carries graph");
        let task_node = projected
            .nodes
            .iter()
            .find(|node| node.id == task.to_string())
            .expect("task node is projected");
        assert_eq!(task_node.code, tickr_proto::identity_code(&task));
        assert_eq!(task_node.ground, "pending");

        let loop_edge = projected.edges.first().expect("loop edge is projected");
        assert_eq!(loop_edge.code, tickr_proto::identity_code(&edge));
        assert_eq!(loop_edge.kind, "loop");
        assert_eq!(loop_edge.gates.len(), 1);
        let gate = &loop_edge.gates[0];
        assert_eq!(gate.kind, "signal");
        assert_eq!(gate.state, "Idle");
        assert_eq!(gate.signal_name.as_deref(), Some("continue"));
        assert_eq!(gate.timeout_secs, Some(30));
        assert!(gate.transitions.is_empty());
    }
}

/// Per-tenant tickr-ctx bucket namespace. Matches the value the other ctx
/// writers use so the graph lands in the bucket the executor reads from.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// Write the `tickr_graph` ctx mirror for a run, if not already present.
///
/// Called for every run (cron- and trigger-fired alike) off the inbound
/// `TaskQueueItem` relay arm — the conductor's only universal per-run sighting
/// of a materialized instance. Idempotent and cheap on the common path: the
/// mirror is written once at materialization, so a present key short-circuits
/// before any definition load. Best-effort — a failure leaves the advisory
/// mirror absent and is surfaced by the caller; it never blocks dispatch.
pub async fn mirror_ctx_graph(
    pool: &PgPool,
    nats: &NatsClient,
    run_id: Uuid,
    workflow_id: Uuid,
) -> Result<()> {
    let js = jetstream::new(nats.clone());
    let kv = get_or_create_ctx_bucket(&js).await?;

    let key = format!(
        "{}/{}",
        sanitize_segment(&run_id.to_string()),
        CTX_GRAPH_KEY
    );

    // Written once at materialization; a later patch owns any rewrite. Skipping
    // a present key keeps this off the definition-load path on every task
    // dispatch after the first.
    let present = kv
        .get(&key)
        .await
        .map_err(|e| anyhow::anyhow!("ctx graph mirror get {key}: {e}"))?
        .is_some();
    if present {
        return Ok(());
    }

    let workflow = match read_latest_workflow_definition(pool, workflow_id).await? {
        Some(w) => w,
        None => {
            // No stored definition for this workflow id — should not happen for
            // a run whose task the conductor is dispatching. Lost-but-logged:
            // the read path just finds no graph, never a server call.
            tracing::warn!(
                "ctx graph mirror: no workflow definition for {workflow_id}; \
                 run {run_id} gets no {CTX_GRAPH_KEY}"
            );
            return Ok(());
        }
    };

    // A freshly materialized instance has never been patched, so its
    // Instance-version header is 0 — the forward-compat token a future
    // external-author compare-and-swap reads.
    let graph = workflow
        .task_graph
        .as_ref()
        .context("workflow definition is missing task graph")?;
    let projection = ctx_graph_projection_from_definition(graph, 0);
    let value: Value = serde_json::to_value(&projection).context("serialize ctx graph")?;
    let envelope = Envelope::new(
        "json",
        value,
        false,
        Producer::System {
            component: "conductor".to_string(),
        },
    );
    let bytes = serde_json::to_vec(&envelope).context("serialize ctx graph envelope")?;
    kv.put(&key, bytes.into())
        .await
        .map_err(|e| anyhow::anyhow!("ctx graph mirror put {key}: {e}"))?;

    Ok(())
}

/// Overwrite the run's `tickr_graph` ctx mirror with the reshaped graph the
/// server produced at patch apply. Unlike the materialization mirror, this
/// does **not** read the definition (a patched live graph diverges from it) and
/// does **not** short-circuit on a present key — a patch is exactly the rewrite
/// the mirror must now reflect. `ctx_graph_json` is a serialized `CtxGraphProjection` the
/// server relayed on the successful `PatchOutcome`, written verbatim so a task
/// re-reading `tickr_graph` sees the new structures (and their fresh identity
/// codes) straight from NATS. Best-effort — a failure leaves the mirror at its
/// prior value and is surfaced by the caller.
pub async fn mirror_reshaped_ctx_graph(
    nats: &NatsClient,
    run_id: Uuid,
    ctx_graph_json: &str,
) -> Result<()> {
    let projection: ip::CtxGraphProjection =
        serde_json::from_str(ctx_graph_json).context("parse reshaped ctx graph projection")?;
    let value: Value =
        serde_json::to_value(projection).context("serialize reshaped ctx graph projection")?;
    let js = jetstream::new(nats.clone());
    let kv = get_or_create_ctx_bucket(&js).await?;
    let key = format!(
        "{}/{}",
        sanitize_segment(&run_id.to_string()),
        CTX_GRAPH_KEY
    );
    let envelope = Envelope::new(
        "json",
        value,
        false,
        Producer::System {
            component: "conductor".to_string(),
        },
    );
    let bytes = serde_json::to_vec(&envelope).context("serialize reshaped ctx graph envelope")?;
    kv.put(&key, bytes.into())
        .await
        .map_err(|e| anyhow::anyhow!("ctx graph re-mirror put {key}: {e}"))?;
    Ok(())
}

/// Get-or-create the per-tenant ctx KV bucket the graph mirror writes into.
/// Shared by the materialization mirror and the patch-apply re-mirror so both
/// land in the same bucket the executor reads from.
async fn get_or_create_ctx_bucket(js: &jetstream::Context) -> Result<jetstream::kv::Store> {
    let bucket_name = format!("ctx-{}", sanitize_segment(DEFAULT_CTX_NAMESPACE));
    match js.get_key_value(&bucket_name).await {
        Ok(kv) => Ok(kv),
        Err(_) => js
            .create_key_value(jetstream::kv::Config {
                bucket: bucket_name.clone(),
                history: 1,
                max_value_size: tickr_ctx::store::MAX_VALUE_SIZE,
                ..Default::default()
            })
            .await
            .context("create ctx bucket for graph mirror"),
    }
}

/// Load the current definition for `workflow_id` from the conductor's
/// per-tenant `workflows` table. The table is keyed `(id, version)` and holds
/// one immutable row per version; a just-materialized instance ran off the
/// latest, so we take the highest version. Returns `None` when no row exists.
async fn read_latest_workflow_definition(
    pool: &PgPool,
    workflow_id: Uuid,
) -> Result<Option<wf::WorkflowDefinition>> {
    let row: Option<(Value,)> = sqlx::query_as(
        "SELECT definition FROM workflows WHERE id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(workflow_id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((definition,)) => Ok(Some(
            crate::definition_store::proto_from_stored_definition(definition)
                .context("decode workflow definition from JSONB")?,
        )),
        None => Ok(None),
    }
}
