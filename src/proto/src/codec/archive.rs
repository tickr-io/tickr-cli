//! Archive projection codecs.
//!
//! PostgreSQL stores the serialized union projection, so reads decode directly
//! into the published archive, runnable, and instance protobuf messages.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use uuid::Uuid;

use crate::archive as ap;
use crate::identity_code;
use crate::instance as ip;
use crate::runnable as rp;
use crate::workflow as wf;

/// Deserialize the union archive projection from a terminal instance's stored
/// JSONB.
pub fn archive_projection_from_json(blob: serde_json::Value) -> Result<ap::ArchiveProjection> {
    serde_json::from_value(blob).context("decode the archived union projection")
}

/// Rehydrate an archived-detail projection directly from the stored union.
/// Every rendered value comes from the published archive, runnable, and
/// instance messages.
pub fn archived_instance_from_json(blob: serde_json::Value) -> Result<ip::ArchivedInstance> {
    archived_instance_from_projection(&archive_projection_from_json(blob)?)
}

/// Render the archive union into the archived half of the instance snapshot.
/// Kept in the published-contract crate so archive readers share the same
/// conversion regardless of which component serves the read.
pub fn archived_instance_from_projection(
    union: &ap::ArchiveProjection,
) -> Result<ip::ArchivedInstance> {
    let runnable = union
        .runnable
        .as_ref()
        .ok_or_else(|| anyhow!("union projection carries no runnable section"))?;
    let graph = runnable
        .graph
        .as_ref()
        .ok_or_else(|| anyhow!("runnable projection carries no graph"))?;
    let minted: HashSet<&str> = union
        .task_instances
        .iter()
        .map(|task| task.task_id.as_str())
        .collect();
    let pre_grounded: HashSet<&str> = union.pre_grounded.iter().map(String::as_str).collect();

    let mut tasks: Vec<_> = runnable
        .tasks
        .iter()
        .map(snapshot_task_from_runnable)
        .collect::<Result<_>>()?;
    tasks.sort_by(|a, b| a.name.cmp(&b.name));

    let task_count = graph
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                wf::NodeType::try_from(node.node_type),
                Ok(wf::NodeType::Task)
            )
        })
        .count() as u64;
    let completed_tasks = graph
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                wf::NodeType::try_from(node.node_type),
                Ok(wf::NodeType::Task)
            ) && matches!(
                rp::GroundState::try_from(node.ground),
                Ok(rp::GroundState::Success)
            ) && (minted.contains(node.id.as_str()) || pre_grounded.contains(node.id.as_str()))
        })
        .count() as u64;

    Ok(ip::ArchivedInstance {
        id: union.id.clone(),
        workflow_id: union.workflow_id.clone(),
        name: union.name.clone(),
        workflow_name: union.workflow_name.clone(),
        workflow_version: union.workflow_version,
        state: union.state.clone(),
        scheduled_at: union.scheduled_at.clone(),
        triggered_at: transition_at(&union.transitions, |state| state == "Triggered"),
        started_at: transition_at(&union.transitions, |state| state == "InProgress"),
        completed_at: transition_at(&union.transitions, |state| {
            matches!(state, "Completed" | "Failed" | "Cancelled")
        }),
        transitions: union.transitions.clone(),
        triggered_by: union.triggered_by.clone(),
        tags: union.tags.clone(),
        task_count,
        completed_tasks,
        tasks,
        task_instances: union.task_instances.clone(),
        graph: Some(snapshot_graph(graph, &minted, &pre_grounded, true)?),
        routing_variables: union.routing_variables.clone(),
        version: union.version,
        applied_patches: union.applied_patches.clone(),
        version_snapshots: union
            .graph_snapshots
            .iter()
            .map(|(version, graph)| {
                Ok((
                    *version,
                    snapshot_graph(graph, &HashSet::new(), &HashSet::new(), false)?,
                ))
            })
            .collect::<Result<_>>()?,
    })
}

fn transition_at(
    transitions: &[ip::StateTransitionView],
    matches_state: impl Fn(&str) -> bool,
) -> Option<String> {
    transitions
        .iter()
        .find(|transition| matches_state(&transition.to))
        .map(|transition| transition.at.clone())
}

fn snapshot_task_from_runnable(task: &wf::TaskDefinition) -> Result<ip::SnapshotTaskDef> {
    let task_type = match wf::TaskType::try_from(task.task_type)
        .map_err(|_| anyhow!("unknown task type discriminant {}", task.task_type))?
    {
        wf::TaskType::Regular => "RegularTask",
        wf::TaskType::Sensor => "SensorTask",
        wf::TaskType::Shadow => "ShadowTask",
    };
    let input_sources = task.input_sources.as_ref();
    let inputs = task
        .inputs
        .iter()
        .enumerate()
        .map(|(index, name)| {
            Ok(ip::TaskInputView {
                name: name.clone(),
                source: input_sources
                    .and_then(|sources| sources.sources.get(index))
                    .and_then(|slot| slot.source.as_ref())
                    .map(snapshot_input_source)
                    .transpose()?,
            })
        })
        .collect::<Result<_>>()?;
    let emits = task
        .emits
        .iter()
        .map(|emit| match emit.emit.as_ref() {
            Some(wf::task_signal_emit::Emit::OnSuccess(success)) => Ok(ip::TaskEmitView {
                kind: "on_success".to_string(),
                signal_name: success.signal_name.clone(),
                from_routing_var: Some(success.from_routing_var.clone()),
            }),
            Some(wf::task_signal_emit::Emit::OnFailure(failure)) => Ok(ip::TaskEmitView {
                kind: "on_failure".to_string(),
                signal_name: failure.signal_name.clone(),
                from_routing_var: None,
            }),
            None => Err(anyhow!("task signal emit carries no variant")),
        })
        .collect::<Result<_>>()?;

    Ok(ip::SnapshotTaskDef {
        id: task.id.clone(),
        name: task.name.clone(),
        task_type: task_type.to_string(),
        max_attempts: task.max_attempts,
        timeout_secs: task.timeout_secs,
        nix_expression_path: task.nix_expression_path.clone(),
        inputs,
        outputs: task.outputs.clone(),
        secrets: task.secrets.clone(),
        routing_vars: task
            .routing_vars
            .iter()
            .map(|routing_var| ip::RoutingVarDeclView {
                name: routing_var.name.clone(),
                var_type: routing_var.var_type.clone(),
            })
            .collect(),
        emits,
    })
}

fn snapshot_input_source(source: &wf::InputSource) -> Result<ip::InputSourceView> {
    let source = source
        .source
        .as_ref()
        .ok_or_else(|| anyhow!("task input source carries no variant"))?;
    Ok(match source {
        wf::input_source::Source::Task(task) => ip::InputSourceView {
            kind: "task".to_string(),
            task: Some(task.name.clone()),
            signal_name: None,
            gate_edge_id: None,
        },
        wf::input_source::Source::Trigger(_) => ip::InputSourceView {
            kind: "trigger".to_string(),
            task: None,
            signal_name: None,
            gate_edge_id: None,
        },
        wf::input_source::Source::Signal(signal) => ip::InputSourceView {
            kind: "signal".to_string(),
            task: None,
            signal_name: Some(signal.signal_name.clone()),
            gate_edge_id: Some(signal.gate_edge_id.clone()),
        },
    })
}

fn snapshot_graph(
    graph: &rp::RunnableGraph,
    minted: &HashSet<&str>,
    pre_grounded: &HashSet<&str>,
    overlay_instance_facts: bool,
) -> Result<ip::SnapshotGraph> {
    let mut nodes = graph
        .nodes
        .iter()
        .map(|node| snapshot_node(node, minted, pre_grounded, overlay_instance_facts))
        .collect::<Result<Vec<_>>>()?;
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    let mut edges = graph
        .edges
        .iter()
        .map(snapshot_edge)
        .collect::<Result<Vec<_>>>()?;
    edges.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(ip::SnapshotGraph {
        start: graph.start.clone(),
        end: graph.end.clone(),
        nodes,
        edges,
    })
}

fn snapshot_node(
    node: &rp::RunnableNode,
    minted: &HashSet<&str>,
    pre_grounded: &HashSet<&str>,
    overlay_instance_facts: bool,
) -> Result<ip::SnapshotNode> {
    let node_type = wf::NodeType::try_from(node.node_type)
        .map_err(|_| anyhow!("unknown node type discriminant {}", node.node_type))?;
    let ground = match rp::GroundState::try_from(node.ground)
        .map_err(|_| anyhow!("unknown ground state discriminant {}", node.ground))?
    {
        rp::GroundState::Pending => "pending",
        rp::GroundState::Success => "success",
        rp::GroundState::Failed => "failed",
        rp::GroundState::Cancelled => "cancelled",
    };
    let id = Uuid::parse_str(&node.id).context("parse runnable node id for identity code")?;
    let is_task = matches!(node_type, wf::NodeType::Task);
    Ok(ip::SnapshotNode {
        code: identity_code(&id),
        id: node.id.clone(),
        kind: match node_type {
            wf::NodeType::Start => "start",
            wf::NodeType::End => "end",
            wf::NodeType::Task => "task",
        }
        .to_string(),
        ground: ground.to_string(),
        grounded_at: node.grounded_at.clone(),
        ghost: overlay_instance_facts
            && is_task
            && node.grounded_at.is_some()
            && !minted.contains(node.id.as_str()),
        pre_grounded: overlay_instance_facts && pre_grounded.contains(node.id.as_str()),
    })
}

fn snapshot_edge(edge: &rp::RunnableEdge) -> Result<ip::SnapshotEdge> {
    let kind = match wf::EdgeKind::try_from(edge.kind)
        .map_err(|_| anyhow!("unknown edge kind discriminant {}", edge.kind))?
    {
        wf::EdgeKind::Control => "control",
        wf::EdgeKind::Data => "data",
        wf::EdgeKind::Loop => "loop",
    };
    let id = Uuid::parse_str(&edge.id).context("parse runnable edge id for identity code")?;
    Ok(ip::SnapshotEdge {
        code: identity_code(&id),
        id: edge.id.clone(),
        sources: edge.sources.clone(),
        targets: edge.targets.clone(),
        kind: kind.to_string(),
        gates: edge
            .gates
            .iter()
            .map(snapshot_gate)
            .collect::<Result<_>>()?,
    })
}

fn snapshot_gate(gate: &rp::RunnableGate) -> Result<ip::GateView> {
    let (state, signal_id) = snapshot_gate_state(
        gate.state
            .as_ref()
            .ok_or_else(|| anyhow!("runnable gate carries no runtime state"))?,
    )?;
    let transitions = gate
        .transitions
        .iter()
        .map(|transition| {
            let (from, _) = snapshot_gate_state(
                transition
                    .from
                    .as_ref()
                    .ok_or_else(|| anyhow!("gate transition carries no from-state"))?,
            )?;
            let (to, _) = snapshot_gate_state(
                transition
                    .to
                    .as_ref()
                    .ok_or_else(|| anyhow!("gate transition carries no to-state"))?,
            )?;
            Ok(ip::StateTransitionView {
                from,
                to,
                at: transition.at.clone(),
            })
        })
        .collect::<Result<_>>()?;
    match gate
        .declaration
        .as_ref()
        .ok_or_else(|| anyhow!("runnable gate carries no declaration"))?
        .kind
        .as_ref()
        .ok_or_else(|| anyhow!("runnable gate declaration carries no variant"))?
    {
        wf::gate::Kind::SignalReceived(signal) => Ok(ip::GateView {
            kind: "signal".to_string(),
            state,
            signal_id,
            signal_name: Some(signal.signal_name.clone()),
            predicate: signal.predicate.clone(),
            captures: signal
                .captures_spec
                .iter()
                .map(|capture| capture.name.clone())
                .collect(),
            routing_var: None,
            op: None,
            value: None,
            timeout_secs: signal.timeout.as_ref().map(|duration| duration.secs),
            duration_secs: None,
            transitions,
        }),
        wf::gate::Kind::PredicateHolds(predicate) => Ok(ip::GateView {
            kind: "predicate".to_string(),
            state,
            signal_id,
            signal_name: None,
            predicate: None,
            captures: Vec::new(),
            routing_var: Some(predicate.routing_var.clone()),
            op: Some(comparison_symbol(predicate.op)?.to_string()),
            value: predicate
                .value
                .as_ref()
                .map(snapshot_routing_value)
                .transpose()?,
            timeout_secs: predicate.timeout.as_ref().map(|duration| duration.secs),
            duration_secs: None,
            transitions,
        }),
        wf::gate::Kind::TimerElapsed(timer) => Ok(ip::GateView {
            kind: "timer".to_string(),
            state,
            signal_id,
            signal_name: None,
            predicate: None,
            captures: Vec::new(),
            routing_var: None,
            op: None,
            value: None,
            timeout_secs: None,
            duration_secs: timer.duration.as_ref().map(|duration| duration.secs),
            transitions,
        }),
    }
}

fn snapshot_gate_state(state: &rp::GateRuntimeState) -> Result<(String, Option<String>)> {
    use rp::gate_runtime_state::State;
    match state
        .state
        .as_ref()
        .ok_or_else(|| anyhow!("gate runtime state carries no variant"))?
    {
        State::Idle(_) => Ok(("Idle".to_string(), None)),
        State::Dispatched(_) => Ok(("Dispatched".to_string(), None)),
        State::Satisfied(satisfied) => {
            Ok(("Satisfied".to_string(), Some(satisfied.signal_id.clone())))
        }
        State::Rejected(_) => Ok(("Rejected".to_string(), None)),
        State::Cancelled(_) => Ok(("Cancelled".to_string(), None)),
    }
}

fn comparison_symbol(op: i32) -> Result<&'static str> {
    match wf::ComparisonOp::try_from(op)
        .map_err(|_| anyhow!("unknown comparison operator discriminant {op}"))?
    {
        wf::ComparisonOp::Eq => Ok("=="),
        wf::ComparisonOp::NotEq => Ok("!="),
        wf::ComparisonOp::Lt => Ok("<"),
        wf::ComparisonOp::Le => Ok("<="),
        wf::ComparisonOp::Gt => Ok(">"),
        wf::ComparisonOp::Ge => Ok(">="),
    }
}

fn snapshot_routing_value(value: &wf::RoutingValue) -> Result<ip::RoutingValueView> {
    use ip::routing_value_view::Value as SnapshotValue;
    let value = match value
        .value
        .as_ref()
        .ok_or_else(|| anyhow!("routing value carries no variant"))?
    {
        wf::routing_value::Value::StringValue(value) => SnapshotValue::StringValue(value.clone()),
        wf::routing_value::Value::IntValue(value) => SnapshotValue::IntValue(*value),
        wf::routing_value::Value::BoolValue(value) => SnapshotValue::BoolValue(*value),
        wf::routing_value::Value::BytesValue(value) => {
            SnapshotValue::BytesValue(value.iter().map(|byte| format!("{byte:02x}")).collect())
        }
    };
    let kind = match &value {
        SnapshotValue::StringValue(_) => "string",
        SnapshotValue::IntValue(_) => "int",
        SnapshotValue::BoolValue(_) => "bool",
        SnapshotValue::BytesValue(_) => "bytes",
    };
    Ok(ip::RoutingValueView {
        kind: kind.to_string(),
        value: Some(value),
    })
}

/// Reconstruct the slim instances-list row from a terminal instance's stored
/// JSONB — the union's list-relevant render metadata plus `task_count`, which
/// the read layer supplies from a count of the archived task-instance rows.
pub fn archive_list_row_from_json(
    instance_json: serde_json::Value,
    task_count: u64,
) -> Result<ip::ArchivedInstanceRow> {
    let union: ap::ArchiveProjection =
        serde_json::from_value(instance_json).context("decode the archived union projection")?;
    Ok(ip::ArchivedInstanceRow {
        id: union.id,
        workflow_id: union.workflow_id,
        workflow_version: union.workflow_version,
        name: union.name,
        state: union.state,
        scheduled_at: union.scheduled_at,
        task_count,
    })
}

/// Rehydrate one archived instance blob into the slim list row — the fields the
/// instances-list view projects per terminal run, reconstructed off the union's
/// list-relevant render metadata. `task_count` is the count of the instance's
/// archived task-instance rows, supplied by the read layer.
pub fn archived_instance_row_from_json(
    instance_json: serde_json::Value,
    task_count: u64,
) -> Result<ip::ArchivedInstanceRow> {
    archive_list_row_from_json(instance_json, task_count)
}

/// Rehydrate the per-instance task list from the terminal instance's stored
/// JSONB. The union projection embeds the archived task-instance records, so
/// this deserializes the union and maps its task-instance rows onto the list
/// row — the same set, at the same fidelity, the instance-detail read renders.
pub fn archived_task_instances_from_json(
    instance_json: serde_json::Value,
) -> Result<Vec<ip::ArchivedTaskInstance>> {
    let union = archive_projection_from_json(instance_json)?;
    let workflow_instance_id = union.id;
    let workflow_id = union.workflow_id;
    Ok(union
        .task_instances
        .into_iter()
        .map(|ti| ip::ArchivedTaskInstance {
            id: ti.id,
            task_id: ti.task_id,
            workflow_instance_id: workflow_instance_id.clone(),
            workflow_id: workflow_id.clone(),
            name: ti.name,
            task_type: ti.task_type,
            state: ti.state,
            executor_id: ti.executor_id,
            attempt: ti.attempt,
        })
        .collect())
}

/// Stamp an archive-grade projection with the read-time `storage` indicator to
/// obtain the instance-detail response snapshot — carrying every render field
/// verbatim and adding the `storage` the caller supplies (the two types share
/// one content mold and one set of sub-messages).
pub fn snapshot_from_archived(a: ip::ArchivedInstance, storage: &str) -> ip::InstanceSnapshot {
    ip::InstanceSnapshot {
        id: a.id,
        workflow_id: a.workflow_id,
        name: a.name,
        workflow_name: a.workflow_name,
        workflow_version: a.workflow_version,
        state: a.state,
        scheduled_at: a.scheduled_at,
        triggered_at: a.triggered_at,
        started_at: a.started_at,
        completed_at: a.completed_at,
        transitions: a.transitions,
        triggered_by: a.triggered_by,
        tags: a.tags,
        storage: storage.to_string(),
        task_count: a.task_count,
        completed_tasks: a.completed_tasks,
        tasks: a.tasks,
        task_instances: a.task_instances,
        graph: a.graph,
        routing_variables: a.routing_variables,
        version: a.version,
        applied_patches: a.applied_patches,
        version_snapshots: a.version_snapshots,
    }
}

/// Sort a node-id set into a deterministic string vector for the wire.
pub fn sorted_ids(ids: HashSet<Uuid>) -> Vec<String> {
    let mut v: Vec<String> = ids.iter().map(Uuid::to_string).collect();
    v.sort();
    v
}

/// Assemble the union from a render snapshot proto, the embedded runnable
/// section, the per-version graph history, and the carried-forward set. Shared
/// by the aggregate egress path and the JSON reconstruction path so both emit
/// one shape.
pub fn assemble_union(
    snap: &ip::InstanceSnapshot,
    runnable: rp::RunnableProjection,
    graph_snapshots: HashMap<u32, rp::RunnableGraph>,
    pre_grounded: Vec<String>,
) -> ap::ArchiveProjection {
    ap::ArchiveProjection {
        // The runnable graph/tasks section — task specs at run fidelity plus the
        // runnable graph with per-node/per-gate runtime overlays.
        runnable: Some(runnable),

        // Render metadata, carried verbatim from the render projection.
        id: snap.id.clone(),
        workflow_id: snap.workflow_id.clone(),
        name: snap.name.clone(),
        workflow_name: snap.workflow_name.clone(),
        workflow_version: snap.workflow_version,
        state: snap.state.clone(),
        scheduled_at: snap.scheduled_at.clone(),
        transitions: snap.transitions.clone(),
        triggered_by: snap.triggered_by.clone(),
        tags: snap.tags.clone(),
        routing_variables: snap.routing_variables.clone(),
        version: snap.version,
        applied_patches: snap.applied_patches.clone(),

        // Per-version graph history carried as the runnable-graph proto (not the
        // render `SnapshotGraph`), so each past version's graph reconstructs at
        // run fidelity — the archived removed-structure / ghost overlay renders
        // unchanged off it.
        graph_snapshots,

        // The archived task-instance records at the fidelity the instance-detail
        // task list renders — the full record, not the lossy list row (the union
        // is the full field set, not a merge of two lossy views).
        task_instances: snap.task_instances.clone(),

        // Carried-forward HyperNode IDs used to derive per-node replay state
        // and completed-run counts.
        pre_grounded,
    }
}
