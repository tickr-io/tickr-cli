//! Workflow-definition graph operations over the published `tickr.workflow`
//! protobuf contract.
//!
//! The structural seal synthesizes start/end wiring, closes orphan edges, and
//! preserves loop strongly-connected components. The implementation depends
//! only on the protobuf graph and UUID generation.

use std::collections::{HashMap, HashSet, VecDeque};

use tickr_proto::workflow as wf;
use uuid::Uuid;

/// The definition's task-id set paired with the protobuf `TaskGraph` sealed by
/// this module. Task IDs form the iteration domain; sentinel nodes do not.
pub struct ProtoDefinitionGraph {
    /// Ids of the definition's tasks (the sentinel start/end nodes are not
    /// members). Sealing wires orphans among these against start/end.
    task_ids: Vec<String>,
    /// The proto task graph the seal mutates in place.
    graph: wf::TaskGraph,
}

impl ProtoDefinitionGraph {
    /// Build the model from a published workflow definition: the task-id set is
    /// read off `def.tasks`, and the graph is cloned so sealing does not mutate
    /// the caller's definition.
    pub fn from_definition(def: &wf::WorkflowDefinition) -> Self {
        let task_ids = def.tasks.iter().map(|t| t.id.clone()).collect();
        let graph = def.task_graph.clone().unwrap_or_default();
        Self { task_ids, graph }
    }

    /// Read-only view of the (possibly sealed) proto task graph.
    pub fn graph(&self) -> &wf::TaskGraph {
        &self.graph
    }

    /// Consume the model, yielding the sealed proto task graph.
    pub fn into_graph(self) -> wf::TaskGraph {
        self.graph
    }

    /// Close the graph's orphans against start/end while respecting loop SCCs.
    ///
    /// A task with no non-loop incoming edge is wired from `start`; a task with
    /// no non-loop outgoing edge is wired to `end`. Orphan determination
    /// ignores `kind = loop` back-edges, so a single-node `mkLoop` task — whose
    /// self-edge would otherwise mask it as already having an in/out-edge — is
    /// still wired to both ends (its exit to End is an explicit gated edge, not
    /// a seal artifact).
    ///
    /// A loop body is entered and exited **as a unit**. A loop-SCC member is
    /// start-wired only when no member already has a non-loop incoming edge (the
    /// author's entry into the head, or an upstream task). Likewise, members
    /// are end-wired only when no member has an authored non-loop exit. The
    /// runtime's SCC teardown grounds all parked siblings when the producer
    /// emits a terminal `loop_control`, so adding a plain sibling→end edge beside
    /// a producer-only gated exit would create a spurious, non-terminable exit.
    /// A single-node self-loop is an SCC of one, so the same aggregate rules
    /// preserve its ordinary start/exit sealing.
    pub fn seal(&mut self) {
        let start = self.graph.start.clone();
        let end = self.graph.end.clone();
        let task_ids = self.task_ids.clone();

        // Loop bodies snapshot: computed once over the original loop-edge
        // subgraph. Adding start→member Control edges introduces no loop edges,
        // so the SCC membership stays valid across the wiring pass below.
        let loop_bodies = loop_sccs(&self.graph);

        // Tasks with no (non-loop) incoming edges — wire from start, unless the
        // task sits in a loop body that already has an entry. The incoming
        // check reads the *live* graph, so wiring the first orphan member of an
        // entry-less body gives that body an entry and its siblings are then
        // skipped (matching the aggregate's in-place mutation during sealing).
        for tid in &task_ids {
            if has_non_loop_incoming(&self.graph, tid) {
                continue;
            }
            let in_entered_body = loop_bodies
                .iter()
                .find(|body| body.contains(tid))
                .is_some_and(|body| body.iter().any(|m| has_non_loop_incoming(&self.graph, m)));
            if in_entered_body {
                continue;
            }
            self.add_seal_edge(&start, tid);
        }

        // Snapshot loop bodies that already have an authored exit before
        // adding any seal edge. Evaluating this live inside the loop would make
        // the first synthesized member→end edge look like an authored body
        // exit and incorrectly suppress sealing the remaining members.
        let exiting_loop_members: HashSet<String> = loop_bodies
            .iter()
            .filter(|body| has_non_loop_edge_leaving_body(&self.graph, body))
            .flat_map(|body| body.iter().cloned())
            .collect();

        // Tasks with no (non-loop) outgoing edges — wire to end. An authored
        // exit from any member closes its whole loop body because loop teardown
        // grounds the SCC as a unit; do not synthesize competing sibling exits.
        for tid in &task_ids {
            if !has_non_loop_outgoing(&self.graph, tid) && !exiting_loop_members.contains(tid) {
                self.add_seal_edge(tid, &end);
            }
        }
    }

    /// Append one synthesized plain-dependency (`Control`, un-gated) edge. The
    /// id is fresh; seal edges carry no author-declared gates.
    fn add_seal_edge(&mut self, source: &str, target: &str) {
        self.graph.edges.push(wf::Edge {
            id: Uuid::new_v4().to_string(),
            sources: vec![source.to_string()],
            targets: vec![target.to_string()],
            kind: wf::EdgeKind::Control as i32,
            gates: Vec::new(),
        });
    }
}

/// True iff `edge` is a `kind = loop` back-edge on the proto contract.
fn is_loop_edge(edge: &wf::Edge) -> bool {
    edge.kind == wf::EdgeKind::Loop as i32
}

/// True iff some non-loop edge targets `tid` — the orphan-source predicate,
/// blind to `kind = loop` back-edges.
fn has_non_loop_incoming(graph: &wf::TaskGraph, tid: &str) -> bool {
    graph
        .edges
        .iter()
        .any(|e| !is_loop_edge(e) && e.targets.iter().any(|t| t == tid))
}

/// True iff some non-loop edge sources from `tid` — the orphan-sink predicate,
/// blind to `kind = loop` back-edges.
fn has_non_loop_outgoing(graph: &wf::TaskGraph, tid: &str) -> bool {
    graph
        .edges
        .iter()
        .any(|e| !is_loop_edge(e) && e.sources.iter().any(|s| s == tid))
}

/// True iff a non-loop edge has a source in `body` and a target outside it.
fn has_non_loop_edge_leaving_body(graph: &wf::TaskGraph, body: &HashSet<String>) -> bool {
    graph.edges.iter().any(|edge| {
        !is_loop_edge(edge)
            && edge.sources.iter().any(|source| body.contains(source))
            && edge.targets.iter().any(|target| !body.contains(target))
    })
}

/// Adjacency over `kind = loop` edges only: `source → target` for every loop
/// edge. Forward (`control` / `data`) edges are invisible — a loop body is a
/// back-edge structure.
fn loop_adjacency(graph: &wf::TaskGraph) -> HashMap<String, Vec<String>> {
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &graph.edges {
        if !is_loop_edge(edge) {
            continue;
        }
        for src in &edge.sources {
            for tgt in &edge.targets {
                adj.entry(src.clone()).or_default().push(tgt.clone());
            }
        }
    }
    adj
}

/// True iff some `kind = loop` edge is a self-edge on `node`. A single-node SCC
/// is a loop body only when it carries such a self-edge.
fn has_loop_self_edge(graph: &wf::TaskGraph, node: &str) -> bool {
    graph.edges.iter().any(|e| {
        is_loop_edge(e)
            && e.sources.iter().any(|s| s == node)
            && e.targets.iter().any(|t| t == node)
    })
}

/// Derive the workflow's **loop bodies**: the strongly-connected components
/// over the `kind = loop` subgraph that are actual cycles — size > 1, or a
/// single node with a `kind = loop` self-edge. Trivial singletons (no loop
/// edge) are excluded, so a pure DAG returns an empty vector.
///
/// Tarjan's SCC, iterated rather than recursed so a pathological chain cannot
/// exhaust the stack. Components are keyed on `kind = loop` edges, not graph
/// cycles in general.
pub(crate) fn loop_sccs(graph: &wf::TaskGraph) -> Vec<HashSet<String>> {
    let adj = loop_adjacency(graph);

    let mut index_of: HashMap<String, usize> = HashMap::new();
    let mut lowlink: HashMap<String, usize> = HashMap::new();
    let mut on_stack: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = Vec::new();
    let mut next_index = 0usize;
    let mut sccs: Vec<HashSet<String>> = Vec::new();

    // Every node participating in the loop subgraph (as a source or a target of
    // a loop edge) is a candidate root. Sorted for deterministic output.
    let mut nodes: Vec<String> = adj
        .keys()
        .cloned()
        .chain(adj.values().flatten().cloned())
        .collect();
    nodes.sort();
    nodes.dedup();

    struct Frame {
        node: String,
        next_child: usize,
    }

    for root in &nodes {
        if index_of.contains_key(root) {
            continue;
        }
        let mut frames: Vec<Frame> = vec![Frame {
            node: root.clone(),
            next_child: 0,
        }];
        index_of.insert(root.clone(), next_index);
        lowlink.insert(root.clone(), next_index);
        next_index += 1;
        stack.push(root.clone());
        on_stack.insert(root.clone());

        while let Some(frame) = frames.last_mut() {
            let node = frame.node.clone();
            let empty = Vec::new();
            let children = adj.get(&node).unwrap_or(&empty);
            if frame.next_child < children.len() {
                let child = children[frame.next_child].clone();
                frame.next_child += 1;
                match index_of.get(&child).copied() {
                    None => {
                        index_of.insert(child.clone(), next_index);
                        lowlink.insert(child.clone(), next_index);
                        next_index += 1;
                        stack.push(child.clone());
                        on_stack.insert(child.clone());
                        frames.push(Frame {
                            node: child,
                            next_child: 0,
                        });
                    }
                    Some(child_index) if on_stack.contains(&child) => {
                        let cur = lowlink[&node];
                        lowlink.insert(node.clone(), cur.min(child_index));
                    }
                    Some(_) => {}
                }
            } else {
                // Done with `node`: if it is an SCC root, pop the component.
                if lowlink[&node] == index_of[&node] {
                    let mut component: HashSet<String> = HashSet::new();
                    loop {
                        let w = stack.pop().expect("tarjan stack non-empty at pop");
                        on_stack.remove(&w);
                        component.insert(w.clone());
                        if w == node {
                            break;
                        }
                    }
                    // Keep only cyclic components — the loop bodies.
                    let is_loop_body = component.len() > 1
                        || component
                            .iter()
                            .next()
                            .is_some_and(|n| has_loop_self_edge(graph, n));
                    if is_loop_body {
                        sccs.push(component);
                    }
                }
                frames.pop();
                if let Some(parent) = frames.last() {
                    let parent_node = parent.node.clone();
                    let child_low = lowlink[&node];
                    let cur = lowlink[&parent_node];
                    lowlink.insert(parent_node, cur.min(child_low));
                }
            }
        }
    }

    sccs
}

// ---------------------------------------------------------------------------
// Dominator validation
// ---------------------------------------------------------------------------

/// A failed dominator check over the protobuf graph. `bypass_path` is the
/// node-id trail from a graph source
/// (the `start` sentinel) to the task that did NOT traverse the gate's edge, so
/// a registration error can name the topology bug concretely. An empty
/// `bypass_path` means the gate's edge is absent from the graph (a wiring bug
/// surfaced as a violation) or the task is unreachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DominatorViolation {
    pub task_id: String,
    pub gate_edge_id: String,
    pub bypass_path: Vec<String>,
}

/// BFS `start → target` over `adjacency`, returning the discovered path when
/// the target is reachable (the bypass witness) or `None` when it is not. Pure
/// helper shared by both dominator checks; the caller shapes the adjacency to
/// encode "with the gate edge removed" or "not through the ancestor".
fn bfs_witness(
    adjacency: &HashMap<&str, Vec<&str>>,
    start: &str,
    target: &str,
) -> Option<Vec<String>> {
    let mut visited: HashSet<&str> = HashSet::new();
    let mut parent: HashMap<&str, &str> = HashMap::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    queue.push_back(start);
    visited.insert(start);

    let mut reached = false;
    while let Some(node) = queue.pop_front() {
        if node == target {
            reached = true;
            break;
        }
        if let Some(neighbors) = adjacency.get(node) {
            for &next in neighbors {
                if visited.insert(next) {
                    parent.insert(next, node);
                    queue.push_back(next);
                }
            }
        }
    }
    if !reached {
        return None;
    }
    let mut path = vec![target.to_string()];
    let mut cur = target;
    while let Some(&prev) = parent.get(cur) {
        path.push(prev.to_string());
        cur = prev;
    }
    path.reverse();
    Some(path)
}

/// Validate `gate_edge_id` dominates `task_id`: every path from `start` to the
/// task traverses the gate's edge. Computed directly — BFS in the graph with
/// the gate's edge removed; if the task is still reachable, that reachability
/// witness is the bypass path.
pub fn validate_dominates(
    graph: &wf::TaskGraph,
    task_id: &str,
    gate_edge_id: &str,
) -> Result<(), DominatorViolation> {
    // A missing gate edge is an upstream wiring bug, not a dominator concern;
    // surface it as a violation with an empty bypass so the call site notices.
    if !graph.edges.iter().any(|e| e.id == gate_edge_id) {
        return Err(DominatorViolation {
            task_id: task_id.to_string(),
            gate_edge_id: gate_edge_id.to_string(),
            bypass_path: Vec::new(),
        });
    }

    // Adjacency with the gate's hyperedge removed: any (source, target) pair
    // riding `gate_edge_id` is dropped.
    let mut adjacency_minus_gate: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &graph.edges {
        if edge.id == gate_edge_id {
            continue;
        }
        for src in &edge.sources {
            for tgt in &edge.targets {
                adjacency_minus_gate
                    .entry(src.as_str())
                    .or_default()
                    .push(tgt.as_str());
            }
        }
    }

    match bfs_witness(&adjacency_minus_gate, &graph.start, task_id) {
        // No path from start to the task without crossing the gate's edge — the
        // gate dominates. (Whether the task is reachable AT ALL via the gate is
        // a separate concern handled by sealing, not dominator analysis.)
        None => Ok(()),
        Some(bypass_path) => Err(DominatorViolation {
            task_id: task_id.to_string(),
            gate_edge_id: gate_edge_id.to_string(),
            bypass_path,
        }),
    }
}

/// Validate `ancestor_task` dominates `descendant_task`: every path from
/// `start` to the descendant traverses the ancestor node. Used by
/// predicate-gate resolution: the producer of a routing variable must dominate
/// every source of the consuming gate's edge so the variable is present when
/// the gate evaluates.
pub fn validate_task_dominates(
    graph: &wf::TaskGraph,
    ancestor_task: &str,
    descendant_task: &str,
) -> Result<(), DominatorViolation> {
    // Self-domination is valid: the runtime merges the producer's routing
    // variables before running its next tasks, so a gate on the producer's own
    // outgoing edge sees the variable at dispatch.
    if ancestor_task == descendant_task {
        return Ok(());
    }

    // Adjacency that refuses to traverse THROUGH the ancestor: the ancestor's
    // outgoing edges are dropped. If the descendant is still reachable, that
    // path bypasses the ancestor and the ancestor does not dominate.
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &graph.edges {
        for src in &edge.sources {
            if src == ancestor_task {
                continue;
            }
            for tgt in &edge.targets {
                adjacency
                    .entry(src.as_str())
                    .or_default()
                    .push(tgt.as_str());
            }
        }
    }

    match bfs_witness(&adjacency, &graph.start, descendant_task) {
        None => Ok(()),
        Some(bypass_path) => Err(DominatorViolation {
            task_id: descendant_task.to_string(),
            // Predicate-dominator callers use `gate_edge_id` to report the
            // ancestor task for this check.
            gate_edge_id: ancestor_task.to_string(),
            bypass_path,
        }),
    }
}

// ---------------------------------------------------------------------------
// Loop-terminability validation
// ---------------------------------------------------------------------------

/// The reserved routing variable every loop-exit edge must reference. The DSL
/// contract owns the reserved value set (`continue` / `done` / `fail`).
const LOOP_CONTROL_VAR: &str = "loop_control";

/// Why a loop body failed the terminability check. The `scc` echoes the
/// offending loop body (sorted node ids) so a registration error can name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoopTerminabilityViolation {
    /// The loop body has no edge leaving it — it can never be exited.
    NoExit { scc: Vec<String> },
    /// A leaving edge exists but is not gated referencing `loop_control`.
    UngatedExit { scc: Vec<String>, edge_id: String },
}

/// True iff `gate` is a predicate gate referencing the reserved `loop_control`
/// routing variable. The value/op are intentionally not checked — this only
/// asserts the exit is `loop_control`-routed.
fn references_loop_control(gate: &wf::Gate) -> bool {
    matches!(
        &gate.kind,
        Some(wf::gate::Kind::PredicateHolds(ph)) if ph.routing_var == LOOP_CONTROL_VAR
    )
}

/// Validate every loop body (the `kind = loop` SCC) in `graph` is terminable:
/// each edge leaving the body is gated referencing `loop_control`, and at least
/// one leaving edge exists. A graph with no loop body passes.
pub fn validate_loop_terminability(
    graph: &wf::TaskGraph,
) -> Result<(), LoopTerminabilityViolation> {
    for scc in loop_sccs(graph) {
        // An edge leaves the body when it has a source inside and a target
        // outside. The `kind = loop` back-edges stay inside (their targets are
        // all in-SCC), so they are never "leaving".
        let mut leaving: Vec<&wf::Edge> = Vec::new();
        for edge in &graph.edges {
            let from_inside = edge.sources.iter().any(|s| scc.contains(s));
            let to_outside = edge.targets.iter().any(|t| !scc.contains(t));
            if from_inside && to_outside {
                leaving.push(edge);
            }
        }

        let mut scc_vec: Vec<String> = scc.into_iter().collect();
        scc_vec.sort();

        if leaving.is_empty() {
            return Err(LoopTerminabilityViolation::NoExit { scc: scc_vec });
        }
        for edge in leaving {
            if !edge.gates.iter().any(references_loop_control) {
                return Err(LoopTerminabilityViolation::UngatedExit {
                    scc: scc_vec,
                    edge_id: edge.id.clone(),
                });
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Signal-input-source resolution
// ---------------------------------------------------------------------------

/// A resolved `from.signal` slot: the declaring task, the input slot index, and
/// the gate-bearing edge whose signal dominates the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSignalSource {
    pub task_id: String,
    pub slot: usize,
    pub gate_edge_id: String,
}

/// Why signal-input-source resolution failed for a task's slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalResolutionError {
    /// No gate carrying the signal dominates the task.
    Unresolved {
        task_id: String,
        signal_name: String,
    },
    /// More than one dominating gate carries the signal — the reference is
    /// structurally ambiguous.
    Ambiguous {
        task_id: String,
        signal_name: String,
        count: usize,
    },
}

/// True iff `gate_edge_id` is the unresolved sentinel — the nil UUID the parser
/// stamps before resolution (an empty or unparseable string counts too).
fn is_unresolved(gate_edge_id: &str) -> bool {
    Uuid::parse_str(gate_edge_id)
        .map(|u| u.is_nil())
        .unwrap_or(true)
}

/// Resolve every unresolved `InputSource::Signal` slot across `tasks` against
/// the (sealed) `graph`: find the unique gate-bearing edge whose `signal_name`
/// matches AND which dominates the declaring task. Zero matches or multiple
/// matches are authoring errors naming the task and signal. The dominator check
/// makes enqueue-time signal-input stamping total. The graph
/// must already be sealed (dominance is judged from the `start` sentinel).
pub fn resolve_signal_input_sources(
    tasks: &[wf::TaskDefinition],
    graph: &wf::TaskGraph,
) -> Result<Vec<ResolvedSignalSource>, SignalResolutionError> {
    let mut resolved = Vec::new();
    for task in tasks {
        let Some(list) = &task.input_sources else {
            continue;
        };
        for (slot, opt) in list.sources.iter().enumerate() {
            let Some(source) = &opt.source else {
                continue;
            };
            let Some(wf::input_source::Source::Signal(sig)) = &source.source else {
                continue;
            };
            if !is_unresolved(&sig.gate_edge_id) {
                continue; // already resolved (defense-in-depth pass)
            }
            // Every edge whose gates include a `SignalReceived` matching this
            // signal name; the dominator check must pick exactly one.
            let mut dominating: Vec<String> = Vec::new();
            for edge in &graph.edges {
                let carries_signal = edge.gates.iter().any(|g| match &g.kind {
                    Some(wf::gate::Kind::SignalReceived(sr)) => sr.signal_name == sig.signal_name,
                    Some(wf::gate::Kind::PredicateHolds(_))
                    | Some(wf::gate::Kind::TimerElapsed(_))
                    | None => false,
                });
                if !carries_signal {
                    continue;
                }
                if validate_dominates(graph, &task.id, &edge.id).is_ok() {
                    dominating.push(edge.id.clone());
                }
            }
            match dominating.len() {
                0 => {
                    return Err(SignalResolutionError::Unresolved {
                        task_id: task.id.clone(),
                        signal_name: sig.signal_name.clone(),
                    });
                }
                1 => resolved.push(ResolvedSignalSource {
                    task_id: task.id.clone(),
                    slot,
                    gate_edge_id: dominating.remove(0),
                }),
                n => {
                    return Err(SignalResolutionError::Ambiguous {
                        task_id: task.id.clone(),
                        signal_name: sig.signal_name.clone(),
                        count: n,
                    });
                }
            }
        }
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests build unsealed protobuf definitions directly, run the graph
    // operations, and compare their results with hand-specified expectations.

    const CONTROL: i32 = wf::EdgeKind::Control as i32;
    const DATA: i32 = wf::EdgeKind::Data as i32;
    const LOOP: i32 = wf::EdgeKind::Loop as i32;

    /// A fresh (start, end) sentinel id pair.
    fn sentinels() -> (String, String) {
        (Uuid::new_v4().to_string(), Uuid::new_v4().to_string())
    }

    /// A minimal regular task definition with the given id and name.
    fn task_def(id: Uuid, name: &str) -> wf::TaskDefinition {
        wf::TaskDefinition {
            id: id.to_string(),
            workflow_id: Uuid::new_v4().to_string(),
            name: name.to_string(),
            task_type: wf::TaskType::Regular as i32,
            nix_expression_path: "/nix/x".to_string(),
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
        }
    }

    /// One hyperedge of the given kind over the source/target id clusters.
    fn edge(
        kind: i32,
        sources: Vec<String>,
        targets: Vec<String>,
        gates: Vec<wf::Gate>,
    ) -> wf::Edge {
        wf::Edge {
            id: Uuid::new_v4().to_string(),
            sources,
            targets,
            kind,
            gates,
        }
    }

    /// Assemble an unsealed workflow definition: the given tasks and author
    /// edges over a graph carrying start/end sentinels plus one Task node per
    /// task.
    fn definition(
        start: &str,
        end: &str,
        tasks: Vec<wf::TaskDefinition>,
        edges: Vec<wf::Edge>,
    ) -> wf::WorkflowDefinition {
        let mut nodes = vec![
            wf::GraphNode {
                id: start.to_string(),
                node_type: wf::NodeType::Start as i32,
            },
            wf::GraphNode {
                id: end.to_string(),
                node_type: wf::NodeType::End as i32,
            },
        ];
        for t in &tasks {
            nodes.push(wf::GraphNode {
                id: t.id.clone(),
                node_type: wf::NodeType::Task as i32,
            });
        }
        wf::WorkflowDefinition {
            id: Uuid::new_v4().to_string(),
            tenant_id: String::new(),
            namespace: String::new(),
            slug: String::new(),
            name: String::new(),
            version: 0,
            tasks,
            task_graph: Some(wf::TaskGraph {
                nodes,
                edges,
                start: start.to_string(),
                end: end.to_string(),
            }),
            trigger: None,
            status: wf::WorkflowStatus::Inactive as i32,
            captures: Vec::new(),
            timeout_secs: None,
            tags: HashMap::new(),
        }
    }

    /// Run the conductor-local seal over a definition, returning the sealed
    /// proto graph.
    fn seal(def: &wf::WorkflowDefinition) -> wf::TaskGraph {
        let mut model = ProtoDefinitionGraph::from_definition(def);
        model.seal();
        model.into_graph()
    }

    /// True iff the sealed graph carries a synthesized `Control`, un-gated
    /// closure edge `start → target`.
    fn start_wires(g: &wf::TaskGraph, target: Uuid) -> bool {
        g.edges.iter().any(|e| {
            e.kind == CONTROL
                && e.gates.is_empty()
                && e.sources == vec![g.start.clone()]
                && e.targets == vec![target.to_string()]
        })
    }

    /// True iff the sealed graph carries a synthesized `Control`, un-gated
    /// closure edge `source → end`.
    fn end_wires(g: &wf::TaskGraph, source: Uuid) -> bool {
        g.edges.iter().any(|e| {
            e.kind == CONTROL
                && e.gates.is_empty()
                && e.sources == vec![source.to_string()]
                && e.targets == vec![g.end.clone()]
        })
    }

    /// A `SignalReceived` gate declaration carrying `name`.
    fn signal_gate(name: &str) -> wf::Gate {
        wf::Gate {
            kind: Some(wf::gate::Kind::SignalReceived(wf::gate::SignalReceived {
                signal_name: name.to_string(),
                predicate: None,
                captures_spec: Vec::new(),
                timeout: None,
            })),
        }
    }

    /// A `PredicateHolds` gate declaration over `var == value` (string).
    fn predicate_gate(var: &str, value: &str) -> wf::Gate {
        wf::Gate {
            kind: Some(wf::gate::Kind::PredicateHolds(wf::gate::PredicateHolds {
                routing_var: var.to_string(),
                op: wf::ComparisonOp::Eq as i32,
                value: Some(wf::RoutingValue {
                    value: Some(wf::routing_value::Value::StringValue(value.to_string())),
                }),
                timeout: None,
            })),
        }
    }

    /// A one-slot `input_sources` carrying an unresolved `from.signal` slot.
    fn signal_input(signal_name: &str) -> wf::InputSourceList {
        wf::InputSourceList {
            sources: vec![wf::OptionalInputSource {
                source: Some(wf::InputSource {
                    source: Some(wf::input_source::Source::Signal(wf::input_source::Signal {
                        signal_name: signal_name.to_string(),
                        gate_edge_id: Uuid::nil().to_string(),
                    })),
                }),
            }],
        }
    }

    // -----------------------------------------------------------------------
    // Seal closure: the sealed graph must add exactly the documented closure
    // edges (start → orphan-source, orphan-sink → end), asserted structurally.
    // -----------------------------------------------------------------------

    /// Two tasks joined by an explicit forward edge, both ends open. Documented
    /// seal: wire start→a and b→end.
    fn linear_orphans() -> (wf::WorkflowDefinition, Uuid, Uuid) {
        let (start, end) = sentinels();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let def = definition(
            &start,
            &end,
            vec![task_def(a, "a"), task_def(b, "b")],
            vec![edge(
                CONTROL,
                vec![a.to_string()],
                vec![b.to_string()],
                vec![],
            )],
        );
        (def, a, b)
    }

    fn assert_seal_linear() {
        let (def, a, b) = linear_orphans();
        let g = seal(&def);
        assert!(start_wires(&g, a), "start→a must be wired");
        assert!(end_wires(&g, b), "b→end must be wired");
        assert!(
            !start_wires(&g, b),
            "b has an incoming edge, not start-wired"
        );
        assert!(!end_wires(&g, a), "a has an outgoing edge, not end-wired");
    }

    #[test]
    fn seal_closes_linear_orphans() {
        assert_seal_linear();
    }

    /// Three fully-disconnected tasks. Documented seal: each wired start→x and
    /// x→end.
    fn fully_disconnected() -> (wf::WorkflowDefinition, Uuid, Uuid, Uuid) {
        let (start, end) = sentinels();
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let def = definition(
            &start,
            &end,
            vec![task_def(a, "a"), task_def(b, "b"), task_def(c, "c")],
            Vec::new(),
        );
        (def, a, b, c)
    }

    fn assert_seal_fully_disconnected() {
        let (def, a, b, c) = fully_disconnected();
        let g = seal(&def);
        for x in [a, b, c] {
            assert!(start_wires(&g, x), "each disconnected task is start-wired");
            assert!(end_wires(&g, x), "each disconnected task is end-wired");
        }
    }

    #[test]
    fn seal_closes_fully_disconnected() {
        assert_seal_fully_disconnected();
    }

    /// A single task carrying a `kind = loop` self-edge. Orphan detection
    /// ignores the loop self-edge, so the seal still wires start→L and L→end
    /// (the entry-less single-node SCC branch).
    fn single_node_self_loop() -> (wf::WorkflowDefinition, Uuid) {
        let (start, end) = sentinels();
        let l = Uuid::new_v4();
        let def = definition(
            &start,
            &end,
            vec![task_def(l, "l")],
            vec![edge(LOOP, vec![l.to_string()], vec![l.to_string()], vec![])],
        );
        (def, l)
    }

    fn assert_seal_single_node_self_loop() {
        let (def, l) = single_node_self_loop();
        let g = seal(&def);
        assert!(start_wires(&g, l), "loop-only incoming → start-wired");
        assert!(end_wires(&g, l), "loop-only outgoing → end-wired");
    }

    #[test]
    fn seal_closes_single_node_self_loop() {
        assert_seal_single_node_self_loop();
    }

    /// A three-task `kind = loop` ring (a→b→c→a, all loop edges) with an
    /// explicit forward entry `d→a`. The ring head `a` has a non-loop incoming
    /// edge, so the whole body is "entered": no ring member is start-wired
    /// (only the driver `d` is), and every ring member reaches end.
    fn loop_ring_with_entry() -> (wf::WorkflowDefinition, Uuid, Uuid, Uuid, Uuid) {
        let (start, end) = sentinels();
        let (d, a, b, c) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let def = definition(
            &start,
            &end,
            vec![
                task_def(d, "d"),
                task_def(a, "a"),
                task_def(b, "b"),
                task_def(c, "c"),
            ],
            vec![
                edge(CONTROL, vec![d.to_string()], vec![a.to_string()], vec![]),
                edge(LOOP, vec![a.to_string()], vec![b.to_string()], vec![]),
                edge(LOOP, vec![b.to_string()], vec![c.to_string()], vec![]),
                edge(LOOP, vec![c.to_string()], vec![a.to_string()], vec![]),
            ],
        );
        (def, d, a, b, c)
    }

    fn assert_seal_loop_ring_with_entry() {
        let (def, d, a, b, c) = loop_ring_with_entry();
        let g = seal(&def);
        // Only the driver is start-wired; the entered ring body is not.
        assert!(start_wires(&g, d), "driver d is start-wired");
        for m in [a, b, c] {
            assert!(
                !start_wires(&g, m),
                "entered ring member is not start-wired"
            );
            assert!(end_wires(&g, m), "each ring member reaches end");
        }
        assert!(!end_wires(&g, d), "d has an outgoing edge, not end-wired");
    }

    #[test]
    fn seal_closes_loop_ring_with_entry() {
        assert_seal_loop_ring_with_entry();
    }

    #[test]
    fn seal_does_not_add_sibling_exit_when_loop_body_has_producer_exit() {
        let (start, end) = sentinels();
        let (head, producer, downstream) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let def = definition(
            &start,
            &end,
            vec![
                task_def(head, "head"),
                task_def(producer, "producer"),
                task_def(downstream, "downstream"),
            ],
            vec![
                edge(CONTROL, vec![start.clone()], vec![head.to_string()], vec![]),
                edge(
                    LOOP,
                    vec![head.to_string()],
                    vec![producer.to_string()],
                    vec![predicate_gate("loop_control", "continue")],
                ),
                edge(
                    LOOP,
                    vec![producer.to_string()],
                    vec![head.to_string()],
                    vec![predicate_gate("loop_control", "continue")],
                ),
                edge(
                    DATA,
                    vec![producer.to_string()],
                    vec![downstream.to_string()],
                    vec![predicate_gate("loop_control", "done")],
                ),
            ],
        );

        let graph = seal(&def);
        assert!(
            !end_wires(&graph, head),
            "the producer's authored exit closes the loop body; the seal must not add a competing head → End edge"
        );
        assert!(
            !end_wires(&graph, producer),
            "the producer already has an authored exit"
        );
        assert!(
            end_wires(&graph, downstream),
            "ordinary downstream orphan sealing remains unchanged"
        );
    }

    /// A fan-in: two orphan-source tasks both feeding one sink via a single
    /// hyperedge. Documented seal: start→a, start→b, and sink→end.
    fn fan_in_hyperedge() -> (wf::WorkflowDefinition, Uuid, Uuid, Uuid) {
        let (start, end) = sentinels();
        let (a, b, sink) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let def = definition(
            &start,
            &end,
            vec![task_def(a, "a"), task_def(b, "b"), task_def(sink, "sink")],
            vec![edge(
                CONTROL,
                vec![a.to_string(), b.to_string()],
                vec![sink.to_string()],
                vec![],
            )],
        );
        (def, a, b, sink)
    }

    fn assert_seal_fan_in_hyperedge() {
        let (def, a, b, sink) = fan_in_hyperedge();
        let g = seal(&def);
        assert!(start_wires(&g, a), "orphan source a is start-wired");
        assert!(start_wires(&g, b), "orphan source b is start-wired");
        assert!(end_wires(&g, sink), "orphan sink is end-wired");
        assert!(!start_wires(&g, sink), "sink has an incoming edge");
        assert!(
            !end_wires(&g, a) && !end_wires(&g, b),
            "sources have outgoing"
        );
    }

    #[test]
    fn seal_closes_fan_in_hyperedge() {
        assert_seal_fan_in_hyperedge();
    }

    /// Every gate construct on one workflow, left unsealed with an orphan at
    /// each end: a `PredicateHolds` loop self-edge on `poll`, and a gated `Data`
    /// exit `poll→notify` carrying a `SignalReceived` gate with captures.
    /// Documented seal: wire start→poll (loop-only incoming) and notify→end
    /// (orphan sink).
    fn rich_every_construct() -> (wf::WorkflowDefinition, Uuid, Uuid) {
        let (start, end) = sentinels();
        let (poll, notify) = (Uuid::new_v4(), Uuid::new_v4());

        let mut poll_task = task_def(poll, "poll");
        poll_task.nix_args = vec!["--flag".to_string()];
        poll_task.outputs = vec!["status".to_string()];
        poll_task.inputs = vec!["seed".to_string(), "prev".to_string()];
        poll_task.secrets = vec!["token".to_string()];
        poll_task.max_attempts = 5;
        poll_task.timeout_secs = Some(120);
        poll_task.routing_vars = vec![wf::RoutingVarDecl {
            name: "decision".to_string(),
            var_type: Some("string".to_string()),
        }];
        poll_task.emits = vec![wf::TaskSignalEmit {
            emit: Some(wf::task_signal_emit::Emit::OnFailure(
                wf::task_signal_emit::OnFailure {
                    signal_name: "poll-failed".to_string(),
                },
            )),
        }];
        poll_task.loop_participant = true;

        let mut notify_task = task_def(notify, "notify");
        notify_task.task_type = wf::TaskType::Shadow as i32;
        notify_task.nix_expression_path = "/nix/notify".to_string();

        let routing_gate = wf::Gate {
            kind: Some(wf::gate::Kind::PredicateHolds(wf::gate::PredicateHolds {
                routing_var: "decision".to_string(),
                op: wf::ComparisonOp::NotEq as i32,
                value: Some(wf::RoutingValue {
                    value: Some(wf::routing_value::Value::StringValue("done".to_string())),
                }),
                timeout: Some(wf::Duration {
                    secs: 600,
                    nanos: 0,
                }),
            })),
        };
        let capture_gate = wf::Gate {
            kind: Some(wf::gate::Kind::SignalReceived(wf::gate::SignalReceived {
                signal_name: "approval".to_string(),
                predicate: Some("$[?@.ok]".to_string()),
                captures_spec: vec![wf::CaptureDeclaration {
                    name: "approver".to_string(),
                    from: Some(wf::CaptureSource {
                        source: Some(wf::capture_source::Source::Trigger(
                            wf::capture_source::Trigger {
                                jsonpath: "$.approver".to_string(),
                            },
                        )),
                    }),
                }],
                timeout: Some(wf::Duration {
                    secs: 86_400,
                    nanos: 0,
                }),
            })),
        };

        let def = definition(
            &start,
            &end,
            vec![poll_task, notify_task],
            vec![
                edge(
                    LOOP,
                    vec![poll.to_string()],
                    vec![poll.to_string()],
                    vec![routing_gate],
                ),
                edge(
                    DATA,
                    vec![poll.to_string()],
                    vec![notify.to_string()],
                    vec![capture_gate],
                ),
            ],
        );
        (def, poll, notify)
    }

    fn assert_seal_rich_every_construct() {
        let (def, poll, notify) = rich_every_construct();
        let g = seal(&def);
        assert!(
            start_wires(&g, poll),
            "poll (loop-only incoming) is start-wired"
        );
        assert!(end_wires(&g, notify), "notify (orphan sink) is end-wired");
        assert!(
            !start_wires(&g, notify),
            "notify has a non-loop incoming edge"
        );
        assert!(!end_wires(&g, poll), "poll has a non-loop outgoing edge");
    }

    #[test]
    fn seal_closes_rich_every_construct() {
        assert_seal_rich_every_construct();
    }

    /// The corpus in one shot, so a single failure names the whole engine
    /// rather than only one shape.
    #[test]
    fn seal_closes_the_whole_corpus() {
        assert_seal_linear();
        assert_seal_fully_disconnected();
        assert_seal_single_node_self_loop();
        assert_seal_loop_ring_with_entry();
        assert_seal_fan_in_hyperedge();
        assert_seal_rich_every_construct();
    }

    /// Sealing an all-orphan pair adds exactly two synthesized closure edges
    /// (start→a and b→end), both `Control` and un-gated.
    #[test]
    fn seal_closes_orphans_with_control_edges() {
        let (def, _a, _b) = linear_orphans();
        let before = def.task_graph.as_ref().unwrap().edges.len();
        let g = seal(&def);
        // One explicit edge + two synthesized closure edges.
        assert_eq!(g.edges.len(), before + 2);
        let synthesized: Vec<&wf::Edge> = g
            .edges
            .iter()
            .filter(|e| e.kind == CONTROL && e.gates.is_empty())
            .filter(|e| e.sources == [g.start.clone()] || e.targets == [g.end.clone()])
            .collect();
        assert_eq!(synthesized.len(), 2, "start→a and b→end were synthesized");
    }

    // -----------------------------------------------------------------------
    // Dominator validation: build graphs with known accept/reject expectations
    // and assert the conductor-local checks match them directly.
    // -----------------------------------------------------------------------

    /// `start → a → {b, c} → d`: `a` dominates `d`, neither branch does.
    fn diamond() -> (wf::WorkflowDefinition, Uuid, Uuid, Uuid, Uuid) {
        let (start, end) = sentinels();
        let (a, b, c, d) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let def = definition(
            &start,
            &end,
            vec![
                task_def(a, "a"),
                task_def(b, "b"),
                task_def(c, "c"),
                task_def(d, "d"),
            ],
            vec![
                edge(CONTROL, vec![start.clone()], vec![a.to_string()], vec![]),
                edge(CONTROL, vec![a.to_string()], vec![b.to_string()], vec![]),
                edge(CONTROL, vec![a.to_string()], vec![c.to_string()], vec![]),
                edge(CONTROL, vec![b.to_string()], vec![d.to_string()], vec![]),
                edge(CONTROL, vec![c.to_string()], vec![d.to_string()], vec![]),
            ],
        );
        (def, a, b, c, d)
    }

    /// Task-dominance: `a` dominates `d`; neither branch (`b`/`c`) does; a task
    /// dominates itself. The diamond bypass is the heart of dominator analysis.
    #[test]
    fn dominator_task_accepts_and_rejects() {
        let (def, a, b, c, d) = diamond();
        let g = seal(&def);
        assert!(
            validate_task_dominates(&g, &a.to_string(), &d.to_string()).is_ok(),
            "a dominates d (every path start→d crosses a)"
        );
        assert!(
            validate_task_dominates(&g, &b.to_string(), &d.to_string()).is_err(),
            "b does not dominate d (the c→d arm bypasses b)"
        );
        assert!(
            validate_task_dominates(&g, &c.to_string(), &d.to_string()).is_err(),
            "c does not dominate d (the b→d arm bypasses c)"
        );
        assert!(
            validate_task_dominates(&g, &a.to_string(), &b.to_string()).is_ok(),
            "a dominates b"
        );
        assert!(
            validate_task_dominates(&g, &a.to_string(), &a.to_string()).is_ok(),
            "self-domination holds"
        );
    }

    /// Edge-dominance: the `a→b` edge dominates `b` (the sole path to `b`
    /// crosses it); the `b→d` edge does NOT dominate `d` (the `c→d` arm
    /// bypasses it, and the bypass path is returned as the witness).
    #[test]
    fn dominator_edge_accepts_and_rejects() {
        let (def, a, b, _c, d) = diamond();
        let g = seal(&def);
        let ab = g
            .edges
            .iter()
            .find(|e| e.sources == vec![a.to_string()] && e.targets == vec![b.to_string()])
            .expect("a→b edge");
        assert!(
            validate_dominates(&g, &b.to_string(), &ab.id).is_ok(),
            "the a→b edge dominates b"
        );
        let bd = g
            .edges
            .iter()
            .find(|e| e.sources == vec![b.to_string()] && e.targets == vec![d.to_string()])
            .expect("b→d edge");
        let violation = validate_dominates(&g, &d.to_string(), &bd.id)
            .expect_err("the b→d edge does not dominate d");
        assert!(
            !violation.bypass_path.is_empty(),
            "a real bypass (start→a→c→d) is witnessed"
        );
    }

    /// A phantom edge id surfaces as a violation with an empty bypass path —
    /// the "missing edge is a wiring bug, not a dominator concern" branch.
    #[test]
    fn proto_dominator_direct_checks() {
        let (def, _l) = loop_terminable();
        let graph = seal(&def);
        let phantom = Uuid::new_v4().to_string();
        let some_task = graph
            .nodes
            .iter()
            .find(|n| n.node_type == wf::NodeType::Task as i32)
            .map(|n| n.id.clone())
            .unwrap();
        let violation = validate_dominates(&graph, &some_task, &phantom)
            .expect_err("a phantom edge must surface as a violation");
        assert!(violation.bypass_path.is_empty());
    }

    // -----------------------------------------------------------------------
    // Loop-terminability: known terminable/non-terminable shapes.
    // -----------------------------------------------------------------------

    /// A self-loop terminable via a `loop_control`-gated exit to End.
    fn loop_terminable() -> (wf::WorkflowDefinition, Uuid) {
        let (start, end) = sentinels();
        let l = Uuid::new_v4();
        let def = definition(
            &start,
            &end,
            vec![task_def(l, "l")],
            vec![
                edge(CONTROL, vec![start.clone()], vec![l.to_string()], vec![]),
                edge(
                    LOOP,
                    vec![l.to_string()],
                    vec![l.to_string()],
                    vec![predicate_gate("loop_control", "continue")],
                ),
                edge(
                    DATA,
                    vec![l.to_string()],
                    vec![end.clone()],
                    vec![predicate_gate("loop_control", "done")],
                ),
            ],
        );
        (def, l)
    }

    /// A self-loop whose only exit is gated on a non-`loop_control` variable —
    /// non-terminable.
    fn loop_ungated_exit() -> (wf::WorkflowDefinition, Uuid) {
        let (start, end) = sentinels();
        let l = Uuid::new_v4();
        let def = definition(
            &start,
            &end,
            vec![task_def(l, "l")],
            vec![
                edge(CONTROL, vec![start.clone()], vec![l.to_string()], vec![]),
                edge(
                    LOOP,
                    vec![l.to_string()],
                    vec![l.to_string()],
                    vec![predicate_gate("loop_control", "continue")],
                ),
                edge(
                    DATA,
                    vec![l.to_string()],
                    vec![end.clone()],
                    vec![predicate_gate("decision", "done")],
                ),
            ],
        );
        (def, l)
    }

    #[test]
    fn loop_terminability_accepts_terminable_and_dag() {
        // A terminable loop (loop_control-gated exit) is accepted.
        let (def, _l) = loop_terminable();
        assert!(validate_loop_terminability(&seal(&def)).is_ok());
        // A pure DAG carries no loop body, so it trivially passes.
        let (dag, ..) = linear_orphans();
        assert!(validate_loop_terminability(&seal(&dag)).is_ok());
    }

    #[test]
    fn loop_terminability_rejects_ungated_and_seal_closed() {
        // A loop whose exit is gated on a non-loop_control variable is rejected.
        let (ungated, _l) = loop_ungated_exit();
        assert!(validate_loop_terminability(&seal(&ungated)).is_err());
        // A single-node self-loop's only exit is the seal-added (un-gated)
        // start/end closure edge — non-terminable.
        let (single, _s) = single_node_self_loop();
        assert!(validate_loop_terminability(&seal(&single)).is_err());
        // A loop ring's members leave only via seal-added un-gated closure
        // edges to end — non-terminable.
        let (ring, ..) = loop_ring_with_entry();
        assert!(validate_loop_terminability(&seal(&ring)).is_err());
        // The rich construct's loop body exits via the (non-loop_control)
        // signal-gated Data edge — non-terminable.
        let (rich, ..) = rich_every_construct();
        assert!(validate_loop_terminability(&seal(&rich)).is_err());
    }

    // -----------------------------------------------------------------------
    // Signal-input-source resolution: resolves to the dominating gate edge, and
    // rejects the unresolved / ambiguous shapes.
    // -----------------------------------------------------------------------

    /// A single `from.signal` slot that resolves: the gate carrying `go` on the
    /// only edge into `b` dominates `b`.
    fn signal_resolves() -> (wf::WorkflowDefinition, Uuid, Uuid) {
        let (start, end) = sentinels();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let mut b_task = task_def(b, "b");
        b_task.inputs = vec!["seed".to_string()];
        b_task.input_sources = Some(signal_input("go"));
        let def = definition(
            &start,
            &end,
            vec![task_def(a, "a"), b_task],
            vec![
                edge(CONTROL, vec![start.clone()], vec![a.to_string()], vec![]),
                edge(
                    DATA,
                    vec![a.to_string()],
                    vec![b.to_string()],
                    vec![signal_gate("go")],
                ),
            ],
        );
        (def, a, b)
    }

    #[test]
    fn signal_resolution_resolves_dominating_gate() {
        let (def, a, b) = signal_resolves();
        let g = seal(&def);
        let resolved = resolve_signal_input_sources(&def.tasks, &g).expect("signal input resolves");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].task_id, b.to_string());
        assert_eq!(resolved[0].slot, 0);
        // The resolved edge is the a→b gate edge, and it dominates b.
        let ab = g
            .edges
            .iter()
            .find(|e| e.sources == vec![a.to_string()] && e.targets == vec![b.to_string()])
            .expect("a→b edge");
        assert_eq!(resolved[0].gate_edge_id, ab.id);
        assert!(validate_dominates(&g, &b.to_string(), &ab.id).is_ok());
    }

    #[test]
    fn signal_resolution_rejects_unresolved() {
        // `b` declares `from.signal = ghost`, but no gate carries `ghost`.
        let (start, end) = sentinels();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let mut b_task = task_def(b, "b");
        b_task.inputs = vec!["seed".to_string()];
        b_task.input_sources = Some(signal_input("ghost"));
        let def = definition(
            &start,
            &end,
            vec![task_def(a, "a"), b_task],
            vec![
                edge(CONTROL, vec![start.clone()], vec![a.to_string()], vec![]),
                edge(CONTROL, vec![a.to_string()], vec![b.to_string()], vec![]),
            ],
        );
        let g = seal(&def);
        assert_eq!(
            resolve_signal_input_sources(&def.tasks, &g),
            Err(SignalResolutionError::Unresolved {
                task_id: b.to_string(),
                signal_name: "ghost".to_string(),
            })
        );
    }

    #[test]
    fn signal_resolution_rejects_ambiguous() {
        // Two gates carry `go`, both dominating `b` (in series) — ambiguous.
        let (start, end) = sentinels();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let mut b_task = task_def(b, "b");
        b_task.inputs = vec!["seed".to_string()];
        b_task.input_sources = Some(signal_input("go"));
        let def = definition(
            &start,
            &end,
            vec![task_def(a, "a"), b_task],
            vec![
                edge(
                    DATA,
                    vec![start.clone()],
                    vec![a.to_string()],
                    vec![signal_gate("go")],
                ),
                edge(
                    DATA,
                    vec![a.to_string()],
                    vec![b.to_string()],
                    vec![signal_gate("go")],
                ),
            ],
        );
        let g = seal(&def);
        assert_eq!(
            resolve_signal_input_sources(&def.tasks, &g),
            Err(SignalResolutionError::Ambiguous {
                task_id: b.to_string(),
                signal_name: "go".to_string(),
                count: 2,
            })
        );
    }

    #[test]
    fn signal_resolution_trivial_when_no_slots() {
        // No signal slots at all — resolution is a trivial empty accept.
        let (def, ..) = linear_orphans();
        let g = seal(&def);
        assert_eq!(resolve_signal_input_sources(&def.tasks, &g), Ok(Vec::new()));
    }
}
