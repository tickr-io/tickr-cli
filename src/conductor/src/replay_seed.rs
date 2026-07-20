//! Replay-seed minting (conductor ingress).
//!
//! A replay seed is the self-contained carrier of a replay's seeded state.
//! It is minted **conductor-side from the source run's archive** and never
//! deserialized from client bytes on any ingress — so the run's trust
//! boundary is the archive, not the caller. This module is the single seam
//! that constructs one: a pure transform from `(archived instance, resume_from,
//! signal_id)` to a seed, testable in isolation.
//!
//! Three pure transforms live here, each the highest-value unit-test target
//! for its behaviour:
//!
//! - **Archived-graph read** — the replay materialises its graph by reading the
//!   source run's archived **final** graph (`task_instance_graph` in the
//!   archived instance blob) verbatim. No definition resolution, no patch-log
//!   replay, no rebuild-then-compare: there is nothing to re-derive and
//!   therefore nothing to diverge from. The only typed reject is a missing
//!   archived blob ([`ReplayReject::VersionUnresolvable`]).
//! - **Fireability validation** — two tiers. A typed reject fires only when a
//!   re-run **root** has no incident HyperEdge all of whose sources are
//!   `Grounded(Success)`. Doomed interior joins (a fan-in whose sibling arm
//!   died) are **enumerated**, not rejected — they faithfully reproduce the
//!   source run's own behaviour.
//! - **Seed mint** — assembles the deterministic id, the pre-grounded set, the
//!   verbatim-seeded graph (pre-grounded HyperNodes keep their archived
//!   `GroundKind`; the resume-from forward closure is reset to `Pending`;
//!   boundary gates are seeded `Idle`), and the archived task specs.

use std::collections::{HashMap, HashSet};

use crate::replay_pipeline::replay_instance_id;
use tickr_proto::runnable as rp;
use tickr_proto::signal as sp;
use tickr_proto::workflow as wf;
use uuid::Uuid;

/// The runnable graph's ground-state discriminants, named so the seeding logic
/// reads against the published projection's enum rather than magic `i32`s.
const GROUND_PENDING: i32 = rp::GroundState::Pending as i32;
const GROUND_SUCCESS: i32 = rp::GroundState::Success as i32;
const GROUND_FAILED: i32 = rp::GroundState::Failed as i32;
const NODE_TYPE_TASK: i32 = wf::NodeType::Task as i32;

/// Outgoing adjacency over the runnable graph's hyperedges: every source node
/// points at every target of an incident edge. Rebuilt here because the wire
/// projection carries the edge set only (the adjacency caches stay derived).
fn outgoing_adjacency(graph: &rp::RunnableGraph) -> HashMap<String, Vec<String>> {
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &graph.edges {
        for src in &edge.sources {
            for tgt in &edge.targets {
                adj.entry(src.clone()).or_default().push(tgt.clone());
            }
        }
    }
    adj
}

/// Find a node by its graph-slot id.
fn node_by_id<'a>(graph: &'a rp::RunnableGraph, id: &str) -> Option<&'a rp::RunnableNode> {
    graph.nodes.iter().find(|n| n.id == id)
}

/// A typed reason a replay cannot materialise. Surfaced to the caller (on the
/// real ingress) as a typed reject; the durable pipeline-row parking of the
/// outcome is carried by the archive-reader slice — this module raises the
/// check at materialisation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayReject {
    /// A re-run **root** (a HyperNode in the resume-from frontier) has no
    /// incident HyperEdge all of whose sources are `Grounded(Success)`: the
    /// resume frontier itself is unfireable.
    RootUnfireable { root: Uuid },
    /// The source run has **zero** `Grounded(Failed)` HyperNodes (a cancelled /
    /// timed-out run) so the default resume-from is empty — the caller must
    /// pass `resume_from` explicitly.
    NoFailedNodes,
    /// The source run's archived blob is absent — nothing to replay. A full
    /// `just fresh` drops `postgres-data`, wiping the archive; a live registry reset
    /// leaves it intact, so a run stays replayable across that.
    VersionUnresolvable,
}

/// The outcome of a successful fireability validation. Doomed interior joins
/// are **enumerated** here (not rejected): they faithfully reproduce the source
/// run's own behaviour — a fan-in that could never fire because a sibling arm
/// died — and are surfaced to the operator ("these HyperNodes stay blocked")
/// rather than blocking the replay.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FireabilityReport {
    /// HyperNodes in the re-run closure that can never become fireable because
    /// every incident edge depends on a dead sibling arm outside the closure.
    pub doomed: Vec<Uuid>,
}

/// The default resume-from frontier: every `Grounded(Failed)` HyperNode. The
/// cascade tail lies in the failed nodes' forward closure and re-runs, so this
/// default never trips root-fireability validation.
pub fn default_resume_from(graph: &rp::RunnableGraph) -> Vec<Uuid> {
    let mut roots: Vec<Uuid> = graph
        .nodes
        .iter()
        .filter(|n| n.ground == GROUND_FAILED)
        .filter_map(|n| Uuid::parse_str(&n.id).ok())
        .collect();
    roots.sort();
    roots
}

/// The forward closure of `seeds` over the graph's outgoing adjacency — the set
/// of HyperNodes that re-run (the roots and everything downstream).
fn forward_closure(graph: &rp::RunnableGraph, seeds: &[String]) -> HashSet<String> {
    let adj = outgoing_adjacency(graph);
    let mut closure: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = seeds.to_vec();
    while let Some(n) = stack.pop() {
        if !closure.insert(n.clone()) {
            continue;
        }
        if let Some(outs) = adj.get(&n) {
            for next in outs {
                if !closure.contains(next) {
                    stack.push(next.clone());
                }
            }
        }
    }
    closure
}

/// True iff the node has at least one incident HyperEdge all of whose sources
/// are `Grounded(Success)` in the archived graph. The synthetic `start` node is
/// pre-grounded `Success`, so a `start → root` edge qualifies.
fn has_all_success_incident_edge(graph: &rp::RunnableGraph, node: &str) -> bool {
    graph.edges.iter().any(|edge| {
        edge.targets.iter().any(|t| t == node)
            && !edge.sources.is_empty()
            && edge
                .sources
                .iter()
                .all(|src| node_by_id(graph, src).is_some_and(|n| n.ground == GROUND_SUCCESS))
    })
}

/// Two-tier fireability validation over `(archived graph, resume_from)`.
///
/// - Empty resume-from (a source run with zero `Grounded(Failed)` HyperNodes)
///   → [`ReplayReject::NoFailedNodes`].
/// - A resume-from root with no all-`Success`-sources incident edge →
///   [`ReplayReject::RootUnfireable`].
/// - Otherwise `Ok`, enumerating doomed interior joins.
pub fn validate_fireability(
    graph: &rp::RunnableGraph,
    resume_from: &[Uuid],
) -> Result<FireabilityReport, ReplayReject> {
    if resume_from.is_empty() {
        return Err(ReplayReject::NoFailedNodes);
    }
    for &root in resume_from {
        if !has_all_success_incident_edge(graph, &root.to_string()) {
            return Err(ReplayReject::RootUnfireable { root });
        }
    }

    // Doom enumeration. A node in the re-run closure becomes fireable-eventually
    // iff it is a root, or it has an incident edge every source of which is
    // either pre-grounded `Success` (carried forward) or itself
    // fireable-eventually (re-runs to `Success`). Fixpoint from the roots
    // outward; anything in the closure that never enters the set is doomed —
    // a join waiting on a dead sibling arm outside the closure.
    let roots: Vec<String> = resume_from.iter().map(Uuid::to_string).collect();
    let closure = forward_closure(graph, &roots);
    let mut fireable: HashSet<String> = roots.iter().cloned().collect();
    loop {
        let mut changed = false;
        for n in &closure {
            if fireable.contains(n) {
                continue;
            }
            let ok = graph.edges.iter().any(|edge| {
                edge.targets.iter().any(|t| t == n)
                    && !edge.sources.is_empty()
                    && edge.sources.iter().all(|src| {
                        fireable.contains(src)
                            || node_by_id(graph, src).is_some_and(|s| s.ground == GROUND_SUCCESS)
                    })
            });
            if ok {
                fireable.insert(n.clone());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    let mut doomed: Vec<Uuid> = closure
        .iter()
        .filter(|n| !fireable.contains(*n))
        .filter(|n| node_by_id(graph, n).is_some_and(|nd| nd.node_type == NODE_TYPE_TASK))
        .filter_map(|n| Uuid::parse_str(n).ok())
        .collect();
    doomed.sort();
    Ok(FireabilityReport { doomed })
}

/// Build the seeded graph the server materialises the replay from: the archived
/// graph with the resume-from forward closure reset to `Pending`, boundary
/// gates seeded `Idle`, and every other grounded HyperNode carrying its archived
/// `GroundKind` verbatim. Returns the seeded graph and the pre-grounded set (the
/// carried-forward, still-`Grounded` Task HyperNodes) persisted on the
/// provenance.
fn build_seeded_graph(
    graph: &rp::RunnableGraph,
    resume_from: &[Uuid],
) -> (rp::RunnableGraph, Vec<Uuid>) {
    let roots: Vec<String> = resume_from.iter().map(Uuid::to_string).collect();
    let closure = forward_closure(graph, &roots);
    let mut seeded = graph.clone();

    // Reset the re-run closure to un-grounded — those HyperNodes re-execute.
    for node in seeded.nodes.iter_mut() {
        if closure.contains(&node.id) {
            node.ground = GROUND_PENDING;
            node.grounded_at = None;
        }
    }

    // Seed boundary gates `Idle`: any gate on an edge whose target re-runs must
    // re-arm on the current world, not fire on the stale world-state the run was
    // escaping. Seeding `Satisfied` would leave the edge reading `Fired` with no
    // re-arm.
    for edge in seeded.edges.iter_mut() {
        if edge.targets.iter().any(|t| closure.contains(t)) {
            for gate in edge.gates.iter_mut() {
                gate.state = Some(rp::GateRuntimeState {
                    state: Some(rp::gate_runtime_state::State::Idle(
                        rp::gate_runtime_state::Idle {},
                    )),
                });
            }
        }
    }

    // The pre-grounded set: Task HyperNodes still `Grounded` after the reset —
    // carried forward with their archived `GroundKind` verbatim (a cascade
    // victim stays `Grounded(Cancelled)`, never re-stamped `Grounded(Failed)`).
    let mut pre_grounded: Vec<Uuid> = seeded
        .nodes
        .iter()
        .filter(|n| n.node_type == NODE_TYPE_TASK && n.ground != GROUND_PENDING)
        .filter_map(|n| Uuid::parse_str(&n.id).ok())
        .collect();
    pre_grounded.sort();

    (seeded, pre_grounded)
}

/// Mint the replay seed for a replay signal from the source run's archive.
///
/// Operates on the **runnable projection** of the source run — the runnable
/// `graph` and the `seeded_tasks` map the caller rehydrates from the published
/// archive contract, never the server `WorkflowInstance`/`TaskInstance`
/// aggregate. It resolves the resume-from frontier (the caller's choice, or the
/// default all-`Grounded(Failed)` set), validates fireability, and assembles the
/// self-contained seed the server materialises under
/// `replay_instance_id = UUIDv5(source_instance_id, signal_id)`. Two retries of
/// the *same* replay signal (the same conductor-minted `signal_id`) mint the
/// same instance id — idempotent by construction.
///
/// Returns the seed alongside the [`FireabilityReport`] (doom enumeration) for
/// the replay response, or a [`ReplayReject`] the caller surfaces as a typed
/// reject.
pub fn mint_replay_seed(
    source_instance_id: Uuid,
    graph: &rp::RunnableGraph,
    seeded_tasks: Vec<wf::TaskDefinition>,
    workflow_version: i64,
    resume_from: Option<Vec<Uuid>>,
    signal_id: Uuid,
) -> Result<(sp::ReplaySeed, FireabilityReport), ReplayReject> {
    // The default frontier (omitted `resume_from`) is every `Grounded(Failed)`
    // HyperNode.
    let resume_from = match resume_from {
        Some(rf) if !rf.is_empty() => rf,
        _ => default_resume_from(graph),
    };

    let report = validate_fireability(graph, &resume_from)?;
    let (seeded_graph, pre_grounded) = build_seeded_graph(graph, &resume_from);

    // The seed uses the published Signal contract and shared runnable graph and
    // task-spec messages.
    let seed = sp::ReplaySeed {
        replay_instance_id: replay_instance_id(source_instance_id, signal_id).to_string(),
        pre_grounded: pre_grounded.iter().map(Uuid::to_string).collect(),
        seeded_graph: Some(seeded_graph),
        seeded_tasks,
        workflow_version,
    };
    Ok((seed, report))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The replay-seed transforms operate on the published runnable projection
    // (`rp::RunnableGraph`) and proto task specs (`wf::TaskDefinition`), so the
    // fixtures below construct those proto values directly — no server
    // aggregate is materialised or projected.

    const PENDING: i32 = rp::GroundState::Pending as i32;
    const SUCCESS: i32 = rp::GroundState::Success as i32;
    const FAILED: i32 = rp::GroundState::Failed as i32;
    const CANCELLED: i32 = rp::GroundState::Cancelled as i32;

    /// Build a runnable graph directly from proto literals: `task_nodes` are the
    /// task HyperNodes with their archived grounds; the synthetic `start`
    /// sentinel is pre-grounded `Success` (so `start → root` edges qualify as
    /// all-success incident edges) and `end` is `Pending`. Edges are plain
    /// `Control` hyperedges over the given source/target id clusters.
    fn runnable_graph(
        task_nodes: &[(Uuid, i32)],
        edges: &[(Vec<Uuid>, Vec<Uuid>)],
        start: Uuid,
        end: Uuid,
    ) -> rp::RunnableGraph {
        let mut nodes = vec![
            rp::RunnableNode {
                id: start.to_string(),
                node_type: wf::NodeType::Start as i32,
                ground: SUCCESS,
                grounded_at: None,
            },
            rp::RunnableNode {
                id: end.to_string(),
                node_type: wf::NodeType::End as i32,
                ground: PENDING,
                grounded_at: None,
            },
        ];
        for (id, ground) in task_nodes {
            nodes.push(rp::RunnableNode {
                id: id.to_string(),
                node_type: wf::NodeType::Task as i32,
                ground: *ground,
                grounded_at: None,
            });
        }
        let edges = edges
            .iter()
            .map(|(sources, targets)| runnable_edge(sources, targets, Vec::new()))
            .collect();
        rp::RunnableGraph {
            nodes,
            edges,
            start: start.to_string(),
            end: end.to_string(),
            head: Vec::new(),
            tail: String::new(),
        }
    }

    /// One `Control` runnable hyperedge over the given source/target clusters,
    /// carrying the supplied gate declarations (each defaulting to no runtime
    /// state).
    fn runnable_edge(sources: &[Uuid], targets: &[Uuid], gates: Vec<wf::Gate>) -> rp::RunnableEdge {
        rp::RunnableEdge {
            id: Uuid::new_v4().to_string(),
            sources: sources.iter().map(Uuid::to_string).collect(),
            targets: targets.iter().map(Uuid::to_string).collect(),
            kind: wf::EdgeKind::Control as i32,
            gates: gates
                .into_iter()
                .map(|declaration| rp::RunnableGate {
                    declaration: Some(declaration),
                    state: None,
                    transitions: Vec::new(),
                })
                .collect(),
        }
    }

    /// A `start → a → b → end` chain with the given grounds on `a` and `b`.
    fn chain_graph(
        a: Uuid,
        b: Uuid,
        start: Uuid,
        end: Uuid,
        a_ground: i32,
        b_ground: i32,
    ) -> rp::RunnableGraph {
        runnable_graph(
            &[(a, a_ground), (b, b_ground)],
            &[
                (vec![start], vec![a]),
                (vec![a], vec![b]),
                (vec![b], vec![end]),
            ],
            start,
            end,
        )
    }

    /// A minimal proto task spec with a matching id — the seeded-tasks payload
    /// the mint carries verbatim.
    fn task_def(id: Uuid, name: &str) -> wf::TaskDefinition {
        wf::TaskDefinition {
            id: id.to_string(),
            workflow_id: Uuid::new_v4().to_string(),
            name: name.to_string(),
            task_type: wf::TaskType::Regular as i32,
            nix_expression_path: "x".to_string(),
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

    /// The seed's runnable graph — the seed rides the wire as proto, so a test
    /// reads its fields directly off the published projection.
    fn seeded_graph(seed: &sp::ReplaySeed) -> rp::RunnableGraph {
        seed.seeded_graph.clone().expect("seeded graph present")
    }

    /// A node's runtime ground in the seeded graph, found by its id.
    fn node_ground(graph: &rp::RunnableGraph, id: Uuid) -> i32 {
        graph
            .nodes
            .iter()
            .find(|n| n.id == id.to_string())
            .expect("node present")
            .ground
    }

    #[test]
    fn default_resume_from_is_every_failed_node() {
        let (a, b, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = chain_graph(a, b, start, end, SUCCESS, FAILED);
        let rf = default_resume_from(&graph);
        assert_eq!(
            rf,
            vec![b],
            "only the Grounded(Failed) node is a default root"
        );
    }

    #[test]
    fn default_frontier_never_trips_root_fireability() {
        // a Success, b Failed: default resume-from is [b]; b's incident edge
        // (a → b) has all-Success sources, so validation passes.
        let (a, b, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = chain_graph(a, b, start, end, SUCCESS, FAILED);
        let rf = default_resume_from(&graph);
        assert!(validate_fireability(&graph, &rf).is_ok());
    }

    #[test]
    fn zero_failed_nodes_typed_rejects() {
        // A cancelled/timed-out run: no Grounded(Failed) node → the default
        // frontier is empty → NoFailedNodes, instructing the caller to pass
        // resume_from explicitly.
        let (a, b, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = chain_graph(a, b, start, end, SUCCESS, CANCELLED);
        let rf = default_resume_from(&graph);
        assert_eq!(
            validate_fireability(&graph, &rf),
            Err(ReplayReject::NoFailedNodes)
        );
    }

    #[test]
    fn unfireable_resume_root_typed_rejects() {
        // Resume from `b` while `a` (its only source) is Grounded(Failed): no
        // incident edge of `b` has all-Success sources → RootUnfireable.
        let (a, b, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = chain_graph(a, b, start, end, FAILED, CANCELLED);
        assert_eq!(
            validate_fireability(&graph, &[b]),
            Err(ReplayReject::RootUnfireable { root: b })
        );
    }

    #[test]
    fn doomed_interior_join_is_enumerated_not_rejected() {
        // Diamond: start → a, start → c, {a,c} → j → end. `a` failed, `c`
        // died (Cancelled). Resume from `a` only: `j` is in the closure but its
        // fan-in needs `c`, which is dead and outside the closure — `j` is
        // enumerated as doomed, the replay is NOT rejected.
        let (a, c, j, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = runnable_graph(
            &[(a, FAILED), (c, CANCELLED), (j, PENDING)],
            &[
                (vec![start], vec![a]),
                (vec![start], vec![c]),
                (vec![a, c], vec![j]),
                (vec![j], vec![end]),
            ],
            start,
            end,
        );
        let report = validate_fireability(&graph, &[a]).expect("not rejected");
        assert!(
            report.doomed.contains(&j),
            "the fan-in whose sibling arm died is enumerated doomed, got {:?}",
            report.doomed
        );
    }

    #[test]
    fn mint_seeds_verbatim_groundkinds_and_resets_closure() {
        // a Success, b Failed. Default resume-from = [b]; b resets to Pending,
        // a carries forward Grounded(Success) verbatim.
        let (a, b, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = chain_graph(a, b, start, end, SUCCESS, FAILED);
        let source = Uuid::new_v4();
        let signal = Uuid::new_v4();
        let (seed, _report) = mint_replay_seed(
            source,
            &graph,
            vec![task_def(a, "a"), task_def(b, "b")],
            1,
            None,
            signal,
        )
        .expect("mint");
        assert_eq!(
            seed.replay_instance_id,
            replay_instance_id(source, signal).to_string()
        );
        assert_eq!(
            seed.pre_grounded,
            vec![a.to_string()],
            "only `a` carries forward"
        );
        let g = seeded_graph(&seed);
        assert_eq!(
            node_ground(&g, a),
            SUCCESS,
            "carried node keeps its archived kind verbatim"
        );
        assert_eq!(
            node_ground(&g, b),
            PENDING,
            "resume-from root is reset to Pending"
        );
    }

    #[test]
    fn mint_preserves_cascade_cancelled_kind_verbatim() {
        // Two independent arms off start: `a` (Cancelled cascade victim) and
        // `b` (Failed). Resuming from `b` only leaves `a` outside the re-run
        // closure, so it carries forward Grounded(Cancelled) verbatim — never
        // re-stamped Failed.
        let (a, b, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = runnable_graph(
            &[(a, CANCELLED), (b, FAILED)],
            &[
                (vec![start], vec![a]),
                (vec![start], vec![b]),
                (vec![a], vec![end]),
                (vec![b], vec![end]),
            ],
            start,
            end,
        );
        let source = Uuid::new_v4();
        let signal = Uuid::new_v4();
        let (seed, _r) = mint_replay_seed(
            source,
            &graph,
            vec![task_def(a, "a"), task_def(b, "b")],
            1,
            Some(vec![b]),
            signal,
        )
        .expect("mint");
        let g = seeded_graph(&seed);
        assert_eq!(
            node_ground(&g, a),
            CANCELLED,
            "cascade victim stays Cancelled verbatim"
        );
    }

    #[test]
    fn mint_is_idempotent_for_the_same_signal() {
        let (a, b, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let graph = chain_graph(a, b, start, end, SUCCESS, FAILED);
        let source = Uuid::new_v4();
        let signal = Uuid::new_v4();
        let first = mint_replay_seed(
            source,
            &graph,
            vec![task_def(a, "a"), task_def(b, "b")],
            1,
            None,
            signal,
        )
        .unwrap()
        .0;
        let second = mint_replay_seed(
            source,
            &graph,
            vec![task_def(a, "a"), task_def(b, "b")],
            1,
            None,
            signal,
        )
        .unwrap()
        .0;
        assert_eq!(first.replay_instance_id, second.replay_instance_id);
    }

    /// A gated runnable hyperedge — a `Control` edge carrying one gate
    /// declaration (used to build the construct-corpus graphs directly as
    /// proto).
    fn gated_edge(sources: &[Uuid], targets: &[Uuid], gate: wf::Gate) -> rp::RunnableEdge {
        runnable_edge(sources, targets, vec![gate])
    }

    /// A `PredicateHolds` gate declaration over `var != value` (string).
    fn routing_gate(var: &str, value: &str) -> wf::Gate {
        wf::Gate {
            kind: Some(wf::gate::Kind::PredicateHolds(wf::gate::PredicateHolds {
                routing_var: var.to_string(),
                op: wf::ComparisonOp::NotEq as i32,
                value: Some(wf::RoutingValue {
                    value: Some(wf::routing_value::Value::StringValue(value.to_string())),
                }),
                timeout: Some(wf::Duration {
                    secs: 600,
                    nanos: 0,
                }),
            })),
        }
    }

    /// A capture-bearing `SignalReceived` gate declaration extracting `approver`
    /// from `$.approver`.
    fn capture_signal_gate() -> wf::Gate {
        wf::Gate {
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
        }
    }

    /// A `TimerElapsed` gate declaration.
    fn timer_gate(secs: u64) -> wf::Gate {
        wf::Gate {
            kind: Some(wf::gate::Kind::TimerElapsed(wf::gate::TimerElapsed {
                duration: Some(wf::Duration { secs, nanos: 0 }),
            })),
        }
    }

    /// A loop-task proto spec carrying `nix_args` + `loop_participant`.
    fn loop_task_def(id: Uuid) -> wf::TaskDefinition {
        wf::TaskDefinition {
            nix_args: vec!["--flag".to_string(), "--iter".to_string()],
            loop_participant: true,
            ..task_def(id, "poll")
        }
    }

    /// The construct corpus a replay seed must survive, built directly as a
    /// runnable projection: a loop task carrying `nix_args` + `loop_participant`,
    /// a routing (`PredicateHolds`) loop back-edge, a capture-bearing signal
    /// gate, and a timer gate. The seed is minted from the corpus and asserted
    /// to preserve every construct — the gate declarations in the seeded graph
    /// and the loop task's run-fidelity fields in the seeded task specs.
    #[test]
    fn seed_preserves_the_construct_corpus() {
        let (poll, notify, wait, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let mut graph = runnable_graph(
            &[(poll, SUCCESS), (notify, FAILED), (wait, PENDING)],
            &[(vec![start], vec![poll]), (vec![wait], vec![end])],
            start,
            end,
        );
        // The loop back-edge (routing gate), the capture-signal gate, and the
        // timer gate — the gated hyperedges the corpus exercises.
        graph
            .edges
            .push(gated_edge(&[poll], &[poll], routing_gate("status", "done")));
        graph
            .edges
            .push(gated_edge(&[poll], &[notify], capture_signal_gate()));
        graph
            .edges
            .push(gated_edge(&[notify], &[wait], timer_gate(30)));

        let source = Uuid::new_v4();
        let signal = Uuid::new_v4();
        let (seed, _report) = mint_replay_seed(
            source,
            &graph,
            vec![
                loop_task_def(poll),
                task_def(notify, "notify"),
                task_def(wait, "wait"),
            ],
            9,
            Some(vec![notify]),
            signal,
        )
        .expect("mint");

        // Every construct survives into the seeded graph's gate declarations.
        let g = seeded_graph(&seed);
        let mut has_routing = false;
        let mut has_signal = false;
        let mut has_timer = false;
        for edge in &g.edges {
            for gate in &edge.gates {
                match gate.declaration.as_ref().and_then(|d| d.kind.as_ref()) {
                    Some(wf::gate::Kind::PredicateHolds(ph)) if ph.routing_var == "status" => {
                        has_routing = true;
                    }
                    Some(wf::gate::Kind::SignalReceived(sr)) if sr.signal_name == "approval" => {
                        has_signal = true;
                        assert_eq!(sr.captures_spec.len(), 1);
                        assert_eq!(sr.captures_spec[0].name, "approver");
                    }
                    Some(wf::gate::Kind::TimerElapsed(_)) => has_timer = true,
                    _ => {}
                }
            }
        }
        assert!(
            has_routing && has_signal && has_timer,
            "the construct corpus survives minting (capture gate, timer gate, routing gate)"
        );

        // The loop task's run-fidelity fields survive into the seeded task specs.
        let seeded_poll = seed
            .seeded_tasks
            .iter()
            .find(|t| t.id == poll.to_string())
            .expect("seeded poll task present");
        assert!(seeded_poll.loop_participant);
        assert_eq!(seeded_poll.nix_args, ["--flag", "--iter"]);
    }

    /// Runtime replay across the construct corpus — driven through the actual
    /// runtime consumers, proving the runnable projection is runtime-usable
    /// after minting:
    ///
    /// 1. a replayed capture-bearing signal gate **re-arms** (its runtime state
    ///    resets to `Idle`) and **extracts its declared captures** when the real
    ///    JSONPath extractor runs against a signal — proving the capture `from`
    ///    source survived into the seed and is runtime-usable;
    /// 2. a replayed task **runs with its `nix_args`** — the rehydrated task
    ///    definition the executor dispatches carries them verbatim;
    /// 3. a replayed loop task is **treated as a loop participant** — the runtime
    ///    park classifier parks it, the branch a non-participant never takes.
    #[test]
    fn replayed_corpus_runtime_behaviours() {
        use tickr_ctx::envelope::SignalSource;

        use crate::captures_extractor::extract_captures;

        let (poll, notify, start, end) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let mut graph = runnable_graph(
            &[(poll, SUCCESS), (notify, SUCCESS)],
            &[(vec![start], vec![poll]), (vec![notify], vec![end])],
            start,
            end,
        );
        // The loop back-edge (routing gate) on the loop task.
        graph
            .edges
            .push(gated_edge(&[poll], &[poll], routing_gate("status", "done")));
        // The capture-bearing signal gate, `Satisfied` on the source run so the
        // re-arm to `Idle` on replay is an observable transition.
        let mut signal_edge = gated_edge(&[poll], &[notify], capture_signal_gate());
        for gate in signal_edge.gates.iter_mut() {
            gate.state = Some(rp::GateRuntimeState {
                state: Some(rp::gate_runtime_state::State::Satisfied(
                    rp::gate_runtime_state::Satisfied {
                        signal_id: Uuid::new_v4().to_string(),
                    },
                )),
            });
        }
        graph.edges.push(signal_edge);

        let source = Uuid::new_v4();
        let signal = Uuid::new_v4();
        let seeded_tasks = vec![loop_task_def(poll), task_def(notify, "notify")];
        // Resume from the loop task so it re-runs on replay.
        let (seed, _report) = mint_replay_seed(
            source,
            &graph,
            seeded_tasks.clone(),
            4,
            Some(vec![poll]),
            signal,
        )
        .expect("mint replay seed off the projection");
        let seeded = seeded_graph(&seed);

        // (1) The capture gate re-arms and extracts its declared captures.
        let capture_gate = seeded
            .edges
            .iter()
            .flat_map(|e| e.gates.iter())
            .find(|g| {
                matches!(
                    g.declaration.as_ref().and_then(|d| d.kind.as_ref()),
                    Some(wf::gate::Kind::SignalReceived(_))
                )
            })
            .expect("capture gate present in the seeded graph");
        assert!(
            matches!(
                capture_gate.state.as_ref().and_then(|s| s.state.as_ref()),
                Some(rp::gate_runtime_state::State::Idle(_))
            ),
            "the replayed capture gate re-arms to Idle (it was Satisfied on the source run)"
        );
        let Some(wf::gate::Kind::SignalReceived(sr)) = capture_gate
            .declaration
            .as_ref()
            .and_then(|d| d.kind.as_ref())
        else {
            unreachable!("filtered to SignalReceived above")
        };
        // Runtime, not round-trip: run the real JSONPath extractor. A dropped
        // `from` would leave no jsonpath to extract against.
        let payload = serde_json::json!({ "approver": "alice", "ok": true });
        let extracted = extract_captures(&payload, &sr.captures_spec, signal, SignalSource::Manual)
            .expect("capture extraction runs on the rehydrated declarations");
        assert_eq!(extracted.len(), 1, "the declared capture is extracted");
        assert_eq!(extracted[0].name, "approver");
        assert_eq!(
            extracted[0].envelope.value,
            serde_json::json!("alice"),
            "the capture `from` source survived into the seed and extracts at runtime"
        );

        // (2) The replayed task runs with its nix_args: the rehydrated task
        // definition carried in the seed keeps them verbatim.
        let rehydrated_poll = seed
            .seeded_tasks
            .iter()
            .find(|t| t.id == poll.to_string())
            .expect("loop task in the seeded set");
        assert_eq!(
            rehydrated_poll.nix_args,
            ["--flag", "--iter"],
            "the replayed task's nix_args flow into the seeded task definition"
        );

        // (3) The replayed loop task retains the participant marker consumed
        // by the loop scheduler.
        assert!(rehydrated_poll.loop_participant);
    }
}
