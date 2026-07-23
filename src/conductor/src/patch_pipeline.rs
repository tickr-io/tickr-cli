//! Conductor Patch pipeline.
//!
//! Brings an externally-submitted Patch in over the command bus and through
//! to the server's apply path. The conductor owns the *document*: it
//! evaluates the raw Nickel Patch source and checks the ops are well-formed
//! (the server does not speak Nickel) — it never dry-runs the ops against a
//! graph copy; the server is the sole structural authority and re-validates
//! against the live graph at apply.
//!
//! On receipt: mint `patch_id`, return it synchronously (the submitter polls
//! the row — feedback is never a long synchronous hold), open a durable
//! lifecycle row (`Validating → Building → Submitted → Applied | Rejected |
//! BuildFailed`), relay a `PatchWorkflowInstance` envelope to the server, and
//! correlate the server's `PatchOutcome` back onto the row. A patch with no
//! new tasks skips `Building` entirely and relays its single validate+apply
//! envelope straight from ingress.
//!
//! **Build-then-apply:** a task-bearing `AddNode` (a full `mkTask` the system
//! never built) opens the row at `Building` — **nothing armed on the instance
//! during the build window**, so a slow build can never pin the run — and
//! queues one patch-keyed build per new task (registration's build machinery:
//! per-task rows, queue-group workers, the last-one-out finalizer's
//! conditional-UPDATE lock, keyed by `patch_key`). On build success the row
//! flips `Building → Submitted` and the finalizer relays the single
//! validate+apply envelope; on build failure the row settles `BuildFailed`
//! conductor-internally — no envelope, the built artifacts are orphaned. A
//! self-patch's Stall (armed on the server atomically with the emitting task's
//! completion) is released by that apply, or by the author-declared stall-TTL
//! backstop if no apply ever lands.
//!
//! **Self-patch ingress:** a completing task can carry a raw Patch document
//! on its reserved `tickr_patch` output; the relay's completion drain
//! detects it, parses it here, and forks it into this same pipeline with
//! `patch_id = node_id` — so `patch_key = UUIDv5(instance, node_id)` is
//! attempt-invariant and a retried completion lands on the same row
//! (applies once).
//!
//! Durability is persist-at-ingress + re-drive-to-settlement: the parsed ops
//! are persisted on the row before any relay attempt, and a non-terminal row
//! is re-sent on backoff until an outcome settles it. Re-drive is safe
//! because a redelivered patch is just another duplicate class the
//! `patch_key` dedup index absorbs — the server replays the recorded outcome
//! for a key it has already applied, and a settled row ignores late
//! correlations.
//!
//! One Patch at a time per instance: a Patch arriving while another is still
//! unsettled is rejected *and still recorded* on its own row — the patch
//! table is the complete audit of every request, including concurrency
//! rejections. (The server enforces the same rule authoritatively against
//! its live Stall; this ingress check is the fast path that spares the
//! round trip.)

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::patch_repository::{
    PatchIngressInput as RepositoryPatchIngressInput,
    PatchIngressOutcome as RepositoryPatchIngressOutcome, PatchOutcomeCorrelation,
    PatchOutcomeInput, PatchRedriveCandidate, PatchTaskBuildResult, PatchTaskSpecification,
    PatchTerminalOutcome,
};
pub use tickr_migrations::patch_repository::{PatchProvenance, PatchSourceFormat};
use tickr_proto::patch as pp;
use tickr_proto::workflow as wf;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::build_pipeline::{BuildExecutor, BuildOutcome, TaskBuildJob};
use crate::parser::builder::project_gate_proto;
use crate::parser::types::{ParsedGate, ParsedInputBinding, ParsedTask};

pub mod local;

/// Reject a chain whose `steps` name the same `handle` twice **within one
/// scope** (a step list). The same string may recur in a sibling scope — the
/// scope path disambiguates — so the check is per-scope, recursing into any
/// nested sub-chain. Returns the offending handle on the first collision. Run at
/// conductor parse, before the operation rides the wire.
fn chain_handles_unique(steps: &[pp::ChainStep]) -> Result<(), String> {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for step in steps {
        if !seen.insert(step.handle.as_str()) {
            return Err(step.handle.clone());
        }
        if let Some(pp::StepNode {
            node: Some(pp::step_node::Node::Nested(nested)),
        }) = &step.node
        {
            chain_handles_unique(&nested.steps)?;
        }
    }
    Ok(())
}

/// Build a proto primitive AddNode op carrying a full task spec under `node_id`.
fn add_node_op(node_id: Uuid, task: Option<wf::TaskDefinition>) -> pp::AddressedPatchOp {
    pp::AddressedPatchOp {
        op: Some(pp::addressed_patch_op::Op::AddNode(
            pp::addressed_patch_op::AddNode {
                node_id: node_id.to_string(),
                task,
            },
        )),
    }
}

/// A leaf chain step naming a built task by its minted node id.
fn chain_leaf(handle: String, node_id: Uuid) -> pp::ChainStep {
    pp::ChainStep {
        handle,
        node: Some(pp::StepNode {
            node: Some(pp::step_node::Node::Task(pp::step_node::Task {
                node_id: node_id.to_string(),
            })),
        }),
    }
}

/// A nested chain step forming a child scope.
fn chain_nested(handle: String, steps: Vec<pp::ChainStep>) -> pp::ChainStep {
    pp::ChainStep {
        handle,
        node: Some(pp::StepNode {
            node: Some(pp::step_node::Node::Nested(pp::step_node::Nested { steps })),
        }),
    }
}

/// Accessor helpers for protobuf patch operations.
pub trait AddressedPatchOpExt {
    /// The node id an `AddNode` op mints; `None` for other ops.
    fn added_node_id(&self) -> Option<Uuid>;
    /// The full task spec an `AddNode` carries for a never-built task; `None`
    /// for a spec-less `AddNode` or a non-`AddNode` op.
    fn added_task(&self) -> Option<&wf::TaskDefinition>;
}

impl AddressedPatchOpExt for pp::AddressedPatchOp {
    fn added_node_id(&self) -> Option<Uuid> {
        match &self.op {
            Some(pp::addressed_patch_op::Op::AddNode(n)) => Uuid::parse_str(&n.node_id).ok(),
            _ => None,
        }
    }

    fn added_task(&self) -> Option<&wf::TaskDefinition> {
        match &self.op {
            Some(pp::addressed_patch_op::Op::AddNode(n)) => n.task.as_ref(),
            _ => None,
        }
    }
}

/// Repository lifecycle states. Logical schema constraints enforce the same
/// canonical strings on every SQL implementation.
pub const STATUS_VALIDATING: &str = "Validating";
pub const STATUS_BUILDING: &str = "Building";
pub const STATUS_SUBMITTED: &str = "Submitted";
pub const STATUS_APPLIED: &str = "Applied";
pub const STATUS_REJECTED: &str = "Rejected";
pub const STATUS_BUILD_FAILED: &str = "BuildFailed";

/// How often the re-drive loop scans for unsettled rows, and how long a row
/// must sit untouched before it is re-sent. `updated_at` is the backoff
/// anchor: every successful re-send bumps it, so a row is re-driven at most
/// once per `REDRIVE_MIN_AGE` until settlement.
pub const REDRIVE_INTERVAL: Duration = Duration::from_secs(5);
pub const REDRIVE_MIN_AGE: Duration = Duration::from_secs(10);

/// The stable logical identity of an externally-submitted Patch:
/// `UUIDv5(workflow_instance_id, patch_id)`. The conductor-minted `patch_id`
/// is the author key, so every re-drive of the same ingressed request
/// computes the same `patch_key` and lands on the same lifecycle row.
pub fn patch_key(workflow_instance_id: Uuid, patch_id: Uuid) -> Uuid {
    Uuid::new_v5(&workflow_instance_id, patch_id.as_bytes())
}

/// The verbatim authored Patch source, retained keyed by patch so the reading
/// surface can render exactly what was submitted — never a re-encoding of the
/// lowered `ops`. Captured at the outermost parse boundary: the Nickel an
/// external author wrote, or the JSON document a self-patching task emitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchSource {
    pub text: String,
    pub format: PatchSourceFormat,
}

impl PatchSource {
    pub fn nickel(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            format: PatchSourceFormat::Nickel,
        }
    }

    pub fn json(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            format: PatchSourceFormat::Json,
        }
    }
}

/// What a Patch document evaluates to after lowering: the ordered primitive
/// op list (task-bearing `AddNode`s carry their lowered `Task` under a
/// freshly-minted node id), an optional submitter why-string that rides into
/// apply provenance, and the verbatim authored `source` (retained as submitted,
/// never re-encoded from the lowered ops).
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedPatch {
    pub ops: Vec<pp::AddressedPatchOp>,
    /// The un-lowered composite operation this document expressed (an
    /// `insert`), or `None` for a primitive-op-only patch. Carried alongside
    /// `ops` (an `insert`'s inserted task rides `ops` as a task-bearing
    /// `AddNode`) so the server lowers the edge rewrite against its live graph.
    pub operation: Option<pp::PatchOperation>,
    pub reason: Option<String>,
    /// The author-declared `stall_ttl` (seconds), if the document carried one.
    /// Consumed only on the self-patch path: it is stamped onto the emitting
    /// task's completion so the server arms the self-patch Stall's TTL backstop
    /// with it. `None` falls back to the server default.
    pub stall_ttl: Option<i64>,
    pub source: PatchSource,
}

impl ParsedPatch {
    /// The never-built tasks this Patch introduces: `(minted node id, task)`
    /// for every task-bearing `AddNode`. Non-empty means the Patch must ride
    /// the build pipeline (`Building` → build → apply-on-success) instead of
    /// relaying straight through.
    pub fn new_tasks(&self) -> Vec<(Uuid, &wf::TaskDefinition)> {
        self.ops
            .iter()
            .filter_map(|op| match &op.op {
                Some(pp::addressed_patch_op::Op::AddNode(n)) => n
                    .task
                    .as_ref()
                    .and_then(|t| Uuid::parse_str(&n.node_id).ok().map(|id| (id, t))),
                _ => None,
            })
            .collect()
    }
}

/// The document-level op shape, before lowering. Identical to the wire
/// kernel except `AddNode` may carry a full `mkTask` body (`task`), in which
/// case the document's `node_id` is a **placeholder**: lowering mints a
/// fresh, globally-unique node id (the collision-checked identity) and
/// rewrites every later reference to the placeholder, so the same `mkTask`
/// written N times under N placeholders fans out to N distinct siblings.
/// The document-level op shape. References to *existing* structures are
/// [`StructureRef`]s (an identity code the author read off the ctx graph, or a
/// full UUID); `AddNode.node_id` stays a `Uuid` placeholder that lowering
/// re-mints. The server resolves the codes against the live graph at apply.
#[derive(Debug, Deserialize)]
enum RawPatchOp {
    AddNode {
        node_id: Uuid,
        #[serde(default)]
        task: Option<ParsedTask>,
    },
    AddEdge {
        sources: Vec<String>,
        targets: Vec<String>,
        kind: wf::EdgeKind,
        #[serde(default)]
        gates: Vec<ParsedGate>,
    },
    RemoveNode {
        node_id: String,
    },
    RemoveEdge {
        edge_id: String,
    },
}

/// The raw evaluated document — a tagged union carrying either a primitive-op
/// patch (`{ ops }`, the slice-03 shape) or a composite operation the server
/// lowers (`{ insert }`). The presence of the `ops` / `insert` key is the tag;
/// exactly one arm must be populated. Additive: a future operation is a new
/// optional key, never a wire break, and a `{ ops }` document keeps parsing
/// unchanged.
#[derive(Debug, Deserialize)]
struct RawPatchDocument {
    #[serde(default)]
    ops: Option<Vec<RawPatchOp>>,
    #[serde(default)]
    insert: Option<RawInsertOperation>,
    #[serde(default)]
    chain: Option<RawChainOperation>,
    #[serde(default)]
    fork: Option<RawForkOperation>,
    #[serde(default)]
    branch: Option<RawBranchOperation>,
    #[serde(default)]
    expand: Option<RawExpandOperation>,
    #[serde(default)]
    swap: Option<RawSwapOperation>,
    #[serde(default, rename = "loop")]
    loop_op: Option<RawLoopOperation>,
    #[serde(default)]
    cut: Option<RawCutOperation>,
    #[serde(default)]
    prune: Option<RawPruneOperation>,
    #[serde(default)]
    trim: Option<RawTrimOperation>,
    #[serde(default)]
    truncate: Option<RawTruncateOperation>,
    #[serde(default)]
    reason: Option<String>,
    /// Author-declared **Patch intent**: how long (seconds) to hold the Stall a
    /// self-patch arms at its emitting task's completion. Consumed only on the
    /// self-patch path; `None` falls back to the server default.
    #[serde(default)]
    stall_ttl: Option<i64>,
}

/// The `insert` operation document shape: `{ anchor, task }`. The author names
/// the anchor by identity code (or full UUID) and supplies the never-built
/// task; the conductor mints the task's node id at ingress, and the server
/// discovers and rewrites the affected edges at apply.
#[derive(Debug, Deserialize)]
struct RawInsertOperation {
    anchor: String,
    task: ParsedTask,
}

/// The `chain` operation document shape: `{ anchor, steps }`. The author names
/// the anchor by identity code (or UUID) and supplies a **scope-tree** of steps
/// — each a scope-local `handle` plus either a leaf `task` (built at-patch) or a
/// nested `steps` sub-chain. The conductor mints one node id per leaf task at
/// ingress, checks per-scope handle uniqueness, and the server threads the
/// sequence and re-seats the anchor's out-edges at apply.
#[derive(Debug, Deserialize)]
struct RawChainOperation {
    anchor: String,
    steps: Vec<RawChainStep>,
    /// Optional per-edge relay spec for a Data-edge anchor: the gated `Data`
    /// edge to relay across and its routing variable. Carried un-lowered to the
    /// server, which selects the interpose arm from live state at apply.
    #[serde(default)]
    relay: Option<RawChainRelay>,
}

/// The `relay` sub-document of a `chain`: `{ edge, var }`. `edge` names the
/// gated `Data` edge to relay across (identity code or full UUID); `var` names
/// its routing variable. Projected verbatim to the server's [`ChainRelay`].
#[derive(Debug, Deserialize)]
struct RawChainRelay {
    edge: String,
    var: String,
}

/// The `fork` operation document shape: `{ anchor, arms }`. The author names
/// the anchor by identity code (or UUID) and supplies a list of **arms** — each
/// arm is one `RawChainStep` (a scope-tree), reusing the chain step shape: a
/// leaf `task` is a single-task arm, a nested `steps` sub-chain is a multi-task
/// arm. The conductor mints one node id per leaf task at ingress, checks
/// per-scope handle uniqueness across the arms, and the server fans the anchor
/// out to every arm and rejoins them at a barrier at apply.
#[derive(Debug, Deserialize)]
struct RawForkOperation {
    anchor: String,
    arms: Vec<RawChainStep>,
}

/// The `branch` operation document shape: `{ anchor, arms }`. Like `fork` but
/// each arm carries a **selecting gate**: a [`RawBranchArm`] is a `RawChainStep`
/// (handle + leaf `task` or nested `steps`) plus a `gate` (a `mkSignalGate` /
/// `mkPredicateGate` / `mkTimerGate` body). The conductor mints one node id per
/// leaf task at ingress, projects each arm's gate to a runtime [`Gate`], checks
/// per-scope handle uniqueness across the arms, and the server gates the fan-out
/// and rejoins the arms at a selection join at apply.
#[derive(Debug, Deserialize)]
struct RawBranchOperation {
    anchor: String,
    arms: Vec<RawBranchArm>,
}

/// One raw branch arm: a `RawChainStep` (a `handle` and exactly one of a leaf
/// `task` or a nested `steps` sub-chain) plus a selecting `gate`. The gate is the
/// author-declared precondition on the arm's feeder edge — the whole point of a
/// branch over a fork.
#[derive(Debug, Deserialize)]
struct RawBranchArm {
    handle: String,
    gate: ParsedGate,
    #[serde(default)]
    task: Option<ParsedTask>,
    #[serde(default)]
    steps: Option<Vec<RawChainStep>>,
}

/// One raw chain step: a `handle` and exactly one of a leaf `task` or a nested
/// `steps` sub-chain. The presence of `task` vs `steps` is the leaf/nested tag.
#[derive(Debug, Deserialize)]
struct RawChainStep {
    handle: String,
    #[serde(default)]
    task: Option<ParsedTask>,
    #[serde(default)]
    steps: Option<Vec<RawChainStep>>,
}

/// The `expand` operation document shape: `{ anchor, over }`. **expand** is
/// sugar over **fork** — one arm per element of a map. `over` is that map: each
/// entry's key is the arm's scope-local **handle** and its value carries the
/// arm's leaf `task` or nested `steps` sub-chain (the same leaf/nested shape a
/// chain step uses, minus the handle — the map key *is* the handle). Carried as
/// a `BTreeMap` so element order is the deterministic sorted-key order, matching
/// the DSL's definition-time unroll. The conductor lowers each entry to one
/// [`ChainStep`] and hands the result to the server as a `PatchOperation::Expand`,
/// which fans out through the hand-authored fork lowering.
#[derive(Debug, Deserialize)]
struct RawExpandOperation {
    anchor: String,
    over: BTreeMap<String, RawExpandArm>,
}

/// One raw expand arm — a map value in [`RawExpandOperation::over`]. Carries
/// exactly one of a leaf `task` or a nested `steps` sub-chain; the arm's handle
/// is the map key, not a field here. The presence of `task` vs `steps` is the
/// leaf/nested tag, identical to a [`RawChainStep`].
#[derive(Debug, Deserialize)]
struct RawExpandArm {
    #[serde(default)]
    task: Option<ParsedTask>,
    #[serde(default)]
    steps: Option<Vec<RawChainStep>>,
}

/// The `swap` operation document shape: `{ target, task }`. The author names the
/// `target` HyperNode to substitute by its identity code (or a full UUID) and
/// supplies the replacement `mkTask`; the conductor mints the replacement's node
/// id at ingress, and the server re-seats the target's incident edges onto the
/// replacement and removes the target (only a Pending, un-minted target is
/// removable — a Running or Grounded one is declined structurally at apply).
#[derive(Debug, Deserialize)]
struct RawSwapOperation {
    target: String,
    task: ParsedTask,
}

/// A `loop` operation document shape: `{ path }`. The author names the ordered
/// simple path head→…→tail by identity code (or full UUID); the server
/// cyclifies it against the live graph at apply. A `loop` mints no tasks (it
/// re-kinds existing edges and mints the back-edge), so it rides no
/// task-bearing `AddNode` — the op list is empty and the operation carries only
/// the path to cyclify.
#[derive(Debug, Deserialize)]
struct RawLoopOperation {
    path: Vec<String>,
}

/// A `cut` operation document shape: `{ target }`. The author names the mid-path
/// node to excise by identity code (or full UUID); the server removes it and
/// bridges the flow across the gap at apply. Like `loop`, a `cut` mints no tasks
/// (it removes structure and re-seats edges), so it rides no task-bearing
/// `AddNode` — the op list is empty and the operation carries only the target.
#[derive(Debug, Deserialize)]
struct RawCutOperation {
    target: String,
}

/// A `prune` operation document shape: `{ arm }`. The author names the fan-out
/// arm to drop by the identity code (or full UUID) of its head — the node the
/// fork/branch anchor feeds; the server discovers the whole arm, removes it, and
/// narrows the join at apply. Like `cut`, a `prune` mints no tasks (it removes
/// structure and re-seats the join edge), so it rides no task-bearing `AddNode`
/// — the op list is empty and the operation carries only the arm head.
#[derive(Debug, Deserialize)]
struct RawPruneOperation {
    arm: String,
}

/// A `trim` operation document shape: `{ anchor }`. The author names the anchor
/// (the `trim-until` bound) by identity code (or full UUID); the server discovers
/// the leading run exclusively upstream of it, removes it, and reconnects
/// `Start → anchor` at apply. Like `cut`, a `trim` mints no tasks (it removes
/// structure and reconnects the frontier), so it rides no task-bearing `AddNode`
/// — the op list is empty and the operation carries only the anchor.
#[derive(Debug, Deserialize)]
struct RawTrimOperation {
    anchor: String,
}

/// A `truncate` operation document shape: `{ anchor }`. The author names the
/// anchor by identity code (or full UUID); the server discovers the tail
/// exclusively downstream of it, removes it, and reconnects `anchor → End` at
/// apply. Like `trim`, a `truncate` mints no tasks, so it rides no task-bearing
/// `AddNode` — the op list is empty and the operation carries only the anchor.
#[derive(Debug, Deserialize)]
struct RawTruncateOperation {
    anchor: String,
}

/// Lower a document `mkTask` body into the runtime `Task`. `Task::new` mints
/// the fresh, globally-unique task/node id build-at-patch keys on.
/// `workflow_id` is stamped nil: a patched-in task belongs to an *instance*,
/// not a registered definition, and every read path takes the workflow id
/// off the instance. Constructs whose semantics only exist at registration
/// (signal emits, structured input sources — both resolved by
/// registration-time passes the patch path does not run) are rejected loudly
/// rather than silently dropped.
fn lower_patch_task(parsed: &ParsedTask) -> Result<(Uuid, wf::TaskDefinition), PatchError> {
    if !parsed.emits.is_empty() {
        return Err(PatchError::Parse(format!(
            "task `{}`: `emits` is not supported on a patched-in task",
            parsed.name
        )));
    }
    if parsed
        .inputs
        .iter()
        .any(|i| matches!(i, ParsedInputBinding::Structured { .. }))
    {
        return Err(PatchError::Parse(format!(
            "task `{}`: structured `inputs` sources are not supported on a patched-in task",
            parsed.name
        )));
    }
    // A patched-in task is built through the shipped nix pipeline before apply
    // (`nix build` on its expression; exec is `nix run`). A missing expression
    // is an authoring error rejected loudly at ingress — this retires the old
    // hardcoded build-at-patch default, which silently built every spec-less
    // task against one canned expression.
    let nix_expression_path = parsed
        .nix_expression_path
        .as_deref()
        .filter(|p| !p.trim().is_empty())
        .ok_or_else(|| {
            PatchError::Parse(format!(
                "task `{}`: a patched-in task must declare a real, buildable \
                 `nix_expression_path` (with a `#fragment`) — there is no default expression",
                parsed.name
            ))
        })?;
    // The minted node id is the built task's identity — the same id the
    // task-bearing `AddNode` and any referencing operation carry.
    let node_id = Uuid::new_v4();
    let timeout_secs = match parsed.timeout.as_ref() {
        None => None,
        Some(raw) => {
            let dur = crate::parser::duration::parse_duration(raw).map_err(|e| {
                PatchError::Parse(format!(
                    "task `{}` has invalid `timeout` value `{}`: {}",
                    parsed.name, raw, e
                ))
            })?;
            Some(dur.as_secs())
        }
    };
    let task = wf::TaskDefinition {
        id: node_id.to_string(),
        workflow_id: Uuid::nil().to_string(),
        name: parsed.name.clone(),
        // A patched-in task is a RegularTask; the enum makes a garbage
        // discriminator unrepresentable, replacing the vestigial `command`.
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: nix_expression_path.to_string(),
        nix_args: parsed.args.clone(),
        outputs: parsed.outputs.clone(),
        inputs: parsed.inputs.iter().map(|i| i.name().to_string()).collect(),
        secrets: parsed.secrets.clone(),
        max_attempts: parsed.max_attempts.unwrap_or(3),
        // Structured input sources are rejected above; a patched-in task carries
        // only bare-name inputs.
        input_sources: None,
        timeout_secs,
        // `emits` are rejected above.
        emits: Vec::new(),
        routing_vars: parsed
            .routing_vars
            .iter()
            .map(|rv| wf::RoutingVarDecl {
                name: rv.name.clone(),
                var_type: rv.var_type.clone(),
            })
            .collect(),
        loop_participant: false,
    };
    Ok((node_id, task))
}

/// Lower raw document ops to wire kernel ops: mint a fresh node id per
/// task-bearing `AddNode` and rewrite every subsequent reference to its
/// placeholder. A spec-less `AddNode` keeps its authored id — that id was
/// already minted fresh by the sugar that compiled the document, and the
/// server's collision tooth rejects a clash regardless.
fn lower_ops(raw: Vec<RawPatchOp>) -> Result<Vec<pp::AddressedPatchOp>, PatchError> {
    let mut minted: HashMap<Uuid, Uuid> = HashMap::new();
    // Rewrite a reference: a UUID that names a freshly-minted placeholder is
    // rewritten to the minted node id; a code (an existing structure the server
    // resolves at apply) and a non-placeholder UUID pass through unchanged.
    let rewrite = |minted: &HashMap<Uuid, Uuid>, r: &str| -> String {
        match Uuid::parse_str(r) {
            // A placeholder UUID is remapped to the minted node id; a
            // non-placeholder UUID passes through unchanged.
            Ok(u) => minted.get(&u).copied().unwrap_or(u).to_string(),
            // An identity code (an existing structure the server resolves at
            // apply) passes through unchanged.
            Err(_) => r.to_string(),
        }
    };
    let mut ops = Vec::with_capacity(raw.len());
    for op in raw {
        match op {
            RawPatchOp::AddNode {
                node_id,
                task: Some(parsed),
            } => {
                let (fresh, task) = lower_patch_task(&parsed)?;
                minted.insert(node_id, fresh);
                ops.push(add_node_op(fresh, Some(task)));
            }
            RawPatchOp::AddNode {
                node_id,
                task: None,
            } => ops.push(add_node_op(node_id, None)),
            RawPatchOp::AddEdge {
                sources,
                targets,
                kind,
                gates,
            } => ops.push(pp::AddressedPatchOp {
                op: Some(pp::addressed_patch_op::Op::AddEdge(
                    pp::addressed_patch_op::AddEdge {
                        sources: sources.iter().map(|s| rewrite(&minted, s)).collect(),
                        targets: targets.iter().map(|t| rewrite(&minted, t)).collect(),
                        kind: kind as i32,
                        gates: gates
                            .iter()
                            .map(|g| {
                                project_gate_proto(g, "patch AddEdge")
                                    .map_err(|e| PatchError::Parse(e.to_string()))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                )),
            }),
            RawPatchOp::RemoveNode { node_id } => ops.push(pp::AddressedPatchOp {
                op: Some(pp::addressed_patch_op::Op::RemoveNode(
                    pp::addressed_patch_op::RemoveNode {
                        node_id: rewrite(&minted, &node_id),
                    },
                )),
            }),
            RawPatchOp::RemoveEdge { edge_id } => ops.push(pp::AddressedPatchOp {
                op: Some(pp::addressed_patch_op::Op::RemoveEdge(
                    pp::addressed_patch_op::RemoveEdge { edge_id },
                )),
            }),
        }
    }
    Ok(ops)
}

#[derive(Debug, Error)]
pub enum PatchError {
    /// The document failed to evaluate, or evaluated to something that is not
    /// a well-formed op list. Surfaced to the submitter as a 400.
    #[error("invalid Patch document: {0}")]
    Parse(String),
    #[error("patch persistence failed: {0}")]
    Persist(#[from] tickr_migrations::backend::RepositoryError),
}

/// Evaluate a raw Nickel Patch document and check it is well-formed. This is
/// document validity only — op semantics against the live graph are the
/// server's call.
pub async fn parse_patch_document(nickel_source: &str) -> Result<ParsedPatch, PatchError> {
    let json = crate::parser::nickel::nickel_eval(nickel_source)
        .await
        .map_err(|e| PatchError::Parse(e.to_string()))?;
    let mut parsed = parse_patch_document_json(&json)?;
    // Retain the verbatim Nickel the author wrote — not the evaluated JSON — so
    // the reading surface renders exactly what was submitted.
    parsed.source = PatchSource::nickel(nickel_source);
    Ok(parsed)
}

/// The pure tail of document validation: the evaluated JSON must deserialize
/// into `{ ops, reason? }` where every op is one of the four kernel
/// primitives, and the op list must be non-empty (an empty Patch would stall
/// an instance to change nothing). Lowering then mints a fresh node id per
/// task-bearing `AddNode` (build-at-patch) and rewrites placeholder
/// references, so the returned ops are wire-ready.
pub fn parse_patch_document_json(json: &str) -> Result<ParsedPatch, PatchError> {
    let raw: RawPatchDocument = serde_json::from_str(json).map_err(|e| {
        PatchError::Parse(format!(
            "document must evaluate to {{ ops, reason? }} or {{ insert, reason? }} \
             with kernel-primitive ops: {e}"
        ))
    })?;
    // Tagged union: exactly one of `ops` / `insert` / `chain` / `fork` /
    // `branch` / `expand` / `swap` / `loop` / `cut` / `prune` / `trim` /
    // `truncate` names the patch shape. The presence of a key is the tag; more
    // than one is ambiguous.
    let tag_count = raw.ops.is_some() as u8
        + raw.insert.is_some() as u8
        + raw.chain.is_some() as u8
        + raw.fork.is_some() as u8
        + raw.branch.is_some() as u8
        + raw.expand.is_some() as u8
        + raw.swap.is_some() as u8
        + raw.loop_op.is_some() as u8
        + raw.cut.is_some() as u8
        + raw.prune.is_some() as u8
        + raw.trim.is_some() as u8
        + raw.truncate.is_some() as u8;
    if tag_count > 1 {
        return Err(PatchError::Parse(
            "a Patch document carries exactly one of `ops`, `insert`, `chain`, `fork`, `branch`, \
             `expand`, `swap`, `loop`, `cut`, `prune`, `trim`, or `truncate`"
                .to_string(),
        ));
    }
    // Each arm resolves to `(ops, operation)`. The tag-count guard above already
    // rejected a document naming more than one, so the first populated key wins
    // and an empty document falls through to the trailing reject.
    let (ops, operation) = if let Some(ops) = raw.ops {
        // A primitive-op patch (slice 03): an empty op list would stall an
        // instance to change nothing.
        if ops.is_empty() {
            return Err(PatchError::Parse("ops must be non-empty".to_string()));
        }
        (lower_ops(ops)?, None)
    } else if let Some(insert) = raw.insert {
        lower_insert_operation(insert)?
    } else if let Some(chain) = raw.chain {
        lower_chain_operation(chain)?
    } else if let Some(fork) = raw.fork {
        lower_fork_operation(fork)?
    } else if let Some(branch) = raw.branch {
        lower_branch_operation(branch)?
    } else if let Some(expand) = raw.expand {
        lower_expand_operation(expand)?
    } else if let Some(swap) = raw.swap {
        lower_swap_operation(swap)?
    } else if let Some(loop_op) = raw.loop_op {
        lower_loop_operation(loop_op)?
    } else if let Some(cut) = raw.cut {
        lower_cut_operation(cut)
    } else if let Some(prune) = raw.prune {
        lower_prune_operation(prune)
    } else if let Some(trim) = raw.trim {
        lower_trim_operation(trim)
    } else if let Some(truncate) = raw.truncate {
        lower_truncate_operation(truncate)
    } else {
        return Err(PatchError::Parse(
            "a Patch document must carry `ops` or an operation such as `insert`, `chain`, \
             `fork`, `branch`, `expand`, `swap`, `loop`, `cut`, `prune`, `trim`, or `truncate`"
                .to_string(),
        ));
    };
    Ok(ParsedPatch {
        ops,
        operation,
        reason: raw.reason,
        stall_ttl: raw.stall_ttl,
        // The evaluated JSON document is the verbatim source for a self-patch
        // emitted as JSON; the Nickel entry point (`parse_patch_document`)
        // overrides this with the author's Nickel.
        source: PatchSource::json(json),
    })
}

/// Lower an `insert` operation document to `(ops, operation)`: the inserted
/// task is built at-patch, so it rides `ops` as a single task-bearing `AddNode`
/// under a freshly-minted node id, and the operation carries the anchor + that
/// same node id for the server to lower the edge rewrite against its live
/// graph. The task must carry a real, buildable `nix_expression_path` with a
/// `#fragment` — the interpose is meaningless without a task that actually
/// builds and runs.
fn lower_insert_operation(
    insert: RawInsertOperation,
) -> Result<(Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>), PatchError> {
    let (node_id, task) = lower_patch_task(&insert.task)?;
    if !task.nix_expression_path.contains('#') {
        return Err(PatchError::Parse(format!(
            "insert task `{}`: `nix_expression_path` `{}` must include a `#fragment` \
             selecting the buildable attribute",
            insert.task.name, task.nix_expression_path
        )));
    }
    // The node id minted for the built task IS the identity the interpose wires
    // to; the AddNode and the operation reference the same id.
    let ops = vec![add_node_op(node_id, Some(task))];
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Insert(
            pp::patch_operation::Insert {
                anchor: insert.anchor,
                node_id: node_id.to_string(),
            },
        )),
    });
    Ok((ops, operation))
}

/// Lower a `chain` operation document to `(ops, operation)`. Each leaf task is
/// built at-patch, so it rides `ops` as a task-bearing `AddNode` under a
/// freshly-minted node id, and the operation carries the anchor + the scope-tree
/// of handles (bound to those minted ids) for the server to lower the edge
/// rewrite against its live graph. Handles must be unique **within each scope**
/// (per-scope uniqueness — the same string may recur in a sibling scope), and a
/// chain must interpose at least one node.
fn lower_chain_operation(
    chain: RawChainOperation,
) -> Result<(Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>), PatchError> {
    let mut ops = Vec::new();
    let steps = lower_chain_steps(chain.steps, &mut ops)?;
    if steps.is_empty() {
        return Err(PatchError::Parse(
            "chain `steps` must be non-empty — a chain interposes at least one node".to_string(),
        ));
    }
    // Per-scope handle uniqueness (the reusable server-side check): a duplicate
    // handle within one scope is rejected here at conductor parse.
    if let Err(dup) = chain_handles_unique(&steps) {
        return Err(PatchError::Parse(format!(
            "duplicate chain handle `{dup}` within one scope — handles are unique per scope"
        )));
    }
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Chain(
            pp::patch_operation::Chain {
                anchor: chain.anchor,
                steps,
                relay: chain.relay.map(|r| pp::ChainRelay {
                    edge: r.edge,
                    var: r.var,
                }),
            },
        )),
    });
    Ok((ops, operation))
}

/// Lower a `fork` operation document to `(ops, operation)`. Each arm is one
/// chain step (a scope-tree); every leaf task across the arms is built at-patch,
/// riding `ops` as a task-bearing `AddNode` under a freshly-minted node id, and
/// the operation carries the anchor + the arms (bound to those minted ids) for
/// the server to fan out and rejoin at a barrier against its live graph. A fork
/// must fan into at least one arm, and handles are unique **within each scope**
/// (the same string may recur across sibling arms — the scope path disambiguates
/// it). Reuses the chain step lowering and per-scope uniqueness check verbatim.
fn lower_fork_operation(
    fork: RawForkOperation,
) -> Result<(Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>), PatchError> {
    let mut ops = Vec::new();
    let arms = lower_chain_steps(fork.arms, &mut ops)?;
    if arms.is_empty() {
        return Err(PatchError::Parse(
            "fork `arms` must be non-empty — a fork fans into at least one arm".to_string(),
        ));
    }
    // Per-scope handle uniqueness across the arms (the reusable server-side
    // check): a duplicate handle within one scope is rejected here at parse.
    if let Err(dup) = chain_handles_unique(&arms) {
        return Err(PatchError::Parse(format!(
            "duplicate fork handle `{dup}` within one scope — handles are unique per scope"
        )));
    }
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Fork(pp::patch_operation::Fork {
            anchor: fork.anchor,
            arms,
        })),
    });
    Ok((ops, operation))
}

/// Lower a `branch` operation document to `(ops, operation)`. Like `fork`, but
/// each arm carries a **selecting gate**: every leaf task is built at-patch
/// (riding `ops` as a task-bearing `AddNode`), each arm's gate is validated and
/// projected to a runtime [`Gate`] (the same projection registration runs on an
/// edge gate), and the operation carries the anchor + the gated arms for the
/// server to gate the fan-out and rejoin at a selection join. A branch must fan
/// into at least one arm, and handles are unique **within each scope** (the same
/// string may recur across sibling arms — the scope path disambiguates it).
fn lower_branch_operation(
    branch: RawBranchOperation,
) -> Result<(Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>), PatchError> {
    if branch.arms.is_empty() {
        return Err(PatchError::Parse(
            "branch `arms` must be non-empty — a branch fans into at least one gated arm"
                .to_string(),
        ));
    }
    // Project each arm's selecting gate and rebuild its chain step. The gate is
    // validated and projected exactly as a registration-time edge gate is (empty
    // signal name, malformed JSONPath / op / value / duration all rejected here),
    // and always lands `Idle` — a fresh precondition, never a forged
    // pre-satisfied one.
    let mut gates = Vec::with_capacity(branch.arms.len());
    let mut raw_steps = Vec::with_capacity(branch.arms.len());
    for arm in branch.arms {
        let gate = project_gate_proto(&arm.gate, &arm.handle)
            .map_err(|e| PatchError::Parse(format!("branch arm `{}` gate: {e}", arm.handle)))?;
        gates.push(gate);
        raw_steps.push(RawChainStep {
            handle: arm.handle,
            task: arm.task,
            steps: arm.steps,
        });
    }
    let mut ops = Vec::new();
    let steps = lower_chain_steps(raw_steps, &mut ops)?;
    // Per-scope handle uniqueness across the arms (the reusable server-side
    // check): a duplicate handle within one scope is rejected here at parse.
    if let Err(dup) = chain_handles_unique(&steps) {
        return Err(PatchError::Parse(format!(
            "duplicate branch handle `{dup}` within one scope — handles are unique per scope"
        )));
    }
    // Pair each projected gate with its arm's chain step, in declaration order.
    let arms: Vec<pp::BranchArm> = steps
        .into_iter()
        .zip(gates)
        .map(|(arm, gate)| pp::BranchArm {
            gate: Some(gate),
            arm: Some(arm),
        })
        .collect();
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Branch(
            pp::patch_operation::Branch {
                anchor: branch.anchor,
                arms,
            },
        )),
    });
    Ok((ops, operation))
}

/// Lower an `expand` operation document to `(ops, operation)`. **expand** is
/// sugar over **fork**: `over` is a map whose keys are the arm handles and whose
/// values carry each arm's leaf task or nested sub-chain. Each map entry becomes
/// one [`ChainStep`] (key → handle), so the lowering reuses the chain-step
/// machinery verbatim and produces a `PatchOperation::Expand` the server fans
/// out through the hand-authored fork lowering. Map keys are unique by
/// construction (a `BTreeMap`), so root-scope handles never collide; a duplicate
/// handle inside a nested arm is still rejected. An expand must fan over at
/// least one element.
fn lower_expand_operation(
    expand: RawExpandOperation,
) -> Result<(Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>), PatchError> {
    // An expand arm is a chain step whose handle is the map key — rebuild each
    // entry into a `RawChainStep` and reuse the chain-step lowering (task build,
    // nested recursion, per-scope validation) unchanged. `BTreeMap` iteration is
    // sorted-key order, so the arm order is deterministic and matches the DSL's
    // definition-time unroll over the same map.
    let steps: Vec<RawChainStep> = expand
        .over
        .into_iter()
        .map(|(handle, arm)| RawChainStep {
            handle,
            task: arm.task,
            steps: arm.steps,
        })
        .collect();
    let mut ops = Vec::new();
    let arms = lower_chain_steps(steps, &mut ops)?;
    if arms.is_empty() {
        return Err(PatchError::Parse(
            "expand `over` must be non-empty — an expand fans over at least one element"
                .to_string(),
        ));
    }
    // Per-scope handle uniqueness across the nested arms (the reusable
    // server-side check): root-scope keys are unique via the map, so this only
    // ever catches a duplicate inside a nested sub-chain.
    if let Err(dup) = chain_handles_unique(&arms) {
        return Err(PatchError::Parse(format!(
            "duplicate expand handle `{dup}` within one scope — handles are unique per scope"
        )));
    }
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Expand(
            pp::patch_operation::Expand {
                anchor: expand.anchor,
                arms,
            },
        )),
    });
    Ok((ops, operation))
}

/// Lower a `swap` operation document to `(ops, operation)`: the replacement task
/// is built at-patch, so it rides `ops` as a single task-bearing `AddNode` under
/// a freshly-minted node id, and the operation carries the target reference + that
/// same node id for the server to re-seat the target's incident edges and remove
/// it against the live graph. Like `insert`, the replacement task must carry a
/// real, buildable `nix_expression_path` with a `#fragment` — a substitution is
/// meaningless without a task that actually builds and runs.
fn lower_swap_operation(
    swap: RawSwapOperation,
) -> Result<(Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>), PatchError> {
    let (node_id, task) = lower_patch_task(&swap.task)?;
    if !task.nix_expression_path.contains('#') {
        return Err(PatchError::Parse(format!(
            "swap task `{}`: `nix_expression_path` `{}` must include a `#fragment` \
             selecting the buildable attribute",
            swap.task.name, task.nix_expression_path
        )));
    }
    // The node id minted for the built replacement IS the identity the re-seat
    // wires to; the AddNode and the operation reference the same id.
    let ops = vec![add_node_op(node_id, Some(task))];
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Swap(pp::patch_operation::Swap {
            target: swap.target,
            node_id: node_id.to_string(),
        })),
    });
    Ok((ops, operation))
}

/// Lower a `loop` operation document to `(ops, operation)`. A `loop` cyclifies
/// an existing simple path — it mints no tasks, so the op list is empty and the
/// operation carries only the ordered `path` the server cyclifies against its
/// live graph at apply. A path must name at least one node.
fn lower_loop_operation(
    loop_op: RawLoopOperation,
) -> Result<(Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>), PatchError> {
    if loop_op.path.is_empty() {
        return Err(PatchError::Parse(
            "loop `path` must be non-empty — a loop cyclifies at least one node".to_string(),
        ));
    }
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Loop(pp::patch_operation::Loop {
            path: loop_op.path,
        })),
    });
    Ok((Vec::new(), operation))
}

/// Lower a `cut` operation document to `(ops, operation)`. A `cut` removes an
/// existing mid-path node and bridges the gap — it mints no tasks, so the op
/// list is empty and the operation carries only the `target` the server excises
/// against its live graph at apply. There is nothing to validate at parse (the
/// target is always present and mid-path / un-run enforcement is a live-graph
/// fact the server checks); this lowering is infallible.
fn lower_cut_operation(
    cut: RawCutOperation,
) -> (Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>) {
    (
        Vec::new(),
        Some(pp::PatchOperation {
            kind: Some(pp::patch_operation::Kind::Cut(pp::patch_operation::Cut {
                target: cut.target,
            })),
        }),
    )
}

/// Lower a `prune` operation document to `(ops, operation)`. A `prune` drops an
/// existing fan-out arm and narrows the join — it mints no tasks, so the op list
/// is empty and the operation carries only the `arm` head the server discovers
/// and excises against its live graph at apply. There is nothing to validate at
/// parse (the arm head is always present, and the whole-arm un-run / fan-out
/// boundary is a live-graph fact the server checks); this lowering is infallible.
fn lower_prune_operation(
    prune: RawPruneOperation,
) -> (Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>) {
    (
        Vec::new(),
        Some(pp::PatchOperation {
            kind: Some(pp::patch_operation::Kind::Prune(
                pp::patch_operation::Prune { arm: prune.arm },
            )),
        }),
    )
}

/// Lower a `trim` operation document to `(ops, operation)`. A `trim` removes the
/// leading run up to an existing anchor and reconnects `Start` — it mints no
/// tasks, so the op list is empty and the operation carries only the `anchor` the
/// server excises up to against its live graph at apply. There is nothing to
/// validate at parse (the anchor is always present, and the leading-run un-run /
/// boundary is a live-graph fact the server checks); this lowering is infallible.
fn lower_trim_operation(
    trim: RawTrimOperation,
) -> (Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>) {
    (
        Vec::new(),
        Some(pp::PatchOperation {
            kind: Some(pp::patch_operation::Kind::Trim(pp::patch_operation::Trim {
                anchor: trim.anchor,
            })),
        }),
    )
}

/// Lower a `truncate` operation document to `(ops, operation)`. A `truncate`
/// removes the tail downstream of an existing anchor and reconnects `End` — it
/// mints no tasks, so the op list is empty and the operation carries only the
/// `anchor` the server excises after against its live graph at apply. There is
/// nothing to validate at parse (the anchor is always present, and the tail un-run
/// / boundary is a live-graph fact the server checks); this lowering is infallible.
fn lower_truncate_operation(
    truncate: RawTruncateOperation,
) -> (Vec<pp::AddressedPatchOp>, Option<pp::PatchOperation>) {
    (
        Vec::new(),
        Some(pp::PatchOperation {
            kind: Some(pp::patch_operation::Kind::Truncate(
                pp::patch_operation::Truncate {
                    anchor: truncate.anchor,
                },
            )),
        }),
    )
}

/// Lower one scope's raw chain steps to the server `ChainStep` tree, minting a
/// node id per leaf task and pushing its task-bearing `AddNode` onto `ops`. A
/// step carries exactly one of a leaf `task` or a nested `steps` sub-chain
/// (recursed into a child scope).
fn lower_chain_steps(
    raw: Vec<RawChainStep>,
    ops: &mut Vec<pp::AddressedPatchOp>,
) -> Result<Vec<pp::ChainStep>, PatchError> {
    let mut out = Vec::with_capacity(raw.len());
    for step in raw {
        let node = match (step.task, step.steps) {
            (Some(_), Some(_)) => {
                return Err(PatchError::Parse(format!(
                    "chain step `{}` carries both a `task` and nested `steps` — exactly one",
                    step.handle
                )))
            }
            (None, None) => {
                return Err(PatchError::Parse(format!(
                    "chain step `{}` must carry a leaf `task` or a nested `steps` sub-chain",
                    step.handle
                )))
            }
            (Some(task), None) => {
                let (node_id, task) = lower_patch_task(&task)?;
                if !task.nix_expression_path.contains('#') {
                    return Err(PatchError::Parse(format!(
                        "chain task `{}`: `nix_expression_path` `{}` must include a `#fragment` \
                         selecting the buildable attribute",
                        step.handle, task.nix_expression_path
                    )));
                }
                // The node id minted for the built task IS the identity the leaf
                // handle binds to; the AddNode and the leaf step reference it.
                ops.push(add_node_op(node_id, Some(task)));
                chain_leaf(step.handle.clone(), node_id)
            }
            (None, Some(nested)) => {
                let nested = lower_chain_steps(nested, ops)?;
                if nested.is_empty() {
                    return Err(PatchError::Parse(format!(
                        "nested chain scope `{}` must be non-empty",
                        step.handle
                    )));
                }
                chain_nested(step.handle.clone(), nested)
            }
        };
        out.push(node);
    }
    Ok(out)
}

/// Parse a self-patch document as carried on a completing task's reserved
/// `tickr_patch` output. A string value is raw Nickel source (the task
/// authored the document; the conductor owns the parser); any other JSON
/// shape is treated as the already-evaluated document.
pub async fn parse_self_patch_document(
    value: &serde_json::Value,
) -> Result<ParsedPatch, PatchError> {
    match value {
        serde_json::Value::String(nickel_source) => parse_patch_document(nickel_source).await,
        other => parse_patch_document_json(&other.to_string()),
    }
}

/// Relay seam for the outbound `PatchWorkflowInstance` envelope. A trait so
/// the pipeline is testable against a recording sender without standing up
/// the relay client — the same decoupling `WakeupRelaySender` uses.
#[async_trait::async_trait]
pub trait PatchRelaySender: Send + Sync {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> Result<()>;
}

/// Default sender wired against the global conductor relay channel.
pub struct DefaultPatchRelaySender;

#[async_trait::async_trait]
impl PatchRelaySender for DefaultPatchRelaySender {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> Result<()> {
        crate::relay::send_patch_workflow_instance(envelope).await
    }
}

/// One patch lifecycle row, as read back for replay / re-drive.
#[derive(Debug, Clone)]
pub struct PatchRow {
    pub patch_key: Uuid,
    pub patch_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub status: String,
    pub ops: serde_json::Value,
    pub reason: Option<String>,
    pub outcome: Option<String>,
    pub applied_version: Option<i64>,
    /// Self/external authorship, persisted at ingress so a re-driven or
    /// build-finalized envelope carries the same discriminant.
    pub provenance: PatchProvenance,
    /// The un-lowered composite operation (an `insert`), persisted at ingress
    /// so the validate+apply envelope the finalizer rebuilds — and any re-drive
    /// — carries the anchor + built node id the server needs to lower the edge
    /// rewrite. `None` for a primitive-op patch.
    pub operation: Option<pp::PatchOperation>,
}

impl PatchRow {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            STATUS_APPLIED | STATUS_REJECTED | STATUS_BUILD_FAILED
        )
    }
}

fn row_from_repository(row: tickr_migrations::patch_repository::PatchLifecycleRow) -> PatchRow {
    PatchRow {
        patch_key: row.patch_key,
        patch_id: row.patch_id,
        workflow_instance_id: row.workflow_instance_id,
        status: row.status.as_str().to_owned(),
        ops: serde_json::to_value(row.ops).expect("published Patch operations serialize"),
        reason: row.reason,
        outcome: row.outcome,
        applied_version: row.applied_version,
        provenance: row.provenance,
        operation: row.operation,
    }
}

/// One build-job-per-new-task message for build-at-patch, mirroring
/// registration's `TaskBuildJob` keyed by `patch_key` instead of
/// `(workflow_id, version)`. Published after the ingress transaction commits;
/// patch build workers consume the queue with NATS queue-group semantics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PatchTaskBuildJob {
    pub patch_key: Uuid,
    pub workflow_instance_id: Uuid,
    pub task_id: Uuid,
    pub nix_expression_path: String,
}

/// NATS subject / queue group for patch-keyed per-task build jobs. A
/// dedicated subject (not registration's `conductor_build_queue`) because the
/// finalizer discipline differs: the last-one-out flip lands on the patch
/// lifecycle row and ships the apply/abandon envelope, not the workflow row
/// and the submission queue.
pub const PATCH_BUILD_QUEUE_SUBJECT: &str = "conductor_patch_build_queue";
pub const PATCH_BUILD_QUEUE_GROUP: &str = "conductor-patch-build-workers";

/// Ingress verdict for one Patch request.
#[derive(Debug)]
pub enum PatchIngress {
    /// Row open, relay under way (or queued for re-drive). Poll the row.
    /// `build_jobs` is non-empty when the Patch introduces new tasks: the row
    /// sits at `Building` and the caller must publish the jobs onto the patch
    /// build queue after this returns (publish-after-commit ordering).
    Accepted {
        patch_id: Uuid,
        patch_key: Uuid,
        build_jobs: Vec<PatchTaskBuildJob>,
    },
    /// One Patch at a time: another Patch for this instance is still
    /// unsettled. Recorded terminally `Rejected` on its own row.
    RejectedInProgress {
        patch_id: Uuid,
        patch_key: Uuid,
        reason: String,
    },
    /// The `patch_key` already holds a row — a redelivered request. The row
    /// replays its state; a terminal row's recorded outcome is never re-run
    /// and the envelope is NOT re-relayed here (the re-drive loop owns
    /// non-terminal redelivery).
    Replayed { row: PatchRow },
}

/// Ingress one Patch request: dedup by `patch_key`, enforce
/// one-Patch-at-a-time, persist the row, then relay and flip
/// `Validating → Submitted`. The row commit happens *before* the relay
/// attempt — a send failure leaves a durable `Validating` row for the
/// re-drive loop, so the request can never be lost after it was acknowledged.
///
/// `patch_id` is minted by the caller (the command consumer) so that a
/// redelivered internal request can carry the same identity into the same
/// row; a fresh external submit always mints fresh.
pub async fn process_patch(
    repositories: &WriterRepositoryBundle,
    sender: &dyn PatchRelaySender,
    workflow_instance_id: Uuid,
    patch_id: Uuid,
    parsed: ParsedPatch,
    provenance: PatchProvenance,
) -> Result<PatchIngress, PatchError> {
    let key = patch_key(workflow_instance_id, patch_id);
    let new_tasks: Vec<(Uuid, wf::TaskDefinition)> = parsed
        .new_tasks()
        .into_iter()
        .map(|(id, task)| (id, task.clone()))
        .collect();
    let task_specifications = new_tasks
        .iter()
        .map(|(task_id, task)| PatchTaskSpecification {
            task_id: *task_id,
            routing_vars: &task.routing_vars,
        })
        .collect();

    let ingress = repositories
        .ingress_patch(RepositoryPatchIngressInput {
            patch_key: key,
            patch_id,
            workflow_instance_id,
            ops: &parsed.ops,
            operation: parsed.operation.as_ref(),
            reason: parsed.reason.as_deref(),
            provenance,
            source: &parsed.source.text,
            source_format: parsed.source.format,
            tasks: task_specifications,
        })
        .await?;

    match ingress {
        RepositoryPatchIngressOutcome::Existing { row } => {
            return Ok(PatchIngress::Replayed {
                row: row_from_repository(row),
            });
        }
        RepositoryPatchIngressOutcome::RejectedInProgress { reason } => {
            return Ok(PatchIngress::RejectedInProgress {
                patch_id,
                patch_key: key,
                reason,
            });
        }
        RepositoryPatchIngressOutcome::Accepted { .. } => {}
    }

    if new_tasks.is_empty() {
        let envelope = pp::PatchEnvelope {
            workflow_instance_id: workflow_instance_id.to_string(),
            patch_key: key.to_string(),
            ops: parsed.ops,
            reason: parsed.reason,
            provenance: provenance.to_proto(),
            operation: parsed.operation,
        };
        match sender.send(&envelope).await {
            Ok(()) => {
                repositories.mark_patch_submitted(key).await?;
            }
            Err(error) => {
                eprintln!("patch relay send failed for {key} (will re-drive): {error}");
            }
        }
        return Ok(PatchIngress::Accepted {
            patch_id,
            patch_key: key,
            build_jobs: Vec::new(),
        });
    }

    let build_jobs = new_tasks
        .into_iter()
        .map(|(task_id, task)| PatchTaskBuildJob {
            patch_key: key,
            workflow_instance_id,
            task_id,
            nix_expression_path: task.nix_expression_path,
        })
        .collect();
    Ok(PatchIngress::Accepted {
        patch_id,
        patch_key: key,
        build_jobs,
    })
}

/// Publish patch build jobs onto the patch build queue. Called after the
/// ingress transaction committed (publish-after-commit ordering). A publish
/// failure leaves the row at `Building` — loud, pollable, and terminal only
/// via operator resubmission; the stall-TTL backstop resumes the instance.
pub async fn publish_patch_build_jobs(
    nats: &async_nats::Client,
    jobs: &[PatchTaskBuildJob],
) -> Result<()> {
    for job in jobs {
        let payload = bincode::serialize(job)
            .map_err(|e| anyhow::anyhow!("serialize PatchTaskBuildJob: {e}"))?;
        nats.publish(PATCH_BUILD_QUEUE_SUBJECT, payload.into())
            .await
            .map_err(|e| anyhow::anyhow!("publish PatchTaskBuildJob: {e}"))?;
    }
    nats.flush()
        .await
        .map_err(|e| anyhow::anyhow!("flush patch build jobs: {e}"))?;
    Ok(())
}

/// Typed result of one atomic patch-build settlement operation.
///
/// Only `Submitted` carries an apply-publication intent; every losing outcome
/// is side-effect-free at the caller.
pub use tickr_migrations::patch_repository::PatchBuildSettlementOutcome;
pub type PatchBuildFinalize = PatchBuildSettlementOutcome;

/// Atomically records one per-Task result and evaluates the aggregate Patch
/// lifecycle. The repository commits the winning `Submitted` transition before
/// returning its apply-publication intent. Only that winner sends the envelope;
/// a failed send leaves the durable `Submitted` row for re-drive.
pub async fn finalize_patch_after_build(
    repositories: &WriterRepositoryBundle,
    sender: &dyn PatchRelaySender,
    patch_key: Uuid,
    task_id: Uuid,
    outcome: &BuildOutcome,
) -> Result<PatchBuildFinalize> {
    let result = match outcome {
        BuildOutcome::Success => PatchTaskBuildResult::Success,
        BuildOutcome::Failure { error } => PatchTaskBuildResult::Failure { error },
    };
    let settlement = repositories
        .settle_patch_task_build(patch_key, task_id, result)
        .await?;

    if let PatchBuildFinalize::Submitted(row) = &settlement {
        let envelope = envelope_from_repository_row(row);
        if let Err(error) = sender.send(&envelope).await {
            eprintln!(
                "patch validate+apply relay send failed for {patch_key} \
                 (will re-drive): {error}"
            );
        }
    }

    Ok(settlement)
}

/// One patch-build job: build via the injected executor (the same
/// `BuildExecutor` seam registration uses — a `TaskBuildJob` shell carries
/// the patch identity in `workflow_id` purely for the executor's logs),
/// settle the per-task result, and run the aggregate finalizer.
async fn process_patch_build_job(
    repositories: &WriterRepositoryBundle,
    executor: &dyn BuildExecutor,
    sender: &dyn PatchRelaySender,
    msg: async_nats::Message,
) {
    let job: PatchTaskBuildJob = match bincode::deserialize(&msg.payload) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("patch build worker: malformed PatchTaskBuildJob: {e}");
            return;
        }
    };
    let build_input = TaskBuildJob {
        workflow_id: job.patch_key,
        workflow_version: 0,
        task_id: job.task_id,
        nix_expression_path: job.nix_expression_path,
    };
    let outcome = executor.build(&build_input).await;
    if let Err(error) =
        finalize_patch_after_build(repositories, sender, job.patch_key, job.task_id, &outcome).await
    {
        eprintln!(
            "patch build worker: settlement failed for {}/{}: {error}",
            job.patch_key, job.task_id
        );
    }
}

/// Patch build worker: queue-group consumer over the patch build queue, the
/// patch-keyed sibling of registration's `start_build_worker`. Runs until
/// the cancellation token fires.
pub async fn start_patch_build_worker(
    nats: async_nats::Client,
    repositories: Arc<WriterRepositoryBundle>,
    executor: Arc<dyn BuildExecutor>,
    sender: Arc<dyn PatchRelaySender>,
    cancel: CancellationToken,
) -> Result<()> {
    use futures::StreamExt;
    println!(
        "Starting conductor patch build worker on {}",
        PATCH_BUILD_QUEUE_SUBJECT
    );
    let mut sub = nats
        .queue_subscribe(PATCH_BUILD_QUEUE_SUBJECT, PATCH_BUILD_QUEUE_GROUP.into())
        .await?;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                println!("Patch build worker received shutdown signal.");
                break;
            }
            Some(msg) = sub.next() => {
                process_patch_build_job(&repositories, executor.as_ref(), sender.as_ref(), msg).await;
            }
            else => {
                println!("Patch build queue subscription ended.");
                break;
            }
        }
    }
    Ok(())
}

/// Rebuild the single validate+apply relay envelope for a decoded repository
/// row.
fn envelope_from_repository_row(
    row: &tickr_migrations::patch_repository::PatchLifecycleRow,
) -> pp::PatchEnvelope {
    pp::PatchEnvelope {
        workflow_instance_id: row.workflow_instance_id.to_string(),
        patch_key: row.patch_key.to_string(),
        ops: row.ops.clone(),
        reason: row.reason.clone(),
        provenance: row.provenance.to_proto(),
        operation: row.operation.clone(),
    }
}

/// Correlate a server `PatchOutcome` envelope onto its lifecycle row. The
/// conditional UPDATE is the terminal-state guard: only a non-terminal row
/// settles, so duplicate outcomes (re-drive echoes, redelivered envelopes)
/// are absorbed silently.
#[derive(Debug, PartialEq, Eq)]
pub enum OutcomeCorrelation {
    Settled,
    /// The row was already terminal, or the key is unknown (an outcome for a
    /// patch this conductor never ingressed). Absorbed.
    Absorbed,
}

pub async fn correlate_outcome(
    repositories: &WriterRepositoryBundle,
    outcome: &pp::PatchOutcome,
) -> Result<OutcomeCorrelation, tickr_migrations::backend::RepositoryError> {
    let patch_key = match Uuid::parse_str(&outcome.patch_key) {
        Ok(key) => key,
        Err(_) => return Ok(OutcomeCorrelation::Absorbed),
    };
    let workflow_instance_id = match Uuid::parse_str(&outcome.workflow_instance_id) {
        Ok(id) => id,
        Err(_) => return Ok(OutcomeCorrelation::Absorbed),
    };
    let terminal_outcome = match outcome
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.kind.as_ref())
    {
        Some(pp::patch_outcome_kind::Kind::Applied(applied)) => PatchTerminalOutcome::Applied {
            version: i64::from(applied.version),
        },
        Some(pp::patch_outcome_kind::Kind::Rejected(rejected)) => PatchTerminalOutcome::Rejected {
            reason: &rejected.reason,
        },
        None => return Ok(OutcomeCorrelation::Absorbed),
    };
    Ok(
        match repositories
            .correlate_patch_outcome(PatchOutcomeInput {
                patch_key,
                workflow_instance_id,
                outcome: terminal_outcome,
            })
            .await?
        {
            PatchOutcomeCorrelation::Won => OutcomeCorrelation::Settled,
            PatchOutcomeCorrelation::AlreadySettled(_)
            | PatchOutcomeCorrelation::Absent
            | PatchOutcomeCorrelation::Conflicted => OutcomeCorrelation::Absorbed,
        },
    )
}

/// One re-drive pass: re-send every unsettled `Validating` / `Submitted` row
/// that has sat untouched for at least `min_age`, bumping `updated_at` on
/// each successful send (the backoff anchor). Returns how many envelopes
/// were re-sent. A send failure leaves the row for the next pass — the relay
/// is self-healing.
///
/// `Building` rows are deliberately NOT re-driven: their builds are in flight
/// and settle through the finalizer. Every re-driven row re-sends the same
/// single validate+apply envelope — build-then-apply has no phase to derive,
/// and the server's apply-time re-validation + redelivery dedup make a resend
/// idempotent.
pub async fn redrive_unsettled(
    repositories: &WriterRepositoryBundle,
    sender: &dyn PatchRelaySender,
    min_age: Duration,
) -> Result<usize, tickr_migrations::backend::RepositoryError> {
    let rows = repositories.unsettled_patches_older_than(min_age).await?;
    let mut sent = 0usize;
    for candidate in rows {
        let row = match candidate {
            PatchRedriveCandidate::Ready(row) => row,
            PatchRedriveCandidate::Corrupt { identity, error } => {
                eprintln!(
                    "patch re-drive: stored operation for {identity} is corrupt and was skipped: {error}"
                );
                continue;
            }
        };
        let envelope = envelope_from_repository_row(&row);
        match sender.send(&envelope).await {
            Ok(()) => {
                repositories.mark_patch_submitted(row.patch_key).await?;
                sent += 1;
            }
            Err(error) => {
                eprintln!(
                    "patch re-drive send failed for {} (will retry): {}",
                    row.patch_key, error
                );
            }
        }
    }
    Ok(sent)
}

/// The steady-state re-drive loop: every `REDRIVE_INTERVAL`, re-send
/// unsettled rows older than `REDRIVE_MIN_AGE` until shutdown.
pub async fn run_patch_redrive(
    repositories: Arc<WriterRepositoryBundle>,
    sender: Arc<dyn PatchRelaySender>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                println!("patch re-drive: shutdown signal received");
                return;
            }
            _ = tokio::time::sleep(REDRIVE_INTERVAL) => {
                match redrive_unsettled(&repositories, sender.as_ref(), REDRIVE_MIN_AGE).await {
                    Ok(0) => {}
                    Ok(n) => println!("patch re-drive: re-sent {n} unsettled patch(es)"),
                    Err(e) => eprintln!("patch re-drive pass failed: {e}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The composite operation's inner kind, for assertion. Panics if the parsed
    /// patch carries no operation — every operation test expects one.
    fn op_kind(parsed: &ParsedPatch) -> pp::patch_operation::Kind {
        parsed
            .operation
            .as_ref()
            .expect("operation present")
            .kind
            .clone()
            .expect("operation kind present")
    }

    /// True when the chain step is a leaf task.
    fn step_is_task(s: &pp::ChainStep) -> bool {
        matches!(
            s.node.as_ref().and_then(|n| n.node.as_ref()),
            Some(pp::step_node::Node::Task(_))
        )
    }

    /// The step count of a nested chain step, or `None` if it is a leaf task.
    fn step_nested_len(s: &pp::ChainStep) -> Option<usize> {
        match s.node.as_ref().and_then(|n| n.node.as_ref()) {
            Some(pp::step_node::Node::Nested(n)) => Some(n.steps.len()),
            _ => None,
        }
    }

    #[test]
    fn patch_key_is_stable_and_instance_scoped() {
        let wi = Uuid::new_v4();
        let pid = Uuid::new_v4();
        assert_eq!(patch_key(wi, pid), patch_key(wi, pid));
        assert_ne!(patch_key(wi, pid), patch_key(Uuid::new_v4(), pid));
        assert_ne!(patch_key(wi, pid), patch_key(wi, Uuid::new_v4()));
    }

    #[test]
    fn well_formed_document_parses_to_kernel_ops() {
        let node = Uuid::new_v4();
        let json = format!(
            r#"{{ "ops": [ {{ "AddNode": {{ "node_id": "{node}" }} }},
                           {{ "RemoveEdge": {{ "edge_id": "{node}" }} }} ],
                  "reason": "fan-out" }}"#
        );
        let parsed = parse_patch_document_json(&json).expect("parse");
        assert_eq!(parsed.ops.len(), 2);
        assert_eq!(parsed.reason.as_deref(), Some("fan-out"));
        assert_eq!(parsed.ops[0].added_node_id(), Some(node));
    }

    #[test]
    fn document_without_ops_is_rejected() {
        assert!(matches!(
            parse_patch_document_json(r#"{ "ops": [] }"#),
            Err(PatchError::Parse(_))
        ));
        assert!(matches!(
            parse_patch_document_json(r#"{ "reason": "no ops key" }"#),
            Err(PatchError::Parse(_))
        ));
    }

    #[test]
    fn document_with_unknown_op_is_rejected() {
        let json = r#"{ "ops": [ { "RedirectEdge": { "edge_id": "00000000-0000-0000-0000-000000000000" } } ] }"#;
        assert!(matches!(
            parse_patch_document_json(json),
            Err(PatchError::Parse(_))
        ));
    }

    /// A task JSON body in the `mkTask` document shape.
    fn task_json(name: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name, "command": "shell", "args": ["run"], "outputs": ["report"],
            "nix_expression_path": "/patch/enrich.nix",
            "routing_vars": [ { "name": "verdict", "kind": "routing-var", "type": "string" } ]
        })
    }

    /// A task JSON body for an insert, carrying the given nix expression.
    fn insert_task_json(nix: &str) -> serde_json::Value {
        serde_json::json!({
            "name": "inserted", "command": "shell", "args": ["run"],
            "outputs": ["report"], "nix_expression_path": nix
        })
    }

    /// The Patch document is a tagged union: an `insert` document lowers to a
    /// single task-bearing `AddNode` (the inserted task, built at-patch under a
    /// freshly-minted node id) plus the un-lowered `insert` operation, whose
    /// anchor is the author's code and whose node id is the SAME minted id.
    #[test]
    fn insert_document_lowers_to_add_node_and_operation() {
        let doc = serde_json::json!({
            "insert": { "anchor": "AB12", "task": insert_task_json("flake#enrich") },
            "reason": "splice enrich after AB12"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse insert");
        assert_eq!(
            parsed.ops.len(),
            1,
            "the inserted task rides ops as an AddNode"
        );
        let node_id = parsed.ops[0].added_node_id().expect("add node");
        let task = parsed.ops[0].added_task().expect("task carried");
        assert_eq!(
            task.id,
            node_id.to_string(),
            "AddNode id IS the built task id"
        );
        assert_eq!(
            parsed.new_tasks().len(),
            1,
            "insert always rides the build path"
        );
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Insert(i) => {
                assert_eq!(i.anchor, "AB12");
                assert_eq!(
                    i.node_id,
                    node_id.to_string(),
                    "operation wires to the same minted node id"
                );
            }
            other => panic!("expected an insert operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("splice enrich after AB12"));
    }

    /// A `swap` document lowers to one task-bearing `AddNode` (the replacement,
    /// built at-patch under a freshly-minted node id) plus the un-lowered `swap`
    /// operation, whose target is the author's code and whose node id is the
    /// SAME minted id the AddNode carries.
    #[test]
    fn swap_document_lowers_to_add_node_and_operation() {
        let doc = serde_json::json!({
            "swap": { "target": "AB12", "task": insert_task_json("flake#replacement") },
            "reason": "substitute AB12"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse swap");
        assert_eq!(
            parsed.ops.len(),
            1,
            "the replacement task rides ops as an AddNode"
        );
        let node_id = parsed.ops[0].added_node_id().expect("add node");
        let task = parsed.ops[0].added_task().expect("task carried");
        assert_eq!(
            task.id,
            node_id.to_string(),
            "AddNode id IS the built task id"
        );
        assert_eq!(
            parsed.new_tasks().len(),
            1,
            "swap always rides the build path"
        );
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Swap(s) => {
                assert_eq!(s.target, "AB12");
                assert_eq!(
                    s.node_id,
                    node_id.to_string(),
                    "operation wires to the same minted node id"
                );
            }
            other => panic!("expected a swap operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("substitute AB12"));
    }

    /// A `loop` document lowers to an empty op list (it cyclifies existing
    /// structure, minting no tasks) plus the un-lowered `loop` operation
    /// carrying the ordered path of identity codes the server cyclifies.
    #[test]
    fn loop_document_lowers_to_operation_with_no_ops() {
        let doc = serde_json::json!({
            "loop": { "path": ["AB12", "CD34"] },
            "reason": "cyclify AB12→CD34"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse loop");
        assert!(parsed.ops.is_empty(), "a loop mints no tasks");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Loop(l) => {
                assert_eq!(l.path, vec!["AB12".to_string(), "CD34".to_string()]);
            }
            other => panic!("expected a loop operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("cyclify AB12→CD34"));
    }

    /// A `loop` with an empty path is rejected at parse.
    #[test]
    fn loop_document_with_empty_path_is_rejected() {
        let doc = serde_json::json!({ "loop": { "path": [] } });
        assert!(parse_patch_document_json(&doc.to_string()).is_err());
    }

    /// A `cut` document lowers to an empty op list (it excises existing
    /// structure, minting no tasks) plus the un-lowered `cut` operation carrying
    /// the target identity code the server excises and bridges at apply.
    #[test]
    fn cut_document_lowers_to_operation_with_no_ops() {
        let doc = serde_json::json!({
            "cut": { "target": "AB12" },
            "reason": "drop AB12"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse cut");
        assert!(parsed.ops.is_empty(), "a cut mints no tasks");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Cut(c) => {
                assert_eq!(c.target, "AB12");
            }
            other => panic!("expected a cut operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("drop AB12"));
    }

    /// A `prune` document lowers to a `Prune` operation naming the fan-out arm by
    /// its head's identity code, minting no tasks — the server discovers the arm
    /// and narrows the join at apply. This is the conductor end of the end-to-end
    /// slice (DSL → parse → server heal).
    #[test]
    fn prune_document_lowers_to_operation_with_no_ops() {
        let doc = serde_json::json!({
            "prune": { "arm": "CD34" },
            "reason": "branch no longer relevant"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse prune");
        assert!(parsed.ops.is_empty(), "a prune mints no tasks");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Prune(p) => {
                assert_eq!(p.arm, "CD34");
            }
            other => panic!("expected a prune operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("branch no longer relevant"));
    }

    /// A document naming two operation arms (`cut` and `prune`) is ambiguous and
    /// rejected by the tagged-union guard.
    #[test]
    fn a_document_with_two_operation_arms_is_rejected() {
        let doc = serde_json::json!({
            "cut": { "target": "AB12" },
            "prune": { "arm": "CD34" },
        });
        assert!(matches!(
            parse_patch_document_json(&doc.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// A `trim` document lowers to a `Trim` operation naming the anchor by its
    /// identity code, minting no tasks — the server discovers the leading run and
    /// reconnects `Start` at apply. This is the conductor end of the end-to-end
    /// slice (DSL → parse → server heal).
    #[test]
    fn trim_document_lowers_to_operation_with_no_ops() {
        let doc = serde_json::json!({
            "trim": { "anchor": "AB12" },
            "reason": "drop the leading section"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse trim");
        assert!(parsed.ops.is_empty(), "a trim mints no tasks");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Trim(t) => {
                assert_eq!(t.anchor, "AB12");
            }
            other => panic!("expected a trim operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("drop the leading section"));
    }

    /// A `truncate` document lowers to a `Truncate` operation naming the anchor by
    /// its identity code, minting no tasks — the server discovers the tail and
    /// reconnects `End` at apply.
    #[test]
    fn truncate_document_lowers_to_operation_with_no_ops() {
        let doc = serde_json::json!({
            "truncate": { "anchor": "CD34" },
            "reason": "lop off the tail"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse truncate");
        assert!(parsed.ops.is_empty(), "a truncate mints no tasks");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Truncate(t) => {
                assert_eq!(t.anchor, "CD34");
            }
            other => panic!("expected a truncate operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("lop off the tail"));
    }

    /// A `swap` whose replacement task lacks a `#fragment` is rejected at parse —
    /// a substitution is meaningless without a buildable replacement.
    #[test]
    fn swap_document_without_buildable_fragment_is_rejected() {
        let doc = serde_json::json!({
            "swap": { "target": "AB12", "task": insert_task_json("flake") },
        });
        assert!(matches!(
            parse_patch_document_json(&doc.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// The other arm of the tagged union: a primitive-op-only patch (slice 03)
    /// parses unchanged, carrying no operation.
    #[test]
    fn primitive_op_document_parses_with_no_operation() {
        let node = Uuid::new_v4();
        let json = format!(r#"{{ "ops": [ {{ "AddNode": {{ "node_id": "{node}" }} }} ] }}"#);
        let parsed = parse_patch_document_json(&json).expect("parse primitive");
        assert!(parsed.operation.is_none());
        assert_eq!(parsed.ops.len(), 1);
    }

    /// A document naming both arms, or neither, is rejected — exactly one shape.
    #[test]
    fn document_with_both_or_neither_arm_is_rejected() {
        let both = serde_json::json!({
            "ops": [ { "AddNode": { "node_id": Uuid::new_v4() } } ],
            "insert": { "anchor": "AB12", "task": insert_task_json("flake#x") }
        });
        assert!(matches!(
            parse_patch_document_json(&both.to_string()),
            Err(PatchError::Parse(_))
        ));
        let neither = serde_json::json!({ "reason": "no shape" });
        assert!(matches!(
            parse_patch_document_json(&neither.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// A `chain` document lowers to one task-bearing `AddNode` per leaf (built
    /// at-patch) plus the un-lowered `Chain` operation — a scope-tree of handles
    /// bound to those minted ids, threaded in declaration order.
    #[test]
    fn chain_document_lowers_to_add_nodes_and_operation() {
        let doc = serde_json::json!({
            "chain": { "anchor": "AB12", "steps": [
                { "handle": "a", "task": insert_task_json("flake#a") },
                { "handle": "b", "task": insert_task_json("flake#b") },
            ]},
            "reason": "extend after AB12"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse chain");
        assert_eq!(parsed.ops.len(), 2, "one AddNode per leaf task");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Chain(c) => {
                assert_eq!(c.anchor, "AB12");
                assert_eq!(c.steps.len(), 2);
                for (i, step) in c.steps.iter().enumerate() {
                    match step.node.as_ref().and_then(|n| n.node.as_ref()) {
                        Some(pp::step_node::Node::Task(t)) => {
                            assert_eq!(
                                t.node_id,
                                parsed.ops[i].added_node_id().unwrap().to_string()
                            );
                        }
                        other => panic!("expected a leaf task step, got {other:?}"),
                    }
                }
            }
            other => panic!("expected a chain operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("extend after AB12"));
    }

    /// A `chain` document's optional `relay = { edge, var }` parses through to the
    /// `Chain` operation's `relay` field — the per-edge spec the server needs to
    /// select the variable-absent arm for a Data-edge anchor.
    #[test]
    fn chain_document_carries_the_relay_spec() {
        let doc = serde_json::json!({
            "chain": {
                "anchor": "AB12",
                "steps": [ { "handle": "a", "task": insert_task_json("flake#a") } ],
                "relay": { "edge": "CD34", "var": "verdict" },
            }
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse chain with relay");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Chain(c) => {
                let relay = c.relay.expect("relay parsed");
                assert_eq!(relay.edge, "CD34");
                assert_eq!(relay.var, "verdict");
            }
            other => panic!("expected a chain operation with a relay, got {other:?}"),
        }
    }

    /// A nested sub-chain forms a child scope: the same handle string may recur
    /// across sibling scopes (disambiguated by scope path), but a duplicate
    /// handle **within one scope** is rejected at conductor parse.
    #[test]
    fn chain_nesting_scopes_handles_and_rejects_within_scope_duplicates() {
        // `a` at the root scope and `a` inside the `grp` scope — accepted.
        let ok = serde_json::json!({ "chain": { "anchor": "AB12", "steps": [
            { "handle": "a", "task": insert_task_json("flake#a") },
            { "handle": "grp", "steps": [
                { "handle": "a", "task": insert_task_json("flake#c") }
            ]},
        ]}});
        let parsed =
            parse_patch_document_json(&ok.to_string()).expect("sibling-scope reuse is fine");
        assert_eq!(parsed.ops.len(), 2, "one AddNode per leaf across scopes");

        // Two `a` handles in the same (root) scope — rejected.
        let dup = serde_json::json!({ "chain": { "anchor": "AB12", "steps": [
            { "handle": "a", "task": insert_task_json("flake#a") },
            { "handle": "a", "task": insert_task_json("flake#b") },
        ]}});
        assert!(matches!(
            parse_patch_document_json(&dup.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// A `fork` document lowers to one task-bearing `AddNode` per leaf (built
    /// at-patch) plus the un-lowered `Fork` operation — a list of arms, each a
    /// chain step, the anchor bound to the author's code. An arm may be a single
    /// leaf task or a nested sub-chain.
    #[test]
    fn fork_document_lowers_to_add_nodes_and_operation() {
        let doc = serde_json::json!({
            "fork": { "anchor": "AB12", "arms": [
                { "handle": "left", "task": insert_task_json("flake#l") },
                { "handle": "right", "steps": [
                    { "handle": "r1", "task": insert_task_json("flake#r1") },
                    { "handle": "r2", "task": insert_task_json("flake#r2") },
                ]},
            ]},
            "reason": "fan out after AB12"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse fork");
        assert_eq!(parsed.ops.len(), 3, "one AddNode per leaf task across arms");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Fork(fk) => {
                assert_eq!(fk.anchor, "AB12");
                assert_eq!(fk.arms.len(), 2, "two arms");
                // First arm is a single leaf task; second is a nested sub-chain.
                assert!(step_is_task(&fk.arms[0]));
                assert_eq!(step_nested_len(&fk.arms[1]), Some(2));
            }
            other => panic!("expected a fork operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("fan out after AB12"));
    }

    /// A fork's arms are scopes: the same handle string may recur across sibling
    /// arms (disambiguated by scope path), but a duplicate handle **within one
    /// scope** is rejected, and an armless fork is rejected.
    #[test]
    fn fork_scopes_handles_and_rejects_empty_or_duplicate() {
        // `x` reused across two sibling arms — accepted (distinct scopes).
        let ok = serde_json::json!({ "fork": { "anchor": "AB12", "arms": [
            { "handle": "a", "steps": [ { "handle": "x", "task": insert_task_json("flake#a") } ] },
            { "handle": "b", "steps": [ { "handle": "x", "task": insert_task_json("flake#b") } ] },
        ]}});
        let parsed = parse_patch_document_json(&ok.to_string()).expect("sibling-arm reuse is fine");
        assert_eq!(parsed.ops.len(), 2);

        // Two `arm` handles at the root (arm-list) scope — rejected.
        let dup = serde_json::json!({ "fork": { "anchor": "AB12", "arms": [
            { "handle": "arm", "task": insert_task_json("flake#a") },
            { "handle": "arm", "task": insert_task_json("flake#b") },
        ]}});
        assert!(matches!(
            parse_patch_document_json(&dup.to_string()),
            Err(PatchError::Parse(_))
        ));

        // An armless fork changes nothing — rejected.
        let empty = serde_json::json!({ "fork": { "anchor": "AB12", "arms": [] } });
        assert!(matches!(
            parse_patch_document_json(&empty.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// AC (branch): a `branch` document lowers to one task-bearing `AddNode` per
    /// leaf plus a `PatchOperation::Branch` whose arms each carry a projected
    /// selecting gate — the `mkSignalGate` / `mkPredicateGate` bodies become
    /// runtime `Gate` values, `Idle`, ready to gate the fan-out. An arm may be a
    /// single leaf task or a nested sub-chain.
    #[test]
    fn branch_document_lowers_to_add_nodes_and_gated_operation() {
        let doc = serde_json::json!({
            "branch": { "anchor": "AB12", "arms": [
                {
                    "handle": "left",
                    "gate": { "kind": "signal-gate", "signal": { "name": "go_left" } },
                    "task": insert_task_json("flake#l")
                },
                {
                    "handle": "right",
                    "gate": {
                        "kind": "predicate-gate", "routing_var": "verdict",
                        "op": "Eq", "value": "go"
                    },
                    "steps": [
                        { "handle": "r1", "task": insert_task_json("flake#r1") },
                        { "handle": "r2", "task": insert_task_json("flake#r2") },
                    ]
                },
            ]},
            "reason": "gated fan-out after AB12"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse branch");
        assert_eq!(parsed.ops.len(), 3, "one AddNode per leaf task across arms");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Branch(b) => {
                assert_eq!(b.anchor, "AB12");
                assert_eq!(b.arms.len(), 2, "two gated arms");
                // Arm gates projected onto proto gates (declaration only).
                assert!(matches!(
                    b.arms[0].gate.as_ref().and_then(|g| g.kind.as_ref()),
                    Some(wf::gate::Kind::SignalReceived(sr)) if sr.signal_name == "go_left"
                ));
                assert!(matches!(
                    b.arms[1].gate.as_ref().and_then(|g| g.kind.as_ref()),
                    Some(wf::gate::Kind::PredicateHolds(ph)) if ph.routing_var == "verdict"
                ));
                // Arm scope-trees: a single leaf and a nested sub-chain.
                assert!(step_is_task(b.arms[0].arm.as_ref().unwrap()));
                assert_eq!(step_nested_len(b.arms[1].arm.as_ref().unwrap()), Some(2));
            }
            other => panic!("expected a branch operation, got {other:?}"),
        }
        assert_eq!(parsed.reason.as_deref(), Some("gated fan-out after AB12"));
    }

    /// A branch's arms are scopes: the same handle string may recur across
    /// sibling arms (disambiguated by scope path), but a duplicate handle
    /// **within one scope** is rejected, an armless branch is rejected, and a
    /// malformed arm gate is rejected at parse.
    #[test]
    fn branch_scopes_handles_and_rejects_empty_duplicate_or_malformed_gate() {
        let sig =
            |name: &str| serde_json::json!({ "kind": "signal-gate", "signal": { "name": name } });

        // `x` reused across two sibling arms — accepted (distinct scopes).
        let ok = serde_json::json!({ "branch": { "anchor": "AB12", "arms": [
            { "handle": "a", "gate": sig("go_a"),
              "steps": [ { "handle": "x", "task": insert_task_json("flake#a") } ] },
            { "handle": "b", "gate": sig("go_b"),
              "steps": [ { "handle": "x", "task": insert_task_json("flake#b") } ] },
        ]}});
        let parsed = parse_patch_document_json(&ok.to_string()).expect("sibling-arm reuse is fine");
        assert_eq!(parsed.ops.len(), 2);

        // Two `arm` handles at the root scope — rejected.
        let dup = serde_json::json!({ "branch": { "anchor": "AB12", "arms": [
            { "handle": "arm", "gate": sig("go_a"), "task": insert_task_json("flake#a") },
            { "handle": "arm", "gate": sig("go_b"), "task": insert_task_json("flake#b") },
        ]}});
        assert!(matches!(
            parse_patch_document_json(&dup.to_string()),
            Err(PatchError::Parse(_))
        ));

        // An armless branch changes nothing — rejected.
        let empty = serde_json::json!({ "branch": { "anchor": "AB12", "arms": [] } });
        assert!(matches!(
            parse_patch_document_json(&empty.to_string()),
            Err(PatchError::Parse(_))
        ));

        // A malformed arm gate (empty signal name) is rejected at projection.
        let bad_gate = serde_json::json!({ "branch": { "anchor": "AB12", "arms": [
            { "handle": "a", "gate": sig(""), "task": insert_task_json("flake#a") },
        ]}});
        assert!(matches!(
            parse_patch_document_json(&bad_gate.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// AC: a patch-time `expand` over a runtime map lowers to the same fork
    /// shape server-side. The document carries `over` — a map whose keys are arm
    /// handles and whose values carry each arm's task/sub-chain — and lowers to
    /// one task-bearing `AddNode` per leaf plus a `PatchOperation::Expand` whose
    /// arms are the map entries (key → handle), in sorted-key order.
    #[test]
    fn expand_document_lowers_to_add_nodes_and_expand_operation() {
        let doc = serde_json::json!({
            "expand": { "anchor": "AB12", "over": {
                "left":  { "task": insert_task_json("flake#l") },
                "right": { "steps": [
                    { "handle": "r1", "task": insert_task_json("flake#r1") },
                    { "handle": "r2", "task": insert_task_json("flake#r2") },
                ]},
            }},
            "reason": "fan out over discovered set"
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse expand");
        assert_eq!(parsed.ops.len(), 3, "one AddNode per leaf task across arms");
        match op_kind(&parsed) {
            pp::patch_operation::Kind::Expand(x) => {
                assert_eq!(x.anchor, "AB12");
                assert_eq!(x.arms.len(), 2, "two arms, one per map element");
                // Sorted-key order: `left` (leaf task) before `right` (nested).
                assert_eq!(x.arms[0].handle, "left");
                assert!(step_is_task(&x.arms[0]));
                assert_eq!(x.arms[1].handle, "right");
                assert_eq!(step_nested_len(&x.arms[1]), Some(2));
            }
            other => panic!("expected an expand operation, got {other:?}"),
        }
        assert_eq!(
            parsed.reason.as_deref(),
            Some("fan out over discovered set")
        );
    }

    /// An expand's arms carry exactly one of a leaf `task` or nested `steps`, an
    /// empty map is rejected (nothing to fan over), and `expand` is mutually
    /// exclusive with the other tags.
    #[test]
    fn expand_rejects_empty_ambiguous_or_malformed_arm() {
        // An expand over an empty map changes nothing — rejected.
        let empty = serde_json::json!({ "expand": { "anchor": "AB12", "over": {} } });
        assert!(matches!(
            parse_patch_document_json(&empty.to_string()),
            Err(PatchError::Parse(_))
        ));

        // An arm value carrying neither a task nor steps — rejected.
        let neither = serde_json::json!({ "expand": { "anchor": "AB12", "over": {
            "a": {}
        }}});
        assert!(matches!(
            parse_patch_document_json(&neither.to_string()),
            Err(PatchError::Parse(_))
        ));

        // Two tags at once (`expand` and `fork`) is ambiguous — rejected.
        let ambiguous = serde_json::json!({
            "expand": { "anchor": "AB12", "over": { "a": { "task": insert_task_json("flake#a") } } },
            "fork":   { "anchor": "AB12", "arms": [ { "handle": "b", "task": insert_task_json("flake#b") } ] },
        });
        assert!(matches!(
            parse_patch_document_json(&ambiguous.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// Retire the hardcoded build-at-patch default: an insert task with no nix
    /// expression is rejected loudly at ingress, not silently built against a
    /// canned expression.
    #[test]
    fn insert_task_without_nix_expression_is_rejected() {
        let mut task = insert_task_json("flake#enrich");
        task.as_object_mut().unwrap().remove("nix_expression_path");
        let doc = serde_json::json!({ "insert": { "anchor": "AB12", "task": task } });
        assert!(matches!(
            parse_patch_document_json(&doc.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// An insert task whose nix expression carries no `#fragment` is rejected —
    /// the fragment selects the buildable attribute, without which the exec-side
    /// `nix run` is inert.
    #[test]
    fn insert_task_without_fragment_is_rejected() {
        let doc = serde_json::json!({
            "insert": { "anchor": "AB12", "task": insert_task_json("/no/fragment.nix") }
        });
        assert!(matches!(
            parse_patch_document_json(&doc.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// Build-at-patch lowering: a task-bearing `AddNode` mints a fresh node id
    /// (never trusting the document's placeholder) and rewrites every later
    /// reference to the placeholder — edges and removals alike.
    #[test]
    fn task_bearing_add_node_mints_fresh_id_and_rewrites_references() {
        let placeholder = Uuid::new_v4();
        let existing = Uuid::new_v4();
        let doc = serde_json::json!({
            "ops": [
                { "AddNode": { "node_id": placeholder, "task": task_json("enrich") } },
                { "AddEdge": { "sources": [existing], "targets": [placeholder],
                               "kind": "Control", "gates": [] } },
            ]
        });
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse");
        let minted = parsed.ops[0].added_node_id().expect("add node");
        assert_ne!(minted, placeholder, "the placeholder is never the identity");

        let task = parsed.ops[0].added_task().expect("task carried");
        assert_eq!(task.id, minted.to_string(), "task id IS the minted node id");
        assert_eq!(task.nix_expression_path, "/patch/enrich.nix");
        assert_eq!(task.routing_vars.len(), 1);

        match parsed.ops[1].op.as_ref() {
            Some(pp::addressed_patch_op::Op::AddEdge(e)) => {
                assert_eq!(
                    e.sources,
                    vec![existing.to_string()],
                    "existing ids pass through"
                );
                assert_eq!(
                    e.targets,
                    vec![minted.to_string()],
                    "placeholder refs are rewritten"
                );
            }
            other => panic!("expected AddEdge, got {other:?}"),
        }
        assert_eq!(parsed.new_tasks().len(), 1);
    }

    /// Dynamic fan-out: the SAME `mkTask` body written N times (N distinct
    /// placeholders) lowers to N distinct minted siblings.
    #[test]
    fn same_task_body_written_n_times_mints_n_distinct_siblings() {
        let p1 = Uuid::new_v4();
        let p2 = Uuid::new_v4();
        let p3 = Uuid::new_v4();
        let doc = serde_json::json!({ "ops": [
            { "AddNode": { "node_id": p1, "task": task_json("worker") } },
            { "AddNode": { "node_id": p2, "task": task_json("worker") } },
            { "AddNode": { "node_id": p3, "task": task_json("worker") } },
        ]});
        let parsed = parse_patch_document_json(&doc.to_string()).expect("parse");
        let ids: std::collections::HashSet<Uuid> = parsed
            .ops
            .iter()
            .filter_map(|op| op.added_node_id())
            .collect();
        assert_eq!(ids.len(), 3, "one fresh globally-unique id per AddNode");
    }

    /// Registration-only constructs are rejected loudly rather than silently
    /// dropped: `emits` (completion-synth wiring is a registration-time pass).
    #[test]
    fn patched_task_with_emits_is_rejected() {
        let mut task = task_json("emitter");
        task["emits"] = serde_json::json!([
            { "kind": "signal-emit", "signal": { "name": "s" }, "from_routing_var": "verdict" }
        ]);
        let doc = serde_json::json!({ "ops": [
            { "AddNode": { "node_id": Uuid::new_v4(), "task": task } },
        ]});
        assert!(matches!(
            parse_patch_document_json(&doc.to_string()),
            Err(PatchError::Parse(_))
        ));
    }

    /// A self-patch value that is already the evaluated JSON document parses
    /// without a Nickel round-trip.
    #[tokio::test]
    async fn self_patch_json_object_document_parses() {
        let node = Uuid::new_v4();
        let doc = serde_json::json!({
            "ops": [ { "AddNode": { "node_id": node, "task": task_json("fanout") } } ],
            "reason": "runtime fan-out"
        });
        let parsed = parse_self_patch_document(&doc).await.expect("parse");
        assert_eq!(parsed.ops.len(), 1);
        assert_eq!(parsed.reason.as_deref(), Some("runtime fan-out"));
        assert_eq!(parsed.new_tasks().len(), 1);
    }

    /// The evaluated JSON document is retained verbatim as the authored source,
    /// tagged `json` — the reading surface renders exactly what was submitted.
    #[test]
    fn json_document_retains_verbatim_source() {
        let node = Uuid::new_v4();
        let json = format!(r#"{{ "ops": [ {{ "AddNode": {{ "node_id": "{node}" }} }} ] }}"#);
        let parsed = parse_patch_document_json(&json).expect("parse");
        assert_eq!(parsed.source.format, PatchSourceFormat::Json);
        assert_eq!(parsed.source.text, json, "source is the verbatim document");
    }

    /// A self-patch emitted as an already-evaluated JSON object retains that
    /// object as its verbatim `json` source (no server-side re-encoding).
    #[tokio::test]
    async fn self_patch_json_object_retains_verbatim_source() {
        let node = Uuid::new_v4();
        let doc = serde_json::json!({
            "ops": [ { "AddNode": { "node_id": node } } ],
            "reason": "runtime fan-out"
        });
        let parsed = parse_self_patch_document(&doc).await.expect("parse");
        assert_eq!(parsed.source.format, PatchSourceFormat::Json);
        assert_eq!(
            parsed.source.text,
            doc.to_string(),
            "the emitted document is retained as submitted"
        );
    }

    #[test]
    fn source_format_discriminants_are_stable() {
        assert_eq!(PatchSourceFormat::Nickel.as_str(), "nickel");
        assert_eq!(PatchSourceFormat::Json.as_str(), "json");
    }
}
