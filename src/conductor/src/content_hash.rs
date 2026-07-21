//! Workflow content hashing — the basis for system-assigned versions.
//!
//! The version a workflow registers at is derived from a hash of its
//! *identity-affecting content*, so re-submitting byte-identical content is a
//! clean no-op and only substantive changes mint a new version. Two
//! requirements shape this module:
//!
//! 1. **Allowlist, fail-safe.** Only an explicit set of fields feeds the hash
//!    (task graph topology, per-task task_type/args/expression/inputs/outputs/
//!    secrets/retries/routing/emits/timeouts, the trigger, captures, and the
//!    workflow timeout). Cosmetic fields — display `name`, `tags`, and the
//!    identity/runtime columns — are excluded, so editing a label never bumps
//!    the version. A new field added to `Workflow`/`Task` is excluded from the
//!    hash until deliberately allowlisted; the [`drift_guard`] test fails until
//!    someone classifies it, so the default is "does not affect identity".
//!
//! 2. **Id-independent.** `Task` ids and edge/`gate_edge_id` values are random
//!    (`Uuid::new_v4()`) and differ on every parse of the same source. Hashing
//!    the raw serialized `Workflow` would therefore never match across
//!    re-submissions. So the projection is **name-based**: tasks are keyed and
//!    sorted by name, edges are projected to sorted source/target *name* sets,
//!    and the random ids never enter the hash.
//!
//! Serialization for the hash is canonical (sorted keys, stable encoding) via
//! [`crate::canonical_json`]; a storage JSON round-trip is never the hash input.

use serde_json::{json, Value};
use std::collections::HashMap;
use tickr_proto::workflow as wf;

/// Allowlisted, identity-affecting keys of the serialized `Task`. A change to
/// any of these bumps the workflow's version; anything not listed here (or in
/// [`TASK_COSMETIC_KEYS`]) trips the drift-guard test.
pub const TASK_HASHED_KEYS: &[&str] = &[
    "name",
    "task_type",
    "nix_expression_path",
    "nix_args",
    "inputs",
    "input_sources",
    "outputs",
    "secrets",
    "max_attempts",
    "routing_vars",
    "emits",
    "timeout_secs",
    // Loop participation changes runtime behaviour (park vs complete), so it is
    // identity-affecting. Derived at parse time from the `kind = loop` edges,
    // which also feed the hash via the edge projection.
    "loop_participant",
];

/// Serialized `Task` keys that do NOT affect identity: the random/ derived ids.
pub const TASK_COSMETIC_KEYS: &[&str] = &["id", "workflow_id"];

/// Allowlisted, identity-affecting top-level keys of the serialized `Workflow`
/// (`WorkflowRepr`). `tasks` and `task_graph` are identity-affecting too but are
/// hashed through the name-based projection rather than copied raw, so they are
/// listed separately in [`WORKFLOW_STRUCTURAL_KEYS`].
pub const WORKFLOW_HASHED_KEYS: &[&str] = &["trigger", "captures", "timeout_secs"];

/// Structural keys hashed via the id-independent projection (not copied raw).
pub const WORKFLOW_STRUCTURAL_KEYS: &[&str] = &["tasks", "task_graph"];

/// Serialized `Workflow` keys that do NOT affect identity: identity segments
/// (which determine `workflow_id`, not the version), cosmetic labels, the
/// system-assigned version itself, and runtime/legacy columns.
pub const WORKFLOW_COSMETIC_KEYS: &[&str] = &[
    "id",
    // Tenant folds into `workflow_id` via the identity seed, so it selects the
    // workflow, not the version — hashing it here would double-count identity.
    "tenant_id",
    "namespace",
    "slug",
    "name",
    "version",
    "tags",
    "status",
];

/// Hex SHA-256 of the workflow's identity-affecting content. Stable across
/// re-parses of identical source (id-independent) and across cosmetic-only
/// edits (allowlist), so equal hashes mean "the same workflow content".
pub fn content_hash(def: &wf::WorkflowDefinition) -> String {
    let projection = project(def);
    let digest = crate::canonical_json::hash(Some(&projection));
    hex(&digest)
}

/// Hex SHA-256 of the workflow's *cosmetic* fields — the ones excluded from the
/// content hash (display `name` and `tags`). Paired with [`content_hash`], it
/// lets the register resolver tell a byte-identical re-submit (both hashes
/// match → NoOp) from a cosmetic-only edit on otherwise-identical content (only
/// the content hash matches → Refreshed).
pub fn cosmetic_hash(def: &wf::WorkflowDefinition) -> String {
    let projection = json!({
        "name": def.name,
        "tags": serde_json::to_value(&def.tags).unwrap_or(Value::Null),
    });
    hex(&crate::canonical_json::hash(Some(&projection)))
}

/// The canonical, id-independent projection that feeds the hash. Built directly
/// from the proto contract the parser emits; the random task/edge ids and the
/// per-slot `gate_edge_id` never enter the hash (labels are name-based), so a
/// re-parse of identical source hashes identically. Exposed for tests that want
/// to assert on the projected shape.
pub fn project(def: &wf::WorkflowDefinition) -> Value {
    // node id -> stable label (task name, or a sentinel for start/end).
    let mut labels: HashMap<String, String> = HashMap::new();
    for t in &def.tasks {
        labels.insert(t.id.clone(), t.name.clone());
    }
    let graph = def.task_graph.clone().unwrap_or_default();
    labels.insert(graph.start.clone(), "\u{0}start".to_string());
    labels.insert(graph.end.clone(), "\u{0}end".to_string());
    let label = |id: &str| -> String {
        labels
            .get(id)
            // An id with no label should not occur; fall back to a fixed marker
            // (never the raw uuid, which would reintroduce instability).
            .cloned()
            .unwrap_or_else(|| "\u{0}unknown".to_string())
    };

    // Tasks: project each onto its allowlisted keys, sorted by name.
    let mut tasks: Vec<Value> = def.tasks.iter().map(project_task).collect();
    tasks.sort_by(|a, b| task_name(a).cmp(task_name(b)));

    // Edges: source/target *name* sets + edge kind + gate declarations, in
    // canonical order. `kind` (control / data / loop) is identity-affecting — a
    // `loop` back-edge is a different workflow from a `data` forward edge.
    let mut edges: Vec<Value> = graph
        .edges
        .iter()
        .map(|edge| {
            let mut sources: Vec<String> = edge.sources.iter().map(|s| label(s)).collect();
            sources.sort();
            let mut targets: Vec<String> = edge.targets.iter().map(|t| label(t)).collect();
            targets.sort();
            let gates: Vec<Value> = edge
                .gates
                .iter()
                .map(|g| serde_json::to_value(g).unwrap_or(Value::Null))
                .collect();
            json!({ "sources": sources, "targets": targets, "kind": edge.kind, "gates": gates })
        })
        .collect();
    edges.sort_by_key(|a| a.to_string());

    json!({
        "tasks": tasks,
        "edges": edges,
        "trigger": serde_json::to_value(&def.trigger).unwrap_or(Value::Null),
        "captures": serde_json::to_value(&def.captures).unwrap_or(Value::Null),
        "timeout_secs": def.timeout_secs,
    })
}

/// Project one proto task onto its allowlisted, identity-affecting fields.
/// The random ids are dropped and `input_sources` is normalized so the per-slot
/// `gate_edge_id` (resolved per parse) never enters the hash — the signal name
/// carries the meaning.
fn project_task(task: &wf::TaskDefinition) -> Value {
    json!({
        "name": task.name,
        "task_type": task.task_type,
        "nix_expression_path": task.nix_expression_path,
        "nix_args": task.nix_args,
        "inputs": task.inputs,
        "input_sources": normalize_input_sources(task.input_sources.as_ref()),
        "outputs": task.outputs,
        "secrets": task.secrets,
        "max_attempts": task.max_attempts,
        "routing_vars": serde_json::to_value(&task.routing_vars).unwrap_or(Value::Null),
        "emits": serde_json::to_value(&task.emits).unwrap_or(Value::Null),
        "timeout_secs": task.timeout_secs,
        "loop_participant": task.loop_participant,
    })
}

/// Project the structured input-source vector, keeping the per-slot shape and
/// (for a `Signal` slot) the signal name — but never the random `gate_edge_id`,
/// which differs on every parse of the same source.
fn normalize_input_sources(list: Option<&wf::InputSourceList>) -> Value {
    match list {
        None => Value::Null,
        Some(list) => Value::Array(
            list.sources
                .iter()
                .map(
                    |slot| match slot.source.as_ref().and_then(|s| s.source.as_ref()) {
                        None => Value::Null,
                        Some(wf::input_source::Source::Task(t)) => json!({ "task": t.name }),
                        Some(wf::input_source::Source::Trigger(_)) => json!({ "trigger": {} }),
                        Some(wf::input_source::Source::Signal(s)) => {
                            json!({ "signal_name": s.signal_name })
                        }
                    },
                )
                .collect(),
        ),
    }
}

fn task_name(task_value: &Value) -> &str {
    task_value
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::builder::parse_workflow_from_json;
    use std::collections::HashSet;

    /// A two-task workflow JSON with an explicit edge, a gate, and assorted
    /// allowlisted task fields. `slug`/`name`/`tags` are parameterised so tests
    /// can vary cosmetic fields; `first_task_expr` sets task `a`'s
    /// `nix_expression_path` so tests can vary a genuinely identity-affecting
    /// (hashed) field.
    fn wf_json(slug: &str, name: &str, tags: &str, first_task_expr: &str) -> String {
        format!(
            r#"{{
                "slug": "{slug}",
                "name": "{name}",
                "args": [],
                "outputs": [],
                "tags": {tags},
                "tasks": [
                    {{
                        "name": "g",
                        "args": [],
                        "outputs": [],
                        "tasks": [
                            {{ "name": "a", "args": ["x"], "outputs": [], "nix_expression_path": "{first_task_expr}" }},
                            {{ "name": "b", "args": [], "outputs": [], "nix_expression_path": "q" }}
                        ],
                        "edges": [ {{ "sources": ["a"], "targets": ["b"] }} ]
                    }}
                ]
            }}"#
        )
    }

    async fn hash_of(json: &str) -> String {
        let wf = parse_workflow_from_json(json, "default").await.unwrap();
        content_hash(&wf)
    }

    #[tokio::test]
    async fn identical_content_hashes_equal_despite_random_ids() {
        // Two independent parses of identical source assign different random
        // task/edge UUIDs, yet the name-based projection must hash identically —
        // this is what makes a re-submit a NoOp.
        let a = hash_of(&wf_json("s", "Display", "{}", "p")).await;
        let b = hash_of(&wf_json("s", "Display", "{}", "p")).await;
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn cosmetic_only_change_does_not_change_the_hash() {
        // Different display name, slug, and tags — none are identity-affecting,
        // so the content hash is unchanged.
        let a = hash_of(&wf_json("slug-a", "Name A", "{}", "p")).await;
        let b = hash_of(&wf_json("slug-b", "Name B", r#"{"env":"prod"}"#, "p")).await;
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn structural_change_changes_the_hash() {
        // A different per-task nix_expression_path is identity-affecting.
        let a = hash_of(&wf_json("s", "n", "{}", "p")).await;
        let b = hash_of(&wf_json("s", "n", "{}", "p2")).await;
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn adding_a_task_changes_the_hash() {
        let two = hash_of(&wf_json("s", "n", "{}", "p")).await;
        let three = r#"{
            "command": "AddWorkflow", "slug": "s", "name": "n", "args": [], "outputs": [],
            "tasks": [ { "command": "AddTaskGroup", "name": "g", "args": [], "outputs": [], "tasks": [
                { "command": "AddTask", "name": "a", "args": ["x"], "outputs": [], "nix_expression_path": "p" },
                { "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "q" },
                { "command": "AddTask", "name": "c", "args": [], "outputs": [], "nix_expression_path": "r" }
            ], "edges": [ { "sources": ["a"], "targets": ["b"] } ] } ]
        }"#;
        assert_ne!(two, hash_of(three).await);
    }

    /// Drift-guard: every serialized `Workflow`/`Task` key must be consciously
    /// classified as identity-affecting (hashed / structural) or cosmetic. A
    /// new field added to either struct fails this test until someone decides
    /// which bucket it belongs in — so the default is "excluded from identity".
    #[tokio::test]
    async fn drift_guard_every_serialized_key_is_classified() {
        let wf = parse_workflow_from_json(&wf_json("s", "n", "{}", "p"), "default")
            .await
            .unwrap();

        let wf_value = serde_json::to_value(&wf).unwrap();
        let wf_allowed: HashSet<&str> = WORKFLOW_HASHED_KEYS
            .iter()
            .chain(WORKFLOW_STRUCTURAL_KEYS)
            .chain(WORKFLOW_COSMETIC_KEYS)
            .copied()
            .collect();
        for key in wf_value.as_object().unwrap().keys() {
            assert!(
                wf_allowed.contains(key.as_str()),
                "unclassified WorkflowDefinition field `{key}`: add it to content_hash's \
                 WORKFLOW_HASHED_KEYS / WORKFLOW_STRUCTURAL_KEYS / WORKFLOW_COSMETIC_KEYS"
            );
        }

        let task = wf.tasks.first().unwrap();
        let task_value = serde_json::to_value(task).unwrap();
        let task_allowed: HashSet<&str> = TASK_HASHED_KEYS
            .iter()
            .chain(TASK_COSMETIC_KEYS)
            .copied()
            .collect();
        for key in task_value.as_object().unwrap().keys() {
            assert!(
                task_allowed.contains(key.as_str()),
                "unclassified Task field `{key}`: add it to content_hash's \
                 TASK_HASHED_KEYS or TASK_COSMETIC_KEYS"
            );
        }
    }
}
