use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct ParsedTask {
    pub name: String,
    pub args: Vec<String>,
    pub nix_expression_path: Option<String>,
    pub outputs: Vec<String>,
    /// Mixed-shape input list: bare strings declare input names; structured
    /// records also carry trigger, task, or signal source information.
    #[serde(default)]
    pub inputs: Vec<ParsedInputBinding>,
    #[serde(default)]
    pub secrets: Vec<String>,
    // Per-task retry budget. An omitted value defaults to three attempts.
    #[serde(default)]
    pub max_attempts: Option<u32>,
    /// Author-declared `mkRoutingVar` entries owned by this task. The
    /// single-producer validator rejects ownership of the same name by
    /// multiple tasks.
    #[serde(default)]
    pub routing_vars: Vec<ParsedRoutingVar>,
    /// Author-declared `mkSignalEmit` / `mkSignalEmitOnFailure`
    /// producer constructs. Each entry tells the conductor's
    /// task-completion path to synthesize a Wakeup on the named
    /// signal at the appropriate completion outcome.
    #[serde(default)]
    pub emits: Vec<ParsedSignalEmit>,
    /// Per-task timeout as a duration string (`30s`, `5m`, `1h`). The
    /// builder projects this onto `Task.timeout_secs` via the duration
    /// parser; malformed values fail registration. `None` (the field
    /// omitted) preserves today's behaviour for tasks that don't opt in.
    #[serde(default)]
    pub timeout: Option<String>,
}

/// JSON-shape mirror of `mkSignalEmit` and `mkSignalEmitOnFailure`.
/// Internally tagged on `kind`; new emit variants land here as new
/// tags without a wire break.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ParsedSignalEmit {
    /// At task-completion-grounded-Success: synthesize a Wakeup
    /// whose payload is the named routing variable's value. The
    /// routing variable must be declared on the SAME task (the
    /// parser's co-location validator enforces this).
    SignalEmit {
        signal: ParsedSignalRef,
        from_routing_var: String,
    },
    /// At task-completion-grounded-Failure: synthesize a Wakeup
    /// whose payload is the auto-populated `FailureContext`.
    /// Lands in slice 04 — accepted here so the parser surface is
    /// uniform; the runtime hook is a follow-up.
    SignalEmitOnFailure { signal: ParsedSignalRef },
}

/// JSON-shape mirror of `mkRoutingVar`. `type` is an optional hint
/// for parser-side type checking against `mkPredicateGate.value` —
/// not enforced at runtime.
#[derive(Deserialize, Debug, Clone)]
pub struct ParsedRoutingVar {
    pub name: String,
    #[serde(default)]
    pub kind: Option<String>,
    /// Optional declared type: `"string" | "int" | "bool" | "bytes"`.
    /// When set, the parser type-checks `mkPredicateGate.value`
    /// against this declaration at registration.
    #[serde(default, rename = "type")]
    pub var_type: Option<String>,
}

/// One entry in `ParsedTask.inputs`. The untagged enum disambiguates
/// by JSON shape: a string deserializes as `Bare`; an object with
/// `name` and `from` keys deserializes as `Structured`. Mixed lists
/// are supported position-by-position so authors can opt into the
/// strict declaration only for inputs that need it.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ParsedInputBinding {
    Bare(String),
    Structured {
        name: String,
        from: ParsedInputSource,
    },
}

impl ParsedInputBinding {
    /// The name of the input, regardless of which shape it was
    /// authored in. The builder uses this for the existing
    /// `Task.inputs` list (the name-only list that `tickr-ctx
    /// capture`'s strict-output check and the executor's
    /// `TICKR_INPUTS` env var both consume).
    pub fn name(&self) -> &str {
        match self {
            ParsedInputBinding::Bare(s) => s.as_str(),
            ParsedInputBinding::Structured { name, .. } => name.as_str(),
        }
    }
}

/// Source declaration for a structured input. Tagged externally
/// (`{ "task": { "name": "..." } }`, `{ "trigger": true }`,
/// `{ "signal": {...} }`) so each variant picks itself from the
/// JSON shape.
///
/// `Signal` carries the raw gate attrset emitted by `mkSignalGate`;
/// the parser reads only the inner `signal.name` and lets the rest
/// ride along as JSON noise. The dominator check at registration
/// resolves the gate's edge_id from `(signal_name, declaring task)`.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ParsedInputSource {
    Task { name: String },
    Trigger(serde_json::Value),
    Signal(serde_json::Value),
}

#[derive(Deserialize, Debug)]
pub struct ParsedWorkflow {
    /// Author-written lowercase-kebab handle, the immutable half of the
    /// workflow's `namespace.slug` identity. Required — an absent slug is a
    /// parse-class error (no serde default), so identity is never derived
    /// from a missing input. The namespace, by contrast, is supplied at
    /// registration (not in the source), so it does not appear here.
    pub slug: String,
    pub name: String,
    pub args: Vec<String>,
    pub outputs: Vec<String>,
    pub tasks: Vec<ParsedTaskGroup>,
    /// Capture declarations carried alongside the workflow definition. Lowered
    /// to the published capture contract by the builder before registration.
    #[serde(default)]
    pub captures: Vec<ParsedCaptureDeclaration>,
    /// Operator-meaningful labels from `mkWorkflow.tags`. The conductor
    /// parser is the authoritative registration ingress, so the tag
    /// validator runs here before the workflow definition reaches the
    /// in-memory `Workflow`. Empty when the field is omitted.
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Structured trigger configuration from `triggerOn = { kind = ...; }`.
    /// The conductor parser projects this onto `Workflow.trigger`; absent it,
    /// the workflow fires only on explicit invocation (`FireNow`).
    #[serde(default, rename = "triggerOn")]
    pub trigger_on: Option<ParsedTriggerConfig>,
    /// Per-workflow timeout as a duration string. Projected onto
    /// `Workflow.timeout_secs` via the duration parser; malformed values
    /// fail registration. `None` means no workflow-level timeout.
    #[serde(default)]
    pub timeout: Option<String>,
}

/// Structured `triggerOn` shape mirroring the proto `TriggerConfig`
/// oneof. The `kind` field discriminates the variants on the wire; the
/// parser lowers each onto the in-memory `Workflow`.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ParsedTriggerConfig {
    Cron {
        expr: String,
    },
    FireNow,
    WaitsOnSignal {
        #[serde(default)]
        signal: ParsedSignalRef,
        #[serde(default)]
        predicate: Option<String>,
        #[serde(default)]
        captures: Vec<ParsedCaptureDeclaration>,
    },
}

/// JSON-shape of an `mkSignal { name = ...; }` reference. The DSL emits
/// `{ name = "..."; kind = "signal"; }`; the parser only reads `name`.
#[derive(Deserialize, Debug, Clone, Default)]
pub struct ParsedSignalRef {
    #[serde(default)]
    pub name: String,
}

/// JSON-shape mirror of the proto `CaptureDeclaration`. Stays parser-local
/// so the JSON layer can evolve without re-shaping the runtime type that
/// lives on `Workflow`.
#[derive(Deserialize, Debug, Clone)]
pub struct ParsedCaptureDeclaration {
    pub name: String,
    pub from: ParsedCaptureSource,
}

/// JSON-shape mirror of the proto `CaptureSource` oneof. Tagged externally
/// (`{"trigger": {"jsonpath": "..."}}`) so new variants land as new map
/// keys without a wire break.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ParsedCaptureSource {
    Trigger { jsonpath: String },
}

#[derive(Deserialize, Debug)]
pub struct ParsedTaskGroup {
    pub name: String,
    pub tasks: Vec<ParsedTask>,
    pub args: Vec<String>,
    pub outputs: Vec<String>,
    #[serde(default)]
    pub edges: Vec<ParsedEdge>,
    /// `mkLoop`'s loop-iteration cap. Parsed and carried but **not enforced**
    /// in this slice — the deterministic tracer self-terminates, so there is
    /// no runaway to bound yet. Present so the surface round-trips; no read
    /// path consumes it.
    #[serde(default)]
    pub max_iterations: Option<u64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ParsedEdge {
    pub sources: Vec<String>,
    pub targets: Vec<String>,
    #[serde(default)]
    pub kind: Option<String>,
    /// Optional author-declared gate. Internally-tagged on `kind`:
    /// `signal-gate` for `mkSignalGate` (external-signal-driven) or
    /// `predicate-gate` for `mkPredicateGate` (routing-variable-
    /// driven). Absent for non-gated edges.
    #[serde(default)]
    pub gate: Option<ParsedGate>,
}

/// Internally-tagged enum over the gate variants the DSL accepts.
/// New variants land here as new `kind` tags without a wire break.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ParsedGate {
    SignalGate(ParsedSignalGate),
    PredicateGate(ParsedPredicateGate),
    TimerGate(ParsedTimerGate),
}

/// JSON-shape mirror of `mkTimerGate`. Just the wall-clock
/// duration; no signal, no predicate, no routing variable.
#[derive(Deserialize, Debug, Clone)]
pub struct ParsedTimerGate {
    pub duration: String,
}

/// JSON-shape mirror of `mkSignalGate` from the DSL. `signal` carries
/// the typed reference the conductor parser resolves at registration;
/// `predicate`, `captures`, and `timeout` are author-declared
/// configuration projected onto `Gate::SignalReceived` on the runtime
/// `Workflow`.
#[derive(Deserialize, Debug, Clone)]
pub struct ParsedSignalGate {
    pub signal: ParsedSignalRef,
    #[serde(default)]
    pub predicate: Option<String>,
    #[serde(default)]
    pub captures: Vec<ParsedCaptureDeclaration>,
    /// Duration string (`30s`, `5m`, `1h`). The parser projects this
    /// onto a `Duration` value on the runtime gate; activation by the
    /// `GateExpiry` timer lives in the follow-up gate-ergonomics work.
    #[serde(default)]
    pub timeout: Option<String>,
}

/// JSON-shape mirror of `mkPredicateGate` from the DSL. Carries the
/// routing variable name, comparison operator, and expected
/// `RoutingValue` (as a generic JSON value the parser type-checks
/// against the producer's declared `mkRoutingVar.type` and
/// projects onto the runtime `Gate::PredicateHolds`).
#[derive(Deserialize, Debug, Clone)]
pub struct ParsedPredicateGate {
    pub routing_var: String,
    /// Comparison operator: `"Eq" | "NotEq" | "Lt" | "Le" | "Gt" |
    /// "Ge"`. Mirrors the `ComparisonOp` enum on the server side.
    pub op: String,
    pub value: serde_json::Value,
    /// Duration string for the optional `GateExpiry` timer that
    /// transitions the gate `Rejected` if the routing variable
    /// never lands (or lands with a non-matching value).
    #[serde(default)]
    pub timeout: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsed_task_group_defaults_edges_to_empty_when_absent() {
        let json = r#"{
            "command": "AddTaskGroup",
            "name": "g",
            "args": [],
            "outputs": [],
            "tasks": []
        }"#;
        let group: ParsedTaskGroup = serde_json::from_str(json).unwrap();
        assert!(group.edges.is_empty());
    }

    #[test]
    fn parsed_task_group_deserializes_edges_when_present() {
        let json = r#"{
            "command": "AddTaskGroup",
            "name": "g",
            "args": [],
            "outputs": [],
            "tasks": [],
            "edges": [
                { "sources": ["a", "b"], "targets": ["c"] },
                { "sources": ["c"], "targets": ["d"], "kind": "data" }
            ]
        }"#;
        let group: ParsedTaskGroup = serde_json::from_str(json).unwrap();
        assert_eq!(group.edges.len(), 2);
        assert_eq!(group.edges[0].sources, vec!["a", "b"]);
        assert_eq!(group.edges[0].targets, vec!["c"]);
        assert!(group.edges[0].kind.is_none());
        assert_eq!(group.edges[1].kind.as_deref(), Some("data"));
    }

    #[test]
    fn workflow_json_without_command_field_deserializes() {
        // The vestigial `command` field was dropped from the DSL and the parse
        // structs in lockstep; a document authored with no `command` on the
        // workflow, task-group, or task must still deserialise at the boundary.
        let json = r#"{
            "slug": "no-command",
            "name": "no_command",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "name": "a",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "p"
                        }
                    ]
                }
            ]
        }"#;
        let workflow: ParsedWorkflow = serde_json::from_str(json).unwrap();
        assert_eq!(workflow.tasks.len(), 1);
        assert_eq!(workflow.tasks[0].tasks.len(), 1);
        assert_eq!(workflow.tasks[0].tasks[0].name, "a");
    }

    #[test]
    fn legacy_workflow_json_still_deserializes() {
        // Mirror of polyglot-workflow.json shape (no `edges` field anywhere).
        let json = r#"{
            "command": "AddWorkflow",
            "slug": "polyglot-workflow",
            "name": "polyglot_workflow",
            "args": [],
            "outputs": [],
            "schedule": "* * * * *",
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "polyglot-workflow-tg",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask",
                            "name": "hello_python",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "github:tickr-io/polyglot-workflow#helloPython"
                        }
                    ]
                }
            ]
        }"#;
        let workflow: ParsedWorkflow = serde_json::from_str(json).unwrap();
        assert_eq!(workflow.tasks.len(), 1);
        assert!(workflow.tasks[0].edges.is_empty());
    }
}
