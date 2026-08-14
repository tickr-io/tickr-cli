use crate::definition_graph::{self, ProtoDefinitionGraph, SignalResolutionError};
use crate::parser::types::{
    ParsedCaptureDeclaration, ParsedCaptureSource, ParsedGate, ParsedInputBinding,
    ParsedInputSource, ParsedPredicateGate, ParsedSignalEmit, ParsedSignalGate, ParsedTaskGroup,
    ParsedTimerGate, ParsedTriggerConfig, ParsedWorkflow,
};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tickr_proto::workflow as wf;
use tickr_proto::{derive_workflow_id, TenantId};
use uuid::Uuid;

impl ParsedCaptureDeclaration {
    /// Project a parsed capture declaration onto the published protobuf shape.
    fn to_proto_capture(&self) -> wf::CaptureDeclaration {
        let source = match &self.from {
            ParsedCaptureSource::Trigger { jsonpath } => {
                wf::capture_source::Source::Trigger(wf::capture_source::Trigger {
                    jsonpath: jsonpath.clone(),
                })
            }
        };
        wf::CaptureDeclaration {
            name: self.name.clone(),
            from: Some(wf::CaptureSource {
                source: Some(source),
            }),
        }
    }
}

/// Reject user tags in the reserved system namespace.
fn validate_definition_tags(tags: &HashMap<String, String>) -> Result<()> {
    let mut reserved: Vec<_> = tags
        .keys()
        .filter(|key| key.starts_with("tickr/"))
        .cloned()
        .collect();
    reserved.sort();
    if reserved.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "workflow tags use reserved tickr/ keys: {}",
            reserved.join(", ")
        ))
    }
}

/// Validate capture declarations against the published definition model. A
/// capture may only read the trigger payload and cannot reuse a task output.
fn validate_captures_with_outputs(
    captures: &[wf::CaptureDeclaration],
    tasks: &[wf::TaskDefinition],
) -> Result<()> {
    let task_outputs: std::collections::HashSet<_> = tasks
        .iter()
        .flat_map(|task| task.outputs.iter())
        .map(String::as_str)
        .collect();
    let mut names = std::collections::HashSet::new();
    for capture in captures {
        if !names.insert(capture.name.as_str()) {
            return Err(anyhow!("duplicate capture declaration `{}`", capture.name));
        }
        if task_outputs.contains(capture.name.as_str()) {
            return Err(anyhow!(
                "capture declaration `{}` conflicts with a task output",
                capture.name
            ));
        }
        let Some(wf::capture_source::Source::Trigger(trigger)) = capture
            .from
            .as_ref()
            .and_then(|source| source.source.as_ref())
        else {
            return Err(anyhow!(
                "capture declaration `{}` has no trigger source",
                capture.name
            ));
        };
        trigger
            .jsonpath
            .parse::<serde_json_path::JsonPath>()
            .map_err(|error| anyhow!("capture `{}` has invalid JSONPath: {error}", capture.name))?;
    }
    Ok(())
}

/// Default `max_attempts` when a task omits it, matching the DSL contract.
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Build the protobuf `TaskDefinition` list for one parsed task group.
/// Per-input structured `from = { ... }` bindings use a parallel `input_sources`
/// slot; a `Signal` slot is stamped with the nil `gate_edge_id`
/// sentinel here and resolved in the dominator pass after all edges exist.
/// `timeout_secs` and `loop_participant` are stamped by the caller.
fn group_to_proto_tasks(
    group: &ParsedTaskGroup,
    workflow_id: Uuid,
) -> Result<Vec<wf::TaskDefinition>> {
    group
        .tasks
        .iter()
        .map(|task| {
            let nix_expression_path = task
                .nix_expression_path
                .as_deref()
                .filter(|path| !path.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "task `{}` must declare a non-empty `nix_expression_path`; no default is available",
                        task.name
                    )
                })?;
            let inputs: Vec<String> = task.inputs.iter().map(|i| i.name().to_string()).collect();
            let input_sources = if task
                .inputs
                .iter()
                .any(|i| matches!(i, ParsedInputBinding::Structured { .. }))
            {
                let sources = task
                    .inputs
                    .iter()
                    .map(|i| {
                        let source = match i {
                            ParsedInputBinding::Bare(_) => None,
                            ParsedInputBinding::Structured { from, .. } => match from {
                                ParsedInputSource::Task { name } => {
                                    Some(wf::input_source::Source::Task(wf::input_source::Task {
                                        name: name.clone(),
                                    }))
                                }
                                ParsedInputSource::Trigger(_) => Some(
                                    wf::input_source::Source::Trigger(wf::input_source::Trigger {}),
                                ),
                                ParsedInputSource::Signal(value) => {
                                    // The DSL emits the full gate attrset under
                                    // `signal`; only the inner `signal.name` is
                                    // read. `gate_edge_id` resolves in the
                                    // dominator pass after all edges are added.
                                    let signal_name = value
                                        .get("signal")
                                        .and_then(|s| s.get("name"))
                                        .and_then(|n| n.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    Some(wf::input_source::Source::Signal(
                                        wf::input_source::Signal {
                                            signal_name,
                                            gate_edge_id: Uuid::nil().to_string(),
                                        },
                                    ))
                                }
                            },
                        };
                        wf::OptionalInputSource {
                            source: source.map(|s| wf::InputSource { source: Some(s) }),
                        }
                    })
                    .collect();
                Some(wf::InputSourceList { sources })
            } else {
                None
            };

            let emits = task
                .emits
                .iter()
                .map(|e| {
                    let emit = match e {
                        ParsedSignalEmit::SignalEmit {
                            signal,
                            from_routing_var,
                        } => {
                            wf::task_signal_emit::Emit::OnSuccess(wf::task_signal_emit::OnSuccess {
                                signal_name: signal.name.clone(),
                                from_routing_var: from_routing_var.clone(),
                            })
                        }
                        ParsedSignalEmit::SignalEmitOnFailure { signal } => {
                            wf::task_signal_emit::Emit::OnFailure(wf::task_signal_emit::OnFailure {
                                signal_name: signal.name.clone(),
                            })
                        }
                    };
                    wf::TaskSignalEmit { emit: Some(emit) }
                })
                .collect();

            let routing_vars = task
                .routing_vars
                .iter()
                .map(|rv| wf::RoutingVarDecl {
                    name: rv.name.clone(),
                    var_type: rv.var_type.clone(),
                })
                .collect();

            Ok(wf::TaskDefinition {
                id: Uuid::new_v4().to_string(),
                workflow_id: workflow_id.to_string(),
                name: task.name.clone(),
                // A DSL-authored task is always a RegularTask.
                task_type: wf::TaskType::Regular as i32,
                nix_expression_path: nix_expression_path.to_string(),
                nix_args: task.args.clone(),
                outputs: task.outputs.clone(),
                inputs,
                secrets: task.secrets.clone(),
                max_attempts: task.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS),
                input_sources,
                timeout_secs: None,
                emits,
                routing_vars,
                loop_participant: false,
            })
        })
        .collect()
}

/// Parse workflow JSON into the canonical protobuf `WorkflowDefinition`.
///
/// For each task group:
/// 1. Build every task as a proto `TaskDefinition` and mint one graph node each.
/// 2. Populate the edge set:
///    - If the group declared explicit `mkEdge`s, resolve names → ids and insert
///      each proto edge.
///    - Otherwise, synthesize chain edges from task-list order
///      (`task[i] → task[i+1]`) for concise serial groups.
/// 3. Seal over the proto model (`ProtoDefinitionGraph::seal`): link
///    orphan-source tasks to `start`, orphan-sink tasks to `end`.
///
/// Validation: edges referencing unknown task names are rejected.
///
/// **No cycle check.** Tickr's task graph is a hypergraph with the grounding
/// model — cycles produce runtime deadlock (every edge has a Pending source,
/// nothing fires) rather than incorrect results. Load-time cycle rejection
/// is a DAG concept that doesn't apply here; future iterative/feedback
/// patterns may even rely on cycles.
///
/// `tenant` is the data plane's [`TenantId`]; it becomes the leading identity
/// segment so two tenants registering the same `namespace.slug` derive distinct
/// ids. Callers that carry the conductor's ambient tenant use the
/// [`parse_workflow_from_json`] wrapper, which reads it from the environment.
pub async fn parse_workflow_from_json_for_tenant(
    json_str: &str,
    tenant: TenantId,
    namespace: &str,
) -> Result<wf::WorkflowDefinition> {
    let parsed_workflow: ParsedWorkflow = serde_json::from_str(json_str)?;
    // Reject `tickr/`-namespaced user tags at the registration ingress before
    // they can shadow the system tags materialized on every instance.
    validate_definition_tags(&parsed_workflow.tags)?;
    // Routing variables must be single-producer; the validator
    // walks every task's `mkRoutingVar` declarations and rejects
    // multi-task ownership before any edge / gate processing.
    validate_single_producer(&parsed_workflow)?;
    // Producer-side `mkSignalEmit` declarations must reference a
    // routing variable owned by the same task. Cross-task references
    // surface as a registration error.
    validate_emit_co_location(&parsed_workflow)?;
    // Identity derives from the composite `tenant.namespace.slug` (UUID v5).
    // The tenant is the conductor's own id (renders as a hyphenated UUID —
    // dot-free by grammar); the namespace is supplied at registration (absent
    // normalises to `default`); the slug is author-written. All three segments
    // forbid `.` (the tenant by UUID rendering, namespace/slug by the kebab
    // grammar), so the dot separators stay injective and two tenants registering
    // the same `namespace.slug` derive distinct ids. Renaming the display `name`
    // no longer reforges the id — only the tenant, namespace, or slug is an
    // identity boundary.
    let namespace = if namespace.trim().is_empty() {
        "default"
    } else {
        namespace
    };
    validate_identity_segment(namespace, "namespace")?;
    validate_identity_segment(&parsed_workflow.slug, "slug")?;
    // One derivation site, shared with the server's SUBMIT_WORKFLOW recompute-
    // check (`derive_workflow_id`), so the stamp and the admission recompute can
    // never drift apart.
    let workflow_uuid_v5 = derive_workflow_id(tenant, namespace, &parsed_workflow.slug);

    // Project the workflow-level `timeout` string onto `timeout_secs`. Malformed
    // durations fail registration before the deadline reaches the scheduler.
    let timeout_secs = match parsed_workflow.timeout.as_ref() {
        Some(raw) => {
            let dur = crate::parser::duration::parse_duration(raw).map_err(|e| {
                anyhow!(
                    "workflow `{}` has invalid `timeout` value `{}`: {}",
                    parsed_workflow.name,
                    raw,
                    e
                )
            })?;
            Some(dur.as_secs())
        }
        None => None,
    };

    // Two-phase ingestion so an edge in one group can reference a task in
    // another (e.g. a `mkLoop` body's whole-body exit fan-in into a downstream
    // `judge` group): (1) build every group's tasks, then (2) resolve every
    // group's edges against the now-complete task set, then (3) seal **once**.
    //
    // Phase 1: build all proto tasks across all groups. `name_to_id` maps a task
    // name to its freshly-minted id for edge resolution; `group_task_ids` keeps
    // per-group order for implicit-chain synthesis.
    let mut proto_tasks: Vec<wf::TaskDefinition> = Vec::new();
    let mut name_to_id: HashMap<String, Uuid> = HashMap::new();
    let mut group_task_ids: Vec<Vec<Uuid>> = Vec::with_capacity(parsed_workflow.tasks.len());
    for parsed_task_group in &parsed_workflow.tasks {
        let mut tasks = group_to_proto_tasks(parsed_task_group, workflow_uuid_v5)?;
        // Per-task `timeout` strings (1:1 with the parsed task list by index).
        for (i, parsed_task) in parsed_task_group.tasks.iter().enumerate() {
            if let Some(raw) = parsed_task.timeout.as_ref() {
                let dur = crate::parser::duration::parse_duration(raw).map_err(|e| {
                    anyhow!(
                        "task `{}` has invalid `timeout` value `{}`: {}",
                        parsed_task.name,
                        raw,
                        e
                    )
                })?;
                tasks[i].timeout_secs = Some(dur.as_secs());
            }
        }

        // Stamp `loop_participant` on every task that is a source of a
        // `kind = loop` edge — the graph-less task-manager reads loop-ness
        // task-locally on a park turn.
        let loop_source_names: std::collections::HashSet<&str> = parsed_task_group
            .edges
            .iter()
            .filter(|e| e.kind.as_deref() == Some("loop"))
            .flat_map(|e| e.sources.iter().map(|s| s.as_str()))
            .collect();
        if !loop_source_names.is_empty() {
            for t in tasks.iter_mut() {
                if loop_source_names.contains(t.name.as_str()) {
                    t.loop_participant = true;
                }
            }
        }

        let mut task_ids = Vec::with_capacity(tasks.len());
        for t in &tasks {
            let id = Uuid::parse_str(&t.id).expect("freshly minted task id parses");
            name_to_id.insert(t.name.clone(), id);
            task_ids.push(id);
        }
        group_task_ids.push(task_ids);
        proto_tasks.extend(tasks);
    }

    // Sentinel start/end node ids and the graph node set.
    let start_id = Uuid::new_v4();
    let end_id = Uuid::new_v4();
    let mut nodes: Vec<wf::GraphNode> = Vec::with_capacity(proto_tasks.len() + 2);
    nodes.push(wf::GraphNode {
        id: start_id.to_string(),
        node_type: wf::NodeType::Start as i32,
    });
    nodes.push(wf::GraphNode {
        id: end_id.to_string(),
        node_type: wf::NodeType::End as i32,
    });
    for t in &proto_tasks {
        nodes.push(wf::GraphNode {
            id: t.id.clone(),
            node_type: wf::NodeType::Task as i32,
        });
    }

    // Resolve an edge endpoint name to a graph-slot id. A declared task wins;
    // the reserved `End` / `Start` sentinels resolve to the terminal nodes only
    // when no task claims the name.
    let resolve = |names: &[String]| -> Result<Vec<String>> {
        names
            .iter()
            .map(|name| {
                if let Some(id) = name_to_id.get(name) {
                    return Ok(id.to_string());
                }
                match name.as_str() {
                    "End" => Ok(end_id.to_string()),
                    "Start" => Ok(start_id.to_string()),
                    _ => Err(anyhow!("edge references unknown task: {}", name)),
                }
            })
            .collect()
    };

    // Phase 2: populate edges, resolving names against the complete task set.
    let mut edges: Vec<wf::Edge> = Vec::new();
    for (parsed_task_group, task_ids) in parsed_workflow.tasks.iter().zip(&group_task_ids) {
        if parsed_task_group.edges.is_empty() {
            // Synthesize implicit chain from list order: t[0] → t[1] → ... → t[n-1].
            for pair in task_ids.windows(2) {
                edges.push(wf::Edge {
                    id: Uuid::new_v4().to_string(),
                    sources: vec![pair[0].to_string()],
                    targets: vec![pair[1].to_string()],
                    kind: wf::EdgeKind::Control as i32,
                    gates: Vec::new(),
                });
            }
        } else {
            for edge in &parsed_task_group.edges {
                let sources = resolve(&edge.sources)?;
                let targets = resolve(&edge.targets)?;
                // Carry the author-declared edge `kind` onto the proto edge.
                let kind = edge_kind_to_proto(edge.kind.as_deref());
                let gates = match &edge.gate {
                    Some(ParsedGate::SignalGate(g)) => {
                        vec![project_signal_gate_proto(g, &parsed_workflow.name)?]
                    }
                    Some(ParsedGate::PredicateGate(g)) => {
                        vec![project_predicate_gate_proto(g, &parsed_workflow.name)?]
                    }
                    Some(ParsedGate::TimerGate(g)) => {
                        vec![project_timer_gate_proto(g, &parsed_workflow.name)?]
                    }
                    None => Vec::new(),
                };
                edges.push(wf::Edge {
                    id: Uuid::new_v4().to_string(),
                    sources,
                    targets,
                    kind: kind as i32,
                    gates,
                });
            }
        }
    }

    // Assemble the unsealed definition. Version is system-assigned by the
    // register pipeline from the content hash; it is left at the unassigned `0`
    // sentinel here.
    let mut def = wf::WorkflowDefinition {
        id: workflow_uuid_v5.to_string(),
        tenant_id: tenant.to_string(),
        namespace: namespace.to_string(),
        slug: parsed_workflow.slug.clone(),
        name: parsed_workflow.name.clone(),
        version: 0,
        tasks: proto_tasks,
        task_graph: Some(wf::TaskGraph {
            nodes,
            edges,
            start: start_id.to_string(),
            end: end_id.to_string(),
        }),
        trigger: None,
        status: wf::WorkflowStatus::Inactive as i32,
        captures: Vec::new(),
        timeout_secs,
        tags: parsed_workflow.tags.clone(),
    };

    // Phase 3: seal once over the proto model — close orphans against start/end.
    let mut model = ProtoDefinitionGraph::from_definition(&def);
    model.seal();
    def.task_graph = Some(model.into_graph());

    // (4) Resolve `InputSource::Signal { gate_edge_id: nil }` slots: find the
    //     unique gate-bearing edge whose `signal_name` matches AND which
    //     dominates the declaring task. The dominator check is the runtime
    //     invariant that makes the enqueue-time stamping total.
    apply_signal_resolution(&mut def)?;

    // (5) Predicate-gate dominator check: every `PredicateHolds` gate references
    //     a routing variable; ordinarily the producing task must dominate every
    //     source of the consuming edge. The reserved loop-control path instead
    //     relies on park-fire and SCC teardown for sources in the producer's
    //     loop body.
    validate_predicate_gate_dominators_proto(&parsed_workflow, &def, &name_to_id)?;

    // (6) Loop-terminability check: every loop body (the `kind = loop` SCC) must
    //     be exitable via a `loop_control`-gated edge. This asserts a loop is
    //     *terminable*; it does not reject cycles (the deliberate
    //     no-load-time-cycle-rejection stance is preserved).
    if let Err(v) =
        definition_graph::validate_loop_terminability(def.task_graph.as_ref().expect("graph"))
    {
        return Err(anyhow!(
            "workflow `{}` declares a non-terminable loop: {:?}",
            parsed_workflow.name,
            v
        ));
    }

    // Workflow-level captures + the firing trigger.
    def.captures = parsed_workflow
        .captures
        .iter()
        .map(|c| c.to_proto_capture())
        .collect();
    def.trigger = Some(build_trigger(&parsed_workflow, &def)?);

    Ok(def)
}

/// Lower the resolved DSL trigger onto the proto `Trigger`. Absent a `triggerOn`,
/// the workflow fires only on explicit invocation (`FireNow`). A `waits-on-signal`
/// trigger's captures are validated against the same JSONPath and collision
/// rules as `mkWorkflow.captures`.
fn build_trigger(parsed: &ParsedWorkflow, def: &wf::WorkflowDefinition) -> Result<wf::Trigger> {
    let kind = match parsed.trigger_on.clone() {
        Some(ParsedTriggerConfig::Cron { expr }) => wf::trigger::Kind::Cron(expr),
        Some(ParsedTriggerConfig::FireNow) | None => {
            wf::trigger::Kind::FireNow(wf::trigger::FireNow {})
        }
        Some(ParsedTriggerConfig::WaitsOnSignal {
            signal,
            predicate,
            captures: trigger_captures,
        }) => {
            if signal.name.trim().is_empty() {
                return Err(anyhow!(
                    "`triggerOn = {{ kind = \"waits-on-signal\" }}` requires a `signal` \
                     reference: `signal = mkSignal {{ name = ... }}` did not resolve"
                ));
            }
            if let Some(raw) = predicate.as_deref() {
                if let Err(e) = raw.parse::<serde_json_path::JsonPath>() {
                    return Err(anyhow!(
                        "`triggerOn.predicate` for workflow `{}` is not a valid JSONPath filter: {}",
                        parsed.name,
                        e
                    ));
                }
            }
            let captures: Vec<wf::CaptureDeclaration> = trigger_captures
                .iter()
                .map(|c| c.to_proto_capture())
                .collect();
            validate_captures_with_outputs(&captures, &def.tasks)?;
            wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
                signal_name: signal.name,
                predicate,
                captures: trigger_captures
                    .iter()
                    .map(|c| c.to_proto_capture())
                    .collect(),
            })
        }
    };
    Ok(wf::Trigger { kind: Some(kind) })
}

/// Parse using the conductor's ambient tenant, sourced from the environment
/// (`TICKR_TENANT_SLUG`, falling back to the default slug). A conductor process
/// is pinned to one tenant, so this is the entry point its registration
/// pipeline uses — the ambient tenant becomes the leading identity segment with
/// no hard-coded value. Callers needing an explicit tenant (e.g. tests
/// exercising cross-tenant isolation) use [`parse_workflow_from_json_for_tenant`].
pub async fn parse_workflow_from_json(
    json_str: &str,
    namespace: &str,
) -> Result<wf::WorkflowDefinition> {
    parse_workflow_from_json_for_tenant(json_str, TenantId::from_env(), namespace).await
}

/// Walk every task's `mkSignalEmit` declarations and enforce
/// co-location: `from_routing_var` must reference a `mkRoutingVar`
/// declared on the SAME task. Cross-task `from_routing_var`
/// references are rejected at registration with the colliding
/// task and variable named. `mkSignalEmitOnFailure` is exempt —
/// it has no routing-variable reference.
pub(crate) fn validate_emit_co_location(parsed: &ParsedWorkflow) -> Result<()> {
    for tg in &parsed.tasks {
        for task in &tg.tasks {
            let declared: std::collections::HashSet<&str> = task
                .routing_vars
                .iter()
                .map(|rv| rv.name.as_str())
                .collect();
            for emit in &task.emits {
                if let ParsedSignalEmit::SignalEmit {
                    from_routing_var,
                    signal,
                } = emit
                {
                    if !declared.contains(from_routing_var.as_str()) {
                        return Err(anyhow!(
                            "`mkSignalEmit {{ signal = mkSignal {{ name = \"{}\"; }}; \
                             from_routing_var = \"{}\"; }}` on task `{}` requires \
                             `mkRoutingVar {{ name = \"{}\"; }}` to be declared on the \
                             SAME task — cross-task `from_routing_var` references are \
                             rejected.",
                            signal.name,
                            from_routing_var,
                            task.name,
                            from_routing_var,
                        ));
                    }
                    if signal.name.trim().is_empty() {
                        return Err(anyhow!(
                            "`mkSignalEmit.signal` on task `{}` must resolve to a declared \
                             `mkSignal {{ name = ... }}`",
                            task.name,
                        ));
                    }
                }
                if let ParsedSignalEmit::SignalEmitOnFailure { signal } = emit {
                    if signal.name.trim().is_empty() {
                        return Err(anyhow!(
                            "`mkSignalEmitOnFailure.signal` on task `{}` must resolve to a \
                             declared `mkSignal {{ name = ... }}`",
                            task.name,
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Cross-reference every task's `mkRoutingVar` declarations and
/// reject multi-task ownership of any routing-variable name. The
/// single-producer / completion-only rule is what keeps
/// `PredicateHolds` evaluation deterministic; the validator is a
/// pure function over the parsed workflow shape so it can be unit-
/// tested without runtime infrastructure.
pub(crate) fn validate_single_producer(parsed: &ParsedWorkflow) -> Result<()> {
    use std::collections::HashMap;
    let mut owners: HashMap<String, Vec<String>> = HashMap::new();
    for tg in &parsed.tasks {
        for task in &tg.tasks {
            for rv in &task.routing_vars {
                owners
                    .entry(rv.name.clone())
                    .or_default()
                    .push(task.name.clone());
            }
        }
    }
    let mut duplicates: Vec<(String, Vec<String>)> =
        owners.into_iter().filter(|(_, ts)| ts.len() > 1).collect();
    if duplicates.is_empty() {
        return Ok(());
    }
    duplicates.sort_by(|a, b| a.0.cmp(&b.0));
    let parts: Vec<String> = duplicates
        .iter()
        .map(|(name, tasks)| format!("`{}` declared by tasks {:?}", name, tasks))
        .collect();
    Err(anyhow!(
        "routing variables must be single-producer; the workflow declared duplicate ownership: {}",
        parts.join("; ")
    ))
}

/// For every `Gate::PredicateHolds` on the in-memory workflow, resolve the
/// routing variable to its producing task and ordinarily require the producer
/// to dominate every source of the gate's edge. That guarantee puts the value
/// in `instance.routing_variables` before an ordinary grounded source can make
/// the gate evaluable.
///
/// Sources in the same loop SCC as the reserved `loop_control` producer are the
/// narrow exception. A continuing participant stays ungrounded and advances its
/// `kind = loop` edge through park-fire, which decodes the completing turn's
/// control directly. On `done` / `fail`, SCC teardown grounds the producer's
/// siblings only after that producer's terminal update arrives. Both paths make
/// the control available without graph dominance, including when the sole
/// producer is not the loop head. Other routing variables, and sources outside
/// that producer's loop body, retain the ordinary dominance requirement.
fn validate_predicate_gate_dominators_proto(
    parsed: &ParsedWorkflow,
    def: &wf::WorkflowDefinition,
    name_to_id: &HashMap<String, Uuid>,
) -> Result<()> {
    // Build a routing_var name → producer task name map from the parsed
    // workflow. The single-producer validator already rejected duplicates, so
    // each name maps to at most one task.
    let mut producers: HashMap<String, String> = HashMap::new();
    for tg in &parsed.tasks {
        for task in &tg.tasks {
            for rv in &task.routing_vars {
                producers.insert(rv.name.clone(), task.name.clone());
            }
        }
    }

    let graph = def.task_graph.as_ref().expect("graph present");
    let loop_bodies = definition_graph::loop_sccs(graph);
    for edge in &graph.edges {
        for gate in &edge.gates {
            let Some(wf::gate::Kind::PredicateHolds(ph)) = &gate.kind else {
                continue;
            };
            let routing_var = &ph.routing_var;
            let Some(producer_name) = producers.get(routing_var) else {
                return Err(anyhow!(
                    "`mkPredicateGate` references routing variable `{}` which no task declares; \
                     add `mkRoutingVar {{ name = \"{}\" }}` to the producing task's `routing_vars`",
                    routing_var,
                    routing_var,
                ));
            };
            let producer_id = name_to_id
                .get(producer_name)
                .ok_or_else(|| {
                    anyhow!(
                        "internal error: producer task `{}` not in workflow",
                        producer_name
                    )
                })?
                .to_string();

            let producer_loop_body = (routing_var == "loop_control")
                .then(|| loop_bodies.iter().find(|body| body.contains(&producer_id)))
                .flatten();

            for src in &edge.sources {
                // Park-fire advances continuing ring members without grounding;
                // terminal SCC teardown grounds siblings only after this
                // producer's done/fail update. The reserved control is therefore
                // available for every source in the producer's own loop body
                // even when ordinary graph dominance does not hold.
                if producer_loop_body.is_some_and(|body| body.contains(src)) {
                    continue;
                }
                if let Err(v) = definition_graph::validate_task_dominates(graph, &producer_id, src)
                {
                    return Err(anyhow!(
                        "`mkPredicateGate {{ routing_var = \"{}\" }}` requires producer task `{}` \
                         to dominate every source of the gate's edge; bypass-path: {:?}",
                        routing_var,
                        producer_name,
                        v.bypass_path,
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Resolve every unresolved `InputSource::Signal { gate_edge_id: nil }` slot over
/// the sealed proto graph and patch the resolved `gate_edge_id` back onto the
/// definition. Delegates the dominator resolution to the conductor-local
/// definition-graph engine; a zero/ambiguous match maps back to the same
/// task-named registration errors as before.
fn apply_signal_resolution(def: &mut wf::WorkflowDefinition) -> Result<()> {
    let graph = def.task_graph.as_ref().expect("graph present");
    let resolved = definition_graph::resolve_signal_input_sources(&def.tasks, graph)
        .map_err(|e| signal_resolution_error(&def.tasks, e))?;
    for r in resolved {
        if let Some(task) = def.tasks.iter_mut().find(|t| t.id == r.task_id) {
            if let Some(list) = task.input_sources.as_mut() {
                if let Some(wf::OptionalInputSource {
                    source:
                        Some(wf::InputSource {
                            source: Some(wf::input_source::Source::Signal(sig)),
                        }),
                }) = list.sources.get_mut(r.slot)
                {
                    sig.gate_edge_id = r.gate_edge_id;
                }
            }
        }
    }
    Ok(())
}

/// Map a conductor-local signal-resolution failure back to the task-named
/// registration error the DSL author sees.
fn signal_resolution_error(
    tasks: &[wf::TaskDefinition],
    err: SignalResolutionError,
) -> anyhow::Error {
    let name_of = |id: &str| {
        tasks
            .iter()
            .find(|t| t.id == id)
            .map(|t| t.name.clone())
            .unwrap_or_else(|| id.to_string())
    };
    match err {
        SignalResolutionError::Unresolved {
            task_id,
            signal_name,
        } => anyhow!(
            "task `{}` declares `from.signal = mkSignalGate {{ signal.name = \"{}\" }}` \
             but no gate carrying that signal dominates the task; \
             every path from a graph source to the task must traverse the gate's edge",
            name_of(&task_id),
            signal_name,
        ),
        SignalResolutionError::Ambiguous {
            task_id,
            signal_name,
            count,
        } => anyhow!(
            "task `{}` declares `from.signal = mkSignalGate {{ signal.name = \"{}\" }}` \
             but {} gates with this signal name dominate the task — \
             the reference is ambiguous; rename one or pick a structurally-distinct \
             signal name",
            name_of(&task_id),
            signal_name,
            count,
        ),
    }
}

/// Map the DSL edge `kind` string onto the proto `EdgeKind`; unrecognised /
/// absent values fall back to `Control` (the author-side Nickel contract is the
/// validating gate).
fn edge_kind_to_proto(s: Option<&str>) -> wf::EdgeKind {
    match s {
        Some("data") => wf::EdgeKind::Data,
        Some("loop") => wf::EdgeKind::Loop,
        _ => wf::EdgeKind::Control,
    }
}

fn duration_secs_to_proto(d: std::time::Duration) -> wf::Duration {
    wf::Duration {
        secs: d.as_secs(),
        nanos: d.subsec_nanos(),
    }
}

/// Validate the parsed gate at registration and project onto a proto
/// `Gate::SignalReceived`. Catches the four registration-time errors
/// synchronously: empty signal name, malformed predicate JSONPath, malformed
/// timeout duration, and (deferred to the server) non-singular capture queries.
fn project_signal_gate_proto(parsed: &ParsedSignalGate, workflow_name: &str) -> Result<wf::Gate> {
    if parsed.signal.name.trim().is_empty() {
        return Err(anyhow!(
            "`mkSignalGate.signal` ref must resolve to a declared `mkSignal {{ name = ... }}`"
        ));
    }
    if let Some(raw) = parsed.predicate.as_deref() {
        if let Err(e) = raw.parse::<serde_json_path::JsonPath>() {
            return Err(anyhow!(
                "`mkSignalGate.predicate` on workflow `{}` is not a valid JSONPath filter: {}",
                workflow_name,
                e
            ));
        }
    }
    let captures_spec = parsed
        .captures
        .iter()
        .map(|c| c.to_proto_capture())
        .collect();
    let timeout = match parsed.timeout.as_deref() {
        None => None,
        Some(raw) => Some(duration_secs_to_proto(
            crate::parser::duration::parse_duration(raw).map_err(|e| {
                anyhow!(
                    "`mkSignalGate.timeout` on workflow `{}` value `{}` failed to parse: {}",
                    workflow_name,
                    raw,
                    e
                )
            })?,
        )),
    };
    Ok(wf::Gate {
        kind: Some(wf::gate::Kind::SignalReceived(wf::gate::SignalReceived {
            signal_name: parsed.signal.name.clone(),
            predicate: parsed.predicate.clone(),
            captures_spec,
            timeout,
        })),
    })
}

/// Validate the parsed predicate-gate at registration and project onto a proto
/// `Gate::PredicateHolds`. Catches malformed `op` strings, malformed `value`
/// JSON shape, and malformed `timeout` durations synchronously.
fn project_predicate_gate_proto(
    parsed: &ParsedPredicateGate,
    workflow_name: &str,
) -> Result<wf::Gate> {
    if parsed.routing_var.trim().is_empty() {
        return Err(anyhow!(
            "`mkPredicateGate.routing_var` on workflow `{}` must name a declared `mkRoutingVar`",
            workflow_name,
        ));
    }
    let op = match parsed.op.as_str() {
        "Eq" => wf::ComparisonOp::Eq,
        "NotEq" => wf::ComparisonOp::NotEq,
        "Lt" => wf::ComparisonOp::Lt,
        "Le" => wf::ComparisonOp::Le,
        "Gt" => wf::ComparisonOp::Gt,
        "Ge" => wf::ComparisonOp::Ge,
        other => {
            return Err(anyhow!(
                "`mkPredicateGate.op` on workflow `{}` must be one of \
                 \"Eq\" / \"NotEq\" / \"Lt\" / \"Le\" / \"Gt\" / \"Ge\"; got `{}`",
                workflow_name,
                other,
            ));
        }
    };
    let value = json_to_routing_value_proto(&parsed.value).map_err(|e| {
        anyhow!(
            "`mkPredicateGate.value` on workflow `{}` is not a supported scalar: {}",
            workflow_name,
            e
        )
    })?;
    let timeout = match parsed.timeout.as_deref() {
        None => None,
        Some(raw) => Some(duration_secs_to_proto(
            crate::parser::duration::parse_duration(raw).map_err(|e| {
                anyhow!(
                    "`mkPredicateGate.timeout` on workflow `{}` value `{}` failed to parse: {}",
                    workflow_name,
                    raw,
                    e
                )
            })?,
        )),
    };
    Ok(wf::Gate {
        kind: Some(wf::gate::Kind::PredicateHolds(wf::gate::PredicateHolds {
            routing_var: parsed.routing_var.clone(),
            op: op as i32,
            value: Some(value),
            timeout,
        })),
    })
}

/// Validate the parsed timer-gate at registration and project onto a proto
/// `Gate::TimerElapsed`. The only failure path is a malformed duration string.
fn project_timer_gate_proto(parsed: &ParsedTimerGate, workflow_name: &str) -> Result<wf::Gate> {
    let duration = crate::parser::duration::parse_duration(&parsed.duration).map_err(|e| {
        anyhow!(
            "`mkTimerGate.duration` on workflow `{}` value `{}` failed to parse: {}",
            workflow_name,
            parsed.duration,
            e
        )
    })?;
    Ok(wf::Gate {
        kind: Some(wf::gate::Kind::TimerElapsed(wf::gate::TimerElapsed {
            duration: Some(duration_secs_to_proto(duration)),
        })),
    })
}

/// Project a generic JSON `Value` onto a proto `RoutingValue`. Strings,
/// integers, and booleans only — floats and nested structures are rejected
/// (`RoutingValue` is a closed scalar shape; NaN breaks equality/hashing).
fn json_to_routing_value_proto(v: &serde_json::Value) -> Result<wf::RoutingValue, &'static str> {
    let value = match v {
        serde_json::Value::String(s) => wf::routing_value::Value::StringValue(s.clone()),
        serde_json::Value::Bool(b) => wf::routing_value::Value::BoolValue(*b),
        serde_json::Value::Number(n) => wf::routing_value::Value::IntValue(
            n.as_i64().ok_or("only integer numbers are supported")?,
        ),
        _ => return Err("supported values are strings, integers, or booleans"),
    };
    Ok(wf::RoutingValue { value: Some(value) })
}

/// Validate a parsed gate and project it onto the published proto [`wf::Gate`],
/// dispatching on the `kind` tag. The proto twin of [`project_gate`]: shared by
/// registration (edges on a workflow definition) and the patch pipeline (a
/// primitive `AddEdge`'s gates and a `branch` arm's selecting gate), so gates
/// authored on every surface are validated and projected identically. The
/// projected gate carries the authored declaration.
pub(crate) fn project_gate_proto(parsed: &ParsedGate, label: &str) -> Result<wf::Gate> {
    match parsed {
        ParsedGate::SignalGate(g) => project_signal_gate_proto(g, label),
        ParsedGate::PredicateGate(g) => project_predicate_gate_proto(g, label),
        ParsedGate::TimerGate(g) => project_timer_gate_proto(g, label),
    }
}

/// Validate an identity segment (`namespace` or `slug`) against the shared
/// kebab grammar `[a-z0-9-]{1,64}`. The DSL's `Slug` contract is the primary
/// author-time surface; this is the conductor-side defence-in-depth that also
/// guards the registration-supplied `namespace`, which never passes through the
/// DSL. A violation is a parse-class error naming the rule.
fn validate_identity_segment(value: &str, what: &str) -> Result<()> {
    let len = value.chars().count();
    let well_formed = (1..=64).contains(&len)
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if well_formed {
        Ok(())
    } else {
        Err(anyhow!(
            "invalid {what} `{value}`: must match [a-z0-9-]{{1,64}} \
             (lowercase letters, digits, `-`; 1–64 chars; no `.`)"
        ))
    }
}

#[cfg(test)]
mod tests {
    // The parser now emits the published proto `wf::WorkflowDefinition`
    // directly, so the assertions below read proto fields off that contract via
    // the small accessor helpers here.
    // `parse_workflow_from_json` / `..._for_tenant` resolve to the module's real
    // proto-returning entry points through the `use super::*` glob.
    use super::*;

    /// Find a task by name and parse its graph-slot id.
    fn task_id_by_name(def: &wf::WorkflowDefinition, name: &str) -> Uuid {
        let t = def
            .tasks
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("task `{name}` not present"));
        Uuid::parse_str(&t.id).expect("task id parses")
    }

    /// Fetch a task definition by its id.
    fn get_task(def: &wf::WorkflowDefinition, id: Uuid) -> &wf::TaskDefinition {
        def.tasks
            .iter()
            .find(|t| t.id == id.to_string())
            .expect("task present")
    }

    /// The (sealed) proto task graph.
    fn graph_of(def: &wf::WorkflowDefinition) -> &wf::TaskGraph {
        def.task_graph.as_ref().expect("task graph present")
    }

    /// The node ids reached by edges whose sources contain `node_id`.
    fn outgoing_targets(g: &wf::TaskGraph, node_id: &str) -> Vec<String> {
        g.edges
            .iter()
            .filter(|e| e.sources.iter().any(|s| s == node_id))
            .flat_map(|e| e.targets.iter().cloned())
            .collect()
    }

    /// The unique edge whose sources contain `from` and targets contain `to`.
    fn edge_between(g: &wf::TaskGraph, from: Uuid, to: Uuid) -> &wf::Edge {
        g.edges
            .iter()
            .find(|e| {
                e.sources.iter().any(|s| *s == from.to_string())
                    && e.targets.iter().any(|t| *t == to.to_string())
            })
            .expect("edge present")
    }

    /// The workflow's waits-on-signal trigger config, when that is its trigger.
    fn waits_on_signal(def: &wf::WorkflowDefinition) -> Option<&wf::WaitsOnSignalConfig> {
        match def.trigger.as_ref()?.kind.as_ref()? {
            wf::trigger::Kind::WaitsOnSignal(cfg) => Some(cfg),
            _ => None,
        }
    }

    /// Builds a minimal one-task workflow JSON carrying an explicit `slug`.
    /// Identity derives from `namespace.slug`, so the tests below vary slug,
    /// name, and namespace independently to pin down the identity boundary.
    fn wf_json(slug: &str, name: &str) -> String {
        format!(
            r#"{{
                "command": "AddWorkflow",
                "slug": "{slug}",
                "name": "{name}",
                "args": [],
                "outputs": [],
                "tasks": [
                    {{
                        "command": "AddTaskGroup",
                        "name": "tg",
                        "args": [],
                        "outputs": [],
                        "tasks": [
                            {{ "command": "AddTask", "name": "t", "args": [], "outputs": [], "nix_expression_path": "x" }}
                        ]
                    }}
                ]
            }}"#
        )
    }

    #[tokio::test]
    async fn task_expression_path_is_required_without_a_machine_specific_default() {
        for value in ["null", "\"\"", "\"   \""] {
            let json = format!(
                r#"{{
                    "slug": "s", "name": "n", "args": [], "outputs": [],
                    "tasks": [{{ "name": "g", "args": [], "outputs": [], "tasks": [
                        {{ "name": "a", "args": [], "outputs": [], "nix_expression_path": {value} }}
                    ] }}]
                }}"#
            );
            let error = parse_workflow_from_json_for_tenant(
                &json,
                TenantId::from_slug("path-validation-test"),
                "default",
            )
            .await
            .expect_err("missing or blank path must be rejected");
            assert!(
                error.to_string().contains(
                    "task `a` must declare a non-empty `nix_expression_path`; no default is available"
                ),
                "unexpected error: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn task_expression_path_preserves_portable_authored_references() {
        let json = r#"{
            "slug": "s", "name": "n", "args": [], "outputs": [],
            "tasks": [{ "name": "g", "args": [], "outputs": [], "tasks": [
                { "name": "a", "args": [], "outputs": [], "nix_expression_path": "github:tickr-io/example#task" }
            ] }]
        }"#;
        let definition = parse_workflow_from_json_for_tenant(
            json,
            TenantId::from_slug("portable-reference-test"),
            "default",
        )
        .await
        .unwrap();
        assert_eq!(
            definition.tasks[0].nix_expression_path,
            "github:tickr-io/example#task"
        );
    }

    #[tokio::test]
    async fn task_authored_without_command_parses_to_regular_task_not_addtask() {
        // Regression for the original defect: the vestigial `command = "AddTask"`
        // string was piped into the task_type slot, so every task's kind was the
        // literal "AddTask". A task authored with no `command` must now parse to
        // the true default kind, and the corrupted string can never appear.
        let json = r#"{
            "slug": "s", "name": "n", "args": [], "outputs": [],
            "tasks": [ { "name": "g", "args": [], "outputs": [], "tasks": [
                { "name": "a", "args": [], "outputs": [], "nix_expression_path": "x" }
            ] } ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let task = def.tasks.first().expect("one task");
        // A task authored with no `command` parses to the default Regular kind;
        // the old corrupted "AddTask" string has no representation in the closed
        // proto `TaskType` enum at all.
        assert_eq!(task.task_type, wf::TaskType::Regular as i32);
    }

    #[tokio::test]
    async fn renaming_the_display_name_preserves_identity() {
        // Renaming the display `name` (slug + namespace unchanged) must not
        // reforge the id — fixing a label never orphans a workflow's history.
        let a = parse_workflow_from_json(&wf_json("daily-sync", "Daily Sync"), "default")
            .await
            .unwrap();
        let b = parse_workflow_from_json(&wf_json("daily-sync", "Nightly Sync"), "default")
            .await
            .unwrap();
        assert_eq!(a.id, b.id);
        assert_eq!(a.slug, "daily-sync");
        assert_eq!(b.name, "Nightly Sync");
    }

    #[tokio::test]
    async fn changing_content_under_same_slug_preserves_identity() {
        // Two structurally different workflows under the same slug+namespace
        // resolve to the same id — content does not participate in identity.
        let one_task = parse_workflow_from_json(&wf_json("same-slug", "n"), "default")
            .await
            .unwrap();
        let two_task_json = r#"{
            "command": "AddWorkflow",
            "slug": "same-slug",
            "name": "n",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "tg",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ]
                }
            ]
        }"#;
        let two_task = parse_workflow_from_json(two_task_json, "default")
            .await
            .unwrap();
        assert_eq!(one_task.id, two_task.id);
    }

    #[tokio::test]
    async fn changing_the_slug_produces_a_distinct_id() {
        // The slug is the consciously-owned identity boundary: changing it
        // mints a new workflow by design.
        let a = parse_workflow_from_json(&wf_json("slug-a", "n"), "default")
            .await
            .unwrap();
        let b = parse_workflow_from_json(&wf_json("slug-b", "n"), "default")
            .await
            .unwrap();
        assert_ne!(a.id, b.id);
    }

    #[tokio::test]
    async fn omitted_namespace_equals_explicit_default() {
        // An absent namespace normalises to the literal `default` *before*
        // identity derivation, so there is no namespace-less identity shape.
        let omitted = parse_workflow_from_json(&wf_json("s", "n"), "")
            .await
            .unwrap();
        let explicit = parse_workflow_from_json(&wf_json("s", "n"), "default")
            .await
            .unwrap();
        assert_eq!(omitted.id, explicit.id);
        assert_eq!(omitted.namespace, "default");
    }

    #[tokio::test]
    async fn same_slug_in_different_namespaces_are_distinct() {
        // Two teams picking the same slug in separate namespaces are distinct
        // workflows by construction — the namespace qualifies the slug.
        let etl = parse_workflow_from_json(&wf_json("daily-sync", "n"), "etl")
            .await
            .unwrap();
        let reporting = parse_workflow_from_json(&wf_json("daily-sync", "n"), "reporting")
            .await
            .unwrap();
        assert_ne!(etl.id, reporting.id);
    }

    #[tokio::test]
    async fn same_namespace_slug_under_two_tenants_are_distinct() {
        // The tenant is the leading identity segment, so two tenants that
        // register the identical `namespace.slug` never collide on one
        // `workflow_id` — isolation holds by construction, not convention.
        let acme = parse_workflow_from_json_for_tenant(
            &wf_json("daily-sync", "n"),
            TenantId::from_slug("acme"),
            "reporting",
        )
        .await
        .unwrap();
        let globex = parse_workflow_from_json_for_tenant(
            &wf_json("daily-sync", "n"),
            TenantId::from_slug("globex"),
            "reporting",
        )
        .await
        .unwrap();
        assert_ne!(acme.id, globex.id);
    }

    #[tokio::test]
    async fn changing_only_the_tenant_produces_a_distinct_id() {
        // Holding namespace + slug fixed and varying only the tenant reforges
        // the id: the tenant is an identity boundary like the slug and
        // namespace.
        let tenant_a = TenantId::from_slug("acme");
        let tenant_b = TenantId::from_slug("globex");
        let a = parse_workflow_from_json_for_tenant(&wf_json("s", "n"), tenant_a, "default")
            .await
            .unwrap();
        let b = parse_workflow_from_json_for_tenant(&wf_json("s", "n"), tenant_b, "default")
            .await
            .unwrap();
        assert_ne!(a.id, b.id);
        // The tenant segment renders as a parseable, dot-free UUID — that is
        // what keeps the dot-separated `tenant.namespace.slug` seed injective
        // across the added segment.
        assert!(tenant_a.to_string().parse::<Uuid>().is_ok());
        assert!(!tenant_a.to_string().contains('.'));
    }

    #[tokio::test]
    async fn parser_persists_the_registration_tenant_on_the_definition() {
        // The tenant folds into `workflow_id` one-way, so the definition must
        // carry it explicitly for a downstream `workflow_id → tenant` lookup.
        // Registering under a tenant lands that exact tenant on the workflow.
        let tenant = TenantId::from_slug("acme");
        let workflow = parse_workflow_from_json_for_tenant(&wf_json("s", "n"), tenant, "default")
            .await
            .unwrap();
        assert_eq!(workflow.tenant_id, tenant.to_string());
        // Two tenants registering the same `namespace.slug` are separable by the
        // persisted tenant, not just by the opaque id.
        let other = parse_workflow_from_json_for_tenant(
            &wf_json("s", "n"),
            TenantId::from_slug("globex"),
            "default",
        )
        .await
        .unwrap();
        assert_ne!(workflow.tenant_id, other.tenant_id);
    }

    #[tokio::test]
    async fn empty_namespace_still_normalises_to_default_under_three_segment_seed() {
        // The empty-namespace → `default` normalisation survives the tenant
        // fold: under one tenant, an omitted namespace resolves the same id as
        // an explicit `default`.
        let tenant = TenantId::from_slug("acme");
        let omitted = parse_workflow_from_json_for_tenant(&wf_json("s", "n"), tenant, "")
            .await
            .unwrap();
        let explicit = parse_workflow_from_json_for_tenant(&wf_json("s", "n"), tenant, "default")
            .await
            .unwrap();
        assert_eq!(omitted.id, explicit.id);
        assert_eq!(omitted.namespace, "default");
    }

    #[tokio::test]
    async fn ambient_wrapper_threads_the_environment_tenant_into_the_seed() {
        // The conductor sources its tenant from the environment (no hard-coded
        // value); the ambient two-arg entry must derive the same id as an
        // explicit call carrying that same env-derived tenant. Restore the
        // process environment so ordinary `cargo test` remains order-independent.
        let previous = std::env::var_os(tickr_proto::tenant::TENANT_SLUG_ENV);
        std::env::set_var(tickr_proto::tenant::TENANT_SLUG_ENV, "acme");
        let ambient = parse_workflow_from_json(&wf_json("s", "n"), "default")
            .await
            .unwrap();
        let explicit = parse_workflow_from_json_for_tenant(
            &wf_json("s", "n"),
            TenantId::from_slug("acme"),
            "default",
        )
        .await
        .unwrap();
        match previous {
            Some(value) => std::env::set_var(tickr_proto::tenant::TENANT_SLUG_ENV, value),
            None => std::env::remove_var(tickr_proto::tenant::TENANT_SLUG_ENV),
        }
        assert_eq!(ambient.id, explicit.id);
    }

    #[tokio::test]
    async fn absent_slug_is_a_parse_error() {
        // No serde default for `slug`: an absent slug fails deserialization, so
        // identity is never derived from a missing input.
        let json = r#"{
            "command": "AddWorkflow",
            "name": "n",
            "args": [],
            "outputs": [],
            "tasks": [
                { "command": "AddTaskGroup", "name": "tg", "args": [], "outputs": [], "tasks": [] }
            ]
        }"#;
        assert!(parse_workflow_from_json(json, "default").await.is_err());
    }

    #[tokio::test]
    async fn malformed_slug_is_a_parse_error() {
        // Defence-in-depth over the DSL `Slug` contract: an uppercase slug
        // violates the kebab grammar and fails registration naming the rule.
        let err = parse_workflow_from_json(&wf_json("Bad_Slug", "n"), "default")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("slug"), "got: {err}");
    }

    #[tokio::test]
    async fn malformed_namespace_is_a_parse_error() {
        // The registration-supplied namespace is grammar-checked too, since it
        // never passes through the DSL's author-time contract.
        let err = parse_workflow_from_json(&wf_json("s", "n"), "Bad.NS")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("namespace"), "got: {err}");
    }

    #[tokio::test]
    async fn legacy_workflow_loads_with_chain_semantics() {
        // Mirror of polyglot-workflow.json (no `edges`).
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "polyglot_workflow",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "polyglot-workflow-tg",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask",
                            "name": "task_a",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "x"
                        },
                        {
                            "command": "AddTask",
                            "name": "task_b",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "x"
                        },
                        {
                            "command": "AddTask",
                            "name": "task_c",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "x"
                        }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let a = task_id_by_name(&def, "task_a");
        let b = task_id_by_name(&def, "task_b");
        let c = task_id_by_name(&def, "task_c");
        let graph = graph_of(&def);

        // Implicit chain: start → a → b → c → end
        assert!(outgoing_targets(graph, &graph.start).contains(&a.to_string()));
        assert!(outgoing_targets(graph, &a.to_string()).contains(&b.to_string()));
        assert!(outgoing_targets(graph, &b.to_string()).contains(&c.to_string()));
        assert!(outgoing_targets(graph, &c.to_string()).contains(&graph.end));
    }

    #[tokio::test]
    async fn explicit_edges_produce_fan_out_dag() {
        // 3 extractors → transform → load
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "fanout_workflow",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "fanout-tg",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "extract_a", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "extract_b", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "extract_c", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "transform", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "load",      "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        { "sources": ["extract_a", "extract_b", "extract_c"], "targets": ["transform"] },
                        { "sources": ["transform"], "targets": ["load"] }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let ea = task_id_by_name(&def, "extract_a");
        let eb = task_id_by_name(&def, "extract_b");
        let ec = task_id_by_name(&def, "extract_c");
        let tr = task_id_by_name(&def, "transform");
        let ld = task_id_by_name(&def, "load");
        let graph = graph_of(&def);

        // All three extractors are direct successors of start (no incoming edges otherwise).
        for &x in &[ea, eb, ec] {
            assert!(
                outgoing_targets(graph, &graph.start).contains(&x.to_string()),
                "{:?} should be linked from start",
                x
            );
        }
        // All three extractors point to transform.
        for &x in &[ea, eb, ec] {
            assert!(outgoing_targets(graph, &x.to_string()).contains(&tr.to_string()));
        }
        // transform → load → end
        assert!(outgoing_targets(graph, &tr.to_string()).contains(&ld.to_string()));
        assert!(outgoing_targets(graph, &ld.to_string()).contains(&graph.end));
    }

    /// Golden-output regression for an ETL workflow (`submit-job → monitor-job
    /// → clean-job`). This workflow's correctness materially depends on chain
    /// ordering, so the fixture is inlined here (self-contained, no external
    /// file) and the assertions pin the edge order.
    #[tokio::test]
    async fn golden_etl_chain_is_preserved() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "etl", "name": "etl",
            "args": [], "outputs": [], "schedule": "* * * * *",
            "tasks": [{
                "command": "AddTaskGroup", "name": "extract",
                "args": [], "outputs": [],
                "tasks": [
                    {"command": "AddTask", "name": "submit-job",
                     "args": ["job", "submit", "extract.yaml"],
                     "nix_expression_path": "tickr-k8s", "outputs": ["pod-name"]},
                    {"command": "AddTask", "name": "monitor-job",
                     "args": ["job", "monitor"],
                     "nix_expression_path": "tickr-k8s", "outputs": []},
                    {"command": "AddTask", "name": "clean-job",
                     "args": ["job", "clean"],
                     "nix_expression_path": "tickr-k8s", "outputs": []}
                ]
            }]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let graph = graph_of(&def);

        let submit = task_id_by_name(&def, "submit-job");
        let monitor = task_id_by_name(&def, "monitor-job");
        let clean = task_id_by_name(&def, "clean-job");

        assert!(outgoing_targets(graph, &graph.start).contains(&submit.to_string()));
        assert!(outgoing_targets(graph, &submit.to_string()).contains(&monitor.to_string()));
        assert!(outgoing_targets(graph, &monitor.to_string()).contains(&clean.to_string()));
        assert!(outgoing_targets(graph, &clean.to_string()).contains(&graph.end));
    }

    #[tokio::test]
    async fn workflow_definition_tags_round_trip_into_workflow() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "tagged_workflow",
            "args": [],
            "outputs": [],
            "tags": { "env": "prod", "team": "billing" },
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "t", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let tags = &def.tags;
        assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(tags.get("team").map(String::as_str), Some("billing"));
    }

    #[tokio::test]
    async fn workflow_with_no_tags_field_loads_with_empty_tag_map() {
        // The `tags` field stays optional — workflows that don't declare
        // it keep working unchanged.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "untagged_workflow",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "t", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        assert!(def.tags.is_empty());
    }

    #[tokio::test]
    async fn registration_rejects_definition_tags_with_system_namespace_prefix() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "reserved_clash",
            "args": [],
            "outputs": [],
            "tags": { "tickr/workflow_id": "x", "tickr/trigger_source": "y", "env": "prod" },
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": []
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("tickr/workflow_id") && msg.contains("tickr/trigger_source"),
            "error must surface both offending keys, got: {}",
            msg
        );
    }

    #[tokio::test]
    async fn registration_accepts_keys_that_contain_tickr_but_not_as_prefix() {
        // Only the prefix is reserved; substring matches must pass.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "substring_only",
            "args": [],
            "outputs": [],
            "tags": { "mytickr/x": "ok", "internal_tickr_owner": "team-a" },
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": []
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        assert_eq!(def.tags.get("mytickr/x").map(String::as_str), Some("ok"));
    }

    #[tokio::test]
    async fn waits_on_signal_trigger_on_projects_onto_workflow() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "wants-user-paid",
            "args": [],
            "outputs": [],
            "triggerOn": {
                "kind": "waits-on-signal",
                "signal": { "name": "user-paid" },
                "predicate": "$[?@.amount > 100]",
                "captures": [
                    { "name": "user_email", "from": { "trigger": { "jsonpath": "$.user.email" } } }
                ]
            },
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "t", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let cfg = waits_on_signal(&def).expect("waits-on-signal must be the projected trigger");
        assert_eq!(cfg.signal_name, "user-paid");
        assert_eq!(cfg.predicate.as_deref(), Some("$[?@.amount > 100]"));
        assert_eq!(cfg.captures.len(), 1);
        assert_eq!(cfg.captures[0].name, "user_email");
        // The trigger is waits-on-signal, not cron.
        assert!(matches!(
            def.trigger.as_ref().and_then(|t| t.kind.as_ref()),
            Some(wf::trigger::Kind::WaitsOnSignal(_))
        ));
    }

    #[tokio::test]
    async fn waits_on_signal_with_empty_signal_ref_is_rejected() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "missing-signal-ref",
            "args": [],
            "outputs": [],
            "triggerOn": { "kind": "waits-on-signal", "signal": {} },
            "tasks": [
                { "command": "AddTaskGroup", "name": "g", "args": [], "outputs": [], "tasks": [] }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        assert!(
            err.to_string().contains("signal"),
            "expected signal-ref error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn waits_on_signal_with_invalid_predicate_is_rejected() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "bad-predicate",
            "args": [],
            "outputs": [],
            "triggerOn": {
                "kind": "waits-on-signal",
                "signal": { "name": "x" },
                "predicate": "$[?(garbage]"
            },
            "tasks": [
                { "command": "AddTaskGroup", "name": "g", "args": [], "outputs": [], "tasks": [] }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        assert!(
            err.to_string().contains("JSONPath") || err.to_string().contains("predicate"),
            "expected predicate-validation error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn signal_emit_co_location_validator_rejects_cross_task_routing_var() {
        // task `a` declares `mkSignalEmit { from_routing_var =
        // "coverage" }` but `coverage` is declared by task `b`.
        // Cross-task `from_routing_var` references are rejected at
        // registration.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "cross_task_emit",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x",
                            "emits": [{
                                "kind": "signal-emit",
                                "signal": { "name": "coverage-emitted" },
                                "from_routing_var": "coverage"
                            }]
                        },
                        {
                            "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [{ "name": "coverage", "kind": "routing-var" }]
                        }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("coverage") && msg.contains("a") && msg.contains("SAME task"),
            "error must name routing var and offending task: {}",
            msg
        );
    }

    #[tokio::test]
    async fn signal_emit_on_failure_projects_onto_runtime_emits() {
        // `mkSignalEmitOnFailure` carries no `from_routing_var` —
        // the synthesizer auto-populates the payload with the
        // failing task's lineage. The parser projects it onto
        // `TaskSignalEmit::OnFailure`.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "failure_emit",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x",
                            "emits": [{
                                "kind": "signal-emit-on-failure",
                                "signal": { "name": "deployment-failed" }
                            }]
                        }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let a_id = task_id_by_name(&def, "a");
        let task = get_task(&def, a_id);
        let emits = &task.emits;
        assert_eq!(emits.len(), 1);
        match emits[0].emit.as_ref() {
            Some(wf::task_signal_emit::Emit::OnFailure(f)) => {
                assert_eq!(f.signal_name, "deployment-failed");
            }
            other => panic!("expected OnFailure, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn task_with_both_success_and_failure_emits_projects_both() {
        // Authors who want a "succeeded or failed" signal pair
        // declare both on the same task. The parser projects each
        // independently and the parser doesn't forbid the mix.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "both_emits",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [{ "name": "decision", "kind": "routing-var" }],
                            "emits": [
                                { "kind": "signal-emit", "signal": { "name": "ok" }, "from_routing_var": "decision" },
                                { "kind": "signal-emit-on-failure", "signal": { "name": "ng" } }
                            ]
                        }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let a_id = task_id_by_name(&def, "a");
        let task = get_task(&def, a_id);
        let emits = &task.emits;
        assert_eq!(emits.len(), 2);
        let mut found_success = false;
        let mut found_failure = false;
        for e in emits {
            match e.emit.as_ref() {
                Some(wf::task_signal_emit::Emit::OnSuccess(s)) => {
                    assert_eq!(s.signal_name, "ok");
                    assert_eq!(s.from_routing_var, "decision");
                    found_success = true;
                }
                Some(wf::task_signal_emit::Emit::OnFailure(f)) => {
                    assert_eq!(f.signal_name, "ng");
                    found_failure = true;
                }
                None => panic!("emit carries a kind"),
            }
        }
        assert!(found_success && found_failure);
    }

    #[tokio::test]
    async fn signal_emit_co_located_routing_var_projects_onto_runtime_emits() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "co_located",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [{ "name": "decision", "kind": "routing-var" }],
                            "emits": [{
                                "kind": "signal-emit",
                                "signal": { "name": "approval" },
                                "from_routing_var": "decision"
                            }]
                        }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let a_id = task_id_by_name(&def, "a");
        let task = get_task(&def, a_id);
        let emits = &task.emits;
        assert_eq!(emits.len(), 1);
        match emits[0].emit.as_ref() {
            Some(wf::task_signal_emit::Emit::OnSuccess(s)) => {
                assert_eq!(s.signal_name, "approval");
                assert_eq!(s.from_routing_var, "decision");
            }
            other => panic!("expected OnSuccess, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn predicate_gate_projects_onto_runtime_gate_with_resolved_op() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "predicate_gate",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask",
                            "name": "test",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "x",
                            "routing_vars": [
                                { "name": "coverage", "kind": "routing-var", "type": "int" }
                            ]
                        },
                        { "command": "AddTask", "name": "deploy", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        {
                            "sources": ["test"],
                            "targets": ["deploy"],
                            "gate": {
                                "kind": "predicate-gate",
                                "routing_var": "coverage",
                                "op": "Ge",
                                "value": 80
                            }
                        }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let test_id = task_id_by_name(&def, "test");
        let deploy_id = task_id_by_name(&def, "deploy");
        let graph = graph_of(&def);
        let edge = edge_between(graph, test_id, deploy_id);
        let Some(wf::gate::Kind::PredicateHolds(ph)) = edge.gates[0].kind.as_ref() else {
            panic!("expected PredicateHolds, got {:?}", edge.gates[0]);
        };
        assert_eq!(ph.routing_var, "coverage");
        assert_eq!(ph.op, wf::ComparisonOp::Ge as i32);
        assert_eq!(
            ph.value,
            Some(wf::RoutingValue {
                value: Some(wf::routing_value::Value::IntValue(80)),
            })
        );
        // Definition gates are declaration-only; per-instance runtime state
        // (`Idle`) is stamped server-side on apply and has no field on the
        // published definition shape.
    }

    #[tokio::test]
    async fn single_producer_validator_rejects_duplicate_routing_var_ownership() {
        // Two tasks declaring the same routing variable name: must
        // surface a registration error naming both tasks.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "dup_owners",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        {
                            "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [{ "name": "shared", "kind": "routing-var" }]
                        },
                        {
                            "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [{ "name": "shared", "kind": "routing-var" }]
                        }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("shared"),
            "error must name the duplicate routing var: {}",
            msg
        );
        assert!(
            msg.contains("\"a\"") && msg.contains("\"b\""),
            "error must list both owning tasks: {}",
            msg
        );
    }

    #[tokio::test]
    async fn predicate_gate_with_non_dominating_producer_is_rejected() {
        // Diamond: `start → a → {test, lint} → release`. `test`
        // produces `coverage`. A predicate gate on `lint → release`
        // references `coverage` — but the gate's source (`lint`)
        // is not dominated by the producer (`test`) since the
        // build → lint path bypasses test. Registration must fail.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "no_dominator",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "build", "args": [], "outputs": [], "nix_expression_path": "x" },
                        {
                            "command": "AddTask", "name": "test", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [{ "name": "coverage", "kind": "routing-var" }]
                        },
                        { "command": "AddTask", "name": "lint", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "release", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        { "sources": ["build"], "targets": ["test"] },
                        { "sources": ["build"], "targets": ["lint"] },
                        {
                            "sources": ["lint"],
                            "targets": ["release"],
                            "gate": {
                                "kind": "predicate-gate",
                                "routing_var": "coverage",
                                "op": "Ge",
                                "value": 80
                            }
                        }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("coverage") && msg.contains("test") && msg.contains("dominate"),
            "error must name routing var, producer, and bypass: {}",
            msg
        );
    }

    #[tokio::test]
    async fn non_head_loop_control_producer_is_accepted_for_ring_and_body_exit() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "non-head-loop-producer",
            "name": "non_head_loop_producer",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "loop",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "inspect", "args": [], "outputs": [], "nix_expression_path": "x" },
                        {
                            "command": "AddTask", "name": "decide", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [{ "name": "loop_control", "kind": "routing-var", "type": "string" }]
                        },
                        { "command": "AddTask", "name": "finalize", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        { "sources": ["Start"], "targets": ["inspect"] },
                        {
                            "sources": ["inspect"], "targets": ["decide"], "kind": "loop",
                            "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "continue" }
                        },
                        {
                            "sources": ["decide"], "targets": ["inspect"], "kind": "loop",
                            "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "continue" }
                        },
                        {
                            "sources": ["inspect", "decide"], "targets": ["finalize"], "kind": "data",
                            "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "done" }
                        }
                    ]
                }
            ]
        }"#;

        let def = parse_workflow_from_json(json, "default")
            .await
            .expect("same-SCC loop_control sources do not require ordinary dominance");
        let inspect = task_id_by_name(&def, "inspect");
        let decide = task_id_by_name(&def, "decide");
        let finalize = task_id_by_name(&def, "finalize");
        let graph = graph_of(&def);
        assert_eq!(
            edge_between(graph, inspect, decide).kind,
            wf::EdgeKind::Loop as i32
        );
        let exit = edge_between(graph, inspect, finalize);
        assert_eq!(exit.kind, wf::EdgeKind::Data as i32);
        assert_eq!(
            exit.sources,
            [inspect.to_string(), decide.to_string()],
            "the default whole-body exit remains intact"
        );
    }

    #[tokio::test]
    async fn ordinary_predicate_on_loop_edge_still_requires_dominance() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "ordinary-loop-predicate",
            "name": "ordinary_loop_predicate",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "loop",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "inspect", "args": [], "outputs": [], "nix_expression_path": "x" },
                        {
                            "command": "AddTask", "name": "decide", "args": [], "outputs": [], "nix_expression_path": "x",
                            "routing_vars": [
                                { "name": "loop_control", "kind": "routing-var", "type": "string" },
                                { "name": "decision", "kind": "routing-var", "type": "string" }
                            ]
                        }
                    ],
                    "edges": [
                        { "sources": ["Start"], "targets": ["inspect"] },
                        {
                            "sources": ["inspect"], "targets": ["decide"], "kind": "loop",
                            "gate": { "kind": "predicate-gate", "routing_var": "decision", "op": "Eq", "value": "continue" }
                        },
                        {
                            "sources": ["decide"], "targets": ["inspect"], "kind": "loop",
                            "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "continue" }
                        },
                        {
                            "sources": ["decide"], "targets": ["End"], "kind": "data",
                            "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "done" }
                        }
                    ]
                }
            ]
        }"#;

        let err = parse_workflow_from_json(json, "default")
            .await
            .expect_err("non-loop routing variables retain ordinary dominance checks");
        let message = err.to_string();
        assert!(
            message.contains("decision")
                && message.contains("decide")
                && message.contains("dominate"),
            "error names the ordinary variable, producer, and dominance rule: {message}"
        );
    }

    #[tokio::test]
    async fn from_signal_input_resolves_to_dominating_gates_edge_id() {
        // Two-task workflow: build → test. Edge build→test has a
        // signal gate `approval`. Task `test` declares
        // `from.signal = approvalGate { signal.name = "approval" }`
        // on its `approver` input. The dominator pass resolves
        // gate_edge_id to the build→test edge's id.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "signal_input",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "build", "args": [], "outputs": [], "nix_expression_path": "x" },
                        {
                            "command": "AddTask",
                            "name": "test",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "x",
                            "inputs": [
                                {
                                    "name": "approver",
                                    "from": {
                                        "signal": {
                                            "kind": "signal-gate",
                                            "signal": { "name": "approval" }
                                        }
                                    }
                                }
                            ]
                        }
                    ],
                    "edges": [
                        {
                            "sources": ["build"],
                            "targets": ["test"],
                            "gate": {
                                "kind": "signal-gate",
                                "signal": { "name": "approval" }
                            }
                        }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let test_id = task_id_by_name(&def, "test");
        let task = get_task(&def, test_id);
        let list = task
            .input_sources
            .as_ref()
            .expect("test task carries input_sources");
        assert_eq!(list.sources.len(), 1);
        match list.sources[0]
            .source
            .as_ref()
            .and_then(|s| s.source.as_ref())
        {
            Some(wf::input_source::Source::Signal(sig)) => {
                assert_eq!(sig.signal_name, "approval");
                let gate_edge_id = &sig.gate_edge_id;
                assert!(
                    Uuid::parse_str(gate_edge_id).is_ok_and(|u| !u.is_nil()),
                    "dominator pass must resolve gate_edge_id"
                );
                // Confirm the resolved edge does in fact have a
                // SignalReceived gate matching "approval".
                let graph = graph_of(&def);
                let edge = graph
                    .edges
                    .iter()
                    .find(|e| &e.id == gate_edge_id)
                    .expect("edge exists");
                let matches = edge.gates.iter().any(|g| {
                    matches!(
                        g.kind.as_ref(),
                        Some(wf::gate::Kind::SignalReceived(sr)) if sr.signal_name == "approval"
                    )
                });
                assert!(matches, "resolved edge must carry the matching gate");
            }
            other => panic!("expected InputSource::Signal, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn from_signal_input_with_no_dominating_gate_is_rejected() {
        // Diamond: build → {test, lint}; gate on build→test edge;
        // task `release` after the diamond declares
        // `from.signal = approval`. The gate on build→test does NOT
        // dominate `release` (the build→lint→release path bypasses
        // the gate). Registration must fail.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "no_dominator",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "build", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "test", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "lint", "args": [], "outputs": [], "nix_expression_path": "x" },
                        {
                            "command": "AddTask",
                            "name": "release",
                            "args": [],
                            "outputs": [],
                            "nix_expression_path": "x",
                            "inputs": [
                                {
                                    "name": "approver",
                                    "from": { "signal": { "kind": "signal-gate", "signal": { "name": "approval" } } }
                                }
                            ]
                        }
                    ],
                    "edges": [
                        { "sources": ["build"], "targets": ["test"], "gate": { "kind": "signal-gate", "signal": { "name": "approval" } } },
                        { "sources": ["build"], "targets": ["lint"] },
                        { "sources": ["test", "lint"], "targets": ["release"] }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("release") && msg.contains("approval") && msg.contains("dominate"),
            "error must name the task, signal, and dominator concern: {}",
            msg
        );
    }

    #[tokio::test]
    async fn explicit_edge_with_signal_gate_projects_onto_edge() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "gated_workflow",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "fetch_order", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "ship", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        {
                            "sources": ["fetch_order"],
                            "targets": ["ship"],
                            "gate": {
                                "kind": "signal-gate",
                                "signal": { "name": "payment-cleared" },
                                "predicate": "$[?@.amount > 100]",
                                "captures": [
                                    { "name": "receipt_url", "from": { "trigger": { "jsonpath": "$.receipt" } } }
                                ],
                                "timeout": "5m"
                            }
                        }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let fetch = task_id_by_name(&def, "fetch_order");
        let ship = task_id_by_name(&def, "ship");
        let graph = graph_of(&def);

        // Find the edge that goes fetch → ship.
        let edge = edge_between(graph, fetch, ship);
        assert_eq!(edge.gates.len(), 1, "exactly one gate must be projected");
        let Some(wf::gate::Kind::SignalReceived(sr)) = edge.gates[0].kind.as_ref() else {
            panic!("expected SignalReceived, got {:?}", edge.gates[0]);
        };
        assert_eq!(sr.signal_name, "payment-cleared");
        assert_eq!(sr.predicate.as_deref(), Some("$[?@.amount > 100]"));
        assert_eq!(sr.captures_spec.len(), 1);
        assert_eq!(sr.captures_spec[0].name, "receipt_url");
        assert_eq!(
            sr.timeout,
            Some(wf::Duration {
                secs: 300,
                nanos: 0,
            })
        );
        // Definition gates are declaration-only; the per-instance `Idle` runtime
        // state is a server-apply concern absent from the published shape.
    }

    #[tokio::test]
    async fn explicit_edge_without_gate_still_works() {
        // Non-gated edges must keep round-tripping unchanged.
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "ungated",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        { "sources": ["a"], "targets": ["b"] }
                    ]
                }
            ]
        }"#;
        let def = parse_workflow_from_json(json, "default").await.unwrap();
        let a = task_id_by_name(&def, "a");
        let b = task_id_by_name(&def, "b");
        let graph = graph_of(&def);
        let edge = edge_between(graph, a, b);
        assert!(
            edge.gates.is_empty(),
            "ungated edges must project an empty gates list"
        );
    }

    #[tokio::test]
    async fn signal_gate_with_missing_signal_ref_is_rejected() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "bad_gate",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        {
                            "sources": ["a"],
                            "targets": ["b"],
                            "gate": { "kind": "signal-gate", "signal": {} }
                        }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        assert!(
            err.to_string().contains("signal"),
            "expected signal-ref validation error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn signal_gate_with_invalid_predicate_is_rejected() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "bad_gate_pred",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        {
                            "sources": ["a"],
                            "targets": ["b"],
                            "gate": {
                                "kind": "signal-gate",
                                "signal": { "name": "x" },
                                "predicate": "$[?(garbage]"
                            }
                        }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        assert!(
            err.to_string().contains("JSONPath") || err.to_string().contains("predicate"),
            "expected predicate-validation error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn signal_gate_with_malformed_timeout_is_rejected() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "bad_gate_to",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "a", "args": [], "outputs": [], "nix_expression_path": "x" },
                        { "command": "AddTask", "name": "b", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        {
                            "sources": ["a"],
                            "targets": ["b"],
                            "gate": {
                                "kind": "signal-gate",
                                "signal": { "name": "x" },
                                "timeout": "twelve_minutes"
                            }
                        }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        assert!(
            err.to_string().contains("timeout"),
            "expected timeout-parse error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn explicit_edge_with_unknown_task_is_rejected() {
        let json = r#"{
            "command": "AddWorkflow", "slug": "wf",
            "name": "bad_ref",
            "args": [],
            "outputs": [],
            "tasks": [
                {
                    "command": "AddTaskGroup",
                    "name": "g",
                    "args": [],
                    "outputs": [],
                    "tasks": [
                        { "command": "AddTask", "name": "real_task", "args": [], "outputs": [], "nix_expression_path": "x" }
                    ],
                    "edges": [
                        { "sources": ["real_task"], "targets": ["ghost_task"] }
                    ]
                }
            ]
        }"#;
        let err = parse_workflow_from_json(json, "default").await.unwrap_err();
        assert!(
            err.to_string().contains("ghost_task"),
            "error should name the missing task: {}",
            err
        );
    }

    /// Parity guard at the language boundary: the DSL author surface and the
    /// parser projection are two implementations of one operator vocabulary,
    /// and the one drift mode they admit is the literal lists diverging
    /// silently. This pins them together. Every PascalCase string the Nickel
    /// `Operator` contract accepts must drive `project_predicate_gate` to a
    /// `Gate::PredicateHolds` (the projection's `op` match arm exists), and a
    /// string outside that set must be rejected. If a new operator is added
    /// to one side only, this test fails.
    ///
    /// Source of truth for the Nickel side: `dsl/contracts.ncl` `Operator`.
    #[test]
    fn operator_set_parity_with_nickel_contract() {
        // The exact set the Nickel `Operator` contract enumerates.
        let nickel_operators = ["Eq", "NotEq", "Lt", "Le", "Gt", "Ge"];

        for op in nickel_operators {
            let parsed = ParsedPredicateGate {
                routing_var: "v".to_string(),
                op: op.to_string(),
                value: serde_json::json!(1),
                timeout: None,
            };
            let gate = project_predicate_gate_proto(&parsed, "wf").unwrap_or_else(|e| {
                panic!("Nickel `Operator` accepts `{op}` but the projection rejects it: {e}")
            });
            assert!(
                matches!(gate.kind, Some(wf::gate::Kind::PredicateHolds(_))),
                "operator `{op}` should project to a PredicateHolds gate",
            );
        }

        // A string outside the shared set must be rejected by the projection,
        // proving the match is exhaustive over exactly the Nickel set.
        let bogus = ParsedPredicateGate {
            routing_var: "v".to_string(),
            op: "Approximately".to_string(),
            value: serde_json::json!(1),
            timeout: None,
        };
        assert!(
            project_predicate_gate_proto(&bogus, "wf").is_err(),
            "an operator outside the Nickel `Operator` set must be rejected",
        );
    }

    /// Parity guard for the routing-var type tags. The Nickel `RoutingVarType`
    /// contract enumerates five tags — `string`, `int`, `bool`, `bytes`, and
    /// `array` — but the runtime `RoutingValue` is a closed scalar enum whose
    /// `type_tag()` yields only the four scalar tags. `array` is intentionally
    /// Nickel-only: it is ahead of the runtime, reserved for future edge-
    /// expansion semantics, and has no `RoutingValue` representation today.
    /// This test pins the four scalar tags against `type_tag()` and documents
    /// that `array` is deliberately NOT a runtime tag.
    ///
    /// Source of truth: `dsl/contracts.ncl` `RoutingVarType` and the
    /// published routing-value contract.
    #[test]
    fn routing_var_type_scalar_tags_parity_with_runtime() {
        use crate::routing_split::routing_value_type_tag;
        use tickr_proto::workflow::{routing_value::Value, RoutingValue};

        let values = [
            RoutingValue {
                value: Some(Value::StringValue(String::new())),
            },
            RoutingValue {
                value: Some(Value::IntValue(0)),
            },
            RoutingValue {
                value: Some(Value::BoolValue(false)),
            },
            RoutingValue {
                value: Some(Value::BytesValue(Vec::new())),
            },
        ];
        let runtime_tags = values.map(|value| routing_value_type_tag(&value));

        // The four scalar tags the Nickel contract shares with the runtime.
        for tag in ["string", "int", "bool", "bytes"] {
            assert!(
                runtime_tags.contains(&tag),
                "Nickel scalar tag `{tag}` has no matching `RoutingValue::type_tag()`",
            );
        }

        // `array` is Nickel-only — ahead of the runtime by design. No
        // `RoutingValue` variant produces it; if one ever does, the type
        // becomes runtime-real and this assertion (and the DSL's scalar-only
        // predicate-gate rule) must be revisited together.
        assert!(
            !runtime_tags.contains(&"array"),
            "`array` must remain Nickel-only; no runtime `RoutingValue` tag may be `array`",
        );
    }
}
