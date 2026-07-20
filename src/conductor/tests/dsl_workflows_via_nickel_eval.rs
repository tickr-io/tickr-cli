//! End-to-end DSL → parser tests: each `.ncl` fixture evaluates via the
//! conductor's `nickel export` parser path, deserialises into the published
//! workflow definition, and the assertions that held against the prior Nix path
//! hold against the Nickel path.
//!
//! Fixtures resolve their `import "lib.ncl"` against `TICKR_DSL_PATHS`,
//! set here to the in-tree Core DSL directory relative to this crate, so
//! the run is reproducible on any machine with `nickel` on PATH — no
//! per-machine absolute path. A machine without `nickel` skips the suite
//! with a clear message rather than failing it.
//!
//! Error-path fixtures assert on the `nickel_eval` step. The Core DSL's
//! Nickel contracts are the primary validation surface: a malformed
//! duration or a reserved `tickr/` tag namespace fails at `nickel export`
//! — before the JSON ever reaches the parser — with the contract's
//! diagnostic on stderr. (The Rust parser
//! keeps equivalent checks as defence-in-depth, but they are unreachable
//! from these fixtures now that the contracts reject upstream.)

use std::path::PathBuf;
use std::process::Command;

use tickr_conductor::parser::nickel::{nickel_eval, DSL_PATHS_ENV};
use tickr_conductor::parser::Parser;
use tickr_proto::workflow as wf;

async fn parse_wf(json: &str, ns: &str) -> anyhow::Result<wf::WorkflowDefinition> {
    Parser::parse_workflow_from_json(json, ns).await
}

fn task_by_name<'a>(definition: &'a wf::WorkflowDefinition, name: &str) -> &'a wf::TaskDefinition {
    definition
        .tasks
        .iter()
        .find(|task| task.name == name)
        .expect("task present")
}

fn task_id_by_name<'a>(definition: &'a wf::WorkflowDefinition, name: &str) -> &'a str {
    task_by_name(definition, name).id.as_str()
}

fn task_graph(definition: &wf::WorkflowDefinition) -> &wf::TaskGraph {
    definition.task_graph.as_ref().expect("task graph present")
}

fn nickel_available() -> bool {
    Command::new("nickel")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// The in-tree Core DSL directory (`tickr/dsl`), relative to this crate's
/// manifest. Exported as `TICKR_DSL_PATHS` so `nickel export` resolves
/// `import "lib.ncl"` without any per-machine configuration.
fn dsl_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("..");
    p.push("..");
    p.push("dsl");
    p
}

fn set_dsl_path() {
    std::env::set_var(DSL_PATHS_ENV, dsl_dir());
}

fn fixture_source(name: &str) -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p.push("dsl_workflows");
    p.push(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read fixture {}: {}", p.display(), e))
}

/// Evaluate a fixture to JSON through the conductor's production Nickel
/// parser path (`nickel export` against the configured search path).
async fn eval(fixture: &str) -> String {
    set_dsl_path();
    nickel_eval(&fixture_source(fixture))
        .await
        .unwrap_or_else(|e| panic!("nickel export failed for {}: {}", fixture, e))
}

/// Evaluate a fixture the conductor would reject and return the
/// `nickel_eval` error string (the forwarded Nickel stderr). Used by the
/// reject-path fixtures whose rule now lives in a Core DSL contract: the
/// failure surfaces at `nickel export`, not at `Parser::parse`.
async fn eval_err(fixture: &str) -> String {
    set_dsl_path();
    nickel_eval(&fixture_source(fixture))
        .await
        .expect_err(&format!("nickel export must reject {}", fixture))
        .to_string()
}

/// Every fixture the suite expects must be present on disk — guards
/// against a stray rename silently dropping coverage.
#[test]
fn every_expected_fixture_is_present_on_disk() {
    for f in [
        "fire_now_with_captures.ncl",
        "waits_on_signal_deferred.ncl",
        "mixed_inputs.ncl",
        "task_timeout_set.ncl",
        "workflow_timeout_set.ncl",
        "task_and_workflow_timeout_both_set.ncl",
        "task_timeout_omitted.ncl",
        "malformed_duration_rejected.ncl",
        "tags_set.ncl",
        "tags_omitted.ncl",
        "tags_reserved_namespace_rejected.ncl",
        "waits_on_signal_with_predicate_and_captures.ncl",
        "edge_with_signal_gate.ncl",
        "predicate_gate_routing.ncl",
        "slug_invalid_rejected.ncl",
        "mkloop_single_task.ncl",
        "mkloop_multi_task_ring.ncl",
        "chain_serial_spine.ncl",
        "handwired_serial_spine.ncl",
        "nested_chain_spine.ncl",
        "handwired_nested_spine.ncl",
        "fragment_overlap_rejected.ncl",
        "fragment_duplicate_name_rejected.ncl",
        "fragment_free_overlap_shape.ncl",
    ] {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("tests");
        p.push("fixtures");
        p.push("dsl_workflows");
        p.push(f);
        assert!(p.exists(), "fixture {f} missing on disk");
    }
}

#[tokio::test]
async fn fire_now_with_captures_fixture_round_trips_through_parser() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("fire_now_with_captures.ncl").await;
    let def = Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect("parser accepts the fire-now-with-captures fixture");
    // FireNow projects directly onto the published trigger declaration.
    assert!(matches!(
        def.trigger
            .as_ref()
            .and_then(|trigger| trigger.kind.as_ref()),
        Some(wf::trigger::Kind::FireNow(_))
    ));

    // captures land on the workflow
    let captures = &def.captures;
    assert_eq!(captures.len(), 1);
    assert_eq!(captures[0].name, "user_email");

    // downstream task records the Trigger source on its input slot
    let task = task_by_name(&def, "notify");
    assert_eq!(task.inputs, ["user_email"]);
    // The structured input slot records the Trigger source on the proto task.
    let task_def = def
        .tasks
        .iter()
        .find(|t| t.name == "notify")
        .expect("notify task present");
    let sources = &task_def
        .input_sources
        .as_ref()
        .expect("structured input populates input_sources")
        .sources;
    assert_eq!(sources.len(), 1);
    assert!(matches!(
        sources[0].source.as_ref().and_then(|is| is.source.as_ref()),
        Some(wf::input_source::Source::Trigger(_))
    ));
}

#[tokio::test]
async fn waits_on_signal_with_predicate_and_captures_fixture_round_trips() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("waits_on_signal_with_predicate_and_captures.ncl").await;
    let workflow = parse_wf(&json, "default")
        .await
        .expect("waits-on-signal with predicate + captures must parse");
    let Some(wf::trigger::Kind::WaitsOnSignal(cfg)) = workflow
        .trigger
        .as_ref()
        .and_then(|trigger| trigger.kind.as_ref())
    else {
        panic!("waits_on_signal config must be projected onto the workflow");
    };
    assert_eq!(cfg.signal_name, "user-paid");
    assert_eq!(cfg.predicate.as_deref(), Some("$[?@.amount > 100]"));
    assert_eq!(cfg.captures.len(), 1);
    assert_eq!(cfg.captures[0].name, "user_email");
}

#[tokio::test]
async fn waits_on_signal_fixture_projects_onto_workflow() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("waits_on_signal_deferred.ncl").await;
    let workflow = parse_wf(&json, "default")
        .await
        .expect("waits-on-signal must be functional");
    let Some(wf::trigger::Kind::WaitsOnSignal(cfg)) = workflow
        .trigger
        .as_ref()
        .and_then(|trigger| trigger.kind.as_ref())
    else {
        panic!("waits_on_signal config must be projected onto the workflow");
    };
    assert_eq!(cfg.signal_name, "approval");
    assert!(cfg.predicate.is_none());
    assert!(cfg.captures.is_empty());
}

#[tokio::test]
async fn mixed_inputs_fixture_preserves_per_slot_sources() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("mixed_inputs.ncl").await;
    let def = Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect("parser accepts the mixed-inputs fixture");
    let consume = task_by_name(&def, "consume");
    assert_eq!(consume.inputs, ["image_digest", "user_email"]);
    let consume_def = def
        .tasks
        .iter()
        .find(|t| t.name == "consume")
        .expect("consume task present");
    let sources = &consume_def
        .input_sources
        .as_ref()
        .expect("any structured entry promotes input_sources to Some")
        .sources;
    assert_eq!(sources.len(), 2);
    assert!(sources[0].source.is_none(), "bare input slot stays None");
    assert!(
        matches!(
            sources[1].source.as_ref().and_then(|is| is.source.as_ref()),
            Some(wf::input_source::Source::Trigger(_))
        ),
        "structured trigger slot populates as Trigger"
    );
}

#[tokio::test]
async fn tags_fixture_round_trips_into_workflow_tags() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("tags_set.ncl").await;
    let workflow = parse_wf(&json, "default")
        .await
        .expect("parser accepts the tags-set fixture");
    let tags = &workflow.tags;
    assert_eq!(tags.get("env").map(String::as_str), Some("prod"));
    assert_eq!(tags.get("team").map(String::as_str), Some("billing"));
    assert!(
        !tags.contains_key("tickr/workflow_id"),
        "definition tags must not carry the `tickr/`-namespaced runtime defaults"
    );
}

#[tokio::test]
async fn tags_omitted_fixture_loads_with_empty_tag_map() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("tags_omitted.ncl").await;
    assert!(
        !json.contains("\"tags\""),
        "JSON should not carry a tags key when the field is omitted: {json}"
    );
    let workflow = parse_wf(&json, "default")
        .await
        .expect("parser accepts a workflow without tags");
    assert!(workflow.tags.is_empty());
}

#[tokio::test]
async fn tags_reserved_namespace_fixture_fails_registration() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let msg = eval_err("tags_reserved_namespace_rejected.ncl").await;
    assert!(
        msg.contains("tickr/workflow_id"),
        "error must name the offending key, got: {msg}"
    );
}

#[tokio::test]
async fn task_timeout_fixture_projects_onto_task_timeout_secs() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("task_timeout_set.ncl").await;
    let workflow = parse_wf(&json, "default")
        .await
        .expect("parser accepts the task-timeout fixture");
    let task = task_by_name(&workflow, "slow_task");
    assert_eq!(task.timeout_secs, Some(30));
    assert_eq!(
        workflow.timeout_secs, None,
        "workflow-level timeout stays unset when only per-task is provided"
    );
}

#[tokio::test]
async fn workflow_timeout_fixture_projects_onto_workflow_timeout_secs() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("workflow_timeout_set.ncl").await;
    let workflow = parse_wf(&json, "default")
        .await
        .expect("parser accepts the workflow-timeout fixture");
    assert_eq!(workflow.timeout_secs, Some(300));
    let task = workflow.tasks.first().expect("at least one task");
    assert_eq!(
        task.timeout_secs, None,
        "task-level timeout stays unset when only per-workflow is provided"
    );
}

#[tokio::test]
async fn both_timeouts_fixture_projects_both_fields_independently() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("task_and_workflow_timeout_both_set.ncl").await;
    let workflow = parse_wf(&json, "default")
        .await
        .expect("parser accepts the both-timeouts fixture");
    assert_eq!(workflow.timeout_secs, Some(3600));
    let task = task_by_name(&workflow, "slow_task");
    assert_eq!(task.timeout_secs, Some(3600));
}

#[tokio::test]
async fn omitted_timeout_fixture_leaves_both_fields_none() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("task_timeout_omitted.ncl").await;
    let workflow = parse_wf(&json, "default")
        .await
        .expect("parser accepts the omitted-timeout fixture");
    assert_eq!(workflow.timeout_secs, None);
    for task in &workflow.tasks {
        assert_eq!(task.timeout_secs, None);
    }
}

#[tokio::test]
async fn malformed_duration_fixture_fails_registration() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    // The `Duration` contract fires on the offending timeout value, so the
    // diagnostic quotes the value (`30q`) and the accepted grammar — but
    // not the enclosing task's `name` (a sibling field the contract can't
    // see). The prior parser-path assertion checked the task name; the
    // contract-path assertion checks the value and the rule instead.
    let msg = eval_err("malformed_duration_rejected.ncl").await;
    assert!(
        msg.contains("30q") && msg.contains("duration"),
        "error must name the offending value and the duration rule, got: {msg}"
    );
}

/// A malformed `slug` is rejected at `nickel export` by the Core DSL's `Slug`
/// contract — the identity-input guarantee surfaces at the author's desk, not
/// at registration. The diagnostic names the offending value and the slug rule.
#[tokio::test]
async fn slug_invalid_fixture_fails_registration() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let msg = eval_err("slug_invalid_rejected.ncl").await;
    assert!(
        msg.contains("Bad_Slug") && msg.contains("slug"),
        "error must name the offending value and the slug rule, got: {msg}"
    );
}

/// An authored workflow routing on a produced routing variable: `producer`
/// declares `mkRoutingVar { name = "decision" }` and two edges each carry a
/// `mkPredicateGate` over it. The fixture must round-trip through the parser
/// — declaring the routing variable on the producer and projecting both
/// edges onto `Gate::PredicateHolds` over `decision` (the producer dominates
/// both edges, so the dominator check passes). Runtime firing (true → fires,
/// false → pending) is proven by the simulation suite; this asserts the
/// authoring shape parses.
#[tokio::test]
async fn predicate_gate_routing_fixture_round_trips_through_parser() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("predicate_gate_routing.ncl").await;
    let def = Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect("parser accepts the predicate-gate-routing fixture");
    // The producer declares the routing variable the gates branch on.
    let producer = task_by_name(&def, "producer");
    let declared: Vec<&str> = producer
        .routing_vars
        .iter()
        .map(|rv| rv.name.as_str())
        .collect();
    assert_eq!(declared, ["decision"]);

    // Two edges carry `PredicateHolds` gates over `decision` — one matching
    // "approve", the other "reject" — both with the `Eq` operator. Asserted on
    // the parsed proto definition, where a gate's `op` is an i32 discriminant
    // and its value a proto scalar.
    let mut predicate_values: Vec<String> = def
        .task_graph
        .as_ref()
        .expect("definition carries a task graph")
        .edges
        .iter()
        .flat_map(|e| e.gates.iter())
        .filter_map(|g| match &g.kind {
            Some(wf::gate::Kind::PredicateHolds(ph))
                if ph.routing_var == "decision" && ph.op == wf::ComparisonOp::Eq as i32 =>
            {
                match ph.value.as_ref().and_then(|v| v.value.as_ref()) {
                    Some(wf::routing_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect();
    predicate_values.sort();
    assert_eq!(
        predicate_values,
        vec!["approve".to_string(), "reject".to_string()],
        "both edges project onto PredicateHolds over `decision`"
    );
}

/// `mkSignalGate` produces the JSON shape the conductor parser projects
/// onto `Gate::SignalReceived`, and the gated workflow parses end-to-end.
/// The raw JSONPath predicate must survive Nickel's exporter intact.
#[tokio::test]
async fn signal_gate_fixture_emits_expected_json_shape() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("edge_with_signal_gate.ncl").await;
    Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect("parser accepts the gated-edge fixture");

    let value: serde_json::Value = serde_json::from_str(&json).expect("nickel output is JSON");
    let edges = value["tasks"][0]["edges"]
        .as_array()
        .expect("tasks[0].edges must be a JSON array");
    assert_eq!(edges.len(), 1, "fixture authors a single gated edge");
    let edge = &edges[0];
    assert_eq!(edge["sources"][0].as_str(), Some("fetch_order"));
    assert_eq!(edge["targets"][0].as_str(), Some("ship"));
    let gate = &edge["gate"];
    assert_eq!(gate["kind"].as_str(), Some("signal-gate"));
    assert_eq!(gate["signal"]["name"].as_str(), Some("payment-cleared"));
    assert_eq!(
        gate["predicate"].as_str(),
        Some("$[?@.amount > 100]"),
        "raw JSONPath predicate must survive Nickel's exporter intact"
    );
    assert_eq!(gate["timeout"].as_str(), Some("5m"));
    let captures = gate["captures"].as_array().expect("captures present");
    assert_eq!(captures.len(), 1);
    assert_eq!(captures[0]["name"].as_str(), Some("receipt_url"));
}

/// `mkLoop` end-to-end through the production Nickel path: a workflow authored
/// with one `mkLoop` call evaluates to the expected group shape and parses
/// into the runnable self-loop graph — `loop_control` declared on the task, a
/// `kind = loop` self-edge gated `continue`, a `kind = data` exit edge gated
/// `done` targeting `End`, and (after the loop-aware seal) `start → looper` so
/// the loop is reachable. This is the graph the runtime spine is independently
/// proven to run to `Completed`; the sugar produces it with no hand-wiring.
#[tokio::test]
async fn mkloop_single_task_evaluates_and_parses_into_runnable_self_loop() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("mkloop_single_task.ncl").await;

    // 1. The evaluated group carries the auto-wired shape: loop_control on the
    //    task, a kind=loop self-edge gated continue, a kind=data exit gated
    //    done to End, and the (carried-not-enforced) max_iterations.
    let value: serde_json::Value = serde_json::from_str(&json).expect("mkLoop JSON parses");
    let group = &value["tasks"][0];
    // The vestigial `command` marker was dropped from the DSL; the group's
    // real shape (loop_control, edges, max_iterations) is asserted below.
    assert!(
        group.is_object(),
        "the loop expands into a task group object"
    );
    assert_eq!(group["max_iterations"].as_u64(), Some(5));
    let task = &group["tasks"][0];
    let rvs = task["routing_vars"]
        .as_array()
        .expect("routing_vars present");
    assert!(
        rvs.iter()
            .any(|rv| rv["name"].as_str() == Some("loop_control")
                && rv["type"].as_str() == Some("string")),
        "loop_control declared as a string routing variable: {rvs:?}"
    );
    let edges = group["edges"].as_array().expect("edges present");
    let loop_edge = edges
        .iter()
        .find(|e| e["kind"].as_str() == Some("loop"))
        .expect("a kind=loop self-edge");
    assert_eq!(loop_edge["sources"][0].as_str(), Some("poll"));
    assert_eq!(loop_edge["targets"][0].as_str(), Some("poll"));
    assert_eq!(
        loop_edge["gate"]["routing_var"].as_str(),
        Some("loop_control")
    );
    assert_eq!(loop_edge["gate"]["value"].as_str(), Some("continue"));
    let exit_edge = edges
        .iter()
        .find(|e| e["kind"].as_str() == Some("data"))
        .expect("a kind=data exit edge");
    assert_eq!(exit_edge["targets"][0].as_str(), Some("End"));
    assert_eq!(exit_edge["gate"]["value"].as_str(), Some("done"));

    // 2. The sugar parses into the runnable graph the spine executes.
    let def = Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect("mkLoop workflow parses (loop-terminability validator accepts it)");
    let looper = task_by_name(&def, "poll");
    assert!(
        looper.loop_participant,
        "the source of a kind=loop edge is stamped loop_participant"
    );
    let looper_id = looper.id.clone();
    let pgraph = def
        .task_graph
        .as_ref()
        .expect("definition carries a task graph");
    // start → looper exists despite the self-loop (loop-aware seal).
    assert!(
        pgraph
            .edges
            .iter()
            .any(|edge| edge.sources.contains(&pgraph.start) && edge.targets.contains(&looper_id)),
        "the loop-aware seal wires start → looper so the loop is reachable"
    );
    // The loop self-edge carries kind=loop and a loop_control==continue gate —
    // asserted on the parsed proto definition (edge `kind` is an i32).
    let looper_s = looper_id;
    let end_s = pgraph.end.clone();
    let self_loop = pgraph
        .edges
        .iter()
        .find(|e| e.sources.contains(&looper_s) && e.targets.contains(&looper_s))
        .expect("self-loop edge present");
    assert_eq!(self_loop.kind, wf::EdgeKind::Loop as i32);
    assert!(self_loop.gates.iter().any(|g| matches!(
        &g.kind,
        Some(wf::gate::Kind::PredicateHolds(ph)) if ph.routing_var == "loop_control"
    )));
    // The exit edge targets End, carries kind=data and a loop_control==done gate.
    let exit = pgraph
        .edges
        .iter()
        .find(|e| e.sources.contains(&looper_s) && e.targets.contains(&end_s))
        .expect("exit edge to End present");
    assert_eq!(exit.kind, wf::EdgeKind::Data as i32);
    assert!(exit.gates.iter().any(|g| matches!(
        &g.kind,
        Some(wf::gate::Kind::PredicateHolds(ph)) if ph.routing_var == "loop_control"
    )));
}

/// Multi-task `mkLoop` end-to-end: a two-node `griller` ⇄ `grilly` ring that
/// exits into a `judge`, authored with one `mkLoop` call. The sugar evaluates
/// to the ring (`kind = loop` back-edges gated `continue`), a single
/// `loop_control` producer (the head), the entry edge into the head, and a
/// whole-body exit fan-in (sources = every body node) gated `done` → the judge;
/// and it parses into the graph the issue-01 runtime tears down — entered at the
/// head only (the sequential ring), not at every body node.
#[tokio::test]
async fn mkloop_multi_task_ring_evaluates_and_parses_into_runnable_loop_body() {
    if !nickel_available() {
        eprintln!("skipping: `nickel` not on PATH");
        return;
    }
    let json = eval("mkloop_multi_task_ring.ncl").await;

    // 1. The evaluated group: a ring, a single producer (head), the entry edge,
    //    and the whole-body exit fan-in.
    let value: serde_json::Value = serde_json::from_str(&json).expect("mkLoop JSON parses");
    let group = &value["tasks"][0];
    // The vestigial `command` marker was dropped from the DSL; the group's
    // real structure (its ring, producer, and edges) is asserted below.
    assert!(
        group.is_object(),
        "the loop expands into a task group object"
    );
    let group_tasks = group["tasks"].as_array().expect("body tasks present");
    // Single producer: only the head (`griller`) declares loop_control.
    let declares_loop_control = |name: &str| {
        group_tasks
            .iter()
            .find(|t| t["name"].as_str() == Some(name))
            .and_then(|t| t["routing_vars"].as_array())
            .is_some_and(|rvs| {
                rvs.iter()
                    .any(|rv| rv["name"].as_str() == Some("loop_control"))
            })
    };
    assert!(
        declares_loop_control("griller"),
        "the head griller declares loop_control"
    );
    assert!(
        !declares_loop_control("grilly"),
        "the sibling grilly never declares loop_control (single producer)"
    );

    let edges = group["edges"].as_array().expect("edges present");
    let find_edge = |srcs: &[&str], tgts: &[&str]| {
        edges.iter().find(|e| {
            let s: Vec<&str> = e["sources"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|v| v.as_str())
                .collect();
            let t: Vec<&str> = e["targets"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|v| v.as_str())
                .collect();
            s.len() == srcs.len()
                && srcs.iter().all(|x| s.contains(x))
                && t.len() == tgts.len()
                && tgts.iter().all(|x| t.contains(x))
        })
    };
    // Ring back-edges, both kind=loop gated continue.
    for (from, to) in [("griller", "grilly"), ("grilly", "griller")] {
        let e = find_edge(&[from], &[to]).unwrap_or_else(|| panic!("ring edge {from}->{to}"));
        assert_eq!(
            e["kind"].as_str(),
            Some("loop"),
            "{from}->{to} is kind=loop"
        );
        assert_eq!(e["gate"]["value"].as_str(), Some("continue"));
    }
    // Entry edge into the head, Start -> griller (no gate, control).
    let entry = find_edge(&["Start"], &["griller"]).expect("entry edge Start->griller");
    assert!(
        entry.get("gate").is_none(),
        "the entry edge is an un-gated control edge"
    );
    // Whole-body exit fan-in: sources = every body node, gated done -> judge.
    let exit =
        find_edge(&["griller", "grilly"], &["judge"]).expect("exit fan-in {griller,grilly}->judge");
    assert_eq!(exit["kind"].as_str(), Some("data"));
    assert_eq!(exit["gate"]["value"].as_str(), Some("done"));

    // 2. The sugar parses into the runnable graph and is entered once.
    let def = Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect("multi-task mkLoop parses (loop-terminability validator accepts it)");
    let griller = task_by_name(&def, "griller");
    let grilly = task_by_name(&def, "grilly");
    let judge = task_by_name(&def, "judge");

    // Both ring nodes are loop participants; the judge is not.
    assert!(griller.loop_participant);
    assert!(grilly.loop_participant);
    assert!(!judge.loop_participant);

    let pgraph = def
        .task_graph
        .as_ref()
        .expect("definition carries a task graph");
    // Entered at the head ONLY — the loop-aware seal does not start-wire the
    // sibling (that would dispatch the whole body at once).
    assert!(
        pgraph
            .edges
            .iter()
            .any(|edge| edge.sources.contains(&pgraph.start) && edge.targets.contains(&griller.id)),
        "start → griller (the head)"
    );
    assert!(
        !pgraph
            .edges
            .iter()
            .any(|edge| edge.sources.contains(&pgraph.start) && edge.targets.contains(&grilly.id)),
        "start does NOT wire to the sibling — the ring is entered once at the head"
    );

    // The whole-body exit fan-in is a hyperedge from both body nodes to the
    // judge — asserted on the parsed proto definition (edge `kind` is an i32).
    let griller_s = griller.id.clone();
    let grilly_s = grilly.id.clone();
    let judge_s = judge.id.clone();
    let fanin = pgraph
        .edges
        .iter()
        .find(|e| e.targets.contains(&judge_s) && e.sources.contains(&griller_s))
        .expect("exit fan-in into the judge present");
    assert_eq!(fanin.kind, wf::EdgeKind::Data as i32);
    assert!(
        fanin.sources.contains(&griller_s) && fanin.sources.contains(&grilly_s),
        "the exit edge fans in from the whole body so the judge runs once the body is done"
    );
    assert!(fanin.gates.iter().any(|g| matches!(
        &g.kind,
        Some(wf::gate::Kind::PredicateHolds(ph)) if ph.routing_var == "loop_control"
    )));
}

/// The bare-verb `chain` combinator lowers byte-identically to its hand-wired
/// twin. `chain [ prepare, build, verify ]` returns a graph fragment that
/// `mkTaskGroup` flattens into loose tasks + the ungated control edges
/// `prepare → build → verify` — exactly what the hand-wirer restates as two
/// `mkEdge`s. Proven end-to-end: both fixtures evaluate through the production
/// Nickel path to a byte-identical wire document, the `graph-fragment` tag
/// never survives export, and both parse into the serial-spine reference shape.
///
/// This proof deliberately does NOT skip when `nickel` is absent — it asserts
/// availability and hard-fails instead. A one-shot run that never exercised the
/// byte-identity proof cannot be allowed to report the criterion as met.
#[tokio::test]
async fn chain_authored_spine_is_byte_identical_to_hand_wired_twin() {
    assert!(
        nickel_available(),
        "the chain byte-identity proof requires `nickel` on PATH; it must hard-fail \
         rather than skip green — a run that never exercised the proof cannot report it met"
    );

    let chain_json = eval("chain_serial_spine.ncl").await;
    let hand_json = eval("handwired_serial_spine.ncl").await;

    // The chain lowering emits exactly the hand-wired wire document: the
    // fragment tag is stripped and the derived control edges match the
    // hand-threaded ones byte-for-byte.
    assert_eq!(
        chain_json, hand_json,
        "chain-authored spine must lower byte-identically to its hand-wired twin"
    );
    // The `graph-fragment` tag never leaks into the exported wire document.
    assert!(
        !chain_json.contains("graph-fragment"),
        "the fragment tag must not appear in the exported wire document: {chain_json}"
    );

    // Both deserialise through the parser; the chain-authored one carries the
    // serial-spine reference shape — three serial tasks threaded by ungated
    // control edges prepare → build → verify.
    let workflow = Parser::parse_workflow_from_json(&chain_json, "default")
        .await
        .expect("chain-authored spine parses");
    Parser::parse_workflow_from_json(&hand_json, "default")
        .await
        .expect("hand-wired spine parses");

    let prepare = task_id_by_name(&workflow, "prepare");
    let build = task_id_by_name(&workflow, "build");
    let verify = task_id_by_name(&workflow, "verify");

    let graph = task_graph(&workflow);
    let control_edge = |from: &str, to: &str| {
        graph.edges.iter().any(|e| {
            e.sources.len() == 1
                && e.targets.len() == 1
                && e.sources.iter().any(|source| source == from)
                && e.targets.iter().any(|target| target == to)
        })
    };
    assert!(
        control_edge(prepare, build),
        "chain threads prepare → build"
    );
    assert!(control_edge(build, verify), "chain threads build → verify");
}

/// `chain` closed under nesting: a spine built from nested sub-sequences
/// (`chain [ chain [prepare, build], chain [test, deploy] ]`) deserialises into
/// the **same parsed workflow** as the fully hand-wired twin. This proves the
/// nesting flatten end-to-end: the two inner chains' own edges survive, each
/// boundary between adjacent elements is linked by exactly one control edge
/// (last-of-i → first-of-i+1), and no boundary is doubled or missed — the
/// resulting control-edge set is exactly the hand-wired `prepare → build →
/// test → deploy` spine.
///
/// Like the flat byte-identity proof, this deliberately hard-fails rather than
/// skips when `nickel` is absent — a run that never exercised the nesting
/// proof cannot report the criterion met.
#[tokio::test]
async fn nested_chain_group_deserialises_same_as_hand_wired_twin() {
    use std::collections::BTreeSet;

    assert!(
        nickel_available(),
        "the nested-chain equivalence proof requires `nickel` on PATH; it must hard-fail \
         rather than skip green — a run that never exercised the proof cannot report it met"
    );

    let chain_json = eval("nested_chain_spine.ncl").await;
    let hand_json = eval("handwired_nested_spine.ncl").await;

    // The nesting flatten strips the fragment tag before export.
    assert!(
        !chain_json.contains("graph-fragment"),
        "the fragment tag must not appear in the exported wire document: {chain_json}"
    );

    let chain_wf = Parser::parse_workflow_from_json(&chain_json, "default")
        .await
        .expect("nested-chain spine parses");
    let hand_wf = Parser::parse_workflow_from_json(&hand_json, "default")
        .await
        .expect("hand-wired nested spine parses");

    // The set of control edges between the four named tasks, as (from, to)
    // name pairs. Edges touching the Start/End sentinels resolve to no name
    // and drop out, so this compares exactly the authored spine.
    let control_edge_names = |definition: &wf::WorkflowDefinition| -> BTreeSet<(String, String)> {
        let names = ["prepare", "build", "test", "deploy"];
        let id_to_name: std::collections::HashMap<&str, &str> = names
            .iter()
            .map(|name| (task_id_by_name(definition, name), *name))
            .collect();
        let mut pairs = BTreeSet::new();
        for edge in &task_graph(definition).edges {
            for source in &edge.sources {
                for target in &edge.targets {
                    if let (Some(source_name), Some(target_name)) = (
                        id_to_name.get(source.as_str()),
                        id_to_name.get(target.as_str()),
                    ) {
                        pairs.insert(((*source_name).to_string(), (*target_name).to_string()));
                    }
                }
            }
        }
        pairs
    };

    let chain_edges = control_edge_names(&chain_wf);
    let hand_edges = control_edge_names(&hand_wf);

    // The nested-chain group and the hand-wired group carry the identical
    // control-edge set — same parsed workflow.
    assert_eq!(
        chain_edges, hand_edges,
        "nested chain must deserialise into the same control-edge set as the hand-wired twin"
    );
    // And that set is exactly the spine, each boundary linked once, none doubled.
    let expected: BTreeSet<(String, String)> =
        [("prepare", "build"), ("build", "test"), ("test", "deploy")]
            .into_iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
    assert_eq!(
        chain_edges, expected,
        "the flattened spine links prepare → build → test → deploy exactly once each"
    );
}

/// Fragment-gated **overlap detection**: a group where a `chain` fragment
/// contributes a `(source, target)` pair that another edge in the group also
/// draws is rejected at `nickel export`. The fixture pairs the fragment's
/// `a → c` with a hand-wired barrier edge `{a, b} → c` — the whole edges differ,
/// so only pair decomposition (`sources × targets`) catches the shared `a → c`.
/// This is the tooth behind the split-at-the-link idiom: a parallel edge over a
/// fragment's pair is a silent gate bypass under per-edge dispatch. The blame
/// names the offending pair.
///
/// Hard-fails rather than skips when `nickel` is absent — a run that never
/// exercised the detection cannot report the criterion met.
#[tokio::test]
async fn fragment_overlap_pair_is_rejected_with_pair_blame() {
    assert!(
        nickel_available(),
        "the fragment-overlap detection proof requires `nickel` on PATH; it must hard-fail \
         rather than skip green — a run that never exercised the proof cannot report it met"
    );
    let msg = eval_err("fragment_overlap_rejected.ncl").await;
    assert!(
        msg.contains("a -> c"),
        "error must name the offending pair `a -> c`, got: {msg}"
    );
}

/// Fragment-gated **group-wide task-name uniqueness**: a name shared by a
/// `chain` fragment task and a loose task in the same group is rejected at
/// `nickel export`, with the blame naming the duplicated name — two tasks never
/// collide in the group's name namespace.
///
/// Hard-fails rather than skips when `nickel` is absent.
#[tokio::test]
async fn fragment_duplicate_task_name_is_rejected_with_name_blame() {
    assert!(
        nickel_available(),
        "the duplicate-name detection proof requires `nickel` on PATH; it must hard-fail \
         rather than skip green — a run that never exercised the proof cannot report it met"
    );
    let msg = eval_err("fragment_duplicate_name_rejected.ncl").await;
    assert!(
        msg.contains("dup") && msg.contains("more than one task"),
        "error must name the duplicated task name `dup`, got: {msg}"
    );
}

/// The detections are **fragment-gated**: the fragment-free twin of the overlap
/// fixture — the identical task set and edge shape (a loose `a → c` beside a
/// barrier `{a, b} → c`, drawing the pair `a → c` twice) but with **no** `chain`
/// fragment — validates cleanly and parses. Proves the detection does not
/// regress existing hand-wired files: the same shape that a fragment would make
/// illegal is tolerated when no fragment is present (the pre-existing hand-wired
/// parallel-edge surface is unchanged).
///
/// Hard-fails rather than skips when `nickel` is absent.
#[tokio::test]
async fn fragment_free_overlap_shape_validates_and_parses() {
    assert!(
        nickel_available(),
        "the fragment-gating proof requires `nickel` on PATH; it must hard-fail \
         rather than skip green — a run that never exercised the proof cannot report it met"
    );
    let json = eval("fragment_free_overlap_shape.ncl").await;
    assert!(
        !json.contains("graph-fragment"),
        "no fragment was ingested; the tag must not appear in the exported document: {json}"
    );
    Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect("a fragment-free group carrying the would-be-rejected shape still parses");
}
