//! Parse-time contract for the loop spine: a hand-authored self-loop (a raw
//! `kind = loop` self-edge gated `loop_control == continue`, a `kind = data`
//! exit edge gated `loop_control == done`, and a `loop_control` routing
//! variable on the one task) round-trips through the parser carrying the edge
//! `kind` onto the published graph and stamping `loop_participant` on the loop
//! task's definition. No `mkLoop` sugar — the fixture is wired by hand.

use tickr_conductor::parser::Parser;
use tickr_proto::workflow as wf;

fn task_id_by_name(definition: &wf::WorkflowDefinition, name: &str) -> String {
    definition
        .tasks
        .iter()
        .find(|task| task.name == name)
        .expect("task present")
        .id
        .clone()
}

/// JSON in the shape `nickel_eval` produces — fed straight to the builder so
/// the test does not depend on `nickel` being on PATH.
const SELF_LOOP_JSON: &str = r#"{
  "slug": "self-loop",
  "name": "self-loop",
  "command": "AddWorkflow",
  "args": [],
  "outputs": [],
  "tasks": [
    {
      "name": "group",
      "command": "AddTaskGroup",
      "args": [],
      "outputs": [],
      "tasks": [
        {
          "name": "looper",
          "command": "echo",
          "args": [],
          "nix_expression_path": "x",
          "outputs": [],
          "routing_vars": [
            { "name": "loop_control", "kind": "routing-var", "type": "string" }
          ]
        },
        {
          "name": "sink",
          "command": "echo",
          "args": [],
          "nix_expression_path": "x",
          "outputs": []
        }
      ],
      "edges": [
        {
          "sources": ["looper"],
          "targets": ["looper"],
          "kind": "loop",
          "gate": {
            "kind": "predicate-gate",
            "routing_var": "loop_control",
            "op": "Eq",
            "value": "continue"
          }
        },
        {
          "sources": ["looper"],
          "targets": ["sink"],
          "kind": "data",
          "gate": {
            "kind": "predicate-gate",
            "routing_var": "loop_control",
            "op": "Eq",
            "value": "done"
          }
        }
      ]
    }
  ]
}"#;

#[tokio::test]
async fn self_loop_carries_edge_kind_and_stamps_loop_participant() {
    let def = Parser::parse_workflow_from_json(SELF_LOOP_JSON, "default")
        .await
        .expect("hand-authored workflow parses");

    // `loop_participant` is stamped on the published task definition at parse
    // time (derived from the `kind = loop` edges), not on the graph node.
    let looper_id = task_id_by_name(&def, "looper");
    let looper = def.tasks.iter().find(|task| task.id == looper_id).unwrap();
    assert!(
        looper.loop_participant,
        "the source of a `kind = loop` edge is stamped loop_participant"
    );
    let sink_id = task_id_by_name(&def, "sink");
    let sink = def.tasks.iter().find(|task| task.id == sink_id).unwrap();
    assert!(
        !sink.loop_participant,
        "a task with no loop edge is not a loop participant"
    );

    // Edge `kind` is carried onto the parsed definition's edges (no longer
    // dropped). The proto edges reference graph-slot node ids as strings.
    let graph = def
        .task_graph
        .as_ref()
        .expect("definition carries a task graph");
    let looper_s = looper_id.to_string();
    let sink_s = sink_id.to_string();
    let loop_edge = graph
        .edges
        .iter()
        .find(|e| e.sources.contains(&looper_s) && e.targets.contains(&looper_s))
        .expect("self-loop edge present");
    assert_eq!(
        loop_edge.kind,
        wf::EdgeKind::Loop as i32,
        "self-edge carries kind=loop"
    );

    let exit_edge = graph
        .edges
        .iter()
        .find(|e| e.sources.contains(&looper_s) && e.targets.contains(&sink_s))
        .expect("exit edge present");
    assert_eq!(
        exit_edge.kind,
        wf::EdgeKind::Data as i32,
        "exit edge carries kind=data"
    );
}

/// A single-task `mkLoop`-shaped group (looper with a `kind = loop` self-edge
/// gated `continue` and a `kind = data` exit edge gated `done` to the reserved
/// `End`) parses into the runnable self-loop: the loop-terminability validator
/// accepts it, and the loop-aware seal wires `start → looper` despite the
/// self-edge so the loop is reachable. JSON is hand-shaped (no `nickel` needed).
const MKLOOP_SHAPED_JSON: &str = r#"{
  "slug": "mkloop-shaped",
  "name": "mkloop-shaped",
  "command": "AddWorkflow",
  "args": [],
  "outputs": [],
  "tasks": [
    {
      "name": "group",
      "command": "AddTaskGroup",
      "args": [],
      "outputs": [],
      "tasks": [
        {
          "name": "looper",
          "command": "echo",
          "args": [],
          "nix_expression_path": "x",
          "outputs": [],
          "routing_vars": [
            { "name": "loop_control", "kind": "routing-var", "type": "string" }
          ]
        }
      ],
      "max_iterations": 5,
      "edges": [
        {
          "sources": ["looper"], "targets": ["looper"], "kind": "loop",
          "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "continue" }
        },
        {
          "sources": ["looper"], "targets": ["End"], "kind": "data",
          "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "done" }
        }
      ]
    }
  ]
}"#;

#[tokio::test]
async fn mkloop_shaped_self_loop_parses_and_is_accepted() {
    let definition = Parser::parse_workflow_from_json(MKLOOP_SHAPED_JSON, "default")
        .await
        .expect("a terminable single-task loop is accepted at parse time");
    let looper = task_id_by_name(&definition, "looper");
    let graph = definition.task_graph.as_ref().expect("task graph");
    // Loop-aware seal: start → looper exists even though the self-loop makes
    // the looper look like it already has an incoming edge.
    assert!(
        graph
            .edges
            .iter()
            .any(|edge| edge.sources.contains(&graph.start) && edge.targets.contains(&looper)),
        "start → looper is sealed in so the loop is reachable"
    );
    // The exit edge reaches the reserved End node.
    assert!(
        graph
            .edges
            .iter()
            .any(|edge| edge.sources.contains(&looper) && edge.targets.contains(&graph.end)),
        "the `End`-targeted exit edge resolves to the workflow end node"
    );
}

#[tokio::test]
async fn loop_with_ungated_exit_is_rejected() {
    // The only edge leaving the loop is not gated on `loop_control` — the
    // loop-terminability validator rejects it at parse time.
    let json = MKLOOP_SHAPED_JSON.replace(
        r#"{
          "sources": ["looper"], "targets": ["End"], "kind": "data",
          "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "done" }
        }"#,
        r#"{ "sources": ["looper"], "targets": ["End"] }"#,
    );
    let err = Parser::parse_workflow_from_json(&json, "default")
        .await
        .expect_err("an ungated loop exit must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("non-terminable loop") || msg.contains("loop_control"),
        "rejection must name the terminability rule: {msg}"
    );
}

#[tokio::test]
async fn loop_with_no_explicit_exit_is_rejected() {
    // No author-declared exit edge: the loop-aware seal closes the looper to
    // End with a plain (ungated) control edge, which the terminability
    // validator rejects — a loop must exit via a `loop_control`-gated edge.
    let json = r#"{
      "slug": "no-exit-loop",
      "name": "no-exit-loop",
      "command": "AddWorkflow",
      "args": [],
      "outputs": [],
      "tasks": [
        {
          "name": "group",
          "command": "AddTaskGroup",
          "args": [],
          "outputs": [],
          "tasks": [
            {
              "name": "looper",
              "command": "echo",
              "args": [],
              "nix_expression_path": "x",
              "outputs": [],
              "routing_vars": [
                { "name": "loop_control", "kind": "routing-var", "type": "string" }
              ]
            }
          ],
          "edges": [
            {
              "sources": ["looper"], "targets": ["looper"], "kind": "loop",
              "gate": { "kind": "predicate-gate", "routing_var": "loop_control", "op": "Eq", "value": "continue" }
            }
          ]
        }
      ]
    }"#;
    let err = Parser::parse_workflow_from_json(json, "default")
        .await
        .expect_err("a loop with no loop_control-gated exit must be rejected");
    assert!(
        err.to_string().contains("non-terminable loop"),
        "rejection must name the terminability rule: {err}"
    );
}
