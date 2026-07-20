//! Parity corpus for the published archived-detail conversion.
//!
//! Two serialized union projections exercise different valid archive shapes.
//! Both must produce the same complete snapshot the API returns for detail reads.

use tickr_proto::codec::archive::{
    archive_projection_from_json, archived_instance_from_json, snapshot_from_archived,
};

const TERMINAL_UNION: &str =
    include_str!("../../conductor/tests/fixtures/terminal_union_instance.json");
const SECONDARY_UNION: &str =
    include_str!("../../conductor/tests/fixtures/secondary_union_instance.json");

fn assert_detail_corpus(blob: &str) {
    let stored: serde_json::Value = serde_json::from_str(blob).expect("union fixture parses");
    let union = archive_projection_from_json(stored.clone()).expect("union fixture decodes");
    let archived = archived_instance_from_json(stored).expect("archived detail renders");
    let snapshot = snapshot_from_archived(archived, "archived");
    let json = serde_json::to_value(&snapshot).expect("snapshot serializes");

    // The complete response contract stays present: identity/lifecycle,
    // task definitions and attempts, graph/gates, routing, and patch history.
    for field in [
        "id",
        "workflow_id",
        "name",
        "workflow_name",
        "workflow_version",
        "state",
        "scheduled_at",
        "triggered_at",
        "started_at",
        "completed_at",
        "transitions",
        "triggered_by",
        "tags",
        "storage",
        "task_count",
        "completed_tasks",
        "tasks",
        "task_instances",
        "graph",
        "routing_variables",
        "version",
        "applied_patches",
        "version_snapshots",
    ] {
        assert!(
            json.get(field).is_some(),
            "detail response retains `{field}`"
        );
    }
    assert_eq!(json["storage"], "archived");
    assert_eq!(json["id"], union.id);
    assert_eq!(json["workflow_id"], union.workflow_id);
    assert_eq!(json["workflow_name"], union.workflow_name);
    assert_eq!(json["workflow_version"], union.workflow_version);
    assert_eq!(json["state"], union.state);
    assert_eq!(
        json["transitions"],
        serde_json::to_value(&union.transitions).unwrap()
    );
    assert_eq!(json["tags"], serde_json::to_value(&union.tags).unwrap());
    assert_eq!(
        json["routing_variables"],
        serde_json::to_value(&union.routing_variables).unwrap()
    );
    assert_eq!(
        json["applied_patches"],
        serde_json::to_value(&union.applied_patches).unwrap()
    );
    assert_eq!(
        json["task_instances"],
        serde_json::to_value(&union.task_instances).unwrap()
    );
    assert_eq!(
        json["version_snapshots"].as_object().unwrap().len(),
        union.graph_snapshots.len()
    );

    // The embedded runnable section remains available for replay while also
    // furnishing the graph and task definitions consumed by detail rendering.
    let runnable = union.runnable.expect("archive union carries runnable data");
    assert_eq!(
        json["tasks"].as_array().unwrap().len(),
        runnable.tasks.len()
    );
    let graph = json["graph"].as_object().expect("detail graph is present");
    let runnable_graph = runnable.graph.expect("runnable graph is present");
    assert_eq!(graph["start"], runnable_graph.start);
    assert_eq!(graph["end"], runnable_graph.end);
    assert_eq!(
        graph["nodes"].as_array().unwrap().len(),
        runnable_graph.nodes.len()
    );
    assert_eq!(
        graph["edges"].as_array().unwrap().len(),
        runnable_graph.edges.len()
    );
}

#[test]
fn terminal_union_preserves_the_archived_detail_response_corpus() {
    assert_detail_corpus(TERMINAL_UNION);
}

#[test]
fn secondary_union_uses_the_same_archived_detail_conversion() {
    assert_detail_corpus(SECONDARY_UNION);
}
