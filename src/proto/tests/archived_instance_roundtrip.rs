//! Encode→decode round-trip proof for the archive-grade projection family.
//!
//! These tests verify that peers decode exactly the archived values sent over
//! the published wire contract and that non-contract fields remain absent.

use prost::Message;
use tickr_proto::instance as ip;

fn round_trip<T>(msg: &T) -> T
where
    T: Message + Default,
{
    let bytes = msg.encode_to_vec();
    T::decode(bytes.as_slice()).expect("a peer must decode what was encoded")
}

fn uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn sample_graph() -> ip::SnapshotGraph {
    ip::SnapshotGraph {
        start: uuid(),
        end: uuid(),
        nodes: vec![ip::SnapshotNode {
            code: "AB12".to_string(),
            id: uuid(),
            kind: "task".to_string(),
            ground: "success".to_string(),
            grounded_at: Some("2026-07-16T00:00:05Z".to_string()),
            ghost: false,
            pre_grounded: false,
        }],
        edges: vec![ip::SnapshotEdge {
            code: "CD34".to_string(),
            id: uuid(),
            sources: vec![uuid()],
            targets: vec![uuid()],
            kind: "control".to_string(),
            gates: vec![ip::GateView {
                kind: "signal".to_string(),
                state: "Satisfied".to_string(),
                signal_id: Some(uuid()),
                signal_name: Some("nightly-done".to_string()),
                predicate: None,
                captures: vec!["amount".to_string()],
                routing_var: None,
                op: None,
                value: None,
                timeout_secs: Some(3600),
                duration_secs: None,
                transitions: vec![ip::StateTransitionView {
                    from: "Idle".to_string(),
                    to: "Satisfied".to_string(),
                    at: "2026-07-16T00:00:01Z".to_string(),
                }],
            }],
        }],
    }
}

fn sample_archived_instance() -> ip::ArchivedInstance {
    ip::ArchivedInstance {
        id: uuid(),
        workflow_id: uuid(),
        name: "nightly-etl".to_string(),
        workflow_name: "etl".to_string(),
        workflow_version: 12,
        state: "Completed".to_string(),
        scheduled_at: Some("2026-07-16T00:00:00Z".to_string()),
        triggered_at: Some("2026-07-16T00:00:01Z".to_string()),
        started_at: Some("2026-07-16T00:00:02Z".to_string()),
        completed_at: Some("2026-07-16T00:00:30Z".to_string()),
        transitions: vec![ip::StateTransitionView {
            from: "InProgress".to_string(),
            to: "Completed".to_string(),
            at: "2026-07-16T00:00:30Z".to_string(),
        }],
        triggered_by: Some(ip::TriggerProvenanceView {
            kind: "Manual".to_string(),
            signal_id: Some(uuid()),
            name: None,
            source_instance: None,
            resume_from: vec![],
        }),
        tags: std::collections::HashMap::from([("team".to_string(), "data".to_string())]),
        task_count: 2,
        completed_tasks: 2,
        tasks: vec![ip::SnapshotTaskDef {
            id: uuid(),
            name: "extract".to_string(),
            task_type: "RegularTask".to_string(),
            max_attempts: 3,
            timeout_secs: Some(60),
            nix_expression_path: "/nix/extract".to_string(),
            inputs: vec![],
            outputs: vec![],
            secrets: vec![],
            routing_vars: vec![],
            emits: vec![],
        }],
        task_instances: vec![ip::SnapshotTaskInstance {
            id: uuid(),
            task_id: uuid(),
            name: "extract".to_string(),
            task_type: "RegularTask".to_string(),
            state: "Completed".to_string(),
            executor_id: Some(uuid()),
            attempt: 0,
            started_at: Some("2026-07-16T00:00:03Z".to_string()),
            completed_at: Some("2026-07-16T00:00:08Z".to_string()),
            cancel_reason: None,
            kill_confirmation: None,
            transitions: vec![],
        }],
        graph: Some(sample_graph()),
        routing_variables: std::collections::HashMap::from([(
            "coverage".to_string(),
            ip::RoutingValueView {
                kind: "int".to_string(),
                value: Some(ip::routing_value_view::Value::IntValue(85)),
            },
        )]),
        version: 1,
        applied_patches: vec![ip::AppliedPatchView {
            patch_key: uuid(),
            prior_version: 0,
            version: 1,
            reason: Some("dynamic fan-out".to_string()),
            provenance: "self".to_string(),
            applied_at: "2026-07-16T00:00:10Z".to_string(),
            ops: vec![ip::PatchOpView {
                op: "AddNode".to_string(),
                node_id: Some(uuid()),
                edge_id: None,
                sources: vec![],
                targets: vec![],
            }],
            // The composite-patch minted map — scope path → minted node id — the
            // lossless patch-provenance the union carries so the archive is a
            // full record the graph alone cannot reconstruct.
            minted_map: std::collections::HashMap::from([("root/fanout".to_string(), uuid())]),
        }],
        version_snapshots: std::collections::HashMap::from([(1, sample_graph())]),
    }
}

#[test]
fn archived_instance_round_trips_whole() {
    let original = sample_archived_instance();
    assert_eq!(original, round_trip(&original));
}

#[test]
fn archived_instance_row_round_trips() {
    let original = ip::ArchivedInstanceRow {
        id: uuid(),
        workflow_id: uuid(),
        workflow_version: 7,
        name: "run-1".to_string(),
        state: "Failed".to_string(),
        scheduled_at: Some("2026-07-16T00:00:00Z".to_string()),
        task_count: 3,
    };
    assert_eq!(original, round_trip(&original));
}

#[test]
fn archived_task_instance_round_trips() {
    let original = ip::ArchivedTaskInstance {
        id: uuid(),
        task_id: uuid(),
        workflow_instance_id: uuid(),
        workflow_id: uuid(),
        name: "extract".to_string(),
        task_type: "RegularTask".to_string(),
        state: "Completed".to_string(),
        executor_id: Some(uuid()),
        attempt: 1,
    };
    assert_eq!(original, round_trip(&original));
}
