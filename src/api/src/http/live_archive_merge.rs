//! Pure merge logic for the UI's live + archive composite views.
//!
//! One rule, three shapes (workflow instances, task instances, dashboard
//! counts): the *archive store is authoritative for terminal rows*; the
//! *live store is authoritative for non-terminal rows*. On id collision the
//! archive row wins — the collision window is the few ms between the
//! conductor's PG insert and the server's live-row retirement during
//! compaction, and the archive's terminal projection is canonical there.
//!
//! Implementation is dedup-by-id with the archive iterated last so its
//! entries overwrite any same-id live entries. The "terminal-from-archive,
//! non-terminal-from-live" partition is then a property of the result
//! (terminal rows arrive only via archive; live rows pre-filter themselves
//! to non-terminal in practice but the merge is correct either way).
//!
//! No I/O — these functions take already-fetched vectors and return a single
//! merged vector. Trivially unit-testable.

use super::dto::{ClockInstance, TaskInstanceResponse, WorkflowInstanceResponse};
use std::collections::HashMap;

/// Merge live + archive workflow-instance projections by id. Archive wins on
/// id collision. Output order is not stable across calls (it follows hash-map
/// iteration); the caller sorts if a stable order is needed.
pub fn merge_instances(
    live: Vec<WorkflowInstanceResponse>,
    archive: Vec<WorkflowInstanceResponse>,
) -> Vec<WorkflowInstanceResponse> {
    let mut by_id: HashMap<String, WorkflowInstanceResponse> = HashMap::new();
    for inst in live {
        by_id.insert(inst.id.clone(), inst);
    }
    for inst in archive {
        by_id.insert(inst.id.clone(), inst);
    }
    by_id.into_values().collect()
}

/// Merge live + archive task-instance projections by id. Same rule as
/// `merge_instances`: archive wins on collision, dedup-by-id.
pub fn merge_tasks(
    live: Vec<TaskInstanceResponse>,
    archive: Vec<TaskInstanceResponse>,
) -> Vec<TaskInstanceResponse> {
    let mut by_id: HashMap<String, TaskInstanceResponse> = HashMap::new();
    for t in live {
        by_id.insert(t.id.clone(), t);
    }
    for t in archive {
        by_id.insert(t.id.clone(), t);
    }
    by_id.into_values().collect()
}

/// Merge live + archive day-clock instances by id. Archive wins on collision
/// (the compaction-window race), same rule as `merge_instances`. The caller
/// passes `live_data_available` through onto the response.
pub fn merge_clock_instances(
    live: Vec<ClockInstance>,
    archive: Vec<ClockInstance>,
) -> Vec<ClockInstance> {
    let mut by_id: HashMap<String, ClockInstance> = HashMap::new();
    for inst in live {
        by_id.insert(inst.id.clone(), inst);
    }
    for inst in archive {
        by_id.insert(inst.id.clone(), inst);
    }
    by_id.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(id: &str, state: &str) -> WorkflowInstanceResponse {
        WorkflowInstanceResponse {
            id: id.to_string(),
            workflow_id: "00000000-0000-0000-0000-000000000000".to_string(),
            workflow_version: 0,
            name: String::new(),
            state: state.to_string(),
            scheduled_at: None,
            task_count: 0,
            completed_tasks: 0,
        }
    }

    #[test]
    fn both_empty_returns_empty() {
        let out = merge_instances(vec![], vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn archive_only_returns_archive() {
        let archive = vec![instance("a", "Completed"), instance("b", "Failed")];
        let out = merge_instances(vec![], archive.clone());
        assert_eq!(out.len(), 2);
        let ids: std::collections::HashSet<_> = out.iter().map(|i| i.id.clone()).collect();
        assert!(ids.contains("a"));
        assert!(ids.contains("b"));
    }

    #[test]
    fn live_only_returns_live() {
        let live = vec![instance("x", "Running"), instance("y", "Scheduled")];
        let out = merge_instances(live.clone(), vec![]);
        assert_eq!(out.len(), 2);
        let ids: std::collections::HashSet<_> = out.iter().map(|i| i.id.clone()).collect();
        assert!(ids.contains("x"));
        assert!(ids.contains("y"));
    }

    #[test]
    fn disjoint_sets_concat() {
        let live = vec![instance("x", "Running")];
        let archive = vec![instance("a", "Completed")];
        let out = merge_instances(live, archive);
        assert_eq!(out.len(), 2);
        let ids: std::collections::HashSet<_> = out.iter().map(|i| i.id.clone()).collect();
        assert!(ids.contains("x"));
        assert!(ids.contains("a"));
    }

    #[test]
    fn id_collision_resolves_to_archive_row() {
        // Same id in both halves. The live row claims `Running`, the archive
        // row claims `Completed`. This models the brief window between PG
        // insert and live-row retirement during compaction — archive should win.
        let live = vec![instance("z", "Running")];
        let archive = vec![instance("z", "Completed")];
        let out = merge_instances(live, archive);
        assert_eq!(out.len(), 1, "dedup must collapse to a single row");
        assert_eq!(out[0].state, "Completed", "archive must win on collision");
    }

    #[test]
    fn many_collisions_all_resolve_to_archive() {
        let live = (0..50)
            .map(|i| instance(&format!("id-{}", i), "Running"))
            .collect::<Vec<_>>();
        let archive = (0..50)
            .map(|i| instance(&format!("id-{}", i), "Completed"))
            .collect::<Vec<_>>();
        let out = merge_instances(live, archive);
        assert_eq!(out.len(), 50);
        assert!(out.iter().all(|i| i.state == "Completed"));
    }

    fn task(id: &str, state: &str) -> TaskInstanceResponse {
        TaskInstanceResponse {
            id: id.to_string(),
            task_id: "00000000-0000-0000-0000-000000000000".to_string(),
            workflow_instance_id: "00000000-0000-0000-0000-000000000000".to_string(),
            workflow_id: "00000000-0000-0000-0000-000000000000".to_string(),
            name: "test".to_string(),
            task_type: "RegularTask".to_string(),
            state: state.to_string(),
            executor_id: None,
            attempt: 0,
        }
    }

    #[test]
    fn merge_tasks_dedup_with_archive_priority() {
        // live has tasks "a" (Running) and "b" (Failed); archive has "b"
        // (Failed, attempt 2) and "c" (Completed). Result: 3 rows, "b"
        // sourced from archive (attempt reflects the archived value).
        let live = vec![task("a", "Running"), task("b", "Failed")];
        let mut b_arch = task("b", "Failed");
        b_arch.attempt = 2;
        let archive = vec![b_arch, task("c", "Completed")];

        let out = merge_tasks(live, archive);
        let by_id: HashMap<&str, &TaskInstanceResponse> =
            out.iter().map(|t| (t.id.as_str(), t)).collect();

        assert_eq!(out.len(), 3);
        assert_eq!(by_id.get("a").unwrap().state, "Running");
        let b = by_id.get("b").unwrap();
        assert_eq!(b.attempt, 2, "archive wins on b — attempt from archive");
        assert_eq!(by_id.get("c").unwrap().state, "Completed");
    }

    #[test]
    fn merge_tasks_both_empty() {
        assert!(merge_tasks(vec![], vec![]).is_empty());
    }

    fn clock(id: &str, state: &str) -> ClockInstance {
        ClockInstance {
            id: id.to_string(),
            workflow_id: "00000000-0000-0000-0000-000000000000".to_string(),
            workflow_name: "wf".to_string(),
            scheduled_at: None,
            state: state.to_string(),
        }
    }

    #[test]
    fn merge_clock_dedups_disjoint_and_collision() {
        let live = vec![clock("x", "InProgress"), clock("z", "Scheduled")];
        let archive = vec![clock("z", "Completed"), clock("a", "Failed")];
        let out = merge_clock_instances(live, archive);
        let by_id: HashMap<&str, &ClockInstance> = out.iter().map(|c| (c.id.as_str(), c)).collect();
        assert_eq!(out.len(), 3, "x, z, a — z deduped");
        assert_eq!(by_id.get("x").unwrap().state, "InProgress");
        assert_eq!(
            by_id.get("z").unwrap().state,
            "Completed",
            "archive wins on the compaction-window collision"
        );
        assert_eq!(by_id.get("a").unwrap().state, "Failed");
    }

    #[test]
    fn merge_clock_both_empty() {
        assert!(merge_clock_instances(vec![], vec![]).is_empty());
    }
}
