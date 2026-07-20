//! Latest-run-state resolver (DC-0014).
//!
//! For each workflow, "latest run" is the state of its newest *fired* instance.
//! The universe is fired instances only — `{Triggered, InProgress, Completed,
//! Failed}`; future-armed `{PendingSchedule, Scheduled}` are excluded (those
//! belong on the "Up next" surface, not on "latest run").
//!
//! The value is composed from two sources, exactly as `/api/dashboard/clock`
//! and `/api/workflows/{id}/instances` do: the conductor's PG archive (latest
//! terminal instance per workflow) and a single live cluster subquery against
//! the coordinator (`GET /api/workflows/instances`, every live instance). The
//! pure [`resolve`] function does the merge; the orchestration wrapper does the
//! two reads. Splitting the two keeps the merge unit-testable with plain
//! vectors — no mock PG or HTTP needed.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::coordinator_client::CoordinatorClient;
use super::dto::WorkflowInstanceResponse;

/// One candidate fired instance, normalised from either the archive or the live
/// read. `state` is the Rust `Debug` form of `WorkflowState` (the shape the
/// wire already uses everywhere), `scheduled_at` is the recency key.
#[derive(Debug, Clone)]
pub struct RunCandidate {
    pub workflow_id: Uuid,
    pub instance_id: String,
    pub state: String,
    pub scheduled_at: Option<DateTime<Utc>>,
}

/// Future-armed states are pre-fired — excluded from the "latest run" universe.
fn is_future_armed(state: &str) -> bool {
    matches!(state, "PendingSchedule" | "Scheduled")
}

/// Is `a` at least as recent as `b`? Recency key is `scheduled_at`; a present
/// timestamp beats an absent one, and ties keep the incumbent.
fn at_least_as_recent(a: &RunCandidate, b: &RunCandidate) -> bool {
    match (a.scheduled_at, b.scheduled_at) {
        (Some(x), Some(y)) => x >= y,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => false,
    }
}

/// Pure merge: pick each workflow's latest fired-instance candidate.
///
/// Rules:
/// - Dedup by `instance_id` with **archive winning** on collision (the
///   compaction-window race, same rule as the live/archive instance merge).
/// - Drop future-armed states from the universe.
/// - Per workflow, pick the most-recent candidate by `scheduled_at`.
/// - Every requested id is present in the output; `None` means the workflow has
///   no fired instance (never ran, or only future-armed instances exist).
///
/// Returns the whole candidate (state + `scheduled_at`) so a caller can read
/// both the latest run *state* and the latest run *timestamp* off the one pick —
/// the two can never name different instances.
pub fn resolve_candidates(
    workflow_ids: &[Uuid],
    archive: Vec<RunCandidate>,
    live: Vec<RunCandidate>,
) -> HashMap<Uuid, Option<RunCandidate>> {
    let requested: HashSet<Uuid> = workflow_ids.iter().copied().collect();
    let mut out: HashMap<Uuid, Option<RunCandidate>> =
        workflow_ids.iter().map(|id| (*id, None)).collect();

    // Dedup by instance id, archive wins (inserted last so it overwrites).
    let mut by_instance: HashMap<String, RunCandidate> = HashMap::new();
    for c in live {
        by_instance.insert(c.instance_id.clone(), c);
    }
    for c in archive {
        by_instance.insert(c.instance_id.clone(), c);
    }

    // Reduce to the most-recent fired candidate per requested workflow.
    let mut best: HashMap<Uuid, RunCandidate> = HashMap::new();
    for (_id, c) in by_instance {
        if !requested.contains(&c.workflow_id) || is_future_armed(&c.state) {
            continue;
        }
        match best.get(&c.workflow_id) {
            Some(existing) if !at_least_as_recent(&c, existing) => {}
            _ => {
                best.insert(c.workflow_id, c);
            }
        }
    }

    for (wid, c) in best {
        out.insert(wid, Some(c));
    }
    out
}

/// State-only view of [`resolve_candidates`] — the latest fired-instance state
/// per workflow (`None` if never fired). The workflows-list handler reads only
/// the state.
pub fn resolve(
    workflow_ids: &[Uuid],
    archive: Vec<RunCandidate>,
    live: Vec<RunCandidate>,
) -> HashMap<Uuid, Option<String>> {
    resolve_candidates(workflow_ids, archive, live)
        .into_iter()
        .map(|(id, c)| (id, c.map(|c| c.state)))
        .collect()
}

/// Read the latest terminal instance per workflow from the conductor's PG
/// archive. The archive holds only terminal rows (`Completed`/`Failed`) by
/// construction, so "latest terminal per workflow" is just the most-recent row
/// per `workflow_id`. Columns (`workflow_id`, `state`, `scheduled_at`,
/// `archived_at`) are indexed — no JSONB extraction.
async fn fetch_archive_candidates(pool: &PgPool) -> Result<Vec<RunCandidate>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (workflow_id) workflow_id, id, state, scheduled_at
        FROM workflow_instances
        ORDER BY workflow_id, scheduled_at DESC NULLS LAST, archived_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| RunCandidate {
            workflow_id: row.get("workflow_id"),
            instance_id: row.get::<Uuid, _>("id").to_string(),
            state: row.get("state"),
            scheduled_at: row.get("scheduled_at"),
        })
        .collect())
}

/// Convert a live coordinator instance projection into a [`RunCandidate`].
fn candidate_from_live(inst: WorkflowInstanceResponse) -> Option<RunCandidate> {
    let workflow_id = Uuid::parse_str(&inst.workflow_id).ok()?;
    let scheduled_at = inst
        .scheduled_at
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));
    Some(RunCandidate {
        workflow_id,
        instance_id: inst.id,
        state: inst.state,
        scheduled_at,
    })
}

/// Resolve the latest fired-instance *candidate* for a batch of workflows in one
/// PG query plus one live cluster subquery. The live read is best-effort: if the
/// coordinator is unreachable the resolver degrades to archive-only (terminal runs
/// still show; in-flight runs are momentarily invisible) rather than failing.
/// Callers that need both the latest state and its timestamp read them off the
/// one returned candidate.
pub async fn resolve_latest_runs(
    pool: &PgPool,
    coordinator: &CoordinatorClient,
    workflow_ids: &[Uuid],
) -> HashMap<Uuid, Option<RunCandidate>> {
    let archive = fetch_archive_candidates(pool).await.unwrap_or_else(|e| {
        eprintln!("latest_run_resolver: archive query failed: {e}");
        Vec::new()
    });

    let live = match coordinator.list_all_workflow_instances().await {
        Ok(instances) => instances
            .into_iter()
            .filter_map(candidate_from_live)
            .collect(),
        Err(e) => {
            eprintln!("latest_run_resolver: live read failed, archive-only: {e}");
            Vec::new()
        }
    };

    resolve_candidates(workflow_ids, archive, live)
}

/// State-only view of [`resolve_latest_runs`] for the workflows-list handler,
/// which needs the latest run state but not its timestamp.
pub async fn resolve_latest_run_states(
    pool: &PgPool,
    coordinator: &CoordinatorClient,
    workflow_ids: &[Uuid],
) -> HashMap<Uuid, Option<String>> {
    resolve_latest_runs(pool, coordinator, workflow_ids)
        .await
        .into_iter()
        .map(|(id, c)| (id, c.map(|c| c.state)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp(secs, 0)
    }

    fn cand(wid: Uuid, iid: &str, state: &str, secs: Option<i64>) -> RunCandidate {
        RunCandidate {
            workflow_id: wid,
            instance_id: iid.to_string(),
            state: state.to_string(),
            scheduled_at: secs.and_then(at),
        }
    }

    #[test]
    fn one_terminal_only_returns_its_state() {
        let w = Uuid::new_v4();
        let out = resolve(&[w], vec![cand(w, "a", "Completed", Some(10))], vec![]);
        assert_eq!(out[&w], Some("Completed".to_string()));
    }

    #[test]
    fn one_live_non_terminal_only_returns_its_state() {
        let w = Uuid::new_v4();
        let out = resolve(&[w], vec![], vec![cand(w, "a", "InProgress", Some(10))]);
        assert_eq!(out[&w], Some("InProgress".to_string()));
    }

    #[test]
    fn live_newer_than_terminal_returns_live() {
        let w = Uuid::new_v4();
        let out = resolve(
            &[w],
            vec![cand(w, "old", "Failed", Some(10))],
            vec![cand(w, "new", "InProgress", Some(20))],
        );
        assert_eq!(out[&w], Some("InProgress".to_string()));
    }

    #[test]
    fn terminal_newer_than_live_returns_terminal() {
        let w = Uuid::new_v4();
        let out = resolve(
            &[w],
            vec![cand(w, "new", "Completed", Some(30))],
            vec![cand(w, "old", "InProgress", Some(20))],
        );
        assert_eq!(out[&w], Some("Completed".to_string()));
    }

    #[test]
    fn id_collision_archive_wins() {
        // Same instance id in both halves (mid-compaction): the archive's
        // terminal projection is canonical.
        let w = Uuid::new_v4();
        let out = resolve(
            &[w],
            vec![cand(w, "same", "Completed", Some(20))],
            vec![cand(w, "same", "InProgress", Some(20))],
        );
        assert_eq!(out[&w], Some("Completed".to_string()));
    }

    #[test]
    fn only_future_armed_returns_none() {
        let w = Uuid::new_v4();
        let out = resolve(
            &[w],
            vec![],
            vec![
                cand(w, "a", "Scheduled", Some(10)),
                cand(w, "b", "PendingSchedule", Some(20)),
            ],
        );
        assert_eq!(out[&w], None);
    }

    #[test]
    fn no_instance_ever_returns_none() {
        let w = Uuid::new_v4();
        let out = resolve(&[w], vec![], vec![]);
        assert_eq!(out[&w], None);
    }

    #[test]
    fn mixed_batch_resolves_each_workflow_independently() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let out = resolve(
            &[a, b, c],
            vec![cand(a, "a1", "Completed", Some(10))],
            vec![
                cand(b, "b1", "InProgress", Some(5)),
                // c has only a future-armed instance.
                cand(c, "c1", "Scheduled", Some(7)),
            ],
        );
        assert_eq!(out[&a], Some("Completed".to_string()));
        assert_eq!(out[&b], Some("InProgress".to_string()));
        assert_eq!(out[&c], None);
        assert_eq!(out.len(), 3, "every requested id present in the result");
    }
}
