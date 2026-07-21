//! In-memory subscription index that maps a wakeup `name` to every
//! workflow currently subscribed via `triggerOn = { kind =
//! "waits-on-signal"; }`. Populated by the per-task build pipeline's
//! finalizer when a workflow flips to `Ready` so external publishers
//! don't see ghost subscribers during a deploy window.
//!
//! The index is in-memory only; the selected definition repository is
//! authoritative. On startup the Conductor rebuilds the index from rows at
//! `status IN ('Ready', 'Submitted')`.
//!
//! Pure data structure: register / unregister / lookup. No I/O. The
//! predicate string is parsed at register time so hot-path eval reads
//! a precomputed `JsonPath` value cached on the entry.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json_path::JsonPath;
use tickr_proto::workflow::CaptureDeclaration;
use uuid::Uuid;

/// Failure modes returned by `register`. The predicate path is the only
/// fallible operation; everything else is data movement.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("predicate is not a valid JSONPath filter expression: {0}")]
    InvalidPredicate(String),
}

/// One subscription's worth of state. `predicate` is the parsed
/// `JsonPath` value (or `None` for "fire on every wakeup with this
/// name"). `merged_captures` is precomputed from
/// `mkWorkflow.captures` + `triggerOn.captures` per the merge rule.
#[derive(Debug, Clone)]
pub struct Entry {
    pub workflow_id: Uuid,
    pub predicate: Option<JsonPath>,
    pub merged_captures: Vec<CaptureDeclaration>,
}

/// Thread-safe wrapper over the in-memory `name → Vec<Entry>` map. The
/// translator clones one of these per process; readers (the wakeup
/// translator) and writers (the lifecycle listeners) share access via
/// an `RwLock` because reads dominate the access pattern.
#[derive(Clone, Default)]
pub struct SubscriptionIndex {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Default)]
struct Inner {
    by_name: HashMap<String, Vec<Entry>>,
    /// Reverse lookup so `unregister(workflow_id)` finds every name
    /// it's subscribed to without scanning the whole map.
    names_by_workflow: HashMap<Uuid, Vec<String>>,
}

impl SubscriptionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `workflow_id` as a subscriber to `name`. Re-registering
    /// the same `workflow_id` replaces the previous entry (idempotent
    /// against a workflow that changes its trigger config on
    /// re-registration). A malformed predicate returns
    /// `Err(InvalidPredicate)`; in that case the previous entry (if
    /// any) is left untouched.
    pub fn register(
        &self,
        workflow_id: Uuid,
        name: &str,
        predicate: Option<&str>,
        merged_captures: Vec<CaptureDeclaration>,
    ) -> Result<(), RegisterError> {
        let parsed = match predicate {
            Some(raw) => Some(
                raw.parse::<JsonPath>()
                    .map_err(|e| RegisterError::InvalidPredicate(e.to_string()))?,
            ),
            None => None,
        };
        let entry = Entry {
            workflow_id,
            predicate: parsed,
            merged_captures,
        };
        let mut guard = self.inner.write().expect("index lock poisoned");
        // Clear out any previous subscriptions this workflow held —
        // a re-register might change the signal name entirely.
        if let Some(prev_names) = guard.names_by_workflow.remove(&workflow_id) {
            for prev_name in prev_names {
                if let Some(bucket) = guard.by_name.get_mut(&prev_name) {
                    bucket.retain(|e| e.workflow_id != workflow_id);
                    if bucket.is_empty() {
                        guard.by_name.remove(&prev_name);
                    }
                }
            }
        }
        guard
            .by_name
            .entry(name.to_string())
            .or_default()
            .push(entry);
        guard
            .names_by_workflow
            .entry(workflow_id)
            .or_default()
            .push(name.to_string());
        Ok(())
    }

    /// Remove every subscription belonging to `workflow_id`. Idempotent
    /// no-op when the workflow has no entries (e.g. it never declared
    /// waits-on-signal, or it was already unregistered after a build
    /// failure).
    pub fn unregister(&self, workflow_id: Uuid) {
        let mut guard = self.inner.write().expect("index lock poisoned");
        if let Some(names) = guard.names_by_workflow.remove(&workflow_id) {
            for n in names {
                if let Some(bucket) = guard.by_name.get_mut(&n) {
                    bucket.retain(|e| e.workflow_id != workflow_id);
                    if bucket.is_empty() {
                        guard.by_name.remove(&n);
                    }
                }
            }
        }
    }

    /// Snapshot the entries subscribed to `name`. Returns an empty
    /// `Vec` for unknown names. Clones the entries so callers can hold
    /// them across awaits without holding the index's read lock.
    pub fn lookup(&self, name: &str) -> Vec<Entry> {
        let guard = self.inner.read().expect("index lock poisoned");
        guard.by_name.get(name).cloned().unwrap_or_default()
    }

    /// Total subscription count across all names. Used by tests and
    /// debug surfaces; not a hot-path call.
    pub fn len(&self) -> usize {
        let guard = self.inner.read().expect("index lock poisoned");
        guard.by_name.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickr_proto::workflow::{capture_source, CaptureSource};

    fn cap(name: &str, jsonpath: &str) -> CaptureDeclaration {
        CaptureDeclaration {
            name: name.to_string(),
            from: Some(CaptureSource {
                source: Some(capture_source::Source::Trigger(capture_source::Trigger {
                    jsonpath: jsonpath.to_string(),
                })),
            }),
        }
    }

    #[test]
    fn register_then_lookup_returns_the_entry() {
        let idx = SubscriptionIndex::new();
        let wid = Uuid::new_v4();
        idx.register(wid, "user-paid", None, vec![]).unwrap();
        let entries = idx.lookup("user-paid");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].workflow_id, wid);
        assert!(entries[0].predicate.is_none());
    }

    #[test]
    fn lookup_unknown_name_returns_empty() {
        let idx = SubscriptionIndex::new();
        assert!(idx.lookup("never-published").is_empty());
    }

    #[test]
    fn register_two_workflows_on_same_name_returns_both() {
        let idx = SubscriptionIndex::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        idx.register(a, "user-paid", None, vec![]).unwrap();
        idx.register(b, "user-paid", None, vec![]).unwrap();
        let entries = idx.lookup("user-paid");
        assert_eq!(entries.len(), 2);
        let ids: Vec<Uuid> = entries.iter().map(|e| e.workflow_id).collect();
        assert!(ids.contains(&a));
        assert!(ids.contains(&b));
    }

    #[test]
    fn unregister_removes_only_the_targeted_workflow() {
        let idx = SubscriptionIndex::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        idx.register(a, "user-paid", None, vec![]).unwrap();
        idx.register(b, "user-paid", None, vec![]).unwrap();
        idx.unregister(a);
        let entries = idx.lookup("user-paid");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].workflow_id, b);
    }

    #[test]
    fn unregister_unknown_workflow_is_a_noop() {
        let idx = SubscriptionIndex::new();
        idx.unregister(Uuid::new_v4()); // no panic
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn re_register_replaces_previous_entry() {
        let idx = SubscriptionIndex::new();
        let wid = Uuid::new_v4();
        idx.register(wid, "user-paid", None, vec![cap("a", "$.a")])
            .unwrap();
        // Re-register with a different capture set on the SAME name.
        idx.register(wid, "user-paid", None, vec![cap("b", "$.b")])
            .unwrap();
        let entries = idx.lookup("user-paid");
        assert_eq!(
            entries.len(),
            1,
            "old entry must be replaced, not duplicated"
        );
        assert_eq!(entries[0].merged_captures.len(), 1);
        assert_eq!(entries[0].merged_captures[0].name, "b");
    }

    #[test]
    fn re_register_changes_signal_name_cleanly() {
        let idx = SubscriptionIndex::new();
        let wid = Uuid::new_v4();
        idx.register(wid, "user-paid", None, vec![]).unwrap();
        // Author renames the signal — re-register under a new name.
        idx.register(wid, "order-completed", None, vec![]).unwrap();
        assert!(idx.lookup("user-paid").is_empty());
        assert_eq!(idx.lookup("order-completed").len(), 1);
    }

    #[test]
    fn predicate_parse_error_leaves_index_untouched() {
        let idx = SubscriptionIndex::new();
        let wid = Uuid::new_v4();
        let result = idx.register(wid, "user-paid", Some("$[?(garbage]"), vec![]);
        assert!(matches!(result, Err(RegisterError::InvalidPredicate(_))));
        assert!(idx.lookup("user-paid").is_empty());
    }

    #[test]
    fn valid_predicate_lands_on_entry() {
        let idx = SubscriptionIndex::new();
        let wid = Uuid::new_v4();
        idx.register(wid, "user-paid", Some("$[?@.amount > 100]"), vec![])
            .unwrap();
        let entries = idx.lookup("user-paid");
        assert!(entries[0].predicate.is_some());
    }

    #[test]
    fn entry_carries_merged_captures() {
        let idx = SubscriptionIndex::new();
        let wid = Uuid::new_v4();
        idx.register(
            wid,
            "user-paid",
            None,
            vec![cap("email", "$.user.email"), cap("amount", "$.amount")],
        )
        .unwrap();
        let entries = idx.lookup("user-paid");
        let names: Vec<&str> = entries[0]
            .merged_captures
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["email", "amount"]);
    }
}
