//! In-memory per-instance gate index. Maps a wakeup `name` to every
//! gate currently dispatched (i.e., every gate whose source set is
//! `Grounded(Success)` on the server side) so the wakeup translator
//! can look up matching gates in O(1) and fire `GateOutcome`
//! envelopes back to the server.
//!
//! The index is in-memory only. The authoritative source is the
//! published live-state snapshot; on every relay reconnect the
//! conductor calls `GET_DISPATCHED_GATES` and re-populates the index
//! from scratch.
//!
//! Pure data structure: register / unregister / lookup / sweep. No
//! I/O. The predicate string is parsed at `register` time so the
//! hot-path evaluation reads a precomputed `JsonPath`.
//!
//! Identity is `(workflow_instance_id, edge_id)` — two instances of
//! the same workflow each get their own gate dispatch and their own
//! `GateOutcome` emission. The reverse index `by_instance` lets
//! `sweep_instance` drop every gate belonging to an archived
//! workflow_instance in one pass.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json_path::JsonPath;
use tickr_proto::workflow::CaptureDeclaration;
use uuid::Uuid;

/// Failure modes returned by `register`. The predicate path is the
/// only fallible operation; everything else is data movement.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("predicate is not a valid JSONPath filter expression: {0}")]
    InvalidPredicate(String),
}

/// One dispatched gate's worth of state. `predicate` is the parsed
/// `JsonPath` (or `None` for "fire on every wakeup with this name").
/// `captures_spec` is the author-declared capture set carried from
/// the workflow's `mkSignalGate { captures = ...; }`.
#[derive(Debug, Clone)]
pub struct Entry {
    pub workflow_instance_id: Uuid,
    pub edge_id: Uuid,
    pub signal_name: String,
    pub predicate: Option<JsonPath>,
    pub captures_spec: Vec<CaptureDeclaration>,
}

/// Thread-safe wrapper over the in-memory indices. The wakeup
/// translator clones one of these per process; readers (translator)
/// and writers (inbound relay handler, compaction sweep,
/// restart-rebuild) share access via an `RwLock` because reads
/// dominate the access pattern.
#[derive(Clone, Default)]
pub struct GateIndex {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Default)]
struct Inner {
    /// Primary store keyed by gate identity. Owns the entry.
    by_gate: HashMap<(Uuid, Uuid), Entry>,
    /// Reverse map for `lookup_by_signal_name` — used per wakeup on
    /// the hot path. Each `Vec` holds the gates currently dispatched
    /// for that signal name; entries are pulled when a gate
    /// satisfies (or when compaction sweeps the workflow instance).
    by_name: HashMap<String, Vec<(Uuid, Uuid)>>,
    /// Reverse map for `sweep_instance`. Each `Vec` holds every
    /// edge_id this workflow instance has currently dispatched.
    by_instance: HashMap<Uuid, Vec<Uuid>>,
}

impl GateIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a dispatched gate. Re-registering the same
    /// `(workflow_instance_id, edge_id)` replaces the previous entry
    /// (idempotent against the restart-rebuild flow that re-runs
    /// `register` for every gate the server still considers
    /// dispatched). A malformed predicate returns
    /// `Err(InvalidPredicate)`; in that case neither the previous
    /// entry nor the reverse indices are modified.
    pub fn register(
        &self,
        workflow_instance_id: Uuid,
        edge_id: Uuid,
        signal_name: &str,
        predicate: Option<&str>,
        captures_spec: Vec<CaptureDeclaration>,
    ) -> Result<(), RegisterError> {
        let parsed = match predicate {
            Some(raw) => Some(
                raw.parse::<JsonPath>()
                    .map_err(|e| RegisterError::InvalidPredicate(e.to_string()))?,
            ),
            None => None,
        };
        let entry = Entry {
            workflow_instance_id,
            edge_id,
            signal_name: signal_name.to_string(),
            predicate: parsed,
            captures_spec,
        };
        let key = (workflow_instance_id, edge_id);
        let mut guard = self.inner.write().expect("gate index lock poisoned");
        // Clear any previous registration for this gate so the
        // reverse maps don't accumulate duplicates on re-registration.
        if let Some(prev) = guard.by_gate.remove(&key) {
            if let Some(bucket) = guard.by_name.get_mut(&prev.signal_name) {
                bucket.retain(|k| k != &key);
                if bucket.is_empty() {
                    guard.by_name.remove(&prev.signal_name);
                }
            }
            if let Some(edges) = guard.by_instance.get_mut(&workflow_instance_id) {
                edges.retain(|e| e != &edge_id);
                if edges.is_empty() {
                    guard.by_instance.remove(&workflow_instance_id);
                }
            }
        }
        guard
            .by_name
            .entry(signal_name.to_string())
            .or_default()
            .push(key);
        guard
            .by_instance
            .entry(workflow_instance_id)
            .or_default()
            .push(edge_id);
        guard.by_gate.insert(key, entry);
        Ok(())
    }

    /// Remove a single gate. Idempotent no-op when the gate isn't
    /// present (e.g., the satisfaction emit raced a compaction
    /// sweep).
    pub fn unregister(&self, workflow_instance_id: Uuid, edge_id: Uuid) {
        let key = (workflow_instance_id, edge_id);
        let mut guard = self.inner.write().expect("gate index lock poisoned");
        if let Some(prev) = guard.by_gate.remove(&key) {
            if let Some(bucket) = guard.by_name.get_mut(&prev.signal_name) {
                bucket.retain(|k| k != &key);
                if bucket.is_empty() {
                    guard.by_name.remove(&prev.signal_name);
                }
            }
            if let Some(edges) = guard.by_instance.get_mut(&workflow_instance_id) {
                edges.retain(|e| e != &edge_id);
                if edges.is_empty() {
                    guard.by_instance.remove(&workflow_instance_id);
                }
            }
        }
    }

    /// Snapshot every gate currently dispatched for `signal_name`.
    /// Returns an empty `Vec` for unknown names. Clones the entries
    /// so the caller can hold them across awaits without holding the
    /// index lock.
    pub fn lookup_by_signal_name(&self, signal_name: &str) -> Vec<Entry> {
        let guard = self.inner.read().expect("gate index lock poisoned");
        let Some(keys) = guard.by_name.get(signal_name) else {
            return Vec::new();
        };
        keys.iter()
            .filter_map(|k| guard.by_gate.get(k).cloned())
            .collect()
    }

    /// Drop every gate belonging to `workflow_instance_id` — for an
    /// archived instance the dispatched gates can't satisfy ever
    /// again. No production caller today: with any conductor able to
    /// drain a staged compaction job, cleanup-on-archive would need a
    /// cross-conductor broadcast, so index freshness relies on the
    /// server-authoritative rebuild at relay reconnect instead.
    pub fn sweep_instance(&self, workflow_instance_id: Uuid) {
        let mut guard = self.inner.write().expect("gate index lock poisoned");
        let Some(edges) = guard.by_instance.remove(&workflow_instance_id) else {
            return;
        };
        for edge_id in edges {
            let key = (workflow_instance_id, edge_id);
            if let Some(prev) = guard.by_gate.remove(&key) {
                if let Some(bucket) = guard.by_name.get_mut(&prev.signal_name) {
                    bucket.retain(|k| k != &key);
                    if bucket.is_empty() {
                        guard.by_name.remove(&prev.signal_name);
                    }
                }
            }
        }
    }

    /// Replace every entry in one shot. Used by the restart-rebuild
    /// path: clear in-memory state, ingest the cluster_query result,
    /// repopulate via this single call so concurrent readers don't
    /// observe a partial index.
    pub fn replace_all(
        &self,
        entries: Vec<(Uuid, Uuid, String, Option<String>, Vec<CaptureDeclaration>)>,
    ) {
        let mut next = Inner::default();
        for (wid, eid, signal_name, predicate, captures_spec) in entries {
            let parsed = match predicate {
                Some(raw) => match raw.parse::<JsonPath>() {
                    Ok(p) => Some(p),
                    Err(e) => {
                        tracing::warn!(
                            target: "gate_index",
                            workflow_instance_id = %wid,
                            edge_id = %eid,
                            error = %e,
                            "skipping gate with invalid predicate during restart-rebuild"
                        );
                        continue;
                    }
                },
                None => None,
            };
            let key = (wid, eid);
            let entry = Entry {
                workflow_instance_id: wid,
                edge_id: eid,
                signal_name: signal_name.clone(),
                predicate: parsed,
                captures_spec,
            };
            next.by_name.entry(signal_name).or_default().push(key);
            next.by_instance.entry(wid).or_default().push(eid);
            next.by_gate.insert(key, entry);
        }
        let mut guard = self.inner.write().expect("gate index lock poisoned");
        *guard = next;
    }

    /// Total gate count across all signal names. Used by tests and
    /// debug surfaces; not a hot-path call.
    pub fn len(&self) -> usize {
        let guard = self.inner.read().expect("gate index lock poisoned");
        guard.by_gate.len()
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
        let idx = GateIndex::new();
        let wid = Uuid::new_v4();
        let eid = Uuid::new_v4();
        idx.register(wid, eid, "paid", None, vec![]).unwrap();
        let entries = idx.lookup_by_signal_name("paid");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].workflow_instance_id, wid);
        assert_eq!(entries[0].edge_id, eid);
        assert!(entries[0].predicate.is_none());
    }

    #[test]
    fn lookup_unknown_name_returns_empty() {
        let idx = GateIndex::new();
        assert!(idx.lookup_by_signal_name("never-published").is_empty());
    }

    #[test]
    fn double_register_same_gate_replaces_previous_entry() {
        let idx = GateIndex::new();
        let wid = Uuid::new_v4();
        let eid = Uuid::new_v4();
        idx.register(wid, eid, "paid", None, vec![cap("a", "$.a")])
            .unwrap();
        idx.register(wid, eid, "paid", None, vec![cap("b", "$.b")])
            .unwrap();
        let entries = idx.lookup_by_signal_name("paid");
        assert_eq!(
            entries.len(),
            1,
            "old entry must be replaced, not duplicated"
        );
        assert_eq!(entries[0].captures_spec[0].name, "b");
    }

    #[test]
    fn unregister_drops_only_the_targeted_gate() {
        let idx = GateIndex::new();
        let wid = Uuid::new_v4();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        idx.register(wid, a, "paid", None, vec![]).unwrap();
        idx.register(wid, b, "paid", None, vec![]).unwrap();
        idx.unregister(wid, a);
        let entries = idx.lookup_by_signal_name("paid");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].edge_id, b);
    }

    /// Verified & recorded (branch / gated-arm re-arm window): the gate index is
    /// a live registry with **no parking buffer** for a name that is momentarily
    /// unregistered. A non-terminal `SignalReceived` re-seat (a re-arm moving the
    /// gate from edge A to edge B) crosses the conductor as two envelopes —
    /// unregister(A) then register(B). Between them the name has zero registered
    /// gates, so a one-shot wakeup arriving in that window matches neither edge
    /// and — with no replay in tickr — is dropped, not parked. The fix is
    /// envelope ordering: register(B) must be applied before unregister(A) (or the
    /// pair made atomic) so the name is never momentarily empty. This is surfaced
    /// for the design-review gate; the test locks the observed drop-not-park
    /// behavior so the window can't silently close or widen unnoticed.
    #[test]
    fn reseat_window_leaves_the_name_unmatched_no_parking() {
        let idx = GateIndex::new();
        let wid = Uuid::new_v4();
        let edge_a = Uuid::new_v4();
        let edge_b = Uuid::new_v4();
        // Gate armed on edge A.
        idx.register(wid, edge_a, "approval", None, vec![]).unwrap();
        assert_eq!(idx.lookup_by_signal_name("approval").len(), 1);
        // Re-seat as two envelopes: unregister(A) first…
        idx.unregister(wid, edge_a);
        // …in the window between the two envelopes, the name is unmatched: a
        // wakeup here would match no gate and be dropped (no buffer holds it).
        assert!(
            idx.lookup_by_signal_name("approval").is_empty(),
            "the re-seat window leaves the name with zero registered gates — an \
             unmatched wakeup is dropped, not parked"
        );
        // …then register(B). Only after this does the name match again.
        idx.register(wid, edge_b, "approval", None, vec![]).unwrap();
        let entries = idx.lookup_by_signal_name("approval");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].edge_id, edge_b);
    }

    #[test]
    fn sweep_instance_drops_every_gate_for_that_instance() {
        let idx = GateIndex::new();
        let live = Uuid::new_v4();
        let other = Uuid::new_v4();
        idx.register(live, Uuid::new_v4(), "paid", None, vec![])
            .unwrap();
        idx.register(live, Uuid::new_v4(), "shipped", None, vec![])
            .unwrap();
        idx.register(other, Uuid::new_v4(), "paid", None, vec![])
            .unwrap();
        idx.sweep_instance(live);
        assert!(idx.lookup_by_signal_name("shipped").is_empty());
        let remaining = idx.lookup_by_signal_name("paid");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].workflow_instance_id, other);
    }

    #[test]
    fn sweep_unknown_instance_is_a_noop() {
        let idx = GateIndex::new();
        idx.sweep_instance(Uuid::new_v4());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn predicate_parse_error_leaves_index_untouched() {
        let idx = GateIndex::new();
        let wid = Uuid::new_v4();
        let eid = Uuid::new_v4();
        let result = idx.register(wid, eid, "paid", Some("$[?(garbage]"), vec![]);
        assert!(matches!(result, Err(RegisterError::InvalidPredicate(_))));
        assert!(idx.lookup_by_signal_name("paid").is_empty());
    }

    #[test]
    fn valid_predicate_lands_on_entry() {
        let idx = GateIndex::new();
        let wid = Uuid::new_v4();
        let eid = Uuid::new_v4();
        idx.register(wid, eid, "paid", Some("$[?@.amount > 100]"), vec![])
            .unwrap();
        let entries = idx.lookup_by_signal_name("paid");
        assert!(entries[0].predicate.is_some());
    }

    #[test]
    fn replace_all_swaps_state_atomically() {
        let idx = GateIndex::new();
        let stale_wid = Uuid::new_v4();
        idx.register(stale_wid, Uuid::new_v4(), "stale", None, vec![])
            .unwrap();
        let fresh_wid = Uuid::new_v4();
        let fresh_eid = Uuid::new_v4();
        idx.replace_all(vec![(
            fresh_wid,
            fresh_eid,
            "fresh".to_string(),
            None,
            vec![],
        )]);
        assert!(idx.lookup_by_signal_name("stale").is_empty());
        let entries = idx.lookup_by_signal_name("fresh");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].edge_id, fresh_eid);
    }

    #[test]
    fn unregister_missing_entry_is_a_silent_noop() {
        // Production path: a CancelPrecondition envelope racing with a
        // wakeup that already satisfied the gate (which calls
        // `unregister` inline) lands on an empty slot. The handler must
        // absorb the duplicate without panic or state corruption.
        let idx = GateIndex::new();
        let wi = Uuid::new_v4();
        let edge = Uuid::new_v4();
        idx.unregister(wi, edge); // first time — slot was never registered
        idx.unregister(wi, edge); // second time — confirm idempotence
        assert_eq!(idx.len(), 0);
        // Register, unregister, then unregister again — same shape from
        // the CancelPrecondition-after-Satisfied race direction.
        idx.register(wi, edge, "x", None, vec![]).expect("register");
        idx.unregister(wi, edge);
        idx.unregister(wi, edge);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn replace_all_skips_entries_with_invalid_predicate() {
        let idx = GateIndex::new();
        let good_wid = Uuid::new_v4();
        let bad_wid = Uuid::new_v4();
        idx.replace_all(vec![
            (
                good_wid,
                Uuid::new_v4(),
                "ok".to_string(),
                Some("$[?@.amount > 100]".to_string()),
                vec![],
            ),
            (
                bad_wid,
                Uuid::new_v4(),
                "broken".to_string(),
                Some("$[?(garbage]".to_string()),
                vec![],
            ),
        ]);
        assert_eq!(idx.lookup_by_signal_name("ok").len(), 1);
        assert!(idx.lookup_by_signal_name("broken").is_empty());
    }

    #[test]
    fn entry_carries_captures_spec() {
        let idx = GateIndex::new();
        let wid = Uuid::new_v4();
        let eid = Uuid::new_v4();
        idx.register(
            wid,
            eid,
            "paid",
            None,
            vec![cap("email", "$.user.email"), cap("amount", "$.amount")],
        )
        .unwrap();
        let entries = idx.lookup_by_signal_name("paid");
        let names: Vec<&str> = entries[0]
            .captures_spec
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["email", "amount"]);
    }
}
