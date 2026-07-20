//! Three-scope ambient resolver for bare-string `tickr-ctx get`
//! lookups. Pure module — takes the scope set + a fetcher trait
//! and returns `Ok(value)` for a unique single-scope hit,
//! `Err(NotFound)` when no scope has the name, and
//! `Err(MultiScopeCollision)` when more than one scope hits.
//!
//! The scopes are:
//!
//! 1. **Trigger scope** — `<trigger_signal_id>/<name>`. Populated
//!    by the conductor's `/trigger` HTTP path when the run was
//!    caused by a wire `Signal::Trigger`.
//! 2. **Ambient gate scopes** — `<gate_signal_id>/<name>` for
//!    every `Satisfied { signal_id }` gate on edges **incident
//!    to the declaring task**. Narrowed deliberately to incident
//!    edges; gates on unrelated branches do not enter the
//!    resolver.
//! 3. **Run scope** — `<run_id>/<name>`. The shared task-output
//!    namespace; today's bare-string default.
//!
//! **Strict declarations bypass the resolver entirely.** A task
//! authored with `inputs = [{ name; from.signal = <gate>; }]`
//! reads `<gate_signal_id>/<name>` directly via the executor's
//! `TICKR_GATE_SIGNAL_ID_<NAME>` envvar; `from.trigger` uses
//! `<trigger_signal_id>/<name>` directly; `from.task` uses the run
//! scope. The ambient resolver is only consulted for bare-string
//! lookups that did not declare a strict source.
//!
//! `HashSet<String>` (not `Vec`) for the gate scope is deliberate:
//! resolution order is never consulted — multi-match is always
//! an error, never a tiebreak by declaration order.

use std::collections::HashSet;
use thiserror::Error;

/// Identity of a scope that successfully resolved the name. Carried
/// on the `MultiScopeCollision` error so the message can name the
/// colliding scopes concretely; the `Trigger` and `Run` variants
/// carry no payload because their ids are uniquely-shaped per run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeHit {
    Trigger,
    /// Gate's wakeup `signal_id`. Multi-gate-collisions name each
    /// gate by signal_id so the operator can trace which gate's
    /// scope owned the value.
    Gate(String),
    Run,
}

/// Failure modes returned by `resolve_ambient`. `NotFound` is the
/// today's bare-string behaviour preserved; `MultiScopeCollision`
/// is the new fail-loud rule that catches name clashes across
/// trigger / gate / run scopes at lookup time rather than letting
/// a silent winner pick.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AmbientError {
    #[error("ambient resolver: name {name:?} not found in any scope")]
    NotFound { name: String },
    #[error(
        "ambient resolver: name {name:?} resolves in multiple scopes {scopes:?}; \
         pin the source with `from.signal`, `from.trigger`, or `from.task` to disambiguate"
    )]
    MultiScopeCollision { name: String, scopes: Vec<ScopeHit> },
}

/// What the resolver needs to know about the runtime context to do
/// its job. Filled in at the executor boundary from the queue item's
/// `originating_signal_id` (trigger), `gate_signal_ids_ambient`
/// (gates), and the run id.
#[derive(Debug, Clone)]
pub struct AmbientScopes {
    /// Trigger signal id, if the run was Signal::Trigger-originated.
    pub trigger: Option<String>,
    /// Set of gate signal ids on edges incident to the declaring
    /// task (the only gates that may contribute ambient values).
    pub gates: HashSet<String>,
    /// Run id — always present.
    pub run: String,
}

/// Fetcher trait. Tests inject an in-memory fake; production wires
/// against the NATS KV store. `fetch` returns the raw value bytes
/// stored at the given key, or `None` for a miss.
pub trait KvFetcher {
    fn fetch(&self, key: &str) -> Option<Vec<u8>>;
}

/// Three-scope walk. Returns the value bytes on a unique hit,
/// `Err(NotFound)` on zero hits, `Err(MultiScopeCollision)` on
/// more than one hit.
///
/// Each scope is probed independently; the resolver does NOT
/// short-circuit on the first hit because that's what produces the
/// silent winner the fail-loud rule is designed to prevent.
pub fn resolve_ambient<F: KvFetcher>(
    name: &str,
    scopes: &AmbientScopes,
    fetcher: &F,
) -> Result<Vec<u8>, AmbientError> {
    let mut hits: Vec<(ScopeHit, Vec<u8>)> = Vec::new();

    if let Some(trigger_id) = &scopes.trigger {
        let key = format!("{}/{}", trigger_id, name);
        if let Some(v) = fetcher.fetch(&key) {
            hits.push((ScopeHit::Trigger, v));
        }
    }
    for gate_id in &scopes.gates {
        let key = format!("{}/{}", gate_id, name);
        if let Some(v) = fetcher.fetch(&key) {
            hits.push((ScopeHit::Gate(gate_id.clone()), v));
        }
    }
    let run_key = format!("{}/{}", scopes.run, name);
    if let Some(v) = fetcher.fetch(&run_key) {
        hits.push((ScopeHit::Run, v));
    }

    match hits.len() {
        0 => Err(AmbientError::NotFound {
            name: name.to_string(),
        }),
        1 => {
            // Single hit — strip and return the bytes.
            let (_scope, value) = hits.into_iter().next().expect("len==1");
            Ok(value)
        }
        _ => {
            let scopes: Vec<ScopeHit> = hits.into_iter().map(|(s, _)| s).collect();
            Err(AmbientError::MultiScopeCollision {
                name: name.to_string(),
                scopes,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// In-memory fake. Tests build a map of `<scope_id>/<name>` →
    /// value and the resolver walks the same shape against it.
    struct InMemoryKv {
        data: HashMap<String, Vec<u8>>,
    }
    impl KvFetcher for InMemoryKv {
        fn fetch(&self, key: &str) -> Option<Vec<u8>> {
            self.data.get(key).cloned()
        }
    }

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn single_hit_in_trigger_scope_returns_value() {
        let mut data = HashMap::new();
        data.insert("sig-1/email".to_string(), b("alice@example.com"));
        let fetcher = InMemoryKv { data };
        let scopes = AmbientScopes {
            trigger: Some("sig-1".to_string()),
            gates: HashSet::new(),
            run: "run-1".to_string(),
        };
        let v = resolve_ambient("email", &scopes, &fetcher).unwrap();
        assert_eq!(v, b("alice@example.com"));
    }

    #[test]
    fn single_hit_in_one_incident_gate_scope_returns_value() {
        let mut data = HashMap::new();
        data.insert("gate-1/approver".to_string(), b("bob@example.com"));
        let fetcher = InMemoryKv { data };
        let mut gates = HashSet::new();
        gates.insert("gate-1".to_string());
        gates.insert("gate-2".to_string()); // doesn't have the key
        let scopes = AmbientScopes {
            trigger: None,
            gates,
            run: "run-1".to_string(),
        };
        let v = resolve_ambient("approver", &scopes, &fetcher).unwrap();
        assert_eq!(v, b("bob@example.com"));
    }

    #[test]
    fn single_hit_in_run_scope_returns_value() {
        let mut data = HashMap::new();
        data.insert("run-1/build_artifact".to_string(), b("/path/to/image.tar"));
        let fetcher = InMemoryKv { data };
        let scopes = AmbientScopes {
            trigger: None,
            gates: HashSet::new(),
            run: "run-1".to_string(),
        };
        let v = resolve_ambient("build_artifact", &scopes, &fetcher).unwrap();
        assert_eq!(v, b("/path/to/image.tar"));
    }

    #[test]
    fn multi_hit_trigger_plus_gate_errors_with_both_scopes_named() {
        let mut data = HashMap::new();
        data.insert("sig-1/email".to_string(), b("author@example.com"));
        data.insert("gate-1/email".to_string(), b("approver@example.com"));
        let fetcher = InMemoryKv { data };
        let mut gates = HashSet::new();
        gates.insert("gate-1".to_string());
        let scopes = AmbientScopes {
            trigger: Some("sig-1".to_string()),
            gates,
            run: "run-1".to_string(),
        };
        let err = resolve_ambient("email", &scopes, &fetcher).unwrap_err();
        match err {
            AmbientError::MultiScopeCollision { name, scopes } => {
                assert_eq!(name, "email");
                assert!(scopes.contains(&ScopeHit::Trigger));
                assert!(scopes.contains(&ScopeHit::Gate("gate-1".to_string())));
            }
            other => panic!("expected MultiScopeCollision, got {:?}", other),
        }
    }

    #[test]
    fn multi_hit_two_gates_errors_with_both_gates_named() {
        let mut data = HashMap::new();
        data.insert("gate-1/email".to_string(), b("a@example.com"));
        data.insert("gate-2/email".to_string(), b("b@example.com"));
        let fetcher = InMemoryKv { data };
        let mut gates = HashSet::new();
        gates.insert("gate-1".to_string());
        gates.insert("gate-2".to_string());
        let scopes = AmbientScopes {
            trigger: None,
            gates,
            run: "run-1".to_string(),
        };
        let err = resolve_ambient("email", &scopes, &fetcher).unwrap_err();
        match err {
            AmbientError::MultiScopeCollision { name: _, scopes } => {
                assert!(scopes.contains(&ScopeHit::Gate("gate-1".to_string())));
                assert!(scopes.contains(&ScopeHit::Gate("gate-2".to_string())));
            }
            other => panic!("expected MultiScopeCollision, got {:?}", other),
        }
    }

    #[test]
    fn multi_hit_trigger_plus_gate_plus_run_errors_with_all_three() {
        let mut data = HashMap::new();
        data.insert("sig-1/x".to_string(), b("trig"));
        data.insert("gate-1/x".to_string(), b("gate"));
        data.insert("run-1/x".to_string(), b("run"));
        let fetcher = InMemoryKv { data };
        let mut gates = HashSet::new();
        gates.insert("gate-1".to_string());
        let scopes = AmbientScopes {
            trigger: Some("sig-1".to_string()),
            gates,
            run: "run-1".to_string(),
        };
        let err = resolve_ambient("x", &scopes, &fetcher).unwrap_err();
        match err {
            AmbientError::MultiScopeCollision { scopes, .. } => {
                assert!(scopes.contains(&ScopeHit::Trigger));
                assert!(scopes.contains(&ScopeHit::Gate("gate-1".to_string())));
                assert!(scopes.contains(&ScopeHit::Run));
            }
            other => panic!("expected MultiScopeCollision, got {:?}", other),
        }
    }

    #[test]
    fn zero_hits_returns_not_found() {
        let fetcher = InMemoryKv {
            data: HashMap::new(),
        };
        let scopes = AmbientScopes {
            trigger: Some("sig-1".to_string()),
            gates: HashSet::new(),
            run: "run-1".to_string(),
        };
        let err = resolve_ambient("missing", &scopes, &fetcher).unwrap_err();
        assert!(matches!(err, AmbientError::NotFound { .. }));
    }

    #[test]
    fn empty_scopes_check_only_run_segment() {
        // No trigger, no gates — only run-scope is probed. The
        // resolver still works (just degrades to today's behaviour).
        let mut data = HashMap::new();
        data.insert("run-1/x".to_string(), b("y"));
        let fetcher = InMemoryKv { data };
        let scopes = AmbientScopes {
            trigger: None,
            gates: HashSet::new(),
            run: "run-1".to_string(),
        };
        let v = resolve_ambient("x", &scopes, &fetcher).unwrap();
        assert_eq!(v, b("y"));
    }
}
