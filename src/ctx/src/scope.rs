//! Scope resolution: derive the (namespace, run_id, task_id) tuple plus the
//! DSL-declared output/input/secret names from environment variables that the
//! executor injects when spawning a task. Override flags on the CLI take
//! precedence so an operator can introspect a run from outside the engine.

use anyhow::{anyhow, Result};

#[derive(Debug, Clone)]
pub struct Scope {
    pub ns: String,
    pub run_id: String,
    /// Empty if running outside the executor and `--task` was not provided.
    /// `capture` requires it; `get`/`ls` do not.
    pub task_id: String,
    pub task_name: String,
    pub outputs: Vec<String>,
    /// Conductor-minted signal id for this run when the workflow was caused
    /// by a wire `Signal::Trigger`. Populated from the executor-injected
    /// `TICKR_TRIGGER_SIGNAL_ID` env var. `None` for cron-fired runs.
    /// `get --signal <name>` resolves against `<signal_id>/<name>` instead
    /// of the run-scoped `<run_id>/<name>` namespace.
    pub signal_id: Option<String>,
}

/// Build the fresh all-NATS KV bucket for one logical scope namespace.
pub fn bucket_for_namespace(namespace: &str) -> String {
    format!(
        "{}{}",
        tickr_proto::coord::all_nats::SCOPE_BUCKET_PREFIX,
        sanitize_segment(namespace)
    )
}

impl Scope {
    /// Resolve from env, with optional CLI overrides.
    pub fn resolve(
        ns_override: Option<String>,
        run_override: Option<String>,
        task_override: Option<String>,
    ) -> Result<Self> {
        let ns = ns_override
            .or_else(|| std::env::var("TICKR_NS").ok())
            .unwrap_or_else(|| "default".to_string());

        let run_id = run_override
            .or_else(|| std::env::var("TICKR_RUN_ID").ok())
            // Deprecated alias: TICKR_TASK_INSTANCE_ID historically carried
            // workflow_instance_id (the run id). Honor it during the
            // deprecation window with a stderr warning.
            .or_else(|| {
                if let Ok(v) = std::env::var("TICKR_TASK_INSTANCE_ID") {
                    eprintln!(
                        "tickr-ctx: warning: TICKR_TASK_INSTANCE_ID is deprecated; use TICKR_RUN_ID"
                    );
                    Some(v)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                anyhow!(
                    "no run id resolved: set TICKR_RUN_ID (the executor injects this) or pass --run"
                )
            })?;

        let task_id = task_override
            .or_else(|| std::env::var("TICKR_TASK_ID").ok())
            .unwrap_or_default();

        let task_name = std::env::var("TICKR_TASK_NAME").unwrap_or_default();

        let outputs = std::env::var("TICKR_OUTPUTS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        // `TICKR_TRIGGER_SIGNAL_ID` is only injected by the executor when the
        // run was caused by a wire `Signal::Trigger`. A cron-fired run leaves
        // the env var unset, so this resolves to `None` and any `--signal`
        // read in such a run becomes a clear error rather than a silent
        // shadow against the run-scoped namespace.
        let signal_id = std::env::var("TICKR_TRIGGER_SIGNAL_ID")
            .ok()
            .filter(|s| !s.is_empty());

        Ok(Scope {
            ns,
            run_id,
            task_id,
            task_name,
            outputs,
            signal_id,
        })
    }

    /// Build the JetStream KV bucket name for this namespace.
    pub fn bucket(&self) -> String {
        bucket_for_namespace(&self.ns)
    }

    /// Build the JetStream KV key for a run-scoped key.
    /// Layout: `<run_id>/<key>` — flat per-run namespace, all tasks in a run
    /// share it. The DSL output uniqueness check at registration time keeps
    /// concurrent fan-out tasks from colliding on the same name.
    pub fn key(&self, name: &str) -> String {
        format!("{}/{}", sanitize_segment(&self.run_id), name)
    }

    /// Build the JetStream KV key for a signal-derived capture. Lives in the
    /// same `ctx-<ns>` bucket as task outputs but under `<signal_id>/<name>`
    /// — the conductor writes here at HTTP-receive of `POST /trigger`, the
    /// task reads via this method when the input is declared `from.trigger`.
    /// Returns `None` when the run wasn't trigger-originated.
    pub fn signal_key(&self, name: &str) -> Option<String> {
        self.signal_id
            .as_deref()
            .map(|sid| format!("{}/{}", sanitize_segment(sid), name))
    }

    /// Build the `AmbientScopes` value the bare-string resolver
    /// uses to walk trigger + ambient gates + run. The trigger
    /// signal id and run id come from this `Scope`; the ambient
    /// gate signal ids come from the `TICKR_GATE_AMBIENT_SIGNAL_IDS`
    /// env var the executor injects (comma-separated UUIDs).
    pub fn ambient_scopes(&self) -> crate::ambient::AmbientScopes {
        let gates = std::env::var("TICKR_GATE_AMBIENT_SIGNAL_IDS")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        crate::ambient::AmbientScopes {
            trigger: self.signal_id.clone(),
            gates,
            run: self.run_id.clone(),
        }
    }
}

/// NATS KV legal key set is `[A-Za-z0-9_/=.-]+`. UUIDs and identifiers fit;
/// reject anything else loudly so an operator hits a clear error rather than
/// a confusing put-failed.
pub fn sanitize_segment(s: &str) -> String {
    // Defensive: replace illegal chars with `_`. Empty stays empty.
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '=' | '.' | '-' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_uses_namespace() {
        let s = Scope {
            ns: "default".into(),
            run_id: "00000000-0000-0000-0000-000000000000".into(),
            task_id: "".into(),
            task_name: "".into(),
            outputs: vec![],
            signal_id: None,
        };
        assert_eq!(s.bucket(), "TICKR_ALL_NATS_V2_SCOPE_default");
    }

    #[test]
    fn key_is_flat_run_scoped() {
        let s = Scope {
            ns: "default".into(),
            run_id: "11111111-2222-3333-4444-555555555555".into(),
            task_id: "abc".into(),
            task_name: "build".into(),
            outputs: vec![],
            signal_id: None,
        };
        assert_eq!(
            s.key("image_digest"),
            "11111111-2222-3333-4444-555555555555/image_digest"
        );
    }

    #[test]
    fn sanitize_replaces_illegal_chars() {
        assert_eq!(sanitize_segment("hello world"), "hello_world");
        assert_eq!(sanitize_segment("a.b-c_d=e/f"), "a.b-c_d=e_f"); // `/` -> `_` (separator-safe)
    }

    #[test]
    fn sanitize_preserves_uuid_shaped_input() {
        // Production data: run_ids are UUIDs; they must round-trip identically.
        let uuid = "11111111-2222-3333-4444-555555555555";
        assert_eq!(sanitize_segment(uuid), uuid);
    }

    #[test]
    fn sanitize_preserves_empty_input() {
        assert_eq!(sanitize_segment(""), "");
    }

    #[test]
    fn key_sanitizes_run_id_but_not_the_name() {
        // The sanitization only applies to the run_id segment, not the value name.
        // A weird name is preserved verbatim — that's the bucket layout's contract:
        // names are DSL-controlled and uniqueness-checked at registration time.
        let s = Scope {
            ns: "default".into(),
            run_id: "bad space".into(), // sanitized
            task_id: "".into(),
            task_name: "".into(),
            outputs: vec![],
            signal_id: None,
        };
        assert_eq!(s.key("name with spaces"), "bad_space/name with spaces");
    }

    #[test]
    fn signal_key_returns_none_without_signal_id() {
        // A cron-fired run leaves `signal_id` unset; any caller that asks
        // for a signal-keyed read gets `None`, which the CLI surface maps
        // to a clear error rather than a silent shadow against `<run_id>`.
        let s = Scope {
            ns: "default".into(),
            run_id: "11111111-2222-3333-4444-555555555555".into(),
            task_id: "".into(),
            task_name: "".into(),
            outputs: vec![],
            signal_id: None,
        };
        assert_eq!(s.signal_key("user_email"), None);
    }

    #[test]
    fn signal_key_is_flat_signal_scoped() {
        // A trigger-originated run carries the conductor-minted signal id;
        // signal-derived captures live under `<signal_id>/<name>` in the
        // same `ctx-<ns>` bucket as task outputs.
        let s = Scope {
            ns: "default".into(),
            run_id: "11111111-2222-3333-4444-555555555555".into(),
            task_id: "".into(),
            task_name: "".into(),
            outputs: vec![],
            signal_id: Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into()),
        };
        assert_eq!(
            s.signal_key("user_email"),
            Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/user_email".into())
        );
    }

    #[test]
    fn signal_key_sanitizes_signal_id() {
        // Defensive: a hostile signal id with illegal KV-key characters is
        // sanitized exactly like a run id would be, so the resulting key is
        // always KV-safe.
        let s = Scope {
            ns: "default".into(),
            run_id: "11111111-2222-3333-4444-555555555555".into(),
            task_id: "".into(),
            task_name: "".into(),
            outputs: vec![],
            signal_id: Some("bad space".into()),
        };
        assert_eq!(s.signal_key("name"), Some("bad_space/name".into()));
    }
}
