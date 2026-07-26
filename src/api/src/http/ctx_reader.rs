//! The ctx KV reader: the API component's read path against the tenant's
//! hardened all-NATS scope bucket, powering the instance Context tab.
//!
//! **Read-only: KV writers are conductor/executor only.** This module never
//! puts, deletes, or creates buckets — a missing bucket reads as an empty
//! scope, not an error, and certainly not a create.
//!
//! Envelope parsing and key derivation are consumed from the ctx crate
//! (`tickr_ctx::envelope::Envelope`, `tickr_ctx::scope::sanitize_segment`) —
//! the key layout is the frozen open-format contract, with one canonical
//! implementation on both sides of the KV.
//!
//! Secret-flagged values are masked **here**, before they reach the wire —
//! the UI renders the mask affordance but never holds the secret bytes.

use async_nats::jetstream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tickr_ctx::envelope::{Envelope, Producer};
use tickr_ctx::scope::sanitize_segment;
use utoipa::ToSchema;

/// Namespace for the tenant's ctx bucket. Mirrors the ctx CLI's resolution:
/// `TICKR_NS`, defaulting to `default`.
pub fn ctx_namespace() -> String {
    std::env::var("TICKR_NS").unwrap_or_else(|_| "default".to_string())
}

#[derive(Debug, Error)]
pub enum CtxReadError {
    /// The KV store could not be reached — distinct from "no values", so the
    /// UI can render an honest degraded state instead of a quiet empty.
    #[error("ctx store unreachable: {0}")]
    Unreachable(String),
}

/// One scope value as the Context tab renders it. `value` is `None` when
/// the envelope is secret-flagged — masked server-side.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq)]
#[schema(as = CtxEntry)]
pub struct CtxEntryView {
    /// The value's name (the key's segment after the scope id).
    pub name: String,
    /// Envelope type tag: `string` / `json` / `int` / `float` / `bool`.
    pub kind: String,
    /// The JSON value; `None` when `secret` (masked) or `!present`.
    pub value: Option<serde_json::Value>,
    pub secret: bool,
    /// `false` when a declared capture resolved to zero matches.
    pub present: bool,
    /// Human lineage summary: `task <name>` or `signal <id> (<source>)`.
    pub producer: String,
    pub created_at: String,
}

/// One satisfied signal gate's capture scope.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq)]
pub struct CtxGateScopeView {
    pub signal_id: String,
    pub entries: Vec<CtxEntryView>,
}

/// The Context tab payload: the run's tickr-ctx scope in its three
/// groupings. `storage` mirrors the snapshot's Storage indicator.
#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq)]
#[schema(as = InstanceContext)]
pub struct InstanceContextResponse {
    pub storage: String,
    pub run: Vec<CtxEntryView>,
    pub trigger: Vec<CtxEntryView>,
    pub gates: Vec<CtxGateScopeView>,
}

fn producer_summary(p: &Producer) -> String {
    match p {
        Producer::Task { task_name, task_id } => {
            if task_name.is_empty() {
                format!("task {}", task_id)
            } else {
                format!("task {}", task_name)
            }
        }
        Producer::Signal { signal_id, source } => format!("signal {} ({:?})", signal_id, source),
        Producer::System { component } => format!("system {}", component),
    }
}

/// Project one parsed envelope into the wire view, masking secrets.
fn entry_view(name: &str, envelope: &Envelope) -> CtxEntryView {
    CtxEntryView {
        name: name.to_string(),
        kind: envelope.kind.clone(),
        value: if envelope.secret || !envelope.present {
            None
        } else {
            Some(envelope.value.clone())
        },
        secret: envelope.secret,
        present: envelope.present,
        producer: producer_summary(&envelope.producer),
        created_at: envelope.created_at.clone(),
    }
}

/// Classify raw `(key, envelope)` pairs into the three groupings by key
/// prefix. Pure — shared by the live KV path and the archive-enrichment
/// path so both derive identical groupings. Unparseable envelopes are
/// skipped (logged by the caller); a missing scope is an empty grouping.
pub fn classify_entries(
    entries: &[(String, serde_json::Value)],
    run_id: &str,
    trigger_signal_id: Option<&str>,
    gate_signal_ids: &[String],
) -> InstanceContextGroups {
    let run_prefix = format!("{}/", sanitize_segment(run_id));
    let trigger_prefix = trigger_signal_id.map(|sid| format!("{}/", sanitize_segment(sid)));
    let gate_prefixes: Vec<(String, String)> = gate_signal_ids
        .iter()
        .map(|sid| (sid.clone(), format!("{}/", sanitize_segment(sid))))
        .collect();

    let mut run = Vec::new();
    let mut trigger = Vec::new();
    let mut gates: Vec<CtxGateScopeView> = gate_signal_ids
        .iter()
        .map(|sid| CtxGateScopeView {
            signal_id: sid.clone(),
            entries: Vec::new(),
        })
        .collect();

    for (key, raw) in entries {
        let envelope: Envelope = match serde_json::from_value(raw.clone()) {
            Ok(e) => e,
            Err(_) => continue, // unknown writer / corrupt entry — skip
        };
        if let Some(name) = key.strip_prefix(&run_prefix) {
            run.push(entry_view(name, &envelope));
            continue;
        }
        if let Some(tp) = &trigger_prefix {
            if let Some(name) = key.strip_prefix(tp.as_str()) {
                trigger.push(entry_view(name, &envelope));
                continue;
            }
        }
        for (sid, gp) in &gate_prefixes {
            if let Some(name) = key.strip_prefix(gp.as_str()) {
                if let Some(scope) = gates.iter_mut().find(|g| &g.signal_id == sid) {
                    scope.entries.push(entry_view(name, &envelope));
                }
                break;
            }
        }
    }

    // Deterministic ordering within each grouping.
    run.sort_by(|a, b| a.name.cmp(&b.name));
    trigger.sort_by(|a, b| a.name.cmp(&b.name));
    for g in &mut gates {
        g.entries.sort_by(|a, b| a.name.cmp(&b.name));
    }
    // Gate scopes with no entries stay listed — "this gate captured nothing"
    // is information, not noise.
    InstanceContextGroups {
        run,
        trigger,
        gates,
    }
}

/// The three groupings, before the storage tag is attached.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceContextGroups {
    pub run: Vec<CtxEntryView>,
    pub trigger: Vec<CtxEntryView>,
    pub gates: Vec<CtxGateScopeView>,
}

/// Read every `(key, envelope-json)` pair relevant to one instance from the
/// live KV. A missing bucket or missing keys read as empty (`Ok(vec![])`);
/// a connection-level failure is `Unreachable` so the caller can keep
/// "no values" and "ctx store down" distinguishable.
pub async fn read_live_entries(
    nats: &async_nats::Client,
    prefixes: &[String],
) -> Result<Vec<(String, serde_json::Value)>, CtxReadError> {
    let js = jetstream::new(nats.clone());
    let bucket = tickr_ctx::scope::bucket_for_namespace(&ctx_namespace());
    let kv = match js.get_key_value(&bucket).await {
        Ok(kv) => kv,
        // get_key_value can't distinguish "bucket absent" from transport
        // failure in its error type; probe connection state to classify.
        Err(e) => {
            return if nats.connection_state() == async_nats::connection::State::Connected {
                Ok(Vec::new()) // bucket genuinely absent — empty scope
            } else {
                Err(CtxReadError::Unreachable(e.to_string()))
            };
        }
    };

    let sanitized: Vec<String> = prefixes
        .iter()
        .map(|p| format!("{}/", sanitize_segment(p)))
        .collect();

    let mut out = Vec::new();
    let mut keys = kv
        .keys()
        .await
        .map_err(|e| CtxReadError::Unreachable(e.to_string()))?;
    while let Some(item) = keys.next().await {
        let key = match item {
            Ok(k) => k,
            Err(_) => continue,
        };
        if !sanitized.iter().any(|p| key.starts_with(p.as_str())) {
            continue;
        }
        match kv.get(&key).await {
            Ok(Some(bytes)) => {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    out.push((key, v));
                }
            }
            Ok(None) => {} // tombstoned mid-scan — a missing key is no error
            Err(_) => {}   // single-key fetch failure degrades to absence
        }
    }
    Ok(out)
}

/// Unpack the compaction enrichment (`workflow_run_info.ctx_envelope`, a
/// JSON array of `{ key, envelope }`) into the same `(key, envelope)` pairs
/// the live path yields, so both classify identically.
pub fn entries_from_enrichment(
    envelope_json: &serde_json::Value,
) -> Vec<(String, serde_json::Value)> {
    let Some(items) = envelope_json.as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let key = item.get("key")?.as_str()?.to_string();
            let env = item.get("envelope")?.clone();
            Some((key, env))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope_json(value: serde_json::Value, secret: bool) -> serde_json::Value {
        serde_json::json!({
            "v": 2,
            "type": "string",
            "value": value,
            "secret": secret,
            "producer": { "kind": "task", "task_id": "t1", "task_name": "extract" },
            "created_at": "2026-06-12T10:00:00Z",
            "sha256": "deadbeef",
        })
    }

    const RUN: &str = "11111111-2222-3333-4444-555555555555";
    const TRIG: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const GATE: &str = "99999999-8888-7777-6666-555555555555";

    #[test]
    fn classifies_by_prefix_into_three_groupings() {
        let entries = vec![
            (
                format!("{RUN}/digest"),
                envelope_json("sha:1".into(), false),
            ),
            (format!("{TRIG}/user"), envelope_json("alice".into(), false)),
            (
                format!("{GATE}/approval"),
                envelope_json("yes".into(), false),
            ),
            (
                "other-run/noise".to_string(),
                envelope_json("x".into(), false),
            ),
        ];
        let groups = classify_entries(&entries, RUN, Some(TRIG), &[GATE.to_string()]);
        assert_eq!(groups.run.len(), 1);
        assert_eq!(groups.run[0].name, "digest");
        assert_eq!(groups.trigger.len(), 1);
        assert_eq!(groups.trigger[0].name, "user");
        assert_eq!(groups.gates.len(), 1);
        assert_eq!(groups.gates[0].entries[0].name, "approval");
    }

    #[test]
    fn missing_scopes_are_empty_groupings_not_errors() {
        let groups = classify_entries(&[], RUN, None, &[]);
        assert!(groups.run.is_empty());
        assert!(groups.trigger.is_empty());
        assert!(groups.gates.is_empty());
    }

    #[test]
    fn secret_values_are_masked_before_the_wire() {
        let entries = vec![(
            format!("{RUN}/api_token"),
            envelope_json("s3cr3t".into(), true),
        )];
        let groups = classify_entries(&entries, RUN, None, &[]);
        assert_eq!(groups.run.len(), 1);
        assert!(groups.run[0].secret);
        assert_eq!(groups.run[0].value, None, "secret bytes must not ship");
    }

    #[test]
    fn corrupt_envelopes_are_skipped_not_fatal() {
        let entries = vec![
            (format!("{RUN}/good"), envelope_json("v".into(), false)),
            (format!("{RUN}/bad"), serde_json::json!({"v": 99})),
        ];
        let groups = classify_entries(&entries, RUN, None, &[]);
        assert_eq!(groups.run.len(), 1);
        assert_eq!(groups.run[0].name, "good");
    }

    #[test]
    fn enrichment_unpacks_to_key_envelope_pairs() {
        let enrichment = serde_json::json!([
            { "key": format!("{RUN}/digest"), "envelope": envelope_json("sha:1".into(), false) },
            { "malformed": true },
        ]);
        let pairs = entries_from_enrichment(&enrichment);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, format!("{RUN}/digest"));
    }
}
