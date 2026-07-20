//! Conductor-side relay enrichment for routing variables.
//!
//! When a task completes, the relay reads the task's declared `mkRoutingVar`
//! specs (from the unified `task_id`-keyed `task_specs` store, written by
//! both registration and patch ingress — a patched-in task is
//! indistinguishable from a registered one here) plus the task's emitted
//! `tickr-ctx` output bag, runs the pure routing-split function, and stamps
//! the declared routing variables onto the outbound `TaskEvent` before it
//! relays to the server.
//! Undeclared outputs stay in the `ctx-<ns>` NATS KV scope (the splitter's
//! `ctx_only` partition) — they are inter-task data the server never needs.
//!
//! All routing-variable logic lives here, conductor-side. The executor stays
//! dumb — it publishes a bare `TaskEvent` and cannot reach the splitter — so
//! that executors remain stateless and ephemeral.
//!
//! This module is the *wiring* over the splitter (lookup specs → read bag →
//! split → set the field). The partition logic itself lives in the pure,
//! unit-tested `routing_split` module; this stays a thin seam over it.

use crate::routing_split::{split, SplitError};
use anyhow::{Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use futures::StreamExt;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::BTreeMap;
use tickr_ctx::envelope::{Envelope, Producer};
use tickr_ctx::scope::sanitize_segment;
use tickr_proto::task as tc;
use tickr_proto::workflow::{RoutingValue, RoutingVarDecl};
use uuid::Uuid;

/// Default namespace for the tickr-ctx KV bucket. Mirrors `tickr_ctx::Scope`'s
/// fallback when `TICKR_NS` is unset.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// Enrichment failure, split by what the relay must do with the completion.
#[derive(Debug, thiserror::Error)]
pub enum EnrichmentError {
    /// The declared-spec lookup violated its one-row invariant. A task id is
    /// minted fresh per build (registration) or per `AddNode` (patch), so it
    /// holds exactly one `task_specs` row: zero rows means the completing
    /// task is in *no* known spec set (an integrity fault — specs are never
    /// deleted). The completion must NOT forward un-enriched — a silently
    /// dropped routing variable parks a loop forever.
    #[error(
        "declared-spec lookup for task {task_id} matched {rows} definition rows \
         (expected exactly 1); failing closed instead of forwarding un-enriched"
    )]
    LookupIntegrity { task_id: Uuid, rows: usize },
    /// A declared routing variable was present in the emitted bag but absent
    /// from the stamped result: the all-or-nothing split dropped a value the
    /// completing task actually emitted (declared/emitted type mismatch or an
    /// unsupported value shape). Forwarding un-enriched would silently strand
    /// any loop gated on the variable, and redelivery cannot fix a
    /// deterministic mismatch — the caller escalates the completion to a
    /// terminal task failure instead. Never raised on bare absence: a
    /// default-bearing variable (`loop_control`) legitimately emits nothing
    /// on a continue-iteration, and failing on absence would kill every
    /// healthy loop on its first iteration.
    #[error(
        "task {task_id}: {fault}; failing closed as a terminal task failure \
         instead of forwarding un-enriched"
    )]
    SplitStageDrop {
        task_id: Uuid,
        fault: DroppedDeclared,
    },
    /// Any other enrichment failure (emitted-bag read). The caller surfaces
    /// it loudly and forwards the un-enriched event, so a task completion is
    /// never dropped on a routing-variable bug.
    #[error(transparent)]
    Forwardable(#[from] anyhow::Error),
}

/// Split-stage fault the pure stamping seam reports: the declared routing
/// variables that were present in the emitted bag but did not make it into
/// the stamped result, plus the splitter error that dropped them. Bare
/// absence (declared but never emitted) is deliberately excluded from
/// `dropped` — only a present-but-dropped value is a fault.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "declared routing variable(s) [{}] present in emitted bag but absent from \
     stamped result: {source}",
    dropped.join(", ")
)]
pub struct DroppedDeclared {
    pub dropped: Vec<String>,
    pub source: SplitError,
}

/// Pure seam: partition the emitted output bag against the task's declared
/// routing-variable specs and write the declared routing variables into
/// `routing_variables`. Undeclared outputs are dropped here — they remain in
/// the `ctx-<ns>` KV scope. Fails closed on the split-stage drop: a declared
/// name present in the emitted bag but absent from the stamped result (the
/// all-or-nothing split errored) comes back as [`DroppedDeclared`]. Bare
/// absence is never a fault — a default-bearing variable (`loop_control`)
/// legitimately emits nothing on a continue-iteration. This is the wiring
/// under test; it performs no I/O.
pub fn stamp_routing_variables(
    declared: &[RoutingVarDecl],
    emitted: BTreeMap<String, Value>,
    routing_variables: &mut BTreeMap<String, RoutingValue>,
) -> Result<(), DroppedDeclared> {
    // The names that must survive onto the stamped result: declared AND
    // actually emitted. Absent-from-bag names are excluded up front so a
    // split failure never escalates a legitimate continue-iteration.
    let expected: Vec<String> = declared
        .iter()
        .filter(|spec| emitted.contains_key(spec.name.as_str()))
        .map(|spec| spec.name.clone())
        .collect();
    match split(declared, emitted) {
        Ok(out) => {
            *routing_variables = out.routing_vars;
            Ok(())
        }
        // The split is all-or-nothing: on error nothing was stamped, so every
        // declared-and-emitted name was dropped.
        Err(source) => Err(DroppedDeclared {
            dropped: expected,
            source,
        }),
    }
}

/// Relay enrichment entry point. No-op for any non-`Completed` `TaskEvent` and
/// for tasks that declare no routing variables. On a completing task it loads
/// the declared specs (keyed by the definition task id carried on the event)
/// and the emitted output bag, runs the seam, and writes the routing variables
/// into the event's `Completed` body in place.
///
/// Errors split three ways for the caller: [`EnrichmentError::LookupIntegrity`]
/// must fail closed by not forwarding the completion (NAK for redelivery),
/// [`EnrichmentError::SplitStageDrop`] must fail closed by escalating the
/// completion to a terminal task failure (the task emitted the declared
/// variable and the split dropped it — deterministic, so redelivery can never
/// enrich it), while [`EnrichmentError::Forwardable`] (bag read) is surfaced
/// loudly and the un-enriched event forwards — a gate over the variable then
/// stays unevaluable rather than the task completion being dropped.
pub async fn enrich_completed_task_event(
    pg_pool: &PgPool,
    nats: &NatsClient,
    event: &mut tc::TaskEvent,
) -> Result<(), EnrichmentError> {
    use tc::task_event::Kind;
    if !matches!(event.kind, Some(Kind::Completed(_))) {
        return Ok(());
    }

    // Identity rides the wire as UUID strings; parse the ids the enrichment
    // looks up. A malformed id is a conductor-internal fault — surface it
    // loudly and forward un-enriched rather than dropping the completion.
    let task_id = Uuid::parse_str(&event.task_id).context("task event task_id")?;
    let workflow_instance_id =
        Uuid::parse_str(&event.workflow_instance_id).context("task event workflow_instance_id")?;
    let task_instance_id =
        Uuid::parse_str(&event.task_instance_id).context("task event task_instance_id")?;

    let declared = load_declared_specs(pg_pool, task_id).await?;
    if declared.is_empty() {
        // The task legitimately declares no routing variables → nothing to
        // enrich; the bare event forwards unchanged.
        return Ok(());
    }

    let emitted = read_emitted_outputs(nats, workflow_instance_id, task_instance_id).await?;

    if let Some(Kind::Completed(completed)) = &mut event.kind {
        // Stamp on the conductor's scalar routing value, then project onto the
        // published proto value model the wire event carries.
        let mut routing_variables = BTreeMap::new();
        stamp_routing_variables(&declared, emitted, &mut routing_variables)
            .map_err(|fault| EnrichmentError::SplitStageDrop { task_id, fault })?;
        completed.routing_variables = routing_variables.into_iter().map(|(k, v)| (k, v)).collect();
    }
    Ok(())
}

/// Load the completing task's declared `mkRoutingVar` specs out of the
/// unified `task_id`-keyed spec store (`task_specs`), keyed by the
/// **definition task id** carried on the event. The store is written by BOTH
/// registration and patch ingress, so a patched-in task resolves exactly like
/// a registered one — one lookup, one fail-closed rule, no
/// registered-vs-patched branch. Task ids are minted fresh per build (or per
/// `AddNode`), so the primary key alone disambiguates; a missing row means
/// the completing task is in *no* known spec set (an integrity fault — specs
/// are never deleted) and is a [`EnrichmentError::LookupIntegrity`] fault the
/// caller fails closed on.
async fn load_declared_specs(
    pg_pool: &PgPool,
    task_id: Uuid,
) -> Result<Vec<RoutingVarDecl>, EnrichmentError> {
    let row: Option<(Value,)> =
        sqlx::query_as("SELECT routing_vars FROM task_specs WHERE task_id = $1")
            .bind(task_id)
            .fetch_optional(pg_pool)
            .await
            .context("read declared routing-variable specs for enrichment")?;

    match row {
        Some((value,)) => {
            Ok(serde_json::from_value(value)
                .context("deserialize declared routing-variable specs")?)
        }
        None => Err(EnrichmentError::LookupIntegrity { task_id, rows: 0 }),
    }
}

/// Read the completing task's reserved self-patch output
/// (`tickr_patch`) out of the `ctx-<ns>` KV bucket, if it published one.
/// A point-read on `<run_id>/tickr_patch`, producer-checked against this
/// task instance so another task's document in the same run scope is never
/// mis-attributed. `None` on absence or on any read fault — detection is
/// best-effort; a genuinely-lost document is lost-but-logged, and the server
/// never stalls for a document the conductor didn't see.
pub async fn read_self_patch_output(
    nats: &NatsClient,
    run_id: Uuid,
    task_instance_id: Uuid,
) -> Option<Value> {
    let js = jetstream::new(nats.clone());
    let bucket = format!("ctx-{}", sanitize_segment(DEFAULT_CTX_NAMESPACE));
    let kv = js.get_key_value(&bucket).await.ok()?;
    let key = format!(
        "{}/{}",
        sanitize_segment(&run_id.to_string()),
        "tickr_patch"
    );
    let bytes = kv.get(&key).await.ok()??;
    let env: Envelope = match serde_json::from_slice(&bytes) {
        Ok(env) => env,
        Err(e) => {
            tracing::warn!("self-patch detection: ctx envelope for {key} failed to parse: {e}");
            return None;
        }
    };
    let producer_id = task_instance_id.to_string();
    match &env.producer {
        Producer::Task { task_id, .. } if task_id == &producer_id => Some(env.value),
        other => {
            tracing::warn!(
                "self-patch detection: ctx key {key} skipped — producer {other:?} \
                 is not completing task instance {producer_id}"
            );
            None
        }
    }
}

/// Read this task's emitted output bag out of the `ctx-<ns>` JetStream KV
/// bucket. Keys are `<run_id>/<name>`; the run scope is shared by every task
/// in the run, so we filter to envelopes whose `Producer::Task` carries this
/// task instance's id. Signal-derived captures live under `<signal_id>/<name>`
/// and are excluded by the run-id prefix. A missing bucket means no outputs.
async fn read_emitted_outputs(
    nats: &NatsClient,
    run_id: Uuid,
    task_instance_id: Uuid,
) -> Result<BTreeMap<String, Value>> {
    let js = jetstream::new(nats.clone());
    let bucket = format!("ctx-{}", sanitize_segment(DEFAULT_CTX_NAMESPACE));
    let kv = match js.get_key_value(&bucket).await {
        Ok(kv) => kv,
        // No bucket → the run published nothing. Not an error.
        Err(_) => return Ok(BTreeMap::new()),
    };

    let prefix = format!("{}/", sanitize_segment(&run_id.to_string()));
    let producer_id = task_instance_id.to_string();
    let mut bag: BTreeMap<String, Value> = BTreeMap::new();

    // NATS KV `keys()` streams every key in the bucket; we client-side filter
    // to this run's prefix. The bucket is per-namespace, not per-run, so cost
    // scales with bucket size — same pattern as the compaction scope read.
    let mut keys = kv.keys().await.context("list ctx-scope KV keys")?;
    while let Some(item) = keys.next().await {
        let key = match item {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(
                    "routing enrichment: ctx KV keys() yielded an error: {e}; \
                     key dropped from emitted bag"
                );
                continue;
            }
        };
        if !key.starts_with(&prefix) {
            continue;
        }
        let name = &key[prefix.len()..];
        match kv.get(&key).await {
            Ok(Some(bytes)) => match serde_json::from_slice::<Envelope>(&bytes) {
                Ok(env) => match &env.producer {
                    Producer::Task { task_id, .. } if task_id == &producer_id => {
                        bag.insert(name.to_string(), env.value);
                    }
                    other => {
                        // Same-run key from another producer: legitimately not
                        // this task's output, but logged so a read-stage gap
                        // (an unexpectedly empty bag) is diagnosable rather
                        // than swallowed.
                        tracing::warn!(
                            "routing enrichment: ctx key {key} skipped — producer {other:?} \
                             is not completing task instance {producer_id}"
                        );
                    }
                },
                Err(e) => tracing::warn!(
                    "routing enrichment: failed to parse ctx envelope JSON for key {key}: {e}; \
                     key dropped from emitted bag"
                ),
            },
            Ok(None) => {} // tombstoned mid-scan; skip
            Err(e) => tracing::warn!(
                "routing enrichment: failed to fetch ctx value for key {key}: {e}; \
                 key dropped from emitted bag"
            ),
        }
    }

    Ok(bag)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, ty: Option<&str>) -> RoutingVarDecl {
        RoutingVarDecl {
            name: name.to_string(),
            var_type: ty.map(String::from),
        }
    }

    fn bag(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn relayed_event_carries_only_declared_routing_variables() {
        // The isolated relay-enrichment seam: declared specs + an emitted bag
        // (mixing a declared output with an undeclared one). The relayed
        // event's routing variables must carry exactly the declared routing
        // variable and exclude the undeclared output.
        let declared = vec![spec("decision", None)];
        let emitted = bag(&[
            ("decision", Value::String("approve".into())),
            ("image_digest", Value::String("sha256:abc".into())),
        ]);
        let mut routing_variables = BTreeMap::new();

        stamp_routing_variables(&declared, emitted, &mut routing_variables).unwrap();

        assert_eq!(routing_variables.len(), 1);
        assert_eq!(
            routing_variables.get("decision"),
            Some(&RoutingValue {
                value: Some(tickr_proto::workflow::routing_value::Value::StringValue(
                    "approve".to_string(),
                )),
            })
        );
        assert!(!routing_variables.contains_key("image_digest"));
    }

    #[test]
    fn no_declared_specs_leaves_routing_variables_empty() {
        let declared: Vec<RoutingVarDecl> = vec![];
        let emitted = bag(&[("image_digest", Value::String("sha256:abc".into()))]);
        let mut routing_variables = BTreeMap::new();

        stamp_routing_variables(&declared, emitted, &mut routing_variables).unwrap();

        assert!(routing_variables.is_empty());
    }

    #[test]
    fn declared_variable_present_in_bag_but_not_stamped_fails_closed() {
        // The narrow fail-closed condition: the task DID emit its declared
        // variable, but the declared/emitted type mismatch makes the
        // all-or-nothing split drop it from the stamped result.
        let declared = vec![spec("coverage", Some("int"))];
        let emitted = bag(&[("coverage", Value::String("eighty".into()))]);
        let mut routing_variables = BTreeMap::new();

        let fault =
            stamp_routing_variables(&declared, emitted, &mut routing_variables).unwrap_err();
        assert_eq!(fault.dropped, vec!["coverage".to_string()]);
        match fault.source {
            SplitError::TypeMismatch {
                declared_type,
                actual_type,
                ..
            } => {
                assert_eq!(declared_type, "int");
                assert_eq!(actual_type, "string");
            }
            other => panic!("expected TypeMismatch, got {:?}", other),
        }
        // Nothing was stamped — the caller escalates the completion to a
        // terminal task failure instead of forwarding it un-enriched.
        assert!(routing_variables.is_empty());
    }

    #[test]
    fn default_bearing_bare_absence_is_not_a_fault() {
        // A default-bearing variable (`loop_control`) legitimately emits
        // nothing on a continue-iteration: bare absence must NOT fail, or
        // every healthy loop would die on its first iteration. Only a
        // present-but-dropped value is a fault.
        let declared = vec![spec("loop_control", Some("string"))];
        let emitted = bag(&[("scratch", Value::String("inter-task data".into()))]);
        let mut routing_variables = BTreeMap::new();

        stamp_routing_variables(&declared, emitted, &mut routing_variables).unwrap();

        assert!(routing_variables.is_empty());
    }

    #[test]
    fn dropped_set_is_exactly_the_declared_names_present_in_the_bag() {
        // Two declared variables, one emitted (with a mismatch), one absent:
        // the fault names only the emitted-and-dropped one. The absent one is
        // a legitimate non-emission, not part of the fault.
        let declared = vec![
            spec("loop_control", Some("string")),
            spec("coverage", Some("int")),
        ];
        let emitted = bag(&[("coverage", Value::String("eighty".into()))]);
        let mut routing_variables = BTreeMap::new();

        let fault =
            stamp_routing_variables(&declared, emitted, &mut routing_variables).unwrap_err();
        assert_eq!(fault.dropped, vec!["coverage".to_string()]);
    }

    #[test]
    fn declared_specs_deserialize_from_projected_definition_json() {
        // The lookup projects `routing_vars` straight out of the stored JSONB
        // definition; that JSON must deserialize into the server declaration
        // type the splitter consumes — no conductor-local mapping in between.
        let projected = serde_json::json!([
            { "name": "loop_control", "var_type": "string" },
            { "name": "decision" }
        ]);
        let declared: Vec<RoutingVarDecl> = serde_json::from_value(projected).unwrap();
        assert_eq!(
            declared,
            vec![spec("loop_control", Some("string")), spec("decision", None)]
        );
    }
}
