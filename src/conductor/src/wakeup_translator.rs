//! Conductor-side translator for `Signal::Wakeup`-shaped ingresses
//! against `waits-on-signal` subscriber workflows.
//!
//! Wakeup ingresses arrive named: `{ name = "user-paid"; payload; ... }`.
//! The translator looks the name up in the subscription index, evaluates
//! each subscriber's optional JSONPath predicate against the payload,
//! extracts the subscriber's merged captures, writes those captures to
//! the selected `signal_captures` repository + the per-tenant `ctx-<ns>`
//! NATS KV bucket, then synthesizes one `Signal::Trigger { source =
//! TriggerSource::Wakeup { name } }` per subscriber. The server's wheel
//! materializes one instance per emitted Trigger, each with
//! `triggered_by = Wakeup { signal_id, name }`.
//!
//! Idempotency: the wakeup's `(idempotency_key, hash({ name, payload }))`
//! is checked against the per-tenant cache before any side effect runs,
//! matching the Trigger / Cancel ingress patterns.
//!
//! Known issue (deliberate, deferred): all N fan-out Triggers share the
//! wakeup's `signal_id`. Two subscribers with overlapping capture names
//! collide in NATS KV (last-writer-wins) and `signal_captures` only
//! retains the first subscriber's row (`ON CONFLICT DO NOTHING`).

use anyhow::{anyhow, Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use serde_json::Value;
use tickr_ctx::envelope::SignalSource;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::scope_repository::ScopeStore;
use tickr_proto::signal as sp;
use uuid::Uuid;

use crate::captures_extractor::{
    extract_captures, ExtractionError, NamedEnvelope as ExtractedEnvelope,
};
use crate::gate_index::{Entry as GateEntry, GateIndex};
use crate::idempotency;
use crate::predicate;
use crate::scope_working_set::write_event_captures;
use crate::signal_captures;
use crate::subscription_index::Entry;
use crate::waits_on_signal_lifecycle::signal_subscription_index;

/// Per-tenant tickr-ctx bucket namespace. Mirrors the trigger pipeline so
/// captures from either source land in the same `ctx-<ns>` bucket.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// Producer intent the transport-specific caller assembles. The hashing
/// payload is `{ "name": ..., "payload": ... }` — `target` is excluded
/// because the waits-on-signal consumer ignores it (target is reserved
/// for the deferred in-graph gate consumer).
pub struct WakeupRequest {
    pub name: String,
    pub payload: Option<Value>,
    pub idempotency_key: Option<String>,
}

/// Outcome of the translator pipeline. The HTTP / NATS adapter projects
/// onto its transport-specific response shape.
pub enum WakeupOutcome {
    /// First arrival for this `(idempotency_key, hash)` (or no key at
    /// all). Captures have been written and `matched_workflows` Triggers
    /// were forwarded. `gates_matched` reflects the parallel hyperedge-
    /// gate fan-out for the same wakeup — each entry maps to one
    /// `GateOutcome { signal_id }` emitted at the server.
    Fresh {
        signal_id: Uuid,
        matched_workflows: u32,
        gates_matched: u32,
    },
    /// Same idempotency key, byte-identical canonical-JSON payload — a
    /// retry. No side effects ran; the cached signal_id is returned.
    Deduplicated { original_signal_id: Uuid },
    /// Same idempotency key, different payload — a publisher bug.
    Conflict {
        original_signal_id: Uuid,
        original_hash: String,
        your_hash: String,
    },
}

/// Forwarder for synthesized Triggers and `GateOutcome`
/// envelopes. Decoupled as a trait so the translator is testable
/// without standing up the relay client. Both methods take an
/// `async fn` because the underlying relay channel is async; in-
/// process tests substitute a recording sender that buffers calls.
#[async_trait::async_trait]
pub trait WakeupRelaySender: Send + Sync {
    async fn send(&self, signal: &sp::Signal) -> Result<()>;
    async fn send_gate_outcome(&self, outcome: &sp::GateOutcome) -> Result<()>;
}

/// Default sender wired against the global conductor relay channel.
pub struct DefaultRelaySender;

#[async_trait::async_trait]
impl WakeupRelaySender for DefaultRelaySender {
    async fn send(&self, signal: &sp::Signal) -> Result<()> {
        crate::relay::send_signal(signal).await
    }
    async fn send_gate_outcome(&self, outcome: &sp::GateOutcome) -> Result<()> {
        crate::relay::send_gate_outcome(outcome).await
    }
}

/// Run the wakeup translation pipeline. Idempotency cache short-circuits
/// before any side effect. On `Fresh`, N Triggers have already been
/// forwarded over `sender` and the returned count reflects how many
/// subscribers matched (after predicate eval); the parallel
/// `gates_matched` count carries the second consumer arm's tally,
/// i.e. how many dispatched hyperedge gates the wakeup satisfied.
///
/// `gate_index` is the per-instance gate index. Callers pass the
/// process-wide singleton in production and an isolated instance
/// from `GateIndex::new()` in tests so concurrent test runs don't
/// stomp on each other.
pub async fn process_wakeup(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    sender: &dyn WakeupRelaySender,
    gate_index: &GateIndex,
    req: WakeupRequest,
) -> Result<WakeupOutcome> {
    process_wakeup_with_working_set(
        repositories,
        Some(nats),
        None,
        sender,
        gate_index,
        req,
        None,
    )
    .await
}

/// Tickr Lite wakeups preserve relay and SQL effects while the profile-disabled
/// ingress cache and NATS working-set mirror are absent.
pub async fn process_wakeup_local(
    repositories: &WriterRepositoryBundle,
    sender: &dyn WakeupRelaySender,
    gate_index: &GateIndex,
    req: WakeupRequest,
) -> Result<WakeupOutcome> {
    process_wakeup_with_working_set(repositories, None, None, sender, gate_index, req, None).await
}

/// Run an External Event wakeup under its durable producer reservation.
/// The supplied sender persists each relay intent before reporting success.
pub async fn process_reserved_wakeup(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    sender: &dyn WakeupRelaySender,
    gate_index: &GateIndex,
    req: WakeupRequest,
    signal_id: Uuid,
) -> Result<WakeupOutcome> {
    process_wakeup_with_working_set(
        repositories,
        Some(nats),
        None,
        sender,
        gate_index,
        req,
        Some(signal_id),
    )
    .await
}
/// Run a reserved External Event wakeup through the selected ScopeStore role.
pub async fn process_reserved_wakeup_with_scope_store(
    repositories: &WriterRepositoryBundle,
    scope_store: &dyn ScopeStore,
    sender: &dyn WakeupRelaySender,
    gate_index: &GateIndex,
    req: WakeupRequest,
    signal_id: Uuid,
) -> Result<WakeupOutcome> {
    process_wakeup_with_working_set(
        repositories,
        None,
        Some(scope_store),
        sender,
        gate_index,
        req,
        Some(signal_id),
    )
    .await
}

async fn process_wakeup_with_working_set(
    repositories: &WriterRepositoryBundle,
    nats: Option<&NatsClient>,
    scope_store: Option<&dyn ScopeStore>,
    sender: &dyn WakeupRelaySender,
    gate_index: &GateIndex,
    req: WakeupRequest,
    reserved_signal_id: Option<Uuid>,
) -> Result<WakeupOutcome> {
    let payload_value = req.payload.unwrap_or(Value::Object(Default::default()));
    let hash_payload = serde_json::json!({
        "name": &req.name,
        "payload": &payload_value,
    });
    let input_hash = crate::canonical_json::hash(Some(&hash_payload));

    let reserved = reserved_signal_id.is_some();
    let signal_id = reserved_signal_id.unwrap_or_else(Uuid::new_v4);

    // Producer idempotency is already decided by the outer reservation on
    // External Event ingress; HTTP/local callers retain the shared cache.
    if !reserved {
        if let (Some(key), Some(nats)) = (req.idempotency_key.as_deref(), nats) {
            let bucket = idempotency::open_bucket(nats)
                .await
                .context("idempotency bucket")?;
            let outcome = idempotency::check_or_insert(&bucket, key, signal_id, &input_hash)
                .await
                .context("idempotency check")?;
            match outcome {
                idempotency::CacheOutcome::Fresh => {}
                idempotency::CacheOutcome::DeduplicatedSameHash { original_signal_id } => {
                    return Ok(WakeupOutcome::Deduplicated { original_signal_id });
                }
                idempotency::CacheOutcome::ConflictDifferentHash {
                    original_signal_id,
                    original_hash,
                } => {
                    return Ok(WakeupOutcome::Conflict {
                        original_signal_id,
                        original_hash,
                        your_hash: hex::encode(input_hash),
                    });
                }
            }
        }
    }

    // 2. Subscription lookup. Each entry carries its parsed predicate
    //    and pre-merged captures so the hot path doesn't re-parse.
    let entries = signal_subscription_index().lookup(&req.name);
    let gate_entries = gate_index.lookup_by_signal_name(&req.name);
    if entries.is_empty() && gate_entries.is_empty() {
        // Verified & recorded (gated-arm / SignalReceived re-arm window): an
        // unmatched wakeup is **dropped, not parked** — there is no buffer that
        // holds it for a gate that arms momentarily later. This opens a real
        // wakeup-loss window when a non-terminal `SignalReceived` gate is
        // re-seated: the re-arm crosses the conductor as unregister(old-edge)
        // then register(new-edge), and a one-shot wakeup landing between the two
        // matches neither and is lost (tickr has no replay). The fix is envelope
        // ordering — register(new) before unregister(old), or make the pair
        // atomic — surfaced here for the design-review gate. See
        // `gate_index::tests::reseat_window_leaves_the_name_unmatched_no_parking`.
        tracing::info!(
            "Wakeup ingress had no subscribers or gates: name={}, signal_id={}",
            req.name,
            signal_id,
        );
        return Ok(WakeupOutcome::Fresh {
            signal_id,
            matched_workflows: 0,
            gates_matched: 0,
        });
    }

    // 3. Per-subscriber: predicate eval, captures extraction, persist,
    //    synthesize Trigger, forward over relay.
    let mut matched_workflows: u32 = 0;
    for entry in entries {
        if !predicate_matches(&entry, &payload_value) {
            continue;
        }
        if nats.is_some() || scope_store.is_some() {
            if let Err(e) = persist_subscriber_captures(
                repositories,
                nats,
                scope_store,
                signal_id,
                &req.name,
                &entry,
                &payload_value,
            )
            .await
            {
                if reserved {
                    return Err(e).context("persist reserved wakeup subscriber captures");
                }
                tracing::warn!(
                    "wakeup translator: persist captures failed for workflow {}: {} (continuing fan-out)",
                    entry.workflow_id,
                    e
                );
            } else if reserved {
                crate::ingress_idempotency::observe_ingress_boundary(
                    crate::ingress_idempotency::IngressBoundary::AfterCapturePersistence,
                );
            }
        }
        let signal = sp::Signal {
            signal_id: signal_id.to_string(),
            idempotency_key: None,
            variant: Some(sp::signal::Variant::Trigger(sp::Trigger {
                workflow_id: entry.workflow_id.to_string(),
                scheduled_at: None,
                source: Some(sp::TriggerSource {
                    source: Some(sp::trigger_source::Source::Wakeup(
                        sp::trigger_source::Wakeup {
                            name: req.name.clone(),
                        },
                    )),
                }),
                // Wakeup-translated triggers carry no user Run name; the
                // materialized instance shows the server default.
                name: None,
                // A wakeup is a first-fire trigger, not a replay.
                replay: None,
            })),
        };
        if let Err(e) = sender.send(&signal).await {
            if reserved {
                return Err(e).context("persist reserved wakeup Signal intent");
            }
            tracing::warn!(
                "wakeup translator: relay send failed for workflow {}: {} (continuing fan-out)",
                entry.workflow_id,
                e
            );
            continue;
        }
        matched_workflows = matched_workflows.saturating_add(1);
    }

    // 4. Per-gate: predicate eval, captures extraction to NATS KV,
    //    synthesize `GateOutcome`, forward over relay, drop entry
    //    from the index. Runs in parallel with the subscriber arm —
    //    a single wakeup can fire both a Trigger and a GateOutcome
    //    on the same `signal_id`; downstream consumers disambiguate
    //    via the typed envelope.
    let mut gates_matched: u32 = 0;
    for gate in gate_entries {
        if !gate_predicate_matches(&gate, &payload_value) {
            continue;
        }
        if nats.is_some() || scope_store.is_some() {
            if let Err(e) = write_gate_captures(
                nats,
                scope_store,
                signal_id,
                &req.name,
                &gate,
                &payload_value,
            )
            .await
            {
                if reserved {
                    return Err(e).context("persist reserved wakeup gate captures");
                }
                tracing::warn!(
                    "wakeup translator: persist gate captures failed for ({}, {}): {} (continuing fan-out)",
                    gate.workflow_instance_id,
                    gate.edge_id,
                    e
                );
            } else if reserved {
                crate::ingress_idempotency::observe_ingress_boundary(
                    crate::ingress_idempotency::IngressBoundary::AfterCapturePersistence,
                );
            }
        }
        let outcome = sp::GateOutcome {
            workflow_instance_id: gate.workflow_instance_id.to_string(),
            edge_id: gate.edge_id.to_string(),
            signal_id: signal_id.to_string(),
        };
        if let Err(e) = sender.send_gate_outcome(&outcome).await {
            if reserved {
                return Err(e).context("persist reserved wakeup GateOutcome intent");
            }
            tracing::warn!(
                "wakeup translator: gate-outcome relay send failed for ({}, {}): {} (continuing fan-out)",
                gate.workflow_instance_id,
                gate.edge_id,
                e
            );
            continue;
        }
        // Gate satisfied — drop it from the index so a follow-up wakeup
        // on the same name doesn't double-fire against the same edge.
        gate_index.unregister(gate.workflow_instance_id, gate.edge_id);
        gates_matched = gates_matched.saturating_add(1);
    }

    // Audit row: one per processed wakeup, written after the fan-out
    // resolves so `matched_workflows` reflects the final count.
    let audit_row = crate::signal_wakeups::SignalWakeupRow {
        signal_id,
        name: req.name.clone(),
        matched_workflows: matched_workflows as i32,
    };
    if let Err(e) = crate::signal_wakeups::insert(repositories, &audit_row).await {
        if reserved {
            return Err(e).context("persist reserved wakeup audit");
        }
        tracing::warn!(
            "wakeup translator: signal_wakeups audit write failed (signal_id={}): {}",
            signal_id,
            e
        );
    }

    Ok(WakeupOutcome::Fresh {
        signal_id,
        matched_workflows,
        gates_matched,
    })
}

fn gate_predicate_matches(entry: &GateEntry, payload: &Value) -> bool {
    match entry.predicate.as_ref() {
        Some(p) => predicate::evaluate(p, payload),
        None => true,
    }
}

/// Mirror the subscriber-captures persistence shape for a matched
/// gate. Writes only NATS KV (the executor's working-set cache via
/// `tickr-ctx get`) — the SQL `signal_captures` archive is keyed on
/// `workflow_id` which the gate-fan-out site doesn't carry; the
/// gate's recovery path is a re-issued `DispatchPrecondition` from
/// the server, so SQL rehydration isn't load-bearing for gates.
async fn write_gate_captures(
    nats: Option<&NatsClient>,
    scope_store: Option<&dyn ScopeStore>,
    signal_id: Uuid,
    name: &str,
    gate: &GateEntry,
    payload: &Value,
) -> Result<()> {
    let extracted = extract_captures(
        payload,
        &gate.captures_spec,
        signal_id,
        SignalSource::Wakeup {
            name: name.to_string(),
        },
    )
    .map_err(
        |ExtractionError::JsonPathParseError {
             name,
             jsonpath,
             message,
         }| {
            anyhow!(
                "gate capture `{}` JSONPath `{}` failed: {}",
                name,
                jsonpath,
                message
            )
        },
    )?;
    if extracted.is_empty() {
        return Ok(());
    }
    write_captures(nats, scope_store, signal_id, gate.edge_id, &extracted).await
}

fn predicate_matches(entry: &Entry, payload: &Value) -> bool {
    match entry.predicate.as_ref() {
        Some(p) => predicate::evaluate(p, payload),
        None => true,
    }
}

async fn persist_subscriber_captures(
    repositories: &WriterRepositoryBundle,
    nats: Option<&NatsClient>,
    scope_store: Option<&dyn ScopeStore>,
    signal_id: Uuid,
    name: &str,
    entry: &Entry,
    payload: &Value,
) -> Result<()> {
    let extracted = extract_captures(
        payload,
        &entry.merged_captures,
        signal_id,
        SignalSource::Wakeup {
            name: name.to_string(),
        },
    )
    .map_err(
        |ExtractionError::JsonPathParseError {
             name,
             jsonpath,
             message,
         }| {
            anyhow!(
                "capture `{}` JSONPath `{}` failed: {}",
                name,
                jsonpath,
                message
            )
        },
    )?;

    // Skip the signal_captures write entirely when this subscriber
    // declared no captures. Otherwise an empty row lands ahead of the
    // signal_wakeups audit row in the GET fallback chain and presents
    // the wakeup as a trigger-shaped response.
    if !extracted.is_empty() {
        let row_envelopes: Vec<signal_captures::NamedEnvelope> = extracted
            .iter()
            .map(|e| signal_captures::NamedEnvelope {
                name: e.name.clone(),
                envelope: e.envelope.clone(),
            })
            .collect();
        // Wakeups resolve no live workflow version, so the version stamp stays
        // NULL on these rows.
        signal_captures::insert(
            repositories,
            signal_id,
            entry.workflow_id,
            None,
            &row_envelopes,
        )
        .await?;
        write_captures(nats, scope_store, signal_id, entry.workflow_id, &extracted).await?;
    }
    Ok(())
}

async fn write_captures(
    nats: Option<&NatsClient>,
    scope_store: Option<&dyn ScopeStore>,
    signal_id: Uuid,
    operation_owner: Uuid,
    captures: &[ExtractedEnvelope],
) -> Result<()> {
    if let Some(nats) = nats {
        write_captures_to_nats(nats, signal_id, captures).await
    } else if let Some(scope_store) = scope_store {
        let run_id = signal_id.to_string();
        write_event_captures(
            scope_store,
            DEFAULT_CTX_NAMESPACE,
            signal_id,
            &run_id,
            operation_owner,
            captures,
        )
        .await
    } else {
        Ok(())
    }
}

async fn write_captures_to_nats(
    nats: &NatsClient,
    signal_id: Uuid,
    captures: &[ExtractedEnvelope],
) -> Result<()> {
    if captures.is_empty() {
        return Ok(());
    }
    let js = jetstream::new(nats.clone());
    let bucket_name = tickr_ctx::scope::bucket_for_namespace(DEFAULT_CTX_NAMESPACE);
    let kv = match js.get_key_value(&bucket_name).await {
        Ok(kv) => kv,
        Err(_) => js
            .create_key_value(jetstream::kv::Config {
                bucket: bucket_name.clone(),
                history: 1,
                max_value_size: tickr_ctx::store::MAX_VALUE_SIZE,
                ..Default::default()
            })
            .await
            .context("create ctx bucket")?,
    };

    let signal_prefix = sanitize_segment(&signal_id.to_string());
    for cap in captures {
        let key = format!("{}/{}", signal_prefix, cap.name);
        let bytes = serde_json::to_vec(&cap.envelope).context("serialize capture envelope")?;
        kv.put(&key, bytes.into())
            .await
            .map_err(|e| anyhow!("nats kv put failed: {}", e))?;
    }
    Ok(())
}

fn sanitize_segment(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '=' | '.' | '-' => c,
            _ => '_',
        })
        .collect()
}
