//! Shared trigger-ingress pipeline.
//!
//! Two ingress transports — the HTTP `/api/workflows/{id}/trigger` route and
//! the NATS `tickr.external.signals` subject — both translate a producer's
//! intent to start a workflow into a wire `Signal::Trigger`. They differ at
//! the transport edge (request body shape, idempotency-key sourcing, relay-
//! forward strategy) but share every step in between: workflow-definition
//! lookup, inputs-vs-declared-captures check, signal_id minting, idempotency
//! cache consultation, JSONPath captures extraction, repository write, NATS
//! KV write, and wire `Signal` construction.
//!
//! This module is the shared middle layer. Callers build a `TriggerRequest`
//! from their transport, invoke `process_trigger`, and adapt the resulting
//! `TriggerOutcome` / `TriggerError` to their response shape. Relay
//! forwarding stays with the caller because the two transports have
//! genuinely different send strategies: HTTP blocks on success/error, NATS
//! NAKs on outbound saturation so JetStream redelivers when the relay
//! drains. The pipeline produces the `Signal` value but doesn't emit it.

use anyhow::{anyhow, Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tickr_ctx::envelope::SignalSource;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_proto::signal as sp;
use tickr_proto::workflow as wf;
use uuid::Uuid;

use crate::canonical_json;
use crate::captures_extractor::{
    extract_captures, ExtractionError, NamedEnvelope as ExtractedEnvelope,
};
use crate::idempotency;
use crate::signal_captures;

/// Per-tenant tickr-ctx bucket namespace. Mirrors the executor's resolver
/// default so manual triggers land in the same `ctx-<ns>` bucket downstream
/// tasks read from.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// Producer intent the transport-specific caller assembles. The hashing
/// payload is supplied by the caller because the HTTP path and the NATS
/// path deliberately diverge on what counts as "the same logical request":
///
/// - HTTP keys only `inputs` (workflow_id is in the URL path; two requests
///   with the same body but different URLs are different routes by
///   convention).
/// - NATS keys `{workflow_id, scheduled_at, inputs}` (workflow_id is
///   in-band, so two envelopes with the same idempotency key but different
///   workflows are different logical requests and must surface as a
///   Collision rather than a Dedup).
pub struct TriggerRequest {
    pub workflow_id: Uuid,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub inputs: Option<Value>,
    /// `None` when the producer didn't supply a key (HTTP without the
    /// `Idempotency-Key` header). Without a key, the pipeline skips the
    /// cache entirely — every call is a Fresh outcome.
    pub idempotency_key: Option<String>,
    /// Producer source attribution stamped on each capture envelope.
    pub source: SignalSource,
    /// Caller-chosen canonical-JSON value to hash for the idempotency
    /// cache. The same hash space is used to compare retries; the pipeline
    /// doesn't second-guess the shape.
    pub hash_payload: Value,
    /// Optional Run name for the materialized instance. Carried onto the wire
    /// `Signal::Trigger` only; deliberately NOT folded into `hash_payload`, so
    /// a retry that differs only in name still dedups to the first instance.
    pub name: Option<String>,
}

/// Outcome of `process_trigger`. The pipeline applies all side effects
/// (SQL repository + NATS writes, cache insertion) only on `Fresh`; on `Dedup`
/// and `Conflict` it short-circuits before any persistence mutation. The
/// caller emits the wire `Signal` (when present) over its own relay path.
pub enum TriggerOutcome {
    /// First arrival for this `(idempotency_key, hash_payload)` pair (or
    /// no idempotency key at all). The pipeline has written captures and
    /// constructed the wire `Signal`; the caller forwards it.
    Fresh { signal_id: Uuid, signal: sp::Signal },
    /// Same idempotency key, byte-identical canonical-JSON payload — an
    /// idempotent retry. No work happened; the cached `signal_id` is what
    /// the original arrival produced. Caller surfaces this to its
    /// transport (HTTP returns 200 + `deduplicated: true`; NATS acks the
    /// inbound message and increments the dedup counter).
    Deduplicated { original_signal_id: Uuid },
    /// Same idempotency key, different canonical-JSON payload — a
    /// publisher bug. No work happened; the cached entry stays. Caller
    /// surfaces this to its transport (HTTP returns 409; NATS acks and
    /// increments the collision counter).
    Conflict {
        original_signal_id: Uuid,
        original_hash: String,
        your_hash: String,
    },
}

/// Failure modes the pipeline distinguishes for the caller. Each variant
/// maps onto a distinct transport-level response: HTTP returns 400 / 404 /
/// 500 by category; NATS logs the structured reason and acks (drops) the
/// inbound message.
#[derive(Debug, thiserror::Error)]
pub enum TriggerError {
    #[error("workflow {workflow_id} not found")]
    WorkflowNotFound { workflow_id: Uuid },
    #[error(
        "workflow declares no captures; the request carried `inputs` but the workflow has no extraction declarations"
    )]
    InputsProvidedButNoCaptures,
    #[error("capture `{name}` JSONPath `{jsonpath}` failed: {message}")]
    CapturesExtractionFailed {
        name: String,
        jsonpath: String,
        message: String,
    },
    #[error("workflow definition lookup: {0}")]
    WorkflowLookup(#[source] anyhow::Error),
    #[error("idempotency cache: {0}")]
    Idempotency(#[source] anyhow::Error),
    #[error("captures archive: {0}")]
    RepositoryWrite(#[source] anyhow::Error),
    #[error("captures cache: {0}")]
    NatsWrite(#[source] anyhow::Error),
}

/// Run the shared trigger pipeline. On `TriggerOutcome::Fresh`, captures
/// have been written to the SQL repository and NATS, the idempotency cache has been
/// populated (when a key was supplied), and the `Signal` is ready for the
/// caller to forward over its preferred relay path.
pub async fn process_trigger(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    req: TriggerRequest,
) -> Result<TriggerOutcome, TriggerError> {
    process_trigger_with_working_set(repositories, Some(nats), req).await
}

/// Tickr Lite Trigger ingress keeps the SQL capture archive and published
/// Signal shape while omitting the profile-disabled NATS working-set cache.
pub async fn process_trigger_local(
    repositories: &WriterRepositoryBundle,
    req: TriggerRequest,
) -> Result<TriggerOutcome, TriggerError> {
    process_trigger_with_working_set(repositories, None, req).await
}

async fn process_trigger_with_working_set(
    repositories: &WriterRepositoryBundle,
    nats: Option<&NatsClient>,
    req: TriggerRequest,
) -> Result<TriggerOutcome, TriggerError> {
    let inputs_provided = req.inputs.is_some();
    let payload = req.inputs.unwrap_or(Value::Object(Default::default()));
    let input_hash = canonical_json::hash(Some(&req.hash_payload));

    // 1. Workflow definition lookup, resolved to the live version — the
    //    same version the server materialises and runs — so a slug
    //    re-registered with changed capture declarations extracts under the
    //    new declarations on the very next trigger. A missing row is a 404 /
    //    structured error; an I/O failure is a 5xx.
    let workflow = load_live_workflow_definition(repositories, req.workflow_id)
        .await
        .map_err(TriggerError::WorkflowLookup)?
        .ok_or(TriggerError::WorkflowNotFound {
            workflow_id: req.workflow_id,
        })?;

    // Capture the live version the resolver landed on so the persisted capture
    // row can stamp it — a future version/Event-variable mismatch then shows up
    // in data, not silently in a wrong run.
    let workflow_version = workflow.version;
    let declared_captures = &workflow.captures;

    // 2. Inputs vs declared captures. Silently dropping inputs is the
    //    worst failure mode for retried producers; refuse loudly.
    if inputs_provided && declared_captures.is_empty() {
        return Err(TriggerError::InputsProvidedButNoCaptures);
    }

    // 3. Mint signal_id up front so it threads through every persistence
    //    site (SQL archive, NATS keys, envelope lineage, wire signal).
    let signal_id = Uuid::new_v4();

    // 4. Idempotency cache. Only consulted when a producer-supplied key
    //    is present. The check_or_insert is atomic against concurrent
    //    retries on the same key.
    if let (Some(key), Some(nats)) = (req.idempotency_key.as_deref(), nats) {
        let bucket = idempotency::open_bucket(nats)
            .await
            .map_err(TriggerError::Idempotency)?;
        let outcome = idempotency::check_or_insert(&bucket, key, signal_id, &input_hash)
            .await
            .map_err(TriggerError::Idempotency)?;
        match outcome {
            idempotency::CacheOutcome::Fresh => {}
            idempotency::CacheOutcome::DeduplicatedSameHash { original_signal_id } => {
                return Ok(TriggerOutcome::Deduplicated { original_signal_id });
            }
            idempotency::CacheOutcome::ConflictDifferentHash {
                original_signal_id,
                original_hash,
            } => {
                return Ok(TriggerOutcome::Conflict {
                    original_signal_id,
                    original_hash,
                    your_hash: hex::encode(input_hash),
                });
            }
        }
    }

    // 5. JSONPath captures extraction. Registration-time validation
    //    rejected malformed paths; an error here is reachable only via a
    //    corrupted persisted workflow definition.
    let extracted = extract_captures(&payload, declared_captures, signal_id, req.source.clone())
        .map_err(
            |ExtractionError::JsonPathParseError {
                 name,
                 jsonpath,
                 message,
             }| {
                TriggerError::CapturesExtractionFailed {
                    name,
                    jsonpath,
                    message,
                }
            },
        )?;

    // 6. Repository write first — durable source of truth. NATS comes after
    //    so a later read-side miss can rehydrate from the SQL archive.
    let row_envelopes: Vec<signal_captures::NamedEnvelope> = extracted
        .iter()
        .map(|e| signal_captures::NamedEnvelope {
            name: e.name.clone(),
            envelope: e.envelope.clone(),
        })
        .collect();
    signal_captures::insert(
        repositories,
        signal_id,
        req.workflow_id,
        Some(workflow_version),
        &row_envelopes,
    )
    .await
    .map_err(TriggerError::RepositoryWrite)?;

    // 6b. Back-fill the (signal_id → run_id) linkage for a future-dated
    //     trigger so the signals read-path can surface the scheduled
    //     instance id *while it is still pending* — an operator needs a
    //     target to call the run back before it fires. Without this the
    //     linkage lands only at fire (off the first `TaskQueueItem`), so a
    //     pending scheduled run reads as absent. Only an explicit
    //     `scheduled_at` is predictable — a fire-now trigger resolves to the
    //     server's `now()` and materializes at once, so early surfacing is
    //     moot there. Best-effort: a failure only defers surfacing to the
    //     fire-time back-fill (both paths compute the same deterministic id;
    //     `mark_materialized` is idempotent under its `IS NULL` guard).
    if let Some(scheduled_at) = req.scheduled_at {
        if let Err(e) = crate::instance_creation_linkage::backfill_pending_schedule_linkage(
            repositories,
            signal_id,
            req.workflow_id,
            scheduled_at,
        )
        .await
        {
            eprintln!(
                "pending-schedule linkage back-fill failed for signal {}: {} (the fire-time back-fill will retry)",
                signal_id, e
            );
        }
    }

    if let Some(nats) = nats {
        write_captures_to_nats(nats, signal_id, &extracted)
            .await
            .map_err(TriggerError::NatsWrite)?;
    }

    // 8. Construct the wire Signal. Producer-supplied key carries onto the
    //    wire so server-side audit logs correlate retries. The transport
    //    source rides alongside so the server stamps Manual vs External
    //    provenance on the resulting instance.
    use sp::trigger_source::Source;
    let wire_source = match &req.source {
        SignalSource::Manual => Source::Manual(sp::trigger_source::Manual {}),
        SignalSource::ExternalNats { subject } => Source::External(sp::trigger_source::External {
            subject: subject.clone(),
        }),
        // Wakeup-as-source is not a Trigger-shaped ingress today; fall
        // back to Manual rather than fabricate a third wire shape.
        SignalSource::Wakeup { .. } => Source::Manual(sp::trigger_source::Manual {}),
    };
    let signal = sp::Signal {
        signal_id: signal_id.to_string(),
        idempotency_key: req.idempotency_key,
        variant: Some(sp::signal::Variant::Trigger(sp::Trigger {
            workflow_id: req.workflow_id.to_string(),
            scheduled_at: req.scheduled_at.map(|dt| dt.to_rfc3339()),
            source: Some(sp::TriggerSource {
                source: Some(wire_source),
            }),
            name: req.name,
            // Ordinary triggers carry no replay seed; the seed is minted only
            // on the dedicated replay ingress, never from a client trigger.
            replay: None,
        })),
    };

    Ok(TriggerOutcome::Fresh { signal_id, signal })
}

/// Load the **live** workflow definition — the one canonical "the version we
/// run" resolver for the conductor's trigger surface.
///
/// A re-registered slug keeps a single `workflows.id` and accumulates one
/// immutable row per version, so a version-blind `WHERE id = $1` load reads an
/// arbitrary row's capture declarations and silently misroutes every trigger.
/// This resolves the latest **live** (`Ready`/`Submitted`) version by insertion
/// order — the exact version the server materialises and runs — mirroring the
/// read path's `latest_live` CTE.
///
/// "Latest" means latest-*live*, not `MAX(version)`: a newer version still
/// `Building` outranks the live `Ready` one under a naive `ORDER BY version
/// DESC`, which would extract mid-build declarations against the server's live
/// graph — the same desync, triggered by a build in flight. Filtering to
/// `Ready`/`Submitted` and ordering by `inserted_at` excludes it. Unlike the
/// API's Default-version resolver there is deliberately no latest-inserted
/// fallback: if no version is live the server holds no runnable graph for the
/// id and there is nothing to materialise, so this returns `None`.
///
/// Both trigger ingresses — the HTTP command-bus path and the NATS
/// external-signal path — reach this through `process_trigger`, so a single
/// resolver corrects both.
pub async fn load_live_workflow_definition(
    repositories: &WriterRepositoryBundle,
    workflow_id: Uuid,
) -> Result<Option<wf::WorkflowDefinition>> {
    repositories
        .live_workflow_definition(workflow_id)
        .await
        .map_err(anyhow::Error::new)
}

/// Mirror the extracted envelopes to the per-tenant `ctx-<ns>` NATS KV
/// bucket keyed by `<signal_id>/<name>`. The bucket is the working-set
/// cache; the selected SQL repository is the durable archive.
async fn write_captures_to_nats(
    nats: &NatsClient,
    signal_id: Uuid,
    captures: &[ExtractedEnvelope],
) -> Result<()> {
    if captures.is_empty() {
        return Ok(());
    }

    let js = jetstream::new(nats.clone());
    let bucket_name = format!("ctx-{}", sanitize_segment(DEFAULT_CTX_NAMESPACE));
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

/// NATS-KV-safe character set. Mirrors `tickr_ctx::scope::sanitize_segment`
/// inline so the conductor's two trigger ingresses can't drift apart.
fn sanitize_segment(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '=' | '.' | '-' => c,
            _ => '_',
        })
        .collect()
}
