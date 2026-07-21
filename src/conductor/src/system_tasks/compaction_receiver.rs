//! Archival persistence for compaction payloads — the conductor half of
//! compaction.
//!
//! `persist_compaction_projection` prepares the archive-grade projection,
//! Task-instance rows, and run enrichment, then delegates the linked
//! three-table transaction to the terminal-archive repository. Idempotent
//! redelivery replaces the same linked projection without changing its stable
//! archive time. The drain supplies one backend-neutral repository bundle for
//! both archive persistence and the terminal Patch audit.
//!
//! The relay path remains only stage + `COMPACTION_ACK`: it never performs an
//! archive repository write.

use crate::proto::{ConductorRelayMessage, EntityType};
use anyhow::{Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use tickr_migrations::archive_repository::ArchiveTerminalWorkflowInput;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_proto::archive as ap;
use uuid::Uuid;

/// Default namespace for the tickr-ctx KV bucket. Mirrors `tickr_ctx::Scope`'s
/// fallback when `TICKR_NS` is unset.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// MinIO bucket that `log_uploader` writes per-task gzip blobs to. Mirrors
/// `log_uploader::STORAGE_BUCKET` so URI derivation here matches the writer.
const LOG_STORAGE_BUCKET: &str = "tickr-logs";

/// Persist the compaction payload through the terminal-archive repository.
///
/// The selected repository owns the archive transaction and terminal audit
/// persistence. NATS-KV read failures and missing tickr-ctx scope remain
/// non-fatal.
pub async fn persist_compaction_projection(
    repository: &WriterRepositoryBundle,
    projection: &ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
    nats: Option<&NatsClient>,
) -> Result<()> {
    let wi_id = Uuid::parse_str(&projection.id).with_context(|| {
        format!(
            "archive projection carried an unparseable id `{}`",
            projection.id
        )
    })?;
    let workflow_id = Uuid::parse_str(&projection.workflow_id).with_context(|| {
        format!(
            "archive projection carried an unparseable workflow_id `{}`",
            projection.workflow_id
        )
    })?;

    // tickr-ctx scope dump (or empty on read error / missing bucket).
    let ctx_envelope_json = match nats {
        Some(client) => read_ctx_scope(client, &projection.id)
            .await
            .unwrap_or_else(|e| {
                eprintln!(
                    "compaction_receiver: tickr-ctx scope read failed for run {}: {} (empty envelope)",
                    wi_id, e
                );
                serde_json::Value::Array(Vec::new())
            }),
        None => serde_json::Value::Array(Vec::new()),
    };

    let log_uris_json = derive_log_uris(projection, workflow_id, wi_id);
    let runtime_params_json = derive_runtime_params(projection, shipped_at);

    let archived_at = shipped_at.unwrap_or_else(Utc::now);
    repository
        .archive_terminal_workflow(ArchiveTerminalWorkflowInput {
            projection,
            ctx_envelope: ctx_envelope_json,
            runtime_params: runtime_params_json,
            log_uris: log_uris_json,
            archived_at,
        })
        .await
        .context("persist the linked terminal archive")?;

    // Terminal-state cleanup for `signal_captures`. Flips `terminal_at` on
    // every row linked to this run and deletes the matching NATS KV
    // `<signal_id>/<name>` keys so the working-set cache reclaims storage
    // immediately. The SQL audit row lingers for the grace window before
    // the periodic repository cleanup removes it.
    if let Some(nats_client) = nats {
        match crate::signal_captures_cleanup::on_workflow_terminal(&repository, nats_client, wi_id)
            .await
        {
            Ok(touched) if !touched.is_empty() => {
                println!(
                    "signal_captures cleanup: marked {} row(s) terminal for run {}",
                    touched.len(),
                    wi_id
                );
            }
            Ok(_) => {} // no linked rows; cron-fired or no-captures run
            Err(e) => {
                // Non-fatal: the row's `terminal_at` may have been flipped
                // partially before the failure, in which case the sweep
                // picks it up later. A full failure leaves the row active
                // until the next terminal event for the same run (idempotent
                // re-run) or operator intervention.
                eprintln!(
                    "signal_captures cleanup: failed for run {}: {} (sweep will retry)",
                    wi_id, e
                );
            }
        }
    }

    // No gate-index sweep here: with stage-then-drain, any conductor may
    // archive this run, so an in-process sweep would only clean the
    // draining instance anyway. Gate-index freshness relies on the
    // server-authoritative relay-reconnect rebuild; entries for an
    // archived instance emit envelopes the server tolerates as no-ops
    // until the next rebuild drops them.

    Ok(())
}

/// Read all `<run_id>/<key>` entries out of the `ctx-<ns>` JetStream KV bucket
/// for this run. Returns a JSON array of `{key, envelope}` objects (envelope
/// kept as opaque JSON — the conductor doesn't interpret it, just stores).
async fn read_ctx_scope(nats: &NatsClient, run_id: &str) -> Result<serde_json::Value> {
    let js = jetstream::new(nats.clone());
    let bucket = format!("ctx-{}", sanitize_segment(DEFAULT_CTX_NAMESPACE));
    let kv = match js.get_key_value(&bucket).await {
        Ok(kv) => kv,
        Err(_) => {
            // No bucket → no scope. Not an error for archival purposes.
            return Ok(serde_json::Value::Array(Vec::new()));
        }
    };

    let prefix = format!("{}/", sanitize_segment(run_id));
    let mut entries: Vec<serde_json::Value> = Vec::new();

    // NATS KV's `keys()` returns a stream of every key in the bucket; we
    // client-side filter to this run's prefix. The bucket is per-namespace,
    // not per-run, so the cost scales with bucket size — revisit if it
    // becomes a hotspot in production.
    let mut keys = kv.keys().await?;
    while let Some(item) = keys.next().await {
        let key = match item {
            Ok(k) => k,
            Err(e) => {
                eprintln!("compaction_receiver: KV keys() yielded error: {}", e);
                continue;
            }
        };
        if !key.starts_with(&prefix) {
            continue;
        }
        match kv.get(&key).await {
            Ok(Some(bytes)) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(env_value) => entries.push(serde_json::json!({
                    "key": key,
                    "envelope": env_value,
                })),
                Err(e) => eprintln!(
                    "compaction_receiver: failed to parse Envelope JSON for key {}: {}",
                    key, e
                ),
            },
            Ok(None) => {} // tombstoned mid-scan; skip
            Err(e) => eprintln!(
                "compaction_receiver: failed to fetch value for key {}: {}",
                key, e
            ),
        }
    }

    Ok(serde_json::Value::Array(entries))
}

/// Derive the S3-uri map `{task_instance_id -> s3://<bucket>/task_logs/...}`
/// for every archived task-instance row. The row carries the task-instance id;
/// the workflow/instance ids come from the projection itself (the rows nest
/// under the instance). Path scheme mirrors the `log_uploader`'s so consumers
/// can find the gzip blob it wrote.
fn derive_log_uris(
    projection: &ap::ArchiveProjection,
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for ti in &projection.task_instances {
        let uri = format!(
            "s3://{}/task_logs/{}/{}/{}.gz",
            LOG_STORAGE_BUCKET, workflow_id, workflow_instance_id, ti.id
        );
        map.insert(ti.id.clone(), serde_json::Value::String(uri));
    }
    serde_json::Value::Object(map)
}

/// Derive workflow-level runtime params from the projection.
///
/// Intentionally narrow — the workflow-update story is unsettled, so the set of
/// "trigger-derived" fields may grow. Captures what's stable today: workflow_id,
/// instance name, scheduled_at, and the published ship time.
fn derive_runtime_params(
    projection: &ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
) -> serde_json::Value {
    serde_json::json!({
        "workflow_id": projection.workflow_id,
        "workflow_instance_name": projection.name,
        "scheduled_at": projection.scheduled_at,
        "shipped_at": shipped_at.map(|t| t.to_rfc3339()),
    })
}

/// Sanitize an identifier for use in a NATS KV key segment. Mirrors
/// `tickr_ctx::scope::sanitize_segment` exactly — kept inline rather than
/// pulled from the `tickr_ctx` crate because that crate is currently a
/// binary, not a library. If `tickr_ctx` is ever published as a lib, swap
/// this for a direct import.
fn sanitize_segment(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '=' | '.' | '-' => c,
            _ => '_',
        })
        .collect()
}

/// Build the `COMPACTION_ACK` reply for a durably staged payload. Echoes the
/// envelope's opaque correlation verbatim; the correlation is never persisted. The conductor sends
/// this back over the relay once the payload is in the NATS work queue; the
/// server's `CompactionManager` consumes it.
pub fn build_ack(workflow_instance_id: &str, correlation: &str) -> ConductorRelayMessage {
    let bytes = tickr_proto::codec::compaction::encode_ack(
        workflow_instance_id.to_string(),
        correlation.to_string(),
    );
    ConductorRelayMessage {
        entity_type: EntityType::CompactionAck as i32,
        payload: bytes,
        // Coordinator stamps the tenant from connection state (handshake), so an
        // individual outbound envelope carries no tenant of its own.
        tenant_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_segment_matches_tickr_ctx_rules() {
        // UUID-shaped input must round-trip.
        let uuid = "11111111-2222-3333-4444-555555555555";
        assert_eq!(sanitize_segment(uuid), uuid);
        // Spaces become underscores.
        assert_eq!(sanitize_segment("hello world"), "hello_world");
        // The forward slash is a separator, not a legal key char.
        assert_eq!(sanitize_segment("a/b"), "a_b");
        // Empty stays empty.
        assert_eq!(sanitize_segment(""), "");
    }
}
