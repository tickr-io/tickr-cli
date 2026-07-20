//! Archival persistence for compaction payloads — the conductor half of
//! compaction.
//!
//! `persist_compaction_projection` writes the terminal instance's archive-grade
//! **projection** + its task-instance rows + the run's enrichment context
//! (tickr-ctx scope dump, log URIs, workflow-level runtime params) into the
//! conductor's PostgreSQL archive in a single transaction, using `ON CONFLICT
//! DO UPDATE` for idempotent retries. The stored `instance` blob is the archive
//! projection itself. It is invoked by the compaction drain
//! (`compaction_drain.rs`), which consumes
//! jobs the relay handler staged in the NATS work queue; the relay path itself
//! never writes Postgres — it stages, ACKs (`build_ack`), and moves on.
//!
//! Writes three FK-linked tables: `workflow_instances`, `task_instances`, and
//! `workflow_run_info`.

use crate::proto::{ConductorRelayMessage, EntityType};
use anyhow::{Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use sqlx::PgPool;
use std::collections::HashSet;
use tickr_proto::archive as ap;
use uuid::Uuid;

/// Default namespace for the tickr-ctx KV bucket. Mirrors `tickr_ctx::Scope`'s
/// fallback when `TICKR_NS` is unset.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// MinIO bucket that `log_uploader` writes per-task gzip blobs to. Mirrors
/// `log_uploader::STORAGE_BUCKET` so URI derivation here matches the writer.
const LOG_STORAGE_BUCKET: &str = "tickr-logs";

/// Persist the compaction payload into the conductor's three-table Postgres
/// archive in a single transaction.
///
/// Idempotent on retry via `INSERT ... ON CONFLICT DO UPDATE`. NATS-KV read
/// failures and missing tickr-ctx scope are non-fatal — the row lands with an
/// empty `ctx_envelope`. Does not probe object storage at archival time; the
/// log_uris column carries deterministically-derived paths.
pub async fn persist_compaction_projection(
    pool: &PgPool,
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
    let scheduled_at = parse_opt_rfc3339(projection.scheduled_at.as_deref())?;
    // Store the archive projection directly.
    let instance_json = serde_json::to_value(projection)?;

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

    let mut tx = pool.begin().await?;

    // No `node_id` column: the archive carries no cluster-node provenance (the
    // tenant reads this database directly).
    sqlx::query(
        r#"
        INSERT INTO workflow_instances
            (id, workflow_id, name, state, scheduled_at, instance)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (id) DO UPDATE SET
            state        = EXCLUDED.state,
            scheduled_at = EXCLUDED.scheduled_at,
            instance     = EXCLUDED.instance,
            archived_at  = now()
        "#,
    )
    .bind(wi_id)
    .bind(workflow_id)
    .bind(&projection.name)
    .bind(&projection.state)
    .bind(scheduled_at)
    .bind(&instance_json)
    .execute(&mut *tx)
    .await?;

    for ti in &projection.task_instances {
        let ti_id = Uuid::parse_str(&ti.id).with_context(|| {
            format!(
                "archived task-instance carried an unparseable id `{}`",
                ti.id
            )
        })?;
        let task_id = Uuid::parse_str(&ti.task_id).with_context(|| {
            format!(
                "archived task-instance carried an unparseable task_id `{}`",
                ti.task_id
            )
        })?;
        // The stored blob is the projection's task-instance row. `attempt` is
        // also indexed as a column so the UI can order attempt history without
        // unpacking JSONB.
        let ti_json = serde_json::to_value(ti)?;
        sqlx::query(
            r#"
            INSERT INTO task_instances
                (id, workflow_instance_id, workflow_id, task_id,
                 name, state, task_instance, attempt)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO UPDATE SET
                state         = EXCLUDED.state,
                task_instance = EXCLUDED.task_instance,
                attempt       = EXCLUDED.attempt,
                archived_at   = now()
            "#,
        )
        .bind(ti_id)
        .bind(wi_id)
        .bind(workflow_id)
        .bind(task_id)
        .bind(&ti.name)
        .bind(&ti.state)
        .bind(&ti_json)
        .bind(ti.attempt as i32)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"
        INSERT INTO workflow_run_info
            (workflow_instance_id, ctx_envelope, runtime_params, log_uris)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (workflow_instance_id) DO UPDATE SET
            ctx_envelope    = EXCLUDED.ctx_envelope,
            runtime_params  = EXCLUDED.runtime_params,
            log_uris        = EXCLUDED.log_uris,
            enriched_at     = now()
        "#,
    )
    .bind(wi_id)
    .bind(ctx_envelope_json)
    .bind(runtime_params_json)
    .bind(log_uris_json)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // Terminal-state cleanup for `signal_captures`. Flips `terminal_at` on
    // every row linked to this run and deletes the matching NATS KV
    // `<signal_id>/<name>` keys so the working-set cache reclaims storage
    // immediately. The Postgres row lingers for the grace window so an
    // operator can audit the originating signal's captures post-mortem; a
    // periodic sweep deletes it after the window elapses.
    if let Some(nats_client) = nats {
        match crate::signal_captures_cleanup::on_workflow_terminal(pool, nats_client, wi_id).await {
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

/// Terminal-time patch settlement audit — the durability net for patches that
/// slipped past the in-flight settlement/re-drive loop.
///
/// Reconciles the durable patch ledger (`workflow_patches` rows for this
/// instance) against the terminal instance's applied-patch log (the set of
/// `patch_key`s in `WorkflowInstance::applied_patches`), and writes a loud,
/// durable discrepancy record for anything that did not settle cleanly. This
/// is a terminal-time AUDIT, not a re-drive: it never mutates or re-applies a
/// patch — that is `patch_pipeline`'s in-flight job. It is the
/// belt-and-suspenders net for a patch that left no terminal-time trace.
///
/// A ledger row is a discrepancy when:
///  - it is still **unsettled** (`Validating`/`Building`/`Submitted`) at
///    terminal time — it never reached an outcome; or
///  - it is `Applied` but its `patch_key` is **absent from the applied-patch
///    log** — the two durable records disagree.
///
/// `Rejected`/`BuildFailed` rows settled with a recorded outcome and are
/// legitimately absent from the applied-patch log, so they are never
/// discrepancies; an `Applied` row present in the log settled cleanly.
///
/// Idempotent: one row per `(workflow_instance_id, patch_key)`, upserted, so
/// compaction redelivery converges. Returns the number of discrepancies
/// recorded.
pub async fn audit_patch_settlement(
    pool: &PgPool,
    workflow_instance_id: Uuid,
    applied_patch_keys: &HashSet<Uuid>,
) -> Result<usize> {
    let ledger: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT patch_key, status FROM workflow_patches WHERE workflow_instance_id = $1",
    )
    .bind(workflow_instance_id)
    .fetch_all(pool)
    .await?;

    let mut recorded = 0usize;
    for (patch_key, status) in ledger {
        let detail = match status.as_str() {
            "Validating" | "Building" | "Submitted" => Some(format!(
                "patch unsettled at terminal compaction (ledger status {status}); \
                 never reached an outcome"
            )),
            "Applied" if !applied_patch_keys.contains(&patch_key) => Some(
                "patch ledger records Applied but patch_key is absent from the \
                 terminal applied-patch log"
                    .to_string(),
            ),
            // Rejected / BuildFailed, or Applied-and-in-log: settled cleanly.
            _ => None,
        };
        let Some(detail) = detail else {
            continue;
        };

        // Loud enough that an operator notices — in the spirit of the other
        // compaction settlement failures on this path.
        eprintln!(
            "patch_settlement_audit: DISCREPANCY instance={} patch_key={} status={}: {}",
            workflow_instance_id, patch_key, status, detail
        );

        sqlx::query(
            r#"
            INSERT INTO workflow_patch_discrepancies
                (workflow_instance_id, patch_key, ledger_status, detail)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (workflow_instance_id, patch_key) DO UPDATE SET
                ledger_status = EXCLUDED.ledger_status,
                detail        = EXCLUDED.detail,
                detected_at   = now()
            "#,
        )
        .bind(workflow_instance_id)
        .bind(patch_key)
        .bind(&status)
        .bind(&detail)
        .execute(pool)
        .await?;
        recorded += 1;
    }

    Ok(recorded)
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

/// Parse an optional RFC3339 timestamp (the projection carries times as strings)
/// back into a `DateTime<Utc>` for a `timestamptz` column bind.
fn parse_opt_rfc3339(s: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    match s {
        Some(s) => Ok(Some(
            DateTime::parse_from_rfc3339(s)
                .with_context(|| {
                    format!("archive projection carried an unparseable timestamp `{s}`")
                })?
                .with_timezone(&Utc),
        )),
        None => Ok(None),
    }
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
