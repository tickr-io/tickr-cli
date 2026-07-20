//! Log-upload step of the compaction drain. Replays one task's log batches
//! from the Log staging stream (JetStream, subject
//! `logs.<workflow_id>.<workflow_instance_id>.<task_instance_id>`),
//! gzip-concatenates them, and writes the single blob to S3-compatible
//! object storage (MinIO in dev) at the deterministic path
//! `task_logs/<wf>/<wi>/<ti>.gz` — the same key shape on both stores.
//!
//! Upload happens for every task outcome — failed attempts included; their
//! logs are the ones operators most need. Purging the subject is a separate
//! call (`purge_task_log_subject`) so the drain can order it after the
//! archive transaction commits: a re-delivered job whose subject was already
//! purged skips the upload (the blob from the first run stands) instead of
//! overwriting it with an empty one.

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self, consumer};
use async_nats::Client as NatsClient;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use opendal::layers::LoggingLayer;
use opendal::Operator;
use std::io::Write;
use std::time::Duration;
use uuid::Uuid;

/// Object-storage bucket the gzip blobs land in.

/// JetStream stream staging task-log batches. Mirrors the executor's
/// publisher and the API's logs resolver — the three must agree on stream
/// name and subject shape.
const LOG_STREAM_NAME: &str = "tickr_task_logs";

/// Subject a task instance's log batches live on. Mirrors the executor's
/// `log_subject`.
fn log_subject(workflow_id: &Uuid, workflow_instance_id: &Uuid, task_instance_id: &Uuid) -> String {
    format!(
        "logs.{}.{}.{}",
        workflow_id, workflow_instance_id, task_instance_id
    )
}

/// End-of-stream marker headers. Mirrors the executor's publisher and the
/// API's logs resolver.
const MARKER_HEADER: &str = "Tickr-Log-Marker";
const MARKER_EXIT_STATUS_HEADER: &str = "Tickr-Exit-Status";
const MARKER_EXIT_REASON_HEADER: &str = "Tickr-Exit-Reason";

/// The End-of-stream marker as read off a task's log subject.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EndOfStreamMarker {
    pub exit_status: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Extract the marker from a message's headers, if the message is one.
fn marker_from_headers(headers: Option<&async_nats::HeaderMap>) -> Option<EndOfStreamMarker> {
    let headers = headers?;
    headers.get(MARKER_HEADER)?;
    let exit_status = headers
        .get(MARKER_EXIT_STATUS_HEADER)
        .and_then(|v| v.as_str().parse::<i64>().ok())
        .unwrap_or(-1);
    let reason = headers
        .get(MARKER_EXIT_REASON_HEADER)
        .map(|v| v.as_str().to_string());
    Some(EndOfStreamMarker {
        exit_status,
        reason,
    })
}

/// Sidecar object key carrying the archived marker. The marker is structured
/// metadata, not log text, so it never lands inside the gzip blob — it gets
/// its own object at the same deterministic key shape, and its absence after
/// archival means the stream had no marker (abnormal end).
fn exit_sidecar_path(
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    task_instance_id: &Uuid,
) -> String {
    format!(
        "task_logs/{}/{}/{}.exit.json",
        workflow_id, workflow_instance_id, task_instance_id
    )
}

/// gzip-concatenate the given log batches and return the compressed bytes.
fn compress_log_batches(batches: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut compressed = Vec::new();
    let mut encoder = GzEncoder::new(&mut compressed, Compression::default());
    for batch in batches {
        encoder
            .write_all(batch)
            .context("Failed to write log data to gzip encoder")?;
    }
    encoder
        .finish()
        .context("Failed to finish gzip compression")?;
    Ok(compressed)
}

/// Build the production object-storage operator (MinIO in dev). The drain
/// constructs this once at startup; tests inject `opendal::services::Memory`
/// instead.
pub fn production_log_storage() -> Result<Operator> {
    let config = crate::config::LogStorageConfig::from_env()?;
    let builder = opendal::services::S3::default()
        .bucket(&config.bucket)
        .endpoint(&config.endpoint)
        .access_key_id(&config.access_key_id)
        .secret_access_key(&config.secret_access_key)
        .region(&config.region);

    Ok(Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish())
}

/// Replay everything currently on a task's log subject. Returns the raw
/// batch payloads in stream order plus the End-of-stream marker if one was
/// published; an absent stream or empty subject yields empty results (not
/// an error) — a task may legitimately have produced no logs, or a
/// re-delivered job may find the subject already purged.
async fn read_log_subject(
    nats: &NatsClient,
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    task_instance_id: &Uuid,
) -> Result<(Vec<Vec<u8>>, Option<EndOfStreamMarker>)> {
    let js = jetstream::new(nats.clone());
    let stream = match js.get_stream(LOG_STREAM_NAME).await {
        Ok(s) => s,
        Err(_) => {
            // No stream → no executor has published a single batch yet.
            return Ok((Vec::new(), None));
        }
    };

    let subject = log_subject(workflow_id, workflow_instance_id, task_instance_id);
    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: subject.clone(),
            deliver_policy: consumer::DeliverPolicy::All,
            ack_policy: consumer::AckPolicy::None,
            ..Default::default()
        })
        .await
        .map_err(|e| {
            anyhow!(
                "failed to create log replay consumer for {}: {}",
                subject,
                e
            )
        })?;

    let mut batches: Vec<Vec<u8>> = Vec::new();
    let mut marker: Option<EndOfStreamMarker> = None;
    loop {
        let mut fetched = consumer
            .fetch()
            .max_messages(500)
            .expires(Duration::from_millis(500))
            .messages()
            .await
            .map_err(|e| anyhow!("log replay fetch on {}: {}", subject, e))?;

        let mut got = 0usize;
        while let Some(msg) = fetched.next().await {
            let msg = msg.map_err(|e| anyhow!("log replay message on {}: {}", subject, e))?;
            // The marker is structured metadata, never log text — record it
            // and keep its (empty) payload out of the batch list.
            if let Some(m) = marker_from_headers(msg.headers.as_ref()) {
                marker = Some(m);
            } else {
                batches.push(msg.payload.to_vec());
            }
            got += 1;
        }
        if got < 500 {
            break;
        }
    }
    Ok((batches, marker))
}

/// Upload one task's staged logs (and End-of-stream marker sidecar) to
/// object storage. Returns `true` when anything was written, `false` when
/// the subject was empty (nothing to upload — never overwrite an existing
/// blob or sidecar with emptiness).
pub async fn upload_task_logs(
    nats: &NatsClient,
    storage: &Operator,
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    task_instance_id: &Uuid,
) -> Result<bool> {
    let (batches, marker) =
        read_log_subject(nats, workflow_id, workflow_instance_id, task_instance_id).await?;
    if batches.is_empty() && marker.is_none() {
        return Ok(false);
    }

    if !batches.is_empty() {
        let compressed_data = compress_log_batches(&batches)?;
        println!(
            "Compressed {} log batches ({} bytes) for task {}",
            batches.len(),
            compressed_data.len(),
            task_instance_id
        );

        // Same key shape as the staging subject, mirrored into object storage.
        let storage_path = format!(
            "task_logs/{}/{}/{}.gz",
            workflow_id, workflow_instance_id, task_instance_id
        );

        // Same-key overwrite keeps the upload idempotent under job redelivery.
        // Preserve OpenDAL error detail so the underlying S3 status
        // (AccessDenied, NoSuchBucket, etc.) reaches the log.
        storage
            .write(&storage_path, compressed_data)
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to write compressed logs to {}: {:#}",
                    storage_path,
                    e
                )
            })?;

        println!("Stored compressed logs at: {}", storage_path);
    }

    // The marker (when present) survives archival as a sidecar object. Its
    // absence after archival is itself the signal: terminal task + no
    // sidecar ⇒ the stream ended without a marker (abnormal end).
    if let Some(marker) = marker {
        let sidecar_path = exit_sidecar_path(workflow_id, workflow_instance_id, task_instance_id);
        let body = serde_json::to_vec(&marker)?;
        storage.write(&sidecar_path, body).await.map_err(|e| {
            anyhow!(
                "Failed to write End-of-stream sidecar to {}: {:#}",
                sidecar_path,
                e
            )
        })?;
    }

    Ok(true)
}

/// Purge a task's log subject from the staging stream. Idempotent — purging
/// an empty or never-written subject succeeds.
pub async fn purge_task_log_subject(
    nats: &NatsClient,
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    task_instance_id: &Uuid,
) -> Result<()> {
    let js = jetstream::new(nats.clone());
    let stream = match js.get_stream(LOG_STREAM_NAME).await {
        Ok(s) => s,
        Err(_) => return Ok(()), // no stream → nothing to purge
    };
    let subject = log_subject(workflow_id, workflow_instance_id, task_instance_id);
    stream
        .purge()
        .filter(&subject)
        .await
        .map_err(|e| anyhow!("failed to purge log subject {}: {}", subject, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    fn gunzip(bytes: &[u8]) -> Vec<u8> {
        let mut decoder = GzDecoder::new(bytes);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("gzip decode");
        out
    }

    #[test]
    fn compress_log_batches_round_trips_through_gunzip() {
        let batches: Vec<Vec<u8>> = vec![
            b"first log batch\n".to_vec(),
            b"second log batch\n".to_vec(),
            b"third log batch\n".to_vec(),
        ];
        let expected: Vec<u8> = batches.iter().flatten().copied().collect();

        let compressed = compress_log_batches(&batches).expect("compression should succeed");
        let decoded = gunzip(&compressed);
        assert_eq!(
            decoded, expected,
            "decompressed bytes must equal concatenation of inputs"
        );
    }

    #[test]
    fn compress_log_batches_empty_input_yields_valid_empty_gzip_stream() {
        // A gzip stream with no payload is still valid gzip — it has headers + trailer.
        let compressed =
            compress_log_batches(&[]).expect("compression of empty batches must succeed");
        assert!(
            !compressed.is_empty(),
            "even empty input produces gzip framing bytes"
        );
        let decoded = gunzip(&compressed);
        assert!(decoded.is_empty(), "decoded payload must be empty");
    }

    #[test]
    fn log_subject_matches_staging_layout() {
        let wf = Uuid::nil();
        let wi = Uuid::nil();
        let ti = Uuid::nil();
        assert_eq!(
            log_subject(&wf, &wi, &ti),
            "logs.00000000-0000-0000-0000-000000000000.00000000-0000-0000-0000-000000000000.00000000-0000-0000-0000-000000000000"
        );
    }
}
