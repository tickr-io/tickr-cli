//! Task-log dispatcher. Resolves a task's logs from whichever store currently
//! holds them: MinIO (archived — gzipped blob written by the compaction
//! drain's log-upload step) or the Log staging stream (live — raw batches
//! the executor published).
//!
//! Order: try MinIO first, then the stream, then 404. The two stores are
//! mutually exclusive at steady state because the compaction drain writes
//! the gzip blob and then purges the task's log subject — so probing MinIO
//! first lands on the first call for any past-run browse (the dominant UI
//! traffic pattern), at the cost of one extra stat against a
//! not-yet-existing key for the live case.
//!
//! Returns decompressed bytes regardless of source so the HTTP handler can
//! shape a single response type that the UI consumes uniformly.

use flate2::read::GzDecoder;
use opendal::Operator;
use std::io::Read;
use std::sync::Arc;
use thiserror::Error;
use tickr_executor::log_stream::{LogStreamProvider, LogStreamRoute};
use tickr_proto::coord::log_stream::{LogTerminal, ReplayedLogRecord};
use uuid::Uuid;

// The bucket name is applied to the `opendal::Operator` at construction time
// (in `start_http_server`), so this constant is unreferenced here — kept so the
// copy stays diff-identical to the conductor's module during the overlap window.
#[allow(dead_code)]
const MINIO_BUCKET: &str = "tickr-logs";

/// The End-of-stream marker, however it was found — message headers on the
/// live stream, or the archived `.exit.json` sidecar. Shape mirrors the
/// conductor's sidecar writer.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct EndOfStreamMarker {
    pub exit_status: i64,
    #[serde(default)]
    pub reason: Option<String>,
}

/// A task's resolved logs: the raw content plus its durable terminal marker.
/// Controlled End-of-stream carries the reported exit; abnormal closure carries
/// an explicit reason rather than masquerading as a successful exit.
#[derive(Debug)]
pub struct TaskLogs {
    pub content: Vec<u8>,
    pub marker: Option<EndOfStreamMarker>,
}

/// One staged log batch with its stream sequence — the Log cursor unit. The
/// sequence is JetStream's, so a client polling `after_seq` can never see a
/// batch twice or miss one.
#[derive(Debug)]
pub struct LogBatch {
    pub seq: u64,
    pub bytes: Vec<u8>,
}

/// A page of staged batches plus the marker if it was within the read range.
#[derive(Debug)]
pub struct LogBatchPage {
    pub batches: Vec<LogBatch>,
    pub marker: Option<EndOfStreamMarker>,
    /// Tail reads only: batches exist on the subject before the first one
    /// returned — drives the "load earlier" affordance.
    pub has_earlier: bool,
}

#[derive(Debug, Error)]
pub enum LogsError {
    /// No logs in either store. UI surfaces as 404.
    #[error("logs not found for task")]
    NotFound,
    /// MinIO returned an unexpected error (not "key absent"). Likely
    /// infrastructure trouble; HTTP layer maps to 5xx.
    #[error("MinIO error: {0}")]
    Minio(String),
    /// Formation-selected LogStaging replay failure. HTTP layer maps to 5xx.
    #[error("Log staging error: {0}")]
    Staging(String),
    /// MinIO returned a gzip blob that did not decompress cleanly. Indicates
    /// log corruption at the storage layer; HTTP layer maps to 5xx.
    #[error("gzip decode error: {0}")]
    GzipDecode(String),
    /// Local final/staging journal access failed.
    #[error("local Log error: {0}")]
    Local(String),
}

/// Constructs the canonical MinIO object key for a task's gzipped log blob.
/// Mirrors the path `log_uploader` writes to so the two stay in sync.
pub fn minio_object_key(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
) -> String {
    format!(
        "task_logs/{}/{}/{}.gz",
        workflow_id, workflow_instance_id, task_instance_id
    )
}

/// Sidecar object key the conductor's log-upload step writes the archived
/// End-of-stream marker to. Mirrors `exit_sidecar_path` there.
pub fn exit_sidecar_key(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
) -> String {
    format!(
        "task_logs/{}/{}/{}.exit.json",
        workflow_id, workflow_instance_id, task_instance_id
    )
}

fn marker_from_terminal(terminal: &LogTerminal) -> EndOfStreamMarker {
    match terminal {
        LogTerminal::EndOfStream { exit } => match exit {
            tickr_proto::coord::log_stream::LogExit::Status(status) => EndOfStreamMarker {
                exit_status: i64::from(*status),
                reason: None,
            },
            tickr_proto::coord::log_stream::LogExit::NoStatus => EndOfStreamMarker {
                exit_status: -1,
                reason: Some("terminated without exit status".to_owned()),
            },
            tickr_proto::coord::log_stream::LogExit::Error(reason) => EndOfStreamMarker {
                exit_status: -1,
                reason: Some(reason.clone()),
            },
        },
        LogTerminal::AbnormalClosure { .. } => EndOfStreamMarker {
            exit_status: -1,
            reason: Some("Executor closed without controlled End-of-stream".to_owned()),
        },
    }
}

/// Local Log query role used by Tickr Lite's same-process API component.
#[async_trait::async_trait]
pub trait LocalTaskLogStore: Send + Sync {
    async fn fetch_task_logs(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
    ) -> Result<TaskLogs, LogsError>;

    async fn fetch_batches_after(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        after_seq: u64,
    ) -> Result<LogBatchPage, LogsError>;

    async fn fetch_tail(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        tail: usize,
        before_seq: Option<u64>,
    ) -> Result<LogBatchPage, LogsError>;
}

#[derive(Clone)]
enum LogsBackend {
    Distributed {
        minio: Operator,
        log_streams: Arc<dyn LogStreamProvider>,
    },
    Local(Arc<dyn LocalTaskLogStore>),
}

/// Dispatcher with the selected formation's Log query role.
#[derive(Clone)]
pub struct LogsResolver {
    backend: LogsBackend,
}

impl LogsResolver {
    pub fn new(minio: Operator, log_streams: Arc<dyn LogStreamProvider>) -> Self {
        Self {
            backend: LogsBackend::Distributed { minio, log_streams },
        }
    }

    pub fn local(store: std::sync::Arc<dyn LocalTaskLogStore>) -> Self {
        Self {
            backend: LogsBackend::Local(store),
        }
    }

    /// Tries MinIO; on `NotFound`, falls through to the Log staging stream;
    /// on both empty, returns `LogsError::NotFound`. Returns decompressed
    /// log content plus the End-of-stream marker if the stream was closed
    /// cleanly — identically sourced from message headers (live) or the
    /// archived sidecar (MinIO), so callers never see two marker shapes.
    pub async fn fetch_task_logs(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
    ) -> Result<TaskLogs, LogsError> {
        if let LogsBackend::Local(store) = &self.backend {
            return store
                .fetch_task_logs(workflow_id, workflow_instance_id, task_instance_id)
                .await;
        }
        let LogsBackend::Distributed { minio, log_streams } = &self.backend else {
            unreachable!()
        };
        // 1. MinIO probe at the deterministic path.
        let key = minio_object_key(workflow_id, workflow_instance_id, task_instance_id);
        match minio.read(&key).await {
            Ok(gzipped) => {
                let content = decode_gzip(&gzipped.to_vec())?;
                let marker = self
                    .read_exit_sidecar(workflow_id, workflow_instance_id, task_instance_id)
                    .await?;
                return Ok(TaskLogs { content, marker });
            }
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                // No blob. A task may have archived a marker without ever
                // logging a line — probe the sidecar before falling through
                // so such tasks don't read as "no logs anywhere".
                if let Some(marker) = self
                    .read_exit_sidecar(workflow_id, workflow_instance_id, task_instance_id)
                    .await?
                {
                    return Ok(TaskLogs {
                        content: Vec::new(),
                        marker: Some(marker),
                    });
                }
            }
            Err(e) => {
                return Err(LogsError::Minio(format!("read {}: {}", key, e)));
            }
        }

        // 2. LogStream fallback — replay committed coverage in identity order.
        let (batches, marker) = read_stream_page(
            log_streams.as_ref(),
            log_stream_route(workflow_id, workflow_instance_id, task_instance_id),
            None,
        )
        .await?;
        if batches.is_empty() && marker.is_none() {
            return Err(LogsError::NotFound);
        }
        Ok(TaskLogs {
            content: batches.into_iter().flat_map(|b| b.bytes).collect(),
            marker,
        })
    }

    /// Incremental read — the Log cursor. Returns only batches with a stream
    /// sequence strictly greater than `after_seq`, plus the marker if it lies
    /// past the cursor (it is published last, so the poll that crosses it is
    /// the tail's natural end). Serves from the stream only: a terminal task
    /// whose subject was already archived returns an empty page, and the
    /// client's no-cursor fetch takes over.
    pub async fn fetch_batches_after(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        after_seq: u64,
    ) -> Result<LogBatchPage, LogsError> {
        if let LogsBackend::Local(store) = &self.backend {
            return store
                .fetch_batches_after(
                    workflow_id,
                    workflow_instance_id,
                    task_instance_id,
                    after_seq,
                )
                .await;
        }
        let LogsBackend::Distributed { log_streams, .. } = &self.backend else {
            unreachable!()
        };
        let (batches, marker) = read_stream_page(
            log_streams.as_ref(),
            log_stream_route(workflow_id, workflow_instance_id, task_instance_id),
            Some(after_seq.saturating_add(1)),
        )
        .await?;
        Ok(LogBatchPage {
            batches,
            marker,
            has_earlier: false,
        })
    }

    /// Tail read — the constant-size first paint. Returns the last `tail`
    /// batches (those before `before_seq` when paging backwards via "load
    /// earlier") and whether earlier batches exist. The marker reports the
    /// whole subject's state regardless of trimming.
    pub async fn fetch_tail(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        tail: usize,
        before_seq: Option<u64>,
    ) -> Result<LogBatchPage, LogsError> {
        if let LogsBackend::Local(store) = &self.backend {
            return store
                .fetch_tail(
                    workflow_id,
                    workflow_instance_id,
                    task_instance_id,
                    tail,
                    before_seq,
                )
                .await;
        }
        let LogsBackend::Distributed { log_streams, .. } = &self.backend else {
            unreachable!()
        };
        let (mut batches, marker) = read_stream_page(
            log_streams.as_ref(),
            log_stream_route(workflow_id, workflow_instance_id, task_instance_id),
            None,
        )
        .await?;
        if let Some(before) = before_seq {
            batches.retain(|b| b.seq < before);
        }
        let has_earlier = batches.len() > tail;
        if has_earlier {
            let drop = batches.len() - tail;
            batches.drain(..drop);
        }
        Ok(LogBatchPage {
            batches,
            marker,
            has_earlier,
        })
    }

    /// Read the archived End-of-stream sidecar, mapping "absent" to `None`.
    async fn read_exit_sidecar(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
    ) -> Result<Option<EndOfStreamMarker>, LogsError> {
        let LogsBackend::Distributed { minio, .. } = &self.backend else {
            return Ok(None);
        };
        let key = exit_sidecar_key(workflow_id, workflow_instance_id, task_instance_id);
        match minio.read(&key).await {
            Ok(bytes) => serde_json::from_slice(&bytes.to_vec())
                .map(Some)
                .map_err(|e| LogsError::Minio(format!("malformed sidecar {}: {}", key, e))),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(LogsError::Minio(format!("read {}: {}", key, e))),
        }
    }
}

fn decode_gzip(bytes: &[u8]) -> Result<Vec<u8>, LogsError> {
    let mut decoder = GzDecoder::new(bytes);
    let mut out = Vec::with_capacity(bytes.len());
    decoder
        .read_to_end(&mut out)
        .map_err(|e| LogsError::GzipDecode(e.to_string()))?;
    Ok(out)
}

fn log_stream_route(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
) -> LogStreamRoute {
    LogStreamRoute {
        workflow_id,
        workflow_instance_id,
        task_instance_id,
    }
}

async fn read_stream_page(
    log_streams: &dyn LogStreamProvider,
    route: LogStreamRoute,
    start_sequence: Option<u64>,
) -> Result<(Vec<LogBatch>, Option<EndOfStreamMarker>), LogsError> {
    let records = log_streams
        .replay_task(route)
        .await
        .map_err(|error| LogsError::Staging(error.to_string()))?;
    let mut batches = Vec::new();
    let mut marker = None;
    let mut cursor = 0_u64;
    for record in records {
        match record {
            ReplayedLogRecord::Accepted { bytes, .. } => {
                cursor = cursor.saturating_add(1);
                if start_sequence.is_none_or(|start| cursor >= start) {
                    batches.push(LogBatch { seq: cursor, bytes });
                }
            }
            ReplayedLogRecord::PreAcceptanceGap(gap) => {
                let covered = gap
                    .last_sequence
                    .saturating_sub(gap.first_sequence)
                    .saturating_add(1);
                cursor = cursor.saturating_add(covered);
            }
            ReplayedLogRecord::Terminal { terminal, .. } => {
                marker = Some(marker_from_terminal(&terminal));
            }
        }
    }
    Ok((batches, marker))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minio_key_matches_log_uploader_layout() {
        let wf = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let wi = Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap();
        let ti = Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap();
        assert_eq!(
            minio_object_key(wf, wi, ti),
            "task_logs/11111111-2222-3333-4444-555555555555/66666666-7777-8888-9999-aaaaaaaaaaaa/bbbbbbbb-cccc-dddd-eeee-ffffffffffff.gz"
        );
    }

    #[test]
    fn gzip_round_trip_yields_original_bytes() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let plain = b"hello logs\nline two\n";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(plain).unwrap();
        let gz = encoder.finish().unwrap();

        let decoded = decode_gzip(&gz).expect("gzip round-trip");
        assert_eq!(decoded, plain);
    }
}
