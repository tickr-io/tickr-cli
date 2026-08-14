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

use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::consumer::pull;
use async_nats::jetstream::{self, consumer};
use async_nats::Client as NatsClient;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use opendal::layers::LoggingLayer;
use opendal::Operator;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write;
use std::time::Duration;
use tickr_proto::coord::all_nats;
use tickr_proto::coord::log_stream::{
    accepted_record_digest, rebuild_log_streams, AcceptedLogRecord, LogSeal, LogStreamIdentity,
    LogStreamState, LogTerminal, PreAcceptanceGap,
};
use uuid::Uuid;

/// Object-storage bucket the gzip blobs land in.

/// JetStream stream staging task-log batches. Mirrors the executor's
/// publisher and the API's logs resolver — the three must agree on stream
/// name and subject shape.
const LOG_STREAM_NAME: &str = tickr_proto::coord::all_nats::LOG_STREAM;

/// Subject a task instance's log batches live on. Mirrors the executor's
/// `log_subject`.
fn log_subject(workflow_id: &Uuid, workflow_instance_id: &Uuid, task_instance_id: &Uuid) -> String {
    format!(
        "{}.{}.{}.{}",
        tickr_proto::coord::all_nats::LOG_SUBJECT_PREFIX,
        workflow_id,
        workflow_instance_id,
        task_instance_id
    )
}

/// The End-of-stream marker as read off a task's log subject.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EndOfStreamMarker {
    pub exit_status: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SealedLogStream {
    identity: LogStreamIdentity,
    committed_frontier: Option<u64>,
    accepted_records: Vec<AcceptedLogRecord>,
    declared_gaps: Vec<PreAcceptanceGap>,
    terminal: LogTerminal,
}

/// Immutable accepted-record, gap, frontier, and terminal snapshot for one
/// Task instance. The digest is the stable Log seal identity used by Compaction.
#[derive(Clone, Debug)]
pub(crate) struct TaskLogSeal {
    task_instance_id: Uuid,
    streams: Vec<SealedLogStream>,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct LogTerminalFence {
    pub stream: LogStreamIdentity,
    pub terminal: LogTerminal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct TaskLogSealIdentity {
    pub task_instance_id: Uuid,
    pub digest: String,
    pub terminal_fences: Vec<LogTerminalFence>,
}

impl TaskLogSeal {
    pub(crate) fn identity(&self) -> TaskLogSealIdentity {
        TaskLogSealIdentity {
            task_instance_id: self.task_instance_id,
            digest: self.digest.clone(),
            terminal_fences: self
                .streams
                .iter()
                .map(|stream| LogTerminalFence {
                    stream: stream.identity.clone(),
                    terminal: stream.terminal.clone(),
                })
                .collect(),
        }
    }

    fn accepted_batches(&self) -> Vec<Vec<u8>> {
        self.streams
            .iter()
            .flat_map(|stream| {
                stream
                    .accepted_records
                    .iter()
                    .map(|record| record.bytes.clone())
            })
            .collect()
    }

    fn terminal_marker(&self) -> Result<EndOfStreamMarker> {
        self.streams
            .last()
            .map(|stream| marker_from_terminal(&stream.terminal))
            .ok_or_else(|| anyhow!("cannot install an empty Log seal"))
    }
}

pub(crate) fn task_log_seal_from_role(
    task_instance_id: Uuid,
    mut seals: Vec<LogSeal>,
) -> Result<Option<TaskLogSeal>> {
    if seals.is_empty() {
        return Ok(None);
    }
    seals.sort_by(|left, right| left.stream().cmp(right.stream()));
    let streams = seals
        .iter()
        .map(|seal| {
            if seal.stream().task_instance_id != task_instance_id
                || accepted_record_digest(seal.accepted_records()) != seal.record_digest()
            {
                bail!("selected Log seal does not match its Task or accepted-record digest");
            }
            let committed_frontier = match seal.terminal() {
                LogTerminal::AbnormalClosure { committed_frontier } => *committed_frontier,
                LogTerminal::EndOfStream { .. } => seal
                    .accepted_records()
                    .last()
                    .map(|record| record.identity.sequence),
            };
            Ok(SealedLogStream {
                identity: seal.stream().clone(),
                committed_frontier,
                accepted_records: seal.accepted_records().to_vec(),
                declared_gaps: Vec::new(),
                terminal: seal.terminal().clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let digest =
        digest_bytes(&serde_json::to_vec(&seals).context("encode selected immutable Log seals")?);
    Ok(Some(TaskLogSeal {
        task_instance_id,
        streams,
        digest,
    }))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FinalLogObjectIdentity {
    pub path: String,
    pub digest: String,
    pub length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FinalLogInstallation {
    pub task_instance_id: Uuid,
    pub seal_digest: String,
    pub log_object: Option<FinalLogObjectIdentity>,
    pub terminal_object: FinalLogObjectIdentity,
}

fn seal_streams(
    task_instance_id: Uuid,
    streams: BTreeMap<LogStreamIdentity, LogStreamState>,
) -> Result<Option<TaskLogSeal>> {
    if streams.is_empty() {
        return Ok(None);
    }
    let streams = streams
        .into_values()
        .map(|state| {
            let terminal = state
                .terminal()
                .cloned()
                .ok_or_else(|| anyhow!("cannot seal an open Log staging stream"))?;
            Ok(SealedLogStream {
                identity: state.identity().clone(),
                committed_frontier: state.committed_frontier(),
                accepted_records: state.accepted_records(),
                declared_gaps: state.declared_gaps(),
                terminal,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let digest =
        digest_bytes(&serde_json::to_vec(&streams).context("encode immutable Log seal snapshot")?);
    Ok(Some(TaskLogSeal {
        task_instance_id,
        streams,
        digest,
    }))
}

/// Replay the common LogStream state and durably close any stream whose Task
/// is already terminal. Repeating this operation returns the same seal.
pub(crate) async fn seal_task_logs(
    nats: &NatsClient,
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    task_instance_id: &Uuid,
) -> Result<Option<TaskLogSeal>> {
    let js = jetstream::new(nats.clone());
    let stream = match js.get_stream(LOG_STREAM_NAME).await {
        Ok(stream) => stream,
        Err(_) => return Ok(None),
    };
    let subject = log_subject(workflow_id, workflow_instance_id, task_instance_id);
    let mut recovered_terminal = false;

    loop {
        let consumer = stream
            .create_consumer(pull::Config {
                filter_subject: subject.clone(),
                deliver_policy: consumer::DeliverPolicy::All,
                ack_policy: consumer::AckPolicy::None,
                ..Default::default()
            })
            .await
            .with_context(|| format!("create Log replay consumer for {subject}"))?;
        let mut durable_records = Vec::new();
        loop {
            let mut fetched = consumer
                .fetch()
                .max_messages(500)
                .expires(Duration::from_millis(500))
                .messages()
                .await
                .with_context(|| format!("fetch Log replay records on {subject}"))?;
            let mut count = 0_usize;
            while let Some(message) = fetched.next().await {
                let message =
                    message.map_err(|error| anyhow!("Log replay on {subject}: {error}"))?;
                durable_records.push(
                    all_nats::decode_log_record(
                        |name| {
                            message
                                .headers
                                .as_ref()
                                .and_then(|headers| headers.get(name))
                                .map(|value| value.as_str().to_owned())
                        },
                        &message.payload,
                    )
                    .map_err(anyhow::Error::msg)?,
                );
                count += 1;
            }
            if count < 500 {
                break;
            }
        }

        let streams = rebuild_log_streams(durable_records)?;
        let open = streams
            .values()
            .filter(|state| state.terminal().is_none())
            .map(|state| (state.identity().clone(), state.committed_frontier()))
            .collect::<Vec<_>>();
        if open.is_empty() {
            return seal_streams(*task_instance_id, streams);
        }
        if recovered_terminal {
            bail!("abnormal Log terminal did not become replayable");
        }
        for (identity, frontier) in open {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(all_nats::LOG_PROTOCOL_HEADER, all_nats::LOG_PROTOCOL);
            headers.insert(all_nats::LOG_KIND_HEADER, all_nats::LOG_KIND_ABNORMAL);
            headers.insert(
                all_nats::LOG_TASK_INSTANCE_HEADER,
                identity.task_instance_id.to_string().as_str(),
            );
            headers.insert(
                all_nats::LOG_PICKUP_GENERATION_HEADER,
                identity.pickup_generation.to_string().as_str(),
            );
            if let Some(frontier) = frontier {
                headers.insert(
                    all_nats::LOG_COMMITTED_FRONTIER_HEADER,
                    frontier.to_string().as_str(),
                );
            }
            let message_id = format!(
                "log:{}:{}:terminal",
                identity.task_instance_id, identity.pickup_generation
            );
            headers.insert("Nats-Msg-Id", message_id.as_str());
            js.publish_with_headers(subject.clone(), headers, Vec::new().into())
                .await
                .context("publish abnormal Log terminal")?
                .await
                .context("await abnormal Log terminal acceptance")?;
        }
        recovered_terminal = true;
    }
}

fn digest_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

async fn write_final_log_object(
    storage: &Operator,
    path: String,
    bytes: Vec<u8>,
) -> Result<FinalLogObjectIdentity> {
    let identity = FinalLogObjectIdentity {
        path,
        digest: digest_bytes(&bytes),
        length: bytes.len() as u64,
    };
    storage
        .write(&identity.path, bytes)
        .await
        .with_context(|| format!("write final Log object `{}`", identity.path))?;
    Ok(identity)
}

async fn verify_final_log_object(
    storage: &Operator,
    identity: &FinalLogObjectIdentity,
) -> Result<()> {
    let bytes = storage
        .read(&identity.path)
        .await
        .with_context(|| format!("read installed final Log object `{}`", identity.path))?;
    if bytes.len() as u64 != identity.length {
        bail!(
            "final Log object `{}` length mismatch: expected {}, found {}",
            identity.path,
            identity.length,
            bytes.len()
        );
    }
    if digest_bytes(&bytes.to_vec()) != identity.digest {
        bail!("final Log object `{}` digest mismatch", identity.path);
    }
    Ok(())
}

pub(crate) async fn install_task_logs(
    storage: &Operator,
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    seal: &TaskLogSeal,
) -> Result<FinalLogInstallation> {
    let batches = seal.accepted_batches();
    let log_object = if batches.is_empty() {
        None
    } else {
        let path = format!(
            "task_logs/{}/{}/{}.gz",
            workflow_id, workflow_instance_id, seal.task_instance_id
        );
        Some(write_final_log_object(storage, path, compress_log_batches(&batches)?).await?)
    };
    let terminal_path =
        exit_sidecar_path(workflow_id, workflow_instance_id, &seal.task_instance_id);
    let terminal_bytes =
        serde_json::to_vec(&seal.terminal_marker()?).context("encode final Log terminal")?;
    let terminal_object = write_final_log_object(storage, terminal_path, terminal_bytes).await?;
    Ok(FinalLogInstallation {
        task_instance_id: seal.task_instance_id,
        seal_digest: seal.digest.clone(),
        log_object,
        terminal_object,
    })
}

pub(crate) async fn verify_task_log_installation(
    storage: &Operator,
    installation: &FinalLogInstallation,
) -> Result<()> {
    if let Some(log_object) = &installation.log_object {
        verify_final_log_object(storage, log_object).await?;
    }
    verify_final_log_object(storage, &installation.terminal_object).await
}

/// Compatibility entry point for callers that archive one task directly.
/// Compaction uses the explicit seal, install, and verify phases above.
pub async fn upload_task_logs(
    nats: &NatsClient,
    storage: &Operator,
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    task_instance_id: &Uuid,
) -> Result<bool> {
    let Some(seal) =
        seal_task_logs(nats, workflow_id, workflow_instance_id, task_instance_id).await?
    else {
        return Ok(false);
    };
    let installation = install_task_logs(storage, workflow_id, workflow_instance_id, &seal).await?;
    verify_task_log_installation(storage, &installation).await?;
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

/// Purge Accepted Log records while retaining one terminal fence per sealed
/// pickup generation, so a late writer still observes immutable state.
pub(crate) async fn purge_sealed_task_logs(
    nats: &NatsClient,
    workflow_id: &Uuid,
    workflow_instance_id: &Uuid,
    seal: &TaskLogSealIdentity,
) -> Result<()> {
    let js = jetstream::new(nats.clone());
    let stream = js
        .get_stream(LOG_STREAM_NAME)
        .await
        .context("open Log staging stream for sealed purge")?;
    let subject = log_subject(workflow_id, workflow_instance_id, &seal.task_instance_id);
    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: subject.clone(),
            deliver_policy: consumer::DeliverPolicy::All,
            ack_policy: consumer::AckPolicy::None,
            ..Default::default()
        })
        .await
        .with_context(|| format!("create sealed Log purge consumer for {subject}"))?;
    let expected_terminals = seal
        .terminal_fences
        .iter()
        .map(|fence| (fence.stream.clone(), fence.terminal.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut retained_terminals = BTreeMap::new();
    let mut sequences_to_purge = Vec::new();
    loop {
        let mut fetched = consumer
            .fetch()
            .max_messages(500)
            .expires(Duration::from_millis(500))
            .messages()
            .await
            .with_context(|| format!("fetch sealed Log purge records on {subject}"))?;
        let mut count = 0_usize;
        while let Some(message) = fetched.next().await {
            let message =
                message.map_err(|error| anyhow!("sealed Log purge on {subject}: {error}"))?;
            let sequence = message
                .info()
                .map_err(|error| anyhow!("read sealed Log sequence on {subject}: {error}"))?
                .stream_sequence;
            let record = all_nats::decode_log_record(
                |name| {
                    message
                        .headers
                        .as_ref()
                        .and_then(|headers| headers.get(name))
                        .map(|value| value.as_str().to_owned())
                },
                &message.payload,
            )
            .map_err(anyhow::Error::msg)?;
            match record {
                tickr_proto::coord::log_stream::ReplayedLogRecord::Terminal {
                    stream,
                    terminal,
                } => {
                    if retained_terminals.insert(stream, terminal).is_some() {
                        bail!("sealed Log staging contains duplicate terminal records");
                    }
                }
                tickr_proto::coord::log_stream::ReplayedLogRecord::Accepted { .. }
                | tickr_proto::coord::log_stream::ReplayedLogRecord::PreAcceptanceGap(_) => {
                    sequences_to_purge.push(sequence);
                }
            }
            count += 1;
        }
        if count < 500 {
            break;
        }
    }
    if retained_terminals != expected_terminals {
        bail!("retained Log terminal fences do not match immutable seal");
    }
    for sequence in sequences_to_purge {
        stream
            .delete_message(sequence)
            .await
            .with_context(|| format!("purge Accepted Log or gap at stream sequence {sequence}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;
    #[cfg(not(madsim))]
    use testcontainers_modules::nats::{Nats, NatsServerCmd};
    #[cfg(not(madsim))]
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    #[cfg(not(madsim))]
    use testcontainers_modules::testcontainers::ImageExt;

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

    fn sealed_log(bytes: &[u8], gap_last: u64, exit_status: i32) -> TaskLogSeal {
        use tickr_proto::coord::log_stream::{
            LogExit, LogRecordIdentity, LogRecordSubmission, PreAcceptanceGap,
        };

        let identity = LogStreamIdentity {
            task_instance_id: Uuid::nil(),
            pickup_generation: 1,
        };
        let mut state = LogStreamState::new(identity.clone());
        state
            .apply_accepted(LogRecordSubmission::new(
                LogRecordIdentity {
                    stream: identity.clone(),
                    sequence: 0,
                },
                bytes.to_vec(),
            ))
            .unwrap();
        state
            .apply_gap(PreAcceptanceGap {
                stream: identity.clone(),
                first_sequence: 1,
                last_sequence: gap_last,
                dropped_records: gap_last,
            })
            .unwrap();
        state
            .apply_terminal(LogTerminal::EndOfStream {
                exit: LogExit::Status(exit_status),
            })
            .unwrap();
        seal_streams(Uuid::nil(), BTreeMap::from([(identity, state)]))
            .unwrap()
            .unwrap()
    }

    #[test]
    fn immutable_log_seal_covers_records_gaps_frontier_and_terminal() {
        let unchanged = sealed_log(b"accepted", 1, 0);
        assert_eq!(unchanged.digest, sealed_log(b"accepted", 1, 0).digest);
        assert_ne!(unchanged.digest, sealed_log(b"changed", 1, 0).digest);
        assert_ne!(unchanged.digest, sealed_log(b"accepted", 2, 0).digest);
        assert_ne!(unchanged.digest, sealed_log(b"accepted", 1, 7).digest);

        let mut terminal_state = LogStreamState::new(LogStreamIdentity {
            task_instance_id: Uuid::nil(),
            pickup_generation: 2,
        });
        terminal_state
            .apply_terminal(LogTerminal::AbnormalClosure {
                committed_frontier: None,
            })
            .unwrap();
        assert!(matches!(
            terminal_state.apply_accepted(
                tickr_proto::coord::log_stream::LogRecordSubmission::new(
                    tickr_proto::coord::log_stream::LogRecordIdentity {
                        stream: terminal_state.identity().clone(),
                        sequence: 0,
                    },
                    b"late".to_vec(),
                )
            ),
            Err(tickr_proto::coord::log_stream::LogStreamViolation::AppendAfterTerminal)
        ));
    }

    #[test]
    fn log_subject_matches_staging_layout() {
        let wf = Uuid::nil();
        let wi = Uuid::nil();
        let ti = Uuid::nil();
        assert_eq!(
            log_subject(&wf, &wi, &ti),
            "tickr.all_nats.v2.log_staging.00000000-0000-0000-0000-000000000000.00000000-0000-0000-0000-000000000000.00000000-0000-0000-0000-000000000000"
        );
    }
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn sealed_purge_verifies_terminal_fences_before_deleting_records() -> Result<()> {
        let cmd = NatsServerCmd::default().with_jetstream();
        let container = match Nats::default().with_cmd(&cmd).start().await {
            Ok(container) => container,
            Err(error) => {
                eprintln!("skipping: NATS testcontainer unavailable: {error}");
                return Ok(());
            }
        };
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .context("read isolated NATS port")?;
        let url = format!("nats://127.0.0.1:{port}");
        let mut connected = None;
        for _ in 0..50 {
            match async_nats::connect(&url).await {
                Ok(client) => {
                    connected = Some(client);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        let nats = connected.context("isolated NATS did not accept connections")?;
        let js = jetstream::new(nats.clone());
        js.get_or_create_stream(jetstream::stream::Config {
            name: LOG_STREAM_NAME.to_owned(),
            subjects: vec![all_nats::LOG_STREAM_SUBJECTS.to_owned()],
            ..Default::default()
        })
        .await
        .context("create isolated Log staging stream")?;

        let workflow_id = Uuid::new_v4();
        let workflow_instance_id = Uuid::new_v4();
        let task_instance_id = Uuid::new_v4();
        let subject = log_subject(&workflow_id, &workflow_instance_id, &task_instance_id);
        let bytes = b"accepted record".to_vec();
        let content_digest = tickr_proto::coord::log_stream::content_digest(&bytes);
        let mut accepted_headers = async_nats::HeaderMap::new();
        accepted_headers.insert(all_nats::LOG_PROTOCOL_HEADER, all_nats::LOG_PROTOCOL);
        accepted_headers.insert(all_nats::LOG_KIND_HEADER, all_nats::LOG_KIND_ACCEPTED);
        accepted_headers.insert(
            all_nats::LOG_TASK_INSTANCE_HEADER,
            task_instance_id.to_string().as_str(),
        );
        accepted_headers.insert(all_nats::LOG_PICKUP_GENERATION_HEADER, "1");
        accepted_headers.insert(all_nats::LOG_SEQUENCE_HEADER, "0");
        accepted_headers.insert(all_nats::LOG_COMMITTED_FRONTIER_HEADER, "0");
        accepted_headers.insert(all_nats::LOG_CONTENT_DIGEST_HEADER, content_digest.as_str());
        accepted_headers.insert(
            "Nats-Msg-Id",
            format!("log:{task_instance_id}:1:record:0").as_str(),
        );
        js.publish_with_headers(subject.clone(), accepted_headers, bytes.into())
            .await?
            .await?;

        let mut terminal_headers = async_nats::HeaderMap::new();
        terminal_headers.insert(all_nats::LOG_PROTOCOL_HEADER, all_nats::LOG_PROTOCOL);
        terminal_headers.insert(all_nats::LOG_KIND_HEADER, all_nats::LOG_KIND_END);
        terminal_headers.insert(
            all_nats::LOG_TASK_INSTANCE_HEADER,
            task_instance_id.to_string().as_str(),
        );
        terminal_headers.insert(all_nats::LOG_PICKUP_GENERATION_HEADER, "1");
        terminal_headers.insert(all_nats::LOG_COMMITTED_FRONTIER_HEADER, "0");
        terminal_headers.insert(all_nats::LOG_EXIT_KIND_HEADER, "status");
        terminal_headers.insert(all_nats::LOG_EXIT_STATUS_HEADER, "1");
        terminal_headers.insert(
            "Nats-Msg-Id",
            format!("log:{task_instance_id}:1:terminal").as_str(),
        );
        js.publish_with_headers(subject.clone(), terminal_headers, Default::default())
            .await?
            .await?;

        let actual = seal_task_logs(
            &nats,
            &workflow_id,
            &workflow_instance_id,
            &task_instance_id,
        )
        .await?
        .context("sealed Log staging is present")?
        .identity();
        let mut mismatched = actual.clone();
        mismatched.terminal_fences[0].terminal = LogTerminal::EndOfStream {
            exit: tickr_proto::coord::log_stream::LogExit::Status(2),
        };

        let error = purge_sealed_task_logs(&nats, &workflow_id, &workflow_instance_id, &mismatched)
            .await
            .expect_err("mismatched terminal fence must fail closed");
        assert!(error
            .to_string()
            .contains("terminal fences do not match immutable seal"));
        let mut stream = js.get_stream(LOG_STREAM_NAME).await?;
        assert_eq!(
            stream.info().await?.state.messages,
            2,
            "fence verification must precede every destructive delete"
        );

        purge_sealed_task_logs(&nats, &workflow_id, &workflow_instance_id, &actual).await?;
        assert_eq!(
            stream.info().await?.state.messages,
            1,
            "verified purge retains only the immutable terminal fence"
        );
        purge_sealed_task_logs(&nats, &workflow_id, &workflow_instance_id, &actual).await?;
        assert_eq!(
            stream.info().await?.state.messages,
            1,
            "retry after a partial or completed purge is idempotent"
        );
        Ok(())
    }
}
