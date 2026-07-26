//! Conductor-side compaction staging and drain — the stage-then-drain
//! half of compaction.
//!
//! The relay handler **stages** an inbound `CompactionEnvelope` (proto: the
//! archive-grade projection + an opaque correlation) durably in the
//! per-tenant NATS work queue (`tickr.compaction.jobs`) and ACKs the
//! server immediately — the ACK means "durably staged", not "archived",
//! so live-state retirement is never gated on object-storage or repository
//! latency. The staged message is the only copy of the payload in the gap
//! between ACK and archive; that is why staging awaits the JetStream
//! publish acknowledgement before the relay ACK is sent.
//!
//! The **compaction drain** (this module's worker) consumes the queue and
//! performs the archival per job: upload every task's logs from the Log
//! staging stream (every outcome — failed attempts included), tickr-ctx
//! scope read + the three-table archive transaction + signal-captures
//! cleanup, then purge the log subjects. Every conductor instance runs a
//! drain against the same durable consumer, so any instance can drain any
//! staged job and throughput scales with the stateless tier. The drain
//! ACKs the work-queue message only after the whole job completes;
//! at-least-once redelivery of a half-finished job converges: the archive
//! transaction upserts, the blob overwrite is same-key, and the subject
//! purge is idempotent (an already-purged subject skips the upload rather
//! than overwriting the blob with emptiness).

use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::consumer::{pull, PullConsumer};
use async_nats::jetstream::{self, kv, stream};
use async_nats::{Client as NatsClient, HeaderMap};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use opendal::Operator;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::scope_repository::ScopeStore;
use tickr_proto::archive as ap;
use tickr_proto::codec::compaction::decode_envelope;
use tickr_proto::coord::all_nats::{COMPACTION_ACK_WAIT, COMPACTION_STAGING_BUCKET};
use tickr_proto::coord::{
    log_stream::LogSeal, CompactionFuture, CompactionStaging, CompactionStagingDelivery,
    CompactionStagingSeal,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::system_tasks::compaction_receiver::{
    cleanup_ctx_scope, open_nats_scope_reader, persist_compaction_projection_with_scope,
    persist_compaction_projection_with_selected_scope, seal_ctx_scope, CompactionScopeSnapshot,
    CompactionScopeSnapshotReader, RoleCompactionScopeSnapshotReader,
};
use crate::system_tasks::log_uploader::{
    install_task_logs, purge_sealed_task_logs, seal_task_logs, task_log_seal_from_role,
    verify_task_log_installation, FinalLogInstallation, TaskLogSeal, TaskLogSealIdentity,
};
const MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";
const PAYLOAD_KEY_PREFIX: &str = "payload";
const QUEUED_KEY_PREFIX: &str = "queued";
const COMPLETE_KEY_PREFIX: &str = "complete";
const SEAL_KEY_PREFIX: &str = "seal";
const INSTALLATION_KEY_PREFIX: &str = "installation";
const ARCHIVE_COMMIT_KEY_PREFIX: &str = "archive_commit";
/// Compaction-only view of the selected accepted-Log role.
#[async_trait]
pub trait CompactionLogStaging: Send + Sync {
    async fn seal_task(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
    ) -> Result<Vec<LogSeal>>;

    async fn purge_task_after_archive(
        &self,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
        seals: &[LogSeal],
        archive_identity: &[u8],
    ) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactionBoundary {
    BeforeStagingMutation,
    AfterStagingMutation,
    BeforeDurabilityProof,
    AfterDurabilityProof,
    BeforeCrossPlaneAcknowledgement,
    AfterCrossPlaneAcknowledgement,
    BeforeDrainReceipt,
    AfterDrainReceipt,
    BeforeLogSeal,
    AfterLogSeal,
    BeforeFinalLogInstallation,
    AfterFinalLogInstallation,
    BeforeFinalLogVerification,
    AfterFinalLogVerification,
    BeforeArchiveCommit,
    AfterArchiveCommit,
    BeforeLogPurge,
    AfterLogPurge,
    BeforeStagingCompletion,
    AfterStagingCompletion,
    BeforeScopeCleanup,
    AfterScopeCleanup,
}

#[cfg(test)]
impl CompactionBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::BeforeStagingMutation => "before-staging-mutation",
            Self::AfterStagingMutation => "after-staging-mutation",
            Self::BeforeDurabilityProof => "before-durability-proof",
            Self::AfterDurabilityProof => "after-durability-proof",
            Self::BeforeCrossPlaneAcknowledgement => "before-cross-plane-acknowledgement",
            Self::AfterCrossPlaneAcknowledgement => "after-cross-plane-acknowledgement",
            Self::BeforeDrainReceipt => "before-drain-receipt",
            Self::AfterDrainReceipt => "after-drain-receipt",
            Self::BeforeLogSeal => "before-log-seal",
            Self::AfterLogSeal => "after-log-seal",
            Self::BeforeFinalLogInstallation => "before-final-log-installation",
            Self::AfterFinalLogInstallation => "after-final-log-installation",
            Self::BeforeFinalLogVerification => "before-final-log-verification",
            Self::AfterFinalLogVerification => "after-final-log-verification",
            Self::BeforeArchiveCommit => "before-archive-commit",
            Self::AfterArchiveCommit => "after-archive-commit",
            Self::BeforeLogPurge => "before-log-purge",
            Self::AfterLogPurge => "after-log-purge",
            Self::BeforeStagingCompletion => "before-staging-completion",
            Self::AfterStagingCompletion => "after-staging-completion",
            Self::BeforeScopeCleanup => "before-scope-cleanup",
            Self::AfterScopeCleanup => "after-scope-cleanup",
        }
    }
}

#[cfg(test)]
pub(crate) fn observe_compaction_boundary(boundary: CompactionBoundary) {
    if std::env::var("TICKR_TEST_COMPACTION_CRASH_BOUNDARY").as_deref() == Ok(boundary.name()) {
        std::process::exit(86);
    }
}

#[cfg(not(test))]
#[inline(always)]
pub(crate) const fn observe_compaction_boundary(_boundary: CompactionBoundary) {}

/// The archive-grade content of one staged compaction job, decoded from the
/// proto envelope. The `shipped_at` enrichment rides the wire wrapper, not the
/// projection.
struct DecodedJob {
    projection: ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CompactionSeal {
    workflow_instance_id: Uuid,
    scope_digest: String,
    task_logs: Vec<TaskLogSealIdentity>,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SelectedCompactionSeal {
    scope: CompactionScopeSnapshot,
    logs: Vec<LogSeal>,
    compaction: CompactionSeal,
}

#[derive(Serialize)]
struct CompactionSealContent<'a> {
    workflow_instance_id: Uuid,
    scope_digest: &'a str,
    task_logs: &'a [TaskLogSealIdentity],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FinalLogArchiveIdentity {
    compaction_seal_digest: String,
    task_logs: Vec<FinalLogInstallation>,
}

/// Decode a staged job from the proto envelope. Returns `None` when the bytes
/// are not a compaction envelope — a genuine poison job. The drain accepts
/// exactly the current staged encoding.
fn decode_job(bytes: &[u8]) -> Option<DecodedJob> {
    let envelope = decode_envelope(bytes).ok()?;
    let shipped_at = envelope
        .shipped_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc));
    // `decode_envelope` guarantees a projection.
    envelope.projection.map(|projection| DecodedJob {
        projection,
        shipped_at,
    })
}

/// Subject the relay handler stages compaction payloads onto. Dotted
/// hierarchy matches the conductor's other tenant-local subjects
/// (`tickr.external.signals`, `tickr.api.commands`).
pub const SUBJECT: &str = tickr_proto::coord::all_nats::COMPACTION_SUBJECT;

/// JetStream stream backing the subject. WorkQueue retention: an acked
/// (drained) job auto-deletes; an unacked job survives conductor death
/// and redelivers. NATS stream names cannot contain dots, so the stream
/// uses underscores while the subject keeps the dotted form.
pub const STREAM_NAME: &str = tickr_proto::coord::all_nats::COMPACTION_STREAM;

/// Durable pull-consumer name shared by every conductor instance — NATS
/// load-balances staged jobs across whichever instances are pulling.
pub const CONSUMER_NAME: &str = tickr_proto::coord::all_nats::COMPACTION_CONSUMER;

/// Create or fetch the work-queue stream. Idempotent — an existing stream
/// is returned without reconciliation, matching the conductor's
/// create-if-absent posture for its other JetStream surfaces.
async fn ensure_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    let stream_cfg = stream::Config {
        name: STREAM_NAME.to_string(),
        subjects: vec![SUBJECT.to_string()],
        retention: stream::RetentionPolicy::WorkQueue,
        storage: stream::StorageType::File,
        ..Default::default()
    };
    js.get_or_create_stream(stream_cfg)
        .await
        .map_err(|e| anyhow!("failed to get_or_create stream {}: {}", STREAM_NAME, e))
}

async fn ensure_staging_store(js: &jetstream::Context) -> Result<kv::Store> {
    match js.get_key_value(COMPACTION_STAGING_BUCKET).await {
        Ok(store) => Ok(store),
        Err(_) => match js
            .create_key_value(kv::Config {
                bucket: COMPACTION_STAGING_BUCKET.to_owned(),
                history: 1,
                storage: stream::StorageType::File,
                ..Default::default()
            })
            .await
        {
            Ok(store) => Ok(store),
            Err(_) => js
                .get_key_value(COMPACTION_STAGING_BUCKET)
                .await
                .context("opening concurrently-created Compaction identity store"),
        },
    }
}

fn staging_key(prefix: &str, identity: &str) -> String {
    format!("{prefix}.{identity}")
}

fn digest(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

fn compaction_identity(payload: &[u8]) -> Result<String> {
    let envelope = decode_envelope(payload).context("decode Compaction envelope for staging")?;
    let projection = envelope
        .projection
        .as_ref()
        .ok_or_else(|| anyhow!("Compaction envelope has no archive projection"))?;
    Ok(Uuid::parse_str(&projection.id)
        .context("Compaction archive projection has invalid workflow instance id")?
        .to_string())
}

async fn read_stage_value(store: &kv::Store, key: &str) -> Result<Option<kv::Entry>> {
    store
        .entry(key)
        .await
        .with_context(|| format!("reading Compaction staging key `{key}`"))
}

async fn create_stage_value(store: &kv::Store, key: &str, value: &[u8], label: &str) -> Result<()> {
    match store.create(key, value.to_vec().into()).await {
        Ok(_) => Ok(()),
        Err(_) => match read_stage_value(store, key).await? {
            Some(existing) if existing.value.as_ref() == value => Ok(()),
            Some(_) => Err(anyhow!(
                "Compaction identity conflict while creating {label} `{key}`"
            )),
            None => Err(anyhow!(
                "ambiguous Compaction staging acknowledgement for {label} `{key}`"
            )),
        },
    }
}

async fn read_staged_json<T: DeserializeOwned>(
    store: &kv::Store,
    key: &str,
    label: &str,
) -> Result<Option<T>> {
    read_stage_value(store, key)
        .await?
        .map(|entry| {
            serde_json::from_slice(&entry.value).with_context(|| format!("decode {label} `{key}`"))
        })
        .transpose()
}

async fn create_staged_json<T: Serialize>(
    store: &kv::Store,
    key: &str,
    value: &T,
    label: &str,
) -> Result<()> {
    let bytes = serde_json::to_vec(value).with_context(|| format!("encode {label}"))?;
    create_stage_value(store, key, &bytes, label).await
}

fn build_compaction_seal(
    workflow_instance_id: Uuid,
    scope_digest: String,
    task_logs: &[TaskLogSeal],
) -> Result<CompactionSeal> {
    let mut task_logs = task_logs
        .iter()
        .map(TaskLogSeal::identity)
        .collect::<Vec<_>>();
    task_logs.sort_by_key(|seal| seal.task_instance_id);
    let content = CompactionSealContent {
        workflow_instance_id,
        scope_digest: &scope_digest,
        task_logs: &task_logs,
    };
    let digest = digest(&serde_json::to_vec(&content).context("encode immutable Compaction seal")?);
    Ok(CompactionSeal {
        workflow_instance_id,
        scope_digest,
        task_logs,
        digest,
    })
}

async fn validate_staged_payload(store: &kv::Store, identity: &str, payload: &[u8]) -> Result<()> {
    let payload_digest = digest(payload);
    let complete_key = staging_key(COMPLETE_KEY_PREFIX, identity);
    if let Some(completed_digest) = read_stage_value(store, &complete_key).await? {
        return if completed_digest.value.as_ref() == payload_digest.as_bytes() {
            Ok(())
        } else {
            Err(anyhow!(
                "Compaction identity `{identity}` conflicts with completed payload"
            ))
        };
    }

    let payload_key = staging_key(PAYLOAD_KEY_PREFIX, identity);
    match read_stage_value(store, &payload_key).await? {
        Some(existing) if existing.value.as_ref() == payload => Ok(()),
        Some(_) => Err(anyhow!(
            "Compaction identity `{identity}` conflicts with staged payload"
        )),
        None => Err(anyhow!(
            "Compaction identity `{identity}` has no durable staged payload"
        )),
    }
}

async fn complete_compaction_staging(
    nats: &NatsClient,
    identity: &str,
    payload: &[u8],
) -> Result<()> {
    let js = jetstream::new(nats.clone());
    let store = ensure_staging_store(&js).await?;
    validate_staged_payload(&store, identity, payload).await?;

    let payload_digest = digest(payload);
    let complete_key = staging_key(COMPLETE_KEY_PREFIX, identity);
    observe_compaction_boundary(CompactionBoundary::BeforeStagingCompletion);
    create_stage_value(
        &store,
        &complete_key,
        payload_digest.as_bytes(),
        "completion marker",
    )
    .await?;

    for key in [
        staging_key(PAYLOAD_KEY_PREFIX, identity),
        staging_key(QUEUED_KEY_PREFIX, identity),
    ] {
        if read_stage_value(&store, &key).await?.is_some() {
            store
                .delete(&key)
                .await
                .with_context(|| format!("cleaning Compaction staging key `{key}`"))?;
        }
    }
    observe_compaction_boundary(CompactionBoundary::AfterStagingCompletion);
    Ok(())
}

/// Durably stage a raw compaction payload (the proto envelope bytes, exactly
/// as received off the relay) into the work queue. Returns only after
/// JetStream acknowledges the publish — this is the durability boundary
/// the `CompactionAck` reply to the server rests on, so a `Ok(())` here
/// is the precondition for sending that ACK.
pub async fn stage_compaction_payload(nats: &NatsClient, payload_bytes: Vec<u8>) -> Result<()> {
    let identity = compaction_identity(&payload_bytes)?;
    let payload_digest = digest(&payload_bytes);
    let js = jetstream::new(nats.clone());
    ensure_stream(&js).await?;
    let store = ensure_staging_store(&js).await?;

    let complete_key = staging_key(COMPLETE_KEY_PREFIX, &identity);
    if let Some(completed_digest) = read_stage_value(&store, &complete_key).await? {
        return if completed_digest.value.as_ref() == payload_digest.as_bytes() {
            Ok(())
        } else {
            Err(anyhow!(
                "Compaction identity `{identity}` conflicts with completed payload"
            ))
        };
    }

    let payload_key = staging_key(PAYLOAD_KEY_PREFIX, &identity);
    match read_stage_value(&store, &payload_key).await? {
        Some(existing) if existing.value.as_ref() == payload_bytes.as_slice() => {}
        Some(_) => {
            return Err(anyhow!(
                "Compaction identity `{identity}` conflicts with staged payload"
            ));
        }
        None => {
            observe_compaction_boundary(CompactionBoundary::BeforeStagingMutation);
            create_stage_value(&store, &payload_key, &payload_bytes, "raw payload").await?;
            observe_compaction_boundary(CompactionBoundary::AfterStagingMutation);
        }
    }

    let queued_key = staging_key(QUEUED_KEY_PREFIX, &identity);
    if let Some(queued_digest) = read_stage_value(&store, &queued_key).await? {
        return if queued_digest.value.as_ref() == payload_digest.as_bytes() {
            Ok(())
        } else {
            Err(anyhow!(
                "Compaction identity `{identity}` has conflicting queue evidence"
            ))
        };
    }

    let mut headers = HeaderMap::new();
    let message_id = format!("compaction:{identity}");
    headers.insert(MESSAGE_ID_HEADER, message_id.as_str());
    observe_compaction_boundary(CompactionBoundary::BeforeDurabilityProof);
    js.publish_with_headers(SUBJECT, headers, payload_bytes.into())
        .await
        .context("publishing Compaction job to work queue")?
        .await
        .context("awaiting JetStream publish ack for Compaction job")?;
    observe_compaction_boundary(CompactionBoundary::AfterDurabilityProof);

    create_stage_value(
        &store,
        &queued_key,
        payload_digest.as_bytes(),
        "queue evidence",
    )
    .await
}

/// Fresh all-NATS adapter for the backend-neutral Compaction staging role.
#[derive(Clone)]
pub struct AllNatsCompactionStaging {
    nats: NatsClient,
}

impl AllNatsCompactionStaging {
    pub const fn new(nats: NatsClient) -> Self {
        Self { nats }
    }
}

struct AllNatsCompactionDelivery {
    staging: AllNatsCompactionStaging,
    identity: String,
    payload: Vec<u8>,
    message: Option<async_nats::jetstream::Message>,
}

impl CompactionStagingDelivery for AllNatsCompactionDelivery {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn load_seal(&self) -> CompactionFuture<'_, Result<Option<CompactionStagingSeal>, String>> {
        Box::pin(async move {
            let store = ensure_staging_store(&jetstream::new(self.staging.nats.clone()))
                .await
                .map_err(|error| error.to_string())?;
            let Some(entry) =
                read_stage_value(&store, &staging_key(SEAL_KEY_PREFIX, &self.identity))
                    .await
                    .map_err(|error| error.to_string())?
            else {
                return Ok(None);
            };
            let (encoded, digest, source_references): (Vec<u8>, String, u64) =
                serde_json::from_slice(&entry.value)
                    .context("decode all-NATS immutable Compaction seal")
                    .map_err(|error| error.to_string())?;
            Ok(Some(CompactionStagingSeal::new(
                encoded,
                digest,
                source_references,
            )))
        })
    }

    fn record_seal<'a>(
        &'a self,
        seal: &'a CompactionStagingSeal,
    ) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let store = ensure_staging_store(&jetstream::new(self.staging.nats.clone()))
                .await
                .map_err(|error| error.to_string())?;
            let value =
                serde_json::to_vec(&(seal.encoded(), seal.digest(), seal.source_references()))
                    .context("encode all-NATS immutable Compaction seal")
                    .map_err(|error| error.to_string())?;
            create_stage_value(
                &store,
                &staging_key(SEAL_KEY_PREFIX, &self.identity),
                &value,
                "immutable Compaction role seal",
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn load_archive_identity(&self) -> CompactionFuture<'_, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move {
            let store = ensure_staging_store(&jetstream::new(self.staging.nats.clone()))
                .await
                .map_err(|error| error.to_string())?;
            read_stage_value(
                &store,
                &staging_key(ARCHIVE_COMMIT_KEY_PREFIX, &self.identity),
            )
            .await
            .map(|entry| entry.map(|entry| entry.value.to_vec()))
            .map_err(|error| error.to_string())
        })
    }

    fn record_archive_identity<'a>(
        &'a self,
        archive_identity: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let store = ensure_staging_store(&jetstream::new(self.staging.nats.clone()))
                .await
                .map_err(|error| error.to_string())?;
            create_stage_value(
                &store,
                &staging_key(ARCHIVE_COMMIT_KEY_PREFIX, &self.identity),
                archive_identity,
                "archive-commit evidence",
            )
            .await
            .map_err(|error| error.to_string())
        })
    }

    fn complete<'a>(
        mut self: Box<Self>,
        archive_identity: &'a [u8],
    ) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.record_archive_identity(archive_identity).await?;
            complete_compaction_staging(&self.staging.nats, &self.identity, &self.payload)
                .await
                .map_err(|error| error.to_string())?;
            self.message
                .take()
                .ok_or_else(|| "Compaction delivery already settled".to_owned())?
                .ack()
                .await
                .map_err(|error| format!("acknowledge all-NATS Compaction delivery: {error}"))
        })
    }

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> CompactionFuture<'static, Result<(), String>> {
        Box::pin(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            drop(self);
            Ok(())
        })
    }
}

impl CompactionStaging for AllNatsCompactionStaging {
    fn prepare(&self) -> CompactionFuture<'_, Result<(), String>> {
        Box::pin(async move {
            init_stream_and_consumer(&self.nats)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }

    fn stage<'a>(&'a self, payload: &'a [u8]) -> CompactionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            stage_compaction_payload(&self.nats, payload.to_vec())
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn next(
        &self,
    ) -> CompactionFuture<'_, Result<Option<Box<dyn CompactionStagingDelivery>>, String>> {
        Box::pin(async move {
            let consumer = init_stream_and_consumer(&self.nats)
                .await
                .map_err(|error| error.to_string())?;
            let mut messages = consumer
                .messages()
                .await
                .map_err(|error| format!("open all-NATS Compaction consumer: {error}"))?;
            let Some(message) = messages.next().await else {
                return Ok(None);
            };
            let message =
                message.map_err(|error| format!("read all-NATS Compaction delivery: {error}"))?;
            let payload = message.payload.to_vec();
            let identity = compaction_identity(&payload).map_err(|error| error.to_string())?;
            let store = ensure_staging_store(&jetstream::new(self.nats.clone()))
                .await
                .map_err(|error| error.to_string())?;
            validate_staged_payload(&store, &identity, &payload)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Some(Box::new(AllNatsCompactionDelivery {
                staging: self.clone(),
                identity,
                payload,
                message: Some(message),
            }) as Box<dyn CompactionStagingDelivery>))
        })
    }
}

/// Create stream + durable pull consumer if absent. Idempotent.
pub async fn init_stream_and_consumer(nats: &NatsClient) -> Result<PullConsumer> {
    let js = jetstream::new(nats.clone());
    let stream = ensure_stream(&js).await?;

    let consumer_cfg = pull::Config {
        durable_name: Some(CONSUMER_NAME.to_string()),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ack_wait: COMPACTION_ACK_WAIT,
        ..Default::default()
    };
    stream
        .get_or_create_consumer(CONSUMER_NAME, consumer_cfg)
        .await
        .map_err(|e| anyhow!("failed to get_or_create consumer {}: {}", CONSUMER_NAME, e))
}

/// Archive one staged job end to end: log upload for every task instance,
/// the three-table archive transaction (with tickr-ctx scope read and
/// signal-captures cleanup), then log-subject purge. Each step is
/// idempotent, so a re-run after a mid-job crash converges.
async fn drain_one(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    log_storage: &Operator,
    scope_reader: &dyn CompactionScopeSnapshotReader,
    job: &DecodedJob,
) -> Result<()> {
    let projection = &job.projection;
    let workflow_instance_id = Uuid::parse_str(&projection.id).with_context(|| {
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
    let task_instance_ids = projection
        .task_instances
        .iter()
        .map(|task| {
            Uuid::parse_str(&task.id).with_context(|| {
                format!(
                    "archived task-instance carried an unparseable id `{}`",
                    task.id
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let staging_store = ensure_staging_store(&jetstream::new(nats.clone())).await?;
    let seal_key = staging_key(SEAL_KEY_PREFIX, &projection.id);
    let installation_key = staging_key(INSTALLATION_KEY_PREFIX, &projection.id);
    let archive_commit_key = staging_key(ARCHIVE_COMMIT_KEY_PREFIX, &projection.id);
    let archive_committed_digest = read_stage_value(&staging_store, &archive_commit_key)
        .await?
        .map(|entry| entry.value.to_vec());

    let scope_snapshot = seal_ctx_scope(repositories, scope_reader, projection)
        .await
        .context("seal tickr-ctx scope before final-Log installation")?;
    observe_compaction_boundary(CompactionBoundary::BeforeLogSeal);
    let mut task_log_seals = Vec::with_capacity(task_instance_ids.len());
    let mut missing_log_state = Vec::new();
    for task_instance_id in &task_instance_ids {
        match seal_task_logs(nats, &workflow_id, &workflow_instance_id, task_instance_id)
            .await
            .with_context(|| format!("seal Log staging for task instance {task_instance_id}"))?
        {
            Some(seal) => task_log_seals.push(seal),
            None => missing_log_state.push(*task_instance_id),
        }
    }
    task_log_seals.sort_by_key(|seal| seal.identity().task_instance_id);

    let stored_seal =
        read_staged_json::<CompactionSeal>(&staging_store, &seal_key, "Compaction seal").await?;
    let calculated_seal = if missing_log_state.is_empty() {
        Some(build_compaction_seal(
            workflow_instance_id,
            scope_snapshot.digest.clone(),
            &task_log_seals,
        )?)
    } else {
        None
    };
    let compaction_seal = match (stored_seal, calculated_seal) {
        (Some(stored), Some(calculated)) => {
            let already_committed = archive_committed_digest
                .as_deref()
                .is_some_and(|digest| digest == stored.digest.as_bytes());
            if stored == calculated || already_committed {
                stored
            } else {
                bail!("immutable Compaction seal identity changed");
            }
        }
        (None, Some(calculated)) if archive_committed_digest.is_none() => {
            create_staged_json(&staging_store, &seal_key, &calculated, "Compaction seal").await?;
            calculated
        }
        (Some(stored), None)
            if archive_committed_digest
                .as_deref()
                .is_some_and(|digest| digest == stored.digest.as_bytes()) =>
        {
            stored
        }
        (Some(_), None) => {
            bail!(
                "missing Log staging for {:?} before archive commit",
                missing_log_state
            )
        }
        (None, Some(_)) => bail!("archive commit has no retained immutable Compaction seal"),
        (None, None) => {
            bail!(
                "missing Log staging for {:?} before Compaction seal",
                missing_log_state
            )
        }
    };
    if let Some(committed_digest) = archive_committed_digest.as_deref() {
        if committed_digest != compaction_seal.digest.as_bytes() {
            bail!("archive-commit evidence does not match the immutable Compaction seal");
        }
    }
    if compaction_seal.scope_digest != scope_snapshot.digest {
        bail!("tickr-ctx scope digest changed after the immutable Compaction seal");
    }
    observe_compaction_boundary(CompactionBoundary::AfterLogSeal);

    let stored_installation = read_staged_json::<FinalLogArchiveIdentity>(
        &staging_store,
        &installation_key,
        "final-Log installation",
    )
    .await?;
    let archive_identity = if archive_committed_digest.is_none() && missing_log_state.is_empty() {
        observe_compaction_boundary(CompactionBoundary::BeforeFinalLogInstallation);
        let mut installations = Vec::with_capacity(task_log_seals.len());
        for seal in &task_log_seals {
            installations.push(
                install_task_logs(log_storage, &workflow_id, &workflow_instance_id, seal)
                    .await
                    .with_context(|| {
                        format!(
                            "install final Log for task instance {}",
                            seal.identity().task_instance_id
                        )
                    })?,
            );
        }
        let installed = FinalLogArchiveIdentity {
            compaction_seal_digest: compaction_seal.digest.clone(),
            task_logs: installations,
        };
        create_staged_json(
            &staging_store,
            &installation_key,
            &installed,
            "final-Log installation",
        )
        .await?;
        if let Some(stored) = stored_installation {
            if stored != installed {
                bail!("installed final-Log identity changed across Compaction retry");
            }
        }
        observe_compaction_boundary(CompactionBoundary::AfterFinalLogInstallation);
        installed
    } else {
        stored_installation.ok_or_else(|| {
            anyhow!("purged Log staging has no retained final-Log installation identity")
        })?
    };
    if archive_identity.compaction_seal_digest != compaction_seal.digest {
        bail!("final-Log installation does not match the immutable Compaction seal");
    }

    observe_compaction_boundary(CompactionBoundary::BeforeFinalLogVerification);
    for installation in &archive_identity.task_logs {
        verify_task_log_installation(log_storage, installation)
            .await
            .with_context(|| {
                format!(
                    "verify final Log for task instance {}",
                    installation.task_instance_id
                )
            })?;
    }
    observe_compaction_boundary(CompactionBoundary::AfterFinalLogVerification);

    observe_compaction_boundary(CompactionBoundary::BeforeArchiveCommit);
    persist_compaction_projection_with_scope(
        repositories,
        projection,
        job.shipped_at,
        nats,
        &scope_snapshot,
    )
    .await?;
    create_stage_value(
        &staging_store,
        &archive_commit_key,
        compaction_seal.digest.as_bytes(),
        "archive-commit evidence",
    )
    .await?;
    observe_compaction_boundary(CompactionBoundary::AfterArchiveCommit);

    let applied_patch_keys = projection
        .applied_patches
        .iter()
        .filter_map(|patch| Uuid::parse_str(&patch.patch_key).ok())
        .collect::<HashSet<_>>();
    let discrepancies = repositories
        .audit_patch_settlement(workflow_instance_id, &applied_patch_keys)
        .await
        .with_context(|| format!("patch settlement audit for {workflow_instance_id}"))?;
    for discrepancy in &discrepancies {
        eprintln!(
            "patch_settlement_audit: DISCREPANCY instance={} patch_key={} status={}: {}",
            discrepancy.workflow_instance_id,
            discrepancy.patch_key,
            discrepancy.ledger_status.as_str(),
            discrepancy.detail
        );
    }

    observe_compaction_boundary(CompactionBoundary::BeforeLogPurge);
    let sealed_task_ids = compaction_seal
        .task_logs
        .iter()
        .map(|seal| seal.task_instance_id)
        .collect::<HashSet<_>>();
    if sealed_task_ids != task_instance_ids.iter().copied().collect() {
        bail!("immutable Compaction seal does not cover every Task instance");
    }
    for seal in &compaction_seal.task_logs {
        purge_sealed_task_logs(nats, &workflow_id, &workflow_instance_id, seal)
            .await
            .with_context(|| {
                format!(
                    "purge sealed Log staging for task instance {}",
                    seal.task_instance_id
                )
            })?;
    }
    observe_compaction_boundary(CompactionBoundary::AfterLogPurge);
    Ok(())
}

/// Consume one accepted delivery through only selected role interfaces.
async fn drain_selected_one(
    repositories: &WriterRepositoryBundle,
    delivery: &mut dyn CompactionStagingDelivery,
    log_streams: &dyn CompactionLogStaging,
    scope_store: Arc<dyn ScopeStore>,
    log_storage: &Operator,
) -> Result<Vec<u8>> {
    let payload = delivery.payload();
    let job = decode_job(payload).ok_or_else(|| anyhow!("invalid staged Compaction envelope"))?;
    let projection = &job.projection;
    let workflow_id = Uuid::parse_str(&projection.workflow_id)
        .context("Compaction projection has invalid workflow id")?;
    let workflow_instance_id = Uuid::parse_str(&projection.id)
        .context("Compaction projection has invalid workflow instance id")?;
    let task_instance_ids = projection
        .task_instances
        .iter()
        .map(|task| {
            Uuid::parse_str(&task.id)
                .with_context(|| format!("Compaction projection has invalid Task id `{}`", task.id))
        })
        .collect::<Result<Vec<_>>>()?;

    let (selected_seal, task_log_seals) = match delivery
        .load_seal()
        .await
        .map_err(|error| anyhow!("load immutable Compaction seal: {error}"))?
    {
        Some(role_seal) => {
            let selected: SelectedCompactionSeal = serde_json::from_slice(role_seal.encoded())
                .context("decode immutable selected-role Compaction seal")?;
            let mut task_seals = Vec::with_capacity(task_instance_ids.len());
            for task_instance_id in &task_instance_ids {
                let seals = selected
                    .logs
                    .iter()
                    .filter(|seal| seal.stream().task_instance_id == *task_instance_id)
                    .cloned()
                    .collect();
                task_seals.push(
                    task_log_seal_from_role(*task_instance_id, seals)?
                        .ok_or_else(|| anyhow!("immutable Compaction seal omits Task Log"))?,
                );
            }
            let rebuilt = build_compaction_seal(
                workflow_instance_id,
                selected.scope.digest.clone(),
                &task_seals,
            )?;
            if rebuilt != selected.compaction
                || role_seal.digest() != selected.compaction.digest
                || role_seal.source_references()
                    != u64::try_from(selected.logs.len())
                        .ok()
                        .and_then(|count| count.checked_add(1))
                        .ok_or_else(|| anyhow!("Compaction seal reference count overflow"))?
            {
                bail!("immutable selected-role Compaction seal evidence disagrees");
            }
            (selected, task_seals)
        }
        None => {
            observe_compaction_boundary(CompactionBoundary::BeforeLogSeal);
            let scope_reader = RoleCompactionScopeSnapshotReader::new(scope_store.clone());
            let scope = scope_reader.snapshot(repositories, projection).await?;
            let mut logs = Vec::new();
            let mut task_seals = Vec::with_capacity(task_instance_ids.len());
            for task_instance_id in &task_instance_ids {
                let task_logs = log_streams
                    .seal_task(workflow_id, workflow_instance_id, *task_instance_id)
                    .await
                    .with_context(|| {
                        format!("seal selected Log staging for Task {task_instance_id}")
                    })?;
                task_seals.push(
                    task_log_seal_from_role(*task_instance_id, task_logs.clone())?
                        .ok_or_else(|| anyhow!("selected Log staging has no terminal Task Log"))?,
                );
                logs.extend(task_logs);
            }
            let compaction =
                build_compaction_seal(workflow_instance_id, scope.digest.clone(), &task_seals)?;
            let selected = SelectedCompactionSeal {
                scope,
                logs,
                compaction,
            };
            let encoded = serde_json::to_vec(&selected)
                .context("encode immutable selected Compaction seal")?;
            let source_references = u64::try_from(selected.logs.len())
                .ok()
                .and_then(|count| count.checked_add(1))
                .ok_or_else(|| anyhow!("Compaction seal reference count overflow"))?;
            delivery
                .record_seal(&CompactionStagingSeal::new(
                    encoded,
                    selected.compaction.digest.clone(),
                    source_references,
                ))
                .await
                .map_err(|error| anyhow!("record immutable Compaction seal: {error}"))?;
            observe_compaction_boundary(CompactionBoundary::AfterLogSeal);
            (selected, task_seals)
        }
    };

    let archive_bytes = match delivery
        .load_archive_identity()
        .await
        .map_err(|error| anyhow!("load Compaction archive-commit evidence: {error}"))?
    {
        Some(bytes) => bytes,
        None => {
            observe_compaction_boundary(CompactionBoundary::BeforeFinalLogInstallation);
            let mut installations = Vec::with_capacity(task_log_seals.len());
            for seal in &task_log_seals {
                installations.push(
                    install_task_logs(log_storage, &workflow_id, &workflow_instance_id, seal)
                        .await
                        .with_context(|| {
                            format!(
                                "install final Log for task instance {}",
                                seal.identity().task_instance_id
                            )
                        })?,
                );
            }
            let archive = FinalLogArchiveIdentity {
                compaction_seal_digest: selected_seal.compaction.digest.clone(),
                task_logs: installations,
            };
            observe_compaction_boundary(CompactionBoundary::AfterFinalLogInstallation);
            observe_compaction_boundary(CompactionBoundary::BeforeFinalLogVerification);
            for installation in &archive.task_logs {
                verify_task_log_installation(log_storage, installation).await?;
            }
            observe_compaction_boundary(CompactionBoundary::AfterFinalLogVerification);
            observe_compaction_boundary(CompactionBoundary::BeforeArchiveCommit);
            persist_compaction_projection_with_selected_scope(
                repositories,
                projection,
                job.shipped_at,
                &selected_seal.scope,
            )
            .await?;
            let bytes =
                serde_json::to_vec(&archive).context("encode final-Log archive identity")?;
            delivery
                .record_archive_identity(&bytes)
                .await
                .map_err(|error| anyhow!("record Compaction archive commit: {error}"))?;
            observe_compaction_boundary(CompactionBoundary::AfterArchiveCommit);
            bytes
        }
    };
    let archive: FinalLogArchiveIdentity =
        serde_json::from_slice(&archive_bytes).context("decode final-Log archive identity")?;
    if archive.compaction_seal_digest != selected_seal.compaction.digest {
        bail!("final-Log archive identity does not match immutable Compaction seal");
    }
    for installation in &archive.task_logs {
        verify_task_log_installation(log_storage, installation).await?;
    }

    let applied_patch_keys = projection
        .applied_patches
        .iter()
        .filter_map(|patch| Uuid::parse_str(&patch.patch_key).ok())
        .collect::<HashSet<_>>();
    let discrepancies = repositories
        .audit_patch_settlement(workflow_instance_id, &applied_patch_keys)
        .await
        .with_context(|| format!("patch settlement audit for {workflow_instance_id}"))?;
    for discrepancy in &discrepancies {
        eprintln!(
            "patch_settlement_audit: DISCREPANCY instance={} patch_key={} status={}: {}",
            discrepancy.workflow_instance_id,
            discrepancy.patch_key,
            discrepancy.ledger_status.as_str(),
            discrepancy.detail
        );
    }

    observe_compaction_boundary(CompactionBoundary::BeforeLogPurge);
    for task_instance_id in &task_instance_ids {
        let seals = selected_seal
            .logs
            .iter()
            .filter(|seal| seal.stream().task_instance_id == *task_instance_id)
            .cloned()
            .collect::<Vec<_>>();
        log_streams
            .purge_task_after_archive(
                workflow_id,
                workflow_instance_id,
                *task_instance_id,
                &seals,
                &archive_bytes,
            )
            .await
            .with_context(|| format!("purge selected Log staging for Task {task_instance_id}"))?;
    }
    observe_compaction_boundary(CompactionBoundary::AfterLogPurge);

    let scope_id = selected_seal
        .scope
        .scope_id
        .ok_or_else(|| anyhow!("selected ScopeStore seal has no stable scope identity"))?;
    scope_store
        .record_verified_archive_commit(
            scope_id,
            &selected_seal.scope.digest,
            &archive_bytes,
            Utc::now(),
        )
        .await
        .map_err(|error| anyhow!("record selected ScopeStore archive evidence: {error}"))?;
    observe_compaction_boundary(CompactionBoundary::BeforeScopeCleanup);
    scope_store
        .cleanup_after_verified_archive_commit(
            scope_id,
            &selected_seal.scope.digest,
            &archive_bytes,
            Utc::now(),
        )
        .await
        .map_err(|error| anyhow!("clean selected ScopeStore state: {error}"))?;
    observe_compaction_boundary(CompactionBoundary::AfterScopeCleanup);
    Ok(archive_bytes)
}

/// Drain Compaction through the selected staging, Log, and Scope role interfaces.
pub async fn run_selected_compaction_drain(
    staging: Arc<dyn CompactionStaging>,
    log_streams: Arc<dyn CompactionLogStaging>,
    scope_store: Arc<dyn ScopeStore>,
    repositories: Arc<WriterRepositoryBundle>,
    log_storage: Operator,
    shutdown_token: CancellationToken,
) -> Result<()> {
    staging
        .prepare()
        .await
        .map_err(|error| anyhow!("prepare selected Compaction staging: {error}"))?;
    loop {
        let delivery = tokio::select! {
            _ = shutdown_token.cancelled() => break,
            result = staging.next() => result
                .map_err(|error| anyhow!("receive selected Compaction delivery: {error}"))?,
        };
        let Some(mut delivery) = delivery else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        observe_compaction_boundary(CompactionBoundary::BeforeDrainReceipt);
        let result = drain_selected_one(
            &repositories,
            delivery.as_mut(),
            log_streams.as_ref(),
            scope_store.clone(),
            &log_storage,
        )
        .await;
        observe_compaction_boundary(CompactionBoundary::AfterDrainReceipt);
        match result {
            Ok(archive_identity) => delivery
                .complete(&archive_identity)
                .await
                .map_err(|error| anyhow!("complete selected Compaction staging: {error}"))?,
            Err(error) => {
                delivery
                    .retry(Some(Duration::from_millis(100)))
                    .await
                    .map_err(|retry| anyhow!("retry selected Compaction delivery: {retry}"))?;
                eprintln!("compaction drain failed: {error:#} (delivery released)");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    Ok(())
}

/// Run the all-NATS compaction drain worker until shutdown. Per job: `drain_one`,
/// then ack. A failed job is NAK'd so the queue redelivers it (to this or
/// any other conductor instance); a job whose payload no longer
/// deserializes is dropped with a loud error — staged bytes were
/// deserialized once already on the relay path, so a poison job here means
/// corruption, and NAK-forever would only redeliver it eternally.
pub async fn run_compaction_drain(
    nats: NatsClient,
    repositories: Arc<WriterRepositoryBundle>,
    log_storage: Operator,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let scope_reader = Arc::new(open_nats_scope_reader(&nats).await?);
    run_compaction_drain_with_scope_reader(
        nats,
        repositories,
        log_storage,
        scope_reader,
        shutdown_token,
    )
    .await
}

/// Run the NATS staging worker with a formation-selected immutable scope reader.
/// The worker cannot open either the NATS or Redis scope substrate itself.
pub async fn run_compaction_drain_with_scope_reader(
    nats: NatsClient,
    repositories: Arc<WriterRepositoryBundle>,
    log_storage: Operator,
    scope_reader: Arc<dyn CompactionScopeSnapshotReader>,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let consumer = init_stream_and_consumer(&nats).await?;
    println!(
        "compaction_drain: worker started, subject={}, stream={}, consumer={}",
        SUBJECT, STREAM_NAME, CONSUMER_NAME
    );

    let mut messages = consumer
        .stream()
        .max_messages_per_batch(4)
        .messages()
        .await
        .context("opening compaction drain message stream")?;

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                println!("compaction_drain: shutdown signal received");
                break;
            }
            next = messages.next() => {
                match next {
                    Some(Ok(msg)) => {
                        observe_compaction_boundary(CompactionBoundary::BeforeDrainReceipt);
                        observe_compaction_boundary(CompactionBoundary::AfterDrainReceipt);
                        // Decode the proto envelope; a job that does not decode
                        // is a poison drop.
                        let job = match decode_job(&msg.payload) {
                            Some(j) => j,
                            None => {
                                eprintln!(
                                    "compaction_drain: dropping undeserializable job ({} bytes)",
                                    msg.payload.len(),
                                );
                                if let Err(e) = msg.ack().await {
                                    eprintln!("compaction_drain: ack of poison job failed: {}", e);
                                }
                                continue;
                            }
                        };
                        let wfi_id = job.projection.id.clone();
                        let staging_store =
                            ensure_staging_store(&jetstream::new(nats.clone())).await?;
                        if let Err(error) =
                            validate_staged_payload(&staging_store, &wfi_id, &msg.payload).await
                        {
                            eprintln!(
                                "compaction_drain: staged payload validation failed for {}: {error:#} (NAK; queue redelivers)",
                                wfi_id
                            );
                            if let Err(error) = msg
                                .ack_with(jetstream::AckKind::Nak(None))
                                .await
                            {
                                eprintln!("compaction_drain: NAK failed: {}", error);
                            }
                            continue;
                        }
                        match drain_one(
                            &repositories,
                            &nats,
                            &log_storage,
                            scope_reader.as_ref(),
                            &job,
                        )
                        .await
                        {
                            Ok(()) => {
                                observe_compaction_boundary(CompactionBoundary::BeforeScopeCleanup);
                                if let Err(error) = cleanup_ctx_scope(&nats, &wfi_id).await {
                                    eprintln!(
                                        "compaction_drain: archived scope cleanup failed for {}: {error:#} (NAK; queue redelivers)",
                                        wfi_id
                                    );
                                    if let Err(error) =
                                        msg.ack_with(jetstream::AckKind::Nak(None)).await
                                    {
                                        eprintln!("compaction_drain: NAK failed: {}", error);
                                    }
                                    continue;
                                }
                                observe_compaction_boundary(CompactionBoundary::AfterScopeCleanup);
                                if let Err(error) =
                                    complete_compaction_staging(&nats, &wfi_id, &msg.payload).await
                                {
                                    eprintln!(
                                        "compaction_drain: staging completion failed for {}: {error:#} (NAK; queue redelivers)",
                                        wfi_id
                                    );
                                    if let Err(error) =
                                        msg.ack_with(jetstream::AckKind::Nak(None)).await
                                    {
                                        eprintln!("compaction_drain: NAK failed: {}", error);
                                    }
                                    continue;
                                }
                                if let Err(error) = msg.ack().await {
                                    eprintln!(
                                        "compaction_drain: ack failed for {}: {} (redelivery converges)",
                                        wfi_id, error
                                    );
                                } else {
                                    println!(
                                        "compaction_drain: archived terminal workflow {} (state={})",
                                        wfi_id, job.projection.state
                                    );
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "compaction_drain: archival failed for {}: {e:#} (NAK; queue redelivers)",
                                    wfi_id
                                );
                                if let Err(e) = msg
                                    .ack_with(jetstream::AckKind::Nak(None))
                                    .await
                                {
                                    eprintln!("compaction_drain: NAK failed: {}", e);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("compaction_drain: pull error: {}", e);
                        // Brief sleep so a persistent NATS-side fault doesn't tight-loop.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    None => {
                        // Stream ended cleanly — happens only when the NATS
                        // connection drops; the consumer's reconnect machinery
                        // handles re-establishment on the next worker start.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::fs;
    use std::process::{Command, Stdio};
    use std::time::Instant;

    use prost::Message;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, PgConnection, PgPool};
    use tempfile::NamedTempFile;
    use testcontainers_modules::minio::MinIO;
    use testcontainers_modules::nats::{Nats, NatsServerCmd};
    use testcontainers_modules::testcontainers::core::ExecCommand;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::ImageExt;
    use tickr_ctx::nats_scope::{NatsScopeError, NatsScopeStore};
    use tickr_migrations::{apply_target, MigrationTarget};
    use tickr_proto::archive::{ArchiveProjection, CompactionEnvelope};
    use tickr_proto::coord::log_stream::content_digest;
    use tickr_proto::instance::SnapshotTaskInstance;

    use super::*;
    use crate::relay::stage_compaction_and_send_ack;

    const CHILD_ENV: &str = "TICKR_TEST_COMPACTION_CHILD";
    const NATS_ENV: &str = "TICKR_TEST_COMPACTION_NATS_URL";
    const DATABASE_ENV: &str = "TICKR_TEST_COMPACTION_DATABASE_URL";
    const PAYLOAD_ENV: &str = "TICKR_TEST_COMPACTION_PAYLOAD_PATH";

    async fn start_nats() -> Option<(
        testcontainers_modules::testcontainers::ContainerAsync<Nats>,
        String,
    )> {
        let command = NatsServerCmd::default().with_jetstream();
        let container = match Nats::default()
            .with_tag("2.11.11")
            .with_cmd(&command)
            .start()
            .await
        {
            Ok(container) => container,
            Err(error) => {
                eprintln!("skipping real-process Compaction crashes: {error}");
                return None;
            }
        };
        let port = container.get_host_port_ipv4(4222).await.ok()?;
        let url = format!("nats://127.0.0.1:{port}");
        for _ in 0..20 {
            if async_nats::connect(&url).await.is_ok() {
                return Some((container, url));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("isolated NATS started but did not accept connections");
    }

    async fn start_minio() -> Option<(
        testcontainers_modules::testcontainers::ContainerAsync<MinIO>,
        String,
        String,
        String,
    )> {
        let credential_nonce = Uuid::new_v4().simple().to_string();
        let access_key = format!("TICKR{}", &credential_nonce[..15]);
        let secret_key = format!("tickr-test-{credential_nonce}");
        let container = match MinIO::default()
            .with_env_var("MINIO_ROOT_USER", access_key.clone())
            .with_env_var("MINIO_ROOT_PASSWORD", secret_key.clone())
            .start()
            .await
        {
            Ok(container) => container,
            Err(error) => {
                eprintln!("skipping real-process Compaction crashes: {error}");
                return None;
            }
        };
        let mut create_bucket = container
            .exec(ExecCommand::new(["mkdir", "-p", "/data/tickr-logs"]))
            .await
            .ok()?;
        create_bucket.stdout_to_vec().await.ok()?;
        if create_bucket.exit_code().await.ok().flatten() != Some(0) {
            eprintln!("skipping real-process Compaction crashes: cannot create MinIO bucket");
            return None;
        }
        let port = container.get_host_port_ipv4(9000).await.ok()?;
        let endpoint = format!("http://127.0.0.1:{port}");
        let storage = test_log_storage(&endpoint, &access_key, &secret_key).ok()?;
        for _ in 0..20 {
            if storage.write("readiness", b"ready".to_vec()).await.is_ok() {
                storage.delete("readiness").await.ok()?;
                return Some((container, endpoint, access_key, secret_key));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        eprintln!("skipping real-process Compaction crashes: MinIO bucket is unavailable");
        None
    }

    fn test_log_storage(endpoint: &str, access_key: &str, secret_key: &str) -> Result<Operator> {
        let builder = opendal::services::S3::default()
            .bucket("tickr-logs")
            .endpoint(endpoint)
            .access_key_id(access_key)
            .secret_access_key(secret_key)
            .region("us-east-1");
        Ok(Operator::new(builder)?.finish())
    }

    async fn create_database() -> Option<(String, PgPool)> {
        let base = match std::env::var("TICKR_TEST_PG_URL") {
            Ok(base) => base,
            Err(_) => {
                eprintln!("skipping real-process Compaction crashes: TICKR_TEST_PG_URL is not set");
                return None;
            }
        };
        let admin_url = format!("{base}/postgres");
        let mut admin = PgConnection::connect(&admin_url)
            .await
            .expect("connect shared test Postgres");
        let database_name = format!("t_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
            .execute(&mut admin)
            .await
            .expect("create Compaction crash-test database");
        let database_url = format!("{base}/{database_name}");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("connect Compaction crash-test database");
        apply_target(MigrationTarget::Conductor, &pool)
            .await
            .expect("migrate Compaction crash-test database");
        Some((database_url, pool))
    }

    fn compaction_payload(workflow_instance_id: Uuid) -> (Vec<u8>, Uuid, Uuid) {
        let workflow_id = Uuid::new_v4();
        let task_instance_id = Uuid::new_v4();
        let payload = CompactionEnvelope {
            projection: Some(ArchiveProjection {
                id: workflow_instance_id.to_string(),
                workflow_id: workflow_id.to_string(),
                name: format!("crash-law-{workflow_instance_id}"),
                state: "Completed".to_owned(),
                scheduled_at: Some(Utc::now().to_rfc3339()),
                task_instances: vec![SnapshotTaskInstance {
                    id: task_instance_id.to_string(),
                    task_id: Uuid::new_v4().to_string(),
                    name: "crash-task".to_owned(),
                    task_type: "Regular".to_owned(),
                    state: "Completed".to_owned(),
                    executor_id: Some(Uuid::new_v4().to_string()),
                    attempt: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            correlation: format!("correlation-{workflow_instance_id}"),
            shipped_at: Some(Utc::now().to_rfc3339()),
        }
        .encode_to_vec();
        (payload, workflow_id, task_instance_id)
    }

    async fn seed_empty_scope(nats: &NatsClient, workflow_instance_id: Uuid) -> Result<()> {
        let js = jetstream::new(nats.clone());
        let store = match js
            .get_key_value(tickr_proto::coord::all_nats::DEFAULT_SCOPE_BUCKET)
            .await
        {
            Ok(store) => store,
            Err(_) => {
                js.create_key_value(kv::Config {
                    bucket: tickr_proto::coord::all_nats::DEFAULT_SCOPE_BUCKET.to_owned(),
                    history: 1,
                    storage: stream::StorageType::File,
                    ..Default::default()
                })
                .await?
            }
        };
        NatsScopeStore::new(store, "default")?
            .ensure_scope(&workflow_instance_id.to_string())
            .await?;
        Ok(())
    }

    async fn seed_terminal_log(
        nats: &NatsClient,
        workflow_id: Uuid,
        workflow_instance_id: Uuid,
        task_instance_id: Uuid,
    ) -> Result<()> {
        let js = jetstream::new(nats.clone());
        js.get_or_create_stream(stream::Config {
            name: tickr_proto::coord::all_nats::LOG_STREAM.to_owned(),
            subjects: vec![tickr_proto::coord::all_nats::LOG_STREAM_SUBJECTS.to_owned()],
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await?;
        let subject = format!(
            "{}.{workflow_id}.{workflow_instance_id}.{task_instance_id}",
            tickr_proto::coord::all_nats::LOG_SUBJECT_PREFIX
        );
        let mut accepted_headers = HeaderMap::new();
        accepted_headers.insert(
            tickr_proto::coord::all_nats::LOG_PROTOCOL_HEADER,
            tickr_proto::coord::all_nats::LOG_PROTOCOL,
        );
        accepted_headers.insert(
            tickr_proto::coord::all_nats::LOG_KIND_HEADER,
            tickr_proto::coord::all_nats::LOG_KIND_ACCEPTED,
        );
        accepted_headers.insert(
            tickr_proto::coord::all_nats::LOG_TASK_INSTANCE_HEADER,
            task_instance_id.to_string().as_str(),
        );
        accepted_headers.insert(
            tickr_proto::coord::all_nats::LOG_PICKUP_GENERATION_HEADER,
            "1",
        );
        accepted_headers.insert(tickr_proto::coord::all_nats::LOG_SEQUENCE_HEADER, "0");
        let accepted_bytes = format!("accepted-{task_instance_id}").into_bytes();
        accepted_headers.insert(
            tickr_proto::coord::all_nats::LOG_CONTENT_DIGEST_HEADER,
            content_digest(&accepted_bytes).as_str(),
        );
        accepted_headers.insert(
            MESSAGE_ID_HEADER,
            format!("log:{task_instance_id}:1:record:0").as_str(),
        );
        js.publish_with_headers(subject.clone(), accepted_headers, accepted_bytes.into())
            .await?
            .await?;

        let mut gap_headers = HeaderMap::new();
        gap_headers.insert(
            tickr_proto::coord::all_nats::LOG_PROTOCOL_HEADER,
            tickr_proto::coord::all_nats::LOG_PROTOCOL,
        );
        gap_headers.insert(
            tickr_proto::coord::all_nats::LOG_KIND_HEADER,
            tickr_proto::coord::all_nats::LOG_KIND_GAP,
        );
        gap_headers.insert(
            tickr_proto::coord::all_nats::LOG_TASK_INSTANCE_HEADER,
            task_instance_id.to_string().as_str(),
        );
        gap_headers.insert(
            tickr_proto::coord::all_nats::LOG_PICKUP_GENERATION_HEADER,
            "1",
        );
        gap_headers.insert(tickr_proto::coord::all_nats::LOG_GAP_FIRST_HEADER, "1");
        gap_headers.insert(tickr_proto::coord::all_nats::LOG_GAP_LAST_HEADER, "1");
        gap_headers.insert(tickr_proto::coord::all_nats::LOG_GAP_DROPPED_HEADER, "1");
        gap_headers.insert(
            MESSAGE_ID_HEADER,
            format!("log:{task_instance_id}:1:gap:1:1").as_str(),
        );
        js.publish_with_headers(subject.clone(), gap_headers, Vec::new().into())
            .await?
            .await?;

        let mut terminal_headers = HeaderMap::new();
        terminal_headers.insert(
            tickr_proto::coord::all_nats::LOG_PROTOCOL_HEADER,
            tickr_proto::coord::all_nats::LOG_PROTOCOL,
        );
        terminal_headers.insert(
            tickr_proto::coord::all_nats::LOG_KIND_HEADER,
            tickr_proto::coord::all_nats::LOG_KIND_END,
        );
        terminal_headers.insert(
            tickr_proto::coord::all_nats::LOG_TASK_INSTANCE_HEADER,
            task_instance_id.to_string().as_str(),
        );
        terminal_headers.insert(
            tickr_proto::coord::all_nats::LOG_PICKUP_GENERATION_HEADER,
            "1",
        );
        terminal_headers.insert(tickr_proto::coord::all_nats::LOG_EXIT_KIND_HEADER, "status");
        terminal_headers.insert(tickr_proto::coord::all_nats::LOG_EXIT_STATUS_HEADER, "0");
        terminal_headers.insert(
            MESSAGE_ID_HEADER,
            format!("log:{task_instance_id}:1:terminal").as_str(),
        );
        js.publish_with_headers(subject, terminal_headers, Vec::new().into())
            .await?
            .await?;
        Ok(())
    }

    fn run_child(
        boundary: Option<&str>,
        nats_url: &str,
        database_url: &str,
        log_storage_endpoint: &str,
        log_storage_access_key: &str,
        log_storage_secret_key: &str,
        payload_path: &std::path::Path,
    ) -> std::process::ExitStatus {
        let mut command = Command::new(std::env::current_exe().expect("current test executable"));
        command
            .arg("--exact")
            .arg("system_tasks::compaction_drain::tests::child_all_nats_compaction_process")
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .env(NATS_ENV, nats_url)
            .env(DATABASE_ENV, database_url)
            .env(PAYLOAD_ENV, payload_path)
            .env("TICKR_LOG_STORAGE_ENDPOINT", log_storage_endpoint)
            .env("TICKR_LOG_STORAGE_BUCKET", "tickr-logs")
            .env("TICKR_LOG_STORAGE_ACCESS_KEY_ID", log_storage_access_key)
            .env(
                "TICKR_LOG_STORAGE_SECRET_ACCESS_KEY",
                log_storage_secret_key,
            )
            .env("TICKR_LOG_STORAGE_REGION", "us-east-1")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        match boundary {
            Some(boundary) => {
                command.env("TICKR_TEST_COMPACTION_CRASH_BOUNDARY", boundary);
            }
            None => {
                command.env_remove("TICKR_TEST_COMPACTION_CRASH_BOUNDARY");
            }
        }
        command.status().expect("run Compaction child process")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn child_all_nats_compaction_process() -> Result<()> {
        if std::env::var(CHILD_ENV).as_deref() != Ok("1") {
            return Ok(());
        }

        let nats = async_nats::connect(std::env::var(NATS_ENV)?).await?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&std::env::var(DATABASE_ENV)?)
            .await?;
        let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool));
        let payload = fs::read(std::env::var(PAYLOAD_ENV)?)?;

        let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(1);
        stage_compaction_and_send_ack(&nats, payload, &ack_tx).await?;
        tokio::time::timeout(Duration::from_secs(5), ack_rx.recv())
            .await?
            .ok_or_else(|| anyhow!("CompactionAck channel closed"))?;

        let shutdown = CancellationToken::new();
        let drain = tokio::spawn(run_compaction_drain(
            nats.clone(),
            repositories,
            crate::system_tasks::log_uploader::production_log_storage()?,
            shutdown.clone(),
        ));
        let js = jetstream::new(nats);
        let mut stream = js.get_stream(STREAM_NAME).await?;
        let deadline = Instant::now() + Duration::from_secs(20);
        while stream.info().await?.state.messages > 0 {
            if Instant::now() >= deadline {
                return Err(anyhow!("Compaction queue did not complete after recovery"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        shutdown.cancel();
        drain.await??;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_process_crashes_at_every_compaction_boundary_converge() -> Result<()> {
        let Some((_nats_container, nats_url)) = start_nats().await else {
            return Ok(());
        };
        let Some((database_url, pool)) = create_database().await else {
            return Ok(());
        };
        let Some((
            _minio_container,
            log_storage_endpoint,
            log_storage_access_key,
            log_storage_secret_key,
        )) = start_minio().await
        else {
            return Ok(());
        };
        let log_storage = test_log_storage(
            &log_storage_endpoint,
            &log_storage_access_key,
            &log_storage_secret_key,
        )?;
        let nats = async_nats::connect(&nats_url).await?;
        let boundaries = [
            "before-staging-mutation",
            "after-staging-mutation",
            "before-durability-proof",
            "after-durability-proof",
            "before-cross-plane-acknowledgement",
            "after-cross-plane-acknowledgement",
            "before-drain-receipt",
            "after-drain-receipt",
            "before-log-seal",
            "after-log-seal",
            "before-final-log-installation",
            "after-final-log-installation",
            "before-final-log-verification",
            "after-final-log-verification",
            "before-archive-commit",
            "after-archive-commit",
            "before-log-purge",
            "after-log-purge",
            "before-scope-cleanup",
            "after-scope-cleanup",
            "before-staging-completion",
            "after-staging-completion",
        ];

        for (completed_boundaries, boundary) in boundaries.into_iter().enumerate() {
            let workflow_instance_id = Uuid::new_v4();
            let (payload, workflow_id, task_instance_id) = compaction_payload(workflow_instance_id);
            seed_empty_scope(&nats, workflow_instance_id).await?;
            seed_terminal_log(&nats, workflow_id, workflow_instance_id, task_instance_id).await?;
            let payload_file = NamedTempFile::new()?;
            fs::write(payload_file.path(), &payload)?;

            let crashed = run_child(
                Some(boundary),
                &nats_url,
                &database_url,
                &log_storage_endpoint,
                &log_storage_access_key,
                &log_storage_secret_key,
                payload_file.path(),
            );
            assert_eq!(
                crashed.code(),
                Some(86),
                "child must crash at `{boundary}`, got {crashed}"
            );

            let recovered = run_child(
                None,
                &nats_url,
                &database_url,
                &log_storage_endpoint,
                &log_storage_access_key,
                &log_storage_secret_key,
                payload_file.path(),
            );
            assert!(
                recovered.success(),
                "recovery after `{boundary}` failed with {recovered}"
            );

            let archived: i64 =
                sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = $1")
                    .bind(workflow_instance_id)
                    .fetch_one(&pool)
                    .await?;
            assert_eq!(archived, 1, "`{boundary}` must converge on one archive");

            let js = jetstream::new(nats.clone());
            let store = js.get_key_value(COMPACTION_STAGING_BUCKET).await?;
            let complete_key = staging_key(COMPLETE_KEY_PREFIX, &workflow_instance_id.to_string());
            assert_eq!(
                store.get(&complete_key).await?.as_deref(),
                Some(digest(&payload).as_bytes()),
                "`{boundary}` must retain stable completion evidence"
            );
            let payload_key = staging_key(PAYLOAD_KEY_PREFIX, &workflow_instance_id.to_string());
            assert!(
                store.get(&payload_key).await?.is_none(),
                "`{boundary}` must clean raw staging only after archive commit"
            );

            let seal_key = staging_key(SEAL_KEY_PREFIX, &workflow_instance_id.to_string());
            let seal_bytes = store
                .get(&seal_key)
                .await?
                .ok_or_else(|| anyhow!("missing immutable Compaction seal"))?;
            let seal: CompactionSeal = serde_json::from_slice(&seal_bytes)?;
            assert_eq!(seal.workflow_instance_id, workflow_instance_id);
            assert_eq!(seal.task_logs.len(), 1);
            assert_eq!(seal.task_logs[0].task_instance_id, task_instance_id);
            assert_eq!(seal.task_logs[0].terminal_fences.len(), 1);

            let installation_key =
                staging_key(INSTALLATION_KEY_PREFIX, &workflow_instance_id.to_string());
            let installation_bytes = store
                .get(&installation_key)
                .await?
                .ok_or_else(|| anyhow!("missing final-Log installation identity"))?;
            let installation: FinalLogArchiveIdentity =
                serde_json::from_slice(&installation_bytes)?;
            assert_eq!(installation.compaction_seal_digest, seal.digest);
            assert_eq!(installation.task_logs.len(), 1);
            verify_task_log_installation(&log_storage, &installation.task_logs[0]).await?;

            let commit_key =
                staging_key(ARCHIVE_COMMIT_KEY_PREFIX, &workflow_instance_id.to_string());
            assert_eq!(
                store.get(&commit_key).await?.as_deref(),
                Some(seal.digest.as_bytes()),
                "`{boundary}` must gate purge on verified archive commit"
            );

            let scope_store = NatsScopeStore::new(
                js.get_key_value(tickr_proto::coord::all_nats::DEFAULT_SCOPE_BUCKET)
                    .await?,
                "default",
            )?;
            assert!(matches!(
                scope_store
                    .snapshot(&workflow_instance_id.to_string())
                    .await,
                Err(NatsScopeError::Cleaned { .. })
            ));

            let duplicate = run_child(
                None,
                &nats_url,
                &database_url,
                &log_storage_endpoint,
                &log_storage_access_key,
                &log_storage_secret_key,
                payload_file.path(),
            );
            assert!(
                duplicate.success(),
                "duplicate archive after `{boundary}` failed with {duplicate}"
            );
            assert_eq!(
                store.get(&seal_key).await?.as_deref(),
                Some(seal_bytes.as_ref()),
                "unchanged Compaction state must retain one seal identity"
            );
            let mut stream = js.get_stream(STREAM_NAME).await?;
            assert_eq!(
                stream.info().await?.state.messages,
                0,
                "`{boundary}` must complete the queue exactly once"
            );
            let mut log_stream = js
                .get_stream(tickr_proto::coord::all_nats::LOG_STREAM)
                .await?;
            assert_eq!(
                log_stream.info().await?.state.messages,
                (completed_boundaries + 1) as u64,
                "`{boundary}` must purge Accepted Log records and retain one terminal fence"
            );
        }
        Ok(())
    }
}
