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
use anyhow::{anyhow, Context, Result};
use async_nats::{jetstream, Client as NatsClient};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tickr_ctx::nats_scope::{
    snapshot_from_entries, NatsScopeEntry, NatsScopeError, NatsScopeSnapshot, NatsScopeStore,
};
use tickr_migrations::archive_repository::ArchiveTerminalWorkflowInput;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::scope_repository::{
    decode_tickr_ctx_scope_snapshot, ScopeSnapshotOutcome, ScopeStore, TickrCtxScopeSnapshot,
};
use tickr_proto::archive as ap;
use uuid::Uuid;

/// MinIO bucket that `log_uploader` writes per-task gzip blobs to. Mirrors
/// `log_uploader::STORAGE_BUCKET` so URI derivation here matches the writer.
const LOG_STORAGE_BUCKET: &str = "tickr-logs";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactionScopeEntry {
    pub key: String,
    pub envelope: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactionScopeSnapshot {
    pub scope_id: Option<Uuid>,
    pub owner: String,
    pub entries: Vec<CompactionScopeEntry>,
    pub bytes: Vec<u8>,
    pub digest: String,
    pub value_bytes: usize,
}

#[async_trait]
pub trait CompactionScopeSnapshotReader: Send + Sync {
    async fn snapshot(
        &self,
        repository: &WriterRepositoryBundle,
        projection: &ap::ArchiveProjection,
    ) -> Result<CompactionScopeSnapshot>;
}

pub(crate) struct NatsCompactionScopeSnapshotReader {
    store: NatsScopeStore,
}

pub struct RoleCompactionScopeSnapshotReader {
    store: std::sync::Arc<dyn ScopeStore>,
}

impl RoleCompactionScopeSnapshotReader {
    pub fn new(store: std::sync::Arc<dyn ScopeStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl CompactionScopeSnapshotReader for NatsCompactionScopeSnapshotReader {
    async fn snapshot(
        &self,
        repository: &WriterRepositoryBundle,
        projection: &ap::ArchiveProjection,
    ) -> Result<CompactionScopeSnapshot> {
        let workflow_instance_id = workflow_instance_id(projection)?;
        let owner = tickr_ctx::scope::sanitize_segment(&projection.id);
        match self.store.snapshot(&owner).await {
            Ok(snapshot) => Ok(nats_snapshot(snapshot)),
            Err(NatsScopeError::Cleaned { digest, .. }) => {
                let run_info = repository
                    .archive_run_info(workflow_instance_id)
                    .await
                    .context("read committed scope archive for Compaction redelivery")?
                    .ok_or_else(|| {
                        anyhow!(
                            "cleaned tickr-ctx scope `{owner}` has no committed archive enrichment"
                        )
                    })?;
                snapshot_from_archive(&owner, &digest, &run_info.ctx_envelope)
            }
            Err(error) => Err(error).context("snapshot fresh all-NATS ScopeStore"),
        }
    }
}

#[async_trait]
impl CompactionScopeSnapshotReader for RoleCompactionScopeSnapshotReader {
    async fn snapshot(
        &self,
        _repository: &WriterRepositoryBundle,
        projection: &ap::ArchiveProjection,
    ) -> Result<CompactionScopeSnapshot> {
        let outcome = self
            .store
            .snapshot_tickr_ctx_scope_for_run(&ctx_namespace(), &projection.id, Utc::now())
            .await
            .map_err(|error| anyhow!("snapshot selected ScopeStore: {error}"))?;
        let snapshot = match outcome {
            ScopeSnapshotOutcome::Committed(snapshot)
            | ScopeSnapshotOutcome::Idempotent(snapshot) => snapshot,
            ScopeSnapshotOutcome::Missing => {
                return Err(anyhow!("tickr-ctx scope `{}` is missing", projection.id));
            }
            ScopeSnapshotOutcome::Bound(bound) => {
                return Err(anyhow!("tickr-ctx scope exceeded a bound: {bound:?}"));
            }
            ScopeSnapshotOutcome::Quarantined { diagnostic, .. } => {
                return Err(anyhow!("tickr-ctx scope is quarantined: {diagnostic}"));
            }
        };
        common_snapshot(snapshot, &projection.id)
    }
}

/// Persist the compaction payload through the terminal-archive repository.
///
/// The selected repository owns the archive transaction and terminal audit
/// persistence. A distributed Compaction must first seal a readable tickr-ctx
/// scope; absence, corruption, or an unreadable role store fails the drain.
pub async fn persist_compaction_projection(
    repository: &WriterRepositoryBundle,
    projection: &ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
    nats: Option<&NatsClient>,
) -> Result<()> {
    let scope_snapshot = match nats {
        Some(client) => {
            let reader = open_nats_scope_reader(client)
                .await
                .context("seal tickr-ctx scope before Compaction archive")?;
            Some(
                seal_ctx_scope(repository, &reader, projection)
                    .await
                    .context("seal tickr-ctx scope before Compaction archive")?,
            )
        }
        None => None,
    };
    persist_compaction_projection_inner(
        repository,
        projection,
        shipped_at,
        nats,
        scope_snapshot.as_ref(),
    )
    .await
}

pub(crate) async fn persist_compaction_projection_with_scope(
    repository: &WriterRepositoryBundle,
    projection: &ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
    nats: &NatsClient,
    scope_snapshot: &CompactionScopeSnapshot,
) -> Result<()> {
    persist_compaction_projection_inner(
        repository,
        projection,
        shipped_at,
        Some(nats),
        Some(scope_snapshot),
    )
    .await
}
/// Persist an archive enriched by an already sealed selected-role snapshot.
/// Redis archive evidence and cleanup remain owned by the role adapter.
pub async fn persist_compaction_projection_with_selected_scope(
    repository: &WriterRepositoryBundle,
    projection: &ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
    scope_snapshot: &CompactionScopeSnapshot,
) -> Result<()> {
    persist_compaction_projection_inner(
        repository,
        projection,
        shipped_at,
        None,
        Some(scope_snapshot),
    )
    .await
}

async fn persist_compaction_projection_inner(
    repository: &WriterRepositoryBundle,
    projection: &ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
    nats: Option<&NatsClient>,
    scope_snapshot: Option<&CompactionScopeSnapshot>,
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
    let ctx_envelope_json = scope_snapshot
        .map(scope_archive_entries)
        .transpose()?
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    let log_uris_json = derive_log_uris(projection, workflow_id, wi_id);
    let runtime_params_json = derive_runtime_params(projection, shipped_at);

    repository
        .archive_terminal_workflow(ArchiveTerminalWorkflowInput {
            projection,
            ctx_envelope: ctx_envelope_json,
            runtime_params: runtime_params_json,
            log_uris: log_uris_json,
            archived_at: shipped_at.unwrap_or_else(Utc::now),
        })
        .await
        .context("persist the linked terminal archive")?;
    if let (Some(client), Some(snapshot)) = (nats, scope_snapshot) {
        open_nats_scope_store(client)
            .await?
            .mark_archive_committed(&snapshot.owner, &snapshot.digest)
            .await
            .context("record committed tickr-ctx scope archive")?;
    }

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
            Ok(_) => {}
            Err(error) => {
                eprintln!(
                    "signal_captures cleanup: failed for run {}: {} (sweep will retry)",
                    wi_id, error
                );
            }
        }
    }
    Ok(())
}

/// Seal one immutable snapshot through the selected ScopeStore role.
pub async fn seal_ctx_scope(
    repository: &WriterRepositoryBundle,
    reader: &dyn CompactionScopeSnapshotReader,
    projection: &ap::ArchiveProjection,
) -> Result<CompactionScopeSnapshot> {
    reader.snapshot(repository, projection).await
}

fn snapshot_from_archive(
    owner: &str,
    expected_digest: &str,
    envelope: &serde_json::Value,
) -> Result<CompactionScopeSnapshot> {
    let entries = envelope
        .as_array()
        .ok_or_else(|| anyhow!("committed tickr-ctx scope archive is not an entry array"))?
        .iter()
        .map(|entry| {
            let key = entry
                .get("key")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| anyhow!("committed tickr-ctx scope archive entry has no key"))?
                .to_owned();
            let encoded = entry
                .get("envelope_bytes")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    anyhow!("committed tickr-ctx scope archive entry has no opaque bytes")
                })?;
            Ok(NatsScopeEntry {
                key,
                envelope: hex::decode(encoded)
                    .context("decode committed tickr-ctx scope envelope bytes")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let snapshot = nats_snapshot(snapshot_from_entries(owner, entries)?);
    if snapshot.digest != expected_digest {
        return Err(anyhow!(
            "committed tickr-ctx scope archive digest does not match its cleaned snapshot"
        ));
    }
    Ok(snapshot)
}

pub async fn cleanup_ctx_scope(nats: &NatsClient, run_id: &str) -> Result<()> {
    open_nats_scope_store(nats)
        .await?
        .cleanup_archived(&tickr_ctx::scope::sanitize_segment(run_id))
        .await
        .context("clean archived fresh all-NATS ScopeStore")
}

async fn open_nats_scope_store(nats: &NatsClient) -> Result<NatsScopeStore> {
    let namespace = ctx_namespace();
    let bucket = tickr_ctx::scope::bucket_for_namespace(&namespace);
    let kv = jetstream::new(nats.clone())
        .get_key_value(&bucket)
        .await
        .with_context(|| format!("open admitted ScopeStore bucket {bucket}"))?;
    NatsScopeStore::new(kv, &namespace).context("open fresh all-NATS ScopeStore")
}

pub(crate) async fn open_nats_scope_reader(
    nats: &NatsClient,
) -> Result<NatsCompactionScopeSnapshotReader> {
    Ok(NatsCompactionScopeSnapshotReader {
        store: open_nats_scope_store(nats).await?,
    })
}

fn scope_archive_entries(snapshot: &CompactionScopeSnapshot) -> Result<serde_json::Value> {
    snapshot
        .entries
        .iter()
        .map(|entry| {
            Ok(serde_json::json!({
                "key": entry.key,
                "envelope": serde_json::from_slice::<serde_json::Value>(&entry.envelope)
                    .context("decode validated tickr-ctx envelope for archive response")?,
                "envelope_bytes": hex::encode(&entry.envelope),
            }))
        })
        .collect::<Result<Vec<_>>>()
        .map(serde_json::Value::Array)
}

fn workflow_instance_id(projection: &ap::ArchiveProjection) -> Result<Uuid> {
    Uuid::parse_str(&projection.id).with_context(|| {
        format!(
            "archive projection carried an unparseable id `{}`",
            projection.id
        )
    })
}

fn nats_snapshot(snapshot: NatsScopeSnapshot) -> CompactionScopeSnapshot {
    CompactionScopeSnapshot {
        scope_id: None,
        owner: snapshot.owner,
        entries: snapshot
            .entries
            .into_iter()
            .map(|entry| CompactionScopeEntry {
                key: entry.key,
                envelope: entry.envelope,
            })
            .collect(),
        bytes: snapshot.bytes,
        digest: snapshot.digest,
        value_bytes: snapshot.value_bytes,
    }
}

fn common_snapshot(
    snapshot: TickrCtxScopeSnapshot,
    owner: &str,
) -> Result<CompactionScopeSnapshot> {
    let entries = decode_tickr_ctx_scope_snapshot(&snapshot)?
        .into_iter()
        .map(|(key, envelope)| CompactionScopeEntry { key, envelope })
        .collect();
    Ok(CompactionScopeSnapshot {
        scope_id: Some(snapshot.scope_id),
        owner: tickr_ctx::scope::sanitize_segment(owner),
        entries,
        bytes: snapshot.bytes,
        digest: snapshot.digest,
        value_bytes: snapshot.value_bytes,
    })
}

fn ctx_namespace() -> String {
    std::env::var("TICKR_NS").unwrap_or_else(|_| "default".to_owned())
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
