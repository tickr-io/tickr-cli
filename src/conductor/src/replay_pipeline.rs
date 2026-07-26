//! Conductor-side replay ingress + durable pipeline row.
//!
//! This module is the entry point everything else in the replay feature hangs
//! off. A replay request names a terminal source run; the conductor accepts it,
//! mints a self-contained replay seed **from the archive** (never from client
//! bytes — the run's trust boundary is the archive, not the caller), computes
//! the deterministic replay instance id `UUIDv5(source_instance_id, signal_id)`
//! **at ingress**, drives the server materialisation of a born-Stalled instance
//! under that id, re-hydrates its tickr-ctx scope, releases the born-Stall, and
//! records the whole lifecycle in a durable `workflow_replays` row.
//!
//! The row follows the patch-pipeline precedent ([`crate::patch_pipeline`]) but
//! is **new machinery**, not a reuse of it: persist-at-ingress, a re-drive loop
//! for unsettled rows, and a boot-time reconcile that re-drives a row an
//! interrupted process left behind.
//!
//! ## Lifecycle
//!
//! - `Materializing` → `Released` — the drive completed: the replay Trigger was
//!   relayed (server materialises the born-Stalled instance), the ctx scope was
//!   re-hydrated, and the born-Stall released via the idempotent
//!   `resume_instance`.
//! - `VersionUnresolvable` — terminal park: the source run's archived blob is
//!   absent (nothing to replay). Because the replay graph is **read from the
//!   archive** rather than rebuilt from the definition, a missing blob is the
//!   *only* unresolvable case — a live registry reset leaves the archive
//!   (and so the replay) intact.
//!
//! ## Idempotency
//!
//! `UNIQUE (source_instance_id, idempotency_key)`: a second POST carrying the
//! same key dedup-returns the existing `replay_instance_id`. An omitted key
//! mints a fresh replay every POST.
//!
//! ## Trust boundary
//!
//! No client bytes ever deserialise into a replay seed. The request body is
//! `{ source_instance_id, resume_from?, name?, idempotency_key? }` — a
//! constrained shape with no field able to carry seeded state; the seed is
//! constructed here, from archive rows. The no-smuggle tripwire tests on both
//! ingresses (HTTP trigger body, NATS trigger envelope) enforce that structural
//! absence.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_nats::Client as NatsClient;
use tickr_migrations::backend::{RepositoryError, WriterRepositoryBundle};
use tickr_migrations::replay_repository::{
    ReplayDriveLoadOutcome, ReplayLifecycleInput, ReplayLifecycleInsertOutcome,
    ReplayLifecycleStatus, ReplayRedriveCandidate, ReplaySettlementOutcome, ReplaySource,
};
pub use tickr_migrations::replay_repository::{
    ReplayLifecycleRow as ReplayRow, STATUS_MATERIALIZING, STATUS_RELEASED,
    STATUS_VERSION_UNRESOLVABLE,
};
use tickr_proto::signal as sp;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use tickr_ctx::envelope::{Envelope, Producer, SignalSource};

use crate::canonical_json;
use crate::replay_rehydration::{
    apply_rehydration_via_nats, parent_hydration_gate, plan_rehydration, release_signal,
    ArchivedCtxEntry, ArchivedRun, ArchivedTaskInstanceRow, CarriedKey, RehydrationPlan,
    RehydrationReject,
};
use crate::replay_seed::{default_resume_from, mint_replay_seed, ReplayReject};

pub mod local;

/// How often the re-drive loop scans for unsettled rows, and how long a row
/// must sit untouched before it is re-driven. `updated_at` is the backoff
/// anchor: every re-drive attempt bumps it, so a row is re-driven at most once
/// per `REDRIVE_MIN_AGE` until it settles.
pub const REDRIVE_INTERVAL: Duration = Duration::from_secs(5);
pub const REDRIVE_MIN_AGE: Duration = Duration::from_secs(10);

/// Cycle backstop for the replay-chain ancestor walk. Replay chains are shallow
/// in practice; this only bounds a corrupt self-referential provenance.
const MAX_CHAIN_DEPTH: usize = 4096;

/// The producer's intent, assembled by the transport-specific caller (the
/// command-bus adapter). `resume_from = None` means "resume from every
/// `Grounded(Failed)` HyperNode" — resolved conductor-side from the archive.
pub struct ReplayRequest {
    pub source_instance_id: Uuid,
    /// The operator's chosen resume-from frontier; `None`/empty → the default
    /// all-failed-nodes set.
    pub resume_from: Option<Vec<Uuid>>,
    /// Optional Run name for the materialised replay instance.
    pub name: Option<String>,
    /// Producer-supplied dedup key. `None` → every POST mints a fresh replay.
    pub idempotency_key: Option<String>,
    /// The inputs shadow: capture-name → fresh value. Shadows a declared
    /// trigger capture of the pinned version only — the shadow writes are the
    /// replay signal's genuine `Producer::Signal` inputs, written into the
    /// replay's fresh ctx scope. Undeclared or task-produced keys typed-reject.
    /// Empty → no shadow (the ordinary carry-forward-and-resume path).
    pub inputs: HashMap<String, serde_json::Value>,
}

/// Ingress verdict for one replay request.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplayIngress {
    /// The replay materialised (or is being driven): the row is open, the
    /// Trigger relayed, re-hydration + release under way or queued for
    /// re-drive. `doomed` enumerates interior joins that can never fire (a
    /// fan-in whose sibling arm died) — surfaced to the operator, never a
    /// reject.
    Accepted {
        replay_instance_id: Uuid,
        doomed: Vec<Uuid>,
    },
    /// A collision on `(source_instance_id, idempotency_key)`: the existing
    /// replay's `replay_instance_id` is returned (dedup-return, 200) instead of
    /// minting a duplicate.
    Deduplicated { replay_instance_id: Uuid },
    /// The source run's archived blob is absent — nothing to replay. Parked
    /// terminally on its own row; the deterministic id is still returned so a
    /// keyed retry dedup-returns the same park.
    VersionUnresolvable { replay_instance_id: Uuid },
}

/// A replay request that cannot open a row — a validation reject surfaced
/// synchronously to the caller. (A blob-absent source is NOT here: it parks a
/// row and returns [`ReplayIngress::VersionUnresolvable`].)
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// A resume-from **root** has no incident HyperEdge all of whose sources are
    /// `Grounded(Success)`: the resume frontier itself is unfireable.
    #[error("resume-from root {root} is unfireable: no incident edge has all-Success sources")]
    RootUnfireable { root: Uuid },
    /// The source run has **zero** `Grounded(Failed)` HyperNodes (a cancelled /
    /// timed-out run), so the default resume-from is empty — pass `resume_from`
    /// explicitly.
    #[error("source run has no Grounded(Failed) HyperNodes; pass `resume_from` explicitly")]
    NoFailedNodes,
    /// The run being replayed was itself a replay whose re-hydration never
    /// completed (a cancelled-never-released replay). Naming the nearest
    /// hydration-complete ancestor to replay instead.
    #[error(
        "the replayed run never completed re-hydration; replay {nearest_hydrated_ancestor} instead"
    )]
    ParentNeverHydrated { nearest_hydrated_ancestor: Uuid },
    /// An `inputs` shadow key names a value the pinned version never declared as
    /// a trigger capture. Only declared trigger captures are shadowable.
    #[error("inputs shadow key `{key}` is not a declared trigger capture of the pinned version")]
    ShadowUndeclared { key: String },
    /// An `inputs` shadow key names a task-produced value. History-editing of
    /// task outputs is out of scope — only trigger captures are shadowable.
    #[error(
        "inputs shadow key `{key}` names a task-produced value, which is never shadowable \
         (only declared trigger captures can be shadowed)"
    )]
    ShadowTaskProduced { key: String },
    /// The pinned version's declared-capture schema is not resolvable from the
    /// selected definition repository, so an `inputs` shadow cannot be
    /// validated against the version that actually ran.
    #[error(
        "the pinned version {version}'s declared-capture schema is not in the definition mirror; \
         cannot validate the inputs shadow"
    )]
    ShadowSchemaUnresolvable { version: i64 },
    /// A selected repository operation failed.
    #[error("replay pipeline persistence: {0}")]
    Persist(#[source] RepositoryError),
    /// Reading or decoding an archive blob failed.
    #[error("replay archive read: {0}")]
    Archive(#[source] anyhow::Error),
}

/// The deterministic replay instance id, `UUIDv5(source_instance_id,
/// signal_id)`. The UUID pair is the stable cross-plane replay identity.
pub fn replay_instance_id(source_instance_id: Uuid, signal_id: Uuid) -> Uuid {
    const REPLAY_NAMESPACE: Uuid = Uuid::from_bytes(*b"tickr_replay_id\0");
    let mut name = [0_u8; 32];
    name[..16].copy_from_slice(source_instance_id.as_bytes());
    name[16..].copy_from_slice(signal_id.as_bytes());
    Uuid::new_v5(&REPLAY_NAMESPACE, &name)
}

/// Outbound seam for driving a replay: relaying wire `Signal`s toward the
/// server (the replay Trigger, and the release Resume) and executing a
/// re-hydration plan against NATS. Trait-carried — like the patch pipeline's
/// `PatchRelaySender` — so the row / idempotency / re-drive logic is testable
/// without a live relay or NATS.
#[async_trait::async_trait]
pub trait ReplayRelaySender: Send + Sync {
    /// Relay one wire `Signal` toward the server.
    async fn send(&self, signal: &sp::Signal) -> Result<()>;
    /// Execute a re-hydration plan against the replay's fresh ctx scope (writes
    /// the carried keys and the hydration sentinel).
    async fn rehydrate(&self, replay_run_id: Uuid, plan: &RehydrationPlan) -> Result<()>;
}

/// Default sender wired against the global conductor relay channel and a NATS
/// client for the ctx-scope writes.
pub struct DefaultReplayRelaySender {
    pub nats: NatsClient,
}

#[async_trait::async_trait]
impl ReplayRelaySender for DefaultReplayRelaySender {
    async fn send(&self, signal: &sp::Signal) -> Result<()> {
        crate::relay::send_signal(signal).await
    }
    async fn rehydrate(&self, replay_run_id: Uuid, plan: &RehydrationPlan) -> Result<()> {
        apply_rehydration_via_nats(&self.nats, replay_run_id, plan).await
    }
}

/// Everything the drive needs, rebuilt deterministically from the archive so
/// both the ingress and a re-drive produce identical work.
struct DriveInputs {
    trigger: sp::Signal,
    plan: RehydrationPlan,
    replay_instance_id: Uuid,
    /// Whether the instance is born-Stalled (pre-grounded set non-empty). Only
    /// a born-Stalled instance is re-hydrated and released.
    born_stalled: bool,
}

/// Ingress one replay request.
///
/// Order: idempotency dedup → archive read (blob-absent parks) → seed mint
/// (fireability validation) → chained-replay hydration gate → inputs-shadow
/// validation (version-pinned definition read) → persist the row → drive (relay
/// Trigger, re-hydrate + shadow-write, release) → settle `Released`. A drive
/// failure leaves a durable `Materializing` row the re-drive loop finishes.
pub async fn process_replay(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
    req: ReplayRequest,
) -> Result<ReplayIngress, ReplayError> {
    let signal_id = Uuid::new_v4();

    if let Some(key) = req.idempotency_key.as_deref() {
        if let Some(row) = repositories
            .replay_by_idempotency(req.source_instance_id, key)
            .await
            .map_err(ReplayError::Persist)?
        {
            return Ok(ingress_for_existing(row));
        }
    }

    // The selected repository composes the terminal archive and its pinned
    // definition. Missing archive state preserves the typed terminal park.
    let archived = match repositories
        .replay_source(req.source_instance_id)
        .await
        .map_err(ReplayError::Persist)?
    {
        Some(source) => source,
        None => {
            let replay_id = replay_instance_id(req.source_instance_id, signal_id);
            let input = ReplayLifecycleInput {
                replay_instance_id: replay_id,
                source_instance_id: req.source_instance_id,
                signal_id,
                idempotency_key: req.idempotency_key.clone(),
                status: STATUS_VERSION_UNRESOLVABLE.to_string(),
                resume_from: Vec::new(),
                pre_grounded: Vec::new(),
                name: req.name.clone(),
                seed_sha256: None,
                outcome: Some(
                    "source run's archived blob is absent — nothing to replay".to_string(),
                ),
                shadowed_keys: Vec::new(),
            };
            return match repositories
                .insert_replay_lifecycle(&input)
                .await
                .map_err(ReplayError::Persist)?
            {
                ReplayLifecycleInsertOutcome::Inserted => Ok(ReplayIngress::VersionUnresolvable {
                    replay_instance_id: replay_id,
                }),
                ReplayLifecycleInsertOutcome::Existing(row) => Ok(ingress_for_existing(row)),
            };
        }
    };
    let source_run = source_run_from_repository(req.source_instance_id, &archived);

    // Fireability is validated before the lifecycle insert. Invalid input
    // therefore cannot leave a partial replay row.
    let seed_graph = archived.projection.graph.as_ref().ok_or_else(|| {
        ReplayError::Archive(anyhow::anyhow!("archived projection carries no graph"))
    })?;
    let resolved_resume_from = match &req.resume_from {
        Some(resume_from) if !resume_from.is_empty() => resume_from.clone(),
        _ => default_resume_from(seed_graph),
    };
    let (seed, report) = mint_replay_seed(
        req.source_instance_id,
        seed_graph,
        archived.projection.tasks.clone(),
        archived.projection.workflow_version,
        Some(resolved_resume_from.clone()),
        signal_id,
    )
    .map_err(map_seed_reject)?;
    let replay_id = replay_instance_id(req.source_instance_id, signal_id);
    let seed_pre_grounded = seed_pre_grounded_ids(&seed);

    let ancestors = gather_ancestors(repositories, &source_run)
        .await
        .map_err(ReplayError::Persist)?;
    parent_hydration_gate(&source_run, &ancestors).map_err(map_rehydration_reject)?;

    let (shadow_writes, shadowed_names) = build_shadow_writes(
        archived.pinned_definition.as_ref(),
        archived.projection.workflow_version,
        &req.inputs,
        signal_id,
    )?;
    let seed_sha256 = seed_sha256(&seed);

    // The operation commits before returning `Inserted`; only then may relay,
    // ctx-scope hydration, or release effects begin.
    let input = ReplayLifecycleInput {
        replay_instance_id: replay_id,
        source_instance_id: req.source_instance_id,
        signal_id,
        idempotency_key: req.idempotency_key.clone(),
        status: STATUS_MATERIALIZING.to_string(),
        resume_from: resolved_resume_from.clone(),
        pre_grounded: seed_pre_grounded,
        name: req.name.clone(),
        seed_sha256: Some(seed_sha256),
        outcome: None,
        shadowed_keys: shadowed_names,
    };
    if let ReplayLifecycleInsertOutcome::Existing(row) = repositories
        .insert_replay_lifecycle(&input)
        .await
        .map_err(ReplayError::Persist)?
    {
        return Ok(ingress_for_existing(row));
    }

    if let Err(error) = drive_replay(repositories, sender, replay_id, shadow_writes).await {
        eprintln!("replay drive failed for {replay_id} (will re-drive): {error}");
    }

    Ok(ReplayIngress::Accepted {
        replay_instance_id: replay_id,
        doomed: report.doomed,
    })
}

/// Execute the replay drive: relay the Trigger (server materialises the
/// born-Stalled instance under the deterministic id), re-hydrate the ctx scope,
/// release the born-Stall, and settle the row `Released`. Every step is
/// idempotent under redelivery, so a re-drive of a partially-driven row
/// converges.
async fn perform_drive_effects(sender: &dyn ReplayRelaySender, inputs: &DriveInputs) -> Result<()> {
    // Relay the Trigger first: the release Resume must arrive after the
    // instance exists, and the relay is an ordered stream.
    sender.send(&inputs.trigger).await?;

    // A born-Stalled instance (non-empty pre-grounded set) must have its ctx
    // scope re-hydrated and the born-Stall released; the same re-hydration step
    // also writes the inputs-shadow keys. So run it whenever the instance is
    // born-Stalled OR there are shadow writes to land. For a born-Stalled
    // instance the writes land before any execution (the Stall holds until
    // release); a replay with a shadow but nothing carried forward is not
    // born-Stalled, so its shadow writes land right after materialisation.
    let has_shadow = !inputs.plan.shadowed.is_empty();
    if inputs.born_stalled || has_shadow {
        sender
            .rehydrate(inputs.replay_instance_id, &inputs.plan)
            .await?;
    }
    // Only a born-Stall needs releasing; a never-Stalled instance already runs.
    if inputs.born_stalled {
        sender
            .send(&release_signal(inputs.replay_instance_id))
            .await?;
    }
    Ok(())
}

async fn drive(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
    inputs: &DriveInputs,
) -> Result<()> {
    perform_drive_effects(sender, inputs).await?;

    match repositories
        .settle_replay_released(inputs.replay_instance_id)
        .await?
    {
        ReplaySettlementOutcome::Released
        | ReplaySettlementOutcome::AlreadySettled(ReplayLifecycleStatus::Released) => Ok(()),
        ReplaySettlementOutcome::AlreadySettled(status) => {
            anyhow::bail!(
                "replay {} settled as {} during drive",
                inputs.replay_instance_id,
                status.as_str()
            )
        }
        ReplaySettlementOutcome::Absent => {
            anyhow::bail!(
                "replay {} disappeared before settlement",
                inputs.replay_instance_id
            )
        }
    }
}

/// Load the committed replay decisions and selected source, reconstruct the
/// drive deterministically, and execute it. Only the initial in-process attempt
/// may carry shadow values; durable identity, frontier, and seed decisions
/// always come back through the repository.
async fn drive_replay(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
    replay_instance_id: Uuid,
    shadow_writes: Vec<CarriedKey>,
) -> Result<()> {
    let replay = match repositories.load_replay_drive(replay_instance_id).await? {
        ReplayDriveLoadOutcome::Ready(replay) => replay,
        ReplayDriveLoadOutcome::SourceUnavailable(row) => {
            anyhow::bail!(
                "archive blob for source {} is unavailable while replay {} remains Materializing",
                row.source_instance_id,
                row.replay_instance_id
            )
        }
        ReplayDriveLoadOutcome::AlreadySettled(ReplayLifecycleStatus::Released) => return Ok(()),
        ReplayDriveLoadOutcome::AlreadySettled(status) => {
            anyhow::bail!(
                "replay {replay_instance_id} is already settled as {}",
                status.as_str()
            )
        }
        ReplayDriveLoadOutcome::Absent => {
            anyhow::bail!("replay {replay_instance_id} has no durable lifecycle row")
        }
    };
    let inputs = build_drive_inputs(
        repositories,
        &replay.lifecycle,
        &replay.source,
        shadow_writes,
    )
    .await?;
    drive(repositories, sender, &inputs).await
}

/// Re-drive one unsettled identity through the same committed-row load used by
/// the initial attempt.
async fn drive_row(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
    row: &ReplayRow,
) -> Result<()> {
    drive_replay(repositories, sender, row.replay_instance_id, Vec::new()).await
}

/// Rebuild drive inputs from committed lifecycle decisions and selected source.
/// Any identity, frontier-derived pre-grounding, or seed-witness mismatch fails
/// before relay, hydration, release, or settlement.
async fn build_drive_inputs(
    repositories: &WriterRepositoryBundle,
    row: &ReplayRow,
    archived: &ReplaySource,
    shadow_writes: Vec<CarriedKey>,
) -> Result<DriveInputs> {
    let expected_replay_id = replay_instance_id(row.source_instance_id, row.signal_id);
    if row.replay_instance_id != expected_replay_id {
        anyhow::bail!(
            "replay identity mismatch: row {} but source/signal derive {}",
            row.replay_instance_id,
            expected_replay_id
        );
    }
    let source_run = source_run_from_repository(row.source_instance_id, archived);
    let seed_graph = archived
        .projection
        .graph
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("archived projection carries no graph"))?;
    let (seed, _report) = mint_replay_seed(
        row.source_instance_id,
        seed_graph,
        archived.projection.tasks.clone(),
        archived.projection.workflow_version,
        Some(row.resume_from.clone()),
        row.signal_id,
    )
    .map_err(|reject| {
        anyhow::anyhow!(
            "replay re-drive: seed re-mint rejected for {}: {reject:?}",
            row.replay_instance_id
        )
    })?;
    let reminted_pre_grounded = seed_pre_grounded_ids(&seed);
    if row.pre_grounded != reminted_pre_grounded {
        anyhow::bail!(
            "replay {} pre-grounded decision disagrees with its persisted frontier",
            row.replay_instance_id
        );
    }
    let witness = seed_sha256(&seed);
    if row.seed_sha256.as_deref() != Some(witness.as_str()) {
        anyhow::bail!(
            "replay {} seed integrity witness mismatch",
            row.replay_instance_id
        );
    }
    if !shadow_writes.is_empty() {
        let names = shadow_writes
            .iter()
            .map(|write| write.name.clone())
            .collect::<Vec<_>>();
        if names != row.shadowed_keys {
            anyhow::bail!(
                "replay {} shadow audit disagrees with the initial drive",
                row.replay_instance_id
            );
        }
    }
    let ancestors = gather_ancestors(repositories, &source_run).await?;
    let pre_grounded = reminted_pre_grounded.into_iter().collect();
    let mut plan = plan_rehydration(&source_run, &ancestors, &pre_grounded, row.signal_id);
    plan.shadowed = shadow_writes;
    Ok(DriveInputs {
        trigger: build_trigger_signal(
            &seed,
            archived.workflow_id,
            row.source_instance_id,
            row.signal_id,
            &row.resume_from,
            &row.name,
        ),
        plan,
        replay_instance_id: row.replay_instance_id,
        born_stalled: !row.pre_grounded.is_empty(),
    })
}

/// One re-drive pass: re-drive every `Materializing` row untouched for at least
/// `min_age`. Returns how many rows were re-driven.
pub async fn redrive_unsettled(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
    min_age: Duration,
) -> Result<usize, RepositoryError> {
    let age = chrono::Duration::from_std(min_age).unwrap_or(chrono::Duration::MAX);
    let rows = repositories
        .unsettled_replays_before(chrono::Utc::now() - age)
        .await?;
    let mut driven = 0usize;
    for candidate in rows {
        let row = match candidate {
            ReplayRedriveCandidate::Ready(row) => row,
            ReplayRedriveCandidate::Corrupt { identity, error } => {
                eprintln!(
                    "replay re-drive: stored lifecycle for {identity} is corrupt and was skipped: {error}"
                );
                continue;
            }
        };
        match drive_row(repositories, sender, &row).await {
            Ok(()) => driven += 1,
            Err(error) => eprintln!(
                "replay re-drive: drive failed for {} (will retry): {error}",
                row.replay_instance_id
            ),
        }
    }
    Ok(driven)
}

/// The steady-state re-drive loop: every `REDRIVE_INTERVAL`, re-drive unsettled
/// rows older than `REDRIVE_MIN_AGE` until shutdown.
pub async fn run_replay_redrive(
    repositories: Arc<WriterRepositoryBundle>,
    sender: Arc<dyn ReplayRelaySender>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                println!("replay re-drive: shutdown signal received");
                return;
            }
            _ = tokio::time::sleep(REDRIVE_INTERVAL) => {
                match redrive_unsettled(&repositories, sender.as_ref(), REDRIVE_MIN_AGE).await {
                    Ok(0) => {}
                    Ok(n) => println!("replay re-drive: re-drove {n} unsettled replay(s)"),
                    Err(e) => eprintln!("replay re-drive pass failed: {e}"),
                }
            }
        }
    }
}

/// Boot-time reconcile: re-drive every unsettled replay row once at startup,
/// regardless of age. A process that died mid-drive (Trigger relayed but the
/// ctx re-hydration / release never landed) leaves a durable `Materializing`
/// row; this finishes it before steady-state traffic resumes.
pub async fn reconcile_orphan_replay_rows(
    repositories: &WriterRepositoryBundle,
    sender: &dyn ReplayRelaySender,
) -> Result<usize, RepositoryError> {
    redrive_unsettled(repositories, sender, Duration::ZERO).await
}

/// Build the wire `Signal::Trigger` carrying the replay seed. The seed rides
/// only this localhost relay; `TriggerSource::Replay` records the origin, and
/// the server materialises the instance under `seed.replay_instance_id`.
fn build_trigger_signal(
    seed: &sp::ReplaySeed,
    workflow_id: Uuid,
    source_instance_id: Uuid,
    signal_id: Uuid,
    resume_from: &[Uuid],
    name: &Option<String>,
) -> sp::Signal {
    sp::Signal {
        signal_id: signal_id.to_string(),
        idempotency_key: None,
        variant: Some(sp::signal::Variant::Trigger(sp::Trigger {
            workflow_id: workflow_id.to_string(),
            // Replays are immediate — no scheduled_at; the born-Stall governs
            // the pre-release window.
            scheduled_at: None,
            source: Some(sp::TriggerSource {
                source: Some(sp::trigger_source::Source::Replay(
                    sp::trigger_source::Replay {
                        source_instance_id: source_instance_id.to_string(),
                        resume_from: resume_from.iter().map(Uuid::to_string).collect(),
                    },
                )),
            }),
            name: name.clone(),
            replay: Some(seed.clone()),
        })),
    }
}

/// The post-hoc integrity witness of a minted seed: a canonical sha256 over the
/// seed's JSON. Canonical so map ordering can't perturb the digest.
fn seed_sha256(seed: &sp::ReplaySeed) -> String {
    let value = serde_json::to_value(seed).unwrap_or(serde_json::Value::Null);
    hex::encode(canonical_json::hash(Some(&value)))
}

/// The seed's pre-grounded set as UUIDs. The seed carries them as wire strings;
/// they were minted conductor-side from UUIDs, so every entry parses.
fn seed_pre_grounded_ids(seed: &sp::ReplaySeed) -> Vec<Uuid> {
    seed.pre_grounded
        .iter()
        .filter_map(|s| Uuid::parse_str(s).ok())
        .collect()
}

/// Map a seed-mint reject onto the ingress error surface. `VersionUnresolvable`
/// cannot arise here (the blob is present by the time we mint), so it collapses
/// to an archive error rather than a phantom park.
fn map_seed_reject(reject: ReplayReject) -> ReplayError {
    match reject {
        ReplayReject::RootUnfireable { root } => ReplayError::RootUnfireable { root },
        ReplayReject::NoFailedNodes => ReplayError::NoFailedNodes,
        ReplayReject::VersionUnresolvable => ReplayError::Archive(anyhow::anyhow!(
            "archived graph unexpectedly absent at mint"
        )),
    }
}

fn map_rehydration_reject(reject: RehydrationReject) -> ReplayError {
    match reject {
        RehydrationReject::ParentNeverHydrated {
            nearest_hydrated_ancestor,
        } => ReplayError::ParentNeverHydrated {
            nearest_hydrated_ancestor,
        },
    }
}

fn ingress_for_existing(row: ReplayRow) -> ReplayIngress {
    if row.status == STATUS_VERSION_UNRESOLVABLE {
        ReplayIngress::VersionUnresolvable {
            replay_instance_id: row.replay_instance_id,
        }
    } else {
        ReplayIngress::Deduplicated {
            replay_instance_id: row.replay_instance_id,
        }
    }
}

fn source_run_from_repository(source_instance_id: Uuid, source: &ReplaySource) -> ArchivedRun {
    let ctx_dump = source
        .ctx_envelope
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let key = entry.get("key")?.as_str()?.to_string();
            let envelope = entry.get("envelope")?.clone();
            let envelope_bytes = entry
                .get("envelope_bytes")
                .and_then(serde_json::Value::as_str)
                .and_then(|encoded| hex::decode(encoded).ok())
                .unwrap_or_else(|| serde_json::to_vec(&envelope).unwrap_or_default());
            Some(ArchivedCtxEntry {
                key,
                envelope,
                envelope_bytes,
            })
        })
        .collect();
    ArchivedRun {
        instance_id: source_instance_id,
        replay_source: source.replay_source,
        task_instances: source
            .task_instances
            .iter()
            .map(|task| ArchivedTaskInstanceRow {
                id: task.id,
                node_id: task.node_id,
            })
            .collect(),
        ctx_dump,
    }
}

/// Gather chained replay ancestors through the same selected terminal-archive
/// operation. Missing ancestor state stops attribution without opening a row.
async fn gather_ancestors(
    repositories: &WriterRepositoryBundle,
    source: &ArchivedRun,
) -> Result<HashMap<Uuid, ArchivedRun>, RepositoryError> {
    let mut ancestors = HashMap::new();
    let mut next = source.replay_source;
    let mut guard = 0usize;
    while let Some(parent_id) = next {
        if ancestors.contains_key(&parent_id) {
            break;
        }
        let Some(parent) = repositories.replay_source(parent_id).await? else {
            break;
        };
        let run = source_run_from_repository(parent_id, &parent);
        next = run.replay_source;
        ancestors.insert(parent_id, run);
        guard += 1;
        if guard > MAX_CHAIN_DEPTH {
            break;
        }
    }
    Ok(ancestors)
}

/// Validate and build names-only audited shadow writes from the source's
/// version-pinned definition selected by the repository.
fn build_shadow_writes(
    definition: Option<&tickr_proto::workflow::WorkflowDefinition>,
    workflow_version: i64,
    inputs: &HashMap<String, serde_json::Value>,
    signal_id: Uuid,
) -> Result<(Vec<CarriedKey>, Vec<String>), ReplayError> {
    if inputs.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let definition = definition.ok_or(ReplayError::ShadowSchemaUnresolvable {
        version: workflow_version,
    })?;
    let declared: HashSet<&str> = definition
        .captures
        .iter()
        .map(|capture| capture.name.as_str())
        .collect();
    let task_outputs: HashSet<String> = definition
        .tasks
        .iter()
        .flat_map(|task| task.outputs.iter().cloned())
        .collect();

    let mut writes = Vec::with_capacity(inputs.len());
    let mut names = Vec::with_capacity(inputs.len());
    for (key, value) in inputs {
        if declared.contains(key.as_str()) {
            let envelope = shadow_envelope(value, signal_id);
            let bytes = serde_json::to_vec(&envelope).map_err(|error| {
                ReplayError::Archive(anyhow::anyhow!(
                    "serialize shadow envelope `{key}`: {error}"
                ))
            })?;
            writes.push(CarriedKey {
                name: key.clone(),
                bytes,
            });
            names.push(key.clone());
        } else if task_outputs.contains(key) {
            return Err(ReplayError::ShadowTaskProduced { key: key.clone() });
        } else {
            return Err(ReplayError::ShadowUndeclared { key: key.clone() });
        }
    }
    writes.sort_by(|left, right| left.name.cmp(&right.name));
    names.sort();
    Ok((writes, names))
}

fn shadow_envelope(value: &serde_json::Value, signal_id: Uuid) -> Envelope {
    let producer = Producer::Signal {
        signal_id,
        source: SignalSource::Manual,
    };
    let kind = match value {
        serde_json::Value::String(_) => "string",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => "int",
        serde_json::Value::Number(_) => "float",
        serde_json::Value::Null | serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            "json"
        }
    };
    Envelope::new(kind, value.clone(), false, producer)
}

/// Read one lifecycle row through the selected writer repository.
pub async fn fetch_row(
    repositories: &WriterRepositoryBundle,
    replay_instance_id: Uuid,
) -> Result<Option<ReplayRow>, RepositoryError> {
    repositories.replay_lifecycle(replay_instance_id).await
}
