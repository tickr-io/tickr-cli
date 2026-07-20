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

use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use tickr_ctx::envelope::{Envelope, Producer, SignalSource};
use tickr_proto::codec::archive::archive_projection_from_json;
use tickr_proto::runnable as rp;
use tickr_proto::signal as sp;
use tickr_proto::workflow as wf;

use crate::canonical_json;
use crate::replay_rehydration::{
    apply_rehydration_via_nats, parent_hydration_gate, plan_rehydration, release_signal,
    ArchivedCtxEntry, ArchivedRun, ArchivedTaskInstanceRow, CarriedKey, RehydrationPlan,
    RehydrationReject,
};
use crate::replay_seed::{default_resume_from, mint_replay_seed, ReplayReject};

/// Lifecycle states of a replay row. TEXT in Postgres (matching the
/// `workflow_patches.status` precedent); the CHECK constraint in the migration
/// is the schema-side tripwire.
pub const STATUS_MATERIALIZING: &str = "Materializing";
pub const STATUS_RELEASED: &str = "Released";
pub const STATUS_VERSION_UNRESOLVABLE: &str = "VersionUnresolvable";

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
    /// conductor-Postgres definition mirror, so an `inputs` shadow cannot be
    /// validated against the version that actually ran.
    #[error(
        "the pinned version {version}'s declared-capture schema is not in the definition mirror; \
         cannot validate the inputs shadow"
    )]
    ShadowSchemaUnresolvable { version: i64 },
    /// A Postgres read/write failed.
    #[error("replay pipeline persistence: {0}")]
    Persist(#[source] sqlx::Error),
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

/// One replay lifecycle row, as read back for the dedup-return and the re-drive
/// / boot-reconcile scans.
#[derive(Debug, Clone)]
pub struct ReplayRow {
    pub replay_instance_id: Uuid,
    pub source_instance_id: Uuid,
    pub signal_id: Uuid,
    pub idempotency_key: Option<String>,
    pub status: String,
    pub resume_from: Vec<Uuid>,
    pub name: Option<String>,
    pub seed_sha256: Option<String>,
    /// Names-only audit of the shadowed declared trigger captures (never their
    /// values). Empty when the replay carried no `inputs` shadow.
    pub shadowed_keys: Vec<String>,
}

impl ReplayRow {
    pub fn is_settled(&self) -> bool {
        matches!(
            self.status.as_str(),
            STATUS_RELEASED | STATUS_VERSION_UNRESOLVABLE
        )
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
    pool: &PgPool,
    sender: &dyn ReplayRelaySender,
    req: ReplayRequest,
) -> Result<ReplayIngress, ReplayError> {
    // A fresh signal_id per POST. Two retries of the *same* replay signal mint
    // the same instance id — but a fresh POST (no key, or a new key) mints a
    // fresh signal and so a fresh replay.
    let signal_id = Uuid::new_v4();

    // 1. Idempotency dedup. A prior row under the same (source, key) replays
    //    its recorded outcome instead of minting a duplicate.
    if let Some(key) = req.idempotency_key.as_deref() {
        if let Some(row) = fetch_row_by_idempotency(pool, req.source_instance_id, key)
            .await
            .map_err(ReplayError::Persist)?
        {
            return Ok(match row.status.as_str() {
                STATUS_VERSION_UNRESOLVABLE => ReplayIngress::VersionUnresolvable {
                    replay_instance_id: row.replay_instance_id,
                },
                _ => ReplayIngress::Deduplicated {
                    replay_instance_id: row.replay_instance_id,
                },
            });
        }
    }

    // 2. Archive read. A blob-absent source is the sole unresolvable case —
    //    parked terminally on its own row. The seed's runnable graph is
    //    reconstructed from the stored blob's runnable projection (no instance
    //    aggregate decoded); producer attribution reads the projection-shaped
    //    `source_run` (task-instance rows, no aggregate).
    let archived = match read_archived_projection(pool, req.source_instance_id)
        .await
        .map_err(ReplayError::Archive)?
    {
        Some(src) => src,
        None => {
            let replay_id = replay_instance_id(req.source_instance_id, signal_id);
            park_version_unresolvable(
                pool,
                replay_id,
                req.source_instance_id,
                signal_id,
                req.idempotency_key.as_deref(),
            )
            .await
            .map_err(ReplayError::Persist)?;
            return Ok(ReplayIngress::VersionUnresolvable {
                replay_instance_id: replay_id,
            });
        }
    };
    let source_run = read_archived_run(pool, req.source_instance_id)
        .await
        .map_err(ReplayError::Archive)?
        .ok_or_else(|| {
            ReplayError::Archive(anyhow::anyhow!("archived run vanished between reads"))
        })?;

    // 3. Resolve the resume-from frontier (persisted so a re-drive re-mints the
    //    identical seed) and mint the seed. Fireability validation lives in the
    //    mint — an unfireable resume root, or a run with zero failed nodes,
    //    rejects here without opening a row.
    // The seed is minted directly off the source run's runnable projection — the
    // published archive contract — never a reconstructed server instance
    // aggregate.
    let seed_graph = archived.projection.graph.as_ref().ok_or_else(|| {
        ReplayError::Archive(anyhow::anyhow!("archived projection carries no graph"))
    })?;
    let seed_tasks = archived.projection.tasks.clone();

    let resolved_resume_from = match &req.resume_from {
        Some(rf) if !rf.is_empty() => rf.clone(),
        _ => default_resume_from(seed_graph),
    };
    let (seed, report) = mint_replay_seed(
        req.source_instance_id,
        seed_graph,
        seed_tasks,
        archived.projection.workflow_version,
        Some(resolved_resume_from.clone()),
        signal_id,
    )
    .map_err(map_seed_reject)?;

    // The deterministic replay instance id, recomputed from the same inputs the
    // seed minted from — the seed now carries it as a wire string, so the typed
    // id used for row keys / drive is derived here rather than re-parsed.
    let replay_id = replay_instance_id(req.source_instance_id, signal_id);
    let seed_pre_grounded = seed_pre_grounded_ids(&seed);

    // 4. Chained-replay hydration gate: replaying a run whose own re-hydration
    //    never completed would build on an incomplete coordination state.
    let ancestors = gather_ancestors(pool, &source_run)
        .await
        .map_err(ReplayError::Archive)?;
    parent_hydration_gate(&source_run, &ancestors).map_err(map_rehydration_reject)?;

    // 5. Build the inputs-shadow writes. `inputs` shadows declared trigger
    //    captures of the pinned version only — validated against the definition
    //    mirror read at the archived `workflow_version` (a version-pinned read,
    //    never the version-blind helper), so a capture renamed or dropped in a
    //    newer version doesn't reject a legit refresh on the archived run.
    let (shadow_writes, shadowed_names) = build_shadow_writes(
        pool,
        archived.workflow_id,
        archived.projection.workflow_version,
        &req.inputs,
        signal_id,
    )
    .await?;

    // 6. Plan the re-hydration, fold in the shadow writes, and stamp the witness.
    let pre_grounded: HashSet<Uuid> = seed_pre_grounded.iter().copied().collect();
    let mut plan = plan_rehydration(&source_run, &ancestors, &pre_grounded, signal_id);
    plan.shadowed = shadow_writes;
    let seed_sha256 = seed_sha256(&seed);

    // 7. Persist the row. A concurrent same-key insert loses the UNIQUE race —
    //    treated as a dedup-return of the winner. The row audits the shadowed
    //    capture NAMES only, never the values (a value may be a secret).
    let inserted = insert_materializing_row(
        pool,
        replay_id,
        req.source_instance_id,
        signal_id,
        req.idempotency_key.as_deref(),
        &resolved_resume_from,
        &seed_pre_grounded,
        req.name.as_deref(),
        &seed_sha256,
        &shadowed_names,
    )
    .await
    .map_err(ReplayError::Persist)?;
    if !inserted {
        if let Some(key) = req.idempotency_key.as_deref() {
            if let Some(row) = fetch_row_by_idempotency(pool, req.source_instance_id, key)
                .await
                .map_err(ReplayError::Persist)?
            {
                return Ok(ReplayIngress::Deduplicated {
                    replay_instance_id: row.replay_instance_id,
                });
            }
        }
    }

    // 8. Drive. A relay/NATS failure leaves the durable `Materializing` row for
    //    the re-drive loop — the request is never lost after acknowledgement.
    let inputs = DriveInputs {
        trigger: build_trigger_signal(
            &seed,
            archived.workflow_id,
            req.source_instance_id,
            signal_id,
            &resolved_resume_from,
            &req.name,
        ),
        plan,
        replay_instance_id: replay_id,
        born_stalled: !seed.pre_grounded.is_empty(),
    };
    if let Err(e) = drive(pool, sender, &inputs).await {
        eprintln!("replay drive failed for {replay_id} (will re-drive): {e}");
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
async fn drive(pool: &PgPool, sender: &dyn ReplayRelaySender, inputs: &DriveInputs) -> Result<()> {
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

    flip_to_released(pool, inputs.replay_instance_id).await?;
    Ok(())
}

/// Re-drive one unsettled row: rebuild the drive inputs from the archive
/// (deterministic) and re-run the drive. A re-drive bumps `updated_at` (the
/// backoff anchor) whether or not the drive succeeds, so a wedged drive is
/// re-attempted at most once per `REDRIVE_MIN_AGE`.
async fn drive_row(pool: &PgPool, sender: &dyn ReplayRelaySender, row: &ReplayRow) -> Result<()> {
    let Some(inputs) = build_drive_inputs(pool, row).await? else {
        // The archive blob vanished under us (a mid-flight drop). Loud, but
        // don't settle — the row stays for operator attention.
        eprintln!(
            "replay re-drive: archive blob for source {} absent; skipping {}",
            row.source_instance_id, row.replay_instance_id
        );
        touch_row(pool, row.replay_instance_id).await?;
        return Ok(());
    };
    touch_row(pool, row.replay_instance_id).await?;
    drive(pool, sender, &inputs).await
}

/// Rebuild the drive inputs for a persisted row by re-reading the archive and
/// re-minting the seed deterministically. `None` when the source blob is absent.
async fn build_drive_inputs(pool: &PgPool, row: &ReplayRow) -> Result<Option<DriveInputs>> {
    let Some(archived) = read_archived_projection(pool, row.source_instance_id).await? else {
        return Ok(None);
    };
    let Some(source_run) = read_archived_run(pool, row.source_instance_id).await? else {
        return Ok(None);
    };
    // Re-mint deterministically off the same runnable projection the ingress used.
    let seed_graph = archived
        .projection
        .graph
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("archived projection carries no graph"))?;
    let seed_tasks = archived.projection.tasks.clone();
    let (seed, _report) = match mint_replay_seed(
        row.source_instance_id,
        seed_graph,
        seed_tasks,
        archived.projection.workflow_version,
        Some(row.resume_from.clone()),
        row.signal_id,
    ) {
        Ok(v) => v,
        // A row that fireability-rejects on re-mint is an integrity fault (it
        // passed at ingress). Loud, skip.
        Err(reject) => {
            return Err(anyhow::anyhow!(
                "replay re-drive: seed re-mint rejected for {}: {reject:?}",
                row.replay_instance_id
            ));
        }
    };
    let ancestors = gather_ancestors(pool, &source_run).await?;
    let pre_grounded: HashSet<Uuid> = seed_pre_grounded_ids(&seed).into_iter().collect();
    // A re-drive rebuilds the plan from the archive only — the inputs-shadow
    // values are request-supplied and never persisted (names-only audit), so
    // `plan.shadowed` stays empty here. The first drive already wrote them
    // (idempotent KV puts); a re-drive relies on that write having landed.
    let plan = plan_rehydration(&source_run, &ancestors, &pre_grounded, row.signal_id);
    Ok(Some(DriveInputs {
        trigger: build_trigger_signal(
            &seed,
            archived.workflow_id,
            row.source_instance_id,
            row.signal_id,
            &row.resume_from,
            &row.name,
        ),
        plan,
        replay_instance_id: replay_instance_id(row.source_instance_id, row.signal_id),
        born_stalled: !seed.pre_grounded.is_empty(),
    }))
}

/// One re-drive pass: re-drive every `Materializing` row untouched for at least
/// `min_age`. Returns how many rows were re-driven.
pub async fn redrive_unsettled(
    pool: &PgPool,
    sender: &dyn ReplayRelaySender,
    min_age: Duration,
) -> Result<usize, sqlx::Error> {
    let rows = fetch_unsettled_older_than(pool, min_age).await?;
    let mut driven = 0usize;
    for row in rows {
        match drive_row(pool, sender, &row).await {
            Ok(()) => driven += 1,
            Err(e) => eprintln!(
                "replay re-drive: drive failed for {} (will retry): {e}",
                row.replay_instance_id
            ),
        }
    }
    Ok(driven)
}

/// The steady-state re-drive loop: every `REDRIVE_INTERVAL`, re-drive unsettled
/// rows older than `REDRIVE_MIN_AGE` until shutdown.
pub async fn run_replay_redrive(
    pool: Arc<PgPool>,
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
                match redrive_unsettled(&pool, sender.as_ref(), REDRIVE_MIN_AGE).await {
                    Ok(0) => {}
                    Ok(n) => println!("replay re-drive: re-drove {n} unsettled replay(s)"),
                    Err(e) => eprintln!("replay re-drive pass failed: {e}"),
                }
            }
        }
    }
}

/// Boot-time reconcile: re-drive every unsettled replay row once at startup,
/// regardless of age. Follows the submission queue's `reconcile_orphan_ready_rows`
/// precedent — a process that died mid-drive (Trigger relayed but the ctx
/// re-hydration / release never landed) leaves a durable `Materializing` row;
/// this finishes it before steady-state traffic resumes. Runs exactly once on
/// startup.
pub async fn reconcile_orphan_replay_rows(
    pool: &PgPool,
    sender: &dyn ReplayRelaySender,
) -> Result<usize, sqlx::Error> {
    // `Duration::ZERO` → no age floor: every unsettled row is in scope at boot.
    redrive_unsettled(pool, sender, Duration::ZERO).await
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

// ---- Archive reads ---------------------------------------------------------

/// Read a source run's producer-attribution inputs into one [`ArchivedRun`]:
/// its replay provenance, its archived task-instance rows (each naming its
/// owning node), and its terminal ctx dump. The task-instance → node map is
/// reconstructed from archived task-instance rows. Returns `None` when the
/// `workflow_instances` blob is absent.
async fn read_archived_run(pool: &PgPool, id: Uuid) -> Result<Option<ArchivedRun>> {
    let Some(instance_json) = read_instance_blob(pool, id).await? else {
        return Ok(None);
    };
    let replay_source = replay_source_from_blob(&instance_json);
    let task_instances = read_task_instance_rows(pool, id).await?;
    let ctx_dump = read_ctx_dump(pool, id).await?;
    Ok(Some(ArchivedRun {
        instance_id: id,
        replay_source,
        task_instances,
        ctx_dump,
    }))
}

/// Read the raw archived instance JSON blob from `workflow_instances`. `None`
/// when the row is absent.
async fn read_instance_blob(pool: &PgPool, id: Uuid) -> Result<Option<serde_json::Value>> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT instance FROM workflow_instances WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .context("read archived instance blob")?;
    Ok(row.map(|(blob,)| blob))
}

/// A source run reduced to what the replay drive needs off the published
/// contract: the runnable projection reconstructed from the stored instance
/// blob, and the run's workflow id (a top-level column). Names no instance
/// aggregate — the runnable graph is rebuilt from the projection, not decoded
/// from a `WorkflowInstance`.
struct ArchivedSource {
    projection: rp::RunnableProjection,
    workflow_id: Uuid,
}

/// Reconstruct the source run's runnable projection from its archived
/// `workflow_instances` blob without decoding the instance aggregate: the read
/// reconstructs the union archive projection from the raw blob and takes its
/// embedded runnable section, so the replay seed reads the one stored shape
/// every archive reader consumes and no data-plane read site names the instance
/// aggregate. The task-instance rows are not needed here (the replay seed
/// consumes only the runnable graph), so an empty set is passed. `None` when the
/// row is absent.
async fn read_archived_projection(pool: &PgPool, id: Uuid) -> Result<Option<ArchivedSource>> {
    let row: Option<(Uuid, serde_json::Value)> =
        sqlx::query_as("SELECT workflow_id, instance FROM workflow_instances WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .context("read archived instance blob")?;
    match row {
        Some((workflow_id, blob)) => {
            let projection = archive_projection_from_json(blob)
                .context("decode union archive projection from archived blob")?
                .runnable
                .context("union archive projection carries no runnable section")?;
            Ok(Some(ArchivedSource {
                projection,
                workflow_id,
            }))
        }
        None => Ok(None),
    }
}

/// Read the archived task-instance rows for one run — the two producer-
/// attribution facts per row: the task-instance id and its owning node
/// (`task_id`). Both are top-level columns, so no JSONB decoding is needed.
/// Current and superseded retry attempts each have their own row.
async fn read_task_instance_rows(pool: &PgPool, id: Uuid) -> Result<Vec<ArchivedTaskInstanceRow>> {
    let rows: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT id, task_id FROM task_instances WHERE workflow_instance_id = $1")
            .bind(id)
            .fetch_all(pool)
            .await
            .context("read archived task-instance rows")?;
    Ok(rows
        .into_iter()
        .map(|(id, node_id)| ArchivedTaskInstanceRow { id, node_id })
        .collect())
}

/// The replay provenance of an archived run, read from the stored union
/// projection's `triggered_by`. `Some(source)` indicates that the run was
/// itself a replay. This is the parent link the
/// chained-replay walk follows. The union carries provenance as the flattened
/// `TriggerProvenanceView` — `{"kind":"Replay","source_instance":{"id":"…"},…}`
/// — so a Replay is recognised by `kind` and its source id read off
/// `source_instance.id`; every other kind (`"Cron"`, `"Manual"`, …) yields
/// `None`.
fn replay_source_from_blob(instance_json: &serde_json::Value) -> Option<Uuid> {
    let provenance = instance_json.get("triggered_by")?;
    if provenance.get("kind")?.as_str()? != "Replay" {
        return None;
    }
    provenance
        .get("source_instance")?
        .get("id")?
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// Read the terminal ctx dump (`workflow_run_info.ctx_envelope`) — a JSON array
/// of `{key, envelope}` entries — into the re-hydration module's input shape.
/// A missing row (run archived without ctx) is an empty dump, not an error.
async fn read_ctx_dump(pool: &PgPool, id: Uuid) -> Result<Vec<ArchivedCtxEntry>> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT ctx_envelope FROM workflow_run_info WHERE workflow_instance_id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .context("read archived ctx dump")?;
    let Some((value,)) = row else {
        return Ok(Vec::new());
    };
    let Some(arr) = value.as_array() else {
        return Ok(Vec::new());
    };
    Ok(arr
        .iter()
        .filter_map(|entry| {
            let key = entry.get("key")?.as_str()?.to_string();
            let envelope = entry.get("envelope")?.clone();
            Some(ArchivedCtxEntry { key, envelope })
        })
        .collect())
}

/// Gather the owning-ancestor runs a chained replay needs, walking the
/// replay-provenance parent links from `source` outward — one blob per
/// generation. Returns an empty map for an origin run.
async fn gather_ancestors(
    pool: &PgPool,
    source: &ArchivedRun,
) -> Result<HashMap<Uuid, ArchivedRun>> {
    let mut ancestors: HashMap<Uuid, ArchivedRun> = HashMap::new();
    let mut next = source.replay_source;
    let mut guard = 0usize;
    while let Some(pid) = next {
        if ancestors.contains_key(&pid) {
            break;
        }
        let Some(run) = read_archived_run(pool, pid).await? else {
            // Ancestor blob absent — the hydration gate names it; the plan
            // treats its producers as unattributable. Stop the walk here.
            break;
        };
        next = run.replay_source;
        ancestors.insert(pid, run);
        guard += 1;
        if guard > MAX_CHAIN_DEPTH {
            break;
        }
    }
    Ok(ancestors)
}

// ---- Inputs shadow ---------------------------------------------------------

/// Build the inputs-shadow writes and their audit name list.
///
/// `inputs` shadows **declared trigger captures of the pinned version only**.
/// Each shadowed capture becomes a genuine `Producer::Signal` envelope (the
/// replay signal's own input) written into the replay's fresh ctx scope; the
/// returned names (sorted) are audited on the pipeline row — names only, never
/// values, since a shadowed value may be a secret. An empty `inputs` reads the
/// definition mirror not at all.
///
/// Validation is a **definition** read, not an archive read: the archived
/// envelopes carry captured *values* (and their `Producer`), not the
/// declaration list, so `Producer` alone cannot tell a declared-but-unsupplied
/// capture from a genuinely-undeclared key. The declared-capture schema is read
/// from the conductor-Postgres definition mirror **at the archived
/// `workflow_version`** — a version-pinned read, never the version-blind
/// helper: a capture renamed or dropped in a newer version must not reject a
/// legit refresh of a v1 capture on a v1 replay.
async fn build_shadow_writes(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
    inputs: &HashMap<String, serde_json::Value>,
    signal_id: Uuid,
) -> Result<(Vec<CarriedKey>, Vec<String>), ReplayError> {
    if inputs.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // version-pinned read: validate the shadow against the version that
    // actually ran, not whatever the mirror's arbitrary latest row happens to
    // be, so a re-registration that renames/drops a capture can't reject a
    // legitimate credential refresh on the archived run.
    let definition = read_workflow_definition_at(pool, workflow_id, workflow_version)
        .await
        .map_err(ReplayError::Archive)?
        .ok_or(ReplayError::ShadowSchemaUnresolvable {
            version: workflow_version,
        })?;

    let declared: HashSet<&str> = definition
        .captures
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    // Task-produced names are distinguished only to give the operator a precise
    // "history-editing is out of scope" reject rather than a bare "undeclared".
    let task_outputs: HashSet<String> = definition
        .tasks
        .iter()
        .flat_map(|task| task.outputs.iter().cloned())
        .collect();

    let mut writes: Vec<CarriedKey> = Vec::with_capacity(inputs.len());
    let mut names: Vec<String> = Vec::with_capacity(inputs.len());
    for (key, value) in inputs {
        if declared.contains(key.as_str()) {
            let envelope = shadow_envelope(value, signal_id);
            let bytes = serde_json::to_vec(&envelope).map_err(|e| {
                ReplayError::Archive(anyhow::anyhow!("serialize shadow envelope `{key}`: {e}"))
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

    // Deterministic order for the KV write sequence and the audit list.
    writes.sort_by(|a, b| a.name.cmp(&b.name));
    names.sort();
    Ok((writes, names))
}

/// Build a `Producer::Signal` envelope for one shadowed capture value. The
/// envelope's `kind` reflects the JSON shape (matching the ordinary
/// trigger-capture extractor), and the producer is the replay signal — the
/// shadow is a genuine input of the replay's own trigger, not carried state.
fn shadow_envelope(value: &serde_json::Value, signal_id: Uuid) -> Envelope {
    let producer = Producer::Signal {
        signal_id,
        source: SignalSource::Manual,
    };
    let kind = match value {
        serde_json::Value::String(_) => "string",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => "int",
        serde_json::Value::Number(_) => "float",
        serde_json::Value::Null | serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            "json"
        }
    };
    Envelope::new(kind, value.clone(), false, producer)
}

/// Read the registered workflow definition **at a specific version** from the
/// conductor-Postgres definition mirror. The mirror is per-version (`workflows`
/// PK `(id, version)`), so the shadow validation must key on `(id, version)` —
/// the version-blind `WHERE id = $1` read would validate against an arbitrary
/// row and reject a legit v1 refresh after a v2 re-registration. This is a
/// Postgres read, so it survives a live registry reset.
async fn read_workflow_definition_at(
    pool: &PgPool,
    workflow_id: Uuid,
    version: i64,
) -> Result<Option<wf::WorkflowDefinition>> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(workflow_id)
            .bind(version)
            .fetch_optional(pool)
            .await
            .context("read pinned workflow definition")?;
    match row {
        Some((definition,)) => Ok(Some(
            crate::definition_store::proto_from_stored_definition(definition)
                .context("decode pinned workflow definition")?,
        )),
        None => Ok(None),
    }
}

// ---- Row writes / reads ----------------------------------------------------

type ReplayRowTuple = (
    Uuid,
    Uuid,
    Uuid,
    Option<String>,
    String,
    serde_json::Value,
    Option<String>,
    Option<String>,
    serde_json::Value,
);

fn row_from_tuple(t: ReplayRowTuple) -> ReplayRow {
    ReplayRow {
        replay_instance_id: t.0,
        source_instance_id: t.1,
        signal_id: t.2,
        idempotency_key: t.3,
        status: t.4,
        resume_from: serde_json::from_value(t.5).unwrap_or_default(),
        name: t.6,
        seed_sha256: t.7,
        shadowed_keys: serde_json::from_value(t.8).unwrap_or_default(),
    }
}

const ROW_COLUMNS: &str = "replay_instance_id, source_instance_id, signal_id, idempotency_key, \
     status, resume_from, name, seed_sha256, shadowed_keys";

/// Insert the `Materializing` row. Returns `false` when the `(source, key)`
/// UNIQUE constraint rejected the insert (a concurrent same-key ingress won).
#[allow(clippy::too_many_arguments)]
async fn insert_materializing_row(
    pool: &PgPool,
    replay_instance_id: Uuid,
    source_instance_id: Uuid,
    signal_id: Uuid,
    idempotency_key: Option<&str>,
    resume_from: &[Uuid],
    pre_grounded: &[Uuid],
    name: Option<&str>,
    seed_sha256: &str,
    shadowed_keys: &[String],
) -> Result<bool, sqlx::Error> {
    let resume_json = serde_json::to_value(resume_from).unwrap_or(serde_json::Value::Array(vec![]));
    let pre_grounded_json =
        serde_json::to_value(pre_grounded).unwrap_or(serde_json::Value::Array(vec![]));
    // Names-only audit: the shadowed capture names, never their values.
    let shadowed_json =
        serde_json::to_value(shadowed_keys).unwrap_or(serde_json::Value::Array(vec![]));
    let result = sqlx::query(
        "INSERT INTO workflow_replays
            (replay_instance_id, source_instance_id, signal_id, idempotency_key,
             status, resume_from, pre_grounded, name, seed_sha256, shadowed_keys)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT DO NOTHING",
    )
    .bind(replay_instance_id)
    .bind(source_instance_id)
    .bind(signal_id)
    .bind(idempotency_key)
    .bind(STATUS_MATERIALIZING)
    .bind(&resume_json)
    .bind(&pre_grounded_json)
    .bind(name)
    .bind(seed_sha256)
    .bind(&shadowed_json)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Persist the terminal `VersionUnresolvable` park for a blob-absent source.
/// Idempotent under a keyed retry via `ON CONFLICT DO NOTHING`.
async fn park_version_unresolvable(
    pool: &PgPool,
    replay_instance_id: Uuid,
    source_instance_id: Uuid,
    signal_id: Uuid,
    idempotency_key: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO workflow_replays
            (replay_instance_id, source_instance_id, signal_id, idempotency_key,
             status, outcome)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT DO NOTHING",
    )
    .bind(replay_instance_id)
    .bind(source_instance_id)
    .bind(signal_id)
    .bind(idempotency_key)
    .bind(STATUS_VERSION_UNRESOLVABLE)
    .bind("source run's archived blob is absent — nothing to replay")
    .execute(pool)
    .await?;
    Ok(())
}

/// `Materializing → Released` after a successful drive.
async fn flip_to_released(pool: &PgPool, replay_instance_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE workflow_replays
            SET status = 'Released', outcome = 'released', updated_at = now()
          WHERE replay_instance_id = $1 AND status = 'Materializing'",
    )
    .bind(replay_instance_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Bump the re-drive backoff anchor without settling the row.
async fn touch_row(pool: &PgPool, replay_instance_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE workflow_replays SET updated_at = now() WHERE replay_instance_id = $1")
        .bind(replay_instance_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Read one row by its deterministic replay instance id (the pollable /
/// test-assertion read).
pub async fn fetch_row(
    pool: &PgPool,
    replay_instance_id: Uuid,
) -> Result<Option<ReplayRow>, sqlx::Error> {
    let row = sqlx::query_as::<_, ReplayRowTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays WHERE replay_instance_id = $1"
    ))
    .bind(replay_instance_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_from_tuple))
}

/// The idempotency dedup read: an existing keyed row for this source run.
async fn fetch_row_by_idempotency(
    pool: &PgPool,
    source_instance_id: Uuid,
    idempotency_key: &str,
) -> Result<Option<ReplayRow>, sqlx::Error> {
    let row = sqlx::query_as::<_, ReplayRowTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays
          WHERE source_instance_id = $1 AND idempotency_key = $2"
    ))
    .bind(source_instance_id)
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(row_from_tuple))
}

/// List a source run's replays (the reverse-link read), newest first.
pub async fn fetch_replays_for_source(
    pool: &PgPool,
    source_instance_id: Uuid,
) -> Result<Vec<ReplayRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, ReplayRowTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays
          WHERE source_instance_id = $1 ORDER BY created_at DESC"
    ))
    .bind(source_instance_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_from_tuple).collect())
}

/// The re-drive scan: `Materializing` rows untouched for at least `min_age`.
async fn fetch_unsettled_older_than(
    pool: &PgPool,
    min_age: Duration,
) -> Result<Vec<ReplayRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, ReplayRowTuple>(&format!(
        "SELECT {ROW_COLUMNS} FROM workflow_replays
          WHERE status = 'Materializing'
            AND updated_at < now() - make_interval(secs => $1)
          ORDER BY created_at"
    ))
    .bind(min_age.as_secs_f64())
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(row_from_tuple).collect())
}
