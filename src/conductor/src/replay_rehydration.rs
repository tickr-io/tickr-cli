//! Replay re-hydration: carry the source run's coordination state forward.
//!
//! When a terminal run is replayed, its already-grounded HyperNodes carry
//! forward (the pre-grounded set), and so must the coordination state those
//! HyperNodes captured — the `tickr-ctx scope` values later tasks read. This
//! module is the self-contained mechanism that does exactly that: it reads the
//! source run's archived ctx dump (the terminal `workflow_run_info.ctx_envelope`
//! whole-scope dump) and value-copies the carried keys into the **replay's
//! fresh live tickr-ctx scope**, writes a hydration sentinel as the final act,
//! and hands the caller the release command to lift the born-Stall.
//!
//! It encapsulates five behaviours behind one interface, each independently
//! testable over synthetic archived blobs:
//!
//! - **Value-copy, verbatim.** ctx envelopes are small routing state (bulk
//!   payloads live behind `log_uris` pointers), so carried keys copy the
//!   archived envelope **bytes verbatim** — never re-wrapped through
//!   `Envelope::new`, which would stamp a fresh `created_at`/`sha256` and could
//!   launder the value into the boundary-exempt `Producer::System` class.
//! - **Membership predicate (absent-not-stale).** A key re-hydrates iff its
//!   producing HyperNode — chain-resolved through replay provenance — lies in
//!   the pre-grounded set. A key outside the set is **absent** (never-written),
//!   not stale; a key that cannot be attributed to any HyperNode is
//!   **flagged-absent** and enumerated.
//! - **Chained-replay producer resolution.** For a replay whose source run was
//!   itself a replay, a carried value's producer id belongs to an **owning
//!   ancestor** (the run that actually executed the task). We resolve it by
//!   walking the replay-provenance parent links across archived blobs — one
//!   blob consulted per generation (O(depth), not O(values)) — rebuilding each
//!   generation's task-instance → node map from that generation's archived
//!   task-instance rows. Because every attempt is archived against its owning
//!   node, superseded retries resolve naturally.
//! - **Reserved-prefix exclusion.** Infrastructure keys (`tickr_graph`,
//!   `tickr_replay/*`) never carry forward. Load-bearing, not hygiene: the
//!   boundary-exempt `tickr_graph` mirror would otherwise carry the parent's
//!   final graph, and the present-key guard in the graph-mirror writer would
//!   then skip writing the replay's **own** graph mirror.
//! - **Hydration sentinel + release.** `tickr_replay/hydrated` is written as
//!   the **final** re-hydration act; the caller sends the release command only
//!   after this module's `apply_rehydration` returns Ok — so the born-Stall is
//!   released via the idempotent `resume_instance` only after the sentinel has
//!   landed. Chaining is sentinel-gated: replaying a run whose own re-hydration
//!   never completed (a cancelled-never-released replay, whose terminal dump
//!   lacks the sentinel) is a typed [`RehydrationReject::ParentNeverHydrated`].

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use async_nats::jetstream;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::ctx_graph_mirror::CTX_GRAPH_KEY;
use tickr_ctx::envelope::{Envelope, Producer};
use tickr_ctx::scope::sanitize_segment;
use tickr_proto::signal as sp;

/// Per-tenant tickr-ctx bucket namespace. Matches every other ctx writer so the
/// carried keys land in the bucket the executor reads from.
const DEFAULT_CTX_NAMESPACE: &str = "default";

/// Reserved run-scoped key namespace this feature owns. The hydration sentinel
/// lives at [`HYDRATION_SENTINEL_KEY`]; the whole `tickr_replay/` prefix (like
/// the `tickr_graph` mirror) never carries forward.
pub const REPLAY_KEY_PREFIX: &str = "tickr_replay/";

/// The terminal marker of completed re-hydration, written as the final ctx act
/// and gating chained replay. `Producer::System`, payload `{ signal_id,
/// carried_count, key_list_sha256 }`.
pub const HYDRATION_SENTINEL_KEY: &str = "tickr_replay/hydrated";

/// Cycle backstop for the `triggered_by` chain walk. Replay chains are shallow
/// in practice; this only bounds a corrupt self-referential provenance.
const MAX_CHAIN_DEPTH: usize = 4096;

/// One archived ctx entry from a run's terminal dump. The parsed envelope is
/// used only for lineage checks; `envelope_bytes` is the exact accepted payload
/// copied during rehydration.
#[derive(Debug, Clone)]
pub struct ArchivedCtxEntry {
    /// The full run-scoped key as archived, `<sanitized_run_id>/<name>`.
    pub key: String,
    /// The envelope as archived (opaque JSON — read for its `producer`, copied
    /// verbatim on carry).
    pub envelope: serde_json::Value,
    pub envelope_bytes: Vec<u8>,
}

/// One archived task-instance row, reduced to the two facts producer
/// attribution needs: the task-instance ID and the graph node it ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedTaskInstanceRow {
    /// The task-instance id (the row's own identity — the producer id a Task
    /// envelope carries).
    pub id: Uuid,
    /// The graph node this task instance ran. Every attempt of a node —
    /// current or superseded by a retry — is its own archived row naming the
    /// same owning node, so the map covers superseded retries by construction.
    pub node_id: Uuid,
}

/// An archived run reduced to what producer attribution needs: its identity,
/// its replay provenance (the source run a chained replay walks to), its
/// archived task-instance rows, and its terminal ctx dump. This is the unit of
/// input to both the rehydration plan and the sentinel gate.
#[derive(Debug, Clone)]
pub struct ArchivedRun {
    /// The run's own instance id.
    pub instance_id: Uuid,
    /// The source run this run was a replay of, if any. `Some` iff the run's
    /// archived provenance is a Replay; drives the chained-replay ancestor walk
    /// and the sentinel-gated hydration-completeness check.
    pub replay_source: Option<Uuid>,
    /// The run's archived task-instance rows (current and superseded attempts).
    pub task_instances: Vec<ArchivedTaskInstanceRow>,
    pub ctx_dump: Vec<ArchivedCtxEntry>,
}

/// A key carried forward into the replay's fresh scope: the bare name (the
/// run-prefix is re-applied at write time) and the verbatim envelope bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarriedKey {
    pub name: String,
    pub bytes: Vec<u8>,
}

/// The planned re-hydration for a replay: what carries, what could not be
/// attributed, and the sentinel payload facts. Pure — no clock, no I/O — so it
/// is deterministic and testable in isolation. [`apply_rehydration`] executes
/// it against NATS.
#[derive(Debug, Clone)]
pub struct RehydrationPlan {
    /// Keys that carry forward, sorted by name for a deterministic write order
    /// and sentinel digest.
    pub carried: Vec<CarriedKey>,
    /// The inputs-shadow writes: the replay signal's genuine `Producer::Signal`
    /// inputs re-supplying declared trigger captures of the pinned version.
    /// Distinct from `carried` (which is archive-sourced task-output state):
    /// these are request-supplied, so they are NOT reflected in the hydration
    /// sentinel's carried-set digest and are NOT reconstructable on a re-drive
    /// (the row audits their names only, never their values) — a re-drive
    /// leaves this empty and relies on the first drive's write having landed.
    /// Empty when the replay carried no shadow.
    pub shadowed: Vec<CarriedKey>,
    /// Keys that could not be attributed to any HyperNode (a Task producer that
    /// resolves to no node through the chain, a non-reserved System write, or
    /// an unparseable envelope), enumerated and sorted. Surfaced to the
    /// operator — never silently dropped.
    pub flagged_absent: Vec<String>,
    /// `carried.len()`, recorded on the sentinel.
    pub carried_count: usize,
    /// sha256 over the sorted carried key names (newline-joined), recorded on
    /// the sentinel as an integrity witness of the carried set.
    pub key_list_sha256: String,
    /// The replay signal id, recorded on the sentinel payload.
    pub replay_signal_id: Uuid,
}

/// A typed reason a chained replay cannot proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RehydrationReject {
    /// The run being replayed was itself a replay whose re-hydration never
    /// completed — its terminal dump carries no [`HYDRATION_SENTINEL_KEY`], so
    /// building on its coordination state would build on an incomplete scope.
    /// Names the nearest hydration-complete ancestor to replay instead.
    ParentNeverHydrated { nearest_hydrated_ancestor: Uuid },
}

/// Strip the run-id prefix from a full ctx key, yielding the bare name. Keys are
/// `<sanitized_run_id>/<name>`; the name itself may contain `/`
/// (`tickr_replay/hydrated`), so split on the **first** separator only.
fn key_name(full_key: &str) -> &str {
    full_key.splitn(2, '/').nth(1).unwrap_or(full_key)
}

/// Reserved infrastructure keys that never carry forward: the `tickr_graph`
/// mirror and the whole `tickr_replay/` namespace.
fn is_reserved(name: &str) -> bool {
    name == CTX_GRAPH_KEY || name.starts_with(REPLAY_KEY_PREFIX)
}

/// Read an archived entry's `Producer` for the membership predicate. Robust to
/// v=1/v=2 shapes via the `Envelope` deserializer; `None` on an unparseable
/// envelope (treated as unattributable).
fn producer_of(entry: &ArchivedCtxEntry) -> Option<Producer> {
    serde_json::from_value::<Envelope>(entry.envelope.clone())
        .ok()
        .map(|e| e.producer)
}

/// Build the `task-instance-id → node-id` resolver for one archived run from
/// its archived task-instance rows.
///
/// Each row names its owning node (`task_id`), and every attempt is stored as a
/// separate row against that node. One pass therefore maps current and
/// superseded task-instance IDs back to their owning nodes.
fn ti_to_node_for_run(run: &ArchivedRun) -> HashMap<Uuid, Uuid> {
    run.task_instances
        .iter()
        .map(|ti| (ti.id, ti.node_id))
        .collect()
}

/// Resolve a batch of producer task-instance ids to node ids, walking the
/// replay-provenance parent links from `source` outward — **one blob consulted
/// per generation**, resolving the whole remaining batch against each
/// (O(depth), not O(values)). A carried value's producer belongs to whichever
/// ancestor actually executed the task; that generation's archived
/// task-instance rows resolve it.
fn resolve_producers(
    source: &ArchivedRun,
    ancestors: &HashMap<Uuid, ArchivedRun>,
    mut unresolved: HashSet<Uuid>,
) -> HashMap<Uuid, Uuid> {
    let mut resolved: HashMap<Uuid, Uuid> = HashMap::new();
    let mut current: Option<&ArchivedRun> = Some(source);
    let mut guard = 0usize;

    while let Some(run) = current {
        if unresolved.is_empty() {
            break;
        }
        let map = ti_to_node_for_run(run);
        unresolved.retain(|ti| match map.get(ti) {
            Some(node) => {
                resolved.insert(*ti, *node);
                false
            }
            None => true,
        });

        // Advance to the owning-ancestor run along the same replay-provenance
        // link the walk always used — one blob consulted per generation.
        current = run.replay_source.and_then(|src| ancestors.get(&src));

        guard += 1;
        if guard > MAX_CHAIN_DEPTH {
            break;
        }
    }
    resolved
}

/// Plan the re-hydration of a replay from its source run's archive.
///
/// `source` is the run being replayed; `ancestors` holds any owning-ancestor
/// blobs a chained replay needs (keyed by instance id); `pre_grounded` is the
/// replay's pre-grounded node set (a birth-time fact from the Replay
/// provenance); `replay_signal_id` is stamped on the sentinel. Pure and
/// deterministic — the returned plan is executed by [`apply_rehydration`].
pub fn plan_rehydration(
    source: &ArchivedRun,
    ancestors: &HashMap<Uuid, ArchivedRun>,
    pre_grounded: &HashSet<Uuid>,
    replay_signal_id: Uuid,
) -> RehydrationPlan {
    // Collect every Task producer id across the non-reserved entries, then
    // resolve the whole batch through the chain in one pass.
    let mut producer_ids: HashSet<Uuid> = HashSet::new();
    for entry in &source.ctx_dump {
        if is_reserved(key_name(&entry.key)) {
            continue;
        }
        if let Some(Producer::Task { task_id, .. }) = producer_of(entry) {
            if let Ok(ti) = Uuid::parse_str(&task_id) {
                producer_ids.insert(ti);
            }
        }
    }
    let resolved = resolve_producers(source, ancestors, producer_ids);

    let mut carried: Vec<CarriedKey> = Vec::new();
    let mut flagged: Vec<String> = Vec::new();

    for entry in &source.ctx_dump {
        let name = key_name(&entry.key).to_string();
        // Reserved keys are excluded by construction — not carried, not flagged.
        if is_reserved(&name) {
            continue;
        }
        match producer_of(entry) {
            Some(Producer::Task { task_id, .. }) => {
                let node = Uuid::parse_str(&task_id)
                    .ok()
                    .and_then(|ti| resolved.get(&ti).copied());
                match node {
                    // Producing HyperNode carried forward → re-hydrate verbatim.
                    Some(n) if pre_grounded.contains(&n) => {
                        // Copy the archived envelope bytes verbatim; rebuilding
                        // through `Envelope::new` would stamp a fresh
                        // created_at/sha and launder the producer.
                        let bytes = entry.envelope_bytes.clone();
                        carried.push(CarriedKey { name, bytes });
                    }
                    // Producing HyperNode outside the set → absent (the task
                    // re-runs), never-written, not stale, not flagged.
                    Some(_) => {}
                    // No node through the whole chain → unattributable.
                    None => flagged.push(name),
                }
            }
            // Trigger-capture domain: re-supplied fresh by the replay's own
            // trigger (the inputs-shadow lever), never carried here.
            Some(Producer::Signal { .. }) => {}
            // A System write to a non-reserved key is unexpected; flag it rather
            // than carry an un-attributable engine value.
            Some(Producer::System { .. }) => flagged.push(name),
            // Unparseable envelope → cannot attribute → flag.
            None => flagged.push(name),
        }
    }

    carried.sort_by(|a, b| a.name.cmp(&b.name));
    flagged.sort();
    let carried_count = carried.len();
    let key_list_sha256 = sha256_of_names(carried.iter().map(|c| c.name.as_str()));

    RehydrationPlan {
        carried,
        // Archive-sourced planning writes no shadow: the inputs-shadow is
        // request-supplied and set by the ingress after this returns.
        shadowed: Vec::new(),
        flagged_absent: flagged,
        carried_count,
        key_list_sha256,
        replay_signal_id,
    }
}

/// sha256 over a name list (newline-joined). The caller passes names in the
/// plan's sorted order so the digest is stable.
fn sha256_of_names<'a>(names: impl Iterator<Item = &'a str>) -> String {
    let joined = names.collect::<Vec<_>>().join("\n");
    let mut hasher = Sha256::new();
    hasher.update(joined.as_bytes());
    hex::encode(hasher.finalize())
}

/// Does this run's terminal dump carry the hydration sentinel?
fn has_sentinel(ctx_dump: &[ArchivedCtxEntry]) -> bool {
    ctx_dump
        .iter()
        .any(|e| key_name(&e.key) == HYDRATION_SENTINEL_KEY)
}

/// A run is hydration-complete iff it is not a replay (an origin run needs no
/// sentinel) or its terminal dump carries the sentinel.
fn is_hydration_complete(run: &ArchivedRun) -> bool {
    if run.replay_source.is_some() {
        has_sentinel(&run.ctx_dump)
    } else {
        true
    }
}

/// Sentinel-gated chaining check.
///
/// Passes when the run being replayed is hydration-complete. When it is a
/// replay whose sentinel never landed (a cancelled-never-released replay),
/// rejects with [`RehydrationReject::ParentNeverHydrated`] naming the nearest
/// hydration-complete ancestor to replay instead. The gate's evidence rides the
/// archive itself — the terminal dump contains the sentinel iff hydration
/// completed — so it does not depend on any pipeline-row retention.
pub fn parent_hydration_gate(
    source: &ArchivedRun,
    ancestors: &HashMap<Uuid, ArchivedRun>,
) -> Result<(), RehydrationReject> {
    if is_hydration_complete(source) {
        return Ok(());
    }

    let mut parent_id = source.replay_source;
    let mut guard = 0usize;
    while let Some(pid) = parent_id {
        match ancestors.get(&pid) {
            Some(anc) => {
                if is_hydration_complete(anc) {
                    return Err(RehydrationReject::ParentNeverHydrated {
                        nearest_hydrated_ancestor: pid,
                    });
                }
                parent_id = anc.replay_source;
            }
            // The ancestor blob is not available to prove completeness; name the
            // furthest id we could reach — the caller cannot chain past it.
            None => {
                return Err(RehydrationReject::ParentNeverHydrated {
                    nearest_hydrated_ancestor: pid,
                })
            }
        }
        guard += 1;
        if guard > MAX_CHAIN_DEPTH {
            break;
        }
    }

    // The chain root is a non-replay origin (always complete), so this is
    // unreachable in a well-formed chain; name the source itself as a fallback.
    Err(RehydrationReject::ParentNeverHydrated {
        nearest_hydrated_ancestor: source.instance_id,
    })
}

/// The hydration sentinel envelope for a completed re-hydration. Freshly built
/// (`Producer::System`) — this is a genuine engine write, unlike the carried
/// keys which are copied verbatim.
fn build_sentinel_envelope(
    signal_id: Uuid,
    carried_count: usize,
    key_list_sha256: &str,
) -> Envelope {
    let payload = serde_json::json!({
        "signal_id": signal_id,
        "carried_count": carried_count,
        "key_list_sha256": key_list_sha256,
    });
    Envelope::new(
        "json",
        payload,
        false,
        Producer::System {
            component: "conductor".to_string(),
        },
    )
}

/// Get-or-create the per-tenant ctx KV bucket the re-hydration writes into.
/// Mirrors the graph-mirror writer so carried keys land in the executor's
/// bucket.
async fn get_or_create_ctx_bucket(js: &jetstream::Context) -> Result<jetstream::kv::Store> {
    let bucket_name = tickr_ctx::scope::bucket_for_namespace(DEFAULT_CTX_NAMESPACE);
    match js.get_key_value(&bucket_name).await {
        Ok(kv) => Ok(kv),
        Err(_) => js
            .create_key_value(jetstream::kv::Config {
                bucket: bucket_name.clone(),
                history: 1,
                max_value_size: tickr_ctx::store::MAX_VALUE_SIZE,
                ..Default::default()
            })
            .await
            .context("create ctx bucket for replay re-hydration"),
    }
}

/// Execute a [`RehydrationPlan`] against the replay's fresh ctx scope.
///
/// Writes every carried key first, then the hydration sentinel as the **final**
/// act. The caller sends the release command (see [`release_signal`]) only
/// after this returns Ok, so the born-Stall is released strictly after the
/// sentinel has landed. Idempotent on retry: KV puts are last-writer-wins on
/// identical bytes.
pub async fn apply_rehydration(
    kv: &jetstream::kv::Store,
    replay_run_id: Uuid,
    plan: &RehydrationPlan,
) -> Result<()> {
    let prefix = sanitize_segment(&replay_run_id.to_string());

    for carried in &plan.carried {
        let key = format!("{}/{}", prefix, carried.name);
        kv.put(&key, carried.bytes.clone().into())
            .await
            .map_err(|e| anyhow::anyhow!("replay re-hydration put {key}: {e}"))?;
    }

    // The inputs-shadow writes: the replay signal's genuine Producer::Signal
    // inputs, re-supplying declared trigger captures of the pinned version.
    // Written alongside the carried keys so they land before the sentinel (and,
    // for a born-Stalled replay, before release). A shadowed capture name never
    // collides with a carried key: captures and task outputs share the reader
    // namespace and collisions are rejected at registration.
    for shadow in &plan.shadowed {
        let key = format!("{}/{}", prefix, shadow.name);
        kv.put(&key, shadow.bytes.clone().into())
            .await
            .map_err(|e| anyhow::anyhow!("replay inputs-shadow put {key}: {e}"))?;
    }

    // Final act: the sentinel. Release rides on this having landed.
    let sentinel = build_sentinel_envelope(
        plan.replay_signal_id,
        plan.carried_count,
        &plan.key_list_sha256,
    );
    let sentinel_bytes = serde_json::to_vec(&sentinel).context("serialize hydration sentinel")?;
    let sentinel_key = format!("{}/{}", prefix, HYDRATION_SENTINEL_KEY);
    kv.put(&sentinel_key, sentinel_bytes.into())
        .await
        .map_err(|e| anyhow::anyhow!("replay re-hydration sentinel put {sentinel_key}: {e}"))?;

    Ok(())
}

/// Build Tickr Lite's ordered scope values for one replay re-hydration. The
/// local scope transaction commits carried keys, shadows, and the sentinel
/// atomically before the caller relays the release Signal.
pub fn local_rehydration_values(plan: &RehydrationPlan) -> Result<Vec<CarriedKey>> {
    let mut values = Vec::with_capacity(plan.carried.len() + plan.shadowed.len() + 1);
    values.extend(plan.carried.iter().cloned());
    values.extend(plan.shadowed.iter().cloned());
    let sentinel = build_sentinel_envelope(
        plan.replay_signal_id,
        plan.carried_count,
        &plan.key_list_sha256,
    );
    values.push(CarriedKey {
        name: HYDRATION_SENTINEL_KEY.to_owned(),
        bytes: serde_json::to_vec(&sentinel).context("serialize hydration sentinel")?,
    });
    Ok(values)
}

/// Convenience: open the ctx bucket and apply a plan. Used by the conductor
/// ingress that drives a born-Stalled replay to release.
pub async fn apply_rehydration_via_nats(
    nats: &async_nats::Client,
    replay_run_id: Uuid,
    plan: &RehydrationPlan,
) -> Result<()> {
    let js = jetstream::new(nats.clone());
    let kv = get_or_create_ctx_bucket(&js).await?;
    apply_rehydration(&kv, replay_run_id, plan).await
}

/// The thin conductor→server release command that lifts a born-Stalled replay's
/// birth-time Stall via the idempotent `resume_instance`. Its signal identity
/// is derived from the durable replay identity so a crash after relay forwarding
/// retries the same published command rather than minting a second effect. The
/// caller sends this only after [`apply_rehydration`] returns Ok — release rides
/// on the sentinel having landed. A dedicated command, never a zero-op patch:
/// provenance must not lie about what mutated the run.
pub fn release_signal(replay_run_id: Uuid) -> sp::Signal {
    sp::Signal {
        signal_id: Uuid::new_v5(&replay_run_id, b"release").to_string(),
        idempotency_key: None,
        variant: Some(sp::signal::Variant::Resume(sp::Resume {
            workflow_instance_id: replay_run_id.to_string(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickr_ctx::envelope::SignalSource;

    /// Build an archived run from the facts used for producer attribution: its
    /// ID, replay source, task-instance rows, and ctx dump.
    fn archived_run(
        instance_id: Uuid,
        replay_source: Option<Uuid>,
        task_instances: Vec<(Uuid, Uuid)>,
        ctx_dump: Vec<ArchivedCtxEntry>,
    ) -> ArchivedRun {
        ArchivedRun {
            instance_id,
            replay_source,
            task_instances: task_instances
                .into_iter()
                .map(|(id, node_id)| ArchivedTaskInstanceRow { id, node_id })
                .collect(),
            ctx_dump,
        }
    }

    fn task_envelope(task_id: Uuid, value: &str) -> serde_json::Value {
        let env = Envelope::new(
            "string",
            serde_json::Value::String(value.to_string()),
            false,
            Producer::Task {
                task_id: task_id.to_string(),
                task_name: "t".to_string(),
            },
        );
        serde_json::to_value(&env).unwrap()
    }

    fn signal_envelope(value: &str) -> serde_json::Value {
        let env = Envelope::new(
            "string",
            serde_json::Value::String(value.to_string()),
            false,
            Producer::Signal {
                signal_id: Uuid::new_v4(),
                source: SignalSource::Manual,
            },
        );
        serde_json::to_value(&env).unwrap()
    }

    fn system_envelope(value: &str) -> serde_json::Value {
        let env = Envelope::new(
            "json",
            serde_json::json!({ "v": value }),
            false,
            Producer::System {
                component: "conductor".to_string(),
            },
        );
        serde_json::to_value(&env).unwrap()
    }

    fn entry(run: Uuid, name: &str, envelope: serde_json::Value) -> ArchivedCtxEntry {
        let envelope_bytes = serde_json::to_vec(&envelope).unwrap();
        ArchivedCtxEntry {
            key: format!("{}/{}", sanitize_segment(&run.to_string()), name),
            envelope,
            envelope_bytes,
        }
    }

    #[test]
    fn carries_only_pre_grounded_producers_verbatim() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let run = Uuid::new_v4();
        let (ti_a, ti_b) = (Uuid::new_v4(), Uuid::new_v4());

        let out_a = task_envelope(ti_a, "value-a");
        let dump = vec![
            entry(run, "out_a", out_a.clone()),
            entry(run, "out_b", task_envelope(ti_b, "value-b")),
        ];
        // Each task-instance row names its owning node — the map the producer
        // attribution rebuilds from the archived rows.
        let source = archived_run(run, None, vec![(ti_a, a), (ti_b, b)], dump);

        // Only `a` is pre-grounded → only `out_a` carries; `out_b` is outside
        // the set → absent, not flagged.
        let pre_grounded: HashSet<Uuid> = [a].into_iter().collect();
        let plan = plan_rehydration(&source, &HashMap::new(), &pre_grounded, Uuid::new_v4());

        assert_eq!(
            plan.carried.len(),
            1,
            "only the pre-grounded producer carries"
        );
        assert_eq!(plan.carried[0].name, "out_a");
        assert!(
            plan.flagged_absent.is_empty(),
            "an outside-set key is not flagged"
        );

        // Verbatim: the carried bytes deserialize to an envelope whose
        // created_at / sha256 / producer are the archived ones, unchanged.
        let archived: Envelope = serde_json::from_value(out_a).unwrap();
        let carried: Envelope = serde_json::from_slice(&plan.carried[0].bytes).unwrap();
        assert_eq!(carried.created_at, archived.created_at);
        assert_eq!(carried.sha256, archived.sha256);
        assert_eq!(carried.producer, archived.producer);
        let _ = b;
    }

    #[test]
    fn unattributable_key_is_flagged_and_enumerated() {
        let a = Uuid::new_v4();
        let run = Uuid::new_v4();

        // A Task producer whose id maps to no node in this run or any ancestor.
        let orphan_ti = Uuid::new_v4();
        let dump = vec![entry(run, "orphan", task_envelope(orphan_ti, "v"))];
        // `a` has a row, but the orphan producer id has none.
        let source = archived_run(run, None, vec![(Uuid::new_v4(), a)], dump);
        let pre_grounded: HashSet<Uuid> = [a].into_iter().collect();
        let plan = plan_rehydration(&source, &HashMap::new(), &pre_grounded, Uuid::new_v4());

        assert!(plan.carried.is_empty());
        assert_eq!(
            plan.flagged_absent,
            vec!["orphan".to_string()],
            "an unattributable producer is flagged-absent and enumerated"
        );
    }

    #[test]
    fn reserved_and_signal_keys_never_carry() {
        let a = Uuid::new_v4();
        let run = Uuid::new_v4();
        let ti_a = Uuid::new_v4();

        let dump = vec![
            entry(run, "out_a", task_envelope(ti_a, "carry-me")),
            entry(run, CTX_GRAPH_KEY, system_envelope("parent-graph")),
            entry(
                run,
                HYDRATION_SENTINEL_KEY,
                system_envelope("parent-sentinel"),
            ),
            entry(run, "tickr_replay/anything", system_envelope("bookkeeping")),
            entry(run, "trigger_cap", signal_envelope("cred")),
        ];
        let source = archived_run(run, None, vec![(ti_a, a)], dump);
        let pre_grounded: HashSet<Uuid> = [a].into_iter().collect();
        let plan = plan_rehydration(&source, &HashMap::new(), &pre_grounded, Uuid::new_v4());

        let carried_names: Vec<&str> = plan.carried.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            carried_names,
            vec!["out_a"],
            "reserved (tickr_graph, tickr_replay/*) and Signal keys never carry"
        );
        // The reserved parent keys are excluded, not flagged.
        assert!(plan.flagged_absent.is_empty());
    }

    #[test]
    fn superseded_retry_task_instance_resolves_to_node() {
        // A node `a` ran twice: a superseded first attempt and the current one.
        // Both attempts are archived as their own rows against node `a`. A
        // carried value produced by the *superseded* attempt must still resolve
        // to node `a`.
        let a = Uuid::new_v4();
        let run = Uuid::new_v4();
        let (superseded_ti, current_ti) = (Uuid::new_v4(), Uuid::new_v4());

        let dump = vec![entry(run, "out_a", task_envelope(superseded_ti, "v"))];
        let source = archived_run(run, None, vec![(superseded_ti, a), (current_ti, a)], dump);
        let pre_grounded: HashSet<Uuid> = [a].into_iter().collect();
        let plan = plan_rehydration(&source, &HashMap::new(), &pre_grounded, Uuid::new_v4());

        assert_eq!(
            plan.carried.len(),
            1,
            "a value produced by a superseded retry attempt resolves to its node"
        );
        assert_eq!(plan.carried[0].name, "out_a");
        assert!(plan.flagged_absent.is_empty());
    }

    #[test]
    fn chained_replay_resolves_producer_against_owning_ancestor() {
        // R0 (origin) ran `a` and `b`. R1 (replay of R0) carried `a` forward
        // (no task-instance row for `a` in R1) and re-ran `b`. Replaying R1, a
        // carried value produced by R0's `a` must resolve through the chain to
        // node `a` — one blob consulted per generation.
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let r0_id = Uuid::new_v4();
        let (ti_a0, ti_b0) = (Uuid::new_v4(), Uuid::new_v4());
        // R0 ran both nodes: a row per node.
        let r0 = archived_run(r0_id, None, vec![(ti_a0, a), (ti_b0, b)], vec![]);

        // R1 is a replay of R0 that only re-ran `b`, so only `b` has a row.
        let r1_id = Uuid::new_v4();
        let ti_b1 = Uuid::new_v4();

        // R1's terminal dump: `out_a` still carries R0's producer (ti_a0),
        // `out_b` carries R1's producer (ti_b1).
        let dump = vec![
            entry(r1_id, "out_a", task_envelope(ti_a0, "from-r0")),
            entry(r1_id, "out_b", task_envelope(ti_b1, "from-r1")),
        ];
        let source = archived_run(r1_id, Some(r0_id), vec![(ti_b1, b)], dump);
        let ancestors: HashMap<Uuid, ArchivedRun> = [(r0_id, r0)].into_iter().collect();

        // Replaying R1 with both nodes pre-grounded: `out_a` resolves through
        // R0, `out_b` through R1 — both carry.
        let pre_grounded: HashSet<Uuid> = [a, b].into_iter().collect();
        let plan = plan_rehydration(&source, &ancestors, &pre_grounded, Uuid::new_v4());
        let mut names: Vec<&str> = plan.carried.iter().map(|c| c.name.as_str()).collect();
        names.sort();
        assert_eq!(
            names,
            vec!["out_a", "out_b"],
            "the R0-produced value resolves through the chain and carries"
        );
        assert!(plan.flagged_absent.is_empty());
    }

    #[test]
    fn parent_gate_passes_for_origin_and_hydrated_replay() {
        // An origin (non-replay) run needs no sentinel.
        let origin_run = archived_run(Uuid::new_v4(), None, vec![], vec![]);
        assert_eq!(parent_hydration_gate(&origin_run, &HashMap::new()), Ok(()));

        // A replay whose sentinel landed passes.
        let replay_id = Uuid::new_v4();
        let hydrated = archived_run(
            replay_id,
            Some(Uuid::new_v4()),
            vec![],
            vec![entry(
                replay_id,
                HYDRATION_SENTINEL_KEY,
                system_envelope("done"),
            )],
        );
        assert_eq!(parent_hydration_gate(&hydrated, &HashMap::new()), Ok(()));
    }

    #[test]
    fn parent_gate_rejects_replay_whose_sentinel_never_landed() {
        // R0 origin (complete); R1 a replay of R0 whose sentinel never landed.
        let r0_id = Uuid::new_v4();
        let r0 = archived_run(r0_id, None, vec![], vec![]);

        // R1 is a replay of R0 with no sentinel → never hydrated.
        let source = archived_run(Uuid::new_v4(), Some(r0_id), vec![], vec![]);
        let ancestors: HashMap<Uuid, ArchivedRun> = [(r0_id, r0)].into_iter().collect();

        assert_eq!(
            parent_hydration_gate(&source, &ancestors),
            Err(RehydrationReject::ParentNeverHydrated {
                nearest_hydrated_ancestor: r0_id,
            }),
            "the nearest hydration-complete ancestor is named"
        );
    }

    #[test]
    fn key_name_splits_on_first_separator_only() {
        assert_eq!(key_name("run-id/out_a"), "out_a");
        assert_eq!(
            key_name("run-id/tickr_replay/hydrated"),
            "tickr_replay/hydrated"
        );
    }

    #[test]
    fn release_signal_is_a_resume_command() {
        let id = Uuid::new_v4();
        let sig = release_signal(id);
        match sig.variant {
            Some(sp::signal::Variant::Resume(sp::Resume {
                workflow_instance_id,
            })) => assert_eq!(workflow_instance_id, id.to_string()),
            other => panic!("expected Resume, got {:?}", other),
        }
    }
}
