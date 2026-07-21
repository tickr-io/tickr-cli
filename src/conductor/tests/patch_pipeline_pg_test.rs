//! Integration tests for the conductor patch pipeline against ephemeral PG.
//!
//! Covers the lifecycle the pipeline owns end-to-end, with the relay seam
//! stubbed (the trait injection point, no gRPC):
//! - Ingress opens a `Validating` row, relays `PatchWorkflowInstance` with
//!   `patch_key = UUIDv5(workflow_instance_id, patch_id)`, and flips the row
//!   to `Submitted`; a server `PatchOutcome` correlates onto the row
//!   (`Applied` / `Rejected`).
//! - `patch_key` dedup: a redelivered Patch (same `patch_id`, same key)
//!   replays the row's recorded terminal outcome and is NOT re-relayed.
//! - One Patch at a time: a Patch arriving while another for the same
//!   instance is unsettled is rejected AND recorded on its own row.
//! - Persist-at-ingress + re-drive: a failed relay send leaves a durable
//!   `Validating` row; the re-drive pass re-sends it and flips to
//!   `Submitted`.
//! - A late/duplicate outcome against an already-settled row is absorbed.

#![cfg(not(madsim))]

mod common;

use anyhow::Result;
use sqlx::PgPool;
use std::time::Duration;
use tickr_conductor::patch_pipeline::{
    correlate_outcome, patch_key, process_patch, redrive_unsettled, OutcomeCorrelation,
    ParsedPatch, PatchIngress, PatchProvenance, PatchRelaySender, PatchSource, PatchSourceFormat,
};
use tickr_proto::patch as pp;
use tokio::sync::Mutex;
use uuid::Uuid;

async fn start_pg() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

fn repository(pool: &PgPool) -> tickr_migrations::backend::WriterRepositoryBundle {
    tickr_migrations::backend::WriterRepositoryBundle::from_postgres_pool(pool.clone())
}

fn read_repository(pool: &PgPool) -> tickr_migrations::backend::ReadOnlyRepositoryBundle {
    tickr_migrations::backend::ReadOnlyRepositoryBundle::from_postgres_pool(pool.clone())
}

/// Build a published `Applied` outcome for the given instance/patch.
fn applied_outcome(wi: Uuid, key: Uuid, version: u32) -> pp::PatchOutcome {
    pp::PatchOutcome {
        workflow_instance_id: wi.to_string(),
        patch_key: key.to_string(),
        outcome: Some(pp::PatchOutcomeKind {
            kind: Some(pp::patch_outcome_kind::Kind::Applied(
                pp::patch_outcome_kind::Applied { version },
            )),
        }),
        reshaped_graph_json: None,
    }
}

/// Build a published `Rejected` outcome for the given instance/patch.
fn rejected_outcome(wi: Uuid, key: Uuid, reason: &str) -> pp::PatchOutcome {
    pp::PatchOutcome {
        workflow_instance_id: wi.to_string(),
        patch_key: key.to_string(),
        outcome: Some(pp::PatchOutcomeKind {
            kind: Some(pp::patch_outcome_kind::Kind::Rejected(
                pp::patch_outcome_kind::Rejected {
                    reason: reason.to_string(),
                },
            )),
        }),
        reshaped_graph_json: None,
    }
}

/// Recording sender: buffers every relayed envelope for assertion.
struct CapturingPatchSender {
    sent: Mutex<Vec<pp::PatchEnvelope>>,
}

impl CapturingPatchSender {
    fn new() -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
        }
    }

    async fn sent(&self) -> Vec<pp::PatchEnvelope> {
        self.sent.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl PatchRelaySender for CapturingPatchSender {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> Result<()> {
        self.sent.lock().await.push(envelope.clone());
        Ok(())
    }
}

/// Sender that always fails — the relay-down window.
struct FailingPatchSender;

#[async_trait::async_trait]
impl PatchRelaySender for FailingPatchSender {
    async fn send(&self, _envelope: &pp::PatchEnvelope) -> Result<()> {
        Err(anyhow::anyhow!("relay channel down"))
    }
}

fn sample_ops() -> Vec<pp::AddressedPatchOp> {
    let node = Uuid::new_v4();
    vec![pp::AddressedPatchOp {
        op: Some(pp::addressed_patch_op::Op::AddNode(
            pp::addressed_patch_op::AddNode {
                node_id: node.to_string(),
                task: None,
            },
        )),
    }]
}

fn parsed(ops: Vec<pp::AddressedPatchOp>) -> ParsedPatch {
    ParsedPatch {
        ops,
        operation: None,
        reason: Some("test patch".to_string()),
        stall_ttl: None,
        source: PatchSource::nickel("{ ops = [], reason = \"test patch\" }"),
    }
}

/// Happy path: ingress opens the row, relays the envelope (correct
/// `patch_key` derivation, ops and reason intact), flips `Validating →
/// Submitted`; the server's `Applied` outcome correlates onto the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingress_opens_row_relays_and_applied_outcome_correlates() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let ops = sample_ops();

    let ingress = process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        parsed(ops.clone()),
        PatchProvenance::External,
    )
    .await
    .expect("ingress");
    let key = patch_key(wi, patch_id);
    match ingress {
        PatchIngress::Accepted {
            patch_id: returned_id,
            patch_key: returned_key,
            build_jobs,
        } => {
            assert_eq!(returned_id, patch_id, "ingress returns the minted patch_id");
            assert_eq!(returned_key, key);
            assert!(build_jobs.is_empty(), "no new tasks, no build jobs");
        }
        other => panic!("expected Accepted, got {:?}", other),
    }

    // The relay envelope carries the derived key and the parsed document.
    let sent = sender.sent().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].workflow_instance_id, wi.to_string());
    assert_eq!(sent[0].patch_key, key.to_string());
    assert_eq!(sent[0].ops, ops);
    assert_eq!(sent[0].reason.as_deref(), Some("test patch"));

    // Lifecycle: relay succeeded, so the row sits at Submitted awaiting the
    // outcome (no build step — this slice's patches introduce no new tasks).
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Submitted");
    assert_eq!(row.patch_id, patch_id);
    assert_eq!(row.workflow_instance_id, wi);

    // The server's Applied outcome settles the row.
    let outcome = applied_outcome(wi, key, 1);
    assert_eq!(
        correlate_outcome(&repository(&pool), &outcome)
            .await
            .expect("correlate"),
        OutcomeCorrelation::Settled
    );
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Applied");
    assert_eq!(row.applied_version, Some(1));
    assert_eq!(row.outcome.as_deref(), Some("applied"));
}

/// The Conductor retains the submitted patch source verbatim, keyed by patch,
/// and the read path returns it exactly as submitted (no re-encoding of the
/// lowered ops). The source is joinable to the applied-patch record by
/// patch/version: after the server's `Applied` outcome settles the row, the
/// same row carries both the authored source and the resulting version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_patch_retains_verbatim_source_and_read_path_returns_it() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);

    // A representative external (Nickel) submission — the exact text an author
    // wrote, distinct from the lowered ops the pipeline derives from it.
    let authored =
        "{ ops = [ mkInsert { anchor = \"aB3d\", task = enrich } ], reason = \"add step\" }";
    let mut document = parsed(sample_ops());
    document.source = PatchSource::nickel(authored);

    process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        document,
        PatchProvenance::External,
    )
    .await
    .expect("ingress");

    // Read path: the retained source comes back verbatim, tagged Nickel.
    let source = read_repository(&pool)
        .patch_source(patch_id)
        .await
        .expect("read source")
        .expect("source retained for an accepted patch")
        .source
        .expect("source fields retained together");
    assert_eq!(source.text, authored, "retained exactly what was submitted");
    assert_eq!(source.format, PatchSourceFormat::Nickel);

    // Joinability: settle the row Applied, then confirm source and version live
    // on the same row — a reader joins authored source ↔ applied version.
    let outcome = applied_outcome(wi, key, 7);
    assert_eq!(
        correlate_outcome(&repository(&pool), &outcome)
            .await
            .expect("correlate"),
        OutcomeCorrelation::Settled
    );
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.applied_version, Some(7));
    let source = read_repository(&pool)
        .patch_source(patch_id)
        .await
        .expect("read source")
        .expect("source still retained after apply")
        .source
        .expect("source fields retained together");
    assert_eq!(source.text, authored);
}

/// An unknown patch key has no retained source (the read path returns `None`
/// rather than a placeholder).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_patch_has_no_retained_source() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let missing = Uuid::new_v4();
    assert!(read_repository(&pool)
        .patch_source(missing)
        .await
        .expect("read source")
        .is_none());
}

/// A `Rejected` outcome (concurrency loss, re-validation conflict, or a
/// non-live target) correlates onto the row with its reason recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_outcome_correlates_with_reason() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);

    process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        parsed(sample_ops()),
        PatchProvenance::External,
    )
    .await
    .expect("ingress");

    let outcome = rejected_outcome(wi, key, "instance not stalled (late apply)");
    assert_eq!(
        correlate_outcome(&repository(&pool), &outcome)
            .await
            .expect("correlate"),
        OutcomeCorrelation::Settled
    );
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Rejected");
    assert_eq!(
        row.outcome.as_deref(),
        Some("instance not stalled (late apply)")
    );
    assert_eq!(row.applied_version, None);
}

/// `patch_key` dedup: a redelivered Patch (same `patch_id` → same key) does
/// not re-apply — the terminal row replays its recorded outcome and no
/// second relay envelope ships.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redelivered_patch_replays_terminal_outcome_without_re_relay() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);
    let document = parsed(sample_ops());

    process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        document.clone(),
        PatchProvenance::External,
    )
    .await
    .expect("first ingress");
    correlate_outcome(&repository(&pool), &applied_outcome(wi, key, 1))
        .await
        .expect("settle");
    assert_eq!(sender.sent().await.len(), 1);

    // Redelivery: same patch_id computes the same patch_key.
    let replay = process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        document,
        PatchProvenance::External,
    )
    .await
    .expect("redelivered ingress");
    match replay {
        PatchIngress::Replayed { row } => {
            assert_eq!(row.patch_key, key);
            assert_eq!(row.status, "Applied", "the recorded outcome replays");
            assert_eq!(row.applied_version, Some(1));
        }
        other => panic!("expected Replayed, got {:?}", other),
    }
    assert_eq!(
        sender.sent().await.len(),
        1,
        "a replayed terminal patch must not re-relay"
    );
}

/// One Patch at a time: a Patch arriving while another for the same instance
/// is still unsettled is rejected — and still recorded on its own row. The
/// first Patch's row is untouched, and no envelope ships for the loser.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_patch_is_rejected_and_recorded_on_its_own_row() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();

    let first_id = Uuid::new_v4();
    process_patch(
        &repository(&pool),
        &sender,
        wi,
        first_id,
        parsed(sample_ops()),
        PatchProvenance::External,
    )
    .await
    .expect("first ingress");
    assert_eq!(sender.sent().await.len(), 1);

    // Second Patch for the same instance while the first is unsettled.
    let second_id = Uuid::new_v4();
    let ingress = process_patch(
        &repository(&pool),
        &sender,
        wi,
        second_id,
        parsed(sample_ops()),
        PatchProvenance::External,
    )
    .await
    .expect("second ingress");
    let second_key = patch_key(wi, second_id);
    match ingress {
        PatchIngress::RejectedInProgress {
            patch_key: key,
            reason,
            ..
        } => {
            assert_eq!(key, second_key);
            assert!(reason.contains("in progress"), "reason: {reason}");
        }
        other => panic!("expected RejectedInProgress, got {:?}", other),
    }

    // Recorded on its own row, terminally Rejected; never relayed.
    let row = common::fetch_patch_row(&pool, second_key)
        .await
        .expect("fetch")
        .expect("the rejected request still holds a row");
    assert_eq!(row.status, "Rejected");
    assert!(row.outcome.as_deref().unwrap_or("").contains("in progress"));
    assert_eq!(
        sender.sent().await.len(),
        1,
        "the rejected Patch must not relay"
    );

    // The first Patch's row is unaffected.
    let first_row = common::fetch_patch_row(&pool, patch_key(wi, first_id))
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(first_row.status, "Submitted");

    // A different instance is not blocked by this instance's in-flight Patch.
    let other_wi = Uuid::new_v4();
    let other_ingress = process_patch(
        &repository(&pool),
        &sender,
        other_wi,
        Uuid::new_v4(),
        parsed(sample_ops()),
        PatchProvenance::External,
    )
    .await
    .expect("other-instance ingress");
    assert!(matches!(other_ingress, PatchIngress::Accepted { .. }));
}

/// Persist-at-ingress + re-drive: a relay-down ingress leaves a durable
/// `Validating` row (the request is never lost after acknowledgement); the
/// re-drive pass re-sends the exact envelope and flips to `Submitted`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_failure_leaves_validating_row_and_redrive_ships_it() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);
    let ops = sample_ops();

    // Ingress with the relay down: still Accepted — the row is durable.
    let ingress = process_patch(
        &repository(&pool),
        &FailingPatchSender,
        wi,
        patch_id,
        parsed(ops.clone()),
        PatchProvenance::External,
    )
    .await
    .expect("ingress");
    assert!(matches!(ingress, PatchIngress::Accepted { .. }));
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(
        row.status, "Validating",
        "a failed relay send leaves the row for the re-drive loop"
    );

    // Re-drive with the relay back up: the persisted ops rebuild the exact
    // envelope. min_age zero so the pass picks the row up immediately.
    let sender = CapturingPatchSender::new();
    let resent = redrive_unsettled(&repository(&pool), &sender, Duration::from_secs(0))
        .await
        .expect("re-drive");
    assert_eq!(resent, 1);
    let sent = sender.sent().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].patch_key, key.to_string());
    assert_eq!(sent[0].ops, ops);
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Submitted");

    // A settled row leaves the re-drive scan.
    correlate_outcome(&repository(&pool), &applied_outcome(wi, key, 1))
        .await
        .expect("settle");
    let resent = redrive_unsettled(&repository(&pool), &sender, Duration::from_secs(0))
        .await
        .expect("re-drive after settlement");
    assert_eq!(resent, 0, "a terminal row is never re-driven");
}

/// A late or duplicate outcome against an already-settled row is absorbed —
/// the first terminal outcome wins and the audit record never flips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_outcome_against_settled_row_is_absorbed() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);

    process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        parsed(sample_ops()),
        PatchProvenance::External,
    )
    .await
    .expect("ingress");
    correlate_outcome(&repository(&pool), &applied_outcome(wi, key, 1))
        .await
        .expect("settle");

    // A contradictory late outcome (a re-drive echo racing the TTL) must not
    // rewrite the recorded terminal state.
    let late = rejected_outcome(wi, key, "late echo");
    assert_eq!(
        correlate_outcome(&repository(&pool), &late)
            .await
            .expect("correlate"),
        OutcomeCorrelation::Absorbed
    );
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Applied");
    assert_eq!(row.applied_version, Some(1));

    // An outcome for a key this conductor never ingressed is absorbed too.
    let unknown = applied_outcome(wi, Uuid::new_v4(), 2);
    assert_eq!(
        correlate_outcome(&repository(&pool), &unknown)
            .await
            .expect("correlate"),
        OutcomeCorrelation::Absorbed
    );
}

// ---- Build-at-patch (slice 06) ----------------------------------------------

use tickr_conductor::build_pipeline::BuildOutcome;
use tickr_conductor::patch_pipeline::{finalize_patch_after_build, PatchBuildFinalize};
use tickr_migrations::patch_repository::PatchLifecycleStatus;
use tickr_proto::workflow as wf;

/// A minimal proto task definition for a never-built patched-in task, minting a
/// fresh node id used as the task id (as the pipeline does).
fn patch_task(name: &str, nix: &str) -> wf::TaskDefinition {
    wf::TaskDefinition {
        id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::nil().to_string(),
        name: name.to_string(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: nix.to_string(),
        nix_args: vec![],
        outputs: vec![],
        inputs: vec![],
        secrets: vec![],
        max_attempts: 3,
        input_sources: None,
        timeout_secs: None,
        emits: vec![],
        routing_vars: vec![],
        loop_participant: false,
    }
}

fn add_node(task: wf::TaskDefinition) -> (Uuid, pp::AddressedPatchOp) {
    let id = Uuid::parse_str(&task.id).unwrap();
    (
        id,
        pp::AddressedPatchOp {
            op: Some(pp::addressed_patch_op::Op::AddNode(
                pp::addressed_patch_op::AddNode {
                    node_id: task.id.clone(),
                    task: Some(task),
                },
            )),
        },
    )
}

/// A `ParsedPatch` splicing two never-built tasks (full specs carried on the
/// `AddNode` ops) between two placeholder graph nodes. Returns the parsed
/// patch and the two minted task ids.
fn parsed_with_new_tasks() -> (ParsedPatch, Uuid, Uuid) {
    let anchor = Uuid::new_v4();
    let (id1, add1) = add_node(patch_task("enrich-1", "/patch/enrich.nix"));
    let (id2, add2) = add_node(patch_task("enrich-2", "/patch/enrich.nix"));
    let ops = vec![
        add1,
        add2,
        pp::AddressedPatchOp {
            op: Some(pp::addressed_patch_op::Op::AddEdge(
                pp::addressed_patch_op::AddEdge {
                    sources: vec![anchor.to_string()],
                    targets: vec![id1.to_string(), id2.to_string()],
                    kind: wf::EdgeKind::Control as i32,
                    gates: vec![],
                },
            )),
        },
    ];
    (
        ParsedPatch {
            ops,
            operation: None,
            reason: Some("fan-out".to_string()),
            stall_ttl: None,
            source: PatchSource::nickel("{ ops = [ mkInsert ... ], reason = \"fan-out\" }"),
        },
        id1,
        id2,
    )
}

/// A `ParsedPatch` for an `insert`: the inserted task rides `ops` as a single
/// task-bearing `AddNode` under its built node id, and the un-lowered operation
/// carries the anchor code + that same node id. Returns the parsed patch and
/// the minted node id.
fn parsed_insert() -> (ParsedPatch, Uuid) {
    let (node_id, add) = add_node(patch_task("inserted", "flake#enrich"));
    let ops = vec![add];
    let operation = Some(pp::PatchOperation {
        kind: Some(pp::patch_operation::Kind::Insert(
            pp::patch_operation::Insert {
                anchor: "AB12".to_string(),
                node_id: node_id.to_string(),
            },
        )),
    });
    (
        ParsedPatch {
            ops,
            operation,
            reason: Some("insert enrich after AB12".to_string()),
            stall_ttl: None,
            source: PatchSource::nickel("mkInsert { anchor = \"AB12\", task = ... }"),
        },
        node_id,
    )
}

async fn patch_build_row_statuses(pool: &PgPool, key: Uuid) -> Vec<(Uuid, String)> {
    sqlx::query_as::<_, (Uuid, String)>(
        "SELECT task_id, status FROM workflow_patch_task_builds
          WHERE patch_key = $1 ORDER BY task_id",
    )
    .bind(key)
    .fetch_all(pool)
    .await
    .expect("read patch build rows")
}

/// Build-then-apply happy path: ingress opens the row at `Building` with one
/// per-task build row per new task (patch-keyed) and hands the build jobs back
/// — **nothing is relayed at ingress** (no Stall for the build window). The
/// last-one-out finalizer flips `Building → Submitted` only when EVERY per-task
/// row is `success`, and the winner ships the single validate+apply envelope
/// carrying the persisted ops — apply happens only after build success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_at_patch_builds_patch_keyed_and_ships_apply_on_success() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);
    let (document, t1, t2) = parsed_with_new_tasks();

    let ingress = process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        document.clone(),
        PatchProvenance::External,
    )
    .await
    .expect("ingress");
    let build_jobs = match ingress {
        PatchIngress::Accepted { build_jobs, .. } => build_jobs,
        other => panic!("expected Accepted, got {:?}", other),
    };
    assert_eq!(build_jobs.len(), 2, "one build job per new task");
    assert!(build_jobs.iter().all(|j| j.patch_key == key));

    // Row at Building; per-task rows pending; NOTHING relayed at ingress —
    // build-then-apply arms no Stall for the build window.
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Building");
    let rows = patch_build_row_statuses(&pool, key).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, s)| s == "pending"));
    assert!(
        sender.sent().await.is_empty(),
        "nothing is relayed at ingress under build-then-apply"
    );

    // Unified spec store: patch ingress wrote one row per new task.
    for task_id in [t1, t2] {
        let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM task_specs WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .expect("count spec rows");
        assert_eq!(count, 1, "ingress writes the unified spec store");
    }

    // First build succeeds: not last-one-out — no flip, no apply envelope.
    assert_eq!(
        finalize_patch_after_build(&repository(&pool), &sender, key, t1, &BuildOutcome::Success,)
            .await
            .expect("settle t1"),
        PatchBuildFinalize::AwaitingTasks
    );
    assert_eq!(
        common::fetch_patch_row(&pool, key)
            .await
            .expect("fetch")
            .expect("row")
            .status,
        "Building",
        "apply must not ship before every task built"
    );
    assert!(sender.sent().await.is_empty());

    // Second build succeeds: last-one-out flips and ships the single
    // validate+apply envelope.
    let settlement =
        finalize_patch_after_build(&repository(&pool), &sender, key, t2, &BuildOutcome::Success)
            .await
            .expect("settle t2");
    assert!(matches!(settlement, PatchBuildFinalize::Submitted(_)));
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Submitted");
    let sent = sender.sent().await;
    assert_eq!(sent.len(), 1, "one envelope, shipped only on build success");
    assert_eq!(
        sent[0].ops, document.ops,
        "the persisted ops ride the apply"
    );
    assert_eq!(sent[0].reason.as_deref(), Some("fan-out"));
}

/// Insert end-to-end at the conductor: the un-lowered `insert` operation is
/// persisted on the row at ingress and rides the single validate+apply envelope
/// the finalizer rebuilds from that row on build success. This is how the server
/// distinguishes an `insert` from a plain task-bearing `AddNode` — the operation
/// carries the anchor + built node id it needs to lower the interpose.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_operation_persists_and_rides_apply_on_build_success() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);
    let (document, node_id) = parsed_insert();

    let ingress = process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        document.clone(),
        PatchProvenance::SelfEmitted,
    )
    .await
    .expect("ingress");
    let build_jobs = match ingress {
        PatchIngress::Accepted { build_jobs, .. } => build_jobs,
        other => panic!("expected Accepted, got {:?}", other),
    };
    // The inserted task rides the build path (build-at-patch); one build job.
    assert_eq!(build_jobs.len(), 1, "the inserted task builds at-patch");

    // Row at Building; nothing relayed at ingress (no Stall for the build
    // window). The un-lowered operation is persisted on the row (so re-drive /
    // finalize rebuild it), not just relayed.
    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "Building");
    assert_eq!(
        row.operation, document.operation,
        "the un-lowered insert operation is persisted on the row"
    );
    assert!(
        sender.sent().await.is_empty(),
        "nothing is relayed at ingress under build-then-apply"
    );

    // Build success: the last-one-out finalizer flips to Submitted and ships
    // the single validate+apply envelope rebuilt from the row — carrying the
    // un-lowered operation.
    let settlement = finalize_patch_after_build(
        &repository(&pool),
        &sender,
        key,
        node_id,
        &BuildOutcome::Success,
    )
    .await
    .expect("settle build");
    assert!(matches!(settlement, PatchBuildFinalize::Submitted(_)));
    let sent = sender.sent().await;
    assert_eq!(sent.len(), 1);
    // The insert operation survives persistence and rides the apply leg — the
    // server will lower the interpose against its live graph from this.
    assert_eq!(sent[0].operation, document.operation);
    match sent[0].operation.as_ref().and_then(|o| o.kind.as_ref()) {
        Some(pp::patch_operation::Kind::Insert(i)) => {
            assert_eq!(i.anchor, "AB12");
            assert_eq!(
                i.node_id,
                node_id.to_string(),
                "the apply wires the same built node id"
            );
        }
        other => panic!("expected an insert operation on the apply envelope, got {other:?}"),
    }
}

/// Build failure: the patch row settles `BuildFailed` (terminal, with the
/// build error recorded) **conductor-internally — no envelope is ever sent**
/// (build-then-apply arms no Stall for the build window, so there is nothing to
/// release). The built artifacts are orphaned, never dispatched, and a late
/// success finalizer is absorbed by the terminal-state guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_failure_settles_build_failed_conductor_internally() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);
    let (document, t1, t2) = parsed_with_new_tasks();

    process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        document,
        PatchProvenance::External,
    )
    .await
    .expect("ingress");

    let failure = BuildOutcome::Failure {
        error: "nix build exited 1".to_string(),
    };
    assert_eq!(
        finalize_patch_after_build(&repository(&pool), &sender, key, t1, &failure)
            .await
            .expect("settle failure"),
        PatchBuildFinalize::BuildFailed
    );

    let row = common::fetch_patch_row(&pool, key)
        .await
        .expect("fetch")
        .expect("row");
    assert_eq!(row.status, "BuildFailed");
    assert!(
        row.outcome
            .as_deref()
            .unwrap_or("")
            .contains("nix build exited 1"),
        "the build diagnostic is recorded on the row"
    );
    assert!(
        sender.sent().await.is_empty(),
        "build failure settles conductor-internally — no envelope is ever sent"
    );

    // The sibling build completing later cannot resurrect the patch: the
    // terminal-state guard absorbs the late success and no apply ships.
    assert_eq!(
        finalize_patch_after_build(&repository(&pool), &sender, key, t2, &BuildOutcome::Success,)
            .await
            .expect("late settlement"),
        PatchBuildFinalize::AlreadySettled(PatchLifecycleStatus::BuildFailed)
    );
    assert_eq!(
        common::fetch_patch_row(&pool, key)
            .await
            .expect("fetch")
            .expect("row")
            .status,
        "BuildFailed"
    );
    assert!(
        sender.sent().await.is_empty(),
        "no envelope after a build failure — nothing is ever dispatched"
    );
}

/// `Building` rows are never re-driven: builds settle through the finalizer,
/// and nothing is armed for the build window to re-send. A `Submitted` row
/// (the build already succeeded) re-drives the single validate+apply envelope
/// — the server's apply-time re-validation + redelivery dedup make the resend
/// idempotent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redrive_skips_building_rows_and_resends_apply_for_built_patches() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let key = patch_key(wi, patch_id);
    let (document, t1, t2) = parsed_with_new_tasks();

    process_patch(
        &repository(&pool),
        &sender,
        wi,
        patch_id,
        document,
        PatchProvenance::External,
    )
    .await
    .expect("ingress");
    assert!(
        sender.sent().await.is_empty(),
        "nothing relayed at ingress under build-then-apply"
    );

    // Still Building: the re-drive pass must leave it alone.
    let resent = redrive_unsettled(&repository(&pool), &sender, Duration::from_secs(0))
        .await
        .expect("re-drive");
    assert_eq!(resent, 0, "Building rows are not re-driven");
    assert!(sender.sent().await.is_empty());

    // Builds succeed but the apply send is lost (failing sender at finalize):
    // the committed row sits at Submitted for the re-drive loop.
    assert_eq!(
        finalize_patch_after_build(
            &repository(&pool),
            &FailingPatchSender,
            key,
            t1,
            &BuildOutcome::Success,
        )
        .await
        .expect("settle first build"),
        PatchBuildFinalize::AwaitingTasks
    );
    let settlement = finalize_patch_after_build(
        &repository(&pool),
        &FailingPatchSender,
        key,
        t2,
        &BuildOutcome::Success,
    )
    .await
    .expect("settle final build with relay down");
    assert!(matches!(settlement, PatchBuildFinalize::Submitted(_)));
    assert_eq!(
        common::fetch_patch_row(&pool, key)
            .await
            .expect("fetch")
            .expect("row")
            .status,
        "Submitted"
    );

    // Re-drive re-sends the built patch's single validate+apply envelope.
    let resent = redrive_unsettled(&repository(&pool), &sender, Duration::from_secs(0))
        .await
        .expect("re-drive");
    assert_eq!(resent, 1);
    let sent = sender.sent().await;
    assert_eq!(sent.last().unwrap().patch_key, key.to_string());
}

/// Self-patch dedup: a redelivered or retried completion computes the same
/// `patch_key = UUIDv5(instance, node_id)` (the node id is attempt-invariant
/// under retries), so the second ingress replays the existing row — one row,
/// one relay, one apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retried_self_patch_completion_dedups_to_one_row_and_one_relay() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingPatchSender::new();
    let wi = Uuid::new_v4();
    // Self-patch identity: patch_id IS the emitting task's definition node
    // id, exactly as the completion-drain fork passes it.
    let node_id = Uuid::new_v4();
    let key = patch_key(wi, node_id);
    let document = parsed(sample_ops());

    let first = process_patch(
        &repository(&pool),
        &sender,
        wi,
        node_id,
        document.clone(),
        PatchProvenance::External,
    )
    .await
    .expect("first ingress");
    assert!(matches!(first, PatchIngress::Accepted { .. }));
    assert_eq!(sender.sent().await.len(), 1);

    // The retried/redelivered completion forks the same document again.
    let second = process_patch(
        &repository(&pool),
        &sender,
        wi,
        node_id,
        document.clone(),
        PatchProvenance::External,
    )
    .await
    .expect("second ingress");
    match second {
        PatchIngress::Replayed { row } => assert_eq!(row.patch_key, key),
        other => panic!("expected Replayed, got {:?}", other),
    }
    assert_eq!(
        sender.sent().await.len(),
        1,
        "a replayed self-patch must not re-relay (the re-drive loop owns \
         non-terminal redelivery)"
    );
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM workflow_patches WHERE workflow_instance_id = $1")
            .bind(wi)
            .fetch_one(&pool)
            .await
            .expect("count rows");
    assert_eq!(count, 1, "one logical Patch, one row");

    // Settled Applied (the server applied once); a third redelivery replays
    // the terminal outcome and still does not re-relay.
    correlate_outcome(&repository(&pool), &applied_outcome(wi, key, 1))
        .await
        .expect("settle");
    let third = process_patch(
        &repository(&pool),
        &sender,
        wi,
        node_id,
        document,
        PatchProvenance::External,
    )
    .await
    .expect("third ingress");
    match third {
        PatchIngress::Replayed { row } => {
            assert_eq!(row.status, "Applied");
            assert_eq!(row.applied_version, Some(1));
        }
        other => panic!("expected Replayed, got {:?}", other),
    }
    assert_eq!(sender.sent().await.len(), 1, "applies exactly once");
}
