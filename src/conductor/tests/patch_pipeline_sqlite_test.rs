#![cfg(not(madsim))]

use anyhow::Result;
use chrono::Utc;
use sqlx::sqlite::SqlitePoolOptions;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tickr_conductor::build_pipeline::{BuildExecutor, BuildOutcome, TaskBuildJob};
use tickr_conductor::patch_pipeline::local::{
    patch_work_notifications, start_local_patch_worker, LocalPatchWorkerConfig,
};
use tickr_conductor::patch_pipeline::{
    correlate_outcome, patch_key, process_patch, redrive_unsettled, OutcomeCorrelation,
    ParsedPatch, PatchIngress, PatchProvenance, PatchRelaySender, PatchSource,
};
use tickr_migrations::backend::RepositoryFactory;
use tickr_migrations::patch_repository::{
    LeasedPatchBuildSettlementOutcome, PatchBuildLeaseRequest, PatchBuildSettlementOutcome,
    PatchTaskBuildResult,
};
use tickr_proto::config::DataPlaneSql;
use tickr_proto::{patch as pp, workflow as wf};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct FailingSender;

#[async_trait::async_trait]
impl PatchRelaySender for FailingSender {
    async fn send(&self, _envelope: &pp::PatchEnvelope) -> Result<()> {
        anyhow::bail!("relay unavailable")
    }
}

#[derive(Default)]
struct CountingSender(Mutex<Vec<pp::PatchEnvelope>>);

#[async_trait::async_trait]
impl PatchRelaySender for CountingSender {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> Result<()> {
        self.0.lock().await.push(envelope.clone());
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_failure_and_redelivery_preserve_one_sqlite_ingress_row() {
    let directory = TempDir::new().unwrap();
    let url = format!("sqlite://{}", directory.path().join("patches.db").display());
    let migration_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    tickr_migrations::apply_sqlite(
        tickr_migrations::MigrationTarget::Conductor,
        &migration_pool,
    )
    .await
    .unwrap();
    migration_pool.close().await;

    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() });
    let writer = factory.open_writer().await.unwrap();
    let workflow_instance_id = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let source = r#"{"ops":[{"RemoveNode":{"node_id":"aB3d"}}]}"#;
    let parsed = ParsedPatch {
        ops: Vec::new(),
        operation: None,
        reason: Some("relay failure law".to_owned()),
        stall_ttl: None,
        source: PatchSource::json(source),
    };

    assert!(matches!(
        process_patch(
            &writer,
            &FailingSender,
            workflow_instance_id,
            patch_id,
            parsed.clone(),
            PatchProvenance::SelfEmitted,
        )
        .await
        .unwrap(),
        PatchIngress::Accepted { .. }
    ));
    writer.close().await;

    let reader = factory.open_read_only().await.unwrap();
    let status = reader.patch_status(patch_id).await.unwrap().unwrap();
    assert_eq!(status.status.as_str(), "Validating");
    let retained = reader
        .patch_source(patch_id)
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    assert_eq!(retained.text, source);
    reader.close().await;

    let reopened_writer = factory.open_writer().await.unwrap();
    let sender = CountingSender::default();
    match process_patch(
        &reopened_writer,
        &sender,
        workflow_instance_id,
        patch_id,
        parsed,
        PatchProvenance::SelfEmitted,
    )
    .await
    .unwrap()
    {
        PatchIngress::Replayed { row } => {
            assert_eq!(row.patch_key, patch_key(workflow_instance_id, patch_id));
            assert_eq!(row.status, "Validating");
        }
        other => panic!("redelivery did not replay the durable row: {other:?}"),
    }
    assert!(sender.0.lock().await.is_empty());

    assert_eq!(
        redrive_unsettled(&reopened_writer, &sender, Duration::ZERO)
            .await
            .unwrap(),
        1
    );
    let sent = sender.0.lock().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].patch_key,
        patch_key(workflow_instance_id, patch_id).to_string()
    );
    assert_eq!(
        sent[0].workflow_instance_id,
        workflow_instance_id.to_string()
    );
    drop(sent);

    let outcome = pp::PatchOutcome {
        workflow_instance_id: workflow_instance_id.to_string(),
        patch_key: patch_key(workflow_instance_id, patch_id).to_string(),
        outcome: Some(pp::PatchOutcomeKind {
            kind: Some(pp::patch_outcome_kind::Kind::Applied(
                pp::patch_outcome_kind::Applied { version: 1 },
            )),
        }),
        reshaped_graph_json: None,
    };
    assert_eq!(
        correlate_outcome(&reopened_writer, &outcome).await.unwrap(),
        OutcomeCorrelation::Settled
    );
    let correlated_reader = factory.open_read_only().await.unwrap();
    assert_eq!(
        correlated_reader
            .patch_status(patch_id)
            .await
            .unwrap()
            .unwrap()
            .status
            .as_str(),
        "Applied"
    );
    correlated_reader.close().await;
    assert_eq!(
        redrive_unsettled(&reopened_writer, &sender, Duration::ZERO)
            .await
            .unwrap(),
        0,
        "a terminal Patch is never reopened or driven again"
    );
    assert_eq!(sender.0.lock().await.len(), 1);
    reopened_writer.close().await;
}

#[derive(Default)]
struct CountingBuildExecutor(Mutex<Vec<Uuid>>);

#[async_trait::async_trait]
impl BuildExecutor for CountingBuildExecutor {
    async fn build(&self, job: &TaskBuildJob) -> BuildOutcome {
        self.0.lock().await.push(job.task_id);
        BuildOutcome::Success
    }
}

fn patch_task(name: &str) -> wf::TaskDefinition {
    wf::TaskDefinition {
        id: Uuid::new_v4().to_string(),
        workflow_id: Uuid::nil().to_string(),
        name: name.to_owned(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: format!("flake#{name}"),
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

fn parsed_with_new_tasks() -> (ParsedPatch, Vec<Uuid>) {
    let tasks = [patch_task("first"), patch_task("second")];
    let ids = tasks
        .iter()
        .map(|task| Uuid::parse_str(&task.id).unwrap())
        .collect();
    let ops = tasks
        .into_iter()
        .map(|task| pp::AddressedPatchOp {
            op: Some(pp::addressed_patch_op::Op::AddNode(
                pp::addressed_patch_op::AddNode {
                    node_id: task.id.clone(),
                    task: Some(task),
                },
            )),
        })
        .collect();
    (
        ParsedPatch {
            ops,
            operation: None,
            reason: Some("recover Patch builds".to_owned()),
            stall_ttl: None,
            source: PatchSource::nickel("{ ops = [ first second ] }"),
        },
        ids,
    )
}

fn parsed_without_tasks(reason: &str) -> ParsedPatch {
    ParsedPatch {
        ops: Vec::new(),
        operation: None,
        reason: Some(reason.to_owned()),
        stall_ttl: None,
        source: PatchSource::json("{}"),
    }
}

async fn sqlite_factory(directory: &TempDir) -> RepositoryFactory {
    let url = format!("sqlite://{}", directory.path().join("patches.db").display());
    let migration_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    tickr_migrations::apply_sqlite(
        tickr_migrations::MigrationTarget::Conductor,
        &migration_pool,
    )
    .await
    .unwrap();
    migration_pool.close().await;
    RepositoryFactory::new(DataPlaneSql::Sqlite { url })
}

async fn wait_for_sends(sender: &CountingSender, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if sender.0.lock().await.len() >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Patch effect was not recovered");
}

fn applied_outcome(workflow_instance_id: Uuid, patch_id: Uuid) -> pp::PatchOutcome {
    pp::PatchOutcome {
        workflow_instance_id: workflow_instance_id.to_string(),
        patch_key: patch_key(workflow_instance_id, patch_id).to_string(),
        outcome: Some(pp::PatchOutcomeKind {
            kind: Some(pp::patch_outcome_kind::Kind::Applied(
                pp::patch_outcome_kind::Applied { version: 1 },
            )),
        }),
        reshaped_graph_json: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_recovers_committed_builds_through_one_finalizer() {
    let directory = TempDir::new().unwrap();
    let factory = sqlite_factory(&directory).await;
    let writer = factory.open_writer().await.unwrap();
    let workflow_instance_id = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let (parsed, task_ids) = parsed_with_new_tasks();
    assert!(matches!(
        process_patch(
            &writer,
            &FailingSender,
            workflow_instance_id,
            patch_id,
            parsed,
            PatchProvenance::External,
        )
        .await
        .unwrap(),
        PatchIngress::Accepted { .. }
    ));
    writer.close().await;

    let reopened = Arc::new(factory.open_writer().await.unwrap());
    let executor = Arc::new(CountingBuildExecutor::default());
    let sender = Arc::new(CountingSender::default());
    let (notifier, notifications) = patch_work_notifications(NonZeroUsize::new(1).unwrap());
    drop(notifier);
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_patch_worker(
        Arc::clone(&reopened),
        executor.clone(),
        sender.clone(),
        "startup-worker".to_owned(),
        notifications,
        LocalPatchWorkerConfig {
            scan_interval: Duration::from_secs(3600),
            build_lease_duration: Duration::from_secs(30),
            lifecycle_lease_duration: Duration::from_secs(30),
            lifecycle_min_age: Duration::from_secs(3600),
            batch_size: NonZeroUsize::new(8).unwrap(),
        },
        cancel.clone(),
    ));
    wait_for_sends(&sender, 1).await;
    cancel.cancel();
    worker.await.unwrap().unwrap();

    let mut built = executor.0.lock().await.clone();
    built.sort_unstable();
    let mut expected = task_ids;
    expected.sort_unstable();
    assert_eq!(built, expected, "each committed Patch task builds once");
    assert_eq!(
        sender.0.lock().await.len(),
        1,
        "only the winning last-one-out finalizer emits the apply intent"
    );
    let reader = factory.open_read_only().await.unwrap();
    assert_eq!(
        reader
            .patch_status(patch_id)
            .await
            .unwrap()
            .unwrap()
            .status
            .as_str(),
        "Submitted"
    );
    reader.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_claims_recover_without_a_second_finalizer_winner() {
    let directory = TempDir::new().unwrap();
    let factory = sqlite_factory(&directory).await;
    let writer = factory.open_writer().await.unwrap();
    let workflow_instance_id = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let (parsed, _) = parsed_with_new_tasks();
    process_patch(
        &writer,
        &FailingSender,
        workflow_instance_id,
        patch_id,
        parsed,
        PatchProvenance::External,
    )
    .await
    .unwrap();

    let claimed_at = Utc::now();
    let first_claims = writer
        .lease_patch_build_tasks(PatchBuildLeaseRequest {
            owner: "crashed-worker",
            now: claimed_at,
            expires_at: claimed_at + chrono::Duration::milliseconds(20),
            limit: 8,
        })
        .await
        .unwrap();
    let first_order = first_claims
        .iter()
        .map(|lease| lease.task.task_id)
        .collect::<Vec<_>>();
    writer.close().await;

    let reopened = factory.open_writer().await.unwrap();
    assert!(
        reopened
            .lease_patch_build_tasks(PatchBuildLeaseRequest {
                owner: "early-restart",
                now: claimed_at + chrono::Duration::milliseconds(10),
                expires_at: claimed_at + chrono::Duration::seconds(1),
                limit: 8,
            })
            .await
            .unwrap()
            .is_empty(),
        "an unexpired claim survives restart"
    );
    let recovered = reopened
        .lease_patch_build_tasks(PatchBuildLeaseRequest {
            owner: "recovery-worker",
            now: claimed_at + chrono::Duration::milliseconds(21),
            expires_at: claimed_at + chrono::Duration::seconds(1),
            limit: 8,
        })
        .await
        .unwrap();
    assert_eq!(recovered.len(), 2);
    assert_eq!(
        recovered
            .iter()
            .map(|lease| lease.task.task_id)
            .collect::<Vec<_>>(),
        first_order,
        "lease expiry preserves the committed stable selection order"
    );
    assert!(matches!(
        reopened
            .settle_leased_patch_task_build(
                &first_claims[0],
                PatchTaskBuildResult::Success,
                claimed_at + chrono::Duration::milliseconds(21),
            )
            .await
            .unwrap(),
        LeasedPatchBuildSettlementOutcome::LeaseLost
    ));

    let (left, right) = tokio::join!(
        reopened.settle_leased_patch_task_build(
            &recovered[0],
            PatchTaskBuildResult::Success,
            claimed_at + chrono::Duration::milliseconds(22),
        ),
        reopened.settle_leased_patch_task_build(
            &recovered[1],
            PatchTaskBuildResult::Success,
            claimed_at + chrono::Duration::milliseconds(22),
        ),
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                LeasedPatchBuildSettlementOutcome::Settled(PatchBuildSettlementOutcome::Submitted(
                    _
                ))
            ))
            .count(),
        1,
        "competing settlements produce one winning parent finalizer"
    );
    assert!(outcomes.iter().any(|outcome| matches!(
        outcome,
        LeasedPatchBuildSettlementOutcome::Settled(PatchBuildSettlementOutcome::AwaitingTasks)
    )));
    reopened.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_and_closed_notifications_cannot_strand_patch_lifecycle() {
    let directory = TempDir::new().unwrap();
    let factory = sqlite_factory(&directory).await;
    let writer = Arc::new(factory.open_writer().await.unwrap());
    let sender = Arc::new(CountingSender::default());
    let (notifier, notifications) = patch_work_notifications(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(start_local_patch_worker(
        Arc::clone(&writer),
        Arc::new(CountingBuildExecutor::default()),
        sender.clone(),
        "steady-worker".to_owned(),
        notifications,
        LocalPatchWorkerConfig {
            scan_interval: Duration::from_millis(25),
            build_lease_duration: Duration::from_secs(1),
            lifecycle_lease_duration: Duration::from_secs(1),
            lifecycle_min_age: Duration::ZERO,
            batch_size: NonZeroUsize::new(8).unwrap(),
        },
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(30)).await;

    let first_instance = Uuid::new_v4();
    let first_patch = Uuid::new_v4();
    process_patch(
        writer.as_ref(),
        &FailingSender,
        first_instance,
        first_patch,
        parsed_without_tasks("full notification channel"),
        PatchProvenance::External,
    )
    .await
    .unwrap();
    notifier.notify();
    notifier.notify();
    wait_for_sends(&sender, 1).await;
    assert_eq!(
        correlate_outcome(
            writer.as_ref(),
            &applied_outcome(first_instance, first_patch)
        )
        .await
        .unwrap(),
        OutcomeCorrelation::Settled
    );

    drop(notifier);
    let second_instance = Uuid::new_v4();
    let second_patch = Uuid::new_v4();
    process_patch(
        writer.as_ref(),
        &FailingSender,
        second_instance,
        second_patch,
        parsed_without_tasks("closed notification channel"),
        PatchProvenance::External,
    )
    .await
    .unwrap();
    wait_for_sends(&sender, 2).await;
    cancel.cancel();
    worker.await.unwrap().unwrap();

    let sent = sender.0.lock().await;
    assert_eq!(sent.len(), 2);
    assert_eq!(
        sent.iter()
            .map(|envelope| envelope.patch_key.clone())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        2,
        "duplicate hints cannot duplicate a Patch identity"
    );
}
