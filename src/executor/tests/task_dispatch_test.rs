//! Real-NATS integration test for the executor's durable task-dispatch
//! consumer (`task_handler::dispatch_consumer` / `drain_dispatch_to_capacity`).
//!
//! Asserts the two guarantees of the dispatch-side durability:
//!   1. **pull-to-capacity** — the executor pulls at most `cap` tasks
//!      concurrently; the remainder wait durably in the work queue until a slot
//!      frees, then are pulled and run (no available executor ≠ dropped);
//!   2. **ack-after-handoff** — a pulled message stays pending until its handler
//!      proves the complete pickup handoff.
//!
//! Requires Docker (testcontainers). Skipped automatically when the NATS
//! container is unavailable — the startup failure is the skip marker, matching
//! the conductor integration tests.

#[path = "../../../tests/support/attempt_outcome_laws.rs"]
mod attempt_outcome_laws;

use async_nats::jetstream;
use chrono::Utc;
use futures::StreamExt;
use prost::Message;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_executor::component_liveness::ensure_component_liveness_bucket;
use tickr_executor::local_pickup::{
    prepare_pickup, CancellationReconciliation, LocalAttemptOutcome, NoopPickupCheckpoint,
    PickupBoundary, PickupCheckpoint, PickupPreparation, SafeAttemptOutcomeHandoff,
    SafeCancellationFence, TerminalElection,
};
use tickr_executor::nats_pickup::{
    cancellation_acknowledgement_identity, open_pickup_bucket, NatsCancellationFence,
    NatsPickupHandoff, NatsTaskEventWriter,
};
use tickr_executor::self_reaping_key;
use tickr_executor::task_handler::{
    dispatch_consumer, drain_dispatch_to_capacity, ensure_task_cancel_ack_stream,
};
use tickr_executor::task_liveness::ensure_liveness_bucket;
use tickr_executor::wire::{
    encode_cancel_ack, encode_task_event, encode_unhealthy_task_event, CancelRequest, EmitKind,
    KillOutcome,
};
use tickr_proto::coord::{
    component_liveness_key, liveness_key, ComponentLivenessValue, TaskEventWriter,
    COMPONENT_LIVENESS_BUCKET, TASK_CANCEL_ACK_STREAM, TASK_DISPATCH_STREAM, TASK_DISPATCH_SUBJECT,
    TASK_EVENT_STREAM,
};
use tickr_proto::task as tc;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default()
        .with_tag("2.11.8-alpine")
        .with_cmd(&cmd)
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{}", port);
    let mut client = None;
    for _ in 0..20 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((container, client.expect("nats connect")))
}

/// A minimal dispatch on the published `TaskDispatch` contract — enough valid
/// identity for the executor's decode to reconstruct the execution slice. The
/// drain handler under test ignores the payload, so only decode-validity matters.
fn fresh_dispatch_item() -> tc::TaskDispatch {
    let workflow_id = Uuid::new_v4();
    tc::TaskDispatch {
        task_instance_id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        workflow_instance_id: Uuid::new_v4().to_string(),
        workflow_id: workflow_id.to_string(),
        name: "dispatch-task".to_string(),
        // The executor never reads task_type; the proto default (RegularTask) is fine.
        task_type: 0,
        nix_expression_path: "/p".to_string(),
        nix_args: vec![],
        outputs: vec![],
        inputs: vec![],
        secrets: vec![],
        tenant_id: "test".to_string(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: vec![],
    }
}

async fn publish_dispatch(nats: &async_nats::Client, item: &tc::TaskDispatch) {
    let js = jetstream::new(nats.clone());
    js.publish(TASK_DISPATCH_SUBJECT, item.encode_to_vec().into())
        .await
        .expect("publish dispatch")
        .await
        .expect("publish ack");
}

async fn queue_depth(nats: &async_nats::Client) -> u64 {
    let js = jetstream::new(nats.clone());
    let mut stream = js
        .get_stream(TASK_DISPATCH_STREAM)
        .await
        .expect("get dispatch stream");
    stream.info().await.expect("stream info").state.messages
}

/// Poll `queue_depth` until it equals `want` or the budget runs out.
async fn await_queue_depth(nats: &async_nats::Client, want: u64) -> bool {
    for _ in 0..100 {
        if queue_depth(nats).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[derive(Debug, Clone, Copy)]
enum FleetObservationCase {
    Missing,
    Stale,
    Duplicated,
    Contradictory,
}

impl FleetObservationCase {
    const ALL: [Self; 4] = [
        Self::Missing,
        Self::Stale,
        Self::Duplicated,
        Self::Contradictory,
    ];
}

async fn install_fleet_observation(case: FleetObservationCase, nats: &async_nats::Client) {
    if matches!(case, FleetObservationCase::Missing) {
        return;
    }

    let store = ensure_component_liveness_bucket(nats)
        .await
        .expect("component observation bucket");
    let key = component_liveness_key(Uuid::new_v4());
    let js = jetstream::new(nats.clone());
    let value = match case {
        FleetObservationCase::Missing => unreachable!(),
        FleetObservationCase::Stale | FleetObservationCase::Duplicated => ComponentLivenessValue {
            cap: 1,
            in_flight: 0,
        },
        FleetObservationCase::Contradictory => ComponentLivenessValue {
            cap: 0,
            in_flight: usize::MAX,
        },
    };
    let bytes = serde_json::to_vec(&value).expect("serialize fleet observation");
    let ttl = if matches!(case, FleetObservationCase::Stale) {
        Duration::from_secs(1)
    } else {
        Duration::from_secs(60)
    };
    self_reaping_key::arm(&js, COMPONENT_LIVENESS_BUCKET, &key, &bytes, ttl).await;
    if matches!(case, FleetObservationCase::Duplicated) {
        self_reaping_key::arm(&js, COMPONENT_LIVENESS_BUCKET, &key, &bytes, ttl).await;
    }

    if matches!(case, FleetObservationCase::Stale) {
        for _ in 0..40 {
            if store
                .get(&key)
                .await
                .expect("read stale observation")
                .is_none()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("stale fleet observation did not expire");
    } else {
        assert_eq!(
            store
                .get(&key)
                .await
                .expect("read fleet observation")
                .as_deref(),
            Some(bytes.as_slice()),
            "fresh fleet observation must be visible before dispatch"
        );
    }
}

#[tokio::test]
async fn all_nats_dispatch_is_unchanged_by_fleet_observation_matrix() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    let pulled = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let tracker = TaskTracker::new();
    let handler_pulled = Arc::clone(&pulled);
    let drain = tokio::spawn(drain_dispatch_to_capacity(
        consumer,
        Arc::new(Semaphore::new(1)),
        tracker.clone(),
        shutdown.clone(),
        move |message| {
            let pulled = Arc::clone(&handler_pulled);
            async move {
                message.ack().await.expect("ack dispatch");
                pulled.fetch_add(1, Ordering::SeqCst);
            }
        },
    ));

    for (index, case) in FleetObservationCase::ALL.into_iter().enumerate() {
        install_fleet_observation(case, &nats).await;
        publish_dispatch(&nats, &fresh_dispatch_item()).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while pulled.load(Ordering::SeqCst) != index + 1 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{case:?} fleet observation changed Task dispatch"));
        assert!(
            await_queue_depth(&nats, 0).await,
            "{case:?} fleet observation changed queue ownership"
        );
    }

    shutdown.cancel();
    tracker.close();
    tracker.wait().await;
    drain.await.expect("join dispatch drain");
}

#[tokio::test]
async fn pull_to_capacity_leaves_pulled_dispatches_unacknowledged() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };

    const CAP: usize = 2;
    const TOTAL: usize = 5;

    // Create the work queue + shared durable consumer, then publish more
    // dispatches than the cap.
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    for _ in 0..TOTAL {
        publish_dispatch(&nats, &fresh_dispatch_item()).await;
    }
    assert!(
        await_queue_depth(&nats, TOTAL as u64).await,
        "all dispatched tasks must be durably staged in the work queue"
    );

    // Drain pull-to-capacity with a handler that blocks (holds its slot) until
    // `release` fires — so the cap genuinely bounds concurrent in-flight pulls.
    let pulled = Arc::new(AtomicUsize::new(0));
    let release = CancellationToken::new();
    let shutdown = CancellationToken::new();
    let tracker = TaskTracker::new();

    let drain_pulled = Arc::clone(&pulled);
    let drain_release = release.clone();
    let handle = tokio::spawn(drain_dispatch_to_capacity(
        consumer,
        Arc::new(Semaphore::new(CAP)),
        tracker.clone(),
        shutdown.clone(),
        move |message| {
            let pulled = Arc::clone(&drain_pulled);
            let release = drain_release.clone();
            async move {
                pulled.fetch_add(1, Ordering::SeqCst);
                // Hold the slot and source acknowledgement until released.
                release.cancelled().await;
                message.ack().await.expect("ack after simulated handoff");
            }
        },
    ));

    // Exactly CAP tasks get pulled, but none are acknowledged while the
    // simulated handoff is blocked. The rest remain unpulled at full capacity.
    for _ in 0..100 {
        if pulled.load(Ordering::SeqCst) == CAP {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        pulled.load(Ordering::SeqCst),
        CAP,
        "pull-to-capacity must pull at most `cap` tasks while slots are held"
    );
    assert!(
        await_queue_depth(&nats, TOTAL as u64).await,
        "full capacity must leave every dispatch pending until handoff proof"
    );

    // Release the held slots: handlers now acknowledge their proved handoffs,
    // and freed capacity lets the executor pull the remainder.
    release.cancel();
    assert!(
        await_queue_depth(&nats, 0).await,
        "once slots free, the remaining dispatches are pulled and acked"
    );
    for _ in 0..100 {
        if pulled.load(Ordering::SeqCst) == TOTAL {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        pulled.load(Ordering::SeqCst),
        TOTAL,
        "every durably-staged dispatch is eventually pulled and run"
    );

    shutdown.cancel();
    tracker.close();
    tracker.wait().await;
    let _ = handle.await;
}

#[derive(Clone)]
struct FailAt(PickupBoundary);

impl PickupCheckpoint for FailAt {
    fn reached(&self, boundary: PickupBoundary) -> Result<(), String> {
        if boundary == self.0 {
            Err("simulated process crash".to_owned())
        } else {
            Ok(())
        }
    }
}

async fn pull_one(consumer: &jetstream::consumer::PullConsumer) -> jetstream::Message {
    let mut batch = consumer
        .batch()
        .max_messages(1)
        .expires(Duration::from_secs(5))
        .messages()
        .await
        .expect("open one-message pull");
    batch
        .next()
        .await
        .expect("one TaskDispatch delivery")
        .expect("valid TaskDispatch delivery")
}

async fn stream_depth(nats: &async_nats::Client, name: &str) -> u64 {
    let js = jetstream::new(nats.clone());
    let mut stream = js.get_stream(name).await.expect("get stream");
    stream.info().await.expect("stream info").state.messages
}

async fn ensure_task_event_stream(nats: &async_nats::Client) -> Result<(), String> {
    NatsTaskEventWriter::new(nats).prepare().await
}

#[tokio::test]
async fn all_nats_pickup_proves_handoff_and_fences_stale_mutations() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    let dispatch = fresh_dispatch_item();
    publish_dispatch(&nats, &dispatch).await;

    let handoff = NatsPickupHandoff::from_message(
        &nats,
        pickup,
        Some(liveness.clone()),
        pull_one(&consumer).await,
    )
    .await
    .expect("NATS pickup handoff");
    let preparation = prepare_pickup(
        &handoff,
        &NoopPickupCheckpoint,
        "executor-one",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("prepare safe pickup");
    let PickupPreparation::Ready(prepared) = preparation else {
        panic!("expected ready pickup, got {preparation:?}");
    };

    assert!(
        await_queue_depth(&nats, 0).await,
        "source acknowledgement follows complete pickup proof"
    );
    assert_eq!(
        stream_depth(&nats, TASK_EVENT_STREAM).await,
        1,
        "Assigned is staged before launch authorization"
    );
    assert_eq!(prepared.claim.pickup_generation, 1);
    assert_eq!(prepared.claim.owner, "executor-one");
    assert!(prepared.claim.liveness_deadline > Utc::now());
    let liveness_key = liveness_key(
        prepared.task.workflow_id,
        prepared.task.workflow_instance_id,
        prepared.task.task_instance_id,
    );
    assert!(
        liveness
            .get(&liveness_key)
            .await
            .expect("read liveness")
            .is_some(),
        "initial generation-qualified liveness is armed"
    );

    let started = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Started);
    assert!(
        handoff
            .stage_started(&prepared.claim, &started)
            .await
            .expect("stage Started"),
        "Started stages against the proved generation"
    );
    assert_eq!(stream_depth(&nats, TASK_EVENT_STREAM).await, 2);

    let mut stale_generation = prepared.claim.clone();
    stale_generation.pickup_generation += 1;
    assert!(!handoff
        .renew(&stale_generation, chrono::Duration::seconds(30))
        .await
        .expect("reject stale renewal"));
    let mut non_owner = prepared.claim.clone();
    non_owner.owner = "executor-two".to_owned();
    assert!(!handoff
        .stage_started(&non_owner, &started)
        .await
        .expect("reject non-owner Started"));
    let failed = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Failed);
    assert!(handoff
        .outcome_election()
        .elect_terminal(
            &stale_generation,
            LocalAttemptOutcome::ProcessExitedFailure,
            &failed,
            Utc::now(),
        )
        .await
        .is_err());
}

#[tokio::test]
async fn all_nats_poison_is_durably_rejected_before_claim_or_launch() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    let js = jetstream::new(nats.clone());
    js.publish(
        TASK_DISPATCH_SUBJECT,
        b"not-a-TaskDispatch".as_slice().into(),
    )
    .await
    .expect("publish poison")
    .await
    .expect("publish poison ack");

    let handoff = NatsPickupHandoff::from_message(
        &nats,
        pickup.clone(),
        Some(liveness),
        pull_one(&consumer).await,
    )
    .await
    .expect("NATS poison handoff");
    let outcome = prepare_pickup(
        &handoff,
        &NoopPickupCheckpoint,
        "executor-one",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("reject poison");
    let PickupPreparation::PoisonRejected { dispatch_key } = outcome else {
        panic!("expected durable poison rejection, got {outcome:?}");
    };

    assert!(await_queue_depth(&nats, 0).await);
    assert_eq!(stream_depth(&nats, TASK_EVENT_STREAM).await, 0);
    let record = pickup
        .get(&dispatch_key)
        .await
        .expect("read quarantine record")
        .expect("quarantine record exists");
    let record: serde_json::Value =
        serde_json::from_slice(&record).expect("decode quarantine record");
    assert!(record["rejected_reason"].as_str().is_some());
    assert_eq!(record["pickup_generation"], 0);
    assert_eq!(record["assigned_staged"], false);
}

#[tokio::test]
async fn ambiguous_source_ack_recovers_by_stable_identity_without_second_launch() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    publish_dispatch(&nats, &fresh_dispatch_item()).await;

    let delivery = pull_one(&consumer).await;
    let redelivery_control = delivery.clone();
    let first =
        NatsPickupHandoff::from_message(&nats, pickup.clone(), Some(liveness.clone()), delivery)
            .await
            .expect("first handoff");
    let interrupted = prepare_pickup(
        &first,
        &FailAt(PickupBoundary::AfterClaimProof),
        "executor-one",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await;
    assert!(interrupted.is_err());
    assert_eq!(
        stream_depth(&nats, TASK_EVENT_STREAM).await,
        1,
        "the interrupted operation staged exactly one Assigned event"
    );

    redelivery_control
        .ack_with(jetstream::AckKind::Nak(None))
        .await
        .expect("request redelivery after simulated crash");
    let recovered =
        NatsPickupHandoff::from_message(&nats, pickup, Some(liveness), pull_one(&consumer).await)
            .await
            .expect("recovered handoff");
    let outcome = prepare_pickup(
        &recovered,
        &NoopPickupCheckpoint,
        "executor-two",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("resolve ambiguous source acknowledgement");
    assert!(
        matches!(outcome, PickupPreparation::NoWork),
        "recovery completes the stable operation without launch authorization"
    );
    assert!(await_queue_depth(&nats, 0).await);
    assert_eq!(
        stream_depth(&nats, TASK_EVENT_STREAM).await,
        1,
        "redelivery cannot stage a second Assigned event"
    );
}

#[tokio::test]
async fn real_process_exit_wins_one_durable_outcome_election() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    let dispatch = fresh_dispatch_item();
    publish_dispatch(&nats, &dispatch).await;

    let handoff =
        NatsPickupHandoff::from_message(&nats, pickup, Some(liveness), pull_one(&consumer).await)
            .await
            .expect("NATS pickup handoff");
    let PickupPreparation::Ready(prepared) = prepare_pickup(
        &handoff,
        &NoopPickupCheckpoint,
        "executor-one",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("prepare safe pickup") else {
        panic!("expected ready pickup");
    };

    let started = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Started);
    assert!(handoff
        .stage_started(&prepared.claim, &started)
        .await
        .expect("stage Started"));
    assert!(handoff
        .renew(&prepared.claim, chrono::Duration::seconds(30))
        .await
        .expect("first generation-qualified renewal"));

    let status = Command::new("sh")
        .args(["-c", "exit 0"])
        .status()
        .await
        .expect("spawn real task process");
    assert!(status.success(), "observe the real process exit");

    let completed = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Completed);
    let outcomes = handoff.outcome_election();
    assert_eq!(
        outcomes
            .elect_terminal(
                &prepared.claim,
                LocalAttemptOutcome::ProcessExitedSuccess,
                &completed,
                Utc::now(),
            )
            .await
            .expect("commit process-exit election"),
        TerminalElection::Won
    );

    let unhealthy = encode_unhealthy_task_event(&prepared.task);
    assert_eq!(
        outcomes
            .elect_terminal(
                &prepared.claim,
                LocalAttemptOutcome::LivenessExpired,
                &unhealthy,
                Utc::now(),
            )
            .await
            .expect("read elected process-exit outcome"),
        TerminalElection::Settled(LocalAttemptOutcome::ProcessExitedSuccess),
        "late liveness cannot stage a contradictory verdict"
    );
    assert_eq!(
        stream_depth(&nats, TASK_EVENT_STREAM).await,
        2,
        "terminal bytes remain in the durable election record until Conductor staging"
    );
}

#[tokio::test]
async fn all_nats_adapter_satisfies_backend_neutral_attempt_outcome_law() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    publish_dispatch(&nats, &fresh_dispatch_item()).await;

    let handoff =
        NatsPickupHandoff::from_message(&nats, pickup, Some(liveness), pull_one(&consumer).await)
            .await
            .expect("NATS pickup handoff");
    let PickupPreparation::Ready(prepared) = prepare_pickup(
        &handoff,
        &NoopPickupCheckpoint,
        "executor-one",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("prepare safe pickup") else {
        panic!("expected ready pickup");
    };
    let started = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Started);
    assert!(handoff
        .stage_started(&prepared.claim, &started)
        .await
        .expect("stage Started"));

    let winner = attempt_outcome_laws::assert_attempt_outcome_law(
        handoff.outcome_election(),
        &prepared.claim,
    )
    .await;
    assert!(matches!(
        winner,
        LocalAttemptOutcome::ProcessExitedFailure | LocalAttemptOutcome::LivenessExpired
    ));
}

#[tokio::test]
async fn real_process_setup_failure_can_settle_before_started() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    publish_dispatch(&nats, &fresh_dispatch_item()).await;

    let handoff =
        NatsPickupHandoff::from_message(&nats, pickup, Some(liveness), pull_one(&consumer).await)
            .await
            .expect("NATS pickup handoff");
    let PickupPreparation::Ready(prepared) = prepare_pickup(
        &handoff,
        &NoopPickupCheckpoint,
        "executor-one",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("prepare safe pickup") else {
        panic!("expected ready pickup");
    };

    let spawn = Command::new("/definitely/missing/tickr-task")
        .status()
        .await;
    assert!(spawn.is_err(), "observe a real process setup failure");
    let failed = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Failed);
    assert_eq!(
        handoff
            .outcome_election()
            .elect_terminal(
                &prepared.claim,
                LocalAttemptOutcome::ProcessSetupFailed,
                &failed,
                Utc::now(),
            )
            .await
            .expect("commit setup-failure election"),
        TerminalElection::Won
    );
    assert_eq!(
        stream_depth(&nats, TASK_EVENT_STREAM).await,
        1,
        "setup failure stages terminal bytes in the election record before Started"
    );
}

fn cancel_request(dispatch: &tc::TaskDispatch) -> CancelRequest {
    CancelRequest {
        task_instance_id: dispatch.task_instance_id.parse().expect("task UUID"),
        workflow_instance_id: dispatch
            .workflow_instance_id
            .parse()
            .expect("workflow-instance UUID"),
    }
}

#[tokio::test]
async fn real_process_cancellation_restarts_every_durable_boundary_and_replays_one_ack() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    ensure_task_cancel_ack_stream(&nats)
        .await
        .expect("cancel acknowledgement stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    let dispatch = fresh_dispatch_item();
    publish_dispatch(&nats, &dispatch).await;
    let handoff = NatsPickupHandoff::from_message(
        &nats,
        pickup.clone(),
        Some(liveness),
        pull_one(&consumer).await,
    )
    .await
    .expect("NATS pickup handoff");
    let PickupPreparation::Ready(prepared) = prepare_pickup(
        &handoff,
        &NoopPickupCheckpoint,
        "executor-one",
        Uuid::new_v4(),
        chrono::Duration::seconds(30),
    )
    .await
    .expect("prepare safe pickup") else {
        panic!("expected ready pickup");
    };
    let started = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Started);
    assert!(handoff
        .stage_started(&prepared.claim, &started)
        .await
        .expect("stage Started"));

    let request = cancel_request(&dispatch);
    let identity = cancellation_acknowledgement_identity(request);
    let cancellation = NatsCancellationFence::new(&nats, pickup.clone());
    let fence = cancellation
        .commit_cancellation_fence(&identity, request, Utc::now())
        .await
        .expect("commit cancellation fence");
    assert_eq!(
        fence.dispatch_key.as_deref(),
        Some(prepared.claim.dispatch_key.as_str())
    );
    assert_eq!(
        fence.pickup_generation,
        Some(prepared.claim.pickup_generation)
    );
    assert_eq!(fence.owner.as_deref(), Some("executor-one"));

    let cancellation = NatsCancellationFence::new(&nats, pickup.clone());
    assert!(cancellation
        .mark_cancellation_owner_notified(&fence, Utc::now())
        .await
        .expect("persist owner notification"));

    let mut child = Command::new("sh");
    child
        .args(["-c", "trap 'exit 0' TERM; while :; do sleep 1; done"])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    child.process_group(0);
    let mut child = child.spawn().expect("spawn real task process group");
    let pgid = child.id().expect("task process id") as i32;
    #[cfg(unix)]
    nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(pgid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("owner-directed process-group kill");
    let _ = child.wait().await.expect("reap killed process group");

    let acknowledgement = encode_cancel_ack(
        request.task_instance_id,
        request.workflow_instance_id,
        KillOutcome::Killed,
    );
    let cancellation = NatsCancellationFence::new(&nats, pickup.clone());
    assert_eq!(
        cancellation
            .settle_cancellation(
                &fence,
                CancellationReconciliation::Killed,
                &acknowledgement,
                Utc::now(),
            )
            .await
            .expect("persist cancellation reconciliation"),
        Some(TerminalElection::Won)
    );

    let cancellation = NatsCancellationFence::new(&nats, pickup.clone());
    assert_eq!(
        cancellation
            .ensure_acknowledgement_enqueued(&identity)
            .await
            .expect("stage durable cancellation acknowledgement"),
        acknowledgement
    );
    assert_eq!(stream_depth(&nats, TASK_CANCEL_ACK_STREAM).await, 1);
    assert_eq!(
        cancellation
            .ensure_acknowledgement_enqueued(&identity)
            .await
            .expect("replay staged acknowledgement"),
        acknowledgement,
        "duplicate delivery reconstructs the same acknowledgement bytes"
    );
    assert_eq!(
        stream_depth(&nats, TASK_CANCEL_ACK_STREAM).await,
        1,
        "stable NATS message identity suppresses duplicate acknowledgement staging"
    );

    let duplicate = cancellation
        .commit_cancellation_fence(&identity, request, Utc::now())
        .await
        .expect("replay stable cancellation fence");
    assert_eq!(duplicate.dispatch_key, fence.dispatch_key);
    assert_eq!(duplicate.pickup_generation, fence.pickup_generation);
    assert_eq!(duplicate.owner, fence.owner);

    let js = jetstream::new(nats.clone());
    let stream = js
        .get_stream(TASK_CANCEL_ACK_STREAM)
        .await
        .expect("get cancel acknowledgement stream");
    let forwarder = stream
        .get_or_create_consumer(
            "test-cancel-ack-forward",
            jetstream::consumer::pull::Config {
                durable_name: Some("test-cancel-ack-forward".to_owned()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .expect("create acknowledgement forwarder");
    let mut forwarded = forwarder
        .batch()
        .max_messages(1)
        .expires(Duration::from_secs(5))
        .messages()
        .await
        .expect("open acknowledgement forwarding batch");
    let forwarded = forwarded
        .next()
        .await
        .expect("one staged acknowledgement")
        .expect("valid staged acknowledgement");
    assert_eq!(forwarded.payload.as_ref(), acknowledgement.as_slice());
    forwarded
        .ack()
        .await
        .expect("acknowledge only after relay forwarding");
}

#[tokio::test]
async fn queued_cancellation_and_owner_death_converge_without_launch_or_second_terminal() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };
    let consumer = dispatch_consumer(&nats).await.expect("dispatch consumer");
    ensure_task_event_stream(&nats)
        .await
        .expect("task event stream");
    ensure_task_cancel_ack_stream(&nats)
        .await
        .expect("cancel acknowledgement stream");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");

    let queued = fresh_dispatch_item();
    let queued_request = cancel_request(&queued);
    let queued_identity = cancellation_acknowledgement_identity(queued_request);
    let cancellation = NatsCancellationFence::new(&nats, pickup.clone());
    let queued_fence = cancellation
        .commit_cancellation_fence(&queued_identity, queued_request, Utc::now())
        .await
        .expect("commit queued cancellation");
    assert!(queued_fence.owner.is_none());
    let queued_ack = encode_cancel_ack(
        queued_request.task_instance_id,
        queued_request.workflow_instance_id,
        KillOutcome::NoSuchTask,
    );
    assert_eq!(
        cancellation
            .settle_cancellation(
                &queued_fence,
                CancellationReconciliation::NoProcess,
                &queued_ack,
                Utc::now(),
            )
            .await
            .expect("settle queued cancellation"),
        None
    );
    cancellation
        .ensure_acknowledgement_enqueued(&queued_identity)
        .await
        .expect("stage queued acknowledgement");
    publish_dispatch(&nats, &queued).await;
    let queued_handoff = NatsPickupHandoff::from_message(
        &nats,
        pickup.clone(),
        Some(liveness.clone()),
        pull_one(&consumer).await,
    )
    .await
    .expect("queued cancellation handoff");
    assert!(matches!(
        prepare_pickup(
            &queued_handoff,
            &NoopPickupCheckpoint,
            "executor-one",
            Uuid::new_v4(),
            chrono::Duration::seconds(30),
        )
        .await
        .expect("reconcile queued cancellation"),
        PickupPreparation::NoWork
    ));
    assert!(await_queue_depth(&nats, 0).await);
    let rebound = cancellation
        .load_cancellation(&queued_identity)
        .await
        .expect("load rebound queued cancellation")
        .expect("queued fence exists");
    assert!(rebound.dispatch_key.is_some());
    assert_eq!(rebound.pickup_generation, Some(1));

    let active = fresh_dispatch_item();
    publish_dispatch(&nats, &active).await;
    let active_handoff = NatsPickupHandoff::from_message(
        &nats,
        pickup.clone(),
        Some(liveness),
        pull_one(&consumer).await,
    )
    .await
    .expect("active pickup handoff");
    let PickupPreparation::Ready(active_prepared) = prepare_pickup(
        &active_handoff,
        &NoopPickupCheckpoint,
        "executor-that-died",
        Uuid::new_v4(),
        chrono::Duration::milliseconds(10),
    )
    .await
    .expect("prepare owner-death pickup") else {
        panic!("expected active pickup");
    };
    let active_request = cancel_request(&active);
    let active_identity = cancellation_acknowledgement_identity(active_request);
    let cancellation = NatsCancellationFence::new(&nats, pickup.clone());
    let active_fence = cancellation
        .commit_cancellation_fence(&active_identity, active_request, Utc::now())
        .await
        .expect("commit active cancellation before owner death");
    assert_eq!(active_fence.owner.as_deref(), Some("executor-that-died"));
    drop(cancellation);

    let outcomes = active_handoff.outcome_election();
    assert!(outcomes
        .register_liveness_failure(&active_prepared.claim, Utc::now())
        .await
        .expect("persist owner-death liveness evidence"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        outcomes
            .sweep_one_due()
            .await
            .expect("reconcile dead owner"),
        Some((_, TerminalElection::Won))
    ));

    let cancellation = NatsCancellationFence::new(&nats, pickup);
    let recovered = cancellation
        .load_cancellation(&active_identity)
        .await
        .expect("reconstruct owner-death fence")
        .expect("owner-death fence exists");
    assert_eq!(
        recovered.terminal_outcome,
        Some(LocalAttemptOutcome::LivenessExpired)
    );
    let active_ack = encode_cancel_ack(
        active_request.task_instance_id,
        active_request.workflow_instance_id,
        KillOutcome::NoSuchTask,
    );
    assert_eq!(
        cancellation
            .settle_cancellation(
                &recovered,
                CancellationReconciliation::AlreadyExited,
                &active_ack,
                Utc::now(),
            )
            .await
            .expect("settle owner-death cancellation"),
        Some(TerminalElection::Settled(
            LocalAttemptOutcome::LivenessExpired
        ))
    );
    cancellation
        .ensure_acknowledgement_enqueued(&active_identity)
        .await
        .expect("stage owner-death acknowledgement");
}
