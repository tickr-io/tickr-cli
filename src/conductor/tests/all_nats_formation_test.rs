#![cfg(not(madsim))]

use async_nats::jetstream::{self, kv, stream};
use chrono::Utc;
use flate2::read::GzDecoder;
use futures::StreamExt;
use opendal::{services::Memory, Operator};
use prost::Message;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::future::Future;
use std::io::Read;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tickr_conductor::build_pipeline::{
    definition_build_notifications, start_local_definition_build_worker,
    LocalDefinitionBuildWorkerConfig, TestBuildExecutor,
};
use tickr_conductor::patch_pipeline::{
    process_patch, ParsedPatch, PatchIngress, PatchProvenance, PatchRelaySender, PatchSource,
};
use tickr_conductor::proto::ConductorRelayMessage;
use tickr_conductor::register_pipeline::{process_register, RegisterOutcome, RegisterRequest};
use tickr_conductor::relay::{
    drain_attempt_outcomes, drain_task_events, init_relay_tx, task_event_consumer,
};
use tickr_conductor::submission_consumer::{
    definition_submission_notifications, start_local_definition_submission_worker,
    LocalDefinitionSubmissionWorkerConfig,
};
use tickr_conductor::system_tasks::compaction_drain;
use tickr_conductor::system_tasks::{run_compaction_drain, stage_compaction_payload};
use tickr_executor::local_pickup::{
    prepare_pickup, LocalAttemptOutcome, NoopPickupCheckpoint, PickupBoundary, PickupCheckpoint,
    PickupPreparation, SafeAttemptOutcomeHandoff, TerminalElection,
};
use tickr_executor::nats_pickup::{open_pickup_bucket, NatsOutcomeElection, NatsPickupHandoff};
use tickr_executor::task_handler::dispatch_consumer;
use tickr_executor::task_liveness::ensure_liveness_bucket;
use tickr_executor::wire::{encode_task_event, EmitKind};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::{apply_target, MigrationTarget};
use tickr_proto::archive::{ArchiveProjection, CompactionEnvelope};
use tickr_proto::coord::all_nats as names;
use tickr_proto::instance::SnapshotTaskInstance;
use tickr_proto::patch as pp;
use tickr_proto::task as tc;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct FreshAllNats {
    _container: ContainerAsync<Nats>,
    url: String,
    client: async_nats::Client,
}

impl FreshAllNats {
    async fn start() -> Option<Self> {
        let command = NatsServerCmd::default().with_jetstream();
        let container = match Nats::default()
            .with_tag("2.11.11")
            .with_cmd(&command)
            .start()
            .await
        {
            Ok(container) => container,
            Err(error) => {
                eprintln!("skipping: isolated NATS testcontainer unavailable: {error}");
                return None;
            }
        };
        let port = container.get_host_port_ipv4(4222).await.ok()?;
        let url = format!("nats://127.0.0.1:{port}");
        for _ in 0..20 {
            if let Ok(client) = async_nats::connect(&url).await {
                return Some(Self {
                    _container: container,
                    url,
                    client,
                });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("isolated NATS started but did not accept connections");
    }

    async fn provisioned() -> Option<Self> {
        let fixture = Self::start().await?;
        tickr_conductor::all_nats_formation::admit_and_provision(&fixture.client)
            .await
            .expect("admit fresh hardened all-NATS resources");
        Some(fixture)
    }
}

#[tokio::test]
async fn disposable_fixture_provisions_only_the_hardened_resource_set() {
    let Some(fixture) = FreshAllNats::provisioned().await else {
        return;
    };
    let _lifecycle_work = tickr_conductor::lifecycle_work::all_nats_lifecycle_work(&fixture.client)
        .await
        .expect("construct transient all-NATS LifecycleWork subscriptions");
    let js = jetstream::new(fixture.client.clone());

    let mut actual_streams = BTreeSet::new();
    let mut stream_names = js.stream_names();
    while let Some(name) = stream_names.next().await {
        actual_streams.insert(name.expect("stream name"));
    }
    let mut expected_streams: BTreeSet<String> =
        names::STREAM_NAMES.into_iter().map(str::to_owned).collect();
    expected_streams.extend(
        names::KV_BUCKET_NAMES
            .into_iter()
            .map(|bucket| format!("KV_{bucket}")),
    );
    assert_eq!(actual_streams, expected_streams);

    let identity_store = js
        .get_key_value(names::FORMATION_IDENTITY_BUCKET)
        .await
        .expect("identity bucket");
    let identity = identity_store
        .get(names::FORMATION_IDENTITY_KEY)
        .await
        .expect("identity read")
        .expect("identity value");
    assert_eq!(
        identity.as_ref(),
        format!(
            "{};scope={}",
            names::FORMATION_IDENTITY,
            names::DEFAULT_SCOPE_BUCKET
        )
        .as_bytes()
    );

    let consumer_streams = [
        names::TASK_DISPATCH_STREAM,
        names::TASK_EVENT_STREAM,
        names::TASK_CANCEL_STREAM,
        names::TASK_CANCEL_ACK_STREAM,
        names::COMPACTION_STREAM,
        &format!("KV_{}", names::LIVENESS_BUCKET),
        names::EVENT_INGRESS_STREAM,
    ];
    let mut actual_consumers = BTreeSet::new();
    for stream_name in consumer_streams {
        let stream = js.get_stream(stream_name).await.expect("consumer stream");
        let mut consumer_names = stream.consumer_names();
        while let Some(name) = consumer_names.next().await {
            actual_consumers.insert(name.expect("consumer name"));
        }
    }
    assert_eq!(
        actual_consumers,
        names::CONSUMER_NAMES
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
}

#[tokio::test]
async fn nonempty_fresh_state_without_identity_fails_closed() {
    let Some(fixture) = FreshAllNats::start().await else {
        return;
    };
    let js = jetstream::new(fixture.client.clone());
    js.create_stream(stream::Config {
        name: names::TASK_DISPATCH_STREAM.to_owned(),
        subjects: vec![names::TASK_DISPATCH_SUBJECT.to_owned()],
        retention: stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .expect("create fresh stream without identity");
    js.publish(names::TASK_DISPATCH_SUBJECT, "accepted-state".into())
        .await
        .expect("publish fresh state")
        .await
        .expect("persist fresh state");

    let error = tickr_conductor::all_nats_formation::admit_and_provision(&fixture.client)
        .await
        .expect_err("nonempty state without identity must fail");
    assert!(error
        .to_string()
        .contains("state is nonempty but formation identity is missing"));
}

#[tokio::test]
async fn mismatched_fresh_identity_fails_without_provisioning_runtime_resources() {
    let Some(fixture) = FreshAllNats::start().await else {
        return;
    };
    let js = jetstream::new(fixture.client.clone());
    let identity = js
        .create_key_value(kv::Config {
            bucket: names::FORMATION_IDENTITY_BUCKET.to_owned(),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("create identity bucket");
    identity
        .create(
            names::FORMATION_IDENTITY_KEY,
            "other-protocol-set/v9".into(),
        )
        .await
        .expect("write mismatched identity");

    let error = tickr_conductor::all_nats_formation::admit_and_provision(&fixture.client)
        .await
        .expect_err("mismatched identity must fail");
    assert!(error.to_string().contains("identity does not match"));

    for stream_name in names::STREAM_NAMES {
        assert!(js.get_stream(stream_name).await.is_err());
    }
}

#[tokio::test]
async fn mismatched_fresh_resource_configuration_fails_closed() {
    let Some(fixture) = FreshAllNats::start().await else {
        return;
    };
    let js = jetstream::new(fixture.client.clone());
    let identity = js
        .create_key_value(kv::Config {
            bucket: names::FORMATION_IDENTITY_BUCKET.to_owned(),
            history: 1,
            ..Default::default()
        })
        .await
        .expect("create identity bucket");
    identity
        .create(
            names::FORMATION_IDENTITY_KEY,
            format!(
                "{};scope={}",
                names::FORMATION_IDENTITY,
                names::DEFAULT_SCOPE_BUCKET
            )
            .into(),
        )
        .await
        .expect("write admitted identity");
    js.create_stream(stream::Config {
        name: names::TASK_DISPATCH_STREAM.to_owned(),
        subjects: vec!["tickr.all_nats.v2.wrong".to_owned()],
        retention: stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .expect("create mismatched fresh stream");

    let error = tickr_conductor::all_nats_formation::admit_and_provision(&fixture.client)
        .await
        .expect_err("mismatched resource configuration must fail");
    assert!(error.to_string().contains("mismatched configuration"));
}

const PARITY_SCOPE_ENVELOPE: &[u8] = br#"{ "v": 2, "type": "string", "value": "parity", "secret": false, "producer": { "kind": "task", "task_id": "parity", "task_name": "parity-task" }, "created_at": "2026-07-23T00:00:00Z", "sha256": "parity-scope" }"#;
const NORMAL_LOG: &[u8] = b"normal output\n";
const CRASH_LOG_FIRST: &[u8] = b"accepted before crash\n";
const CRASH_LOG_SECOND: &[u8] = b"accepted after crash\n";

fn workflow_source() -> String {
    r#"let utils = import "lib.ncl" in
utils.mkWorkflow {
  slug = "fresh-all-nats-parity",
  name = "fresh-all-NATS-parity",
  args = [],
  outputs = [],
  tasks = [ utils.mkTaskGroup {
    name = "parity",
    args = [],
    outputs = [],
    tasks = [ utils.mkTask {
      name = "parity-task",
      args = [],
      nix_expression_path = "parity-expression",
      outputs = [],
    } ],
  } ],
}"#
    .to_owned()
}

async fn start_postgres() -> (ContainerAsync<Postgres>, String, PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start isolated Postgres");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect isolated Postgres");
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .expect("migrate isolated Postgres");
    (container, url, pool)
}

async fn connect_nats(url: &str) -> async_nats::Client {
    for _ in 0..50 {
        if let Ok(client) = async_nats::connect(url).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("helper could not connect to isolated NATS");
}

fn spawn_helper(test_name: &str, env: &[(&str, String)]) -> Child {
    let mut command = Command::new(std::env::current_exe().expect("current test executable"));
    command
        .arg("--ignored")
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (name, value) in env {
        command.env(name, value);
    }
    command.spawn().expect("spawn real-process helper")
}

async fn wait_for_path(path: &Path) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("process boundary marker missing: {}", path.display()));
}

async fn wait_for_workflow_status(pool: &PgPool, workflow_id: Uuid, status: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let actual: String =
                sqlx::query_scalar("SELECT status FROM workflows WHERE id = $1 AND version = 1")
                    .bind(workflow_id)
                    .fetch_one(pool)
                    .await
                    .expect("read Workflow status");
            if actual == status {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Workflow did not reach {status}"));
}

async fn publish_dispatch(nats: &async_nats::Client, dispatch: &tc::TaskDispatch) {
    jetstream::new(nats.clone())
        .publish(
            names::TASK_DISPATCH_SUBJECT,
            dispatch.encode_to_vec().into(),
        )
        .await
        .expect("publish TaskDispatch")
        .await
        .expect("persist TaskDispatch");
}

async fn pull_one(consumer: &jetstream::consumer::PullConsumer) -> async_nats::jetstream::Message {
    let mut messages = consumer
        .batch()
        .max_messages(1)
        .expires(Duration::from_secs(45))
        .messages()
        .await
        .expect("open TaskDispatch pull");
    messages
        .next()
        .await
        .expect("TaskDispatch delivery")
        .expect("valid TaskDispatch delivery")
}

#[derive(Clone)]
struct BlockAfterClaimProof {
    marker: PathBuf,
}

impl PickupCheckpoint for BlockAfterClaimProof {
    fn reached(&self, boundary: PickupBoundary) -> Result<(), String> {
        if boundary == PickupBoundary::AfterClaimProof {
            std::fs::write(&self.marker, b"claim-proved").map_err(|error| error.to_string())?;
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        Ok(())
    }
}

fn task_dispatch(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_id: Uuid,
    task_instance_id: Uuid,
) -> tc::TaskDispatch {
    tc::TaskDispatch {
        task_instance_id: task_instance_id.to_string(),
        task_id: task_id.to_string(),
        workflow_instance_id: workflow_instance_id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: "parity-task".to_owned(),
        task_type: 0,
        nix_expression_path: "parity-expression".to_owned(),
        nix_args: vec![],
        outputs: vec![],
        inputs: vec![],
        secrets: vec![],
        tenant_id: "parity".to_owned(),
        originating_signal_id: None,
        gate_signal_ids: Default::default(),
        gate_signal_ids_ambient: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawned by the fresh all-NATS parity scenario"]
async fn executor_process_helper() {
    let mode = std::env::var("TICKR_PARITY_EXECUTOR_MODE").expect("Executor helper mode");
    let nats = connect_nats(&std::env::var("TICKR_PARITY_NATS_URL").expect("NATS URL")).await;
    let consumer = dispatch_consumer(&nats)
        .await
        .expect("TaskDispatch consumer");
    let pickup = open_pickup_bucket(&nats).await.expect("pickup bucket");
    let liveness = ensure_liveness_bucket(&nats)
        .await
        .expect("liveness bucket");
    let handoff = NatsPickupHandoff::from_message(
        &nats,
        pickup.clone(),
        Some(liveness),
        pull_one(&consumer).await,
    )
    .await
    .expect("NATS pickup handoff");

    match mode.as_str() {
        "normal" => {
            let PickupPreparation::Ready(prepared) = prepare_pickup(
                &handoff,
                &NoopPickupCheckpoint,
                "normal-executor",
                Uuid::new_v4(),
                chrono::Duration::seconds(5),
            )
            .await
            .expect("normal pickup") else {
                panic!("normal pickup did not authorize launch");
            };
            let started = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Started);
            assert!(handoff
                .stage_started(&prepared.claim, &started)
                .await
                .expect("stage Started"));
            let launch_path = std::env::var("TICKR_PARITY_LAUNCH_PATH").expect("launch path");
            let status = tokio::process::Command::new("sh")
                .args(["-c", "printf 'normal\\n' >> \"$1\"", "sh", &launch_path])
                .status()
                .await
                .expect("launch real Task process");
            assert!(status.success());
            let completed = encode_task_event(&prepared.task, Uuid::new_v4(), EmitKind::Completed);
            assert_eq!(
                handoff
                    .outcome_election()
                    .elect_terminal(
                        &prepared.claim,
                        LocalAttemptOutcome::ProcessExitedSuccess,
                        &completed,
                        Utc::now(),
                    )
                    .await
                    .expect("elect normal terminal"),
                TerminalElection::Won
            );
        }
        "crash" => {
            let marker =
                PathBuf::from(std::env::var("TICKR_PARITY_BOUNDARY_PATH").expect("marker path"));
            let _ = prepare_pickup(
                &handoff,
                &BlockAfterClaimProof { marker },
                "crashing-executor",
                Uuid::new_v4(),
                chrono::Duration::milliseconds(500),
            )
            .await;
            unreachable!("the crashing Executor must be killed at the pickup boundary");
        }
        "recover" => {
            let outcome = prepare_pickup(
                &handoff,
                &NoopPickupCheckpoint,
                "replacement-executor",
                Uuid::new_v4(),
                chrono::Duration::milliseconds(500),
            )
            .await
            .expect("recover ambiguous pickup");
            assert!(
                matches!(outcome, PickupPreparation::NoWork),
                "recovery may complete the source but cannot authorize another launch"
            );
            let election = NatsOutcomeElection::new(pickup);
            let terminal = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(value) = election.sweep_one_due().await.expect("sweep due pickup") {
                        break value;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("liveness recovery did not elect a terminal event");
            assert_eq!(terminal.1, TerminalElection::Won);
            std::fs::write(
                std::env::var("TICKR_PARITY_BOUNDARY_PATH").expect("recovery marker"),
                b"recovered",
            )
            .expect("write recovery marker");
        }
        other => panic!("unknown Executor helper mode {other}"),
    }
}

fn log_subject(workflow_id: Uuid, workflow_instance_id: Uuid, task_instance_id: Uuid) -> String {
    format!(
        "{}.{}.{}.{}",
        names::LOG_SUBJECT_PREFIX,
        workflow_id,
        workflow_instance_id,
        task_instance_id
    )
}

fn log_blob_path(workflow_id: Uuid, workflow_instance_id: Uuid, task_instance_id: Uuid) -> String {
    format!("task_logs/{workflow_id}/{workflow_instance_id}/{task_instance_id}.gz")
}

async fn stage_log_records(
    nats: &async_nats::Client,
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
    records: &[&[u8]],
) {
    let js = jetstream::new(nats.clone());
    let subject = log_subject(workflow_id, workflow_instance_id, task_instance_id);
    for (sequence, payload) in records.iter().enumerate() {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(names::LOG_PROTOCOL_HEADER, names::LOG_PROTOCOL);
        headers.insert(names::LOG_KIND_HEADER, names::LOG_KIND_ACCEPTED);
        headers.insert(
            names::LOG_TASK_INSTANCE_HEADER,
            task_instance_id.to_string().as_str(),
        );
        headers.insert(names::LOG_PICKUP_GENERATION_HEADER, "1");
        headers.insert(names::LOG_SEQUENCE_HEADER, sequence.to_string().as_str());
        headers.insert(
            names::LOG_CONTENT_DIGEST_HEADER,
            tickr_proto::coord::log_stream::content_digest(payload).as_str(),
        );
        headers.insert(
            "Nats-Msg-Id",
            format!("log:{task_instance_id}:1:record:{sequence}").as_str(),
        );
        js.publish_with_headers(subject.clone(), headers, payload.to_vec().into())
            .await
            .expect("publish Accepted Log record")
            .await
            .expect("persist Accepted Log record");
    }

    let mut terminal = async_nats::HeaderMap::new();
    terminal.insert(names::LOG_PROTOCOL_HEADER, names::LOG_PROTOCOL);
    terminal.insert(names::LOG_KIND_HEADER, names::LOG_KIND_END);
    terminal.insert(
        names::LOG_TASK_INSTANCE_HEADER,
        task_instance_id.to_string().as_str(),
    );
    terminal.insert(names::LOG_PICKUP_GENERATION_HEADER, "1");
    terminal.insert(names::LOG_EXIT_KIND_HEADER, "status");
    terminal.insert(names::LOG_EXIT_STATUS_HEADER, "0");
    terminal.insert(
        "Nats-Msg-Id",
        format!("log:{task_instance_id}:1:terminal").as_str(),
    );
    js.publish_with_headers(subject, terminal, Default::default())
        .await
        .expect("publish Log terminal")
        .await
        .expect("persist Log terminal");
}

async fn seed_scope(nats: &async_nats::Client, workflow_instance_id: Uuid) {
    let js = jetstream::new(nats.clone());
    let kv = js
        .get_key_value(names::DEFAULT_SCOPE_BUCKET)
        .await
        .expect("default scope bucket");
    let store =
        tickr_ctx::nats_scope::NatsScopeStore::new(kv, "default").expect("construct scope store");
    let owner = workflow_instance_id.to_string();
    store.ensure_scope(&owner).await.expect("create scope");
    store
        .put(format!("{owner}/result"), PARITY_SCOPE_ENVELOPE.to_vec())
        .await
        .expect("write scope");
}

fn compaction_payload(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    normal_task: Uuid,
    crashed_task: Uuid,
) -> Vec<u8> {
    let task = |id: Uuid, state: &str| SnapshotTaskInstance {
        id: id.to_string(),
        task_id: Uuid::new_v4().to_string(),
        name: "parity-task".to_owned(),
        task_type: "Regular".to_owned(),
        state: state.to_owned(),
        executor_id: Some(Uuid::new_v4().to_string()),
        attempt: 0,
        ..Default::default()
    };
    CompactionEnvelope {
        projection: Some(ArchiveProjection {
            id: workflow_instance_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "fresh-all-nats-parity-instance".to_owned(),
            state: "Failed".to_owned(),
            scheduled_at: Some(Utc::now().to_rfc3339()),
            task_instances: vec![
                task(normal_task, "Completed"),
                task(crashed_task, "Unhealthy"),
            ],
            ..Default::default()
        }),
        correlation: "fresh-all-nats-parity".to_owned(),
        shipped_at: Some(Utc::now().to_rfc3339()),
    }
    .encode_to_vec()
}

fn memory_operator() -> Operator {
    Operator::new(Memory::default())
        .expect("memory object store")
        .finish()
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    let mut decoder = GzDecoder::new(bytes);
    let mut output = Vec::new();
    decoder.read_to_end(&mut output).expect("decode final Log");
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawned by the fresh all-NATS parity scenario"]
async fn conductor_process_helper() {
    let mode = std::env::var("TICKR_PARITY_CONDUCTOR_MODE").expect("Conductor helper mode");
    let nats = connect_nats(&std::env::var("TICKR_PARITY_NATS_URL").expect("NATS URL")).await;
    let postgres_url = std::env::var("TICKR_PARITY_POSTGRES_URL").expect("Postgres URL");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&postgres_url)
        .await
        .expect("connect parity Postgres");
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));

    if mode == "crash" {
        let compaction = compaction_drain::init_stream_and_consumer(&nats)
            .await
            .expect("Compaction consumer");
        let mut compaction_messages = compaction
            .stream()
            .messages()
            .await
            .expect("Compaction delivery stream");
        let _held_compaction =
            tokio::time::timeout(Duration::from_secs(10), compaction_messages.next())
                .await
                .expect("Compaction was not delivered")
                .expect("Compaction stream ended")
                .expect("valid Compaction delivery");

        let (closed_tx, closed_rx) = mpsc::channel::<ConductorRelayMessage>(1);
        drop(closed_rx);
        drain_task_events(
            task_event_consumer(&nats)
                .await
                .expect("TaskEvent consumer"),
            closed_tx,
            repositories,
            nats,
            CancellationToken::new(),
        )
        .await;
        std::fs::write(
            std::env::var("TICKR_PARITY_BOUNDARY_PATH").expect("Conductor crash marker"),
            b"forwarding-failed-with-compaction-pending",
        )
        .expect("write Conductor crash marker");
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    assert_eq!(mode, "recover");
    let minimum_events: usize = std::env::var("TICKR_PARITY_EVENT_COUNT")
        .expect("minimum event count")
        .parse()
        .expect("numeric minimum event count");
    let outcome_cancel = CancellationToken::new();
    let outcome_handle = tokio::spawn(drain_attempt_outcomes(
        nats.clone(),
        None,
        outcome_cancel.clone(),
    ));
    let (relay_tx, mut relay_rx) = mpsc::channel::<ConductorRelayMessage>(16);
    let event_cancel = CancellationToken::new();
    let event_handle = tokio::spawn(drain_task_events(
        task_event_consumer(&nats)
            .await
            .expect("TaskEvent consumer"),
        relay_tx,
        Arc::clone(&repositories),
        nats.clone(),
        event_cancel.clone(),
    ));
    let mut forwarded = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), relay_rx.recv()).await {
            Ok(Some(message)) => forwarded.push(message.payload),
            Ok(None) => panic!("TaskEvent relay closed"),
            Err(_) if forwarded.len() >= minimum_events => break,
            Err(_) => panic!("TaskEvents were not redelivered"),
        }
    }
    event_cancel.cancel();
    outcome_cancel.cancel();
    event_handle.await.expect("join TaskEvent drain");
    outcome_handle.await.expect("join attempt-outcome drain");

    let storage = memory_operator();
    let compaction_cancel = CancellationToken::new();
    let compaction_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&repositories),
        storage.clone(),
        compaction_cancel.clone(),
    ));
    let workflow_instance_id: Uuid = std::env::var("TICKR_PARITY_WORKFLOW_INSTANCE_ID")
        .expect("Workflow instance ID")
        .parse()
        .expect("valid Workflow instance ID");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let archived: i64 =
                sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = $1")
                    .bind(workflow_instance_id)
                    .fetch_one(&pool)
                    .await
                    .expect("read archive state");
            if archived == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("pending Compaction did not resume");

    let workflow_id: Uuid = std::env::var("TICKR_PARITY_WORKFLOW_ID")
        .expect("Workflow ID")
        .parse()
        .expect("valid Workflow ID");
    let normal_task: Uuid = std::env::var("TICKR_PARITY_NORMAL_TASK_INSTANCE_ID")
        .expect("normal Task instance ID")
        .parse()
        .expect("valid normal Task instance ID");
    let crashed_task: Uuid = std::env::var("TICKR_PARITY_CRASH_TASK_INSTANCE_ID")
        .expect("crashed Task instance ID")
        .parse()
        .expect("valid crashed Task instance ID");
    let normal_blob = storage
        .read(&log_blob_path(
            workflow_id,
            workflow_instance_id,
            normal_task,
        ))
        .await
        .expect("read normal final Log");
    assert_eq!(gunzip(&normal_blob.to_vec()), NORMAL_LOG);
    let crash_blob = storage
        .read(&log_blob_path(
            workflow_id,
            workflow_instance_id,
            crashed_task,
        ))
        .await
        .expect("read crash final Log");
    assert_eq!(
        gunzip(&crash_blob.to_vec()),
        [CRASH_LOG_FIRST, CRASH_LOG_SECOND].concat()
    );

    let archived_tasks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_instances WHERE workflow_instance_id = $1")
            .bind(workflow_instance_id)
            .fetch_one(&pool)
            .await
            .expect("read archived Task instances");
    assert_eq!(archived_tasks, 2);
    let js = jetstream::new(nats.clone());
    let staging = js
        .get_key_value(names::COMPACTION_STAGING_BUCKET)
        .await
        .expect("Compaction staging bucket");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let payload_removed = staging
                .get(&format!("payload.{workflow_instance_id}"))
                .await
                .expect("read staged payload")
                .is_none();
            let completion_retained = staging
                .get(&format!("complete.{workflow_instance_id}"))
                .await
                .expect("read Compaction completion")
                .is_some();
            if payload_removed && completion_retained {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Compaction staging cleanup did not complete");
    let mut log_stream = js
        .get_stream(names::LOG_STREAM)
        .await
        .expect("Log staging stream");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if log_stream
                .info()
                .await
                .expect("Log stream state")
                .state
                .messages
                == 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Accepted Log records were not purged to terminal fences");

    compaction_cancel.cancel();
    compaction_handle
        .await
        .expect("join Compaction drain")
        .expect("Compaction drain");
    let evidence = forwarded
        .iter()
        .map(hex::encode)
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        std::env::var("TICKR_PARITY_RECOVERY_PATH").expect("recovery evidence path"),
        evidence,
    )
    .expect("write recovery evidence");
}

struct CapturingPatchSender {
    sent: Mutex<Vec<pp::PatchEnvelope>>,
}

#[async_trait::async_trait]
impl PatchRelaySender for CapturingPatchSender {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> anyhow::Result<()> {
        self.sent.lock().await.push(envelope.clone());
        Ok(())
    }
}

fn assert_only_hardened_resources(client: &async_nats::Client) -> impl Future<Output = ()> + '_ {
    async move {
        let js = jetstream::new(client.clone());
        let mut actual = BTreeSet::new();
        let mut names_stream = js.stream_names();
        while let Some(name) = names_stream.next().await {
            actual.insert(name.expect("stream name"));
        }
        let mut expected: BTreeSet<String> =
            names::STREAM_NAMES.into_iter().map(str::to_owned).collect();
        expected.extend(
            names::KV_BUCKET_NAMES
                .into_iter()
                .map(|bucket| format!("KV_{bucket}")),
        );
        assert_eq!(actual, expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_all_nats_real_process_parity_survives_executor_and_conductor_crashes() {
    assert!(
        Command::new("nickel")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false),
        "Nickel is required for the real registration path"
    );
    let fixture = FreshAllNats::provisioned()
        .await
        .expect("start fresh isolated all-NATS formation");
    let (_postgres, postgres_url, pool) = start_postgres().await;
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(pool.clone()));
    std::env::set_var(
        tickr_conductor::parser::nickel::DSL_PATHS_ENV,
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("dsl"),
    );

    let registration = process_register(
        repositories.as_ref(),
        &fixture.client,
        RegisterRequest {
            nickel_source: workflow_source(),
            namespace: "default".to_owned(),
        },
    )
    .await
    .expect("register Workflow");
    let (workflow_id, workflow_version) = match registration {
        RegisterOutcome::Inserted {
            workflow_id,
            workflow_version,
            ..
        } => (workflow_id, workflow_version),
        _ => panic!("fresh registration was not inserted"),
    };
    assert_eq!(workflow_version, 1);

    let (_definition_notifier, definition_notifications) =
        definition_build_notifications(NonZeroUsize::new(1).expect("non-zero"));
    let definition_cancel = CancellationToken::new();
    let definition_handle = tokio::spawn(start_local_definition_build_worker(
        Arc::clone(&repositories),
        Arc::new(TestBuildExecutor::new()),
        "notification-free-definition".to_owned(),
        definition_notifications,
        LocalDefinitionBuildWorkerConfig {
            scan_interval: Duration::from_millis(50),
            lease_duration: Duration::from_secs(2),
            batch_size: NonZeroUsize::new(4).expect("non-zero"),
        },
        definition_cancel.clone(),
    ));
    wait_for_workflow_status(&pool, workflow_id, "Ready").await;
    definition_cancel.cancel();
    definition_handle
        .await
        .expect("join definition worker")
        .expect("definition worker");
    let successful_builds: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_task_builds \
         WHERE workflow_id = $1 AND workflow_version = 1 AND status = 'success'",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("read definition-build finalization");
    assert_eq!(successful_builds, 1);

    let (definition_relay, mut definition_relay_rx) = mpsc::channel(4);
    init_relay_tx(definition_relay).await;
    let (_submission_notifier, submission_notifications) =
        definition_submission_notifications(NonZeroUsize::new(1).expect("non-zero"));
    let submission_cancel = CancellationToken::new();
    let submission_handle = tokio::spawn(start_local_definition_submission_worker(
        Arc::clone(&repositories),
        "notification-free-submission".to_owned(),
        submission_notifications,
        LocalDefinitionSubmissionWorkerConfig {
            scan_interval: Duration::from_millis(50),
            lease_duration: Duration::from_secs(2),
            batch_size: NonZeroUsize::new(4).expect("non-zero"),
        },
        submission_cancel.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(10), definition_relay_rx.recv())
        .await
        .expect("definition submission was not forwarded")
        .expect("definition relay closed");
    wait_for_workflow_status(&pool, workflow_id, "Submitted").await;
    submission_cancel.cancel();
    submission_handle
        .await
        .expect("join submission worker")
        .expect("submission worker");

    let workflow_instance_id = Uuid::new_v4();
    let patch_sender = CapturingPatchSender {
        sent: Mutex::new(Vec::new()),
    };
    let patch = process_patch(
        repositories.as_ref(),
        &patch_sender,
        workflow_instance_id,
        Uuid::new_v4(),
        ParsedPatch {
            ops: vec![],
            operation: None,
            reason: Some("no Task additions".to_owned()),
            stall_ttl: None,
            source: PatchSource::nickel("{ ops = [], reason = \"no Task additions\" }"),
        },
        PatchProvenance::External,
    )
    .await
    .expect("submit Patch without transient notification");
    let PatchIngress::Accepted { build_jobs, .. } = patch else {
        panic!("Patch was not accepted");
    };
    assert!(
        build_jobs.is_empty(),
        "this Patch has no Task additions, so Patch build is not applicable"
    );
    assert_eq!(patch_sender.sent.lock().await.len(), 1);

    let task_id: Uuid = sqlx::query_scalar(
        "SELECT task_id FROM workflow_task_builds \
         WHERE workflow_id = $1 AND workflow_version = 1 ORDER BY task_id LIMIT 1",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .expect("registered Task identity");
    let normal_task_instance_id = Uuid::new_v4();
    let crashed_task_instance_id = Uuid::new_v4();
    let directory = tempfile::tempdir().expect("parity process markers");
    let launch_path = directory.path().join("launches");

    publish_dispatch(
        &fixture.client,
        &task_dispatch(
            workflow_id,
            workflow_instance_id,
            task_id,
            normal_task_instance_id,
        ),
    )
    .await;
    let mut normal_executor = spawn_helper(
        "executor_process_helper",
        &[
            ("TICKR_PARITY_EXECUTOR_MODE", "normal".to_owned()),
            ("TICKR_PARITY_NATS_URL", fixture.url.clone()),
            (
                "TICKR_PARITY_LAUNCH_PATH",
                launch_path.to_string_lossy().into_owned(),
            ),
        ],
    );
    assert!(
        normal_executor
            .wait()
            .expect("wait for normal Executor")
            .success(),
        "normal Executor helper failed"
    );
    assert_eq!(
        std::fs::read_to_string(&launch_path)
            .expect("normal Task launch")
            .lines()
            .count(),
        1
    );

    publish_dispatch(
        &fixture.client,
        &task_dispatch(
            workflow_id,
            workflow_instance_id,
            task_id,
            crashed_task_instance_id,
        ),
    )
    .await;
    let pickup_boundary = directory.path().join("pickup-boundary");
    let mut crashing_executor = spawn_helper(
        "executor_process_helper",
        &[
            ("TICKR_PARITY_EXECUTOR_MODE", "crash".to_owned()),
            ("TICKR_PARITY_NATS_URL", fixture.url.clone()),
            (
                "TICKR_PARITY_BOUNDARY_PATH",
                pickup_boundary.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_path(&pickup_boundary).await;
    crashing_executor.kill().expect("crash Executor");
    crashing_executor.wait().expect("reap crashed Executor");

    let recovery_marker = directory.path().join("executor-recovered");
    let mut replacement_executor = spawn_helper(
        "executor_process_helper",
        &[
            ("TICKR_PARITY_EXECUTOR_MODE", "recover".to_owned()),
            ("TICKR_PARITY_NATS_URL", fixture.url.clone()),
            (
                "TICKR_PARITY_BOUNDARY_PATH",
                recovery_marker.to_string_lossy().into_owned(),
            ),
        ],
    );
    assert!(
        replacement_executor
            .wait()
            .expect("wait for replacement Executor")
            .success(),
        "replacement Executor helper failed"
    );
    wait_for_path(&recovery_marker).await;
    assert_eq!(
        std::fs::read_to_string(&launch_path)
            .expect("read launch evidence")
            .lines()
            .count(),
        1,
        "the crashed pickup generation must authorize zero additional launches"
    );

    seed_scope(&fixture.client, workflow_instance_id).await;
    stage_log_records(
        &fixture.client,
        workflow_id,
        workflow_instance_id,
        normal_task_instance_id,
        &[NORMAL_LOG],
    )
    .await;
    stage_log_records(
        &fixture.client,
        workflow_id,
        workflow_instance_id,
        crashed_task_instance_id,
        &[CRASH_LOG_FIRST, CRASH_LOG_SECOND],
    )
    .await;
    stage_compaction_payload(
        &fixture.client,
        compaction_payload(
            workflow_id,
            workflow_instance_id,
            normal_task_instance_id,
            crashed_task_instance_id,
        ),
    )
    .await
    .expect("stage Compaction");

    let conductor_boundary = directory.path().join("conductor-boundary");
    let mut crashing_conductor = spawn_helper(
        "conductor_process_helper",
        &[
            ("TICKR_PARITY_CONDUCTOR_MODE", "crash".to_owned()),
            ("TICKR_PARITY_NATS_URL", fixture.url.clone()),
            ("TICKR_PARITY_POSTGRES_URL", postgres_url.clone()),
            (
                "TICKR_PARITY_BOUNDARY_PATH",
                conductor_boundary.to_string_lossy().into_owned(),
            ),
        ],
    );
    wait_for_path(&conductor_boundary).await;
    crashing_conductor.kill().expect("crash Conductor");
    crashing_conductor.wait().expect("reap crashed Conductor");
    // The durable consumer may have prefetched the whole batch before death;
    // wait past its server-owned acknowledgement window before replacement.
    tokio::time::sleep(Duration::from_secs(31)).await;

    let recovery_evidence = directory.path().join("conductor-recovered");
    let mut replacement_conductor = spawn_helper(
        "conductor_process_helper",
        &[
            ("TICKR_PARITY_CONDUCTOR_MODE", "recover".to_owned()),
            ("TICKR_PARITY_NATS_URL", fixture.url.clone()),
            ("TICKR_PARITY_POSTGRES_URL", postgres_url),
            ("TICKR_PARITY_EVENT_COUNT", "5".to_owned()),
            ("TICKR_PARITY_WORKFLOW_ID", workflow_id.to_string()),
            (
                "TICKR_PARITY_WORKFLOW_INSTANCE_ID",
                workflow_instance_id.to_string(),
            ),
            (
                "TICKR_PARITY_NORMAL_TASK_INSTANCE_ID",
                normal_task_instance_id.to_string(),
            ),
            (
                "TICKR_PARITY_CRASH_TASK_INSTANCE_ID",
                crashed_task_instance_id.to_string(),
            ),
            (
                "TICKR_PARITY_RECOVERY_PATH",
                recovery_evidence.to_string_lossy().into_owned(),
            ),
        ],
    );
    assert!(
        replacement_conductor
            .wait()
            .expect("wait for replacement Conductor")
            .success(),
        "replacement Conductor helper failed"
    );
    let forwarded = std::fs::read_to_string(&recovery_evidence)
        .expect("read Conductor recovery evidence")
        .lines()
        .map(|line| {
            let bytes = hex::decode(line).expect("decode forwarded TaskEvent");
            tc::TaskEvent::decode(bytes.as_slice()).expect("decode TaskEvent")
        })
        .collect::<Vec<_>>();
    let normal_events = forwarded
        .iter()
        .filter(|event| event.task_instance_id == normal_task_instance_id.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        normal_events.len(),
        3,
        "normal TaskEvents: {normal_events:?}"
    );
    assert_eq!(
        normal_events
            .iter()
            .filter(|event| matches!(event.kind, Some(tc::task_event::Kind::Completed(_))))
            .count(),
        1
    );
    let crash_events = forwarded
        .iter()
        .filter(|event| event.task_instance_id == crashed_task_instance_id.to_string())
        .collect::<Vec<_>>();
    assert_eq!(crash_events.len(), 2, "crash TaskEvents: {crash_events:?}");
    assert_eq!(
        crash_events
            .iter()
            .filter(|event| matches!(event.kind, Some(tc::task_event::Kind::Unhealthy(_))))
            .count(),
        1,
        "one pickup generation must have exactly one elected terminal TaskEvent"
    );

    let archive_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = $1")
            .bind(workflow_instance_id)
            .fetch_one(&pool)
            .await
            .expect("read final Workflow instance");
    assert_eq!(archive_rows, 1);
    assert_only_hardened_resources(&fixture.client).await;
    drop(fixture);
}
