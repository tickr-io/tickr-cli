//! Integration tests for the compaction stage-then-drain path.
//!
//! Spins up an ephemeral NATS-with-JetStream (the work queue + the Log
//! staging stream) and an ephemeral Postgres (the archive) via
//! testcontainers, with an in-memory opendal operator standing in for
//! object storage, and verifies:
//!   1. Staging is durable and needs no Postgres: `stage_compaction_payload`
//!      lands the job in the work-queue stream with only a NATS client —
//!      the relay path can ACK on stage with no archive write.
//!   2. The drain consumes a staged job and produces the same three-table
//!      archive rows the synchronous path produced.
//!   3. Duplicate staging (server re-ship, queue redelivery, or a drain
//!      that died between commit and queue-ack) converges on re-run.
//!   4. A staging failure yields no `Ok` — the relay handler sends no ACK,
//!      so the server re-ships on its existing triggers.
//!   5. A failed task's staged logs are uploaded at compaction and its log
//!      subject purged afterward.
//!   6. Retried tasks (two attempts = two TaskInstances) land on separate
//!      subjects and separate blobs with no cross-attempt interleaving.
//!
//! Requires Docker running (testcontainers). Skipped automatically when
//! Docker isn't available — the connection failure is the skip marker.

#![cfg(not(madsim))]

mod common;

use chrono::Utc;
use flate2::read::GzDecoder;
use futures::StreamExt;
use opendal::{services::Memory, Operator};
use prost::Message;
use sqlx::Row;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::system_tasks::compaction_drain;
use tickr_conductor::system_tasks::compaction_receiver::persist_compaction_projection;
use tickr_conductor::system_tasks::{run_compaction_drain, stage_compaction_payload};
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::signal_repository::SignalCapturesInput;
use tickr_proto::archive::{ArchiveProjection, CompactionEnvelope};
use tickr_proto::instance::SnapshotTaskInstance;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// A staged compaction job containing the archive projection identity and its
/// per-task rows.
struct Payload {
    workflow_id: Uuid,
    instance_id: Uuid,
    state: &'static str,
    tasks: Vec<SnapshotTaskInstance>,
}

/// One archived task-instance row at the fidelity the projection carries.
fn snapshot_task(state: &str) -> SnapshotTaskInstance {
    SnapshotTaskInstance {
        id: Uuid::new_v4().to_string(),
        task_id: Uuid::new_v4().to_string(),
        name: "test-task".to_string(),
        task_type: "Regular".to_string(),
        state: state.to_string(),
        executor_id: Some(Uuid::new_v4().to_string()),
        attempt: 0,
        ..Default::default()
    }
}

fn build_payload(state: &'static str, task_count: usize) -> Payload {
    Payload {
        workflow_id: Uuid::new_v4(),
        instance_id: Uuid::new_v4(),
        state,
        tasks: (0..task_count).map(|_| snapshot_task(state)).collect(),
    }
}

/// Encode an archive projection in a correlated compaction envelope.
fn encode_proto_job(p: &Payload) -> Vec<u8> {
    let projection = ArchiveProjection {
        id: p.instance_id.to_string(),
        workflow_id: p.workflow_id.to_string(),
        name: format!("compaction-drain-test-{}", p.instance_id),
        state: p.state.to_string(),
        scheduled_at: Some(Utc::now().to_rfc3339()),
        task_instances: p.tasks.clone(),
        ..Default::default()
    };
    CompactionEnvelope {
        projection: Some(projection),
        correlation: "test-correlation".to_string(),
        shipped_at: Some(Utc::now().to_rfc3339()),
    }
    .encode_to_vec()
}

/// Log staging stream name/subject shape, matching the executor's publisher.
const LOG_STREAM_NAME: &str = tickr_proto::coord::all_nats::LOG_STREAM;
const SCOPE_ENVELOPE: &[u8] = br#"{ "v": 2, "type": "string", "value": "archived", "secret": false, "producer": { "kind": "task", "task_id": "task-7", "task_name": "test-task" }, "created_at": "2026-07-23T00:00:00Z", "sha256": "scope-law" }"#;

fn log_subject(workflow_id: Uuid, workflow_instance_id: Uuid, ti_id: &str) -> String {
    format!(
        "{}.{}.{}.{}",
        tickr_proto::coord::all_nats::LOG_SUBJECT_PREFIX,
        workflow_id,
        workflow_instance_id,
        ti_id
    )
}

fn blob_path(workflow_id: Uuid, workflow_instance_id: Uuid, ti_id: &str) -> String {
    format!(
        "task_logs/{}/{}/{}.gz",
        workflow_id, workflow_instance_id, ti_id
    )
}

fn sidecar_path(workflow_id: Uuid, workflow_instance_id: Uuid, ti_id: &str) -> String {
    format!(
        "task_logs/{}/{}/{}.exit.json",
        workflow_id, workflow_instance_id, ti_id
    )
}

/// Publish the controlled terminal record under the accepted-Log protocol.
async fn publish_marker(
    nats: &async_nats::Client,
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    ti_id: &str,
    exit_status: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let js = async_nats::jetstream::new(nats.clone());
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        tickr_proto::coord::all_nats::LOG_PROTOCOL_HEADER,
        tickr_proto::coord::all_nats::LOG_PROTOCOL,
    );
    headers.insert(
        tickr_proto::coord::all_nats::LOG_KIND_HEADER,
        tickr_proto::coord::all_nats::LOG_KIND_END,
    );
    headers.insert(
        tickr_proto::coord::all_nats::LOG_TASK_INSTANCE_HEADER,
        ti_id,
    );
    headers.insert(
        tickr_proto::coord::all_nats::LOG_PICKUP_GENERATION_HEADER,
        "1",
    );
    headers.insert(tickr_proto::coord::all_nats::LOG_EXIT_KIND_HEADER, "status");
    headers.insert(
        tickr_proto::coord::all_nats::LOG_EXIT_STATUS_HEADER,
        exit_status.to_string().as_str(),
    );
    headers.insert("Nats-Msg-Id", format!("log:{ti_id}:1:terminal").as_str());
    js.publish_with_headers(
        log_subject(workflow_id, workflow_instance_id, ti_id),
        headers,
        Default::default(),
    )
    .await?
    .await?;
    Ok(())
}

fn memory_operator() -> Operator {
    Operator::new(Memory::default()).unwrap().finish()
}

async fn seed_scope(
    nats: &async_nats::Client,
    payload: &Payload,
) -> Result<(), Box<dyn std::error::Error>> {
    let namespace = "default";
    let bucket = tickr_ctx::scope::bucket_for_namespace(namespace);
    let js = async_nats::jetstream::new(nats.clone());
    let kv = match js.get_key_value(&bucket).await {
        Ok(kv) => kv,
        Err(_) => {
            js.create_key_value(async_nats::jetstream::kv::Config {
                bucket,
                history: 1,
                max_value_size: tickr_ctx::store::MAX_VALUE_SIZE,
                storage: async_nats::jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await?
        }
    };
    let owner = payload.instance_id.to_string();
    let store = tickr_ctx::nats_scope::NatsScopeStore::new(kv, namespace)?;
    store.ensure_scope(&owner).await?;
    store
        .put(format!("{owner}/result"), SCOPE_ENVELOPE.to_vec())
        .await?;
    Ok(())
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    let mut decoder = GzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).expect("gzip decode");
    out
}

/// Create the Log staging stream the way the executor's `init_log_stream`
/// does, then publish the given batches onto the task's subject.
async fn stage_log_batches(
    nats: &async_nats::Client,
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    ti_id: &str,
    batches: &[&[u8]],
) -> Result<(), Box<dyn std::error::Error>> {
    let js = async_nats::jetstream::new(nats.clone());
    js.get_or_create_stream(async_nats::jetstream::stream::Config {
        name: LOG_STREAM_NAME.to_string(),
        subjects: vec![tickr_proto::coord::all_nats::LOG_STREAM_SUBJECTS.to_string()],
        ..Default::default()
    })
    .await?;
    let subject = log_subject(workflow_id, workflow_instance_id, ti_id);
    for (sequence, batch) in batches.iter().enumerate() {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            tickr_proto::coord::all_nats::LOG_PROTOCOL_HEADER,
            tickr_proto::coord::all_nats::LOG_PROTOCOL,
        );
        headers.insert(
            tickr_proto::coord::all_nats::LOG_KIND_HEADER,
            tickr_proto::coord::all_nats::LOG_KIND_ACCEPTED,
        );
        headers.insert(
            tickr_proto::coord::all_nats::LOG_TASK_INSTANCE_HEADER,
            ti_id,
        );
        headers.insert(
            tickr_proto::coord::all_nats::LOG_PICKUP_GENERATION_HEADER,
            "1",
        );
        headers.insert(
            tickr_proto::coord::all_nats::LOG_SEQUENCE_HEADER,
            sequence.to_string().as_str(),
        );
        headers.insert(
            tickr_proto::coord::all_nats::LOG_CONTENT_DIGEST_HEADER,
            tickr_proto::coord::log_stream::content_digest(batch).as_str(),
        );
        headers.insert(
            "Nats-Msg-Id",
            format!("log:{ti_id}:1:record:{sequence}").as_str(),
        );
        js.publish_with_headers(subject.clone(), headers, batch.to_vec().into())
            .await?
            .await?;
    }
    Ok(())
}

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default().with_cmd(&cmd).start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{}", port);

    let mut client = None;
    for _ in 0..50 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Some((container, client.expect("nats connect")))
}

async fn start_postgres_with_migrations() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

/// Poll until the archive holds the workflow_instance row or the deadline
/// elapses. Returns whether the row appeared.
async fn wait_for_archived(pool: &sqlx::PgPool, wfi_id: Uuid, deadline: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        let count: i64 = sqlx::query("SELECT count(*) FROM workflow_instances WHERE id = $1")
            .bind(wfi_id)
            .fetch_one(pool)
            .await
            .map(|row| row.get(0))
            .unwrap_or(0);
        if count > 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_is_durable_and_needs_no_postgres() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };

    let payload = build_payload("Completed", 1);
    let bytes = encode_proto_job(&payload);

    // No Postgres pool exists anywhere in this test — staging must succeed
    // with NATS alone, because the relay path ACKs on stage.
    stage_compaction_payload(&nats, bytes.clone()).await?;

    // The job is durably in the work-queue stream.
    let js = async_nats::jetstream::new(nats.clone());
    let mut stream = js.get_stream(compaction_drain::STREAM_NAME).await?;
    let info = stream.info().await?;
    assert_eq!(
        info.state.messages, 1,
        "staged job must be durably held by the work-queue stream"
    );
    let staging = js
        .get_key_value(tickr_proto::coord::all_nats::COMPACTION_STAGING_BUCKET)
        .await?;
    let staged = staging
        .get(&format!("payload.{}", payload.instance_id))
        .await?
        .expect("raw payload under stable Compaction identity");
    assert_eq!(staged.as_ref(), bytes.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_identity_rejects_conflicting_payload() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let mut payload = build_payload("Completed", 1);
    let original = encode_proto_job(&payload);
    stage_compaction_payload(&nats, original.clone()).await?;

    payload.state = "Failed";
    let conflicting = encode_proto_job(&payload);
    let error = stage_compaction_payload(&nats, conflicting)
        .await
        .expect_err("same identity with different bytes must conflict");
    assert!(error.to_string().contains("conflicts with staged payload"));

    let js = async_nats::jetstream::new(nats);
    let staging = js
        .get_key_value(tickr_proto::coord::all_nats::COMPACTION_STAGING_BUCKET)
        .await?;
    let staged = staging
        .get(&format!("payload.{}", payload.instance_id))
        .await?
        .expect("original stable payload");
    assert_eq!(
        staged.as_ref(),
        original.as_slice(),
        "a conflict must not replace staged bytes"
    );
    let mut stream = js.get_stream(compaction_drain::STREAM_NAME).await?;
    assert_eq!(stream.info().await?.state.messages, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_death_before_archive_commit_redelivers() -> Result<(), Box<dyn std::error::Error>>
{
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let payload = build_payload("Completed", 1);
    let bytes = encode_proto_job(&payload);
    stage_compaction_payload(&nats, bytes.clone()).await?;

    let consumer = compaction_drain::init_stream_and_consumer(&nats).await?;
    let mut delivery = consumer.stream().messages().await?;
    let first = tokio::time::timeout(Duration::from_secs(5), delivery.next())
        .await?
        .expect("first Compaction delivery")?;
    assert_eq!(first.payload.as_ref(), bytes.as_slice());
    drop(first);
    drop(delivery);

    tokio::time::sleep(tickr_proto::coord::all_nats::COMPACTION_ACK_WAIT).await;
    let replacement = compaction_drain::init_stream_and_consumer(&nats).await?;
    let mut redelivery = replacement.stream().messages().await?;
    let second = tokio::time::timeout(Duration::from_secs(5), redelivery.next())
        .await?
        .expect("redelivered Compaction")?;
    assert_eq!(second.payload.as_ref(), bytes.as_slice());
    second.ack().await.expect("ack redelivered Compaction");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_rejects_missing_and_corrupt_scope() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return Ok(());
    };
    let repositories = WriterRepositoryBundle::from_postgres_pool(pool.clone());

    let missing = build_payload("Completed", 0);
    let missing_projection = CompactionEnvelope::decode(encode_proto_job(&missing).as_slice())?
        .projection
        .expect("test Compaction projection");
    let missing_error =
        persist_compaction_projection(&repositories, &missing_projection, None, Some(&nats))
            .await
            .expect_err("missing scope must fail Compaction");
    assert!(missing_error.to_string().contains("seal tickr-ctx scope"));

    let corrupt = build_payload("Completed", 0);
    seed_scope(&nats, &corrupt).await?;
    let bucket = tickr_ctx::scope::bucket_for_namespace("default");
    let scope = async_nats::jetstream::new(nats.clone())
        .get_key_value(&bucket)
        .await?;
    scope
        .put(
            &format!("{}/result", corrupt.instance_id),
            br#"{"v":99,"opaque":"future"}"#.as_slice().into(),
        )
        .await?;
    let corrupt_projection = CompactionEnvelope::decode(encode_proto_job(&corrupt).as_slice())?
        .projection
        .expect("test Compaction projection");
    let corrupt_error =
        persist_compaction_projection(&repositories, &corrupt_projection, None, Some(&nats))
            .await
            .expect_err("corrupt scope must fail Compaction");
    assert!(corrupt_error.to_string().contains("seal tickr-ctx scope"));

    let archived: i64 =
        sqlx::query_scalar("SELECT count(*) FROM workflow_instances WHERE id = ANY($1)")
            .bind(vec![missing.instance_id, corrupt.instance_id])
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        archived, 0,
        "failed Compaction must not fabricate an archive"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_archives_a_staged_job() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool.as_ref().clone(),
    ));

    let payload = build_payload("Completed", 2);
    let wfi_id = payload.instance_id;
    let staged_bytes = encode_proto_job(&payload);
    seed_scope(&nats, &payload).await?;
    for task in &payload.tasks {
        stage_log_batches(
            &nats,
            payload.workflow_id,
            payload.instance_id,
            &task.id,
            &[],
        )
        .await?;
        publish_marker(&nats, payload.workflow_id, payload.instance_id, &task.id, 0).await?;
    }
    stage_compaction_payload(&nats, staged_bytes).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&repositories),
        memory_operator(),
        shutdown.clone(),
    ));

    assert!(
        wait_for_archived(&pool, wfi_id, Duration::from_secs(15)).await,
        "drain must archive the staged job"
    );

    let ti_count: i64 =
        sqlx::query("SELECT count(*) FROM task_instances WHERE workflow_instance_id = $1")
            .bind(wfi_id)
            .fetch_one(pool.as_ref())
            .await?
            .get(0);
    assert_eq!(ti_count, 2, "expected two task_instance rows");

    let run_info_count: i64 =
        sqlx::query("SELECT count(*) FROM workflow_run_info WHERE workflow_instance_id = $1")
            .bind(wfi_id)
            .fetch_one(pool.as_ref())
            .await?
            .get(0);
    assert_eq!(run_info_count, 1, "expected the enrichment row");
    let archived_scope: serde_json::Value = sqlx::query_scalar(
        "SELECT ctx_envelope FROM workflow_run_info WHERE workflow_instance_id = $1",
    )
    .bind(wfi_id)
    .fetch_one(pool.as_ref())
    .await?;
    assert_eq!(
        archived_scope[0]["envelope_bytes"],
        hex::encode(SCOPE_ENVELOPE),
        "Compaction must archive the exact accepted scope bytes"
    );

    // The drained (acked) job must be gone from the work queue.
    let js = async_nats::jetstream::new(nats.clone());
    let mut stream = js.get_stream(compaction_drain::STREAM_NAME).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let info = stream.info().await?;
        if info.state.messages == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "acked job must be removed from the WorkQueue stream"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let staging = js
        .get_key_value(tickr_proto::coord::all_nats::COMPACTION_STAGING_BUCKET)
        .await?;
    assert!(
        staging.get(&format!("payload.{wfi_id}")).await?.is_none(),
        "raw Compaction staging must be cleaned after archive commit"
    );
    assert!(
        staging.get(&format!("complete.{wfi_id}")).await?.is_some(),
        "stable completion evidence must survive staging cleanup"
    );
    let scope_kv = js
        .get_key_value(&tickr_ctx::scope::bucket_for_namespace("default"))
        .await?;
    let scope = tickr_ctx::nats_scope::NatsScopeStore::new(scope_kv, "default")?;
    let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if scope.keys(&format!("{wfi_id}/")).await?.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < cleanup_deadline,
            "scope cleanup must follow the committed archive and source acknowledgement"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.cancel();
    let _ = drain_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_jobs_converge() -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool.as_ref().clone(),
    ));

    let payload = build_payload("Failed", 1);
    let wfi_id = payload.instance_id;
    let ti_id = Uuid::parse_str(&payload.tasks[0].id)?;
    let bytes = encode_proto_job(&payload);

    // Link one Trigger-derived Event-variable archive and its NATS working key
    // before terminal delivery. The first drain must settle/delete it; the
    // duplicate delivery must remain an idempotent no-op.
    let signal_id = Uuid::new_v4();
    repositories
        .insert_signal_captures(&SignalCapturesInput {
            signal_id,
            workflow_id: payload.workflow_id,
            workflow_version: Some(1),
            captures: serde_json::json!([{
                "name": "order",
                "envelope": {
                    "present": true,
                    "value": {"id": 42},
                    "producer": {
                        "kind": "Signal",
                        "signal_id": signal_id,
                        "source": {"Manual": {}}
                    },
                    "lineage": [{"segment": "inputs.order"}]
                }
            }]),
        })
        .await?;
    repositories.link_signal_captures(signal_id, wfi_id).await?;
    let js = async_nats::jetstream::new(nats.clone());
    let ctx = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: tickr_proto::coord::all_nats::DEFAULT_SCOPE_BUCKET.to_string(),
            ..Default::default()
        })
        .await?;
    let signal_key = format!("{signal_id}/order");
    ctx.put(&signal_key, b"working-value".to_vec().into())
        .await?;

    // Two copies of the same job: the shape a server re-ship produces, and
    // the shape a drain crash between archive-commit and queue-ack produces
    // (the first copy already archived, the second re-runs over it).
    seed_scope(&nats, &payload).await?;
    stage_log_batches(
        &nats,
        payload.workflow_id,
        payload.instance_id,
        &payload.tasks[0].id,
        &[],
    )
    .await?;
    publish_marker(
        &nats,
        payload.workflow_id,
        payload.instance_id,
        &payload.tasks[0].id,
        1,
    )
    .await?;
    stage_compaction_payload(&nats, bytes.clone()).await?;
    stage_compaction_payload(&nats, bytes).await?;
    let mut staged_stream = js.get_stream(compaction_drain::STREAM_NAME).await?;
    assert_eq!(
        staged_stream.info().await?.state.messages,
        1,
        "same-identity duplicate staging must converge on one queue entry"
    );

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&repositories),
        memory_operator(),
        shutdown.clone(),
    ));

    assert!(
        wait_for_archived(&pool, wfi_id, Duration::from_secs(15)).await,
        "drain must archive the job"
    );

    // Wait until the one stable-identity queue entry is consumed.
    let mut stream = js.get_stream(compaction_drain::STREAM_NAME).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while stream.info().await?.state.messages > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "both duplicate jobs must drain"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let wfi_count: i64 = sqlx::query("SELECT count(*) FROM workflow_instances WHERE id = $1")
        .bind(wfi_id)
        .fetch_one(pool.as_ref())
        .await?
        .get(0);
    assert_eq!(
        wfi_count, 1,
        "duplicate jobs must collapse to one archive row"
    );

    let ti_count: i64 = sqlx::query("SELECT count(*) FROM task_instances WHERE id = $1")
        .bind(ti_id)
        .fetch_one(pool.as_ref())
        .await?
        .get(0);
    assert_eq!(ti_count, 1, "task_instance row must also be deduped");

    let terminal_at: Option<chrono::DateTime<Utc>> =
        sqlx::query_scalar("SELECT terminal_at FROM signal_captures WHERE signal_id = $1")
            .bind(signal_id)
            .fetch_one(pool.as_ref())
            .await?;
    assert!(
        terminal_at.is_some(),
        "the archive commit must be followed by SQL-backed terminal marking"
    );
    assert!(
        ctx.get(&signal_key).await?.is_none(),
        "terminal cleanup must remove the Signal/Event-variable NATS key"
    );

    shutdown.cancel();
    let _ = drain_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_cleanup_sql_failure_is_non_fatal_after_archive_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool.as_ref().clone(),
    ));
    let payload = build_payload("Completed", 0);
    let signal_id = Uuid::new_v4();
    repositories
        .insert_signal_captures(&SignalCapturesInput {
            signal_id,
            workflow_id: payload.workflow_id,
            workflow_version: Some(1),
            captures: serde_json::json!([{
                "name": "order",
                "envelope": {
                    "present": true,
                    "value": {"id": 42},
                    "producer": {
                        "kind": "Signal",
                        "signal_id": signal_id,
                        "source": {"Manual": {}}
                    },
                    "lineage": []
                }
            }]),
        })
        .await?;
    repositories
        .link_signal_captures(signal_id, payload.instance_id)
        .await?;
    // The terminal UPDATE commits, but decoding the malformed returned value
    // fails. Compaction must retain the archive commit and ACK the staged job.
    sqlx::query("UPDATE signal_captures SET captures = '{}'::jsonb WHERE signal_id = $1")
        .bind(signal_id)
        .execute(pool.as_ref())
        .await?;

    let js = async_nats::jetstream::new(nats.clone());
    let ctx = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: tickr_proto::coord::all_nats::DEFAULT_SCOPE_BUCKET.to_string(),
            ..Default::default()
        })
        .await?;
    let signal_key = format!("{signal_id}/order");
    ctx.put(&signal_key, b"working-value".to_vec().into())
        .await?;
    seed_scope(&nats, &payload).await?;
    stage_compaction_payload(&nats, encode_proto_job(&payload)).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&repositories),
        memory_operator(),
        shutdown.clone(),
    ));
    assert!(
        wait_for_archived(&pool, payload.instance_id, Duration::from_secs(15)).await,
        "cleanup failure must not roll back the terminal archive"
    );
    let mut stream = js.get_stream(compaction_drain::STREAM_NAME).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while stream.info().await?.state.messages > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "non-fatal cleanup failure must not prevent queue completion"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let terminal_at: Option<chrono::DateTime<Utc>> =
        sqlx::query_scalar("SELECT terminal_at FROM signal_captures WHERE signal_id = $1")
            .bind(signal_id)
            .fetch_one(pool.as_ref())
            .await?;
    assert!(terminal_at.is_some());
    assert!(
        ctx.get(&signal_key).await?.is_some(),
        "a failed SQL decode cannot supply NATS cleanup keys"
    );

    shutdown.cancel();
    let _ = drain_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_tasks_logs_are_archived_and_records_purged(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool.as_ref().clone(),
    ));

    // A failed workflow whose single (failed) task staged two log batches.
    let mut payload = build_payload("Failed", 1);
    payload.tasks[0].state = "Failed".to_string();
    let ti = payload.tasks[0].clone();
    let wfi_id = payload.instance_id;

    stage_log_batches(
        &nats,
        payload.workflow_id,
        payload.instance_id,
        &ti.id,
        &[b"attempt output\n", b"panic: it broke\n"],
    )
    .await?;
    publish_marker(&nats, payload.workflow_id, payload.instance_id, &ti.id, 1).await?;

    let storage = memory_operator();
    seed_scope(&nats, &payload).await?;
    stage_compaction_payload(&nats, encode_proto_job(&payload)).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&repositories),
        storage.clone(),
        shutdown.clone(),
    ));

    assert!(
        wait_for_archived(&pool, wfi_id, Duration::from_secs(15)).await,
        "drain must archive the failed run"
    );

    // The failed task's blob exists and gunzips to the staged batches.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let blob = loop {
        match storage
            .read(&blob_path(payload.workflow_id, payload.instance_id, &ti.id))
            .await
        {
            Ok(b) => break b,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(format!("failed task's log blob missing: {}", e).into()),
        }
    };
    assert_eq!(
        gunzip(&blob.to_vec()),
        b"attempt output\npanic: it broke\n".to_vec(),
        "blob must hold the concatenated batches — and never the marker"
    );

    // The marker survives archival as the sidecar object.
    let sidecar = storage
        .read(&sidecar_path(
            payload.workflow_id,
            payload.instance_id,
            &ti.id,
        ))
        .await?;
    let marker: serde_json::Value = serde_json::from_slice(&sidecar.to_vec())?;
    assert_eq!(
        marker.get("exit_status").and_then(|v| v.as_i64()),
        Some(1),
        "sidecar must carry the marker's exit status"
    );

    // Accepted Log records are purged after archival while the immutable
    // terminal fence remains to reject a late writer.
    let js = async_nats::jetstream::new(nats.clone());
    let mut log_stream = js.get_stream(LOG_STREAM_NAME).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while log_stream.info().await?.state.messages != 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "Accepted Log records must be purged while one terminal fence remains"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown.cancel();
    let _ = drain_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retried_attempts_get_separate_subjects_and_blobs() -> Result<(), Box<dyn std::error::Error>>
{
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let repositories = Arc::new(WriterRepositoryBundle::from_postgres_pool(
        pool.as_ref().clone(),
    ));

    // Two attempts of one task = two TaskInstances sharing task_id, each
    // with its own id — and therefore its own log subject and blob.
    let mut payload = build_payload("Completed", 2);
    let shared_task_id = payload.tasks[0].task_id.clone();
    payload.tasks[1].task_id = shared_task_id;
    payload.tasks[0].state = "Failed".to_string();
    payload.tasks[0].attempt = 0;
    payload.tasks[1].attempt = 1;
    let attempt1 = payload.tasks[0].clone();
    let attempt2 = payload.tasks[1].clone();
    let wfi_id = payload.instance_id;

    stage_log_batches(
        &nats,
        payload.workflow_id,
        payload.instance_id,
        &attempt1.id,
        &[b"attempt one: failed\n"],
    )
    .await?;
    publish_marker(
        &nats,
        payload.workflow_id,
        payload.instance_id,
        &attempt1.id,
        1,
    )
    .await?;
    // Attempt 2's executor "dies" without closing the stream — no marker.
    stage_log_batches(
        &nats,
        payload.workflow_id,
        payload.instance_id,
        &attempt2.id,
        &[b"attempt two: ok\n"],
    )
    .await?;

    let storage = memory_operator();
    seed_scope(&nats, &payload).await?;
    stage_compaction_payload(&nats, encode_proto_job(&payload)).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&repositories),
        storage.clone(),
        shutdown.clone(),
    ));

    assert!(
        wait_for_archived(&pool, wfi_id, Duration::from_secs(15)).await,
        "drain must archive the run"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let (blob1, blob2) = loop {
        match (
            storage
                .read(&blob_path(
                    payload.workflow_id,
                    payload.instance_id,
                    &attempt1.id,
                ))
                .await,
            storage
                .read(&blob_path(
                    payload.workflow_id,
                    payload.instance_id,
                    &attempt2.id,
                ))
                .await,
        ) {
            (Ok(a), Ok(b)) => break (a, b),
            _ if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ => return Err("per-attempt blobs missing".into()),
        }
    };
    assert_eq!(
        gunzip(&blob1.to_vec()),
        b"attempt one: failed\n".to_vec(),
        "attempt 1's blob must hold only attempt 1's batches"
    );
    assert_eq!(
        gunzip(&blob2.to_vec()),
        b"attempt two: ok\n".to_vec(),
        "attempt 2's blob must hold only attempt 2's batches"
    );

    // Terminal isolation rides subject isolation: attempt 1's controlled end
    // archives to its sidecar, while attempt 2 receives a durable abnormal
    // terminal before Compaction archives it.
    assert!(
        storage
            .read(&sidecar_path(
                payload.workflow_id,
                payload.instance_id,
                &attempt1.id
            ))
            .await
            .is_ok(),
        "attempt 1's sidecar must exist"
    );
    let abnormal_sidecar = storage
        .read(&sidecar_path(
            payload.workflow_id,
            payload.instance_id,
            &attempt2.id,
        ))
        .await?;
    let abnormal: serde_json::Value = serde_json::from_slice(&abnormal_sidecar.to_vec())?;
    assert_eq!(abnormal["exit_status"], -1);
    assert_eq!(
        abnormal["reason"],
        "Executor closed without controlled End-of-stream"
    );

    shutdown.cancel();
    let _ = drain_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_failure_yields_no_ok() -> Result<(), Box<dyn std::error::Error>> {
    let Some((nats_container, nats)) = start_nats().await else {
        return Ok(());
    };

    // Kill the NATS server so the JetStream publish cannot be acknowledged.
    nats_container.stop().await?;

    let payload = build_payload("Completed", 1);
    let bytes = encode_proto_job(&payload);

    // The relay handler sends COMPACTION_ACK only on `Ok(())`. With NATS
    // down, staging must not produce `Ok` — an `Err` or a hang both leave
    // the server unACKed, and it re-ships on its existing triggers.
    let staged = tokio::time::timeout(
        Duration::from_secs(5),
        stage_compaction_payload(&nats, bytes),
    )
    .await;
    assert!(
        !matches!(staged, Ok(Ok(()))),
        "staging must not report durable success while NATS is down"
    );

    Ok(())
}
