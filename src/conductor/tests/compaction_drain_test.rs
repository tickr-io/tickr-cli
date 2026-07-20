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
use tickr_conductor::system_tasks::{run_compaction_drain, stage_compaction_payload};
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
const LOG_STREAM_NAME: &str = "tickr_task_logs";

fn log_subject(workflow_id: Uuid, workflow_instance_id: Uuid, ti_id: &str) -> String {
    format!("logs.{}.{}.{}", workflow_id, workflow_instance_id, ti_id)
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

/// Publish an End-of-stream marker the way the executor does: header-tagged,
/// empty payload, exit status in headers.
async fn publish_marker(
    nats: &async_nats::Client,
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    ti_id: &str,
    exit_status: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let js = async_nats::jetstream::new(nats.clone());
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Tickr-Log-Marker", "end-of-stream");
    headers.insert("Tickr-Exit-Status", exit_status.to_string().as_str());
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
        subjects: vec!["logs.>".to_string()],
        ..Default::default()
    })
    .await?;
    let subject = log_subject(workflow_id, workflow_instance_id, ti_id);
    for batch in batches {
        js.publish(subject.clone(), batch.to_vec().into())
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

    // No Postgres pool exists anywhere in this test — staging must succeed
    // with NATS alone, because the relay path ACKs on stage.
    stage_compaction_payload(&nats, encode_proto_job(&payload)).await?;

    // The job is durably in the work-queue stream.
    let js = async_nats::jetstream::new(nats.clone());
    let mut stream = js.get_stream(compaction_drain::STREAM_NAME).await?;
    let info = stream.info().await?;
    assert_eq!(
        info.state.messages, 1,
        "staged job must be durably held by the work-queue stream"
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

    let payload = build_payload("Completed", 2);
    let wfi_id = payload.instance_id;
    stage_compaction_payload(&nats, encode_proto_job(&payload)).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&pool),
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

    let payload = build_payload("Failed", 1);
    let wfi_id = payload.instance_id;
    let ti_id = Uuid::parse_str(&payload.tasks[0].id)?;
    let bytes = encode_proto_job(&payload);

    // Two copies of the same job: the shape a server re-ship produces, and
    // the shape a drain crash between archive-commit and queue-ack produces
    // (the first copy already archived, the second re-runs over it).
    stage_compaction_payload(&nats, bytes.clone()).await?;
    stage_compaction_payload(&nats, bytes).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&pool),
        memory_operator(),
        shutdown.clone(),
    ));

    assert!(
        wait_for_archived(&pool, wfi_id, Duration::from_secs(15)).await,
        "drain must archive the job"
    );

    // Wait until both queue copies are consumed before asserting counts.
    let js = async_nats::jetstream::new(nats.clone());
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

    shutdown.cancel();
    let _ = drain_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_tasks_logs_are_archived_and_subject_purged(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_nats_container, nats)) = start_nats().await else {
        return Ok(());
    };
    let Some((_pg_container, pool)) = start_postgres_with_migrations().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);

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
    stage_compaction_payload(&nats, encode_proto_job(&payload)).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&pool),
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

    // The log subject is purged after archival. The log stream holds only
    // this one subject in this test, so total message count reaching zero
    // is the purge signal.
    let js = async_nats::jetstream::new(nats.clone());
    let mut log_stream = js.get_stream(LOG_STREAM_NAME).await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while log_stream.info().await?.state.messages > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "log subject must be purged after archival"
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
    stage_compaction_payload(&nats, encode_proto_job(&payload)).await?;

    let shutdown = CancellationToken::new();
    let drain_handle = tokio::spawn(run_compaction_drain(
        nats.clone(),
        Arc::clone(&pool),
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

    // Marker isolation rides subject isolation: attempt 1's marker archives
    // to attempt 1's sidecar; attempt 2 (no marker) gets no sidecar — its
    // archived read stays marker-absent (the abnormal-end signal).
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
    assert!(
        storage
            .read(&sidecar_path(
                payload.workflow_id,
                payload.instance_id,
                &attempt2.id
            ))
            .await
            .is_err(),
        "attempt 2 must have no sidecar"
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
