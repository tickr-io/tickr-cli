//! Integration test for `logs_resolver::LogsResolver` as used by the API
//! component's task-log endpoint. Exercises the three steady-state dispatch
//! outcomes:
//!   1. MinIO hit  → returns decompressed bytes from the gzip blob.
//!   2. Stream hit (MinIO miss) → returns concatenated staged batches.
//!   3. Both miss → `LogsError::NotFound`.
//!
//! The MinIO half is backed by `opendal::services::Memory` rather than an S3
//! testcontainer. The dispatcher only depends on the `Operator`'s
//! `Read`/`NotFound` error semantics, and the `Memory` service preserves those
//! exactly — using it sidesteps the bucket-bootstrap dance an S3 testcontainer
//! would otherwise need without changing what the dispatcher sees. The
//! staging half uses the testcontainers-modules NATS image with JetStream
//! enabled, so Log staging stream semantics match production.
//!
//! Requires Docker (testcontainers) for the NATS half. Tests skip cleanly if
//! Docker is unavailable.

#![cfg(not(madsim))]

use async_nats::jetstream;
use flate2::write::GzEncoder;
use flate2::Compression;
use opendal::{services::Memory, Operator};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_api::http::logs_resolver::{exit_sidecar_key, minio_object_key, LogsError, LogsResolver};
use tickr_executor::log_stream::{
    AllNatsLogStream, AllNatsLogStreamProvider, LogStream, LogStreamProvider, LogStreamRoute,
};
use tickr_proto::coord::log_stream::{
    LogExit, LogRecordIdentity, LogRecordSubmission, LogStreamIdentity,
};
use uuid::Uuid;

fn memory_operator() -> Operator {
    Operator::new(Memory::default()).unwrap().finish()
}

async fn start_nats_with_jetstream() -> Option<(
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

    // The container reports "Server is ready" on stderr before JetStream is
    // fully wired in some images; brief retries cover that startup window.
    let mut client = None;
    for _ in 0..10 {
        match async_nats::connect(&url).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    Some((container, client.expect("nats connect")))
}

/// Create the Log staging stream the way the executor's
/// `init_log_stream` does — same stream name, same subject wildcard.
async fn create_log_stream(nats: &async_nats::Client) {
    let js = jetstream::new(nats.clone());
    js.get_or_create_stream(jetstream::stream::Config {
        name: tickr_proto::coord::all_nats::LOG_STREAM.to_string(),
        subjects: vec![tickr_proto::coord::all_nats::LOG_STREAM_SUBJECTS.to_string()],
        ..Default::default()
    })
    .await
    .expect("create log staging stream");
}

fn log_stream_provider(nats: &async_nats::Client) -> Arc<dyn LogStreamProvider> {
    Arc::new(AllNatsLogStreamProvider::new(
        Arc::new(nats.clone()),
        Duration::from_secs(2),
    ))
}

async fn open_log_stream(
    nats: &async_nats::Client,
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
) -> Result<AllNatsLogStream, Box<dyn std::error::Error>> {
    Ok(AllNatsLogStream::open(
        Arc::new(jetstream::new(nats.clone())),
        LogStreamRoute {
            workflow_id,
            workflow_instance_id,
            task_instance_id,
        },
        LogStreamIdentity {
            task_instance_id,
            pickup_generation: 1,
        },
        Duration::from_secs(2),
    )
    .await?)
}

fn submission(stream: &AllNatsLogStream, sequence: u64, bytes: Vec<u8>) -> LogRecordSubmission {
    LogRecordSubmission::new(
        LogRecordIdentity {
            stream: stream.identity().clone(),
            sequence,
        },
        bytes,
    )
}

fn gzip(plain: &[u8]) -> Vec<u8> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(plain).unwrap();
    enc.finish().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn minio_hit_returns_decompressed_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator();
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    let plain = b"hello\nfrom\nminio\n";
    let key = minio_object_key(wf, wi, ti);
    minio.write(&key, gzip(plain)).await?;

    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));
    let logs = resolver.fetch_task_logs(wf, wi, ti).await?;
    assert_eq!(
        logs.content, plain,
        "MinIO hit must return decompressed bytes"
    );
    assert!(
        logs.marker.is_none(),
        "no sidecar written → marker must be absent"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nats_hit_when_minio_misses() -> Result<(), Box<dyn std::error::Error>> {
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator(); // empty
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    let mut stream = open_log_stream(&nats, wf, wi, ti).await?;
    let batches = [
        b"first\n".to_vec(),
        b"second\n".to_vec(),
        b"third\n".to_vec(),
    ];
    for (sequence, batch) in batches.iter().enumerate() {
        stream
            .accept(submission(&stream, sequence as u64, batch.clone()))
            .await?;
    }

    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));
    let logs = resolver.fetch_task_logs(wf, wi, ti).await?;
    let expected: Vec<u8> = batches.iter().flatten().copied().collect();
    assert_eq!(
        logs.content, expected,
        "stream hit must return concatenated batches in publish order"
    );
    assert!(
        logs.marker.is_none(),
        "no marker published → marker must be absent"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_marker_reported_and_excluded_from_content() -> Result<(), Box<dyn std::error::Error>>
{
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator(); // empty
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    let mut stream = open_log_stream(&nats, wf, wi, ti).await?;
    stream
        .accept(submission(&stream, 0, b"the only line\n".to_vec()))
        .await?;
    stream.finish_cleanly(LogExit::Status(2)).await?;

    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));
    let logs = resolver.fetch_task_logs(wf, wi, ti).await?;
    assert_eq!(
        logs.content, b"the only line\n",
        "marker payload must not leak into log content"
    );
    let marker = logs.marker.expect("marker must be reported");
    assert_eq!(marker.exit_status, 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_reads_return_only_new_batches() -> Result<(), Box<dyn std::error::Error>> {
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator();
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    let mut stream = open_log_stream(&nats, wf, wi, ti).await?;
    for (sequence, batch) in [b"one\n".as_slice(), b"two\n", b"three\n"]
        .into_iter()
        .enumerate()
    {
        stream
            .accept(submission(&stream, sequence as u64, batch.to_vec()))
            .await?;
    }

    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));

    // First poll from sequence 0 sees everything, with advancing sequences.
    let page = resolver.fetch_batches_after(wf, wi, ti, 0).await?;
    assert_eq!(page.batches.len(), 3);
    let seqs: Vec<u64> = page.batches.iter().map(|b| b.seq).collect();
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "sequences must advance"
    );
    assert!(page.marker.is_none());
    let cursor = *seqs.last().unwrap();

    // A poll at the cursor with nothing new returns an empty page.
    let page = resolver.fetch_batches_after(wf, wi, ti, cursor).await?;
    assert!(page.batches.is_empty(), "nothing new → empty page");

    // New content plus the marker land past the cursor; the next poll sees
    // exactly them — never the already-shipped batches.
    stream
        .accept(submission(&stream, 3, b"four\n".to_vec()))
        .await?;
    stream.finish_cleanly(LogExit::Status(0)).await?;

    let page = resolver.fetch_batches_after(wf, wi, ti, cursor).await?;
    assert_eq!(page.batches.len(), 1, "only the batch after the cursor");
    assert_eq!(page.batches[0].bytes, b"four\n");
    assert_eq!(
        page.marker
            .expect("marker crosses with this poll")
            .exit_status,
        0
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail_reads_page_backwards() -> Result<(), Box<dyn std::error::Error>> {
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator();
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    let mut stream = open_log_stream(&nats, wf, wi, ti).await?;
    for i in 1..=5u8 {
        stream
            .accept(submission(&stream, u64::from(i - 1), vec![b'0' + i, b'\n']))
            .await?;
    }

    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));

    // Tail of 2 → the last two batches, with earlier content flagged.
    let page = resolver.fetch_tail(wf, wi, ti, 2, None).await?;
    assert_eq!(
        page.batches
            .iter()
            .map(|b| b.bytes.clone())
            .collect::<Vec<_>>(),
        vec![b"4\n".to_vec(), b"5\n".to_vec()]
    );
    assert!(page.has_earlier, "three earlier batches exist");
    let first_seq = page.batches[0].seq;

    // "Load earlier" pages backwards from the first shown sequence.
    let earlier = resolver.fetch_tail(wf, wi, ti, 2, Some(first_seq)).await?;
    assert_eq!(
        earlier
            .batches
            .iter()
            .map(|b| b.bytes.clone())
            .collect::<Vec<_>>(),
        vec![b"2\n".to_vec(), b"3\n".to_vec()]
    );
    assert!(earlier.has_earlier, "batch 1 still earlier");

    let earliest = resolver
        .fetch_tail(wf, wi, ti, 2, Some(earlier.batches[0].seq))
        .await?;
    assert_eq!(earliest.batches.len(), 1);
    assert!(!earliest.has_earlier, "nothing before the first batch");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archived_marker_reported_from_sidecar() -> Result<(), Box<dyn std::error::Error>> {
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator();
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    // The archived layout the compaction drain produces: gzip blob + sidecar.
    let plain = b"archived line\n";
    minio
        .write(&minio_object_key(wf, wi, ti), gzip(plain))
        .await?;
    minio
        .write(
            &exit_sidecar_key(wf, wi, ti),
            serde_json::to_vec(&serde_json::json!({"exit_status": 0}))?,
        )
        .await?;

    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));
    let logs = resolver.fetch_task_logs(wf, wi, ti).await?;
    assert_eq!(logs.content, plain);
    let marker = logs.marker.expect("sidecar marker must be reported");
    assert_eq!(
        marker.exit_status, 0,
        "archived read must report the same marker shape as a live read"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archived_marker_without_blob_is_not_a_404() -> Result<(), Box<dyn std::error::Error>> {
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator();
    let wf = Uuid::new_v4();
    let wi = Uuid::new_v4();
    let ti = Uuid::new_v4();

    // A task that never logged a line but exited cleanly archives a sidecar
    // and no blob.
    minio
        .write(
            &exit_sidecar_key(wf, wi, ti),
            serde_json::to_vec(&serde_json::json!({"exit_status": 0}))?,
        )
        .await?;

    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));
    let logs = resolver.fetch_task_logs(wf, wi, ti).await?;
    assert!(logs.content.is_empty(), "no blob → empty content");
    assert_eq!(logs.marker.expect("marker").exit_status, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returns_not_found_when_both_stores_empty() -> Result<(), Box<dyn std::error::Error>> {
    let (_container, nats) = match start_nats_with_jetstream().await {
        Some(p) => p,
        None => return Ok(()),
    };
    create_log_stream(&nats).await;

    let minio = memory_operator(); // empty
    let resolver = LogsResolver::new(minio, log_stream_provider(&nats));

    let err = resolver
        .fetch_task_logs(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4())
        .await
        .expect_err("both stores empty must yield NotFound");
    assert!(matches!(err, LogsError::NotFound), "got {:?}", err);
    Ok(())
}
