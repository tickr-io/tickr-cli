//! The per-Task-instance log path, behind one deep module.
//!
//! Governing invariant: **the workload is sacred; telemetry is best-effort.**
//! The log path may never block, back-pressure, or crash the task it observes,
//! and may never block that task's completion. Under sustained saturation it
//! drops rather than stalls.
//!
//! Shape (two verbs):
//! - [`TaskLogShipper::start`] takes the child's stdout and spawns a *dumb*
//!   drain (read-line → enqueue, never touching the publish path) plus a
//!   single-flight in-order publisher.
//! - [`TaskLogShipper::finish`] is called by the orchestration *after* it has
//!   already observed the process exit and sent the terminal status update; it
//!   flushes the backlog (bounded by a safety-cap deadline), publishes the
//!   End-of-stream marker last, and tears down.
//!
//! The drain can never block on the publish path, so the OS stdout pipe stays
//! near-empty and the task process can never be back-pressured into a blocked
//! write. Drops happen only at the in-memory floor (a full buffer evicts its
//! oldest line), recorded as a dropped-line *count* on a batch header — never
//! as an in-band log line, because log text is arbitrary and an in-band
//! sentinel could be forged.

use anyhow::{anyhow, Result};
use async_nats::jetstream::{self, stream};
use async_nats::Client as NatsClient;
use async_nats::HeaderMap;
use async_trait::async_trait;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader as StdBufReader, Seek, SeekFrom, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::ChildStdout;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::bytes::Bytes;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// JetStream stream that stages task-log batches until compaction archives
/// them. Mirrored by the conductor's log-upload step and the API's logs
/// resolver — the three must agree on stream name and subject shape. This
/// module is the single owner of that "must all agree" coupling on the
/// executor side.
const LOG_STREAM_NAME: &str = "tickr_task_logs";

/// Wildcard the stream binds. Each task instance gets one subject:
/// `logs.<workflow_id>.<workflow_instance_id>.<task_instance_id>`. No
/// attempt segment — every attempt is its own TaskInstance with its own id.
const LOG_STREAM_SUBJECTS: &str = "logs.>";

/// In-flight log volume is bounded by stream retention. This cap is the
/// generous backstop that keeps a runaway producer from exhausting the
/// JetStream store.
const LOG_STREAM_MAX_BYTES: i64 = 8 * 1024 * 1024 * 1024;

/// JetStream message-dedup window. Set explicitly (rather than falling back to
/// JetStream's ~2-min default) so a batch retried after an ambiguous ack
/// timeout still dedups: the window must comfortably exceed the maximum span
/// over which the publisher re-offers the same `Nats-Msg-Id`.
const LOG_STREAM_DEDUP_WINDOW: Duration = Duration::from_secs(120);

/// JetStream message-id header. Carrying a deterministic id makes a publish
/// retried after an ambiguous timeout idempotent — the stream never accrues a
/// duplicate batch.
const NATS_MSG_ID_HEADER: &str = "Nats-Msg-Id";

/// Batch header carrying the count of log lines dropped (oldest-first) at the
/// buffer floor since the previous batch. A header — never an in-band line —
/// because log content is arbitrary and an in-band sentinel could be forged.
const DROPPED_LINES_HEADER: &str = "Tickr-Log-Dropped";

/// Header marking a message as the End-of-stream marker rather than log text.
const MARKER_HEADER: &str = "Tickr-Log-Marker";
const MARKER_HEADER_VALUE: &str = "end-of-stream";
const MARKER_EXIT_STATUS_HEADER: &str = "Tickr-Exit-Status";
const MARKER_EXIT_REASON_HEADER: &str = "Tickr-Exit-Reason";

/// Header on the End-of-stream marker indicating the stream was truncated: the
/// safety-cap deadline elapsed before the backlog finished shipping, so an
/// un-shipped tail was dropped. Its absence means the stream ended cleanly —
/// keeping the drop-honesty invariant uniform (an un-annotated stream is
/// complete).
const MARKER_INCOMPLETE_HEADER: &str = "Tickr-Log-Incomplete";
const MARKER_INCOMPLETE_VALUE: &str = "tail-dropped";

/// Subject a task instance's log batches are published to.
fn log_subject(workflow_id: &Uuid, workflow_instance_id: &Uuid, task_instance_id: &Uuid) -> String {
    format!(
        "logs.{}.{}.{}",
        workflow_id, workflow_instance_id, task_instance_id
    )
}

/// How a task execution ended, from the executor's point of view. Each variant
/// is one controlled exit path; every one produces exactly one End-of-stream
/// marker. An executor crash produces none — that absence is the abnormal-end
/// signal consumers infer from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskExit {
    /// The task process exited with a status code (zero or not).
    Status(i32),
    /// The task process terminated without a status code (killed by signal).
    NoStatus,
    /// The executor failed to run the task at all (spawn / IO error).
    Error(String),
}

/// Build the End-of-stream marker headers for an exit. Pure so every
/// controlled exit path's marker content is unit-testable. The truncation
/// indicator is added separately by the sink (it is a property of *how the
/// flush ended*, not of the exit), so this stays a pure function of the exit.
fn end_of_stream_headers(exit: &TaskExit) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(MARKER_HEADER, MARKER_HEADER_VALUE);
    match exit {
        TaskExit::Status(code) => {
            headers.insert(MARKER_EXIT_STATUS_HEADER, code.to_string().as_str());
        }
        TaskExit::NoStatus => {
            headers.insert(MARKER_EXIT_STATUS_HEADER, "-1");
            headers.insert(MARKER_EXIT_REASON_HEADER, "terminated without exit status");
        }
        TaskExit::Error(reason) => {
            headers.insert(MARKER_EXIT_STATUS_HEADER, "-1");
            // Header values must be a single line; log text stays in the
            // stream's batches, only a one-line reason rides the marker.
            let one_line: String = reason.replace(['\r', '\n'], " ");
            headers.insert(MARKER_EXIT_REASON_HEADER, one_line.as_str());
        }
    }
    headers
}

/// The log subject identity for one Task instance's run.
#[derive(Clone)]
pub struct LogIdentity {
    pub workflow_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub task_instance_id: Uuid,
}

impl LogIdentity {
    pub fn subject(&self) -> String {
        log_subject(
            &self.workflow_id,
            &self.workflow_instance_id,
            &self.task_instance_id,
        )
    }
}

/// Tunable buffer/batch/publish parameters. `from_env` lets a deployment size
/// them without a rebuild.
#[derive(Clone, Debug)]
pub struct ShipperConfig {
    /// In-memory line capacity. A full buffer evicts its oldest line (the
    /// floor drop) rather than back-pressuring the drain.
    pub buffer_capacity: usize,
    /// Flush a batch once this many lines have accumulated…
    pub batch_max_lines: usize,
    /// …or once this much time has elapsed with at least one line buffered,
    /// whichever comes first.
    pub batch_interval: Duration,
    /// Per-publish acknowledgement timeout. On timeout the head batch is
    /// retried, never dropped.
    pub publish_timeout: Duration,
    /// Cap on the publisher's exponential retry backoff.
    pub publish_backoff_max: Duration,
    /// Generous bound on the post-completion flush. On a multi-minute outage
    /// (or executor shutdown) the shipper publishes the marker with the known
    /// exit status and tears down, dropping any still-unshipped tail.
    pub flush_deadline: Duration,
    /// Directory for transient per-task spill files. A sustained stream
    /// slowdown overflows the in-memory channel into a spill file here rather
    /// than dropping; the file is deleted when the shipper finishes.
    pub spill_dir: PathBuf,
    /// Disk floor: the maximum number of lines held in a task's spill file.
    /// Beyond this (a pathological flood) the oldest spilled line is dropped,
    /// annotated with a dropped-line count exactly like the in-memory floor.
    pub spill_max_lines: usize,
}

impl Default for ShipperConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 10_000,
            batch_max_lines: 256,
            batch_interval: Duration::from_secs(1),
            publish_timeout: Duration::from_secs(5),
            publish_backoff_max: Duration::from_secs(5),
            flush_deadline: Duration::from_secs(120),
            spill_dir: std::env::temp_dir().join("tickr-task-logs"),
            spill_max_lines: 1_000_000,
        }
    }
}

impl ShipperConfig {
    /// Read overrides from the environment, falling back to [`Default`].
    pub fn from_env() -> Self {
        let d = Self::default();
        fn usize_env(key: &str, default: usize) -> usize {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }
        fn ms_env(key: &str, default: Duration) -> Duration {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .map(Duration::from_millis)
                .unwrap_or(default)
        }
        Self {
            buffer_capacity: usize_env("TICKR_LOG_BUFFER_CAPACITY", d.buffer_capacity),
            batch_max_lines: usize_env("TICKR_LOG_BATCH_MAX_LINES", d.batch_max_lines),
            batch_interval: ms_env("TICKR_LOG_BATCH_INTERVAL_MS", d.batch_interval),
            publish_timeout: ms_env("TICKR_LOG_PUBLISH_TIMEOUT_MS", d.publish_timeout),
            publish_backoff_max: ms_env("TICKR_LOG_PUBLISH_BACKOFF_MAX_MS", d.publish_backoff_max),
            flush_deadline: ms_env("TICKR_LOG_FLUSH_DEADLINE_MS", d.flush_deadline),
            spill_dir: std::env::var_os("TICKR_LOG_SPILL_DIR")
                .map(PathBuf::from)
                .unwrap_or(d.spill_dir),
            spill_max_lines: usize_env("TICKR_LOG_SPILL_MAX_LINES", d.spill_max_lines),
        }
    }
}

/// One batch of log lines plus the count of lines dropped at the floor since
/// the previous batch.
#[derive(Debug, PartialEq, Eq)]
struct Batch {
    lines: Vec<String>,
    dropped: u64,
}

impl Batch {
    /// Wire payload: lines joined and newline-terminated so concatenating
    /// batches at read time preserves the line boundary at the joint.
    fn payload(&self) -> Bytes {
        if self.lines.is_empty() {
            return Bytes::new();
        }
        let mut s = self.lines.join("\n");
        s.push('\n');
        Bytes::from(s.into_bytes())
    }
}

/// Shared lifecycle flags between the drain, the publisher, and `finish`.
struct ShipperState {
    /// Set by `finish`: the publisher should flush the buffer and then stop.
    finishing: AtomicBool,
    /// Set by the drain when it ends (EOF / read error / abort): no further
    /// lines will be enqueued. The publisher only concludes it is *done* once
    /// `finishing && drain_done`, so it never declares completion while a line
    /// the drain just read is still in flight to the buffer.
    drain_done: AtomicBool,
}

/// Filename prefix marking a transient task-log spill file. The startup sweep
/// matches on it to clean orphans a prior crash left behind.
const SPILL_FILE_PREFIX: &str = "tickr-tasklog-";

/// The transient on-disk tier: a per-Task-instance newline-delimited file that
/// absorbs a sustained stream slowdown without consuming RAM. It is a FIFO of
/// lines — append at the tail, read oldest from the head — and is **transient
/// and never recovered**: deleted when the shipper finishes (via `Drop`), and
/// on a crash it is simply orphaned and swept at the next startup, never read.
/// The executor holds no state that survives a task or a restart, so the spill
/// is scratch a crash may lose, never a durable spool.
struct Spill {
    path: PathBuf,
    file: File,
    /// Byte offset of the oldest un-read line (the head).
    read_pos: u64,
    /// Byte offset one past the newest line (the tail / append point).
    write_pos: u64,
    /// Live (un-read) line count.
    live_lines: usize,
}

impl Spill {
    fn create(path: PathBuf) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            read_pos: 0,
            write_pos: 0,
            live_lines: 0,
        })
    }

    /// Append one line at the tail.
    fn push(&mut self, line: &str) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(self.write_pos))?;
        self.file.write_all(line.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.write_pos += line.len() as u64 + 1;
        self.live_lines += 1;
        Ok(())
    }

    /// Read and remove the oldest line from the head, preserving emission order.
    /// When the spill drains empty, the file is truncated so it does not grow
    /// across drain cycles.
    fn pop(&mut self) -> std::io::Result<Option<String>> {
        if self.read_pos >= self.write_pos {
            return Ok(None);
        }
        self.file.seek(SeekFrom::Start(self.read_pos))?;
        let mut reader = StdBufReader::new(&self.file);
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            return Ok(None);
        }
        self.read_pos += n as u64;
        self.live_lines -= 1;
        if line.ends_with('\n') {
            line.pop();
        }
        if self.read_pos >= self.write_pos {
            // Fully drained: reset to the start and reclaim the file space.
            self.read_pos = 0;
            self.write_pos = 0;
            self.file.set_len(0)?;
        }
        Ok(Some(line))
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        // Transient-and-non-recovered: the spill never outlives its shipper.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Sweep stale spill files orphaned by a prior crash. Orphans are never read —
/// only deleted — so the executor's first use of local disk does not accrete
/// across crashes. Best-effort: an unreadable directory or undeletable file is
/// logged and skipped, never fatal.
pub fn sweep_spill_dir(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        // Nothing to sweep if the dir was never created.
        Err(_) => return,
    };
    let mut swept = 0u32;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(SPILL_FILE_PREFIX) {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => swept += 1,
                Err(e) => eprintln!("Failed to sweep stale spill file {name}: {e}"),
            }
        }
    }
    if swept > 0 {
        println!("Swept {swept} stale task-log spill file(s) from {dir:?}");
    }
}

/// The in-memory tier (a bounded line buffer) plus its transient on-disk spill
/// overflow, plus the size-or-time batcher. Self-contained and
/// isolation-testable — no NATS, no subprocess.
///
/// Tier ladder: in-memory bounded channel → transient on-disk spill file →
/// drop the oldest overflow **only at the disk floor** (the pathological-flood
/// rung). Without a spill configured the floor is the in-memory bound.
struct LogBuffer {
    inner: Mutex<BufInner>,
    notify: Notify,
    capacity: usize,
    batch_max_lines: usize,
    batch_interval: Duration,
    /// Where to create the spill file on first overflow (`None` disables the
    /// spill tier — the floor is then the in-memory bound).
    spill_path: Option<PathBuf>,
    /// Disk floor for the spill file.
    spill_max_lines: usize,
}

struct BufInner {
    lines: VecDeque<String>,
    /// Lazily created on first overflow so a task that never saturates touches
    /// no disk at all.
    spill: Option<Spill>,
    /// Lines evicted at the floor since the last batch was taken.
    dropped: u64,
}

impl LogBuffer {
    /// In-memory only: overflow drops at the in-memory bound. Used by the
    /// in-memory-tier tests; production always configures a spill.
    #[cfg(test)]
    fn new(cfg: &ShipperConfig) -> Arc<Self> {
        Self::build(cfg, None)
    }

    /// With a transient on-disk spill tier: overflow spills to `spill_path` and
    /// only drops at the disk floor (`spill_max_lines`).
    fn with_spill(cfg: &ShipperConfig, spill_path: PathBuf) -> Arc<Self> {
        Self::build(cfg, Some(spill_path))
    }

    fn build(cfg: &ShipperConfig, spill_path: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BufInner {
                lines: VecDeque::new(),
                spill: None,
                dropped: 0,
            }),
            notify: Notify::new(),
            capacity: cfg.buffer_capacity.max(1),
            batch_max_lines: cfg.batch_max_lines.max(1),
            batch_interval: cfg.batch_interval,
            spill_path,
            spill_max_lines: cfg.spill_max_lines.max(1),
        })
    }

    /// Total un-shipped lines across both tiers.
    fn buffered_len(g: &BufInner) -> usize {
        g.lines.len() + g.spill.as_ref().map_or(0, |s| s.live_lines)
    }

    /// Enqueue one line. Never blocks. When the in-memory tier is full the
    /// oldest in-memory line moves to the transient spill file (preserving
    /// emission order — the spill always holds lines older than memory). A line
    /// is dropped only at the disk floor (or, with no spill configured, at the
    /// in-memory bound). A spill IO error degrades to an in-memory floor drop —
    /// telemetry is best-effort and never blocks or fails the task.
    fn push(&self, line: String) {
        {
            let mut g = self.inner.lock().unwrap();
            if g.lines.len() >= self.capacity {
                let oldest = g.lines.pop_front().expect("capacity >= 1");
                if !self.spill_oldest(&mut g, oldest) {
                    // No spill (or it failed): this is the in-memory floor drop.
                    g.dropped += 1;
                }
            }
            g.lines.push_back(line);
        }
        self.notify.notify_one();
    }

    /// Move one evicted in-memory line down into the spill tier, creating the
    /// spill file lazily on first overflow. Drops the spill's oldest line at the
    /// disk floor first (annotated). Returns `false` if no spill is configured
    /// or an IO error prevented spilling — the caller then takes the in-memory
    /// floor drop instead.
    fn spill_oldest(&self, g: &mut BufInner, line: String) -> bool {
        let Some(path) = &self.spill_path else {
            return false;
        };
        if g.spill.is_none() {
            // Ensure the directory exists, then create the file lazily.
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match Spill::create(path.clone()) {
                Ok(s) => g.spill = Some(s),
                Err(e) => {
                    eprintln!("Failed to create spill file {path:?}: {e}");
                    return false;
                }
            }
        }
        let spill = g.spill.as_mut().expect("spill created above");
        // Disk floor: drop the oldest spilled line to make room (pathological
        // flood only).
        if spill.live_lines >= self.spill_max_lines {
            match spill.pop() {
                Ok(Some(_)) => g.dropped += 1,
                Ok(None) => {}
                Err(e) => {
                    eprintln!("Spill floor-drop failed: {e}");
                    return false;
                }
            }
        }
        match spill.push(&line) {
            Ok(()) => true,
            Err(e) => {
                eprintln!("Spill append failed: {e}");
                false
            }
        }
    }

    /// Wake any waiting consumer (used when the drain ends so the publisher can
    /// observe `drain_done`).
    fn poke(&self) {
        self.notify.notify_one();
    }

    /// Drain up to `batch_max_lines` lines and the accumulated dropped count,
    /// oldest-first across the tiers: the spill (older) drains before the
    /// in-memory tier (newer), so batches preserve emission order across the
    /// memory → spill → memory transition.
    fn drain_locked(&self, g: &mut BufInner) -> Batch {
        let mut lines = Vec::new();
        while lines.len() < self.batch_max_lines {
            // Spill holds the oldest lines — read those first.
            if let Some(spill) = g.spill.as_mut() {
                match spill.pop() {
                    Ok(Some(l)) => {
                        lines.push(l);
                        continue;
                    }
                    Ok(None) => {}
                    Err(e) => eprintln!("Spill read failed, skipping spilled tail: {e}"),
                }
            }
            match g.lines.pop_front() {
                Some(l) => lines.push(l),
                None => break,
            }
        }
        let dropped = std::mem::take(&mut g.dropped);
        Batch { lines, dropped }
    }

    /// Block until a batch is ready to flush — size threshold reached, time
    /// threshold elapsed with something buffered, or `finishing` set with
    /// something buffered (flush partials eagerly). Returns `None` only once
    /// the input is closed (`finishing && drain_done`) and nothing remains —
    /// the publisher's termination condition.
    async fn next_batch(&self, state: &ShipperState) -> Option<Batch> {
        loop {
            let wait = {
                let mut g = self.inner.lock().unwrap();
                let finishing = state.finishing.load(Ordering::Acquire);
                let buffered = Self::buffered_len(&g);
                let have = buffered > 0 || g.dropped > 0;
                if buffered >= self.batch_max_lines || (finishing && have) {
                    return Some(self.drain_locked(&mut g));
                }
                if finishing && state.drain_done.load(Ordering::Acquire) && !have {
                    return None;
                }
                if have {
                    // Something buffered but below the size threshold: flush it
                    // after at most one batch interval (time-based flush).
                    Some(self.batch_interval)
                } else {
                    // Nothing buffered: wait for a push (or a drain-done poke).
                    None
                }
            };
            match wait {
                Some(interval) => {
                    if timeout(interval, self.notify.notified()).await.is_err() {
                        // Interval elapsed first → force-flush the partial batch.
                        let mut g = self.inner.lock().unwrap();
                        if Self::buffered_len(&g) > 0 || g.dropped > 0 {
                            return Some(self.drain_locked(&mut g));
                        }
                    }
                }
                None => {
                    self.notify.notified().await;
                }
            }
        }
    }
}

/// The publish seam. Single-flight and in-order is enforced by the publisher
/// loop (one batch awaited before the next is offered); the trait only has to
/// publish a batch or the marker. Kept minimal — not a 1:1 mirror of the
/// JetStream client — so it stays a deep seam with one real adapter and one
/// test fake.
#[async_trait]
pub trait LogBatchSink: Send + Sync {
    /// Publish one log batch. `msg_id` is the JetStream dedup id, stable across
    /// retries of the same batch. `dropped` is the floor-drop count since the
    /// previous batch, carried as a header. Returns `Err` on timeout/error so
    /// the publisher retries the *same* batch (never drops it).
    async fn publish_batch(
        &self,
        subject: &str,
        msg_id: &str,
        dropped: u64,
        payload: Bytes,
    ) -> Result<()>;

    /// Publish the End-of-stream marker — the truly-last message on the
    /// subject. `tail_dropped` marks a truncated stream so a reader can tell a
    /// clean end from a safety-cap cut.
    async fn publish_marker(
        &self,
        subject: &str,
        exit: &TaskExit,
        tail_dropped: bool,
    ) -> Result<()>;
}

/// The real adapter: wraps a JetStream context and is the consolidation home
/// for the subject shape, the dedup-id / dropped-count / marker headers, the
/// marker publish, and (via [`ensure_log_stream`]) the boot-time stream-ensure.
pub struct JetStreamSink {
    js: Arc<jetstream::Context>,
    publish_timeout: Duration,
}

impl JetStreamSink {
    pub fn new(js: Arc<jetstream::Context>, publish_timeout: Duration) -> Self {
        Self {
            js,
            publish_timeout,
        }
    }

    /// Publish `payload` with `headers` and await the ack, bounding the ack on
    /// `publish_timeout` so a stalled acknowledgement surfaces as an error the
    /// publisher retries — it never parks the publisher.
    async fn publish_awaited(
        &self,
        subject: String,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Result<()> {
        let ack = self
            .js
            .publish_with_headers(subject, headers, payload)
            .await
            .map_err(|e| anyhow!("publish: {e}"))?;
        timeout(self.publish_timeout, ack)
            .await
            .map_err(|_| anyhow!("ack timed out"))?
            .map_err(|e| anyhow!("ack: {e}"))?;
        Ok(())
    }
}

#[async_trait]
impl LogBatchSink for JetStreamSink {
    async fn publish_batch(
        &self,
        subject: &str,
        msg_id: &str,
        dropped: u64,
        payload: Bytes,
    ) -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MSG_ID_HEADER, msg_id);
        if dropped > 0 {
            headers.insert(DROPPED_LINES_HEADER, dropped.to_string().as_str());
        }
        self.publish_awaited(subject.to_string(), headers, payload)
            .await
    }

    async fn publish_marker(
        &self,
        subject: &str,
        exit: &TaskExit,
        tail_dropped: bool,
    ) -> Result<()> {
        let mut headers = end_of_stream_headers(exit);
        if tail_dropped {
            headers.insert(MARKER_INCOMPLETE_HEADER, MARKER_INCOMPLETE_VALUE);
        }
        self.publish_awaited(subject.to_string(), headers, Bytes::new())
            .await
    }
}

/// A sink that discards every batch and marker. Used when the Log staging
/// stream is unavailable: the drain still empties the stdout pipe (the workload
/// must keep running at full speed), the logs are simply not shipped.
pub struct DiscardSink;

#[async_trait]
impl LogBatchSink for DiscardSink {
    async fn publish_batch(&self, _: &str, _: &str, _: u64, _: Bytes) -> Result<()> {
        Ok(())
    }
    async fn publish_marker(&self, _: &str, _: &TaskExit, _: bool) -> Result<()> {
        Ok(())
    }
}

/// Ensure the Log staging stream exists and return a JetStream context. The
/// dedup window is set explicitly so retried batches dedup. Idempotent — an
/// existing stream is reused. Invoked once at executor startup.
pub async fn ensure_log_stream(nats: &NatsClient) -> Result<Arc<jetstream::Context>> {
    let js = jetstream::new(nats.clone());
    js.get_or_create_stream(stream::Config {
        name: LOG_STREAM_NAME.to_string(),
        subjects: vec![LOG_STREAM_SUBJECTS.to_string()],
        max_bytes: LOG_STREAM_MAX_BYTES,
        duplicate_window: LOG_STREAM_DEDUP_WINDOW,
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow!("Failed to get_or_create log stream: {e}"))?;
    Ok(Arc::new(js))
}

/// Read every line from `stdout` and enqueue it. A dumb forwarder: its only
/// job is `next_line() → push`, so it can never block on the publish path. A
/// read error is *not* a task failure — completion is observed independently
/// via `child.wait()`, and a telemetry fault must not flip a workload's
/// outcome — so the drain simply ends.
async fn run_drain(stdout: ChildStdout, buffer: Arc<LogBuffer>, state: Arc<ShipperState>) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => buffer.push(line),
            Ok(None) => break, // EOF
            Err(_) => break,   // read error: telemetry fault, never fails the task
        }
    }
    state.drain_done.store(true, Ordering::Release);
    buffer.poke();
}

/// Single-flight, in-order publisher: pop the head batch, publish, await the
/// ack, advance. On timeout/error the head batch is kept and retried with
/// capped backoff — never dropped (the buffer absorbs everything behind it).
/// Each batch's `Nats-Msg-Id` is `<nonce>-<seq>`; the seq is fixed when the
/// batch is formed, so a retry re-offers the *same* id and dedups.
async fn run_publisher(
    subject: String,
    sink: Arc<dyn LogBatchSink>,
    buffer: Arc<LogBuffer>,
    state: Arc<ShipperState>,
    nonce: Uuid,
    backoff_max: Duration,
) {
    let mut seq: u64 = 0;
    while let Some(batch) = buffer.next_batch(&state).await {
        let msg_id = format!("{nonce}-{seq}");
        let payload = batch.payload();
        let mut backoff = Duration::from_millis(50);
        loop {
            match sink
                .publish_batch(&subject, &msg_id, batch.dropped, payload.clone())
                .await
            {
                Ok(()) => break,
                Err(_) => {
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(backoff_max);
                }
            }
        }
        seq += 1;
    }
}

/// The per-Task-instance log shipper. See the module docs for the invariant.
pub struct TaskLogShipper {
    subject: String,
    sink: Arc<dyn LogBatchSink>,
    state: Arc<ShipperState>,
    buffer: Arc<LogBuffer>,
    drain_handle: JoinHandle<()>,
    publisher_handle: JoinHandle<()>,
    flush_deadline: Duration,
}

impl TaskLogShipper {
    /// Begin draining `stdout` and shipping its batches. Returns immediately;
    /// the drain and publisher run as background tasks.
    pub fn start(
        identity: LogIdentity,
        sink: Arc<dyn LogBatchSink>,
        cfg: &ShipperConfig,
        stdout: ChildStdout,
    ) -> Self {
        let subject = identity.subject();
        let state = Arc::new(ShipperState {
            finishing: AtomicBool::new(false),
            drain_done: AtomicBool::new(false),
        });
        // A fresh nonce *per shipper start* (not per Task-instance id) is
        // load-bearing: a parked loop Task instance is re-delivered as the same
        // task_instance_id across turns, concatenating all turns onto the one
        // subject. Keying the dedup id on task-instance-id + seq would give
        // turn 2's batch 0 the same id as turn 1's — and since turns cycle
        // inside the dedup window, the stream would silently drop every later
        // turn's colliding batches, losing all but the first turn's logs. A
        // fresh per-start nonce makes cross-turn batches distinct.
        let nonce = Uuid::new_v4();

        // The spill file is per-Task-instance *and* per-start (the nonce), so a
        // re-delivered loop turn never reuses a prior turn's file. It is created
        // lazily on first overflow — a task that never saturates touches no disk.
        let spill_path = cfg.spill_dir.join(format!(
            "{SPILL_FILE_PREFIX}{}-{}.spill",
            identity.task_instance_id, nonce
        ));
        let buffer = LogBuffer::with_spill(cfg, spill_path);

        let drain_handle = tokio::spawn(run_drain(stdout, buffer.clone(), state.clone()));
        let publisher_handle = tokio::spawn(run_publisher(
            subject.clone(),
            sink.clone(),
            buffer.clone(),
            state.clone(),
            nonce,
            cfg.publish_backoff_max,
        ));

        Self {
            subject,
            sink,
            state,
            buffer,
            drain_handle,
            publisher_handle,
            flush_deadline: cfg.flush_deadline,
        }
    }

    /// Drain the remainder, flush the backlog, publish the End-of-stream marker
    /// last, and tear down. Bounded: if the flush does not complete within
    /// `flush_deadline` (a multi-minute outage) or the executor is shutting
    /// down, it publishes the marker with the known exit status — flagged
    /// tail-dropped — and tears down, dropping any still-unshipped tail.
    ///
    /// Called by the orchestration *after* the terminal status update has
    /// already been sent, so the marker simply trails it and still means
    /// "truly the last batch."
    pub async fn finish(self, exit: TaskExit, shutdown: &CancellationToken) {
        let TaskLogShipper {
            subject,
            sink,
            state,
            buffer,
            drain_handle,
            mut publisher_handle,
            flush_deadline,
        } = self;

        // Tell the publisher to flush the buffer and stop once drained.
        state.finishing.store(true, Ordering::Release);
        buffer.poke();

        // Wait for the publisher to drain the backlog, bounded by the
        // safety-cap deadline and by executor shutdown. The publisher finishes
        // only after the drain has reached EOF (`drain_done`) and the buffer is
        // empty, so a clean finish ships every buffered line. A lingering
        // descendant holding stdout open keeps the drain from reaching EOF —
        // that case is bounded here and resolves to a tail-dropped marker.
        let mut publisher_done = false;
        let tail_dropped = tokio::select! {
            _ = &mut publisher_handle => { publisher_done = true; false }
            _ = sleep(flush_deadline) => true,
            _ = shutdown.cancelled() => true,
        };

        drain_handle.abort();
        let _ = drain_handle.await;
        // Only abort/join the publisher if it didn't already finish — awaiting a
        // JoinHandle that the select already drove to completion would panic.
        if !publisher_done {
            publisher_handle.abort();
            let _ = publisher_handle.await;
        }

        // The marker is published last — after the publisher has stopped — so
        // it remains the truly-last message on the subject. A publish failure
        // is swallowed: its absence is itself the abnormal-end signal consumers
        // infer from, and the task's outcome must not flip on a telemetry fault.
        if let Err(e) = sink.publish_marker(&subject, &exit, tail_dropped).await {
            eprintln!("End-of-stream marker not published for {subject}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex as StdMutex;

    fn header(headers: &HeaderMap, name: &str) -> Option<String> {
        headers.get(name).map(|v| v.as_str().to_string())
    }

    fn test_config() -> ShipperConfig {
        ShipperConfig {
            buffer_capacity: 4,
            batch_max_lines: 3,
            batch_interval: Duration::from_millis(40),
            publish_timeout: Duration::from_millis(200),
            publish_backoff_max: Duration::from_millis(20),
            flush_deadline: Duration::from_millis(300),
            spill_dir: std::env::temp_dir(),
            spill_max_lines: 1000,
        }
    }

    // ---- end_of_stream_headers (pure) ----

    #[test]
    fn marker_headers_for_clean_exit_carry_zero_status() {
        let h = end_of_stream_headers(&TaskExit::Status(0));
        assert_eq!(
            header(&h, MARKER_HEADER).as_deref(),
            Some(MARKER_HEADER_VALUE)
        );
        assert_eq!(header(&h, MARKER_EXIT_STATUS_HEADER).as_deref(), Some("0"));
        assert_eq!(header(&h, MARKER_EXIT_REASON_HEADER), None);
    }

    #[test]
    fn marker_headers_for_nonzero_exit_carry_the_code() {
        let h = end_of_stream_headers(&TaskExit::Status(2));
        assert_eq!(header(&h, MARKER_EXIT_STATUS_HEADER).as_deref(), Some("2"));
        assert_eq!(header(&h, MARKER_EXIT_REASON_HEADER), None);
    }

    #[test]
    fn marker_headers_for_signal_termination_carry_sentinel_and_reason() {
        let h = end_of_stream_headers(&TaskExit::NoStatus);
        assert_eq!(header(&h, MARKER_EXIT_STATUS_HEADER).as_deref(), Some("-1"));
        assert_eq!(
            header(&h, MARKER_EXIT_REASON_HEADER).as_deref(),
            Some("terminated without exit status")
        );
    }

    #[test]
    fn marker_headers_for_executor_error_carry_one_line_reason() {
        let h = end_of_stream_headers(&TaskExit::Error(
            "spawn failed:\nnix not found\r\non PATH".to_string(),
        ));
        assert_eq!(header(&h, MARKER_EXIT_STATUS_HEADER).as_deref(), Some("-1"));
        let reason = header(&h, MARKER_EXIT_REASON_HEADER).expect("reason header");
        assert!(
            !reason.contains('\n') && !reason.contains('\r'),
            "reason must be a single header-safe line, got {reason:?}"
        );
    }

    // ---- LogBuffer (in-memory tier) ----

    fn running_state() -> ShipperState {
        ShipperState {
            finishing: AtomicBool::new(false),
            drain_done: AtomicBool::new(false),
        }
    }

    #[tokio::test]
    async fn buffer_flushes_a_batch_on_the_size_threshold() {
        let cfg = test_config(); // batch_max_lines = 3
        let buf = LogBuffer::new(&cfg);
        let state = running_state();
        for i in 0..3 {
            buf.push(format!("line {i}"));
        }
        // A full batch is ready immediately — no need to wait for the interval.
        let batch = timeout(Duration::from_millis(10), buf.next_batch(&state))
            .await
            .expect("size threshold should flush without waiting the interval")
            .expect("a batch");
        assert_eq!(batch.lines, vec!["line 0", "line 1", "line 2"]);
        assert_eq!(batch.dropped, 0);
    }

    #[tokio::test]
    async fn buffer_flushes_a_partial_batch_on_the_time_threshold() {
        let cfg = test_config(); // batch_interval = 40ms, batch_max = 3
        let buf = LogBuffer::new(&cfg);
        let state = running_state();
        buf.push("only one".to_string());
        // Below the size threshold: it must still flush once the interval passes.
        let batch = timeout(Duration::from_millis(500), buf.next_batch(&state))
            .await
            .expect("time threshold should flush the partial batch")
            .expect("a batch");
        assert_eq!(batch.lines, vec!["only one"]);
    }

    #[tokio::test]
    async fn buffer_drops_oldest_at_the_floor_and_counts_it() {
        let cfg = test_config(); // capacity = 4
        let buf = LogBuffer::new(&cfg);
        let state = running_state();
        // Push 6 into a capacity-4 buffer: the two oldest are evicted.
        for i in 0..6 {
            buf.push(format!("line {i}"));
        }
        let mut seen = Vec::new();
        let mut dropped_total = 0;
        // Drain everything (finishing + drain_done so next_batch terminates).
        state.finishing.store(true, Ordering::Release);
        state.drain_done.store(true, Ordering::Release);
        while let Some(b) = buf.next_batch(&state).await {
            dropped_total += b.dropped;
            seen.extend(b.lines);
        }
        assert_eq!(dropped_total, 2, "two oldest lines dropped at the floor");
        assert_eq!(
            seen,
            vec!["line 2", "line 3", "line 4", "line 5"],
            "the four newest survive, oldest-first eviction"
        );
    }

    // ---- Spill tier (transient on-disk overflow) ----

    fn unique_spill_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{SPILL_FILE_PREFIX}test-{tag}-{}.spill",
            Uuid::new_v4()
        ))
    }

    async fn drain_all(buf: &LogBuffer) -> (Vec<String>, u64) {
        let state = ShipperState {
            finishing: AtomicBool::new(true),
            drain_done: AtomicBool::new(true),
        };
        let mut lines = Vec::new();
        let mut dropped = 0;
        while let Some(b) = buf.next_batch(&state).await {
            dropped += b.dropped;
            lines.extend(b.lines);
        }
        (lines, dropped)
    }

    #[tokio::test]
    async fn overflow_spills_to_disk_and_round_trips_in_emission_order() {
        // Capacity 2 in-memory, generous disk floor: a sustained overflow
        // spills rather than dropping at the channel bound, and the spill
        // round-trips in order across the memory → spill → memory transition.
        let cfg = ShipperConfig {
            buffer_capacity: 2,
            spill_max_lines: 1000,
            ..test_config()
        };
        let path = unique_spill_path("roundtrip");
        let buf = LogBuffer::with_spill(&cfg, path.clone());
        for i in 0..20 {
            buf.push(format!("line {i}"));
        }
        let (lines, dropped) = drain_all(&buf).await;
        assert_eq!(
            dropped, 0,
            "a spill absorbs overflow — no drop at the bound"
        );
        let expected: Vec<String> = (0..20).map(|i| format!("line {i}")).collect();
        assert_eq!(
            lines, expected,
            "lines ship in emission order via the spill"
        );
    }

    #[tokio::test]
    async fn drop_happens_only_at_the_disk_floor() {
        // Capacity 2 + spill floor 3 → 5 live lines fit; pushing 8 drops the 3
        // oldest, only at the disk floor, annotated.
        let cfg = ShipperConfig {
            buffer_capacity: 2,
            spill_max_lines: 3,
            ..test_config()
        };
        let path = unique_spill_path("floor");
        let buf = LogBuffer::with_spill(&cfg, path);
        for i in 0..8 {
            buf.push(format!("l{i}"));
        }
        let (lines, dropped) = drain_all(&buf).await;
        assert_eq!(dropped, 3, "three oldest dropped at the disk floor");
        assert_eq!(
            lines,
            vec!["l3", "l4", "l5", "l6", "l7"],
            "the five newest survive, oldest-first, in order"
        );
    }

    #[tokio::test]
    async fn spill_file_is_deleted_when_the_buffer_is_dropped() {
        let cfg = ShipperConfig {
            buffer_capacity: 1,
            spill_max_lines: 1000,
            ..test_config()
        };
        let path = unique_spill_path("cleanup");
        let buf = LogBuffer::with_spill(&cfg, path.clone());
        // Overflow so the spill file is actually created.
        for i in 0..5 {
            buf.push(format!("l{i}"));
        }
        assert!(path.exists(), "overflow must create the spill file");
        drop(buf);
        assert!(
            !path.exists(),
            "the transient spill is deleted when the shipper's buffer drops"
        );
    }

    #[tokio::test]
    async fn a_task_that_never_saturates_touches_no_disk() {
        let cfg = ShipperConfig {
            buffer_capacity: 100,
            ..test_config()
        };
        let path = unique_spill_path("nodisk");
        let buf = LogBuffer::with_spill(&cfg, path.clone());
        for i in 0..10 {
            buf.push(format!("l{i}"));
        }
        assert!(
            !path.exists(),
            "the spill file is created lazily — only on first overflow"
        );
        let _ = drain_all(&buf).await;
    }

    #[test]
    fn sweep_removes_orphaned_spill_files_and_leaves_others() {
        let dir = std::env::temp_dir().join(format!("tickr-sweep-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let orphan = dir.join(format!("{SPILL_FILE_PREFIX}{}.spill", Uuid::new_v4()));
        let keep = dir.join("unrelated.txt");
        std::fs::write(&orphan, b"stale crash leftover").unwrap();
        std::fs::write(&keep, b"keep me").unwrap();

        sweep_spill_dir(&dir);

        assert!(!orphan.exists(), "a stale spill file is swept");
        assert!(keep.exists(), "a non-spill file is left untouched");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sweep_on_a_missing_dir_is_a_noop() {
        // First-ever run: the spill dir doesn't exist yet. Sweep must not panic.
        let missing = std::env::temp_dir().join(format!("tickr-absent-{}", Uuid::new_v4()));
        sweep_spill_dir(&missing);
    }

    // ---- Publisher against a fake sink ----

    #[derive(Default)]
    struct FakeInner {
        /// msg_ids the "server" has accepted (JetStream dedup emulation).
        seen: HashSet<String>,
        /// Batches actually stored, in order: (msg_id, payload, dropped).
        stored: Vec<(String, Vec<u8>, u64)>,
        /// Remaining calls to fail with an ack-timeout *after* the server has
        /// already accepted the message (the ambiguous-timeout case).
        fail_after_accept: usize,
        /// Markers published: (exit, tail_dropped).
        markers: Vec<(TaskExit, bool)>,
    }

    #[derive(Default)]
    struct FakeSink {
        inner: StdMutex<FakeInner>,
    }

    #[async_trait]
    impl LogBatchSink for FakeSink {
        async fn publish_batch(
            &self,
            _subject: &str,
            msg_id: &str,
            dropped: u64,
            payload: Bytes,
        ) -> Result<()> {
            let mut g = self.inner.lock().unwrap();
            // JetStream dedup: a msg_id already accepted is stored once.
            if g.seen.insert(msg_id.to_string()) {
                g.stored
                    .push((msg_id.to_string(), payload.to_vec(), dropped));
            }
            // Simulate "server accepted, client ack timed out": the store above
            // already happened, but we report an error so the publisher retries.
            if g.fail_after_accept > 0 {
                g.fail_after_accept -= 1;
                return Err(anyhow!("ack timed out"));
            }
            Ok(())
        }

        async fn publish_marker(
            &self,
            _subject: &str,
            exit: &TaskExit,
            tail_dropped: bool,
        ) -> Result<()> {
            self.inner
                .lock()
                .unwrap()
                .markers
                .push((exit.clone(), tail_dropped));
            Ok(())
        }
    }

    /// Drive the publisher to completion over an already-filled buffer.
    async fn run_publisher_to_completion(sink: Arc<FakeSink>, buffer: Arc<LogBuffer>, nonce: Uuid) {
        let state = Arc::new(ShipperState {
            finishing: AtomicBool::new(true),
            drain_done: AtomicBool::new(true),
        });
        buffer.poke();
        run_publisher(
            "logs.test".to_string(),
            sink.clone(),
            buffer,
            state,
            nonce,
            Duration::from_millis(5),
        )
        .await;
    }

    #[tokio::test]
    async fn publisher_offers_batches_strictly_in_order() {
        // Capacity comfortably above the line count so nothing drops at the
        // floor — this test is about ordering and seq, not eviction.
        let cfg = ShipperConfig {
            buffer_capacity: 100,
            ..test_config()
        }; // batch_max = 3
        let buf = LogBuffer::new(&cfg);
        for i in 0..6 {
            buf.push(format!("l{i}"));
        }
        let sink = Arc::new(FakeSink::default());
        let nonce = Uuid::new_v4();
        run_publisher_to_completion(sink.clone(), buf, nonce).await;

        let g = sink.inner.lock().unwrap();
        let ids: Vec<&str> = g.stored.iter().map(|(id, ..)| id.as_str()).collect();
        assert_eq!(
            ids,
            vec![format!("{nonce}-0"), format!("{nonce}-1")],
            "batches publish in order with a monotonic seq"
        );
        assert_eq!(g.stored[0].1, b"l0\nl1\nl2\n");
        assert_eq!(g.stored[1].1, b"l3\nl4\nl5\n");
    }

    #[tokio::test]
    async fn publisher_retries_a_timed_out_batch_without_duplicating_it() {
        // The server accepts the batch but the ack times out client-side; the
        // publisher retries the same msg_id. The stream must not accrue a
        // duplicate — idempotency via the stable Nats-Msg-Id.
        let cfg = test_config();
        let buf = LogBuffer::new(&cfg);
        buf.push("a".to_string());
        buf.push("b".to_string());
        let sink = Arc::new(FakeSink::default());
        sink.inner.lock().unwrap().fail_after_accept = 1; // first attempt "times out"
        let nonce = Uuid::new_v4();
        run_publisher_to_completion(sink.clone(), buf, nonce).await;

        let g = sink.inner.lock().unwrap();
        assert_eq!(
            g.stored.len(),
            1,
            "the retried batch is stored exactly once (idempotent)"
        );
        assert_eq!(g.stored[0].0, format!("{nonce}-0"));
        assert_eq!(g.stored[0].1, b"a\nb\n");
    }

    #[tokio::test]
    async fn publisher_carries_the_floor_drop_count_as_a_batch_field() {
        let cfg = test_config(); // capacity 4
        let buf = LogBuffer::new(&cfg);
        for i in 0..6 {
            buf.push(format!("l{i}"));
        }
        let sink = Arc::new(FakeSink::default());
        run_publisher_to_completion(sink.clone(), buf, Uuid::new_v4()).await;

        let g = sink.inner.lock().unwrap();
        let dropped_total: u64 = g.stored.iter().map(|(.., d)| *d).sum();
        assert_eq!(dropped_total, 2, "the two floor-dropped lines are reported");
    }

    #[tokio::test]
    async fn a_looping_instance_ships_every_turns_logs_not_just_the_first() {
        // A parked loop Task instance is re-delivered as the same
        // task_instance_id across turns, so every turn's batches concatenate
        // onto the one subject. A fresh shipper (fresh nonce) per turn must make
        // each turn's batches distinct — otherwise turn 2's batch 0 collides
        // with turn 1's batch 0 inside the dedup window and is silently dropped,
        // losing all but the first turn's logs.
        let cfg = test_config();
        // One shared sink — the same subject across turns — that dedups by
        // msg_id exactly like JetStream.
        let sink = Arc::new(FakeSink::default());

        // Turn 1.
        let buf1 = LogBuffer::new(&cfg);
        buf1.push("turn1-line".to_string());
        run_publisher_to_completion(sink.clone(), buf1, Uuid::new_v4()).await;

        // Turn 2 — same task instance, fresh shipper/nonce, seq resets to 0.
        let buf2 = LogBuffer::new(&cfg);
        buf2.push("turn2-line".to_string());
        run_publisher_to_completion(sink.clone(), buf2, Uuid::new_v4()).await;

        let g = sink.inner.lock().unwrap();
        let bodies: Vec<String> = g
            .stored
            .iter()
            .map(|(_, p, _)| String::from_utf8_lossy(p).to_string())
            .collect();
        assert!(
            bodies.iter().any(|b| b.contains("turn1-line")),
            "turn 1 logs present"
        );
        assert!(
            bodies.iter().any(|b| b.contains("turn2-line")),
            "turn 2 logs must survive — fresh nonce avoids cross-turn dedup collision"
        );
    }

    // ---- finish() with a real subprocess + wedged sink ----

    /// A sink whose batch publishes never succeed — models NATS wedged. The
    /// marker still records so the test can assert finish() terminates.
    #[derive(Default)]
    struct WedgedSink {
        markers: StdMutex<Vec<(TaskExit, bool)>>,
    }

    #[async_trait]
    impl LogBatchSink for WedgedSink {
        async fn publish_batch(&self, _: &str, _: &str, _: u64, _: Bytes) -> Result<()> {
            // Never returns success while alive; the publisher backs off and
            // retries. (The publisher task is aborted by finish on the cap.)
            sleep(Duration::from_secs(3600)).await;
            Ok(())
        }
        async fn publish_marker(&self, _: &str, exit: &TaskExit, tail_dropped: bool) -> Result<()> {
            self.markers
                .lock()
                .unwrap()
                .push((exit.clone(), tail_dropped));
            Ok(())
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn finish_reports_completion_even_with_wedged_sink_and_stdout_holding_descendant() {
        use tokio::process::Command;

        // `sh` prints one line, backgrounds a `sleep` that inherits (holds
        // open) stdout, then exits 0. child.wait() must return — completion is
        // observed independently of stdout reaching EOF — and finish() must not
        // hang on the held-open pipe; it caps at flush_deadline and publishes a
        // tail-dropped marker.
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("echo hello; sleep 300 & exit 0");
        cmd.stdout(std::process::Stdio::piped());
        cmd.process_group(0);
        let mut child = cmd.spawn().expect("spawn sh");
        // Capture the pgid *before* reaping — `child.id()` returns None after
        // wait(), and we need it to clean up the backgrounded sleep at the end.
        let pgid = child.id().map(|p| p as i32);
        let stdout = child.stdout.take().expect("capture stdout");

        let cfg = ShipperConfig {
            flush_deadline: Duration::from_millis(150),
            ..test_config()
        };
        let identity = LogIdentity {
            workflow_id: Uuid::new_v4(),
            workflow_instance_id: Uuid::new_v4(),
            task_instance_id: Uuid::new_v4(),
        };
        let sink = Arc::new(WedgedSink::default());
        let shipper = TaskLogShipper::start(identity, sink.clone(), &cfg, stdout);

        // Completion is observed via child.wait(), not stdout-EOF — this returns
        // even though the backgrounded sleep holds stdout open.
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("child.wait must not hang on a stdout-holding descendant")
            .expect("wait ok");
        assert!(status.success());

        // finish() caps on the deadline and publishes the marker (tail-dropped).
        let shutdown = CancellationToken::new();
        timeout(
            Duration::from_secs(5),
            shipper.finish(TaskExit::Status(0), &shutdown),
        )
        .await
        .expect("finish must terminate on the safety-cap deadline");

        let markers = sink.markers.lock().unwrap();
        assert_eq!(markers.len(), 1, "exactly one End-of-stream marker");
        assert_eq!(markers[0].0, TaskExit::Status(0), "marker carries exit");
        assert!(
            markers[0].1,
            "wedged sink + held-open stdout → tail-dropped marker"
        );

        // Clean up the backgrounded sleep so it doesn't linger past the test.
        if let Some(pgid) = pgid {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pgid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }

    #[tokio::test]
    async fn finish_publishes_a_clean_marker_when_the_backlog_ships() {
        // Drive finish() over a buffer that drained cleanly (drain already at
        // EOF, nothing buffered): the marker is published clean, not tail-dropped.
        let cfg = test_config();
        let sink = Arc::new(FakeSink::default());
        let buffer = LogBuffer::new(&cfg);
        let state = Arc::new(ShipperState {
            finishing: AtomicBool::new(false),
            drain_done: AtomicBool::new(false),
        });
        // Simulate a drain that immediately hit EOF.
        state.drain_done.store(true, Ordering::Release);
        let publisher_handle = tokio::spawn(run_publisher(
            "logs.test".to_string(),
            sink.clone(),
            buffer.clone(),
            state.clone(),
            Uuid::new_v4(),
            Duration::from_millis(5),
        ));
        let drain_handle = tokio::spawn(async {});
        let shipper = TaskLogShipper {
            subject: "logs.test".to_string(),
            sink: sink.clone(),
            state,
            buffer,
            drain_handle,
            publisher_handle,
            flush_deadline: Duration::from_millis(500),
        };
        let shutdown = CancellationToken::new();
        timeout(
            Duration::from_secs(2),
            shipper.finish(TaskExit::Status(0), &shutdown),
        )
        .await
        .expect("finish terminates");
        let g = sink.inner.lock().unwrap();
        assert_eq!(g.markers.len(), 1);
        assert!(!g.markers[0].1, "clean finish → marker not tail-dropped");
    }
}
