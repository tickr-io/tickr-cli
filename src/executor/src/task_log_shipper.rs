//! Non-blocking stdout ingestion into one accepted-Log stream.
//!
//! The drain performs only a bounded memory copy under a short mutex. It never
//! awaits Log acceptance or touches disk, so telemetry pressure cannot
//! back-pressure the Task process. Every drained chunk receives its stable
//! sequence before buffering; an evicted chunk therefore becomes an explicit
//! pre-acceptance gap when the Log protocol can next advance.

use crate::log_stream::{LogStream, LogStreamRoute};
use anyhow::Result;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tickr_proto::coord::log_stream::{
    LogExit, LogRecordIdentity, LogRecordSubmission, LogStreamIdentity, PreAcceptanceGap,
};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::process::ChildStdout;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use tickr_proto::coord::log_stream::LogExit as TaskExit;

/// Routing and pickup-generation identity for one Task run.
#[derive(Clone, Debug)]
pub struct LogIdentity {
    pub workflow_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub task_instance_id: Uuid,
    pub pickup_generation: u64,
}

impl LogIdentity {
    pub fn stream_identity(&self) -> LogStreamIdentity {
        LogStreamIdentity {
            task_instance_id: self.task_instance_id,
            pickup_generation: self.pickup_generation,
        }
    }

    pub fn route(&self) -> LogStreamRoute {
        LogStreamRoute {
            workflow_id: self.workflow_id,
            workflow_instance_id: self.workflow_instance_id,
            task_instance_id: self.task_instance_id,
        }
    }
}

/// Bounded pre-acceptance and acknowledgement settings.
#[derive(Clone, Debug)]
pub struct ShipperConfig {
    /// Maximum unaccepted chunks retained in memory, excluding one in flight.
    pub buffer_capacity: usize,
    /// Maximum bytes copied from stdout into one stable record.
    pub record_max_bytes: usize,
    /// Per-operation acceptance acknowledgement timeout.
    pub publish_timeout: Duration,
    /// Cap on acknowledgement retry backoff.
    pub publish_backoff_max: Duration,
    /// Bound on post-process-exit Log flushing.
    pub flush_deadline: Duration,
}

impl Default for ShipperConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 10_000,
            record_max_bytes: 8 * 1024,
            publish_timeout: Duration::from_secs(5),
            publish_backoff_max: Duration::from_secs(5),
            flush_deadline: Duration::from_secs(120),
        }
    }
}

impl ShipperConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        fn usize_env(key: &str, default: usize) -> usize {
            std::env::var(key)
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|value| *value > 0)
                .unwrap_or(default)
        }
        fn duration_env(key: &str, default: Duration) -> Duration {
            std::env::var(key)
                .ok()
                .and_then(|value| value.parse().ok())
                .map(Duration::from_millis)
                .unwrap_or(default)
        }
        Self {
            buffer_capacity: usize_env("TICKR_LOG_BUFFER_CAPACITY", defaults.buffer_capacity),
            record_max_bytes: usize_env("TICKR_LOG_RECORD_MAX_BYTES", defaults.record_max_bytes),
            publish_timeout: duration_env("TICKR_LOG_PUBLISH_TIMEOUT_MS", defaults.publish_timeout),
            publish_backoff_max: duration_env(
                "TICKR_LOG_PUBLISH_BACKOFF_MAX_MS",
                defaults.publish_backoff_max,
            ),
            flush_deadline: duration_env("TICKR_LOG_FLUSH_DEADLINE_MS", defaults.flush_deadline),
        }
    }
}

struct BufferedRecord {
    sequence: u64,
    bytes: Vec<u8>,
}

enum BufferedItem {
    Record(BufferedRecord),
    Gap(PreAcceptanceGap),
}

struct BufferState {
    records: VecDeque<BufferedRecord>,
    gaps: VecDeque<PreAcceptanceGap>,
    next_sequence: u64,
}

struct LogBuffer {
    identity: LogStreamIdentity,
    capacity: usize,
    state: Mutex<BufferState>,
    notify: Notify,
}

impl LogBuffer {
    fn new(identity: LogStreamIdentity, capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            identity,
            capacity: capacity.max(1),
            state: Mutex::new(BufferState {
                records: VecDeque::new(),
                gaps: VecDeque::new(),
                next_sequence: 0,
            }),
            notify: Notify::new(),
        })
    }

    /// Assign a stable sequence and enqueue without awaiting the backend.
    fn push(&self, bytes: Vec<u8>) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let sequence = state.next_sequence;
            let Some(next_sequence) = sequence.checked_add(1) else {
                return;
            };
            state.next_sequence = next_sequence;
            if state.records.len() >= self.capacity {
                let evicted = state
                    .records
                    .pop_front()
                    .expect("a full Log buffer has one oldest record");
                self.record_gap(&mut state, evicted.sequence);
            }
            state.records.push_back(BufferedRecord { sequence, bytes });
        }
        self.notify.notify_one();
    }

    fn record_gap(&self, state: &mut BufferState, sequence: u64) {
        if let Some(last) = state.gaps.back_mut() {
            if last.last_sequence.checked_add(1) == Some(sequence) {
                last.last_sequence = sequence;
                last.dropped_records += 1;
                return;
            }
        }
        state.gaps.push_back(PreAcceptanceGap {
            stream: self.identity.clone(),
            first_sequence: sequence,
            last_sequence: sequence,
            dropped_records: 1,
        });
    }

    fn poke(&self) {
        self.notify.notify_one();
    }

    async fn next_item(&self, lifecycle: &ShipperLifecycle) -> Option<BufferedItem> {
        loop {
            let notified = self.notify.notified();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let gap_sequence = state.gaps.front().map(|gap| gap.first_sequence);
                let record_sequence = state.records.front().map(|record| record.sequence);
                match (gap_sequence, record_sequence) {
                    (Some(gap), Some(record)) if gap <= record => {
                        return state.gaps.pop_front().map(BufferedItem::Gap)
                    }
                    (Some(_), None) => return state.gaps.pop_front().map(BufferedItem::Gap),
                    (_, Some(_)) => return state.records.pop_front().map(BufferedItem::Record),
                    (None, None) => {}
                }
                if lifecycle.finishing.load(Ordering::Acquire)
                    && lifecycle.remaining_drains.load(Ordering::Acquire) == 0
                {
                    return None;
                }
            }
            notified.await;
        }
    }
}

struct ShipperLifecycle {
    finishing: AtomicBool,
    remaining_drains: AtomicUsize,
}

async fn run_drain(
    output: Box<dyn AsyncRead + Unpin + Send>,
    buffer: Arc<LogBuffer>,
    lifecycle: Arc<ShipperLifecycle>,
    record_max_bytes: usize,
) {
    let mut output = BufReader::new(output);
    let mut chunk = vec![0_u8; record_max_bytes.max(1)];
    loop {
        match output.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => buffer.push(chunk[..read].to_vec()),
            Err(_) => break,
        }
    }
    lifecycle.remaining_drains.fetch_sub(1, Ordering::AcqRel);
    buffer.poke();
}

async fn retry_acceptance(stream: &mut dyn LogStream, item: &BufferedItem, backoff_max: Duration) {
    let mut backoff = Duration::from_millis(50);
    loop {
        let result = match item {
            BufferedItem::Record(record) => stream
                .accept(LogRecordSubmission::new(
                    LogRecordIdentity {
                        stream: stream.identity().clone(),
                        sequence: record.sequence,
                    },
                    record.bytes.clone(),
                ))
                .await
                .map(|_| ()),
            BufferedItem::Gap(gap) => stream
                .declare_pre_acceptance_gap(gap.clone())
                .await
                .map(|_| ()),
        };
        if result.is_ok() {
            return;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(backoff_max);
    }
}

async fn run_publisher(
    stream: Arc<AsyncMutex<Box<dyn LogStream>>>,
    buffer: Arc<LogBuffer>,
    lifecycle: Arc<ShipperLifecycle>,
    backoff_max: Duration,
) {
    while let Some(item) = buffer.next_item(&lifecycle).await {
        retry_acceptance(stream.lock().await.as_mut(), &item, backoff_max).await;
    }
}

/// Per-Task Log shipper. Process completion is independent of this object's
/// bounded post-exit flush.
pub struct TaskLogShipper {
    stream: Arc<AsyncMutex<Box<dyn LogStream>>>,
    lifecycle: Arc<ShipperLifecycle>,
    buffer: Arc<LogBuffer>,
    drain_handles: Vec<JoinHandle<()>>,
    publisher_handle: JoinHandle<()>,
    flush_deadline: Duration,
}

impl TaskLogShipper {
    pub fn start(stream: Box<dyn LogStream>, config: &ShipperConfig, stdout: ChildStdout) -> Self {
        Self::start_readers(stream, config, vec![Box::new(stdout)])
    }

    pub fn start_readers(
        stream: Box<dyn LogStream>,
        config: &ShipperConfig,
        readers: Vec<Box<dyn AsyncRead + Unpin + Send>>,
    ) -> Self {
        let identity = stream.identity().clone();
        let stream = Arc::new(AsyncMutex::new(stream));
        let lifecycle = Arc::new(ShipperLifecycle {
            finishing: AtomicBool::new(false),
            remaining_drains: AtomicUsize::new(readers.len()),
        });
        let buffer = LogBuffer::new(identity, config.buffer_capacity);
        let drain_handles = readers
            .into_iter()
            .map(|reader| {
                tokio::spawn(run_drain(
                    reader,
                    buffer.clone(),
                    lifecycle.clone(),
                    config.record_max_bytes,
                ))
            })
            .collect();
        if lifecycle.remaining_drains.load(Ordering::Acquire) == 0 {
            buffer.poke();
        }
        let publisher_handle = tokio::spawn(run_publisher(
            stream.clone(),
            buffer.clone(),
            lifecycle.clone(),
            config.publish_backoff_max,
        ));
        Self {
            stream,
            lifecycle,
            buffer,
            drain_handles,
            publisher_handle,
            flush_deadline: config.flush_deadline,
        }
    }

    pub async fn finish(self, exit: LogExit, shutdown: &CancellationToken) {
        let Self {
            stream,
            lifecycle,
            buffer,
            drain_handles,
            mut publisher_handle,
            flush_deadline,
        } = self;
        lifecycle.finishing.store(true, Ordering::Release);
        buffer.poke();

        let mut publisher_done = false;
        let clean = tokio::select! {
            _ = &mut publisher_handle => {
                publisher_done = true;
                true
            }
            _ = sleep(flush_deadline) => false,
            _ = shutdown.cancelled() => false,
        };
        for handle in drain_handles {
            handle.abort();
            let _ = handle.await;
        }
        if !publisher_done {
            publisher_handle.abort();
            let _ = publisher_handle.await;
        }

        let mut stream = stream.lock().await;
        let terminal = if clean {
            timeout(
                stream_publish_timeout(flush_deadline),
                stream.finish_cleanly(exit),
            )
            .await
        } else {
            timeout(
                stream_publish_timeout(flush_deadline),
                stream.recover_abnormal_closure(),
            )
            .await
        };
        if terminal.as_ref().map_or(true, Result::is_err) {
            eprintln!(
                "Log staging terminal record could not be durably accepted for {:?}",
                stream.identity()
            );
        }
    }
}

fn stream_publish_timeout(flush_deadline: Duration) -> Duration {
    flush_deadline
        .min(Duration::from_secs(5))
        .max(Duration::from_millis(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::process::Stdio;
    use tickr_proto::coord::log_stream::{
        AcceptOutcome, GapOutcome, LogStreamState, ReplayedLogRecord, TerminalOutcome,
    };
    use tokio::process::Command;

    struct UnavailableLogStream {
        identity: LogStreamIdentity,
    }

    #[async_trait]
    impl LogStream for UnavailableLogStream {
        fn identity(&self) -> &LogStreamIdentity {
            &self.identity
        }

        fn committed_frontier(&self) -> Option<u64> {
            None
        }

        async fn accept(&mut self, _: LogRecordSubmission) -> Result<AcceptOutcome> {
            anyhow::bail!("sink unavailable")
        }

        async fn declare_pre_acceptance_gap(&mut self, _: PreAcceptanceGap) -> Result<GapOutcome> {
            anyhow::bail!("sink unavailable")
        }

        async fn finish_cleanly(&mut self, _: LogExit) -> Result<TerminalOutcome> {
            anyhow::bail!("sink unavailable")
        }

        async fn recover_abnormal_closure(&mut self) -> Result<TerminalOutcome> {
            anyhow::bail!("sink unavailable")
        }

        async fn replay(&mut self) -> Result<Vec<ReplayedLogRecord>> {
            Ok(Vec::new())
        }
    }

    struct MemoryLogStream {
        state: LogStreamState,
    }

    impl MemoryLogStream {
        fn new(identity: LogStreamIdentity) -> Self {
            Self {
                state: LogStreamState::new(identity),
            }
        }
    }

    #[async_trait]
    impl LogStream for MemoryLogStream {
        fn identity(&self) -> &LogStreamIdentity {
            self.state.identity()
        }

        fn committed_frontier(&self) -> Option<u64> {
            self.state.committed_frontier()
        }

        async fn accept(&mut self, submission: LogRecordSubmission) -> Result<AcceptOutcome> {
            Ok(self.state.apply_accepted(submission)?)
        }

        async fn declare_pre_acceptance_gap(
            &mut self,
            gap: PreAcceptanceGap,
        ) -> Result<GapOutcome> {
            Ok(self.state.apply_gap(gap)?)
        }

        async fn finish_cleanly(&mut self, exit: LogExit) -> Result<TerminalOutcome> {
            Ok(self.state.apply_terminal(
                tickr_proto::coord::log_stream::LogTerminal::EndOfStream { exit },
            )?)
        }

        async fn recover_abnormal_closure(&mut self) -> Result<TerminalOutcome> {
            Ok(self.state.apply_terminal(
                tickr_proto::coord::log_stream::LogTerminal::AbnormalClosure {
                    committed_frontier: self.state.committed_frontier(),
                },
            )?)
        }

        async fn replay(&mut self) -> Result<Vec<ReplayedLogRecord>> {
            Ok(self.state.replay())
        }
    }

    fn identity() -> LogStreamIdentity {
        LogStreamIdentity {
            task_instance_id: Uuid::nil(),
            pickup_generation: 4,
        }
    }

    #[tokio::test]
    async fn bounded_buffer_declares_evicted_sequences_before_later_records() {
        let buffer = LogBuffer::new(identity(), 2);
        buffer.push(b"zero".to_vec());
        buffer.push(b"one".to_vec());
        buffer.push(b"two".to_vec());
        let lifecycle = ShipperLifecycle {
            finishing: AtomicBool::new(true),
            remaining_drains: AtomicUsize::new(0),
        };
        let first = buffer.next_item(&lifecycle).await.unwrap();
        let second = buffer.next_item(&lifecycle).await.unwrap();
        let third = buffer.next_item(&lifecycle).await.unwrap();
        assert!(matches!(
            first,
            BufferedItem::Gap(PreAcceptanceGap {
                first_sequence: 0,
                last_sequence: 0,
                dropped_records: 1,
                ..
            })
        ));
        assert!(matches!(
            second,
            BufferedItem::Record(BufferedRecord { sequence: 1, .. })
        ));
        assert!(matches!(
            third,
            BufferedItem::Record(BufferedRecord { sequence: 2, .. })
        ));
        assert!(buffer.next_item(&lifecycle).await.is_none());
    }

    #[tokio::test]
    async fn retrying_same_submission_never_duplicates_an_accepted_record() {
        let mut stream = MemoryLogStream::new(identity());
        let submission = LogRecordSubmission::new(
            LogRecordIdentity {
                stream: identity(),
                sequence: 0,
            },
            b"stable".to_vec(),
        );
        assert_eq!(
            stream.accept(submission.clone()).await.unwrap(),
            AcceptOutcome::Accepted
        );
        assert_eq!(
            stream.accept(submission).await.unwrap(),
            AcceptOutcome::AlreadyAccepted
        );
        assert_eq!(
            stream
                .replay()
                .await
                .unwrap()
                .into_iter()
                .filter(|record| matches!(record, ReplayedLogRecord::Accepted { .. }))
                .count(),
            1
        );
    }
    #[tokio::test]
    async fn unavailable_sink_never_backpressures_process_stdout_or_completion() -> Result<()> {
        let mut child = Command::new("sh")
            .args([
                "-c",
                "i=0; while [ \"$i\" -lt 20000 ]; do printf '0123456789abcdef0123456789abcdef\\n'; i=$((i+1)); done",
            ])
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = child.stdout.take().expect("stdout is piped");
        let config = ShipperConfig {
            buffer_capacity: 4,
            record_max_bytes: 256,
            publish_timeout: Duration::from_millis(10),
            publish_backoff_max: Duration::from_millis(20),
            flush_deadline: Duration::from_millis(50),
        };
        let shipper = TaskLogShipper::start(
            Box::new(UnavailableLogStream {
                identity: identity(),
            }),
            &config,
            stdout,
        );

        let status = timeout(Duration::from_secs(5), child.wait()).await??;
        assert!(status.success());
        timeout(
            Duration::from_secs(1),
            shipper.finish(LogExit::Status(0), &CancellationToken::new()),
        )
        .await?;
        Ok(())
    }
}
