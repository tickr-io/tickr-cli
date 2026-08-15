//! Accepted-Log role contract and fresh all-NATS adapter.

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{self, consumer, consumer::pull, stream};
use async_nats::{Client as NatsClient, HeaderMap};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tickr_proto::coord::all_nats;
use tickr_proto::coord::log_stream::{
    rebuild_log_streams, AcceptOutcome, GapOutcome, LogExit, LogRecordSubmission, LogSeal,
    LogStreamIdentity, LogStreamState, LogTerminal, PreAcceptanceGap, ReplayedLogRecord,
    TerminalOutcome,
};
use tokio::sync::OnceCell;
use tokio::time::timeout;
use tokio_util::bytes::Bytes;
use uuid::Uuid;

const NATS_MSG_ID_HEADER: &str = "Nats-Msg-Id";
const LOG_STREAM_MAX_BYTES: i64 = all_nats::LOG_STREAM_MAX_BYTES;
const LOG_STREAM_DEDUP_WINDOW: Duration = all_nats::LOG_STREAM_DEDUP_WINDOW;

/// One role-specific accepted-Log stream. Implementations expose no substrate
/// client and acknowledge only after their durable mutation succeeds.
#[async_trait]
pub trait LogStream: Send {
    fn identity(&self) -> &LogStreamIdentity;
    fn committed_frontier(&self) -> Option<u64>;

    async fn accept(&mut self, submission: LogRecordSubmission) -> Result<AcceptOutcome>;
    async fn declare_pre_acceptance_gap(&mut self, gap: PreAcceptanceGap) -> Result<GapOutcome>;
    async fn finish_cleanly(&mut self, exit: LogExit) -> Result<TerminalOutcome>;
    async fn recover_abnormal_closure(&mut self) -> Result<TerminalOutcome>;
    async fn replay(&mut self) -> Result<Vec<ReplayedLogRecord>>;
}

/// Substrate-neutral routing for every pickup generation of one Task's Log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogStreamRoute {
    pub workflow_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub task_instance_id: Uuid,
}

/// Formation-selected entry point shared by the Task shipper and live-Log
/// reader. Substrate clients remain inside implementations.
#[async_trait]
pub trait LogStreamProvider: Send + Sync {
    async fn prepare(&self) -> Result<()>;
    async fn open(
        &self,
        route: LogStreamRoute,
        identity: LogStreamIdentity,
    ) -> Result<Box<dyn LogStream>>;
    async fn replay_task(&self, route: LogStreamRoute) -> Result<Vec<ReplayedLogRecord>>;

    /// Seal every pickup generation accepted for one terminal Task instance.
    async fn seal_task_for_compaction(&self, _route: LogStreamRoute) -> Result<Vec<LogSeal>> {
        Err(anyhow!(
            "selected LogStaging adapter does not expose Compaction sealing"
        ))
    }

    /// Bind verified final-Log identity to the immutable accepted-Log seals.
    async fn record_verified_archive_commit(
        &self,
        _seals: &[LogSeal],
        _archive_identity: &[u8],
    ) -> Result<()> {
        Err(anyhow!(
            "selected LogStaging adapter does not expose archive evidence"
        ))
    }

    /// Purge accepted Log staging only after verified archive evidence exists.
    async fn purge_after_verified_archive_commit(
        &self,
        _seals: &[LogSeal],
        _archive_identity: &[u8],
    ) -> Result<()> {
        Err(anyhow!(
            "selected LogStaging adapter does not expose Compaction purge"
        ))
    }
}

/// Fresh all-NATS routing stays private to this adapter.
fn all_nats_subject(route: &LogStreamRoute) -> String {
    format!(
        "{}.{}.{}.{}",
        all_nats::LOG_SUBJECT_PREFIX,
        route.workflow_id,
        route.workflow_instance_id,
        route.task_instance_id
    )
}

/// Fresh all-NATS accepted-Log stream.
pub struct AllNatsLogStream {
    js: Arc<jetstream::Context>,
    subject: String,
    publish_timeout: Duration,
    state: LogStreamState,
}

impl AllNatsLogStream {
    pub async fn open(
        js: Arc<jetstream::Context>,
        route: LogStreamRoute,
        identity: LogStreamIdentity,
        publish_timeout: Duration,
    ) -> Result<Self> {
        if route.task_instance_id != identity.task_instance_id {
            return Err(anyhow!("Log route does not match stream identity"));
        }
        let subject = all_nats_subject(&route);
        let mut stream = Self {
            js,
            subject,
            publish_timeout,
            state: LogStreamState::new(identity),
        };
        stream.reload().await?;
        Ok(stream)
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    async fn publish_record(
        &self,
        msg_id: String,
        mut headers: HeaderMap,
        payload: Bytes,
    ) -> Result<()> {
        headers.insert(NATS_MSG_ID_HEADER, msg_id.as_str());
        let ack = self
            .js
            .publish_with_headers(self.subject.clone(), headers, payload)
            .await
            .context("publish Accepted Log record")?;
        if self.publish_timeout.is_zero() {
            return Err(anyhow!("Accepted Log acknowledgement timed out"));
        }
        timeout(self.publish_timeout, ack)
            .await
            .map_err(|_| anyhow!("Accepted Log acknowledgement timed out"))?
            .context("await Accepted Log acknowledgement")?;
        Ok(())
    }

    async fn reload(&mut self) -> Result<()> {
        let mut rebuilt = LogStreamState::new(self.state.identity().clone());
        for record in read_all_nats_records(&self.js, &self.subject).await? {
            if record_stream(&record) != rebuilt.identity() {
                continue;
            }
            match record {
                ReplayedLogRecord::Accepted {
                    identity,
                    content_digest,
                    bytes,
                } => {
                    rebuilt.apply_accepted(LogRecordSubmission {
                        identity,
                        content_digest,
                        bytes,
                    })?;
                }
                ReplayedLogRecord::PreAcceptanceGap(gap) => {
                    rebuilt.apply_gap(gap)?;
                }
                ReplayedLogRecord::Terminal { terminal, .. } => {
                    rebuilt.apply_terminal(terminal)?;
                }
            }
        }
        self.state = rebuilt;
        Ok(())
    }

    fn base_headers(&self, kind: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(all_nats::LOG_PROTOCOL_HEADER, all_nats::LOG_PROTOCOL);
        headers.insert(all_nats::LOG_KIND_HEADER, kind);
        headers.insert(
            all_nats::LOG_TASK_INSTANCE_HEADER,
            self.state.identity().task_instance_id.to_string().as_str(),
        );
        headers.insert(
            all_nats::LOG_PICKUP_GENERATION_HEADER,
            self.state.identity().pickup_generation.to_string().as_str(),
        );
        headers
    }

    fn add_frontier(headers: &mut HeaderMap, frontier: Option<u64>) {
        if let Some(frontier) = frontier {
            headers.insert(
                all_nats::LOG_COMMITTED_FRONTIER_HEADER,
                frontier.to_string().as_str(),
            );
        }
    }

    fn record_message_id(identity: &LogStreamIdentity, sequence: u64) -> String {
        format!(
            "log:{}:{}:record:{}",
            identity.task_instance_id, identity.pickup_generation, sequence
        )
    }

    fn gap_message_id(gap: &PreAcceptanceGap) -> String {
        format!(
            "log:{}:{}:gap:{}-{}",
            gap.stream.task_instance_id,
            gap.stream.pickup_generation,
            gap.first_sequence,
            gap.last_sequence
        )
    }

    fn terminal_message_id(identity: &LogStreamIdentity) -> String {
        format!(
            "log:{}:{}:terminal",
            identity.task_instance_id, identity.pickup_generation
        )
    }
}

#[async_trait]
impl LogStream for AllNatsLogStream {
    fn identity(&self) -> &LogStreamIdentity {
        self.state.identity()
    }

    fn committed_frontier(&self) -> Option<u64> {
        self.state.committed_frontier()
    }

    async fn accept(&mut self, submission: LogRecordSubmission) -> Result<AcceptOutcome> {
        self.reload().await?;
        let mut prospective = self.state.clone();
        let outcome = prospective.apply_accepted(submission.clone())?;
        if outcome == AcceptOutcome::AlreadyAccepted {
            return Ok(outcome);
        }

        let mut headers = self.base_headers(all_nats::LOG_KIND_ACCEPTED);
        headers.insert(
            all_nats::LOG_SEQUENCE_HEADER,
            submission.identity.sequence.to_string().as_str(),
        );
        headers.insert(
            all_nats::LOG_CONTENT_DIGEST_HEADER,
            submission.content_digest.as_str(),
        );
        Self::add_frontier(&mut headers, prospective.committed_frontier());
        self.publish_record(
            Self::record_message_id(&submission.identity.stream, submission.identity.sequence),
            headers,
            Bytes::from(submission.bytes.clone()),
        )
        .await?;
        self.reload().await?;
        match self.state.apply_accepted(submission)? {
            AcceptOutcome::Accepted | AcceptOutcome::AlreadyAccepted => Ok(AcceptOutcome::Accepted),
        }
    }

    async fn declare_pre_acceptance_gap(&mut self, gap: PreAcceptanceGap) -> Result<GapOutcome> {
        self.reload().await?;
        let mut prospective = self.state.clone();
        let outcome = prospective.apply_gap(gap.clone())?;
        if outcome == GapOutcome::AlreadyDeclared {
            return Ok(outcome);
        }

        let mut headers = self.base_headers(all_nats::LOG_KIND_GAP);
        headers.insert(
            all_nats::LOG_GAP_FIRST_HEADER,
            gap.first_sequence.to_string().as_str(),
        );
        headers.insert(
            all_nats::LOG_GAP_LAST_HEADER,
            gap.last_sequence.to_string().as_str(),
        );
        headers.insert(
            all_nats::LOG_GAP_DROPPED_HEADER,
            gap.dropped_records.to_string().as_str(),
        );
        Self::add_frontier(&mut headers, prospective.committed_frontier());
        self.publish_record(Self::gap_message_id(&gap), headers, Bytes::new())
            .await?;
        self.reload().await?;
        match self.state.apply_gap(gap)? {
            GapOutcome::Declared | GapOutcome::AlreadyDeclared => Ok(GapOutcome::Declared),
        }
    }

    async fn finish_cleanly(&mut self, exit: LogExit) -> Result<TerminalOutcome> {
        self.reload().await?;
        let terminal = LogTerminal::EndOfStream { exit: exit.clone() };
        let mut prospective = self.state.clone();
        let outcome = prospective.apply_terminal(terminal.clone())?;
        if outcome == TerminalOutcome::AlreadyRecorded {
            return Ok(outcome);
        }
        let mut headers = self.base_headers(all_nats::LOG_KIND_END);
        match exit {
            LogExit::Status(status) => {
                headers.insert(all_nats::LOG_EXIT_KIND_HEADER, "status");
                headers.insert(
                    all_nats::LOG_EXIT_STATUS_HEADER,
                    status.to_string().as_str(),
                );
            }
            LogExit::NoStatus => {
                headers.insert(all_nats::LOG_EXIT_KIND_HEADER, "no-status");
            }
            LogExit::Error(reason) => {
                headers.insert(all_nats::LOG_EXIT_KIND_HEADER, "error");
                let one_line = reason.replace(['\r', '\n'], " ");
                headers.insert(all_nats::LOG_EXIT_REASON_HEADER, one_line.as_str());
            }
        }
        Self::add_frontier(&mut headers, prospective.committed_frontier());
        self.publish_record(
            Self::terminal_message_id(self.state.identity()),
            headers,
            Bytes::new(),
        )
        .await?;
        self.reload().await?;
        Ok(TerminalOutcome::Recorded)
    }

    async fn recover_abnormal_closure(&mut self) -> Result<TerminalOutcome> {
        self.reload().await?;
        if self.state.terminal().is_some() {
            return Ok(TerminalOutcome::AlreadyRecorded);
        }
        let terminal = LogTerminal::AbnormalClosure {
            committed_frontier: self.state.committed_frontier(),
        };
        let mut prospective = self.state.clone();
        let outcome = prospective.apply_terminal(terminal)?;
        if outcome == TerminalOutcome::AlreadyRecorded {
            return Ok(outcome);
        }
        let mut headers = self.base_headers(all_nats::LOG_KIND_ABNORMAL);
        Self::add_frontier(&mut headers, self.state.committed_frontier());
        self.publish_record(
            Self::terminal_message_id(self.state.identity()),
            headers,
            Bytes::new(),
        )
        .await?;
        self.reload().await?;
        Ok(TerminalOutcome::Recorded)
    }

    async fn replay(&mut self) -> Result<Vec<ReplayedLogRecord>> {
        self.reload().await?;
        Ok(self.state.replay())
    }
}

/// Fresh all-NATS provider. The component sees only `LogStreamProvider`; NATS
/// setup, subjects, replay consumers, and deduplication stay here.
#[derive(Clone)]
pub struct AllNatsLogStreamProvider {
    nats: Arc<NatsClient>,
    context: Arc<OnceCell<Arc<jetstream::Context>>>,
    publish_timeout: Duration,
}

impl AllNatsLogStreamProvider {
    pub fn new(nats: Arc<NatsClient>, publish_timeout: Duration) -> Self {
        Self {
            nats,
            context: Arc::new(OnceCell::new()),
            publish_timeout,
        }
    }

    pub async fn connect(url: &str, publish_timeout: Duration) -> Result<Self> {
        let nats = Arc::new(
            async_nats::connect(url)
                .await
                .context("connect fresh all-NATS Log staging")?,
        );
        Ok(Self::new(nats, publish_timeout))
    }

    async fn context(&self) -> Result<Arc<jetstream::Context>> {
        self.context
            .get_or_try_init(|| ensure_all_nats_log_stream(self.nats.as_ref()))
            .await
            .cloned()
    }
}

#[async_trait]
impl LogStreamProvider for AllNatsLogStreamProvider {
    async fn prepare(&self) -> Result<()> {
        self.context().await.map(|_| ())
    }

    async fn open(
        &self,
        route: LogStreamRoute,
        identity: LogStreamIdentity,
    ) -> Result<Box<dyn LogStream>> {
        Ok(Box::new(
            AllNatsLogStream::open(self.context().await?, route, identity, self.publish_timeout)
                .await?,
        ))
    }

    async fn replay_task(&self, route: LogStreamRoute) -> Result<Vec<ReplayedLogRecord>> {
        let subject = all_nats_subject(&route);
        let records = read_all_nats_records(self.context().await?.as_ref(), &subject).await?;
        let streams = rebuild_log_streams(records)?;
        Ok(streams
            .into_values()
            .flat_map(|stream| stream.replay())
            .collect())
    }
}

/// Ensure the fresh all-NATS Log staging stream exists.
pub async fn ensure_all_nats_log_stream(nats: &NatsClient) -> Result<Arc<jetstream::Context>> {
    let js = jetstream::new(nats.clone());
    js.get_or_create_stream(stream::Config {
        name: all_nats::LOG_STREAM.to_owned(),
        subjects: vec![all_nats::LOG_STREAM_SUBJECTS.to_owned()],
        max_bytes: LOG_STREAM_MAX_BYTES,
        discard: stream::DiscardPolicy::New,
        duplicate_window: LOG_STREAM_DEDUP_WINDOW,
        ..Default::default()
    })
    .await
    .map_err(|error| anyhow!("Failed to get_or_create Log staging stream: {error}"))?;
    Ok(Arc::new(js))
}

async fn read_all_nats_records(
    js: &jetstream::Context,
    subject: &str,
) -> Result<Vec<ReplayedLogRecord>> {
    let stream = js
        .get_stream(all_nats::LOG_STREAM)
        .await
        .context("open all-NATS Log staging stream")?;
    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: subject.to_owned(),
            deliver_policy: consumer::DeliverPolicy::All,
            ack_policy: consumer::AckPolicy::None,
            ..Default::default()
        })
        .await
        .context("create all-NATS Log replay consumer")?;
    let mut records = Vec::new();
    loop {
        let mut fetched = consumer
            .fetch()
            .max_messages(500)
            .expires(Duration::from_millis(250))
            .messages()
            .await
            .context("fetch all-NATS Log replay records")?;
        let mut count = 0_usize;
        while let Some(message) = fetched.next().await {
            let message =
                message.map_err(|error| anyhow!("read all-NATS Log replay record: {error}"))?;
            records.push(
                all_nats::decode_log_record(
                    |name| {
                        message
                            .headers
                            .as_ref()
                            .and_then(|headers| headers.get(name))
                            .map(|value| value.as_str().to_owned())
                    },
                    &message.payload,
                )
                .map_err(anyhow::Error::msg)?,
            );
            count += 1;
        }
        if count < 500 {
            break;
        }
    }
    Ok(records)
}

fn record_stream(record: &ReplayedLogRecord) -> &LogStreamIdentity {
    match record {
        ReplayedLogRecord::Accepted { identity, .. } => &identity.stream,
        ReplayedLogRecord::PreAcceptanceGap(gap) => &gap.stream,
        ReplayedLogRecord::Terminal { stream, .. } => stream,
    }
}
