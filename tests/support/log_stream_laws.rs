use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tickr_executor::log_stream::LogStream;
use tickr_proto::coord::log_stream::{
    AcceptOutcome, GapOutcome, LogExit, LogRecordIdentity, LogRecordSubmission, LogStreamIdentity,
    LogTerminal, PreAcceptanceGap, ReplayedLogRecord, TerminalOutcome,
};
use uuid::Uuid;

pub type OpenFuture = Pin<Box<dyn Future<Output = Result<Box<dyn LogStream>>> + Send>>;

pub fn identity(task_instance_id: Uuid, pickup_generation: u64) -> LogStreamIdentity {
    LogStreamIdentity {
        task_instance_id,
        pickup_generation,
    }
}

pub fn submission(stream: &LogStreamIdentity, sequence: u64, bytes: &[u8]) -> LogRecordSubmission {
    LogRecordSubmission::new(
        LogRecordIdentity {
            stream: stream.clone(),
            sequence,
        },
        bytes.to_vec(),
    )
}

fn gap(stream: &LogStreamIdentity, first: u64, last: u64) -> PreAcceptanceGap {
    PreAcceptanceGap {
        stream: stream.clone(),
        first_sequence: first,
        last_sequence: last,
        dropped_records: last - first + 1,
    }
}

pub async fn assert_log_stream_laws(
    mut open: impl FnMut(LogStreamIdentity, Duration) -> OpenFuture,
) -> Result<()> {
    let task_instance_id = Uuid::new_v4();
    let controlled = identity(task_instance_id, 7);
    let timeout = Duration::from_secs(2);
    let mut stream = open(controlled.clone(), timeout).await?;
    assert_eq!(stream.identity(), &controlled);

    assert_eq!(
        stream.accept(submission(&controlled, 2, b"third")).await?,
        AcceptOutcome::Accepted
    );
    assert_eq!(stream.committed_frontier(), None);
    assert!(stream.replay().await?.is_empty());
    assert_eq!(
        stream
            .declare_pre_acceptance_gap(gap(&controlled, 0, 1))
            .await?,
        GapOutcome::Declared
    );
    assert_eq!(stream.committed_frontier(), Some(2));
    assert_eq!(
        stream.accept(submission(&controlled, 2, b"third")).await?,
        AcceptOutcome::AlreadyAccepted
    );
    assert!(stream
        .accept(submission(&controlled, 2, b"conflicting"))
        .await
        .is_err());
    assert!(stream
        .declare_pre_acceptance_gap(gap(&controlled, 2, 2))
        .await
        .is_err());
    let replay = stream.replay().await?;
    assert!(matches!(
        replay.as_slice(),
        [
            ReplayedLogRecord::PreAcceptanceGap(_),
            ReplayedLogRecord::Accepted { identity, bytes, .. }
        ] if identity.sequence == 2 && bytes == b"third"
    ));
    drop(stream);

    let mut restarted = open(controlled.clone(), timeout).await?;
    assert_eq!(restarted.committed_frontier(), Some(2));
    assert_eq!(restarted.replay().await?, replay);
    assert_eq!(
        restarted.finish_cleanly(LogExit::Status(0)).await?,
        TerminalOutcome::Recorded
    );
    assert!(matches!(
        restarted.replay().await?.last(),
        Some(ReplayedLogRecord::Terminal {
            terminal: LogTerminal::EndOfStream {
                exit: LogExit::Status(0)
            },
            ..
        })
    ));
    assert!(restarted
        .accept(submission(&controlled, 3, b"after terminal"))
        .await
        .is_err());
    drop(restarted);

    let mut controlled_restarted = open(controlled, timeout).await?;
    assert_eq!(
        controlled_restarted.recover_abnormal_closure().await?,
        TerminalOutcome::AlreadyRecorded
    );

    let abnormal = identity(task_instance_id, 8);
    let mut interrupted = open(abnormal.clone(), timeout).await?;
    interrupted
        .accept(submission(&abnormal, 0, b"before crash"))
        .await?;
    drop(interrupted);
    let mut recovered = open(abnormal, timeout).await?;
    assert_eq!(
        recovered.recover_abnormal_closure().await?,
        TerminalOutcome::Recorded
    );
    assert!(matches!(
        recovered.replay().await?.last(),
        Some(ReplayedLogRecord::Terminal {
            terminal: LogTerminal::AbnormalClosure {
                committed_frontier: Some(0)
            },
            ..
        })
    ));
    Ok(())
}
