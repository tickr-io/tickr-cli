use anyhow::Result;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tickr::data_directory::{DataDirectory, RootRelativePath};
use tickr::local_log_staging::{
    content_digest, AcceptOutcome, LocalLogStagingStream, LogExit, LogRecordIdentity,
    LogRecordSubmission, LogStreamIdentity, LogTerminal, PreAcceptanceGap, ReplayedLogRecord,
    TerminalOutcome,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use uuid::Uuid;

fn admitted_data_directory() -> Result<(TempDir, DataDirectory)> {
    let directory = tempfile::tempdir()?;
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    let data_directory = DataDirectory::admit(directory.path())?;
    Ok((directory, data_directory))
}

fn stream_identity() -> LogStreamIdentity {
    LogStreamIdentity {
        task_instance_id: Uuid::new_v4(),
        pickup_generation: 7,
    }
}

fn record(stream: &LogStreamIdentity, sequence: u64) -> LogRecordIdentity {
    LogRecordIdentity {
        stream: stream.clone(),
        sequence,
    }
}

fn submission(
    stream: &LogStreamIdentity,
    sequence: u64,
    bytes: impl Into<Vec<u8>>,
) -> LogRecordSubmission {
    LogRecordSubmission::new(record(stream, sequence), bytes.into())
}

fn accepted(
    stream: &LogStreamIdentity,
    sequence: u64,
    bytes: impl Into<Vec<u8>>,
) -> ReplayedLogRecord {
    let bytes = bytes.into();
    ReplayedLogRecord::Accepted {
        identity: record(stream, sequence),
        content_digest: content_digest(&bytes),
        bytes,
    }
}

fn gap(stream: &LogStreamIdentity, first: u64, last: u64) -> PreAcceptanceGap {
    PreAcceptanceGap {
        stream: stream.clone(),
        first_sequence: first,
        last_sequence: last,
        dropped_records: last - first + 1,
    }
}

#[test]
fn synced_acceptance_survives_ambiguous_retry_and_restart() -> Result<()> {
    let (_temporary, data_directory) = admitted_data_directory()?;
    let identity = stream_identity();
    let mut stream = LocalLogStagingStream::open(&data_directory, identity.clone())?;

    assert_eq!(
        stream.accept(submission(&identity, 0, b"durable payload".to_vec()))?,
        AcceptOutcome::Accepted
    );
    drop(stream);

    let mut recovered = LocalLogStagingStream::open(&data_directory, identity.clone())?;
    assert_eq!(
        recovered.accept(submission(&identity, 0, b"durable payload".to_vec()))?,
        AcceptOutcome::AlreadyAccepted
    );
    assert_eq!(
        recovered.replay(),
        vec![accepted(&identity, 0, b"durable payload".to_vec())]
    );
    assert!(recovered
        .accept(submission(&identity, 0, b"different bytes".to_vec()))
        .is_err());
    Ok(())
}

#[test]
fn committed_frontier_replays_only_contiguous_accepted_and_gap_records() -> Result<()> {
    let (_temporary, data_directory) = admitted_data_directory()?;
    let identity = stream_identity();
    let mut stream = LocalLogStagingStream::open(&data_directory, identity.clone())?;

    stream.accept(LogRecordSubmission::new(
        record(&identity, 2),
        b"third".to_vec(),
    ))?;
    assert_eq!(stream.committed_frontier(), None);
    assert!(stream.replay().is_empty());

    stream.declare_pre_acceptance_gap(gap(&identity, 0, 1))?;
    assert_eq!(stream.committed_frontier(), Some(2));
    assert_eq!(
        stream.replay(),
        vec![
            ReplayedLogRecord::PreAcceptanceGap(gap(&identity, 0, 1)),
            accepted(&identity, 2, b"third".to_vec()),
        ]
    );
    assert!(stream
        .declare_pre_acceptance_gap(gap(&identity, 2, 2))
        .is_err());
    Ok(())
}

#[test]
fn recovery_preserves_clean_end_or_records_abnormal_closure() -> Result<()> {
    let (_temporary, data_directory) = admitted_data_directory()?;
    let abnormal_identity = stream_identity();
    let mut abnormal = LocalLogStagingStream::open(&data_directory, abnormal_identity.clone())?;
    abnormal.accept(LogRecordSubmission::new(
        record(&abnormal_identity, 0),
        b"before crash".to_vec(),
    ))?;
    drop(abnormal);

    let mut recovered = LocalLogStagingStream::open(&data_directory, abnormal_identity.clone())?;
    assert_eq!(
        recovered.recover_abnormal_closure()?,
        TerminalOutcome::Recorded
    );
    assert_eq!(
        recovered.recover_abnormal_closure()?,
        TerminalOutcome::AlreadyRecorded
    );
    assert!(matches!(
        recovered.replay().last(),
        Some(ReplayedLogRecord::Terminal {
            terminal: LogTerminal::AbnormalClosure {
                committed_frontier: Some(0)
            },
            ..
        })
    ));

    let clean_identity = stream_identity();
    let mut clean = LocalLogStagingStream::open(&data_directory, clean_identity.clone())?;
    clean.accept(LogRecordSubmission::new(
        record(&clean_identity, 0),
        b"complete".to_vec(),
    ))?;
    clean.finish_cleanly(LogExit::Status(0))?;
    drop(clean);

    let mut clean_recovered = LocalLogStagingStream::open(&data_directory, clean_identity)?;
    assert_eq!(
        clean_recovered.recover_abnormal_closure()?,
        TerminalOutcome::AlreadyRecorded
    );
    assert!(matches!(
        clean_recovered.replay().last(),
        Some(ReplayedLogRecord::Terminal {
            terminal: LogTerminal::EndOfStream {
                exit: LogExit::Status(0)
            },
            ..
        })
    ));
    Ok(())
}

#[test]
fn restart_discards_only_an_incomplete_unaccepted_append_tail() -> Result<()> {
    let (temporary, data_directory) = admitted_data_directory()?;
    let identity = stream_identity();
    let mut stream = LocalLogStagingStream::open(&data_directory, identity.clone())?;
    stream.accept(LogRecordSubmission::new(
        record(&identity, 0),
        b"safe".to_vec(),
    ))?;
    drop(stream);

    let journal = temporary.path().join(format!(
        "logs/staged/{}-{}.journal",
        identity.task_instance_id, identity.pickup_generation
    ));
    let mut file = OpenOptions::new().append(true).open(journal)?;
    file.write_all(&[0x20, 0, 0])?;
    file.sync_data()?;
    drop(file);

    let recovered = LocalLogStagingStream::open(&data_directory, identity.clone())?;
    assert_eq!(
        recovered.replay(),
        vec![accepted(&identity, 0, b"safe".to_vec())]
    );
    Ok(())
}

#[tokio::test]
async fn real_child_stdout_drains_concurrently_and_completion_precedes_publication() -> Result<()> {
    let (_temporary, data_directory) = admitted_data_directory()?;
    let identity = stream_identity();
    let stream = Arc::new(Mutex::new(LocalLogStagingStream::open(
        &data_directory,
        identity.clone(),
    )?));

    let mut child = Command::new("sh")
        .args(["-c", "printf 'one\\ntwo\\nthree\\n'"])
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().expect("child stdout is piped");
    let drain_stream = Arc::clone(&stream);
    let drain_identity = identity.clone();
    let drain = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut sequence = 0;
        while let Some(line) = lines.next_line().await? {
            drain_stream
                .lock()
                .expect("log stream lock is not poisoned")
                .accept(LogRecordSubmission::new(
                    record(&drain_identity, sequence),
                    line.into_bytes(),
                ))?;
            sequence += 1;
            // Publication remains asynchronous to process completion. This
            // deliberately keeps the drain alive after the tiny child exits.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok::<_, anyhow::Error>(())
    });

    let status = tokio::time::timeout(Duration::from_secs(2), child.wait()).await??;
    assert!(status.success());
    assert!(
        !drain.is_finished(),
        "task completion must not await log publication"
    );
    drain.await??;

    let mut stream = Arc::try_unwrap(stream)
        .map_err(|_| anyhow::anyhow!("drain retained its stream reference"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("log stream lock is poisoned"))?;
    stream.finish_cleanly(LogExit::Status(0))?;
    assert_eq!(stream.committed_frontier(), Some(2));
    assert_eq!(stream.replay().len(), 4);
    Ok(())
}

#[test]
fn sealing_and_final_installation_survive_retries_and_detect_corruption() -> Result<()> {
    let (temporary, data_directory) = admitted_data_directory()?;
    let identity = stream_identity();
    let mut stream = LocalLogStagingStream::open(&data_directory, identity.clone())?;
    stream.accept(LogRecordSubmission::new(
        record(&identity, 0),
        b"first".to_vec(),
    ))?;
    stream.accept(LogRecordSubmission::new(
        record(&identity, 2),
        b"third".to_vec(),
    ))?;
    stream.declare_pre_acceptance_gap(gap(&identity, 1, 1))?;
    stream.finish_cleanly(LogExit::Status(0))?;
    let seal = stream.seal()?;
    assert_eq!(
        seal.accepted_records()
            .iter()
            .map(|record| record.identity.sequence)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    drop(stream);

    let mut recovered = LocalLogStagingStream::open(&data_directory, identity.clone())?;
    assert_eq!(recovered.seal()?, seal);

    let partial = temporary.path().join(format!(
        "tmp/final-logs/{}-{}.log.json.tmp",
        identity.task_instance_id, identity.pickup_generation
    ));
    data_directory.ensure_directory(&RootRelativePath::new("tmp/final-logs")?)?;
    fs::write(&partial, b"incomplete")?;
    fs::set_permissions(&partial, std::fs::Permissions::from_mode(0o600))?;

    let error = LocalLogStagingStream::install_final(&data_directory, &seal)
        .expect_err("partial temporary file must be quarantined loudly");
    assert!(error.to_string().contains("quarantined"));
    assert!(!partial.exists());
    assert!(fs::read_dir(temporary.path().join("quarantine"))?
        .next()
        .is_some());

    let reference = LocalLogStagingStream::install_final(&data_directory, &seal)?;
    assert_eq!(
        LocalLogStagingStream::install_final(&data_directory, &seal)?,
        reference
    );
    LocalLogStagingStream::verify_final(&data_directory, &reference)?;

    let final_log = temporary.path().join(format!(
        "logs/final/{}-{}.log.json",
        identity.task_instance_id, identity.pickup_generation
    ));
    fs::write(&final_log, b"corrupt")?;
    fs::set_permissions(&final_log, std::fs::Permissions::from_mode(0o600))?;
    assert!(LocalLogStagingStream::verify_final(&data_directory, &reference).is_err());

    let mut unknown_protocol = reference.clone();
    unknown_protocol.protocol_identity = "unknown".to_owned();
    assert!(LocalLogStagingStream::verify_final(&data_directory, &unknown_protocol).is_err());
    Ok(())
}
