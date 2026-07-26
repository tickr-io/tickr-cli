//! Durable local Log staging stream for one Task instance pickup generation.
//!
//! The journal accepts a record only after its framed identity and payload have
//! reached `sync_data`. A later retry reads the same identity and therefore
//! cannot append a second copy after an ambiguous acknowledgement.

use crate::data_directory::{DataDirectory, DataDirectoryError, RootRelativePath};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
pub use tickr_executor::log_stream::LogStream;
pub use tickr_proto::coord::log_stream::{
    accepted_record_digest, content_digest, AcceptOutcome, AcceptedLogRecord, GapOutcome, LogExit,
    LogRecordIdentity, LogRecordSubmission, LogSeal, LogStreamIdentity, LogTerminal,
    PreAcceptanceGap, ReplayedLogRecord, TerminalOutcome,
};
use uuid::Uuid;

const JOURNAL_HEADER: &[u8] = b"tickr-local-log-v2\n";
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const FINAL_LOG_PROTOCOL: &str = "tickr-local-final-log-v2";

/// Terminal metadata written beside a final Log.
pub type FinalLogTerminal = LogTerminal;

/// Read-only final-Log view used by the same-process API role.
pub struct FinalLogSnapshot {
    pub records: Vec<AcceptedLogRecord>,
    pub terminal: FinalLogTerminal,
}

/// Backend-neutral identity and integrity evidence for installed final Log files.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FinalLogReference {
    pub protocol_identity: String,
    pub stream: LogStreamIdentity,
    pub record_digest: String,
    pub final_log_digest: String,
    pub exit_metadata_digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FinalLogDocument {
    protocol_identity: String,
    stream: LogStreamIdentity,
    record_digest: String,
    records: Vec<AcceptedLogRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FinalLogExitMetadata {
    protocol_identity: String,
    stream: LogStreamIdentity,
    terminal: FinalLogTerminal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum JournalFrame {
    Accepted(LogRecordSubmission),
    PreAcceptanceGap(PreAcceptanceGap),
    Terminal(LogTerminal),
    Seal { record_digest: String },
}

/// One opened local Log staging stream.
///
/// Each stream owns its journal file. Callers serialize access to an instance;
/// the intended formation wiring has one writer for a Task instance pickup.
pub struct LocalLogStagingStream {
    state: tickr_proto::coord::log_stream::LogStreamState,
    journal: File,
    seal: Option<String>,
}

impl LocalLogStagingStream {
    /// Open the durable journal for `identity`, creating and syncing it first
    /// when this is its initial use.
    pub fn open(data_directory: &DataDirectory, identity: LogStreamIdentity) -> Result<Self> {
        let journal_path = journal_path(&identity)?;
        let journal = data_directory
            .open_or_create_file(&journal_path)
            .context("open local log staging journal")?;
        Self::recover_from(identity, journal)
    }

    /// Open a journal that must already exist.
    ///
    /// Compaction uses this boundary so missing Log state fails instead of
    /// creating an empty substitute during archival.
    pub fn open_existing(
        data_directory: &DataDirectory,
        identity: LogStreamIdentity,
    ) -> Result<Self> {
        let journal_path = journal_path(&identity)?;
        let journal = data_directory
            .open_existing_file(&journal_path, true)
            .context("open existing local log staging journal")?;
        Self::recover_from(identity, journal)
    }

    fn recover_from(identity: LogStreamIdentity, journal: File) -> Result<Self> {
        let mut stream = Self {
            state: tickr_proto::coord::log_stream::LogStreamState::new(identity),
            journal,
            seal: None,
        };
        stream.recover()?;
        Ok(stream)
    }

    pub fn identity(&self) -> &LogStreamIdentity {
        self.state.identity()
    }

    pub fn committed_frontier(&self) -> Option<u64> {
        self.state.committed_frontier()
    }

    /// Freeze the complete accepted record set after the stream has reached a
    /// durable terminal state. Repeating the seal returns the same digest.
    pub fn seal(&mut self) -> Result<LogSeal> {
        if self.state.terminal().is_none() {
            bail!("cannot seal a local log staging stream before its terminal record");
        }
        let seal = self.build_seal()?;
        match &self.seal {
            Some(record_digest) if record_digest == seal.record_digest() => Ok(seal),
            Some(_) => bail!("stored local log seal digest does not match accepted records"),
            None => {
                self.append_frame(&JournalFrame::Seal {
                    record_digest: seal.record_digest().to_owned(),
                })?;
                self.seal = Some(seal.record_digest().to_owned());
                Ok(seal)
            }
        }
    }

    /// Install a sealed Log and its terminal metadata beneath the admitted data
    /// directory. The returned reference identifies content, not a filesystem
    /// location, so archive storage remains backend-neutral.
    pub fn install_final(
        data_directory: &DataDirectory,
        seal: &LogSeal,
    ) -> Result<FinalLogReference> {
        if accepted_record_digest(seal.accepted_records()) != seal.record_digest() {
            bail!("final log seal digest does not match accepted record set");
        }

        let log = FinalLogDocument {
            protocol_identity: FINAL_LOG_PROTOCOL.to_owned(),
            stream: seal.stream().clone(),
            record_digest: seal.record_digest().to_owned(),
            records: seal.accepted_records().to_vec(),
        };
        let exit = FinalLogExitMetadata {
            protocol_identity: FINAL_LOG_PROTOCOL.to_owned(),
            stream: seal.stream().clone(),
            terminal: seal.terminal().clone(),
        };
        let log_bytes = serde_json::to_vec(&log).context("encode final log")?;
        let exit_bytes = serde_json::to_vec(&exit).context("encode final log exit metadata")?;

        install_final_file(
            data_directory,
            &final_log_temporary_path(seal.stream())?,
            &final_log_path(seal.stream())?,
            &log_bytes,
        )?;
        install_final_file(
            data_directory,
            &final_exit_temporary_path(seal.stream())?,
            &final_exit_path(seal.stream())?,
            &exit_bytes,
        )?;

        Ok(FinalLogReference {
            protocol_identity: FINAL_LOG_PROTOCOL.to_owned(),
            stream: seal.stream().clone(),
            record_digest: seal.record_digest().to_owned(),
            final_log_digest: digest(&log_bytes),
            exit_metadata_digest: digest(&exit_bytes),
        })
    }

    /// Re-read installed files and verify the sealed identity and every digest
    /// before a stored final-Log reference is trusted.
    pub fn verify_final(
        data_directory: &DataDirectory,
        reference: &FinalLogReference,
    ) -> Result<()> {
        if reference.protocol_identity != FINAL_LOG_PROTOCOL {
            bail!("unknown final log protocol identity");
        }
        let log_bytes = read_required_file(data_directory, &final_log_path(&reference.stream)?)?;
        if digest(&log_bytes) != reference.final_log_digest {
            bail!("installed final log digest mismatch");
        }
        let log: FinalLogDocument =
            serde_json::from_slice(&log_bytes).context("decode installed final log")?;
        if log.protocol_identity != FINAL_LOG_PROTOCOL {
            bail!("unknown installed final log protocol identity");
        }
        if log.stream != reference.stream || log.record_digest != reference.record_digest {
            bail!("installed final log identity does not match its reference");
        }
        if digest_json(&log.records)? != reference.record_digest {
            bail!("installed final log accepted record digest mismatch");
        }

        let exit_bytes = read_required_file(data_directory, &final_exit_path(&reference.stream)?)?;
        if digest(&exit_bytes) != reference.exit_metadata_digest {
            bail!("installed final log exit metadata digest mismatch");
        }
        let exit: FinalLogExitMetadata = serde_json::from_slice(&exit_bytes)
            .context("decode installed final log exit metadata")?;
        if exit.protocol_identity != FINAL_LOG_PROTOCOL {
            bail!("unknown installed final log exit metadata protocol identity");
        }
        if exit.stream != reference.stream {
            bail!("installed final log exit metadata identity does not match its reference");
        }
        Ok(())
    }

    /// Purge a staged journal only after the installed final files have been
    /// re-verified. Repeating cleanup after a crash is a no-op once the
    /// journal has gone.
    pub fn purge_staged(
        data_directory: &DataDirectory,
        reference: &FinalLogReference,
    ) -> Result<()> {
        Self::verify_final(data_directory, reference)?;
        let journal_path = journal_path(&reference.stream)?;
        if read_optional_file(data_directory, &journal_path)?.is_some() {
            data_directory
                .remove_file(&journal_path)
                .context("purge local log staging journal")?;
        }
        Ok(())
    }

    /// Append and sync an Accepted Log record. The returned success is the
    /// acceptance boundary: retrying the same identity and digest is a lookup.
    pub fn accept(&mut self, submission: LogRecordSubmission) -> Result<AcceptOutcome> {
        let mut prospective = self.state.clone();
        let outcome = prospective.apply_accepted(submission.clone())?;
        if outcome == AcceptOutcome::AlreadyAccepted {
            return Ok(outcome);
        }
        self.append_frame(&JournalFrame::Accepted(submission))?;
        self.state = prospective;
        Ok(AcceptOutcome::Accepted)
    }

    /// Durably cover telemetry discarded before acceptance.
    pub fn declare_pre_acceptance_gap(&mut self, gap: PreAcceptanceGap) -> Result<GapOutcome> {
        let mut prospective = self.state.clone();
        let outcome = prospective.apply_gap(gap.clone())?;
        if outcome == GapOutcome::AlreadyDeclared {
            return Ok(outcome);
        }
        self.append_frame(&JournalFrame::PreAcceptanceGap(gap))?;
        self.state = prospective;
        Ok(GapOutcome::Declared)
    }

    /// Write the sole controlled terminal record after stdout production ends
    /// and all pre-acceptance loss has been covered by a durable gap.
    pub fn finish_cleanly(&mut self, exit: LogExit) -> Result<TerminalOutcome> {
        let terminal = LogTerminal::EndOfStream { exit };
        let mut prospective = self.state.clone();
        let outcome = prospective.apply_terminal(terminal.clone())?;
        if outcome == TerminalOutcome::AlreadyRecorded {
            return Ok(outcome);
        }
        self.append_frame(&JournalFrame::Terminal(terminal))?;
        self.state = prospective;
        Ok(TerminalOutcome::Recorded)
    }

    /// Recovery records abnormal closure rather than manufacturing a clean
    /// End-of-stream after the owning Executor disappeared.
    pub fn recover_abnormal_closure(&mut self) -> Result<TerminalOutcome> {
        if self.state.terminal().is_some() {
            return Ok(TerminalOutcome::AlreadyRecorded);
        }
        let terminal = LogTerminal::AbnormalClosure {
            committed_frontier: self.state.committed_frontier(),
        };
        let mut prospective = self.state.clone();
        prospective.apply_terminal(terminal.clone())?;
        self.append_frame(&JournalFrame::Terminal(terminal))?;
        self.state = prospective;
        Ok(TerminalOutcome::Recorded)
    }

    /// Replay committed records and gaps in identity order, followed by the
    /// durable terminal record when present.
    pub fn replay(&self) -> Vec<ReplayedLogRecord> {
        self.state.replay()
    }

    fn build_seal(&self) -> Result<LogSeal> {
        Ok(self.state.seal()?)
    }

    fn append_frame(&mut self, frame: &JournalFrame) -> Result<()> {
        let payload = serde_json::to_vec(frame).context("encode local log staging frame")?;
        if payload.len() > MAX_FRAME_BYTES {
            bail!("local log staging frame exceeds the admitted maximum size")
        }
        self.journal
            .seek(SeekFrom::End(0))
            .context("seek local log staging journal")?;
        self.journal
            .write_all(&(payload.len() as u32).to_le_bytes())
            .context("append local log staging frame length")?;
        self.journal
            .write_all(&payload)
            .context("append local log staging frame")?;
        self.journal
            .sync_data()
            .context("sync accepted local log staging frame")?;
        Ok(())
    }

    fn recover(&mut self) -> Result<()> {
        let length = self
            .journal
            .metadata()
            .context("inspect local log staging journal")?
            .len();
        if length == 0 {
            self.journal
                .write_all(JOURNAL_HEADER)
                .context("write local log staging journal header")?;
            self.journal
                .sync_all()
                .context("sync local log staging journal header")?;
            return Ok(());
        }

        self.journal
            .seek(SeekFrom::Start(0))
            .context("seek local log staging journal header")?;
        let mut header = vec![0; JOURNAL_HEADER.len()];
        self.journal
            .read_exact(&mut header)
            .context("read local log staging journal header")?;
        if header != JOURNAL_HEADER {
            bail!("unknown local log staging journal format")
        }

        let mut last_good_offset = JOURNAL_HEADER.len() as u64;
        loop {
            let frame_offset = last_good_offset;
            let mut length_bytes = [0; 4];
            match self.journal.read_exact(&mut length_bytes) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.truncate_unaccepted_tail(frame_offset)?;
                    break;
                }
                Err(error) => return Err(error).context("read local log staging frame length"),
            }
            let frame_len = u32::from_le_bytes(length_bytes) as usize;
            if frame_len > MAX_FRAME_BYTES {
                bail!("local log staging frame exceeds the admitted maximum size")
            }
            let mut payload = vec![0; frame_len];
            match self.journal.read_exact(&mut payload) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.truncate_unaccepted_tail(frame_offset)?;
                    break;
                }
                Err(error) => return Err(error).context("read local log staging frame"),
            }
            let frame: JournalFrame =
                serde_json::from_slice(&payload).context("decode local log staging frame")?;
            self.apply_recovered_frame(frame)?;
            last_good_offset = frame_offset + 4 + frame_len as u64;
        }
        Ok(())
    }

    fn truncate_unaccepted_tail(&mut self, offset: u64) -> Result<()> {
        self.journal
            .set_len(offset)
            .context("truncate incomplete unaccepted local log staging tail")?;
        self.journal
            .sync_data()
            .context("sync truncated local log staging tail")?;
        self.journal
            .seek(SeekFrom::End(0))
            .context("seek recovered local log staging journal")?;
        Ok(())
    }

    fn apply_recovered_frame(&mut self, frame: JournalFrame) -> Result<()> {
        if self.seal.is_some() {
            bail!("local log staging journal contains a record after its seal");
        }
        if self.state.terminal().is_some() && !matches!(frame, JournalFrame::Seal { .. }) {
            bail!("local log staging journal contains a record after its terminal record");
        }

        match frame {
            JournalFrame::Accepted(submission) => {
                self.state.apply_accepted(submission)?;
            }
            JournalFrame::PreAcceptanceGap(gap) => {
                self.state.apply_gap(gap)?;
            }
            JournalFrame::Terminal(terminal) => {
                self.state.apply_terminal(terminal)?;
            }
            JournalFrame::Seal { record_digest } => {
                if self.state.terminal().is_none() {
                    bail!("local log staging journal seals records before its terminal record");
                }
                let computed = self.build_seal()?.record_digest().to_owned();
                if computed != record_digest {
                    bail!("local log staging journal seal digest does not match accepted records");
                }
                if self.seal.replace(record_digest).is_some() {
                    bail!("multiple local log staging seals");
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl LogStream for LocalLogStagingStream {
    fn identity(&self) -> &LogStreamIdentity {
        LocalLogStagingStream::identity(self)
    }

    fn committed_frontier(&self) -> Option<u64> {
        LocalLogStagingStream::committed_frontier(self)
    }

    async fn accept(&mut self, submission: LogRecordSubmission) -> Result<AcceptOutcome> {
        LocalLogStagingStream::accept(self, submission)
    }

    async fn declare_pre_acceptance_gap(&mut self, gap: PreAcceptanceGap) -> Result<GapOutcome> {
        LocalLogStagingStream::declare_pre_acceptance_gap(self, gap)
    }

    async fn finish_cleanly(&mut self, exit: LogExit) -> Result<TerminalOutcome> {
        LocalLogStagingStream::finish_cleanly(self, exit)
    }

    async fn recover_abnormal_closure(&mut self) -> Result<TerminalOutcome> {
        LocalLogStagingStream::recover_abnormal_closure(self)
    }

    async fn replay(&mut self) -> Result<Vec<ReplayedLogRecord>> {
        Ok(LocalLogStagingStream::replay(self))
    }
}

fn digest_json(value: &(impl Serialize + ?Sized)) -> Result<String> {
    Ok(digest(
        &serde_json::to_vec(value).context("encode final log digest input")?,
    ))
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_optional_file(
    data_directory: &DataDirectory,
    path: &RootRelativePath,
) -> Result<Option<Vec<u8>>> {
    match data_directory.open_existing_file(path, false) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)
                .with_context(|| format!("read {}", path.as_path().display()))?;
            Ok(Some(bytes))
        }
        Err(DataDirectoryError::Operation { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn read_required_file(data_directory: &DataDirectory, path: &RootRelativePath) -> Result<Vec<u8>> {
    read_optional_file(data_directory, path)?.ok_or_else(|| {
        anyhow!(
            "required final log file is missing: {}",
            path.as_path().display()
        )
    })
}

/// Read an installed local final-Log snapshot, if compaction has already
/// drained its staging journal.
pub fn read_final_log(
    data_directory: &DataDirectory,
    identity: &LogStreamIdentity,
) -> Result<Option<FinalLogSnapshot>> {
    let Some(log_bytes) = read_optional_file(data_directory, &final_log_path(identity)?)? else {
        return Ok(None);
    };
    let log: FinalLogDocument =
        serde_json::from_slice(&log_bytes).context("decode installed final log document")?;
    let exit_bytes = read_required_file(data_directory, &final_exit_path(identity)?)?;
    let exit: FinalLogExitMetadata =
        serde_json::from_slice(&exit_bytes).context("decode installed final log exit metadata")?;
    Ok(Some(FinalLogSnapshot {
        records: log.records,
        terminal: exit.terminal,
    }))
}

fn install_final_file(
    data_directory: &DataDirectory,
    temporary: &RootRelativePath,
    destination: &RootRelativePath,
    expected: &[u8],
) -> Result<()> {
    match read_optional_file(data_directory, destination)? {
        Some(installed) if installed == expected => {
            if read_optional_file(data_directory, temporary)?.is_some() {
                quarantine_temporary_file(data_directory, temporary)?;
                bail!(
                    "quarantined stale temporary final log file: {}",
                    temporary.as_path().display()
                );
            }
            data_directory
                .sync_parent(destination)
                .context("sync already installed final log parent")?;
            return Ok(());
        }
        Some(_) => bail!(
            "installed final log differs from the sealed record set: {}",
            destination.as_path().display()
        ),
        None => {}
    }

    match read_optional_file(data_directory, temporary)? {
        Some(existing) if existing == expected => {}
        Some(_) => {
            quarantine_temporary_file(data_directory, temporary)?;
            bail!(
                "quarantined incomplete or mismatched final log temporary file: {}",
                temporary.as_path().display()
            );
        }
        None => write_temporary_file(data_directory, temporary, expected)?,
    }
    data_directory
        .durable_replace(temporary, destination)
        .context("durably install final log file")
}

fn write_temporary_file(
    data_directory: &DataDirectory,
    temporary: &RootRelativePath,
    contents: &[u8],
) -> Result<()> {
    let mut file = data_directory
        .create_new_file(temporary)
        .context("create final log temporary file")?;
    file.write_all(contents)
        .context("write final log temporary file")?;
    file.sync_all().context("sync final log temporary file")?;
    Ok(())
}

fn quarantine_temporary_file(
    data_directory: &DataDirectory,
    temporary: &RootRelativePath,
) -> Result<()> {
    let name = temporary
        .as_path()
        .file_name()
        .ok_or_else(|| anyhow!("temporary final log file has no name"))?
        .to_string_lossy();
    let quarantine =
        RootRelativePath::new(format!("quarantine/{}-{}.partial", name, Uuid::new_v4()))?;
    data_directory
        .durable_replace(temporary, &quarantine)
        .context("quarantine invalid final log temporary file")
}

fn final_log_path(identity: &LogStreamIdentity) -> Result<RootRelativePath> {
    final_log_path_in(identity, "logs/final", "log.json")
}

fn final_exit_path(identity: &LogStreamIdentity) -> Result<RootRelativePath> {
    final_log_path_in(identity, "logs/final", "exit.json")
}

fn final_log_temporary_path(identity: &LogStreamIdentity) -> Result<RootRelativePath> {
    final_log_path_in(identity, "tmp/final-logs", "log.json.tmp")
}

fn final_exit_temporary_path(identity: &LogStreamIdentity) -> Result<RootRelativePath> {
    final_log_path_in(identity, "tmp/final-logs", "exit.json.tmp")
}

fn final_log_path_in(
    identity: &LogStreamIdentity,
    directory: &str,
    suffix: &str,
) -> Result<RootRelativePath> {
    RootRelativePath::new(format!(
        "{directory}/{}-{}.{}",
        identity.task_instance_id, identity.pickup_generation, suffix
    ))
    .map_err(|error| anyhow!(error))
}

fn journal_path(identity: &LogStreamIdentity) -> Result<RootRelativePath> {
    let mut path = PathBuf::from("logs/staged");
    path.push(format!(
        "{}-{}.journal",
        identity.task_instance_id, identity.pickup_generation
    ));
    RootRelativePath::new(path).map_err(|error| anyhow!(error))
}
