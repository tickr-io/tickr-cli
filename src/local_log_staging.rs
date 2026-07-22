//! Durable local Log staging stream for one Task instance pickup generation.
//!
//! The journal accepts a record only after its framed identity and payload have
//! reached `sync_data`. A later retry reads the same identity and therefore
//! cannot append a second copy after an ambiguous acknowledgement.

use crate::data_directory::{DataDirectory, DataDirectoryError, RootRelativePath};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use uuid::Uuid;

const JOURNAL_HEADER: &[u8] = b"tickr-local-log-v1\n";
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const FINAL_LOG_PROTOCOL: &str = "tickr-local-final-log-v1";

/// Stable identity of one Log staging stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogStreamIdentity {
    pub task_instance_id: Uuid,
    pub pickup_generation: u64,
}

/// Stable identity of one record accepted by a Log staging stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogRecordIdentity {
    pub stream: LogStreamIdentity,
    pub sequence: u64,
}

/// The executor's observed controlled process exit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LogExit {
    Status(i32),
    NoStatus,
    Error(String),
}

/// One durable item available to replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReplayedLogRecord {
    Accepted {
        identity: LogRecordIdentity,
        bytes: Vec<u8>,
    },
    PreAcceptanceGap {
        stream: LogStreamIdentity,
        first_sequence: u64,
        last_sequence: u64,
        dropped_records: u64,
    },
    EndOfStream {
        exit: LogExit,
    },
    AbnormalClosure {
        committed_frontier: Option<u64>,
    },
}

/// The outcome of accepting a payload identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptOutcome {
    Accepted,
    AlreadyAccepted,
}

/// One accepted record frozen into a final Log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AcceptedLogRecord {
    pub identity: LogRecordIdentity,
    pub bytes: Vec<u8>,
}

/// The terminal state written beside a final Log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FinalLogTerminal {
    EndOfStream { exit: LogExit },
    AbnormalClosure { committed_frontier: Option<u64> },
}

/// Read-only final-Log view used by the same-process API role.
pub struct FinalLogSnapshot {
    pub records: Vec<AcceptedLogRecord>,
    pub terminal: FinalLogTerminal,
}

/// Immutable accepted-record snapshot of one staged Log stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogSeal {
    stream: LogStreamIdentity,
    accepted_records: Vec<AcceptedLogRecord>,
    record_digest: String,
    terminal: FinalLogTerminal,
}

impl LogSeal {
    pub fn stream(&self) -> &LogStreamIdentity {
        &self.stream
    }

    pub fn accepted_records(&self) -> &[AcceptedLogRecord] {
        &self.accepted_records
    }

    pub fn record_digest(&self) -> &str {
        &self.record_digest
    }

    pub fn terminal(&self) -> &FinalLogTerminal {
        &self.terminal
    }
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
    Accepted {
        identity: LogRecordIdentity,
        bytes: Vec<u8>,
    },
    PreAcceptanceGap {
        first_sequence: u64,
        last_sequence: u64,
        dropped_records: u64,
    },
    EndOfStream {
        exit: LogExit,
    },
    AbnormalClosure {
        committed_frontier: Option<u64>,
    },
    Seal {
        record_digest: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Coverage {
    Accepted(Vec<u8>),
    Gap,
}

/// One opened local Log staging stream.
///
/// Each stream owns its journal file. Callers serialize access to an instance;
/// the intended formation wiring has one writer for a Task instance pickup.
pub struct LocalLogStagingStream {
    identity: LogStreamIdentity,
    journal: File,
    coverage: BTreeMap<u64, Coverage>,
    gaps: BTreeMap<u64, (u64, u64)>,
    committed_frontier: Option<u64>,
    terminal: Option<JournalFrame>,
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
            identity,
            journal,
            coverage: BTreeMap::new(),
            gaps: BTreeMap::new(),
            committed_frontier: None,
            terminal: None,
            seal: None,
        };
        stream.recover()?;
        Ok(stream)
    }

    pub fn identity(&self) -> &LogStreamIdentity {
        &self.identity
    }

    pub fn committed_frontier(&self) -> Option<u64> {
        self.committed_frontier
    }

    /// Freeze the complete accepted record set after the stream has reached a
    /// durable terminal state. Repeating the seal returns the same digest.
    pub fn seal(&mut self) -> Result<LogSeal> {
        if self.terminal.is_none() {
            bail!("cannot seal a local log staging stream before its terminal record");
        }
        let seal = self.build_seal()?;
        match &self.seal {
            Some(record_digest) if record_digest == &seal.record_digest => Ok(seal),
            Some(_) => bail!("stored local log seal digest does not match accepted records"),
            None => {
                self.append_frame(&JournalFrame::Seal {
                    record_digest: seal.record_digest.clone(),
                })?;
                self.seal = Some(seal.record_digest.clone());
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
        if digest_json(&seal.accepted_records)? != seal.record_digest {
            bail!("final log seal digest does not match accepted record set");
        }

        let log = FinalLogDocument {
            protocol_identity: FINAL_LOG_PROTOCOL.to_owned(),
            stream: seal.stream.clone(),
            record_digest: seal.record_digest.clone(),
            records: seal.accepted_records.clone(),
        };
        let exit = FinalLogExitMetadata {
            protocol_identity: FINAL_LOG_PROTOCOL.to_owned(),
            stream: seal.stream.clone(),
            terminal: seal.terminal.clone(),
        };
        let log_bytes = serde_json::to_vec(&log).context("encode final log")?;
        let exit_bytes = serde_json::to_vec(&exit).context("encode final log exit metadata")?;

        install_final_file(
            data_directory,
            &final_log_temporary_path(&seal.stream)?,
            &final_log_path(&seal.stream)?,
            &log_bytes,
        )?;
        install_final_file(
            data_directory,
            &final_exit_temporary_path(&seal.stream)?,
            &final_exit_path(&seal.stream)?,
            &exit_bytes,
        )?;

        Ok(FinalLogReference {
            protocol_identity: FINAL_LOG_PROTOCOL.to_owned(),
            stream: seal.stream.clone(),
            record_digest: seal.record_digest.clone(),
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

    /// Append and sync a payload record. The returned success is the acceptance
    /// boundary: a retry of the same identity is a lookup, never a new append.
    pub fn accept(&mut self, identity: LogRecordIdentity, bytes: Vec<u8>) -> Result<AcceptOutcome> {
        self.require_stream(&identity)?;
        self.require_open()?;

        match self.coverage.get(&identity.sequence) {
            Some(Coverage::Accepted(existing)) if existing == &bytes => {
                return Ok(AcceptOutcome::AlreadyAccepted)
            }
            Some(Coverage::Accepted(_)) => {
                bail!("log record identity was accepted with different bytes")
            }
            Some(Coverage::Gap) => {
                bail!("log record identity is already represented by a pre-acceptance gap")
            }
            None => {}
        }

        let frame = JournalFrame::Accepted {
            identity: identity.clone(),
            bytes: bytes.clone(),
        };
        self.append_frame(&frame)?;
        self.coverage
            .insert(identity.sequence, Coverage::Accepted(bytes));
        self.advance_frontier();
        Ok(AcceptOutcome::Accepted)
    }

    /// Record telemetry that was deliberately discarded before acceptance.
    ///
    /// A gap occupies the missing sequence range so a later contiguous record
    /// can advance the frontier. It is rejected if any byte in that range was
    /// already accepted, preserving the distinction between loss and accepted
    /// payload.
    pub fn declare_pre_acceptance_gap(
        &mut self,
        first_sequence: u64,
        last_sequence: u64,
        dropped_records: u64,
    ) -> Result<()> {
        self.require_open()?;
        if first_sequence > last_sequence || dropped_records == 0 {
            bail!("a pre-acceptance gap needs a non-empty sequence range and drop count")
        }
        if self
            .coverage
            .range(first_sequence..=last_sequence)
            .next()
            .is_some()
        {
            bail!("a pre-acceptance gap overlaps accepted or previously declared data")
        }

        let frame = JournalFrame::PreAcceptanceGap {
            first_sequence,
            last_sequence,
            dropped_records,
        };
        self.append_frame(&frame)?;
        for sequence in first_sequence..=last_sequence {
            self.coverage.insert(sequence, Coverage::Gap);
        }
        self.gaps
            .insert(first_sequence, (last_sequence, dropped_records));
        self.advance_frontier();
        Ok(())
    }

    /// The executor writes the sole clean terminal marker after it has stopped
    /// producing stdout. A stream with a known hole cannot close cleanly: the
    /// caller must first record a pre-acceptance gap.
    pub fn finish_cleanly(&mut self, exit: LogExit) -> Result<()> {
        match &self.terminal {
            Some(JournalFrame::EndOfStream { exit: existing }) if existing == &exit => {
                return Ok(())
            }
            Some(_) => bail!("a local log staging stream already has a terminal record"),
            None => {}
        }
        if self.frontier_has_hole() {
            bail!("cannot write End-of-stream while the committed frontier has a hole")
        }

        let frame = JournalFrame::EndOfStream { exit };
        self.append_frame(&frame)?;
        self.terminal = Some(frame);
        Ok(())
    }

    /// Recovery records the abnormal terminal state instead of inventing a
    /// clean End-of-stream marker when the executor did not durably write one.
    /// Returns whether it appended the new abnormal-closure record.
    pub fn recover_abnormal_closure(&mut self) -> Result<bool> {
        if self.terminal.is_some() {
            return Ok(false);
        }
        let frame = JournalFrame::AbnormalClosure {
            committed_frontier: self.committed_frontier,
        };
        self.append_frame(&frame)?;
        self.terminal = Some(frame);
        Ok(true)
    }

    /// Replay only committed contiguous data, in sequence order, followed by a
    /// terminal record if one exists. Accepted records beyond a hole remain
    /// durable but invisible until the missing range is accepted or gapped.
    pub fn replay(&self) -> Vec<ReplayedLogRecord> {
        let mut records = Vec::new();
        let Some(frontier) = self.committed_frontier else {
            return self.terminal_record(records);
        };

        let mut sequence = 0;
        while sequence <= frontier {
            match self.coverage.get(&sequence) {
                Some(Coverage::Accepted(bytes)) => records.push(ReplayedLogRecord::Accepted {
                    identity: LogRecordIdentity {
                        stream: self.identity.clone(),
                        sequence,
                    },
                    bytes: bytes.clone(),
                }),
                Some(Coverage::Gap) => {
                    if let Some((last_sequence, dropped_records)) = self.gaps.get(&sequence) {
                        records.push(ReplayedLogRecord::PreAcceptanceGap {
                            stream: self.identity.clone(),
                            first_sequence: sequence,
                            last_sequence: *last_sequence,
                            dropped_records: *dropped_records,
                        });
                    }
                }
                None => unreachable!("the committed frontier is contiguous"),
            }
            sequence = sequence.checked_add(1).expect("u64 frontier overflow");
        }
        self.terminal_record(records)
    }

    fn terminal_record(&self, mut records: Vec<ReplayedLogRecord>) -> Vec<ReplayedLogRecord> {
        match &self.terminal {
            Some(JournalFrame::EndOfStream { exit }) => {
                records.push(ReplayedLogRecord::EndOfStream { exit: exit.clone() })
            }
            Some(JournalFrame::AbnormalClosure { committed_frontier }) => {
                records.push(ReplayedLogRecord::AbnormalClosure {
                    committed_frontier: *committed_frontier,
                })
            }
            _ => {}
        }
        records
    }

    fn require_stream(&self, identity: &LogRecordIdentity) -> Result<()> {
        if identity.stream != self.identity {
            bail!("log record identity belongs to another Task instance pickup generation")
        }
        Ok(())
    }

    fn require_open(&self) -> Result<()> {
        if self.terminal.is_some() {
            bail!("cannot append to a terminal local log staging stream")
        }
        Ok(())
    }

    fn frontier_has_hole(&self) -> bool {
        match (self.committed_frontier, self.coverage.keys().next_back()) {
            (None, Some(_)) => true,
            (Some(frontier), Some(last)) => frontier != *last,
            (_, None) => false,
        }
    }

    fn advance_frontier(&mut self) {
        let mut next = self.committed_frontier.map_or(0, |frontier| frontier + 1);
        while self.coverage.contains_key(&next) {
            self.committed_frontier = Some(next);
            next = match next.checked_add(1) {
                Some(next) => next,
                None => break,
            };
        }
    }

    fn build_seal(&self) -> Result<LogSeal> {
        let accepted_records = self
            .coverage
            .iter()
            .filter_map(|(sequence, coverage)| match coverage {
                Coverage::Accepted(bytes) => Some(AcceptedLogRecord {
                    identity: LogRecordIdentity {
                        stream: self.identity.clone(),
                        sequence: *sequence,
                    },
                    bytes: bytes.clone(),
                }),
                Coverage::Gap => None,
            })
            .collect::<Vec<_>>();
        let terminal = match &self.terminal {
            Some(JournalFrame::EndOfStream { exit }) => {
                FinalLogTerminal::EndOfStream { exit: exit.clone() }
            }
            Some(JournalFrame::AbnormalClosure { committed_frontier }) => {
                FinalLogTerminal::AbnormalClosure {
                    committed_frontier: *committed_frontier,
                }
            }
            _ => bail!("cannot seal a local log staging stream before its terminal record"),
        };
        Ok(LogSeal {
            stream: self.identity.clone(),
            record_digest: digest_json(&accepted_records)?,
            accepted_records,
            terminal,
        })
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
        self.advance_frontier();
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
        if self.terminal.is_some() && !matches!(frame, JournalFrame::Seal { .. }) {
            bail!("local log staging journal contains a record after its terminal record");
        }

        match &frame {
            JournalFrame::Accepted { identity, bytes } => {
                self.require_stream(identity)?;
                match self.coverage.get(&identity.sequence) {
                    Some(Coverage::Accepted(existing)) if existing == bytes => {}
                    Some(_) => {
                        bail!("duplicate local log staging identity conflicts during recovery")
                    }
                    None => {
                        self.coverage
                            .insert(identity.sequence, Coverage::Accepted(bytes.clone()));
                    }
                }
            }
            JournalFrame::PreAcceptanceGap {
                first_sequence,
                last_sequence,
                dropped_records,
            } => {
                if first_sequence > last_sequence || *dropped_records == 0 {
                    bail!("invalid recovered pre-acceptance gap")
                }
                if self
                    .coverage
                    .range(*first_sequence..=*last_sequence)
                    .next()
                    .is_some()
                {
                    bail!("recovered pre-acceptance gap overlaps durable data")
                }
                for sequence in *first_sequence..=*last_sequence {
                    self.coverage.insert(sequence, Coverage::Gap);
                }
                self.gaps
                    .insert(*first_sequence, (*last_sequence, *dropped_records));
            }
            JournalFrame::EndOfStream { .. } | JournalFrame::AbnormalClosure { .. } => {
                if self.terminal.replace(frame.clone()).is_some() {
                    bail!("multiple local log staging terminal records")
                }
            }
            JournalFrame::Seal { record_digest } => {
                if self.terminal.is_none() {
                    bail!("local log staging journal seals records before its terminal record");
                }
                let computed = self.build_seal()?.record_digest;
                if &computed != record_digest {
                    bail!("local log staging journal seal digest does not match accepted records");
                }
                if self.seal.replace(record_digest.clone()).is_some() {
                    bail!("multiple local log staging seals");
                }
            }
        }
        Ok(())
    }
}

fn digest_json(value: &impl Serialize) -> Result<String> {
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
