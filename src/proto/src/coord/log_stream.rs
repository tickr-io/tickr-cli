//! Backend-neutral accepted-Log identities and state laws.
//!
//! Adapters durably write one [`ReplayedLogRecord`] and only then apply it to
//! [`LogStreamState`]. The state model owns conflict detection, contiguous
//! committed-frontier advancement, terminal exclusivity, and identity-ordered
//! replay; storage adapters own only their durability boundary.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use uuid::Uuid;

/// Stable identity of one pickup generation's Log staging stream.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LogStreamIdentity {
    pub task_instance_id: Uuid,
    pub pickup_generation: u64,
}

/// Stable identity of one record submitted to a Log staging stream.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct LogRecordIdentity {
    pub stream: LogStreamIdentity,
    pub sequence: u64,
}

/// One record offered at the pre-acceptance boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogRecordSubmission {
    pub identity: LogRecordIdentity,
    pub content_digest: String,
    pub bytes: Vec<u8>,
}

impl LogRecordSubmission {
    pub fn new(identity: LogRecordIdentity, bytes: Vec<u8>) -> Self {
        let content_digest = content_digest(&bytes);
        Self {
            identity,
            content_digest,
            bytes,
        }
    }

    pub fn has_valid_digest(&self) -> bool {
        self.content_digest == content_digest(&self.bytes)
    }
}

/// A bounded range discarded before any Accepted Log record was created.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreAcceptanceGap {
    pub stream: LogStreamIdentity,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub dropped_records: u64,
}

/// The Executor's observed controlled process exit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LogExit {
    Status(i32),
    NoStatus,
    Error(String),
}

/// The one durable terminal record of a Log staging stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LogTerminal {
    EndOfStream { exit: LogExit },
    AbnormalClosure { committed_frontier: Option<u64> },
}

/// One durable item available to replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReplayedLogRecord {
    Accepted {
        identity: LogRecordIdentity,
        content_digest: String,
        bytes: Vec<u8>,
    },
    PreAcceptanceGap(PreAcceptanceGap),
    Terminal {
        stream: LogStreamIdentity,
        terminal: LogTerminal,
    },
}

/// The outcome of accepting one stable record identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptOutcome {
    Accepted,
    AlreadyAccepted,
}

/// The outcome of declaring one stable pre-acceptance gap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GapOutcome {
    Declared,
    AlreadyDeclared,
}

/// The outcome of writing one stable terminal record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalOutcome {
    Recorded,
    AlreadyRecorded,
}

/// One accepted record frozen into a final Log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AcceptedLogRecord {
    pub identity: LogRecordIdentity,
    pub content_digest: String,
    pub bytes: Vec<u8>,
}

/// Immutable accepted-record snapshot shared by every Log-staging adapter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogSeal {
    stream: LogStreamIdentity,
    accepted_records: Vec<AcceptedLogRecord>,
    record_digest: String,
    terminal: LogTerminal,
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

    pub fn terminal(&self) -> &LogTerminal {
        &self.terminal
    }
}

/// The canonical immutable digest of an accepted-record set.
pub fn accepted_record_digest(records: &[AcceptedLogRecord]) -> String {
    let encoded =
        serde_json::to_vec(records).expect("Accepted Log records always serialize to JSON");
    content_digest(&encoded)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogStreamViolation {
    WrongStream,
    InvalidContentDigest,
    IdentityContentConflict,
    AcceptedIdentityCoveredByGap,
    InvalidGap,
    GapOverlap,
    TerminalConflict,
    AppendAfterTerminal,
    CleanTerminalBeforeContiguousFrontier,
    SealBeforeTerminal,
}

impl Display for LogStreamViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::WrongStream => "Log record belongs to another pickup generation",
            Self::InvalidContentDigest => "Log record content digest does not match its bytes",
            Self::IdentityContentConflict => {
                "Log record identity was accepted with different content"
            }
            Self::AcceptedIdentityCoveredByGap => {
                "Log record identity is covered by a pre-acceptance gap"
            }
            Self::InvalidGap => "pre-acceptance gap is empty or malformed",
            Self::GapOverlap => "pre-acceptance gap overlaps durable Log coverage",
            Self::TerminalConflict => "Log staging stream has a different terminal record",
            Self::AppendAfterTerminal => "cannot append to a terminal Log staging stream",
            Self::CleanTerminalBeforeContiguousFrontier => {
                "cannot write End-of-stream while the committed frontier has a hole"
            }
            Self::SealBeforeTerminal => {
                "cannot seal a Log staging stream before its terminal record"
            }
        };
        formatter.write_str(message)
    }
}

impl Error for LogStreamViolation {}

/// Pure state machine shared by local and distributed Log staging adapters.
#[derive(Clone, Debug)]
pub struct LogStreamState {
    identity: LogStreamIdentity,
    accepted: BTreeMap<u64, AcceptedLogRecord>,
    gaps: BTreeMap<u64, PreAcceptanceGap>,
    committed_frontier: Option<u64>,
    terminal: Option<LogTerminal>,
}

impl LogStreamState {
    pub fn new(identity: LogStreamIdentity) -> Self {
        Self {
            identity,
            accepted: BTreeMap::new(),
            gaps: BTreeMap::new(),
            committed_frontier: None,
            terminal: None,
        }
    }

    pub fn identity(&self) -> &LogStreamIdentity {
        &self.identity
    }

    pub fn committed_frontier(&self) -> Option<u64> {
        self.committed_frontier
    }

    pub fn terminal(&self) -> Option<&LogTerminal> {
        self.terminal.as_ref()
    }

    pub fn accepted_records(&self) -> Vec<AcceptedLogRecord> {
        self.accepted.values().cloned().collect()
    }

    pub fn declared_gaps(&self) -> Vec<PreAcceptanceGap> {
        self.gaps.values().cloned().collect()
    }

    pub fn seal(&self) -> Result<LogSeal, LogStreamViolation> {
        let terminal = self
            .terminal
            .clone()
            .ok_or(LogStreamViolation::SealBeforeTerminal)?;
        let accepted_records = self.accepted_records();
        Ok(LogSeal {
            stream: self.identity.clone(),
            record_digest: accepted_record_digest(&accepted_records),
            accepted_records,
            terminal,
        })
    }

    pub fn apply_accepted(
        &mut self,
        submission: LogRecordSubmission,
    ) -> Result<AcceptOutcome, LogStreamViolation> {
        self.require_open()?;
        if submission.identity.stream != self.identity {
            return Err(LogStreamViolation::WrongStream);
        }
        if !submission.has_valid_digest() {
            return Err(LogStreamViolation::InvalidContentDigest);
        }
        let sequence = submission.identity.sequence;
        if let Some(existing) = self.accepted.get(&sequence) {
            return if existing.content_digest == submission.content_digest
                && existing.bytes == submission.bytes
            {
                Ok(AcceptOutcome::AlreadyAccepted)
            } else {
                Err(LogStreamViolation::IdentityContentConflict)
            };
        }
        if self.gap_covering(sequence).is_some() {
            return Err(LogStreamViolation::AcceptedIdentityCoveredByGap);
        }
        self.accepted.insert(
            sequence,
            AcceptedLogRecord {
                identity: submission.identity,
                content_digest: submission.content_digest,
                bytes: submission.bytes,
            },
        );
        self.advance_frontier();
        Ok(AcceptOutcome::Accepted)
    }

    pub fn apply_gap(&mut self, gap: PreAcceptanceGap) -> Result<GapOutcome, LogStreamViolation> {
        self.require_open()?;
        if gap.stream != self.identity {
            return Err(LogStreamViolation::WrongStream);
        }
        if gap.first_sequence > gap.last_sequence || gap.dropped_records == 0 {
            return Err(LogStreamViolation::InvalidGap);
        }
        if self.gaps.get(&gap.first_sequence) == Some(&gap) {
            return Ok(GapOutcome::AlreadyDeclared);
        }
        if self
            .accepted
            .range(gap.first_sequence..=gap.last_sequence)
            .next()
            .is_some()
            || self.gaps.values().any(|existing| {
                existing.first_sequence <= gap.last_sequence
                    && gap.first_sequence <= existing.last_sequence
            })
        {
            return Err(LogStreamViolation::GapOverlap);
        }
        self.gaps.insert(gap.first_sequence, gap);
        self.advance_frontier();
        Ok(GapOutcome::Declared)
    }

    pub fn apply_terminal(
        &mut self,
        terminal: LogTerminal,
    ) -> Result<TerminalOutcome, LogStreamViolation> {
        if let Some(existing) = &self.terminal {
            return if existing == &terminal {
                Ok(TerminalOutcome::AlreadyRecorded)
            } else {
                Err(LogStreamViolation::TerminalConflict)
            };
        }
        if matches!(terminal, LogTerminal::EndOfStream { .. }) && self.frontier_has_hole() {
            return Err(LogStreamViolation::CleanTerminalBeforeContiguousFrontier);
        }
        self.terminal = Some(terminal);
        Ok(TerminalOutcome::Recorded)
    }

    pub fn replay(&self) -> Vec<ReplayedLogRecord> {
        let mut records = Vec::new();
        let Some(frontier) = self.committed_frontier else {
            self.push_terminal(&mut records);
            return records;
        };
        let mut sequence = 0_u64;
        while sequence <= frontier {
            if let Some(record) = self.accepted.get(&sequence) {
                records.push(ReplayedLogRecord::Accepted {
                    identity: record.identity.clone(),
                    content_digest: record.content_digest.clone(),
                    bytes: record.bytes.clone(),
                });
                sequence = match sequence.checked_add(1) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            }
            let gap = self
                .gaps
                .get(&sequence)
                .expect("the committed frontier is contiguous")
                .clone();
            sequence = match gap.last_sequence.checked_add(1) {
                Some(next) => next,
                None => {
                    records.push(ReplayedLogRecord::PreAcceptanceGap(gap));
                    break;
                }
            };
            records.push(ReplayedLogRecord::PreAcceptanceGap(gap));
        }
        self.push_terminal(&mut records);
        records
    }

    fn require_open(&self) -> Result<(), LogStreamViolation> {
        if self.terminal.is_some() {
            Err(LogStreamViolation::AppendAfterTerminal)
        } else {
            Ok(())
        }
    }

    fn gap_covering(&self, sequence: u64) -> Option<&PreAcceptanceGap> {
        self.gaps
            .range(..=sequence)
            .next_back()
            .map(|(_, gap)| gap)
            .filter(|gap| sequence <= gap.last_sequence)
    }

    fn advance_frontier(&mut self) {
        let mut next = self.committed_frontier.map_or(0, |frontier| frontier + 1);
        loop {
            if self.accepted.contains_key(&next) {
                self.committed_frontier = Some(next);
                next = match next.checked_add(1) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            }
            let Some(gap) = self.gaps.get(&next) else {
                break;
            };
            self.committed_frontier = Some(gap.last_sequence);
            next = match gap.last_sequence.checked_add(1) {
                Some(next) => next,
                None => break,
            };
        }
    }

    fn frontier_has_hole(&self) -> bool {
        let last_accepted = self.accepted.keys().next_back().copied();
        let last_gap = self.gaps.values().map(|gap| gap.last_sequence).max();
        let last = match (last_accepted, last_gap) {
            (Some(accepted), Some(gap)) => Some(accepted.max(gap)),
            (accepted, gap) => accepted.or(gap),
        };
        match (self.committed_frontier, last) {
            (None, Some(_)) => true,
            (Some(frontier), Some(last)) => frontier != last,
            (_, None) => false,
        }
    }

    fn push_terminal(&self, records: &mut Vec<ReplayedLogRecord>) {
        if let Some(terminal) = &self.terminal {
            records.push(ReplayedLogRecord::Terminal {
                stream: self.identity.clone(),
                terminal: terminal.clone(),
            });
        }
    }
}

/// Rebuild every pickup generation represented by durable replay records.
pub fn rebuild_log_streams(
    records: impl IntoIterator<Item = ReplayedLogRecord>,
) -> Result<BTreeMap<LogStreamIdentity, LogStreamState>, LogStreamViolation> {
    let mut streams = BTreeMap::new();
    for record in records {
        let identity = match &record {
            ReplayedLogRecord::Accepted { identity, .. } => identity.stream.clone(),
            ReplayedLogRecord::PreAcceptanceGap(gap) => gap.stream.clone(),
            ReplayedLogRecord::Terminal { stream, .. } => stream.clone(),
        };
        let stream = streams
            .entry(identity.clone())
            .or_insert_with(|| LogStreamState::new(identity));
        match record {
            ReplayedLogRecord::Accepted {
                identity,
                content_digest,
                bytes,
            } => {
                stream.apply_accepted(LogRecordSubmission {
                    identity,
                    content_digest,
                    bytes,
                })?;
            }
            ReplayedLogRecord::PreAcceptanceGap(gap) => {
                stream.apply_gap(gap)?;
            }
            ReplayedLogRecord::Terminal { terminal, .. } => {
                stream.apply_terminal(terminal)?;
            }
        }
    }
    Ok(streams)
}

pub fn content_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream() -> LogStreamIdentity {
        LogStreamIdentity {
            task_instance_id: Uuid::nil(),
            pickup_generation: 7,
        }
    }

    fn submission(sequence: u64, bytes: &[u8]) -> LogRecordSubmission {
        LogRecordSubmission::new(
            LogRecordIdentity {
                stream: stream(),
                sequence,
            },
            bytes.to_vec(),
        )
    }

    #[test]
    fn frontier_advances_only_across_records_and_declared_gaps() {
        let mut state = LogStreamState::new(stream());
        state.apply_accepted(submission(2, b"two")).unwrap();
        assert_eq!(state.committed_frontier(), None);
        state
            .apply_gap(PreAcceptanceGap {
                stream: stream(),
                first_sequence: 0,
                last_sequence: 1,
                dropped_records: 2,
            })
            .unwrap();
        assert_eq!(state.committed_frontier(), Some(2));
        assert_eq!(state.replay().len(), 2);
    }

    #[test]
    fn stable_identity_rejects_different_content() {
        let mut state = LogStreamState::new(stream());
        state.apply_accepted(submission(0, b"first")).unwrap();
        assert_eq!(
            state.apply_accepted(submission(0, b"other")),
            Err(LogStreamViolation::IdentityContentConflict)
        );
    }
}
