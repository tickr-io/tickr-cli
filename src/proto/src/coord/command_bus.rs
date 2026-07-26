//! Backend-neutral metadata and bounds for the Command-bus role.
//!
//! The production protobuf request and response bytes remain unchanged. Each
//! transport carries this metadata beside those bytes so an expired request is
//! rejected before it can enter the sole Conductor mutation path.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

/// Correlation identifier carried beside every encoded command request.
pub const CORRELATION_HEADER: &str = "Tickr-Command-Correlation";
/// Absolute request deadline, as Unix epoch milliseconds.
pub const DEADLINE_HEADER: &str = "Tickr-Command-Deadline-Ms";
/// Hard encoded-payload limit shared by the local and all-NATS implementations.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1_000_000;
/// Maximum requests admitted concurrently by one Command-bus client/consumer.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 64;

/// Transport metadata for one encoded `ApiCommandRequest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandRequestMetadata {
    pub correlation_id: Uuid,
    pub deadline_unix_ms: u64,
}

impl CommandRequestMetadata {
    pub fn new(correlation_id: Uuid, deadline: Duration) -> Self {
        Self {
            correlation_id,
            deadline_unix_ms: unix_time_millis().saturating_add(duration_millis(deadline)),
        }
    }

    pub fn is_expired(self) -> bool {
        unix_time_millis() >= self.deadline_unix_ms
    }

    pub fn remaining(self) -> Option<Duration> {
        let remaining_ms = self.deadline_unix_ms.checked_sub(unix_time_millis())?;
        if remaining_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(remaining_ms))
        }
    }
}

fn unix_time_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    duration_millis(elapsed)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_metadata_expires_and_saturates_without_wrapping() {
        let expired = CommandRequestMetadata {
            correlation_id: Uuid::nil(),
            deadline_unix_ms: 0,
        };
        assert!(expired.is_expired());
        assert_eq!(expired.remaining(), None);

        let bounded = CommandRequestMetadata::new(Uuid::nil(), Duration::from_secs(1));
        assert!(!bounded.is_expired());
        assert!(bounded.remaining().is_some());

        let saturated = CommandRequestMetadata::new(Uuid::nil(), Duration::MAX);
        assert_eq!(saturated.deadline_unix_ms, u64::MAX);
    }
}
