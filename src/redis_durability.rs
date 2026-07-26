use std::{fmt, time::Duration};

use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, ErrorKind};
use sha2::{Digest, Sha256};

pub const PRIMARY_LOCAL_FSYNC_COUNT: u64 = 1;
pub const REQUIRED_REPLICA_ACKNOWLEDGEMENTS: u64 = 0;

const DEFAULT_MUTATION_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_LOCAL_FSYNC_TIMEOUT: Duration = Duration::from_secs(5);

const CONDITIONAL_SET_SCRIPT: &str = r#"
local prior = redis.call('GET', KEYS[1])
if prior then
    if prior ~= ARGV[1] then
        return -1
    end
    redis.call('PSETEX', KEYS[2], ARGV[3], ARGV[2])
    redis.call('PSETEX', KEYS[1], ARGV[3], ARGV[1])
    return 0
end
redis.call('PSETEX', KEYS[2], ARGV[3], ARGV[2])
redis.call('PSETEX', KEYS[1], ARGV[3], ARGV[1])
return 1
"#;

/// Stable logical identity and payload fingerprint for one conditional Redis mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisStableOperation {
    identity_key: String,
    payload_fingerprint: [u8; 32],
}

impl RedisStableOperation {
    pub fn new(
        identity_key: impl Into<String>,
        payload: &[u8],
    ) -> Result<Self, RedisDurabilityError> {
        let identity_key = identity_key.into();
        if identity_key.is_empty() {
            return Err(RedisDurabilityError::new(
                RedisDurabilityFailure::InvalidOperation,
            ));
        }
        Ok(Self {
            identity_key,
            payload_fingerprint: Sha256::digest(payload).into(),
        })
    }

    pub fn identity_key(&self) -> &str {
        &self.identity_key
    }

    fn payload_fingerprint(&self) -> &[u8; 32] {
        &self.payload_fingerprint
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisStableMutationOutcome<T> {
    Applied(T),
    Replayed(T),
    IdentityConflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisStableMutationRecovery {
    Missing,
    Matching,
    IdentityConflict,
}

/// A role-specific mutation must atomically bind its payload to `operation()` before returning.
/// A matching retry may repeat physical writes but must not create a second logical operation.
#[async_trait]
pub trait RedisStableMutation: Send + Sync {
    type Output: Send;

    fn operation(&self) -> &RedisStableOperation;

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError>;

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError>;
}

/// Representative stable conditional mutation used by admission and the common law suite.
pub struct RedisConditionalSetMutation {
    operation: RedisStableOperation,
    target_key: String,
    payload: Vec<u8>,
    retention_millis: u64,
}

impl RedisConditionalSetMutation {
    pub fn new(
        identity_key: impl Into<String>,
        target_key: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        retention: Duration,
    ) -> Result<Self, RedisDurabilityError> {
        let identity_key = identity_key.into();
        let target_key = target_key.into();
        let payload = payload.into();
        let retention_millis = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
        if target_key.is_empty()
            || target_key == identity_key
            || retention_millis == 0
            || payload.is_empty()
        {
            return Err(RedisDurabilityError::new(
                RedisDurabilityFailure::InvalidOperation,
            ));
        }
        Ok(Self {
            operation: RedisStableOperation::new(identity_key, &payload)?,
            target_key,
            payload,
            retention_millis,
        })
    }

    pub fn identity_key(&self) -> &str {
        self.operation.identity_key()
    }

    pub fn target_key(&self) -> &str {
        &self.target_key
    }
}

#[async_trait]
impl RedisStableMutation for RedisConditionalSetMutation {
    type Output = RedisMutationDisposition;

    fn operation(&self) -> &RedisStableOperation {
        &self.operation
    }

    async fn apply(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationOutcome<Self::Output>, RedisMutationError> {
        let result: i64 = redis::cmd("EVAL")
            .arg(CONDITIONAL_SET_SCRIPT)
            .arg(2)
            .arg(self.operation.identity_key())
            .arg(&self.target_key)
            .arg(self.operation.payload_fingerprint().as_slice())
            .arg(&self.payload)
            .arg(self.retention_millis)
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        match result {
            1 => Ok(RedisStableMutationOutcome::Applied(
                RedisMutationDisposition::Applied,
            )),
            0 => Ok(RedisStableMutationOutcome::Replayed(
                RedisMutationDisposition::Replayed,
            )),
            -1 => Ok(RedisStableMutationOutcome::IdentityConflict),
            _ => Err(RedisMutationError::rejected()),
        }
    }

    async fn recover(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<RedisStableMutationRecovery, RedisMutationError> {
        let fingerprint: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.operation.identity_key())
            .query_async(connection)
            .await
            .map_err(RedisMutationError::from_redis)?;
        Ok(match fingerprint {
            None => RedisStableMutationRecovery::Missing,
            Some(actual) if actual.as_slice() == self.operation.payload_fingerprint() => {
                RedisStableMutationRecovery::Matching
            }
            Some(_) => RedisStableMutationRecovery::IdentityConflict,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisMutationDisposition {
    Applied,
    Replayed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisMutationFailure {
    AmbiguousTransport,
    ReadOnlyPrimary,
    OutOfMemory,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisMutationError {
    failure: RedisMutationFailure,
}

impl RedisMutationError {
    pub fn from_redis(error: redis::RedisError) -> Self {
        let failure = if error.is_io_error() {
            RedisMutationFailure::AmbiguousTransport
        } else if error.kind() == ErrorKind::ReadOnly || error.code() == Some("READONLY") {
            RedisMutationFailure::ReadOnlyPrimary
        } else if error.code() == Some("OOM") {
            RedisMutationFailure::OutOfMemory
        } else {
            RedisMutationFailure::Rejected
        };
        Self { failure }
    }

    pub const fn rejected() -> Self {
        Self {
            failure: RedisMutationFailure::Rejected,
        }
    }

    pub const fn failure(&self) -> RedisMutationFailure {
        self.failure
    }
}

/// The only value from which a caller may produce its role-specific durability acknowledgement.
pub struct RedisDurablyCommitted<T> {
    output: T,
}

impl<T> RedisDurablyCommitted<T> {
    pub fn into_output(self) -> T {
        self.output
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RedisDurabilityGuard {
    mutation_timeout: Duration,
    local_fsync_timeout: Duration,
}

impl Default for RedisDurabilityGuard {
    fn default() -> Self {
        Self::new(DEFAULT_MUTATION_TIMEOUT, DEFAULT_LOCAL_FSYNC_TIMEOUT)
    }
}

impl RedisDurabilityGuard {
    pub const fn new(mutation_timeout: Duration, local_fsync_timeout: Duration) -> Self {
        Self {
            mutation_timeout,
            local_fsync_timeout,
        }
    }

    pub async fn execute<M: RedisStableMutation>(
        &self,
        connection: &mut MultiplexedConnection,
        mutation: &M,
    ) -> Result<RedisDurablyCommitted<M::Output>, RedisDurabilityError> {
        let outcome = tokio::time::timeout(self.mutation_timeout, mutation.apply(connection))
            .await
            .map_err(|_| RedisDurabilityError::new(RedisDurabilityFailure::AmbiguousMutation))?
            .map_err(map_mutation_error)?;
        let output = match outcome {
            RedisStableMutationOutcome::Applied(output)
            | RedisStableMutationOutcome::Replayed(output) => output,
            RedisStableMutationOutcome::IdentityConflict => {
                return Err(RedisDurabilityError::new(
                    RedisDurabilityFailure::IdentityConflict,
                ));
            }
        };

        self.prove_primary_local_fsync(connection).await?;
        Ok(RedisDurablyCommitted { output })
    }

    /// Resolves an ambiguous attempt by reading and then retrying the same stable identity.
    pub async fn resolve_ambiguous<M: RedisStableMutation>(
        &self,
        connection: &mut MultiplexedConnection,
        mutation: &M,
    ) -> Result<RedisDurablyCommitted<M::Output>, RedisDurabilityError> {
        let recovery = tokio::time::timeout(self.mutation_timeout, mutation.recover(connection))
            .await
            .map_err(|_| RedisDurabilityError::new(RedisDurabilityFailure::AmbiguousMutation))?
            .map_err(map_mutation_error)?;
        if recovery == RedisStableMutationRecovery::IdentityConflict {
            return Err(RedisDurabilityError::new(
                RedisDurabilityFailure::IdentityConflict,
            ));
        }
        self.execute(connection, mutation).await
    }

    async fn prove_primary_local_fsync(
        &self,
        connection: &mut MultiplexedConnection,
    ) -> Result<(), RedisDurabilityError> {
        let server_timeout_millis = u64::try_from(self.local_fsync_timeout.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let client_timeout = self
            .local_fsync_timeout
            .saturating_add(Duration::from_secs(1));
        let proof = tokio::time::timeout(
            client_timeout,
            redis::cmd("WAITAOF")
                .arg(PRIMARY_LOCAL_FSYNC_COUNT)
                .arg(REQUIRED_REPLICA_ACKNOWLEDGEMENTS)
                .arg(server_timeout_millis)
                .query_async::<(u64, u64)>(connection),
        )
        .await
        .map_err(|_| RedisDurabilityError::new(RedisDurabilityFailure::AmbiguousLocalFsync))?
        .map_err(|error| {
            if error.kind() == ErrorKind::ReadOnly {
                RedisDurabilityError::new(RedisDurabilityFailure::ReadOnlyPrimary)
            } else if error.is_io_error() {
                RedisDurabilityError::new(RedisDurabilityFailure::AmbiguousLocalFsync)
            } else {
                RedisDurabilityError::new(RedisDurabilityFailure::LocalFsyncUnavailable)
            }
        })?;

        // Replica progress is intentionally ignored: this class proves only the primary's local AOF.
        if proof.0 < PRIMARY_LOCAL_FSYNC_COUNT {
            return Err(RedisDurabilityError::new(
                RedisDurabilityFailure::LocalFsyncUnavailable,
            ));
        }
        Ok(())
    }
}

fn map_mutation_error(error: RedisMutationError) -> RedisDurabilityError {
    let failure = match error.failure() {
        RedisMutationFailure::AmbiguousTransport => RedisDurabilityFailure::AmbiguousMutation,
        RedisMutationFailure::ReadOnlyPrimary => RedisDurabilityFailure::ReadOnlyPrimary,
        RedisMutationFailure::OutOfMemory => RedisDurabilityFailure::OutOfMemory,
        RedisMutationFailure::Rejected => RedisDurabilityFailure::MutationRejected,
    };
    RedisDurabilityError::new(failure)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedisDurabilityFailure {
    InvalidOperation,
    AmbiguousMutation,
    AmbiguousLocalFsync,
    MutationRejected,
    IdentityConflict,
    LocalFsyncUnavailable,
    OutOfMemory,
    ReadOnlyPrimary,
}

impl RedisDurabilityFailure {
    fn description(self) -> &'static str {
        match self {
            Self::InvalidOperation => "stable operation metadata is invalid",
            Self::AmbiguousMutation => "mutation outcome is ambiguous",
            Self::AmbiguousLocalFsync => "local-primary fsync outcome is ambiguous",
            Self::MutationRejected => "stable mutation was rejected",
            Self::IdentityConflict => "stable operation identity has a different payload",
            Self::LocalFsyncUnavailable => "local-primary AOF fsync was not proved",
            Self::OutOfMemory => "Redis refused the mutation at its memory limit",
            Self::ReadOnlyPrimary => "the Redis endpoint is not a writable primary",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct RedisDurabilityError {
    failure: RedisDurabilityFailure,
}

impl RedisDurabilityError {
    const fn new(failure: RedisDurabilityFailure) -> Self {
        Self { failure }
    }

    pub const fn failure(&self) -> RedisDurabilityFailure {
        self.failure
    }
}

impl fmt::Display for RedisDurabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Redis durability guard failed: {}",
            self.failure.description()
        )
    }
}

impl fmt::Debug for RedisDurabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisDurabilityError")
            .field("failure", &self.failure)
            .finish()
    }
}

impl std::error::Error for RedisDurabilityError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_local_proof_requires_no_replica_acknowledgements() {
        assert_eq!(PRIMARY_LOCAL_FSYNC_COUNT, 1);
        assert_eq!(REQUIRED_REPLICA_ACKNOWLEDGEMENTS, 0);
    }

    #[test]
    fn stable_identity_fingerprints_payload_without_exposing_it() {
        let first = RedisStableOperation::new("operation", b"payload-secret").unwrap();
        let same = RedisStableOperation::new("operation", b"payload-secret").unwrap();
        let conflict = RedisStableOperation::new("operation", b"other-secret").unwrap();
        assert_eq!(first, same);
        assert_ne!(first, conflict);
    }

    #[test]
    fn durability_errors_never_include_operation_or_payload_values() {
        for failure in [
            RedisDurabilityFailure::InvalidOperation,
            RedisDurabilityFailure::AmbiguousMutation,
            RedisDurabilityFailure::AmbiguousLocalFsync,
            RedisDurabilityFailure::MutationRejected,
            RedisDurabilityFailure::IdentityConflict,
            RedisDurabilityFailure::LocalFsyncUnavailable,
            RedisDurabilityFailure::ReadOnlyPrimary,
        ] {
            let error = RedisDurabilityError::new(failure);
            let diagnostic = format!("{error:?} {error}");
            for secret in [
                "redis.example",
                "credential=secret",
                "operation-secret",
                "payload-secret",
            ] {
                assert!(!diagnostic.contains(secret));
            }
        }
    }
}
