use std::io;
use std::path::PathBuf;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use uuid::Uuid;

pub const LOCAL_PROTOCOL_VERSION: u16 = 1;
pub const MAX_LOCAL_REQUEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_LOCAL_RESPONSE_BYTES: usize = 72 * 1024 * 1024;
pub const ENDPOINT_ENV: &str = "TICKR_CTX_ENDPOINT";
pub const CREDENTIAL_ENV: &str = "TICKR_CTX_CREDENTIAL";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalRequest {
    pub protocol_version: u16,
    pub credential: String,
    pub namespace: String,
    pub run_id: String,
    pub task_id: String,
    pub operation: LocalOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LocalOperation {
    Get {
        key: String,
    },
    Put {
        key: String,
        envelope: Vec<u8>,
        claim_id: Uuid,
    },
    Delete {
        key: String,
        claim_id: Uuid,
    },
    List {
        prefix: String,
    },
    Watch {
        prefix: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LocalEventOperation {
    Put,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalEvent {
    pub operation: LocalEventOperation,
    pub key: String,
    pub envelope: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LocalBoundKind {
    ValueBytes,
    RequestValues,
    RequestBytes,
    ScopeRows,
    ScopeBytes,
    ScopeAgeSeconds,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LocalFailure {
    Unavailable,
    Unauthorized,
    InvalidRequest {
        message: String,
    },
    Bound {
        kind: LocalBoundKind,
        actual: usize,
        limit: usize,
    },
    ScopeMissing,
    ScopeNotWritable {
        state: String,
    },
    ClaimConflict,
    Quarantined {
        diagnostic: String,
    },
    Internal {
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LocalResponse {
    Applied,
    Missing,
    Value { envelope: Vec<u8> },
    Keys { keys: Vec<String> },
    WatchReady,
    Event(LocalEvent),
    Failure(LocalFailure),
}

#[derive(Clone, Debug)]
pub struct LocalClient {
    endpoint: PathBuf,
    credential: String,
    namespace: String,
    run_id: String,
    task_id: String,
}

impl LocalClient {
    pub fn from_environment(
        namespace: &str,
        run_id: &str,
        task_id: &str,
    ) -> io::Result<Option<Self>> {
        let Some(endpoint) = std::env::var_os(ENDPOINT_ENV) else {
            return Ok(None);
        };
        let credential = std::env::var(CREDENTIAL_ENV).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{CREDENTIAL_ENV} is required when {ENDPOINT_ENV} is set"),
            )
        })?;
        if namespace.is_empty() || run_id.is_empty() || task_id.is_empty() || credential.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local tickr-ctx identity is incomplete",
            ));
        }
        Ok(Some(Self {
            endpoint: PathBuf::from(endpoint),
            credential,
            namespace: namespace.to_owned(),
            run_id: run_id.to_owned(),
            task_id: task_id.to_owned(),
        }))
    }

    pub async fn request(&self, operation: LocalOperation) -> io::Result<LocalResponse> {
        let mut stream = UnixStream::connect(&self.endpoint)
            .await
            .map_err(unavailable)?;
        let request = self.request_envelope(operation);
        write_message(&mut stream, &request, MAX_LOCAL_REQUEST_BYTES).await?;
        read_message(&mut stream, MAX_LOCAL_RESPONSE_BYTES)
            .await
            .map_err(unavailable)
    }

    pub async fn watch(&self, prefix: String) -> io::Result<UnixStream> {
        let mut stream = UnixStream::connect(&self.endpoint)
            .await
            .map_err(unavailable)?;
        let request = self.request_envelope(LocalOperation::Watch { prefix });
        write_message(&mut stream, &request, MAX_LOCAL_REQUEST_BYTES).await?;
        let response: LocalResponse = read_message(&mut stream, MAX_LOCAL_RESPONSE_BYTES)
            .await
            .map_err(unavailable)?;
        match response {
            LocalResponse::WatchReady => Ok(stream),
            LocalResponse::Failure(failure) => Err(failure_error(failure)),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected local tickr-ctx watch response: {other:?}"),
            )),
        }
    }

    fn request_envelope(&self, operation: LocalOperation) -> LocalRequest {
        LocalRequest {
            protocol_version: LOCAL_PROTOCOL_VERSION,
            credential: self.credential.clone(),
            namespace: self.namespace.clone(),
            run_id: self.run_id.clone(),
            task_id: self.task_id.clone(),
            operation,
        }
    }
}

pub fn failure_error(failure: LocalFailure) -> io::Error {
    let (kind, message) = match failure {
        LocalFailure::Unavailable => (
            io::ErrorKind::NotConnected,
            "local tickr-ctx endpoint unavailable".to_owned(),
        ),
        LocalFailure::Unauthorized => (
            io::ErrorKind::PermissionDenied,
            "local tickr-ctx task identity rejected".to_owned(),
        ),
        LocalFailure::InvalidRequest { message } => (io::ErrorKind::InvalidInput, message),
        LocalFailure::Bound {
            kind,
            actual,
            limit,
        } => (
            io::ErrorKind::InvalidInput,
            format!("local tickr-ctx {kind:?} bound exceeded: {actual} > {limit}"),
        ),
        LocalFailure::ScopeMissing => (
            io::ErrorKind::NotFound,
            "local tickr-ctx scope is missing".to_owned(),
        ),
        LocalFailure::ScopeNotWritable { state } => (
            io::ErrorKind::InvalidData,
            format!("local tickr-ctx scope is not writable: {state}"),
        ),
        LocalFailure::ClaimConflict => (
            io::ErrorKind::AlreadyExists,
            "local tickr-ctx mutation claim conflicts with an accepted request".to_owned(),
        ),
        LocalFailure::Quarantined { diagnostic } => (
            io::ErrorKind::InvalidData,
            format!("local tickr-ctx scope is quarantined: {diagnostic}"),
        ),
        LocalFailure::Internal { message } => (
            io::ErrorKind::Other,
            format!("local tickr-ctx endpoint failed: {message}"),
        ),
    };
    io::Error::new(kind, message)
}

pub async fn write_message<W, T>(writer: &mut W, value: &T, maximum: usize) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = bincode::serialize(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if bytes.len() > maximum || bytes.len() > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "local tickr-ctx frame is {} bytes (limit {maximum})",
                bytes.len()
            ),
        ));
    }
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(&bytes).await?;
    writer.flush().await
}

pub async fn read_message<R, T>(reader: &mut R, maximum: usize) -> io::Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let length = reader.read_u32().await? as usize;
    if length > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("local tickr-ctx frame is {length} bytes (limit {maximum})"),
        ));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    bincode::deserialize(&bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn unavailable(error: io::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        format!("local tickr-ctx endpoint unavailable: {error}"),
    )
}
