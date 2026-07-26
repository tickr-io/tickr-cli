//! Root-local tickr-ctx access for selected ScopeStore implementations.
//!
//! The endpoint owns no repository or substrate handle. Every operation
//! crosses the bounded writer channel, keeping task processes and socket
//! handlers outside SQLite and Redis.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use tickr_ctx::local::{
    write_message, LocalBoundKind, LocalEvent, LocalEventOperation, LocalFailure, LocalOperation,
    LocalRequest, LocalResponse, CREDENTIAL_ENV, ENDPOINT_ENV, LOCAL_PROTOCOL_VERSION,
    MAX_LOCAL_REQUEST_BYTES, MAX_LOCAL_RESPONSE_BYTES,
};
use tickr_executor::task_handler::TaskContextProvider;
use tickr_executor::wire::DispatchedTask;
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, DeleteTickrCtxScopeInput, ScopeBoundViolation, ScopeCreationOutcome,
    ScopeDeleteOutcome, ScopeMutationRejection, ScopeReadOutcome, ScopeStore, ScopeValueInput,
    ScopeWriteOutcome, StoredScopeValue, TickrCtxScopeState, WriteTickrCtxScopeInput,
    MAX_SCOPE_REQUEST_BYTES, MAX_SCOPE_VALUE_BYTES,
};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::data_directory::{DataDirectory, RootRelativePath};

const WRITER_QUEUE_CAPACITY: usize = 64;
const EVENT_CAPACITY: usize = 256;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 512;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TickrCtxTaskEnvironment {
    endpoint: PathBuf,
    credential: String,
}

impl TickrCtxTaskEnvironment {
    pub fn variables(&self) -> [(String, String); 2] {
        [
            (
                ENDPOINT_ENV.to_owned(),
                self.endpoint.to_string_lossy().into_owned(),
            ),
            (CREDENTIAL_ENV.to_owned(), self.credential.clone()),
        ]
    }
}

#[derive(Clone)]
pub struct TickrCtxEndpointHandle {
    endpoint: PathBuf,
    ready: Arc<AtomicBool>,
    grants: Arc<RwLock<HashMap<String, TaskGrant>>>,
}

impl TickrCtxEndpointHandle {
    pub async fn register_task(
        &self,
        task_id: impl Into<String>,
        namespace: impl Into<String>,
        run_id: impl Into<String>,
        scope_id: Uuid,
    ) -> Result<TickrCtxTaskEnvironment> {
        let task_id = task_id.into();
        let namespace = namespace.into();
        let run_id = run_id.into();
        validate_identity(&task_id, "task id")?;
        validate_identity(&namespace, "namespace")?;
        validate_identity(&run_id, "run id")?;
        let credential = Uuid::new_v4().to_string();
        let grant = TaskGrant {
            task_id: task_id.clone(),
            namespace,
            run_id,
            scope_id,
            credential: credential.clone(),
        };
        let replaced = self.grants.write().await.insert(task_id, grant);
        if replaced.is_some() {
            return Err(anyhow!("tickr-ctx task identity is already registered"));
        }
        Ok(TickrCtxTaskEnvironment {
            endpoint: self.endpoint.clone(),
            credential,
        })
    }

    pub async fn revoke_task(&self, task_id: &str) {
        self.grants.write().await.remove(task_id);
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn clear_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn endpoint(&self) -> &PathBuf {
        &self.endpoint
    }
}

#[derive(Clone)]
pub struct DistributedTickrCtx {
    handle: TickrCtxEndpointHandle,
    store: Arc<dyn ScopeStore>,
    namespace: String,
}

impl DistributedTickrCtx {
    pub fn new(
        handle: TickrCtxEndpointHandle,
        store: Arc<dyn ScopeStore>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            handle,
            store,
            namespace: namespace.into(),
        }
    }
}

#[async_trait::async_trait]
impl TaskContextProvider for DistributedTickrCtx {
    async fn register_task(
        &self,
        task: &DispatchedTask,
    ) -> std::result::Result<HashMap<String, String>, String> {
        let requested_scope_id = task.workflow_instance_id;
        let run_id = requested_scope_id.to_string();
        let claim_id = Uuid::new_v5(&requested_scope_id, b"tickr-distributed-ctx-scope");
        let scope_id = match self
            .store
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: requested_scope_id,
                namespace: &self.namespace,
                run_id: &run_id,
                claim_id,
                values: &[],
                now: Utc::now(),
            })
            .await
            .map_err(|error| format!("create distributed tickr-ctx scope: {error}"))?
        {
            ScopeCreationOutcome::Created | ScopeCreationOutcome::Idempotent => requested_scope_id,
            ScopeCreationOutcome::Collision { existing_scope_id } => existing_scope_id,
            outcome => return Err(format!("create distributed tickr-ctx scope: {outcome:?}")),
        };
        let environment = self
            .handle
            .register_task(
                task.task_instance_id.to_string(),
                self.namespace.clone(),
                run_id,
                scope_id,
            )
            .await
            .map_err(|error| format!("register distributed tickr-ctx task: {error}"))?;
        Ok(environment.variables().into_iter().collect())
    }

    async fn revoke_task(&self, task_instance_id: Uuid) {
        self.handle.revoke_task(&task_instance_id.to_string()).await;
    }
}

#[derive(Clone)]
struct TaskGrant {
    task_id: String,
    namespace: String,
    run_id: String,
    scope_id: Uuid,
    credential: String,
}

#[derive(Clone, Debug)]
struct ScopeEvent {
    scope_id: Uuid,
    event: LocalEvent,
}

pub struct TickrCtxEndpoint {
    listener: UnixListener,
    cleanup: EndpointCleanup,
    ready: Arc<AtomicBool>,
    grants: Arc<RwLock<HashMap<String, TaskGrant>>>,
    writer: TickrCtxScopeWriterClient,
    events: broadcast::Sender<ScopeEvent>,
}

enum EndpointCleanup {
    DataDirectory {
        data_directory: Arc<DataDirectory>,
        socket_path: RootRelativePath,
    },
    Ephemeral(PathBuf),
}

impl TickrCtxEndpoint {
    /// Bind only after data-directory admission and scope recovery have completed.
    /// The returned endpoint remains unavailable until the supervisor marks the
    /// handle ready after registering every critical child.
    pub fn bind_after_recovery(
        data_directory: Arc<DataDirectory>,
        socket_path: RootRelativePath,
        writer: TickrCtxScopeWriterClient,
    ) -> Result<(TickrCtxEndpointHandle, Self)> {
        let endpoint = data_directory
            .prepare_unix_socket_path(&socket_path)
            .context("preparing root-local tickr-ctx endpoint")?;
        let listener = UnixListener::bind(&endpoint).with_context(|| {
            format!(
                "binding root-local tickr-ctx endpoint at {}",
                endpoint.display()
            )
        })?;
        if let Err(error) = data_directory.secure_unix_socket_permissions(&socket_path) {
            let _ = data_directory.remove_unix_socket(&socket_path);
            return Err(error).context("securing root-local tickr-ctx endpoint");
        }

        Ok(Self::from_bound(
            listener,
            endpoint,
            writer,
            EndpointCleanup::DataDirectory {
                data_directory,
                socket_path,
            },
        ))
    }

    /// Bind a process-private task context endpoint after distributed
    /// ScopeStore reconstruction. Child tasks receive only this socket and an
    /// ephemeral grant, never Redis credentials.
    pub fn bind_distributed_after_recovery(
        writer: TickrCtxScopeWriterClient,
    ) -> Result<(TickrCtxEndpointHandle, Self)> {
        let endpoint = std::env::temp_dir().join(format!("tickr-ctx-{}.sock", Uuid::new_v4()));
        let listener = UnixListener::bind(&endpoint).with_context(|| {
            format!(
                "binding distributed tickr-ctx endpoint at {}",
                endpoint.display()
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&endpoint, fs::Permissions::from_mode(0o600))
                .context("securing distributed tickr-ctx endpoint")?;
        }
        Ok(Self::from_bound(
            listener,
            endpoint.clone(),
            writer,
            EndpointCleanup::Ephemeral(endpoint),
        ))
    }

    fn from_bound(
        listener: UnixListener,
        endpoint: PathBuf,
        writer: TickrCtxScopeWriterClient,
        cleanup: EndpointCleanup,
    ) -> (TickrCtxEndpointHandle, Self) {
        let ready = Arc::new(AtomicBool::new(false));
        let grants = Arc::new(RwLock::new(HashMap::new()));
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let handle = TickrCtxEndpointHandle {
            endpoint,
            ready: ready.clone(),
            grants: grants.clone(),
        };
        (
            handle,
            Self {
                listener,
                cleanup,
                ready,
                grants,
                writer,
                events,
            },
        )
    }

    pub async fn run(self, cancel: CancellationToken) -> Result<()> {
        let handler_cancel = cancel.child_token();
        let mut handlers = JoinSet::new();
        let result = loop {
            tokio::select! {
                _ = cancel.cancelled() => break Ok(()),
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        let context = ConnectionContext {
                            ready: self.ready.clone(),
                            grants: self.grants.clone(),
                            writer: self.writer.clone(),
                            events: self.events.clone(),
                            cancel: handler_cancel.clone(),
                        };
                        handlers.spawn(async move { serve_connection(stream, context).await });
                    }
                    Err(error) => break Err(anyhow!("root-local tickr-ctx endpoint accept failed: {error}")),
                },
            }
        };

        self.ready.store(false, Ordering::Release);
        handler_cancel.cancel();
        while let Some(joined) = handlers.join_next().await {
            if let Err(error) = joined {
                if result.is_ok() {
                    return Err(anyhow!(
                        "root-local tickr-ctx connection task failed: {error}"
                    ));
                }
            }
        }
        match &self.cleanup {
            EndpointCleanup::DataDirectory {
                data_directory,
                socket_path,
            } => data_directory
                .remove_unix_socket(socket_path)
                .context("removing root-local tickr-ctx endpoint")?,
            EndpointCleanup::Ephemeral(endpoint) => {
                if let Err(error) = fs::remove_file(endpoint) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        return Err(error).context("removing distributed tickr-ctx endpoint");
                    }
                }
            }
        }
        result
    }
}

#[derive(Clone)]
struct ConnectionContext {
    ready: Arc<AtomicBool>,
    grants: Arc<RwLock<HashMap<String, TaskGrant>>>,
    writer: TickrCtxScopeWriterClient,
    events: broadcast::Sender<ScopeEvent>,
    cancel: CancellationToken,
}

async fn serve_connection(mut stream: UnixStream, context: ConnectionContext) {
    let request = tokio::select! {
        _ = context.cancel.cancelled() => return,
        request = tickr_ctx::local::read_message::<_, LocalRequest>(&mut stream, MAX_LOCAL_REQUEST_BYTES) => {
            match request {
                Ok(request) => request,
                Err(error) => {
                    let _ = write_response(&mut stream, LocalResponse::Failure(LocalFailure::InvalidRequest {
                        message: error.to_string(),
                    })).await;
                    return;
                }
            }
        }
    };

    if !context.ready.load(Ordering::Acquire) {
        let _ = write_response(
            &mut stream,
            LocalResponse::Failure(LocalFailure::Unavailable),
        )
        .await;
        return;
    }
    let grant = match authenticate(&request, &context.grants).await {
        Ok(grant) => grant,
        Err(failure) => {
            let _ = write_response(&mut stream, LocalResponse::Failure(failure)).await;
            return;
        }
    };
    if let Err(failure) = validate_operation(&request.operation) {
        let _ = write_response(&mut stream, LocalResponse::Failure(failure)).await;
        return;
    }

    if let LocalOperation::Watch { prefix } = request.operation {
        serve_watch(stream, context, grant, prefix).await;
        return;
    }

    let response = execute_operation(&context, &grant, request.operation).await;
    let _ = write_response(&mut stream, response).await;
}

async fn authenticate(
    request: &LocalRequest,
    grants: &RwLock<HashMap<String, TaskGrant>>,
) -> std::result::Result<TaskGrant, LocalFailure> {
    if request.protocol_version != LOCAL_PROTOCOL_VERSION {
        return Err(LocalFailure::InvalidRequest {
            message: format!(
                "unsupported local tickr-ctx protocol version {}",
                request.protocol_version
            ),
        });
    }
    let grants = grants.read().await;
    let Some(grant) = grants.get(&request.task_id) else {
        return Err(LocalFailure::Unauthorized);
    };
    if !constant_time_equal(request.credential.as_bytes(), grant.credential.as_bytes())
        || request.namespace != grant.namespace
        || request.run_id != grant.run_id
        || request.task_id != grant.task_id
    {
        return Err(LocalFailure::Unauthorized);
    }
    Ok(grant.clone())
}

fn validate_operation(operation: &LocalOperation) -> std::result::Result<(), LocalFailure> {
    let key = match operation {
        LocalOperation::Get { key }
        | LocalOperation::Put { key, .. }
        | LocalOperation::Delete { key, .. } => key,
        LocalOperation::List { prefix } | LocalOperation::Watch { prefix } => prefix,
    };
    if key.len() > MAX_KEY_BYTES
        || matches!(
            operation,
            LocalOperation::Get { .. } | LocalOperation::Put { .. } | LocalOperation::Delete { .. }
        ) && key.is_empty()
    {
        return Err(LocalFailure::InvalidRequest {
            message: format!("local tickr-ctx key must contain 1 to {MAX_KEY_BYTES} bytes"),
        });
    }
    if let LocalOperation::Put { envelope, .. } = operation {
        if envelope.len() > MAX_SCOPE_VALUE_BYTES {
            return Err(LocalFailure::Bound {
                kind: LocalBoundKind::ValueBytes,
                actual: envelope.len(),
                limit: MAX_SCOPE_VALUE_BYTES,
            });
        }
        let request_bytes = key.len().saturating_add(envelope.len());
        if request_bytes > MAX_SCOPE_REQUEST_BYTES {
            return Err(LocalFailure::Bound {
                kind: LocalBoundKind::RequestBytes,
                actual: request_bytes,
                limit: MAX_SCOPE_REQUEST_BYTES,
            });
        }
    }
    Ok(())
}

async fn execute_operation(
    context: &ConnectionContext,
    grant: &TaskGrant,
    operation: LocalOperation,
) -> LocalResponse {
    match operation {
        LocalOperation::Get { key } => match context.writer.read(grant.scope_id).await {
            Ok(values) => values
                .into_iter()
                .find(|value| value.key == key)
                .map(|value| LocalResponse::Value {
                    envelope: value.envelope,
                })
                .unwrap_or(LocalResponse::Missing),
            Err(failure) => LocalResponse::Failure(failure),
        },
        LocalOperation::List { prefix } => match context.writer.read(grant.scope_id).await {
            Ok(values) => LocalResponse::Keys {
                keys: values
                    .into_iter()
                    .map(|value| value.key)
                    .filter(|key| key.starts_with(&prefix))
                    .collect(),
            },
            Err(failure) => LocalResponse::Failure(failure),
        },
        LocalOperation::Put {
            key,
            envelope,
            claim_id,
        } => match context
            .writer
            .put(grant.scope_id, claim_id, key.clone(), envelope.clone())
            .await
        {
            Ok(MutationOutcome::Applied) => {
                let _ = context.events.send(ScopeEvent {
                    scope_id: grant.scope_id,
                    event: LocalEvent {
                        operation: LocalEventOperation::Put,
                        key,
                        envelope,
                    },
                });
                LocalResponse::Applied
            }
            Ok(MutationOutcome::Idempotent) => LocalResponse::Applied,
            Ok(MutationOutcome::Missing) => LocalResponse::Missing,
            Err(failure) => LocalResponse::Failure(failure),
        },
        LocalOperation::Delete { key, claim_id } => match context
            .writer
            .delete(grant.scope_id, claim_id, key.clone())
            .await
        {
            Ok(MutationOutcome::Applied) => {
                let _ = context.events.send(ScopeEvent {
                    scope_id: grant.scope_id,
                    event: LocalEvent {
                        operation: LocalEventOperation::Delete,
                        key,
                        envelope: Vec::new(),
                    },
                });
                LocalResponse::Applied
            }
            Ok(MutationOutcome::Idempotent) => LocalResponse::Applied,
            Ok(MutationOutcome::Missing) => LocalResponse::Missing,
            Err(failure) => LocalResponse::Failure(failure),
        },
        LocalOperation::Watch { .. } => LocalResponse::Failure(LocalFailure::InvalidRequest {
            message: "watch must use the streaming request path".to_owned(),
        }),
    }
}

async fn serve_watch(
    mut stream: UnixStream,
    context: ConnectionContext,
    grant: TaskGrant,
    prefix: String,
) {
    let mut events = context.events.subscribe();
    let values = match context.writer.read(grant.scope_id).await {
        Ok(values) => values,
        Err(failure) => {
            let _ = write_response(&mut stream, LocalResponse::Failure(failure)).await;
            return;
        }
    };
    if write_response(&mut stream, LocalResponse::WatchReady)
        .await
        .is_err()
    {
        return;
    }
    for value in values
        .into_iter()
        .filter(|value| value.key.starts_with(&prefix))
    {
        let response = LocalResponse::Event(LocalEvent {
            operation: LocalEventOperation::Put,
            key: value.key,
            envelope: value.envelope,
        });
        if write_response(&mut stream, response).await.is_err() {
            return;
        }
    }

    loop {
        tokio::select! {
            _ = context.cancel.cancelled() => return,
            event = events.recv() => match event {
                Ok(event) if event.scope_id == grant.scope_id && event.event.key.starts_with(&prefix) => {
                    if write_response(&mut stream, LocalResponse::Event(event.event)).await.is_err() {
                        return;
                    }
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

async fn write_response(stream: &mut UnixStream, response: LocalResponse) -> std::io::Result<()> {
    write_message(stream, &response, MAX_LOCAL_RESPONSE_BYTES).await
}

fn validate_identity(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_IDENTITY_BYTES {
        return Err(anyhow!(
            "local tickr-ctx {label} must contain 1 to {MAX_IDENTITY_BYTES} bytes"
        ));
    }
    Ok(())
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let maximum = left.len().max(right.len());
    for index in 0..maximum {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationOutcome {
    Applied,
    Idempotent,
    Missing,
}

enum WriterRequest {
    Read {
        scope_id: Uuid,
        reply: oneshot::Sender<std::result::Result<Vec<StoredScopeValue>, LocalFailure>>,
    },
    Put {
        scope_id: Uuid,
        claim_id: Uuid,
        key: String,
        envelope: Vec<u8>,
        reply: oneshot::Sender<std::result::Result<MutationOutcome, LocalFailure>>,
    },
    Delete {
        scope_id: Uuid,
        claim_id: Uuid,
        key: String,
        reply: oneshot::Sender<std::result::Result<MutationOutcome, LocalFailure>>,
    },
}

#[derive(Clone)]
pub struct TickrCtxScopeWriterClient {
    sender: mpsc::Sender<WriterRequest>,
}

pub struct TickrCtxScopeWriter {
    store: Arc<dyn ScopeStore>,
    receiver: mpsc::Receiver<WriterRequest>,
}

impl TickrCtxScopeWriter {
    pub fn new(store: Arc<dyn ScopeStore>) -> (TickrCtxScopeWriterClient, TickrCtxScopeWriter) {
        let (sender, receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
        (
            TickrCtxScopeWriterClient { sender },
            TickrCtxScopeWriter { store, receiver },
        )
    }

    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                request = self.receiver.recv() => match request {
                    Some(request) => self.handle(request).await,
                    None => return Err(anyhow!("Tickr Lite scope writer request channel closed")),
                }
            }
        }
    }

    async fn handle(&self, request: WriterRequest) {
        match request {
            WriterRequest::Read { scope_id, reply } => {
                let result = self.store.read_tickr_ctx_scope(scope_id, Utc::now()).await;
                let _ = reply.send(match result {
                    Ok(ScopeReadOutcome::Present(values)) => Ok(values),
                    Ok(ScopeReadOutcome::Archived(_)) => Err(LocalFailure::ScopeNotWritable {
                        state: "archived".to_owned(),
                    }),
                    Ok(ScopeReadOutcome::Missing) => Err(LocalFailure::ScopeMissing),
                    Ok(ScopeReadOutcome::Bound(bound)) => Err(bound_failure(bound)),
                    Ok(ScopeReadOutcome::Quarantined { diagnostic, .. }) => {
                        Err(LocalFailure::Quarantined { diagnostic })
                    }
                    Err(error) => Err(LocalFailure::Internal {
                        message: error.to_string(),
                    }),
                });
            }
            WriterRequest::Put {
                scope_id,
                claim_id,
                key,
                envelope,
                reply,
            } => {
                let values = [ScopeValueInput {
                    key: &key,
                    envelope: &envelope,
                }];
                let result = self
                    .store
                    .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                        scope_id,
                        claim_id,
                        values: &values,
                        now: Utc::now(),
                    })
                    .await;
                let _ = reply.send(match result {
                    Ok(ScopeWriteOutcome::Applied { .. }) => Ok(MutationOutcome::Applied),
                    Ok(ScopeWriteOutcome::Idempotent) => Ok(MutationOutcome::Idempotent),
                    Ok(ScopeWriteOutcome::Missing) => Ok(MutationOutcome::Missing),
                    Ok(ScopeWriteOutcome::ClaimConflict) => Err(LocalFailure::ClaimConflict),
                    Ok(ScopeWriteOutcome::NotWritable(state)) => Err(not_writable(state)),
                    Ok(ScopeWriteOutcome::Rejected(rejection)) => Err(rejection_failure(rejection)),
                    Ok(ScopeWriteOutcome::Quarantined { diagnostic, .. }) => {
                        Err(LocalFailure::Quarantined { diagnostic })
                    }
                    Err(error) => Err(LocalFailure::Internal {
                        message: error.to_string(),
                    }),
                });
            }
            WriterRequest::Delete {
                scope_id,
                claim_id,
                key,
                reply,
            } => {
                let result = self
                    .store
                    .delete_tickr_ctx_scope_value(DeleteTickrCtxScopeInput {
                        scope_id,
                        claim_id,
                        key: &key,
                        now: Utc::now(),
                    })
                    .await;
                let _ = reply.send(match result {
                    Ok(ScopeDeleteOutcome::Deleted) => Ok(MutationOutcome::Applied),
                    Ok(ScopeDeleteOutcome::Idempotent) => Ok(MutationOutcome::Idempotent),
                    Ok(ScopeDeleteOutcome::Missing | ScopeDeleteOutcome::MissingKey) => {
                        Ok(MutationOutcome::Missing)
                    }
                    Ok(ScopeDeleteOutcome::ClaimConflict) => Err(LocalFailure::ClaimConflict),
                    Ok(ScopeDeleteOutcome::NotWritable(state)) => Err(not_writable(state)),
                    Ok(ScopeDeleteOutcome::Bound(bound)) => Err(bound_failure(bound)),
                    Ok(ScopeDeleteOutcome::Quarantined { diagnostic, .. }) => {
                        Err(LocalFailure::Quarantined { diagnostic })
                    }
                    Err(error) => Err(LocalFailure::Internal {
                        message: error.to_string(),
                    }),
                });
            }
        }
    }
}

impl TickrCtxScopeWriterClient {
    async fn read(
        &self,
        scope_id: Uuid,
    ) -> std::result::Result<Vec<StoredScopeValue>, LocalFailure> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(WriterRequest::Read { scope_id, reply })
            .await
            .map_err(|_| LocalFailure::Unavailable)?;
        response.await.map_err(|_| LocalFailure::Unavailable)?
    }

    async fn put(
        &self,
        scope_id: Uuid,
        claim_id: Uuid,
        key: String,
        envelope: Vec<u8>,
    ) -> std::result::Result<MutationOutcome, LocalFailure> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(WriterRequest::Put {
                scope_id,
                claim_id,
                key,
                envelope,
                reply,
            })
            .await
            .map_err(|_| LocalFailure::Unavailable)?;
        response.await.map_err(|_| LocalFailure::Unavailable)?
    }

    async fn delete(
        &self,
        scope_id: Uuid,
        claim_id: Uuid,
        key: String,
    ) -> std::result::Result<MutationOutcome, LocalFailure> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(WriterRequest::Delete {
                scope_id,
                claim_id,
                key,
                reply,
            })
            .await
            .map_err(|_| LocalFailure::Unavailable)?;
        response.await.map_err(|_| LocalFailure::Unavailable)?
    }
}

fn rejection_failure(rejection: ScopeMutationRejection) -> LocalFailure {
    match rejection {
        ScopeMutationRejection::EmptyRequest => LocalFailure::InvalidRequest {
            message: "local tickr-ctx mutation is empty".to_owned(),
        },
        ScopeMutationRejection::Bound(bound) => bound_failure(bound),
        ScopeMutationRejection::Envelope { key, reason } => LocalFailure::InvalidRequest {
            message: format!("local tickr-ctx envelope at `{key}` was rejected: {reason:?}"),
        },
    }
}

fn bound_failure(bound: ScopeBoundViolation) -> LocalFailure {
    let (kind, actual, limit) = match bound {
        ScopeBoundViolation::ValueBytes { actual, limit, .. } => {
            (LocalBoundKind::ValueBytes, actual, limit)
        }
        ScopeBoundViolation::RequestValues { actual, limit } => {
            (LocalBoundKind::RequestValues, actual, limit)
        }
        ScopeBoundViolation::RequestBytes { actual, limit } => {
            (LocalBoundKind::RequestBytes, actual, limit)
        }
        ScopeBoundViolation::ScopeRows { actual, limit } => {
            (LocalBoundKind::ScopeRows, actual, limit)
        }
        ScopeBoundViolation::ScopeBytes { actual, limit } => {
            (LocalBoundKind::ScopeBytes, actual, limit)
        }
        ScopeBoundViolation::ScopeAgeSeconds { actual, limit } => (
            LocalBoundKind::ScopeAgeSeconds,
            actual.max(0) as usize,
            limit.max(0) as usize,
        ),
    };
    LocalFailure::Bound {
        kind,
        actual,
        limit,
    }
}

fn not_writable(state: TickrCtxScopeState) -> LocalFailure {
    LocalFailure::ScopeNotWritable {
        state: format!("{state:?}").to_lowercase(),
    }
}
