//! Bounded local request/reply transport for the Tickr Lite Command bus.
//!
//! Callers see protobuf request/reply values and transport outcomes only. The
//! bounded queue and per-request reply channel stay private to this module, so
//! no caller can hold a SQLite transaction or depend on an in-process channel.

use std::future::Future;
use std::num::NonZeroUsize;

use prost::Message as _;
use tickr_proto::coord::command_bus::{CommandRequestMetadata, DEFAULT_MAX_PAYLOAD_BYTES};
use tickr_proto::tickr_api::{ApiCommandRequest, ApiCommandResponse};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::client::BusError;

/// Local Command-bus bounds selected by the resolved formation descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalCommandBusConfig {
    pub capacity: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
}

impl Default for LocalCommandBusConfig {
    fn default() -> Self {
        Self {
            capacity: NonZeroUsize::new(64).expect("non-zero constant"),
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                .expect("non-zero constant"),
        }
    }
}

struct LocalRequest {
    metadata: CommandRequestMetadata,
    payload: Vec<u8>,
    reply: oneshot::Sender<Result<Vec<u8>, BusError>>,
}

/// Cloneable API-side handle for one local Command writer.
///
/// Its fields are private deliberately: callers cannot reach the queue or its
/// reply channels and therefore cannot bypass the request/reply contract.
#[derive(Clone)]
pub struct LocalCommandBus {
    sender: mpsc::Sender<LocalRequest>,
    max_payload_bytes: usize,
}

/// Conductor-side endpoint. There is exactly one receiver and `run` handles one
/// request at a time, serializing every mutation through its owning writer.
pub struct LocalCommandWriter {
    receiver: mpsc::Receiver<LocalRequest>,
}

/// Construct one bounded local Command bus and its sole writer endpoint.
pub fn bounded(config: LocalCommandBusConfig) -> (LocalCommandBus, LocalCommandWriter) {
    let (sender, receiver) = mpsc::channel(config.capacity.get());
    (
        LocalCommandBus {
            sender,
            max_payload_bytes: config.max_payload_bytes.get(),
        },
        LocalCommandWriter { receiver },
    )
}

impl LocalCommandBus {
    pub(crate) async fn request(
        &self,
        request: ApiCommandRequest,
        metadata: CommandRequestMetadata,
    ) -> Result<ApiCommandResponse, BusError> {
        let payload = request.encode_to_vec();
        if payload.len() > self.max_payload_bytes {
            return Err(BusError::TooLarge);
        }
        let timeout = metadata.remaining().ok_or(BusError::Timeout)?;
        let (reply, response) = oneshot::channel();
        self.sender
            .try_send(LocalRequest {
                metadata,
                payload,
                reply,
            })
            .map_err(|_| BusError::Unavailable)?;

        let bytes = tokio::time::timeout(timeout, response)
            .await
            .map_err(|_| BusError::Timeout)?
            .map_err(|_| BusError::Unavailable)??;
        ApiCommandResponse::decode(bytes.as_slice()).map_err(|_| BusError::Malformed)
    }
}

impl LocalCommandWriter {
    /// Serve requests serially until formation cancellation or all API handles
    /// are gone. The handler owns the Conductor state and returns an encoded
    /// production `ApiCommandResponse`.
    pub async fn run<F, Fut>(mut self, cancel: CancellationToken, handler: F)
    where
        F: Fn(Vec<u8>) -> Fut,
        Fut: Future<Output = Vec<u8>>,
    {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                request = self.receiver.recv() => {
                    let Some(request) = request else { break };
                    if request.metadata.is_expired() {
                        let _ = request.reply.send(Err(BusError::Timeout));
                        continue;
                    }
                    let response = handler(request.payload).await;
                    let _ = request.reply.send(Ok(response));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tickr_proto::tickr_api as api;
    use tickr_proto::tickr_api::{
        api_command_request, api_command_response, CancelRequest, ErrorPayload, PatchRequest,
        PingPayload, PingRequest, RegisterRequest, ReplayRequest, TriggerRequest, WakeupRequest,
    };

    fn metadata(deadline: Duration) -> CommandRequestMetadata {
        CommandRequestMetadata::new(uuid::Uuid::new_v4(), deadline)
    }

    fn request_bodies() -> Vec<api_command_request::Body> {
        vec![
            api_command_request::Body::Register(RegisterRequest::default()),
            api_command_request::Body::Trigger(TriggerRequest::default()),
            api_command_request::Body::Cancel(CancelRequest::default()),
            api_command_request::Body::Wakeup(WakeupRequest::default()),
            api_command_request::Body::Patch(PatchRequest::default()),
            api_command_request::Body::Replay(ReplayRequest::default()),
            api_command_request::Body::Ping(PingRequest {}),
        ]
    }

    fn request_tag(body: &api_command_request::Body) -> u8 {
        match body {
            api_command_request::Body::Register(_) => 1,
            api_command_request::Body::Trigger(_) => 2,
            api_command_request::Body::Cancel(_) => 3,
            api_command_request::Body::Wakeup(_) => 4,
            api_command_request::Body::Patch(_) => 5,
            api_command_request::Body::Replay(_) => 6,
            api_command_request::Body::Ping(_) => 7,
        }
    }

    fn response_tag(payload: &api_command_response::Payload) -> u8 {
        match payload {
            api_command_response::Payload::Register(_) => 1,
            api_command_response::Payload::Trigger(_) => 2,
            api_command_response::Payload::Cancel(_) => 3,
            api_command_response::Payload::Wakeup(_) => 4,
            api_command_response::Payload::Patch(_) => 5,
            api_command_response::Payload::Replay(_) => 6,
            api_command_response::Payload::Ping(_) => 7,
            api_command_response::Payload::Error(_) => 8,
        }
    }

    #[tokio::test]
    async fn every_production_request_variant_round_trips_over_one_serial_writer() {
        let (local, writer) = bounded(LocalCommandBusConfig::default());
        let cancel = CancellationToken::new();
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(AtomicUsize::new(0));
        let task = {
            let in_flight = Arc::clone(&in_flight);
            let peak = Arc::clone(&peak);
            let observed = Arc::clone(&observed);
            tokio::spawn(writer.run(cancel.clone(), move |bytes| {
                let in_flight = Arc::clone(&in_flight);
                let peak = Arc::clone(&peak);
                let observed = Arc::clone(&observed);
                async move {
                    let request = ApiCommandRequest::decode(bytes.as_slice()).unwrap();
                    let payload = match request.body.unwrap() {
                        api_command_request::Body::Register(_) => {
                            api_command_response::Payload::Register(api::RegisterPayload::default())
                        }
                        api_command_request::Body::Trigger(_) => {
                            api_command_response::Payload::Trigger(api::TriggerPayload::default())
                        }
                        api_command_request::Body::Cancel(_) => {
                            api_command_response::Payload::Cancel(api::CancelPayload::default())
                        }
                        api_command_request::Body::Wakeup(_) => {
                            api_command_response::Payload::Wakeup(api::WakeupPayload::default())
                        }
                        api_command_request::Body::Patch(_) => {
                            api_command_response::Payload::Patch(api::PatchPayload::default())
                        }
                        api_command_request::Body::Replay(_) => {
                            api_command_response::Payload::Replay(api::ReplayPayload::default())
                        }
                        api_command_request::Body::Ping(_) => {
                            api_command_response::Payload::Ping(PingPayload {})
                        }
                    };
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(current, Ordering::SeqCst);
                    observed.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    ApiCommandResponse {
                        status_code: 200,
                        payload: Some(payload),
                    }
                    .encode_to_vec()
                }
            }))
        };

        let calls = request_bodies().into_iter().map(|body| {
            let local = local.clone();
            let expected_tag = request_tag(&body);
            tokio::spawn(async move {
                let response = local
                    .request(
                        ApiCommandRequest { body: Some(body) },
                        metadata(Duration::from_secs(1)),
                    )
                    .await
                    .unwrap();
                (expected_tag, response)
            })
        });
        for call in calls {
            let (expected_tag, response) = call.await.unwrap();
            assert_eq!(response.status_code, 200);
            assert_eq!(
                response_tag(response.payload.as_ref().unwrap()),
                expected_tag
            );
        }
        assert_eq!(observed.load(Ordering::SeqCst), 7);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
        cancel.cancel();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn unavailable_timeout_malformed_and_payload_limit_match_command_bus_outcomes() {
        let config = LocalCommandBusConfig {
            capacity: NonZeroUsize::new(1).unwrap(),
            max_payload_bytes: NonZeroUsize::new(8).unwrap(),
        };
        let (unavailable, writer) = bounded(config);
        drop(writer);
        assert!(matches!(
            unavailable
                .request(
                    ApiCommandRequest::default(),
                    metadata(Duration::from_secs(1))
                )
                .await,
            Err(BusError::Unavailable)
        ));

        let (timed_out, writer) = bounded(LocalCommandBusConfig::default());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(writer.run(cancel.clone(), |_| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Vec::new()
        }));
        assert!(matches!(
            timed_out
                .request(
                    ApiCommandRequest::default(),
                    metadata(Duration::from_millis(10))
                )
                .await,
            Err(BusError::Timeout)
        ));
        cancel.cancel();
        task.await.unwrap();

        let (malformed, writer) = bounded(LocalCommandBusConfig::default());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(writer.run(cancel.clone(), |_| async { vec![0xff] }));
        assert!(matches!(
            malformed
                .request(
                    ApiCommandRequest::default(),
                    metadata(Duration::from_secs(1))
                )
                .await,
            Err(BusError::Malformed)
        ));
        cancel.cancel();
        task.await.unwrap();

        let (too_large, _writer) = bounded(config);
        let request = ApiCommandRequest {
            body: Some(api_command_request::Body::Register(RegisterRequest {
                nickel_source: "larger than eight bytes".to_string(),
                namespace: String::new(),
            })),
        };
        assert!(matches!(
            too_large
                .request(request, metadata(Duration::from_secs(1)))
                .await,
            Err(BusError::TooLarge)
        ));
    }

    #[tokio::test]
    async fn unsupported_and_cancelled_requests_do_not_break_the_writer() {
        let (local, writer) = bounded(LocalCommandBusConfig::default());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(writer.run(cancel.clone(), |bytes| async move {
            let request = ApiCommandRequest::decode(bytes.as_slice()).unwrap();
            if request.body.is_none() {
                ApiCommandResponse {
                    status_code: 501,
                    payload: Some(api_command_response::Payload::Error(ErrorPayload {
                        code: api::CommandErrorCode::UnsupportedCommand as i32,
                        message: "unsupported command".to_string(),
                    })),
                }
                .encode_to_vec()
            } else {
                ApiCommandResponse {
                    status_code: 200,
                    payload: Some(api_command_response::Payload::Ping(PingPayload {})),
                }
                .encode_to_vec()
            }
        }));

        let unsupported = local
            .request(
                ApiCommandRequest::default(),
                metadata(Duration::from_secs(1)),
            )
            .await
            .unwrap();
        assert_eq!(unsupported.status_code, 501);

        let cancelled = {
            let local = local.clone();
            tokio::spawn(async move {
                local
                    .request(
                        ApiCommandRequest {
                            body: Some(api_command_request::Body::Ping(PingRequest {})),
                        },
                        metadata(Duration::from_secs(1)),
                    )
                    .await
            })
        };
        cancelled.abort();
        let _ = cancelled.await;

        let response = local
            .request(
                ApiCommandRequest {
                    body: Some(api_command_request::Body::Ping(PingRequest {})),
                },
                metadata(Duration::from_secs(1)),
            )
            .await
            .unwrap();
        assert_eq!(response.status_code, 200);
        cancel.cancel();
        task.await.unwrap();
    }
}
