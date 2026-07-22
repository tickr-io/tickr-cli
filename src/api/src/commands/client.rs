//! Formation-selected Command-bus client. Encodes an `ApiCommandRequest`,
//! sends it over distributed NATS Core or bounded local request/reply with a
//! per-command deadline, and decodes the `ApiCommandResponse`. Transport
//! failures map to synthesized HTTP responses; a decoded reply is handed to
//! the per-command handler, which forwards the Conductor's `status_code`
//! verbatim.

use std::time::Duration;

use super::local::{bounded, LocalCommandBus, LocalCommandBusConfig, LocalCommandWriter};
use async_nats::Client;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use prost::Message as _;

use tickr_proto::tickr_api::{
    api_command_request, ApiCommandRequest, ApiCommandResponse, CommandErrorCode, PingRequest,
};

/// The single subject every command kind travels on. Names the
/// API<->conductor relationship rather than the verb.
pub const COMMAND_SUBJECT: &str = "tickr.api.commands";

/// API-side Command bus selected by the resolved formation.
///
/// Both transports carry the same production protobuf envelopes and expose
/// the same deadline, availability, malformed-reply, and payload-limit
/// outcomes. Transport internals are not visible to HTTP handlers.
#[derive(Clone)]
pub enum CommandBus {
    Nats(Client),
    Local(LocalCommandBus),
}

impl CommandBus {
    pub fn nats(client: Client) -> Self {
        Self::Nats(client)
    }

    /// Construct the Tickr Lite Command bus and its sole Conductor writer.
    pub fn local(config: LocalCommandBusConfig) -> (Self, LocalCommandWriter) {
        let (client, writer) = bounded(config);
        (Self::Local(client), writer)
    }

    pub async fn send(
        &self,
        request: ApiCommandRequest,
        deadline: Duration,
    ) -> Result<ApiCommandResponse, BusError> {
        match self {
            Self::Nats(client) => send_command(client, request, deadline).await,
            Self::Local(client) => client.request(request, deadline).await,
        }
    }
}

/// Per-command request deadlines. The API's HTTP timeout equals the selected
/// transport deadline; the floors differ because the commands have genuinely
/// different latency profiles. Defaults are the hard values;
/// an operator override seam can replace this struct without touching call
/// sites.
#[derive(Clone, Copy, Debug)]
pub struct CommandDeadlines {
    pub register: Duration,
    pub trigger: Duration,
    pub cancel: Duration,
    pub wakeup: Duration,
    /// Patch ingress: the conductor evaluates the raw Nickel document (a
    /// subprocess `nickel export`) before it acks, so this shares register's
    /// generous floor — it is a parse-and-open-row round-trip, not the apply,
    /// which the submitter polls for asynchronously off the lifecycle row.
    pub patch: Duration,
    /// Replay ingress: the conductor reads the archive, mints the seed, opens
    /// the row, relays the materialise Trigger, re-hydrates the ctx scope, and
    /// releases the born-Stall before it acks. A few archive reads plus NATS KV
    /// writes — generous like patch, still a round-trip, not a run.
    pub replay: Duration,
    /// Ping: a side-effect-free command-consumer liveness probe (the health
    /// surface's Conductor row). The conductor does nothing but ack, so the
    /// floor is tight — a slow or absent reply *is* the not-responsive signal.
    pub ping: Duration,
}

impl Default for CommandDeadlines {
    fn default() -> Self {
        Self {
            register: Duration::from_secs(30),
            trigger: Duration::from_secs(5),
            cancel: Duration::from_secs(20),
            wakeup: Duration::from_secs(10),
            patch: Duration::from_secs(30),
            replay: Duration::from_secs(30),
            ping: Duration::from_secs(2),
        }
    }
}

/// Transport-level failure of a command round-trip — distinct from a
/// conductor-returned error, which rides inside a decoded `ApiCommandResponse`
/// as an `ErrorPayload`.
#[derive(Debug)]
pub enum BusError {
    /// The selected transport or its Conductor responder is unavailable. -> 503.
    Unavailable,
    /// The conductor didn't reply within the deadline. -> 504.
    Timeout,
    /// A reply arrived but didn't decode as `ApiCommandResponse`. -> 502.
    Malformed,
    /// The encoded command exceeded the selected transport's payload limit.
    /// Reachable on register and Patch, which carry raw Nickel source. -> 413.
    TooLarge,
}

/// Encode and send one command, awaiting the conductor's reply within
/// `deadline`. The deadline doubles as the NATS request timeout, so a slow
/// conductor surfaces as `BusError::Timeout` rather than hanging.
pub async fn send_command(
    nats: &Client,
    request: ApiCommandRequest,
    deadline: Duration,
) -> Result<ApiCommandResponse, BusError> {
    let bytes = request.encode_to_vec();
    let req = async_nats::Request::new()
        .payload(bytes.into())
        .timeout(Some(deadline));
    match nats.send_request(COMMAND_SUBJECT, req).await {
        Ok(msg) => {
            ApiCommandResponse::decode(msg.payload.as_ref()).map_err(|_| BusError::Malformed)
        }
        Err(e) => match e.kind() {
            async_nats::client::RequestErrorKind::NoResponders => Err(BusError::Unavailable),
            async_nats::client::RequestErrorKind::TimedOut => Err(BusError::Timeout),
            // The command outgrew the broker's max payload (rejected
            // client-side before it left). -> 413.
            async_nats::client::RequestErrorKind::MaxPayloadExceeded => Err(BusError::TooLarge),
            // A client/IO-level failure — including an unroutable subject —
            // means the bus didn't carry the command; surface it as
            // unavailable rather than a 500.
            async_nats::client::RequestErrorKind::InvalidSubject
            | async_nats::client::RequestErrorKind::Other => Err(BusError::Unavailable),
        },
    }
}

/// Issue a side-effect-free Ping over the selected Command bus.
pub async fn ping_command_bus(
    command_bus: &CommandBus,
    deadline: Duration,
) -> Result<(), BusError> {
    let request = ApiCommandRequest {
        body: Some(api_command_request::Body::Ping(PingRequest {})),
    };
    command_bus.send(request, deadline).await.map(|_| ())
}

/// Issue a side-effect-free `Ping` over the command bus and report whether the
/// command consumer answered within `deadline`. This is a dedicated variant, not
/// a reuse of a read command, so the probe touches no state — an explicit "does
/// the command consumer answer" check. It backs the health surface's Conductor
/// row: `Ok(())` ⇒ a reply decoded (the command plane is responsive); `Err` ⇒
/// `NoResponders`/timeout/malformed (a wedged or absent command consumer, even
/// while the broker link is up). It asserts only that the command consumer
/// answers — NOT that the relay loop is live (a relay-liveness check is deferred).
pub async fn ping_conductor(nats: &Client, deadline: Duration) -> Result<(), BusError> {
    ping_command_bus(&CommandBus::nats(nats.clone()), deadline).await
}

/// Render a transport failure into its synthesized HTTP response. These are
/// the only statuses the API mints itself; everything else is the conductor's
/// `status_code` forwarded verbatim.
pub fn bus_error_response(e: BusError) -> Response {
    match e {
        BusError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "command bus unavailable"})),
        )
            .into_response(),
        BusError::Timeout => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(serde_json::json!({"error": "upstream timeout"})),
        )
            .into_response(),
        BusError::Malformed => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": "malformed upstream reply"})),
        )
            .into_response(),
        BusError::TooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({"error": "command payload too large"})),
        )
            .into_response(),
    }
}

/// Generic rendering of a conductor-returned `ErrorPayload`: forwards the
/// envelope's `status_code` and renders `{error: <CODE_NAME>, message}`. Used
/// for the `UNSUPPORTED_COMMAND` -> 501 path and as a fallback for commands
/// whose handler doesn't special-case the error body shape. Per-command
/// handlers that must preserve a different historical error body (e.g.
/// register's `{success:false, message}`) render the `ErrorPayload` themselves
/// instead of calling this.
pub fn public_error_message(status: StatusCode, message: String) -> String {
    if status.is_server_error() {
        "internal server error".to_string()
    } else {
        message
    }
}

pub fn error_payload_response(status_code: u32, code: i32, message: String) -> Response {
    let status =
        StatusCode::from_u16(status_code as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let code_name = CommandErrorCode::try_from(code)
        .unwrap_or(CommandErrorCode::Internal)
        .as_str_name();
    let message = public_error_message(status, message);
    (
        status,
        Json(serde_json::json!({"error": code_name, "message": message})),
    )
        .into_response()
}

#[cfg(test)]
mod response_security_tests {
    use super::*;

    #[test]
    fn server_errors_are_redacted_but_client_errors_are_preserved() {
        let secret = "postgres://user:secret@host/db logs.secret.subject".to_string();
        assert_eq!(
            public_error_message(StatusCode::INTERNAL_SERVER_ERROR, secret.clone()),
            "internal server error"
        );
        assert_eq!(
            public_error_message(StatusCode::BAD_REQUEST, secret.clone()),
            secret
        );
    }
}
