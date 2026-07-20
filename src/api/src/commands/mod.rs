//! API-side command bus: the thin write surface that forwards UI/agent write
//! requests to the conductor over NATS core request/reply.
//!
//! `client` owns the transport (encode the proto envelope, `nats.request(...)`
//! with the per-command deadline, decode the reply, map transport failures to
//! HTTP responses). The per-command HTTP handlers in `http::routes` build the
//! request envelope from the parsed body and render the typed reply payload
//! into today's HTTP body shape.

pub mod client;
