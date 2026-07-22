//! API-side Command bus: the thin write surface that forwards UI and agent
//! mutations to the Conductor.
//!
//! `client` selects distributed NATS Core or bounded local request/reply while
//! retaining one protobuf and failure contract. `local` owns the private
//! in-process transport used by Tickr Lite. Per-command HTTP handlers build the
//! request envelope and render the typed response without observing the
//! selected transport.

pub mod client;
pub mod local;
