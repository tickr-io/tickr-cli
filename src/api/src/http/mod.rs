//! HTTP API for the tickr API component. Mirrors the conductor's `http/`
//! module layout so the read handlers and their support modules port over
//! unchanged.

pub mod archive_queries;
pub mod coordinator_client;
pub mod ctx_reader;
pub mod dto;
pub mod health;
pub mod latest_run_resolver;
pub mod live_archive_merge;
pub mod logs_resolver;
pub(crate) mod openapi;
pub mod routes;

pub use routes::start_http_server;
