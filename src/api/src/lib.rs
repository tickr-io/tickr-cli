//! tickr API component.
//!
//! Terminates the UI's `/api/*` HTTP surface as a standalone binary, a sibling
//! of the conductor. It reads the conductor's Postgres archive, the live
//! cluster state (via the coordinator's internal HTTP), and task logs (MinIO +
//! NATS KV) — but never mutates orchestration state and never depends on the
//! conductor crate. Writes (register / trigger / cancel / wakeup) remain with
//! the conductor.

pub mod app;
pub mod commands;
pub mod config;
pub mod http;
pub mod signal_cancels;
pub mod signal_captures;
pub mod signal_wakeups;

pub use app::run_api;
