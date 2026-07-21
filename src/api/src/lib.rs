//! tickr API component.
//!
//! Terminates the UI's `/api/*` HTTP surface as a standalone binary, a sibling
//! of the Conductor. It reads the selected Data-plane repository, the live
//! cluster state (via the coordinator's internal HTTP), and Task logs (object
//! storage plus NATS KV), but never mutates orchestration state or depends on the
//! conductor crate. Writes (register / trigger / cancel / wakeup) remain with
//! the conductor.

pub mod app;
pub mod commands;
pub mod config;
pub mod http;
pub mod repository;

pub use app::run_api;
