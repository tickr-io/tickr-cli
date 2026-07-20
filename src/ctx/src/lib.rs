//! Library surface for tickr-ctx. The CLI binary at `src/main.rs` is built
//! on top of these modules; downstream crates (e.g. the conductor's
//! signal-derived captures path) link against them directly so envelope
//! shape and KV bucket conventions stay in one place.

pub mod ambient;
pub mod envelope;
pub mod scope;
pub mod store;
