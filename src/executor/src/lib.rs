pub mod app;
// pub mod client;     // Commented out - missing proto definitions
pub mod component_liveness;
pub mod local_pickup;
pub mod self_reaping_key;
pub mod task_handler;
pub mod task_liveness;
pub mod task_log_shipper;
pub mod wire;

// Re-export proto types from tickr_proto crate
pub use tickr_proto as proto;
