//! Codecs for Tickr's published protobuf contracts.
//!
//! These modules provide JSON/protobuf archive rehydration, definition and
//! signal transforms, and compaction envelope encoding from one shared wire
//! model.

pub mod archive;
pub mod compaction;
pub mod definition;
pub mod signal;
