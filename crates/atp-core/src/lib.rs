//! Conversion engine for `anything-to-parquet`.
//!
//! Layering: sources produce `(columns, rows)` pages -> the engine owns schema
//! evolution -> the sink writes partitioned Parquet and hashes it -> the
//! manifest records what happened. The CLI and the TUI are both thin clients of
//! [`engine::run`]; neither contains conversion logic.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod batch;
pub mod engine;
pub mod error;
pub mod flatten;
pub mod manifest;
pub mod sink;
pub mod source;
pub mod tui;
pub mod value;

pub use error::{Error, Result};
pub use value::{Field, Schema, Ty, Value};
