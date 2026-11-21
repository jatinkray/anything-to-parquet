//! Pure library for reading Prometheus / Thanos TSDB blocks.
//!
//! The reader is layout-oriented: it parses the block index, reads chunk
//! records with CRC checks, applies tombstones, and streams one series at a
//! time. It never writes to the source.

//! Prometheus/Thanos TSDB block reading for `anything-to-parquet`.
//!
//! The reader modules (`block`, `chunks`, `index`, `matcher`, `meta`, `source`,
//! `tombstones`, `varint`) are vendored from `tsdb2parqtel`
//! (https://github.com/jatinkray/tsdb-parquet-convert, Apache-2.0; see
//! PROVENANCE.md). [`adapter`] exposes them to the atp engine as a source.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod adapter;
pub mod block;
pub mod chunks;
pub mod error;
pub mod index;
pub mod matcher;
pub mod meta;
pub mod source;
pub mod tombstones;
pub mod varint;

pub use adapter::{TsdbOptions, TsdbShape, TsdbSource};
pub use block::{Block, StaleMarkerPolicy};
pub use chunks::{ChunkSamples, Encoding, FloatSample};
pub use error::{Error, Result};
pub use index::{ChunkMeta, Index, Labels, SeriesEntry};
pub use matcher::Selector;
pub use meta::{BlockMeta, DownsampleMeta, ThanosMeta};
pub use source::{
    BlockInfo, DownsamplePolicy, ErrorRecord, ExternalLabels, OnError, ScanStats, SeriesRecord,
    Source, SourceOptions,
};
