//! Partitioned Parquet sink.
//!
//! Guarantees:
//! - a `.parquet` file only appears after it is fully written (`.tmp` + rename);
//! - every file is hashed while it is written, so `atp verify` can prove the
//!   bytes did not change after the fact without a second pass;
//! - row groups are bounded (`--row-group-rows`), files are bounded
//!   (`--max-file-rows`), and open files are capped (`--max-open-files`) so a
//!   run with hundreds of day-partitions does not exhaust file descriptors.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_select::take::take;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression as PqCompression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;

use crate::error::{Error, Result};
use crate::value::Schema;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Zstd,
    Snappy,
    Uncompressed,
}

impl Compression {
    pub fn parse(s: &str) -> Option<Compression> {
        Some(match s.to_ascii_lowercase().as_str() {
            "zstd" => Compression::Zstd,
            "snappy" => Compression::Snappy,
            "none" | "uncompressed" => Compression::Uncompressed,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Compression::Zstd => "zstd",
            Compression::Snappy => "snappy",
            Compression::Uncompressed => "uncompressed",
        }
    }

    fn to_parquet(self) -> Result<PqCompression> {
        Ok(match self {
            Compression::Zstd => PqCompression::ZSTD(
                ZstdLevel::try_new(3).map_err(|e| Error::msg(format!("zstd level: {e}")))?,
            ),
            Compression::Snappy => PqCompression::SNAPPY,
            Compression::Uncompressed => PqCompression::UNCOMPRESSED,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Partition {
    None,
    Hour,
    Day,
}

impl Partition {
    pub fn parse(s: &str) -> Option<Partition> {
        Some(match s.to_ascii_lowercase().as_str() {
            "none" => Partition::None,
            "hour" => Partition::Hour,
            "day" => Partition::Day,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Partition::None => "none",
            Partition::Hour => "hour",
            Partition::Day => "day",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SinkOptions {
    pub compression: Compression,
    pub row_group_rows: usize,
    pub max_open_files: usize,
    pub max_file_rows: u64,
}

impl Default for SinkOptions {
    fn default() -> Self {
        SinkOptions {
            compression: Compression::Zstd,
            row_group_rows: 100_000,
            max_open_files: 16,
            max_file_rows: 5_000_000,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileRecord {
    /// Path relative to the output root, with `/` separators.
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    /// xxh3-64 of the file bytes, hex.
    pub digest: String,
    pub schema_hash: String,
}

#[derive(Default)]
struct HashState {
    hasher: Option<Xxh3>,
    bytes: u64,
}

impl HashState {
    fn new() -> Self {
        HashState {
            hasher: Some(Xxh3::new()),
            bytes: 0,
        }
    }

    fn update(&mut self, buf: &[u8]) {
        if let Some(h) = self.hasher.as_mut() {
            h.update(buf);
        }
        self.bytes += buf.len() as u64;
    }

    fn digest(&self) -> String {
        match &self.hasher {
            Some(h) => format!("{:016x}", h.digest()),
            None => String::new(),
        }
    }
}

struct HashingWriter {
    inner: BufWriter<File>,
    state: Arc<Mutex<HashState>>,
}

impl Write for HashingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if let Ok(mut st) = self.state.lock() {
            st.update(&buf[..n]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct OpenFile {
    seq: u32,
    segment: u32,
    writer: ArrowWriter<HashingWriter>,
    rows: u64,
    state: Arc<Mutex<HashState>>,
    schema_hash: String,
}

pub struct ParquetSink {
    root: PathBuf,
    options: SinkOptions,
    schema: Schema,
    segment: u32,
    seq: u32,
    open: HashMap<String, OpenFile>,
    order: Vec<String>,
    files: Vec<FileRecord>,
    schemas: Vec<Schema>,
    bytes: u64,
    rows: u64,
}

impl ParquetSink {
    pub fn open(root: impl Into<PathBuf>, options: SinkOptions, schema: Schema) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|e| Error::Path {
            action: "create",
            path: root.clone(),
            reason: e.to_string(),
        })?;
        let schemas = vec![schema.clone()];
        Ok(ParquetSink {
            root,
            options,
            schema,
            segment: 0,
            seq: 0,
            open: HashMap::new(),
            order: Vec::new(),
            files: Vec::new(),
            schemas,
            bytes: 0,
            rows: 0,
        })
    }

    /// Bytes written so far, including files still open.
    pub fn bytes(&self) -> u64 {
        let live: u64 = self
            .open
            .values()
            .filter_map(|f| f.state.lock().ok().map(|s| s.bytes))
            .sum();
        self.bytes + live
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn files_written(&self) -> usize {
        self.files.len()
    }

    pub fn schemas(&self) -> &[Schema] {
        &self.schemas
    }

    pub fn files(&self) -> &[FileRecord] {
        &self.files
    }

    /// Close every open file and continue with a widened schema. The manifest
    /// records all schemas; files already written keep their original types.
    pub fn roll(&mut self, schema: Schema) -> Result<()> {
        if schema == self.schema {
            return Ok(());
        }
        self.close_all()?;
        self.segment += 1;
        self.seq = 0;
        self.schemas.push(schema.clone());
        self.schema = schema;
        Ok(())
    }

    /// Write one batch. `partitions[i]` is the partition directory for row `i`;
    /// `None` writes a single unpartitioned file series.
    pub fn write(&mut self, batch: &RecordBatch, partitions: Option<&[String]>) -> Result<()> {
        let Some(labels) = partitions else {
            return self.write_group(batch, "");
        };
        if labels.len() != batch.num_rows() {
            return Err(Error::msg(format!(
                "internal: {} partition labels for {} rows",
                labels.len(),
                batch.num_rows()
            )));
        }
        let mut groups: Vec<(String, Vec<u32>)> = Vec::new();
        for (i, label) in labels.iter().enumerate() {
            match groups.iter_mut().find(|(k, _)| k == label) {
                Some((_, idx)) => idx.push(i as u32),
                None => groups.push((label.clone(), vec![i as u32])),
            }
        }
        for (label, indices) in groups {
            let sub = if indices.len() == batch.num_rows() {
                batch.clone()
            } else {
                take_columns(batch, &indices)?
            };
            self.write_group(&sub, &label)?;
        }
        Ok(())
    }

    fn write_group(&mut self, batch: &RecordBatch, partition: &str) -> Result<()> {
        if !self.open.contains_key(partition) {
            self.open_file(partition)?;
        }
        let entry = self
            .open
            .get_mut(partition)
            .ok_or_else(|| Error::msg("internal: writer just opened but missing"))?;
        entry.writer.write(batch)?;
        entry.rows += batch.num_rows() as u64;
        self.rows += batch.num_rows() as u64;
        if entry.rows >= self.options.max_file_rows {
            self.close_partition(partition)?;
        }
        Ok(())
    }

    fn rel_path(&self, partition: &str, segment: u32, seq: u32) -> String {
        if partition.is_empty() {
            format!("part-{segment:02}-{seq:05}.parquet")
        } else {
            format!("{partition}/part-{segment:02}-{seq:05}.parquet")
        }
    }

    fn open_file(&mut self, partition: &str) -> Result<()> {
        while self.open.len() >= self.options.max_open_files.max(1) {
            match self.order.first().cloned() {
                Some(oldest) => self.close_partition(&oldest)?,
                None => break,
            }
        }
        let rel = self.rel_path(partition, self.segment, self.seq);
        let seq = self.seq;
        let segment = self.segment;
        self.seq += 1;
        let path = self.root.join(&rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = tmp_path(&path);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        let props = WriterProperties::builder()
            .set_compression(self.options.compression.to_parquet()?)
            .set_max_row_group_row_count(Some(self.options.row_group_rows.max(1)))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_dictionary_enabled(true)
            .set_created_by(format!("anything-to-parquet {}", env!("CARGO_PKG_VERSION")))
            .build();
        let state = Arc::new(Mutex::new(HashState::new()));
        let writer = ArrowWriter::try_new(
            HashingWriter {
                inner: BufWriter::with_capacity(256 * 1024, file),
                state: state.clone(),
            },
            Arc::new(self.schema.to_arrow()),
            Some(props),
        )?;
        self.open.insert(
            partition.to_string(),
            OpenFile {
                seq,
                segment,
                writer,
                rows: 0,
                state,
                schema_hash: self.schema.hash(),
            },
        );
        self.order.push(partition.to_string());
        Ok(())
    }

    fn close_partition(&mut self, partition: &str) -> Result<()> {
        let Some(open) = self.open.remove(partition) else {
            return Ok(());
        };
        self.order.retain(|p| p != partition);
        let OpenFile {
            seq,
            segment,
            writer,
            rows,
            state,
            schema_hash,
        } = open;
        let mut hashing = writer.into_inner()?;
        hashing.flush()?;
        let (digest, bytes) = match state.lock() {
            Ok(st) => (st.digest(), st.bytes),
            Err(_) => (String::new(), 0),
        };
        drop(hashing);
        let rel = self.rel_path(partition, segment, seq);
        let path = self.root.join(&rel);
        std::fs::rename(tmp_path(&path), &path)?;
        self.bytes += bytes;
        self.files.push(FileRecord {
            path: rel,
            rows,
            bytes,
            digest,
            schema_hash,
        });
        Ok(())
    }

    fn close_all(&mut self) -> Result<()> {
        let partitions: Vec<String> = self.open.keys().cloned().collect();
        for p in partitions {
            self.close_partition(&p)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<SinkResult> {
        self.close_all()?;
        let mut files = std::mem::take(&mut self.files);
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(SinkResult {
            root: self.root,
            files,
            schemas: self.schemas,
            rows: self.rows,
            bytes: self.bytes,
        })
    }
}

pub struct SinkResult {
    pub root: PathBuf,
    pub files: Vec<FileRecord>,
    pub schemas: Vec<Schema>,
    pub rows: u64,
    pub bytes: u64,
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    name.push_str(".tmp");
    path.with_file_name(name)
}

fn take_columns(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let idx = UInt32Array::from(indices.to_vec());
    let arrays: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c, &idx, None))
        .collect::<std::result::Result<_, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), arrays)?)
}

/// Partition directory for a timestamp in epoch millis. Rows without a
/// timestamp go to `dt=_unknown` rather than being dropped.
pub fn partition_label(partition: Partition, timestamp_ms: Option<i64>) -> Option<String> {
    match partition {
        Partition::None => None,
        Partition::Hour | Partition::Day => {
            let label = match timestamp_ms.and_then(chrono::DateTime::from_timestamp_millis) {
                Some(dt) => match partition {
                    Partition::Hour => dt.format("dt=%Y-%m-%d/hour=%H").to_string(),
                    _ => dt.format("dt=%Y-%m-%d").to_string(),
                },
                None => "dt=_unknown".to_string(),
            };
            Some(label)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{Field, Ty};
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field as AField, Schema as ASchema};
    use std::sync::Arc;

    fn float_batch(n: f64, name: &str) -> RecordBatch {
        let schema = Arc::new(ASchema::new(vec![
            AField::new("n", DataType::Float64, true),
            AField::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Float64Array::from(vec![n; 3])),
                Arc::new(StringArray::from(vec![name; 3])),
            ],
        )
        .expect("batch")
    }

    fn batch(n: i64, name: &str) -> RecordBatch {
        let schema = Arc::new(ASchema::new(vec![
            AField::new("n", DataType::Int64, true),
            AField::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![n; 3])),
                Arc::new(StringArray::from(vec![name; 3])),
            ],
        )
        .expect("batch")
    }

    fn schema() -> Schema {
        Schema::new(vec![Field::new("n", Ty::I64), Field::new("s", Ty::Str)])
    }

    #[test]
    fn writes_one_file_and_hashes_it() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut sink =
            ParquetSink::open(dir.path(), SinkOptions::default(), schema()).expect("open");
        sink.write(&batch(1, "a"), None).expect("write");
        let out = sink.finish().expect("finish");
        assert_eq!(out.files.len(), 1);
        assert_eq!(out.files[0].rows, 3);
        assert_eq!(out.files[0].path, "part-00-00000.parquet");
        assert_eq!(out.files[0].digest.len(), 16);
        assert!(dir.path().join(&out.files[0].path).exists());
        assert!(!dir.path().join("part-00-00000.parquet.tmp").exists());
    }

    #[test]
    fn partitions_split_batches_by_label() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut sink =
            ParquetSink::open(dir.path(), SinkOptions::default(), schema()).expect("open");
        let labels = vec![
            "dt=2026-11-21/hour=10".to_string(),
            "dt=2026-11-21/hour=11".to_string(),
            "dt=2026-11-21/hour=10".to_string(),
        ];
        sink.write(&batch(7, "x"), Some(&labels)).expect("write");
        let out = sink.finish().expect("finish");
        assert_eq!(out.files.len(), 2);
        let mut rows = 0;
        for f in &out.files {
            rows += f.rows;
            assert!(f.path.contains("hour="), "{}", f.path);
        }
        assert_eq!(rows, 3);
    }

    #[test]
    fn partition_labels_are_utc_and_explicit_about_missing_timestamps() {
        let ms = 1_795_255_200_000; // 2026-11-21T10:00:00Z
        assert_eq!(
            partition_label(Partition::Hour, Some(ms)).as_deref(),
            Some("dt=2026-11-21/hour=10")
        );
        assert_eq!(
            partition_label(Partition::Day, Some(ms)).as_deref(),
            Some("dt=2026-11-21")
        );
        assert_eq!(
            partition_label(Partition::Day, None).as_deref(),
            Some("dt=_unknown")
        );
        assert_eq!(partition_label(Partition::None, Some(ms)), None);
    }

    #[test]
    fn rolling_schema_closes_files_and_keeps_both_schemas() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut sink =
            ParquetSink::open(dir.path(), SinkOptions::default(), schema()).expect("open");
        sink.write(&batch(1, "a"), None).expect("write");
        let mut wider = schema();
        wider.fields[0].ty = Ty::F64;
        sink.roll(wider.clone()).expect("roll");
        sink.write(&float_batch(2.5, "b"), None).expect("write");
        let out = sink.finish().expect("finish");
        assert_eq!(out.schemas.len(), 2);
        assert_eq!(out.files.len(), 2);
        assert_eq!(out.files[0].schema_hash, schema().hash());
        assert_eq!(out.files[1].schema_hash, wider.hash());
    }

    #[test]
    fn open_file_cap_forces_closing() {
        let dir = tempfile::tempdir().expect("tmp");
        let opts = SinkOptions {
            max_open_files: 2,
            ..SinkOptions::default()
        };
        let mut sink = ParquetSink::open(dir.path(), opts, schema()).expect("open");
        for h in 0..5 {
            let labels = vec![format!("dt=2026-11-2{}/hour=0{h}", h); 3];
            sink.write(&batch(h as i64, "v"), Some(&labels))
                .expect("write");
        }
        let out = sink.finish().expect("finish");
        assert_eq!(out.files.len(), 5);
        assert_eq!(out.rows, 15);
    }
}
