//! Prometheus/Thanos block source for the atp engine.
//!
//! The vendored reader walks blocks synchronously and emits one [`SeriesRecord`]
//! at a time; this adapter streams those records from a dedicated thread through
//! a two-slot channel, so conversion memory stays bounded by one series plus one
//! page, not by the block set. Rows are samples:
//!
//! ```text
//! timestamp | metric | value | series_fingerprint | <label columns...>     (wide)
//! timestamp | metric | value | series_fingerprint | labels (JSON)          (long)
//! ```
//!
//! `series_fingerprint` is the same xxh3-64 identity `tsdb2parqtel` writes, so a
//! dataset converted by either tool can be reconciled on that column.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use atp_core::source::{Page, Source, SourceInfo};
use atp_core::value::{Field, Schema, Ty, Value};
use atp_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::matcher::Selector;
use crate::source::{
    DownsamplePolicy, ExternalLabels, OnError, SeriesRecord, Source as BlockSource, SourceOptions,
};
use crate::StaleMarkerPolicy;

pub const TIMESTAMP_COLUMN: &str = "timestamp";
pub const METRIC_COLUMN: &str = "metric";
pub const VALUE_COLUMN: &str = "value";
pub const FINGERPRINT_COLUMN: &str = "series_fingerprint";
pub const LABELS_COLUMN: &str = "labels";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TsdbShape {
    /// `timestamp, metric, value, series_fingerprint`: the compact default.
    Series,
    /// Adds one column per label name, sorted; absent labels are null.
    Wide,
    /// Adds one JSON object column with every label.
    Long,
}

impl TsdbShape {
    pub fn parse(s: &str) -> Option<TsdbShape> {
        Some(match s.to_ascii_lowercase().as_str() {
            "series" | "lean" => TsdbShape::Series,
            "wide" | "labels-wide" => TsdbShape::Wide,
            "long" | "labels" => TsdbShape::Long,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TsdbShape::Series => "series",
            TsdbShape::Wide => "wide",
            TsdbShape::Long => "long",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TsdbOptions {
    /// Local block directory, a directory of blocks, or `s3://`/`gs://`/`az://`.
    pub path: String,
    pub shape: TsdbShape,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub blocks: Vec<String>,
    pub matchers: Vec<String>,
    pub stale: StaleMarkerPolicy,
    pub on_error: OnError,
    pub external_labels: ExternalLabels,
    pub downsampled: DownsamplePolicy,
    pub dedup_replica_labels: Vec<String>,
}

impl TsdbOptions {
    pub fn new(path: impl Into<String>) -> Self {
        TsdbOptions {
            path: path.into(),
            shape: TsdbShape::Series,
            from_ms: None,
            to_ms: None,
            blocks: Vec::new(),
            matchers: Vec::new(),
            stale: StaleMarkerPolicy::Drop,
            on_error: OnError::Fail,
            external_labels: ExternalLabels::Include,
            downsampled: DownsamplePolicy::PreferRaw,
            dedup_replica_labels: Vec::new(),
        }
    }

    fn source_options(&self) -> Result<SourceOptions> {
        let mut options = SourceOptions::new(&self.path);
        options.min_time_ms = self.from_ms;
        options.max_time_ms = self.to_ms;
        options.block_ulids = self.blocks.iter().cloned().collect();
        options.stale_markers = self.stale;
        options.on_error = self.on_error;
        options.external_labels = self.external_labels;
        options.downsampled = self.downsampled;
        options.dedup_replica_labels = self.dedup_replica_labels.clone();
        if !self.matchers.is_empty() {
            let mut parts = Vec::with_capacity(self.matchers.len());
            for raw in &self.matchers {
                let trimmed = raw.trim();
                let body = trimmed
                    .strip_prefix('{')
                    .and_then(|v| v.strip_suffix('}'))
                    .unwrap_or(trimmed);
                parts.push(body.to_string());
            }
            let selector = format!("{{{}}}", parts.join(","));
            options.matcher =
                Some(Selector::parse(&selector).map_err(|e| Error::source_err(e.to_string()))?);
        }
        Ok(options)
    }
}

pub struct TsdbSource {
    label_names: Vec<String>,
    schema: Schema,
    info: SourceInfo,
    reader: Option<BlockSource>,
    receiver: Option<tokio::sync::mpsc::Receiver<std::result::Result<SeriesRecord, String>>>,
    thread: Option<std::thread::JoinHandle<()>>,
    pending: VecDeque<Vec<Value>>,
    columns: Vec<String>,
    shape: TsdbShape,
    scan_finished: bool,
}

impl TsdbSource {
    /// Opens the block set. Remote (`s3://`, `gs://`, `az://`) sources are staged
    /// to a temporary directory by the reader, which can take a while for large
    /// prefixes: this call is synchronous by design and the CLI/TUI run it before
    /// the conversion loop.
    pub fn open(options: TsdbOptions) -> Result<Self> {
        let source_options = options.source_options()?;
        let reader = BlockSource::open(source_options)
            .map_err(|e| Error::source_err(format!("opening TSDB blocks: {e}")))?;

        let infos = reader.block_infos();
        let blocks = infos.len();
        let series: u64 = infos.iter().map(|i| i.series as u64).sum();
        let samples: u64 = infos.iter().map(|i| i.samples).sum();
        let bounds = reader.time_bounds_ms();
        let mut notes = vec![format!(
            "{blocks} block(s), {series} series, ~{samples} samples, shape {}",
            options.shape.as_str()
        )];
        if let Some((min, max)) = bounds {
            notes.push(format!(
                "block time range {} .. {}",
                atp_core::value::render_timestamp(min),
                atp_core::value::render_timestamp(max)
            ));
        }
        if !options.matchers.is_empty() {
            notes.push(format!("selector {{{}}}", options.matchers.join(",")));
        }
        if options.from_ms.is_some() || options.to_ms.is_some() {
            notes.push(format!(
                "sample window {} .. {}",
                options
                    .from_ms
                    .map(atp_core::value::render_timestamp)
                    .unwrap_or_else(|| "-inf".into()),
                options
                    .to_ms
                    .map(atp_core::value::render_timestamp)
                    .unwrap_or_else(|| "+inf".into())
            ));
        }

        let mut label_names: Vec<String> = reader
            .label_names()
            .into_keys()
            .filter(|name| name != "__name__")
            .collect();
        label_names.sort();
        label_names.dedup();

        let columns = match options.shape {
            TsdbShape::Series => vec![
                TIMESTAMP_COLUMN.to_string(),
                METRIC_COLUMN.to_string(),
                VALUE_COLUMN.to_string(),
                FINGERPRINT_COLUMN.to_string(),
            ],
            TsdbShape::Wide => {
                let mut cols = vec![
                    TIMESTAMP_COLUMN.to_string(),
                    METRIC_COLUMN.to_string(),
                    VALUE_COLUMN.to_string(),
                    FINGERPRINT_COLUMN.to_string(),
                ];
                cols.extend(label_names.iter().cloned());
                cols
            }
            TsdbShape::Long => vec![
                TIMESTAMP_COLUMN.to_string(),
                METRIC_COLUMN.to_string(),
                VALUE_COLUMN.to_string(),
                FINGERPRINT_COLUMN.to_string(),
                LABELS_COLUMN.to_string(),
            ],
        };
        let mut schema = Schema::default();
        for (index, name) in columns.iter().enumerate() {
            let ty = match index {
                0 => Ty::TimestampMillis,
                2 => Ty::F64,
                _ => Ty::Str,
            };
            schema.fields.push(Field::new(name.clone(), ty));
        }

        let info = SourceInfo {
            kind: "tsdb".to_string(),
            location: options.path.clone(),
            rows_estimate: Some(samples),
            bytes_estimate: None,
            notes,
        };

        Ok(TsdbSource {
            shape: options.shape,
            label_names,
            schema,
            info,
            reader: Some(reader),
            receiver: None,
            thread: None,
            pending: VecDeque::new(),
            columns,
            scan_finished: false,
        })
    }

    fn start_scan(&mut self) -> Result<()> {
        let reader = self
            .reader
            .take()
            .ok_or_else(|| Error::msg("internal: TSDB reader already consumed"))?;
        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<SeriesRecord, String>>(2);
        let handle = std::thread::Builder::new()
            .name("atp-tsdb-scan".to_string())
            .spawn(move || {
                let outcome = reader.scan(|record| {
                    tx.blocking_send(Ok(record)).map_err(|_| {
                        crate::error::Error::Source("conversion stopped reading".to_string())
                    })
                });
                if let Err(error) = outcome {
                    // The receiver may be gone; either way the scan ends here.
                    let _ = tx.blocking_send(Err(format!("scanning TSDB blocks: {error}")));
                }
            })
            .map_err(|e| Error::msg(format!("cannot start TSDB reader thread: {e}")))?;
        self.thread = Some(handle);
        self.receiver = Some(rx);
        Ok(())
    }

    fn buffer_series(&mut self, record: SeriesRecord) {
        let metric = record.metric_name().unwrap_or_default().to_string();
        let fingerprint = format!("{:016x}", fingerprint(&record.labels));
        let labels_json = if self.shape == TsdbShape::Long {
            serde_json::to_string(&record.labels).unwrap_or_else(|_| "{}".to_string())
        } else {
            String::new()
        };
        let mut template = vec![Value::Null; self.schema.fields.len()];
        template[1] = Value::Str(Arc::from(metric));
        template[3] = Value::Str(Arc::from(fingerprint));
        match self.shape {
            TsdbShape::Series => {}
            TsdbShape::Wide => {
                for (i, name) in self.label_names.iter().enumerate() {
                    if let Some(value) = record.labels.get(name) {
                        template[4 + i] = Value::Str(Arc::from(value.as_str()));
                    }
                }
            }
            TsdbShape::Long => template[4] = Value::Str(Arc::from(labels_json)),
        }
        for sample in record.samples {
            let mut row = template.clone();
            row[0] = Value::Timestamp(sample.timestamp_ms);
            row[2] = Value::F64(sample.value);
            self.pending.push_back(row);
        }
    }
}

#[async_trait]
impl Source for TsdbSource {
    fn describe(&self) -> SourceInfo {
        self.info.clone()
    }

    async fn infer(&mut self) -> Result<Schema> {
        Ok(self.schema.clone())
    }

    async fn next_page(&mut self, max_rows: usize) -> Result<Option<Page>> {
        if self.receiver.is_none() && !self.scan_finished {
            self.start_scan()?;
        }
        loop {
            if self.pending.len() >= max_rows {
                break;
            }
            let Some(receiver) = self.receiver.as_mut() else {
                break;
            };
            match receiver.recv().await {
                Some(Ok(record)) => self.buffer_series(record),
                Some(Err(message)) => return Err(Error::source_err(message)),
                None => {
                    self.receiver = None;
                    self.scan_finished = true;
                    break;
                }
            }
        }
        if self.pending.is_empty() {
            return Ok(None);
        }
        let mut rows = Vec::with_capacity(max_rows.min(self.pending.len()));
        while rows.len() < max_rows {
            match self.pending.pop_front() {
                Some(row) => rows.push(row),
                None => break,
            }
        }
        Ok(Some(Page {
            columns: self.columns.clone(),
            rows,
        }))
    }

    fn timestamp_column(&self) -> Option<String> {
        Some(TIMESTAMP_COLUMN.to_string())
    }

    async fn finish(&mut self) -> Result<()> {
        // Dropping the receiver makes the producer's blocking_send fail, which
        // stops the scan at the next series boundary.
        self.receiver = None;
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
        Ok(())
    }
}

impl Drop for TsdbSource {
    fn drop(&mut self) {
        self.receiver = None;
    }
}

/// Stable series identity over the label set, byte-identical to
/// `tsdb2parqtel`'s `series_fingerprint` so datasets from either tool join.
pub fn fingerprint(labels: &BTreeMap<String, String>) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for (key, value) in labels {
        hasher.update(key.as_bytes());
        hasher.update(&[0xff]);
        hasher.update(value.as_bytes());
        hasher.update(&[0xfe]);
    }
    hasher.digest()
}

/// Every label name in a block set, for schema previews without scanning.
pub fn label_names(source: &TsdbSource) -> &[String] {
    &source.label_names
}

/// Convenience for callers that only need a set of selector strings validated.
pub fn validate_matchers(matchers: &[String]) -> Result<()> {
    let mut options = TsdbOptions::new(".");
    options.matchers = matchers.to_vec();
    let _ = options.source_options()?;
    Ok(())
}

/// Block ULIDs present in a directory, for `--tsdb-block` completion and tests.
pub fn block_ulids(path: &str) -> Result<BTreeSet<String>> {
    let reader = BlockSource::open(SourceOptions::new(path))
        .map_err(|e| Error::source_err(format!("opening TSDB blocks: {e}")))?;
    Ok(reader
        .block_infos()
        .into_iter()
        .map(|info| info.meta.ulid)
        .collect())
}

/// Stats used by the TUI/progress bar before a run.
pub fn series_count(path: &str) -> Result<u64> {
    let reader = BlockSource::open(SourceOptions::new(path))
        .map_err(|e| Error::source_err(format!("opening TSDB blocks: {e}")))?;
    Ok(reader
        .block_infos()
        .iter()
        .map(|info| info.series as u64)
        .sum())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture(name: &str) -> String {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
            .display()
            .to_string()
    }

    /// The real block starts at 2022-04-15T05:23:42Z; a two-minute window keeps
    /// the test quick while still exercising every series.
    fn windowed(name: &str) -> TsdbOptions {
        let mut options = TsdbOptions::new(fixture(name));
        options.from_ms = Some(1_650_005_022_282);
        options.to_ms = Some(1_650_005_022_282 + 120_000);
        options
    }

    #[tokio::test]
    async fn wide_shape_emits_one_row_per_sample_with_labels() {
        let mut options = windowed("testblock");
        options.shape = TsdbShape::Wide;
        let mut source = TsdbSource::open(options).unwrap();
        let schema = source.infer().await.unwrap();
        let names = schema.names();
        assert_eq!(
            &names[..4],
            ["timestamp", "metric", "value", "series_fingerprint"]
        );
        assert!(names.contains(&"job".to_string()), "{names:?}");
        assert!(names.contains(&"namespace".to_string()), "{names:?}");

        let mut rows = 0usize;
        while let Some(page) = source.next_page(512).await.unwrap() {
            rows += page.rows.len();
            assert_eq!(page.columns, names);
            for row in &page.rows {
                assert!(matches!(row[0], Value::Timestamp(_)));
                assert!(matches!(&row[1], Value::Str(metric) if !metric.is_empty()));
                assert!(matches!(row[2], Value::F64(_)));
                assert!(matches!(&row[3], Value::Str(s) if s.len() == 16));
            }
        }
        assert!(rows > 10_000, "expected a window of rows, got {rows}");
        source.finish().await.unwrap();
    }

    #[tokio::test]
    async fn long_shape_carries_labels_as_json() {
        let mut options = windowed("testblock");
        options.shape = TsdbShape::Long;
        let mut source = TsdbSource::open(options).unwrap();
        source.infer().await.unwrap();
        let page = source.next_page(10).await.unwrap().unwrap();
        let json = match &page.rows[0][4] {
            Value::Str(s) => serde_json::from_str::<BTreeMap<String, String>>(s).unwrap(),
            other => panic!("expected JSON text, got {other:?}"),
        };
        assert!(json.contains_key("__name__"), "{json:?}");
    }

    #[tokio::test]
    async fn matcher_selects_series_by_metric_name() {
        let mut options = windowed("testblock");
        options.matchers = vec!["__name__=\"ALERTS\"".to_string()];
        let mut source = TsdbSource::open(options).unwrap();
        source.infer().await.unwrap();
        let page = source.next_page(5).await.unwrap().unwrap();
        assert_eq!(page.rows.len(), 5);
        assert!(matches!(&page.rows[0][1], Value::Str(metric) if metric.as_ref() == "ALERTS"));
    }

    #[tokio::test]
    async fn series_without_a_metric_name_are_skipped() {
        // index_format_v1 carries labels `bar`/`foo` but no `__name__`, exactly
        // the case tsdb2parqtel skips; the adapter must not invent a metric.
        let mut source = TsdbSource::open(TsdbOptions::new(fixture("index_format_v1"))).unwrap();
        source.infer().await.unwrap();
        assert!(source.next_page(10).await.unwrap().is_none());
        source.finish().await.unwrap();
    }

    #[test]
    fn fingerprint_is_stable_and_order_independent() {
        let mut a = BTreeMap::new();
        a.insert("__name__".to_string(), "up".to_string());
        a.insert("job".to_string(), "api".to_string());
        let mut b = BTreeMap::new();
        b.insert("job".to_string(), "api".to_string());
        b.insert("__name__".to_string(), "up".to_string());
        assert_eq!(fingerprint(&a), fingerprint(&b));
        assert_eq!(format!("{:016x}", fingerprint(&a)).len(), 16);
        b.insert("job".to_string(), "web".to_string());
        assert_ne!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn matchers_accept_bare_and_braced_forms() {
        validate_matchers(&["job=\"api\"".to_string(), "{env!=\"dev\"}".to_string()]).unwrap();
        assert!(validate_matchers(&["job=~\"(\"".to_string()]).is_err());
    }

    #[test]
    fn source_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<TsdbSource>();
    }

    #[test]
    fn shape_parsing() {
        assert_eq!(TsdbShape::parse("series"), Some(TsdbShape::Series));
        assert_eq!(TsdbShape::parse("wide"), Some(TsdbShape::Wide));
        assert_eq!(TsdbShape::parse("long"), Some(TsdbShape::Long));
        assert_eq!(TsdbShape::parse("columns"), None);
    }
}
