//! Source abstraction: everything that can emit rows with a schema.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::value::{Schema, Value};

pub mod csv_source;
pub mod es;
pub mod jsonl;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Csv,
    Jsonl,
    Es,
    Tsdb,
}

impl SourceKind {
    pub fn parse(s: &str) -> Option<SourceKind> {
        Some(match s {
            "csv" | "tsv" => SourceKind::Csv,
            "jsonl" | "ndjson" | "json" => SourceKind::Jsonl,
            "es" | "elasticsearch" | "opensearch" => SourceKind::Es,
            "tsdb" | "prometheus" | "thanos" => SourceKind::Tsdb,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SourceKind::Csv => "csv",
            SourceKind::Jsonl => "jsonl",
            SourceKind::Es => "elasticsearch",
            SourceKind::Tsdb => "tsdb",
        }
    }
}

/// Detect the source kind from the location:
/// - `http(s)://` is Elasticsearch;
/// - `s3://`, `gs://`, `az://` are Prometheus block prefixes;
/// - a directory holding `meta.json` (or blocks one level down) is TSDB;
/// - otherwise the file extension decides.
pub fn detect_kind(location: &str) -> Option<SourceKind> {
    if location.starts_with("http://") || location.starts_with("https://") {
        return Some(SourceKind::Es);
    }
    if ["s3://", "gs://", "az://"]
        .iter()
        .any(|scheme| location.starts_with(scheme))
    {
        return Some(SourceKind::Tsdb);
    }
    let path = std::path::Path::new(location);
    if path.is_dir() {
        if path.join("meta.json").is_file() {
            return Some(SourceKind::Tsdb);
        }
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let child = entry.path();
                if child.is_dir() && child.join("meta.json").is_file() {
                    return Some(SourceKind::Tsdb);
                }
            }
        }
        return None;
    }
    let lower = location.to_ascii_lowercase();
    let base = lower.strip_suffix(".gz").unwrap_or(&lower);
    let ext = base.rsplit('.').next().unwrap_or("");
    match ext {
        "csv" | "tsv" => Some(SourceKind::Csv),
        "jsonl" | "ndjson" => Some(SourceKind::Jsonl),
        "json" => Some(SourceKind::Jsonl),
        _ => None,
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EsConfig {
    pub index: String,
    /// Raw Query DSL body; `None` means `{"match_all": {}}`.
    pub query: Option<String>,
    pub api_key: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub scroll: String,
    pub page_size: usize,
    /// Total slices to walk sequentially; each gets its own scroll context.
    pub slices: usize,
    pub insecure: bool,
}

impl EsConfig {
    pub fn new(index: impl Into<String>) -> Self {
        EsConfig {
            index: index.into(),
            query: None,
            api_key: None,
            user: None,
            password: None,
            scroll: "5m".to_string(),
            page_size: 1000,
            slices: 1,
            insecure: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SourceConfig {
    pub kind: SourceKind,
    pub location: String,
    pub timestamp_column: Option<String>,
    pub csv_delimiter: Option<u8>,
    pub es: EsConfig,
    /// Rows used for CSV/JSONL type inference.
    pub sample_rows: usize,
}

impl SourceConfig {
    pub fn new(kind: SourceKind, location: impl Into<String>) -> Self {
        SourceConfig {
            kind,
            location: location.into(),
            timestamp_column: None,
            csv_delimiter: None,
            es: EsConfig::default(),
            sample_rows: 10_000,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceInfo {
    pub kind: String,
    pub location: String,
    pub rows_estimate: Option<u64>,
    pub bytes_estimate: Option<u64>,
    pub notes: Vec<String>,
}

/// One page of rows, aligned with `columns`. `columns` is resent per page so the
/// engine never assumes a stable shape between pages: a JSONL file can introduce
/// a key on line 900_000 and the engine will add a column and roll the schema
/// segment.
#[derive(Debug)]
pub struct Page {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[async_trait]
pub trait Source: Send {
    fn describe(&self) -> SourceInfo;
    async fn infer(&mut self) -> Result<Schema>;
    async fn next_page(&mut self, max_rows: usize) -> Result<Option<Page>>;
    fn timestamp_column(&self) -> Option<String>;
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn open(config: &SourceConfig) -> Result<Box<dyn Source>> {
    match config.kind {
        SourceKind::Csv => Ok(Box::new(csv_source::CsvSource::open(config)?)),
        SourceKind::Jsonl => Ok(Box::new(jsonl::JsonlSource::open(config)?)),
        SourceKind::Es => Ok(Box::new(es::EsSource::new(config)?)),
        SourceKind::Tsdb => Err(Error::msg(
            "TSDB blocks are opened by the atp-tsdb crate; use the atp binary",
        )),
    }
}

pub(crate) fn read_error(path: &std::path::Path, err: std::io::Error) -> Error {
    Error::Path {
        action: "read",
        path: path.to_path_buf(),
        reason: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_detection_prefers_scheme_then_extension() {
        assert_eq!(detect_kind("https://es.example:9200"), Some(SourceKind::Es));
        assert_eq!(detect_kind("/data/dump.CSV"), Some(SourceKind::Csv));
        assert_eq!(detect_kind("events.jsonl.gz"), Some(SourceKind::Jsonl));
        assert_eq!(detect_kind("events.CSV.GZ"), Some(SourceKind::Csv));
        assert_eq!(detect_kind("events.ndjson"), Some(SourceKind::Jsonl));
        assert_eq!(detect_kind("/data/unknown"), None);
    }

    #[test]
    fn kind_round_trips_through_text() {
        for k in [
            SourceKind::Csv,
            SourceKind::Jsonl,
            SourceKind::Es,
            SourceKind::Tsdb,
        ] {
            assert_eq!(SourceKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(SourceKind::parse("parquet"), None);
    }
}
