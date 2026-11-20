//! Newline-delimited JSON source.
//!
//! Supports `.jsonl`, `.ndjson`, `.json` (one document per line) and their
//! gzip variants. Column order is first-seen order; a key that appears after the
//! sample is appended to the schema by the engine and previous rows become null.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::Value as Json;

use crate::error::{Error, Result};
use crate::flatten::flatten_with_timestamp;
use crate::value::{merge_ty, Field, Schema, Ty, Value};

use super::{read_error, Page, Source, SourceConfig, SourceInfo};

pub struct JsonlSource {
    path: PathBuf,
    timestamp: Option<String>,
    sample_rows: usize,
    columns: Vec<String>,
    reader: Option<Box<dyn BufRead + Send>>,
    pending: VecDeque<Vec<Value>>,
    line_no: u64,
    eof: bool,
}

impl JsonlSource {
    pub fn open(config: &SourceConfig) -> Result<Self> {
        let path = PathBuf::from(&config.location);
        // Fail early with a path error instead of a confusing empty schema.
        File::open(&path).map_err(|e| read_error(&path, e))?;
        Ok(JsonlSource {
            path,
            timestamp: config.timestamp_column.clone(),
            sample_rows: config.sample_rows.max(1),
            columns: Vec::new(),
            reader: None,
            pending: VecDeque::new(),
            line_no: 0,
            eof: false,
        })
    }

    fn ensure_reader(&mut self) -> Result<()> {
        if self.reader.is_some() {
            return Ok(());
        }
        let file = File::open(&self.path).map_err(|e| read_error(&self.path, e))?;
        let gz = self
            .path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("gz"));
        let boxed: Box<dyn BufRead + Send> = if gz {
            Box::new(BufReader::with_capacity(
                256 * 1024,
                flate2::read::MultiGzDecoder::new(BufReader::new(file)),
            ))
        } else {
            Box::new(BufReader::with_capacity(256 * 1024, file))
        };
        self.reader = Some(boxed);
        Ok(())
    }

    fn read_line(&mut self, line: &mut String) -> Result<usize> {
        self.ensure_reader()?;
        let Some(reader) = self.reader.as_mut() else {
            return Err(Error::msg("internal: reader not initialised"));
        };
        reader
            .read_line(line)
            .map_err(|e| read_error(&self.path, e))
    }

    /// Read one line and flatten it into the current column set. New keys are
    /// registered and null-padded into buffered rows so the page stays
    /// rectangular.
    fn read_record(&mut self) -> Result<Option<Vec<Value>>> {
        loop {
            let mut line = String::new();
            let n = self.read_line(&mut line)?;
            if n == 0 {
                return Ok(None);
            }
            self.line_no += 1;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let doc: Json = serde_json::from_str(trimmed).map_err(|e| {
                Error::source_err(format!("{}:{}: {e}", self.path.display(), self.line_no))
            })?;
            let (pairs, bad_ts) = flatten_with_timestamp(&doc, self.timestamp.as_deref());
            if let Some(text) = bad_ts {
                return Err(Error::BadTimestamp {
                    column: self.timestamp.clone().unwrap_or_default(),
                    record: self.line_no,
                    value: text,
                });
            }
            let mut row = Vec::with_capacity(self.columns.len());
            for _ in &self.columns {
                row.push(Value::Null);
            }
            for (name, value) in pairs {
                let idx = match self.columns.iter().position(|c| c == &name) {
                    Some(i) => i,
                    None => {
                        self.columns.push(name);
                        for buffered in self.pending.iter_mut() {
                            buffered.push(Value::Null);
                        }
                        self.columns.len() - 1
                    }
                };
                if idx >= row.len() {
                    row.resize(idx + 1, Value::Null);
                }
                row[idx] = value;
            }
            return Ok(Some(row));
        }
    }
}

#[async_trait]
impl Source for JsonlSource {
    fn describe(&self) -> SourceInfo {
        SourceInfo {
            kind: "jsonl".to_string(),
            location: self.path.display().to_string(),
            rows_estimate: None,
            bytes_estimate: std::fs::metadata(&self.path).ok().map(|m| m.len()),
            notes: vec![format!(
                "sampled {} rows for types; nested objects flattened with '.'",
                self.sample_rows
            )],
        }
    }

    async fn infer(&mut self) -> Result<Schema> {
        let mut types: Vec<Ty> = Vec::new();
        while self.pending.len() < self.sample_rows {
            match self.read_record()? {
                Some(row) => {
                    if types.len() < row.len() {
                        types.resize(row.len(), Ty::Null);
                    }
                    for (i, v) in row.iter().enumerate() {
                        types[i] = merge_ty(types[i], v.ty());
                    }
                    self.pending.push_back(row);
                }
                None => {
                    self.eof = true;
                    break;
                }
            }
        }
        if self.columns.is_empty() {
            return Err(Error::source_err(format!(
                "{} has no JSON documents",
                self.path.display()
            )));
        }
        let mut fields = Vec::with_capacity(self.columns.len());
        for (name, mut ty) in self.columns.iter().zip(types) {
            if self.timestamp.as_deref() == Some(name.as_str()) {
                ty = Ty::TimestampMillis;
            }
            if ty == Ty::Null {
                ty = Ty::Str;
            }
            fields.push(Field::new(name.clone(), ty));
        }
        Ok(Schema::new(fields))
    }

    async fn next_page(&mut self, max_rows: usize) -> Result<Option<Page>> {
        let mut rows = Vec::with_capacity(max_rows.min(8192));
        while rows.len() < max_rows {
            if let Some(row) = self.pending.pop_front() {
                // Buffered rows may predate columns discovered since; pad them.
                let mut row = row;
                if row.len() < self.columns.len() {
                    row.resize(self.columns.len(), Value::Null);
                }
                rows.push(row);
                continue;
            }
            if self.eof {
                break;
            }
            match self.read_record()? {
                Some(row) => {
                    if row.len() > self.columns.len() {
                        self.columns.resize(row.len(), String::new());
                    }
                    rows.push(row);
                }
                None => {
                    self.eof = true;
                    break;
                }
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        // Every row is padded to the full column set, including rows collected
        // before a late key was discovered in this same page.
        for row in rows.iter_mut() {
            row.resize(self.columns.len(), Value::Null);
        }
        Ok(Some(Page {
            columns: self.columns.clone(),
            rows,
        }))
    }

    fn timestamp_column(&self) -> Option<String> {
        self.timestamp.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SourceKind;
    use std::io::Write;

    fn write_tmp(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("atp-jsonl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("tmp dir");
        let path = dir.join(name);
        let mut f = File::create(&path).expect("create");
        f.write_all(body.as_bytes()).expect("write");
        path
    }

    #[tokio::test]
    async fn flattens_nested_docs_and_unions_late_keys() {
        let path = write_tmp(
            "ev.jsonl",
            concat!(
                r#"{"@timestamp":"2026-11-21T10:00:00Z","http":{"method":"GET"},"n":1}"#,
                "\n\n",
                r#"{"@timestamp":"2026-11-21T10:00:01Z","http":{"method":"POST","status":200},"n":2.5,"late":true}"#,
                "\n"
            ),
        );
        let mut cfg = SourceConfig::new(SourceKind::Jsonl, path.display().to_string());
        cfg.timestamp_column = Some("@timestamp".to_string());
        let mut src = JsonlSource::open(&cfg).expect("open");
        let schema = src.infer().await.expect("infer");
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"http.method"));
        assert!(names.contains(&"http.status"));
        assert!(names.contains(&"late"));
        let ts = schema
            .fields
            .iter()
            .find(|f| f.name == "@timestamp")
            .expect("ts");
        assert_eq!(ts.ty, Ty::TimestampMillis);
        let n = schema.fields.iter().find(|f| f.name == "n").expect("n");
        assert_eq!(n.ty, Ty::F64);
    }

    #[tokio::test]
    async fn late_keys_pad_buffered_rows() {
        let path = write_tmp(
            "late.jsonl",
            "{\"a\":1}\n{\"a\":2,\"b\":\"x\"}\n{\"a\":3}\n",
        );
        let mut cfg = SourceConfig::new(SourceKind::Jsonl, path.display().to_string());
        cfg.sample_rows = 1;
        let mut src = JsonlSource::open(&cfg).expect("open");
        src.infer().await.expect("infer");
        let page = src.next_page(10).await.expect("page").expect("rows");
        assert_eq!(page.columns, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(page.rows.len(), 3);
        assert!(page.rows.iter().all(|r| r.len() == 2));
        assert_eq!(page.rows[2][1], Value::Null);
    }

    #[tokio::test]
    async fn invalid_json_reports_the_line() {
        let path = write_tmp("bad.jsonl", "{\"a\":1}\n{oops}\n");
        let mut cfg = SourceConfig::new(SourceKind::Jsonl, path.display().to_string());
        cfg.sample_rows = 1;
        let mut src = JsonlSource::open(&cfg).expect("open");
        src.infer().await.expect("infer");
        let err = src.next_page(10).await.expect_err("bad json");
        assert!(err.to_string().contains(":2:"), "{err}");
    }
}
