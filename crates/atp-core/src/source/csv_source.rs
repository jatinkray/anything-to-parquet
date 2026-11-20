//! CSV/TSV source: delimiter sniffing, header detection, sampled inference.
//!
//! Sampling is bounded (`--sample-rows`, default 10k). Values that appear after
//! the sample are handled by the engine's promotion path, so a 40 GB file whose
//! column turns numeric on line 39 million still converts without data loss.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::value::{merge_ty, parse_timestamp, Field, Schema, Ty, Value};

use super::{read_error, Page, Source, SourceConfig, SourceInfo};

const DELIMITERS: [u8; 4] = *b",;\t|";

pub struct CsvSource {
    path: PathBuf,
    timestamp: Option<String>,
    sample_rows: usize,
    delimiter: u8,
    header: Vec<String>,
    reader: csv::Reader<Box<dyn Read + Send>>,
    pending: VecDeque<Vec<Value>>,
    read_rows: u64,
    eof: bool,
}

impl CsvSource {
    pub fn open(config: &SourceConfig) -> Result<Self> {
        let path = PathBuf::from(&config.location);
        let delimiter = match config.csv_delimiter {
            Some(d) => d,
            None => sniff_delimiter(&path)?,
        };
        let reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .flexible(true)
            .from_reader(open_maybe_gzip(&path)?);
        let mut source = CsvSource {
            path,
            timestamp: config.timestamp_column.clone(),
            sample_rows: config.sample_rows.max(1),
            delimiter,
            header: Vec::new(),
            reader,
            pending: VecDeque::new(),
            read_rows: 0,
            eof: false,
        };
        source.header = read_header(&mut source.reader, &source.path)?;
        Ok(source)
    }

    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    fn parse_record(&mut self, record: &csv::StringRecord) -> Result<Vec<Value>> {
        let mut row = Vec::with_capacity(self.header.len());
        for (i, name) in self.header.iter().enumerate() {
            let cell = record.get(i).unwrap_or("").trim();
            row.push(parse_cell(cell, name, self.timestamp.as_deref())?);
        }
        Ok(row)
    }
}

fn read_header(reader: &mut csv::Reader<Box<dyn Read + Send>>, path: &Path) -> Result<Vec<String>> {
    let headers = reader.headers().map_err(Error::Csv)?.clone();
    if headers.is_empty() {
        return Err(Error::source_err(format!(
            "{} has no header row; CSV input needs a header",
            path.display()
        )));
    }
    Ok(headers
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                format!("column_{}", i + 1)
            } else {
                trimmed.to_string()
            }
        })
        .collect())
}

fn open_maybe_gzip(path: &Path) -> Result<Box<dyn Read + Send>> {
    let file = File::open(path).map_err(|e| read_error(path, e))?;
    if path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("gz"))
    {
        Ok(Box::new(flate2::read::MultiGzDecoder::new(BufReader::new(
            file,
        ))))
    } else {
        Ok(Box::new(BufReader::with_capacity(256 * 1024, file)))
    }
}

/// Pick the delimiter with the most occurrences in the first non-empty line,
/// ignoring quoted runs. Candidates: `, ; \t |`.
fn sniff_delimiter(path: &Path) -> Result<u8> {
    let mut f = File::open(path).map_err(|e| read_error(path, e))?;
    let mut buf = vec![0u8; 64 * 1024];
    let n = f.read(&mut buf).map_err(|e| read_error(path, e))?;
    buf.truncate(n);
    if path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("gz"))
    {
        return Ok(b',');
    }
    let line = buf.split(|b| *b == b'\n').next().unwrap_or(&[]);
    let mut best = (0usize, b',');
    for d in DELIMITERS {
        let mut count = 0usize;
        let mut in_quotes = false;
        for b in line {
            match b {
                b'"' => in_quotes = !in_quotes,
                c if *c == d && !in_quotes => count += 1,
                _ => {}
            }
        }
        if count > best.0 {
            best = (count, d);
        }
    }
    Ok(best.1)
}

fn parse_cell(cell: &str, column: &str, timestamp: Option<&str>) -> Result<Value> {
    if cell.is_empty() {
        return Ok(Value::Null);
    }
    if timestamp.is_some_and(|t| t == column) {
        return parse_timestamp(cell)
            .map(Value::Timestamp)
            .ok_or_else(|| Error::BadTimestamp {
                column: column.to_string(),
                record: 0,
                value: cell.to_string(),
            });
    }
    if cell.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if cell.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    // Leading zeros mean an identifier (zip codes, account numbers), not a
    // number: keeping them as text avoids a silent 0123 -> 123 rewrite.
    let numeric_like =
        !(cell.len() > 1 && cell.starts_with('0') && cell.bytes().all(|b| b.is_ascii_digit()));
    if numeric_like {
        if let Ok(i) = cell.parse::<i64>() {
            return Ok(Value::I64(i));
        }
        if let Ok(f) = cell.parse::<f64>() {
            if f.is_finite() {
                return Ok(Value::F64(f));
            }
        }
    }
    Ok(Value::Str(cell.to_string()))
}

fn value_ty(v: &Value) -> Ty {
    v.ty()
}

#[async_trait]
impl Source for CsvSource {
    fn describe(&self) -> SourceInfo {
        let bytes = std::fs::metadata(&self.path).ok().map(|m| m.len());
        SourceInfo {
            kind: "csv".to_string(),
            location: self.path.display().to_string(),
            rows_estimate: None,
            bytes_estimate: bytes,
            notes: vec![format!(
                "delimiter {:?}, {} columns, sampled {} rows for types",
                self.delimiter as char,
                self.header.len(),
                self.sample_rows
            )],
        }
    }

    async fn infer(&mut self) -> Result<Schema> {
        let mut types = vec![Ty::Null; self.header.len()];
        let mut record = csv::StringRecord::new();
        let mut sampled = 0usize;
        while sampled < self.sample_rows {
            let more = self.reader.read_record(&mut record).map_err(Error::Csv)?;
            if !more {
                self.eof = true;
                break;
            }
            if record.len() == 1 && record.get(0).is_some_and(|s| s.trim().is_empty()) {
                continue;
            }
            self.read_rows += 1;
            let row = self.parse_record(&record)?;
            for (i, v) in row.iter().enumerate() {
                types[i] = merge_ty(types[i], value_ty(v));
            }
            self.pending.push_back(row);
            sampled += 1;
        }
        let mut fields = Vec::with_capacity(self.header.len());
        for (name, mut ty) in self.header.iter().zip(types) {
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
                rows.push(row);
                continue;
            }
            if self.eof {
                break;
            }
            let mut record = csv::StringRecord::new();
            let more = self.reader.read_record(&mut record).map_err(Error::Csv)?;
            if !more {
                self.eof = true;
                break;
            }
            if record.len() == 1 && record.get(0).is_some_and(|s| s.trim().is_empty()) {
                continue;
            }
            self.read_rows += 1;
            rows.push(self.parse_record(&record)?);
        }
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(Page {
            columns: self.header.clone(),
            rows,
        }))
    }

    fn timestamp_column(&self) -> Option<String> {
        self.timestamp.clone()
    }
}

/// Header detection used by `atp inspect` and the TUI source screen.
pub fn looks_like_csv(path: &Path) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some(e) => matches!(e.to_ascii_lowercase().as_str(), "csv" | "tsv"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("atp-csv-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("tmp dir");
        let path = dir.join(name);
        let mut f = File::create(&path).expect("create");
        f.write_all(body.as_bytes()).expect("write");
        path
    }

    #[tokio::test]
    async fn infers_types_and_keeps_leading_zero_ids_as_text() {
        let path = write_tmp(
            "ids.csv",
            "id,count,ratio,ok,when\n0123,4,0.5,true,2026-11-21T10:00:00Z\n0456,7,1.5,false,\n",
        );
        let mut cfg = SourceConfig::new(super::super::SourceKind::Csv, path.display().to_string());
        cfg.timestamp_column = Some("when".to_string());
        let mut src = CsvSource::open(&cfg).expect("open");
        let schema = src.infer().await.expect("infer");
        assert_eq!(schema.fields[0].ty, Ty::Str);
        assert_eq!(schema.fields[1].ty, Ty::I64);
        assert_eq!(schema.fields[2].ty, Ty::F64);
        assert_eq!(schema.fields[3].ty, Ty::Bool);
        assert_eq!(schema.fields[4].ty, Ty::TimestampMillis);
        let page = src.next_page(10).await.expect("page").expect("some");
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0][0], Value::Str("0123".into()));
    }

    #[test]
    fn sniffs_semicolon_and_tab() {
        let p = write_tmp("semi.csv", "a;b;c\n1;2;3\n");
        assert_eq!(sniff_delimiter(&p).expect("sniff"), b';');
        let p = write_tmp("tab.tsv", "a\tb\tc\n1\t2\t3\n");
        assert_eq!(sniff_delimiter(&p).expect("sniff"), b'\t');
    }

    #[test]
    fn bad_timestamp_is_a_typed_error() {
        let err = parse_cell("yesterday", "@timestamp", Some("@timestamp")).expect_err("bad ts");
        assert!(matches!(err, Error::BadTimestamp { .. }));
        assert_eq!(err.exit_code(), 3);
    }
}
