//! Run manifest: the contract between a conversion and everything downstream.
//!
//! One file per output directory (`_manifest.json`) that answers: what was
//! written, from where, with which schemas, how many rows, and the content hash
//! of every Parquet file. `atp verify` checks the output against it.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;

use crate::error::{Error, Result};
use crate::sink::{Compression, FileRecord, Partition};
use crate::source::SourceInfo;
use crate::value::Schema;

pub const MANIFEST_NAME: &str = "_manifest.json";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestOptions {
    pub compression: Compression,
    pub partition: Partition,
    pub row_group_rows: usize,
    pub strict: bool,
    pub timestamp_column: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub tool: String,
    pub run_id: String,
    /// `complete`, `cancelled` or `failed`. Only `complete` means every source
    /// row was written.
    pub status: String,
    pub error: Option<String>,
    pub started_at: String,
    pub finished_at: String,
    pub elapsed_ms: u128,
    pub source: SourceInfo,
    pub options: ManifestOptions,
    pub schemas: Vec<Schema>,
    pub files: Vec<FileRecord>,
    pub total_rows: u64,
    pub total_bytes: u64,
}

impl Manifest {
    pub fn path_in(root: &Path) -> PathBuf {
        root.join(MANIFEST_NAME)
    }

    pub fn write(&self, root: &Path) -> Result<PathBuf> {
        let path = Manifest::path_in(root);
        let tmp = root.join(format!("{MANIFEST_NAME}.tmp"));
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, &path)?;
        Ok(path)
    }

    pub fn read(path: &Path) -> Result<Manifest> {
        let bytes = std::fs::read(path).map_err(|e| Error::Path {
            action: "read",
            path: path.to_path_buf(),
            reason: e.to_string(),
        })?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn schema_hashes(&self) -> Vec<String> {
        self.schemas.iter().map(|s| s.hash()).collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifyProblem {
    pub path: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifyReport {
    pub ok: bool,
    pub status: String,
    pub files_checked: usize,
    pub rows_manifest: u64,
    pub rows_on_disk: u64,
    pub bytes_manifest: u64,
    pub problems: Vec<VerifyProblem>,
}

impl VerifyReport {
    pub fn summary(&self) -> String {
        if self.ok {
            format!(
                "verified {} file(s), {} rows, {} bytes",
                self.files_checked, self.rows_on_disk, self.bytes_manifest
            )
        } else {
            format!(
                "{} problem(s): {}",
                self.problems.len(),
                self.problems
                    .iter()
                    .map(|p| format!("{}: {}", p.path, p.detail))
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        }
    }
}

/// Re-read every file named in the manifest, recompute its digest and row count
/// from the Parquet footer, and compare with what was recorded at write time.
pub fn verify(manifest_path: &Path) -> Result<VerifyReport> {
    let manifest = Manifest::read(manifest_path)?;
    let root = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let mut report = VerifyReport {
        ok: true,
        status: manifest.status.clone(),
        files_checked: 0,
        rows_manifest: manifest.total_rows,
        rows_on_disk: 0,
        bytes_manifest: manifest.total_bytes,
        problems: Vec::new(),
    };
    if manifest.status != "complete" {
        report.ok = false;
        report.problems.push(VerifyProblem {
            path: MANIFEST_NAME.to_string(),
            detail: format!("manifest status is `{}`", manifest.status),
        });
    }
    let mut total_bytes = 0u64;
    for record in &manifest.files {
        let path = root.join(&record.path);
        report.files_checked += 1;
        match hash_file(&path) {
            Ok((digest, bytes)) => {
                total_bytes += bytes;
                if digest != record.digest {
                    report.ok = false;
                    report.problems.push(VerifyProblem {
                        path: record.path.clone(),
                        detail: format!("digest {digest} != manifest {}", record.digest),
                    });
                }
                if bytes != record.bytes {
                    report.ok = false;
                    report.problems.push(VerifyProblem {
                        path: record.path.clone(),
                        detail: format!("bytes {bytes} != manifest {}", record.bytes),
                    });
                }
            }
            Err(e) => {
                report.ok = false;
                report.problems.push(VerifyProblem {
                    path: record.path.clone(),
                    detail: e.to_string(),
                });
                continue;
            }
        }
        match parquet_rows(&path) {
            Ok(rows) => {
                report.rows_on_disk += rows;
                if rows != record.rows {
                    report.ok = false;
                    report.problems.push(VerifyProblem {
                        path: record.path.clone(),
                        detail: format!("rows {rows} != manifest {}", record.rows),
                    });
                }
            }
            Err(e) => {
                report.ok = false;
                report.problems.push(VerifyProblem {
                    path: record.path.clone(),
                    detail: format!("not a readable Parquet file: {e}"),
                });
            }
        }
    }
    report.bytes_manifest = total_bytes;
    if report.rows_on_disk != report.rows_manifest {
        report.ok = false;
        report.problems.push(VerifyProblem {
            path: MANIFEST_NAME.to_string(),
            detail: format!(
                "rows on disk {} != manifest total {}",
                report.rows_on_disk, report.rows_manifest
            ),
        });
    }
    Ok(report)
}

fn hash_file(path: &Path) -> Result<(String, u64)> {
    let mut f = File::open(path).map_err(|e| Error::Path {
        action: "open",
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;
    let mut hasher = Xxh3::new();
    let mut buf = vec![0u8; 1024 * 1024];
    let mut bytes = 0u64;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        bytes += n as u64;
    }
    Ok((format!("{:016x}", hasher.digest()), bytes))
}

fn parquet_rows(path: &Path) -> Result<u64> {
    let file = File::open(path).map_err(|e| Error::Path {
        action: "open",
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;
    let reader = SerializedFileReader::new(file)?;
    Ok(reader.metadata().file_metadata().num_rows() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{Field, Ty};

    #[test]
    fn manifest_round_trips_and_reports_schema_hashes() {
        let schema = Schema::new(vec![Field::new("a", Ty::I64)]);
        let dir = tempfile::tempdir().expect("tmp");
        let m = Manifest {
            tool: "atp test".into(),
            run_id: "r1".into(),
            status: "complete".into(),
            error: None,
            started_at: "t0".into(),
            finished_at: "t1".into(),
            elapsed_ms: 5,
            source: SourceInfo {
                kind: "csv".into(),
                location: "x.csv".into(),
                rows_estimate: Some(1),
                bytes_estimate: Some(3),
                notes: vec![],
            },
            options: ManifestOptions {
                compression: Compression::Zstd,
                partition: Partition::Day,
                row_group_rows: 10,
                strict: false,
                timestamp_column: None,
            },
            schemas: vec![schema.clone()],
            files: vec![],
            total_rows: 0,
            total_bytes: 0,
        };
        let path = m.write(dir.path()).expect("write");
        let back = Manifest::read(&path).expect("read");
        assert_eq!(back.schema_hashes(), vec![schema.hash()]);
        assert_eq!(back.options.partition, Partition::Day);
    }

    #[test]
    fn verify_flags_missing_files() {
        let dir = tempfile::tempdir().expect("tmp");
        let m = Manifest {
            tool: "atp test".into(),
            run_id: "r2".into(),
            status: "complete".into(),
            error: None,
            started_at: "t0".into(),
            finished_at: "t1".into(),
            elapsed_ms: 1,
            source: SourceInfo {
                kind: "csv".into(),
                location: "x.csv".into(),
                rows_estimate: None,
                bytes_estimate: None,
                notes: vec![],
            },
            options: ManifestOptions {
                compression: Compression::Zstd,
                partition: Partition::None,
                row_group_rows: 10,
                strict: false,
                timestamp_column: None,
            },
            schemas: vec![],
            files: vec![FileRecord {
                path: "part-00-00000.parquet".into(),
                rows: 1,
                bytes: 10,
                digest: "0000000000000000".into(),
                schema_hash: "x".into(),
            }],
            total_rows: 1,
            total_bytes: 10,
        };
        let path = m.write(dir.path()).expect("write");
        let report = verify(&path).expect("verify");
        assert!(!report.ok);
        assert!(report.problems.iter().any(|p| p.detail.contains("open")));
    }
}
