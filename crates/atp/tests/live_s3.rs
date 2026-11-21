#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Live S3 scenarios: the real binary against a real bucket, then the objects
//! are read back independently (manifest, bytes, xxh3 digests, Parquet footers).
//!
//! Disabled unless both variables are set:
//!
//! ```text
//! ATP_LIVE_S3=1
//! ATP_LIVE_S3_BUCKET=<bucket>          # region/credentials from AWS_* env
//! ATP_LIVE_S3_PREFIX=atp-live          # optional prefix, default `atp-live`
//! ```
//!
//! `make live-s3` sources `aws.env` and runs these serially. Each scenario uses
//! a unique prefix; objects are deleted after the assertions pass.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use atp_core::engine::{self, RunOptions};
use atp_core::manifest::Manifest;
use atp_core::sink::Partition;
use atp_core::source::{Page, Source, SourceConfig, SourceInfo, SourceKind};
use atp_core::{Result as AtpResult, Schema};
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::file::reader::{FileReader, SerializedFileReader};
use xxhash_rust::xxh3::xxh3_64;

struct LiveEnv {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    root: String,
}

impl LiveEnv {
    fn from_env(scenario: &str) -> Option<LiveEnv> {
        if std::env::var("ATP_LIVE_S3").ok().as_deref() != Some("1") {
            eprintln!("skip: set ATP_LIVE_S3=1 to run live S3 scenarios");
            return None;
        }
        let Ok(bucket) = std::env::var("ATP_LIVE_S3_BUCKET") else {
            eprintln!("skip: ATP_LIVE_S3_BUCKET is not set");
            return None;
        };
        if bucket.trim().is_empty() {
            eprintln!("skip: ATP_LIVE_S3_BUCKET is empty");
            return None;
        }
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .expect("S3 client");
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        let base = std::env::var("ATP_LIVE_S3_PREFIX").unwrap_or_else(|_| "atp-live".to_string());
        let root = format!(
            "{}/{stamp}-{}-{scenario}",
            base.trim_matches('/'),
            std::process::id()
        );
        Some(LiveEnv {
            store: Arc::new(store),
            bucket,
            root,
        })
    }

    fn url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.root)
    }
}

struct Stored {
    manifest: Manifest,
    /// Key relative to the run prefix -> object bytes.
    objects: BTreeMap<String, Vec<u8>>,
}

async fn load(env: &LiveEnv) -> Stored {
    let prefix = ObjectPath::from(env.root.as_str());
    let listed: Vec<_> = env
        .store
        .list(Some(&prefix))
        .try_collect()
        .await
        .expect("list run prefix");
    let mut objects = BTreeMap::new();
    for meta in listed {
        let relative = meta
            .location
            .as_ref()
            .strip_prefix(&format!("{}/", env.root))
            .expect("object inside run prefix")
            .to_string();
        let bytes = env
            .store
            .get(&meta.location)
            .await
            .expect("get object")
            .bytes()
            .await
            .expect("object body");
        objects.insert(relative, bytes.to_vec());
    }
    let manifest_bytes = objects.get("_manifest.json").expect("manifest object");
    let manifest = serde_json::from_slice(manifest_bytes).expect("manifest json");
    Stored { manifest, objects }
}

fn assert_objects_match_manifest(stored: &Stored, expected_status: &str) -> u64 {
    let manifest = &stored.manifest;
    assert_eq!(manifest.status, expected_status);

    let listed: BTreeSet<&str> = stored.objects.keys().map(String::as_str).collect();
    let mut expected: BTreeSet<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
    expected.insert("_manifest.json");
    assert_eq!(listed, expected, "objects differ from manifest file list");
    assert!(
        !listed.iter().any(|key| key.ends_with(".tmp")),
        "a staging file leaked into the bucket: {listed:?}"
    );

    let mut rows = 0u64;
    for record in &manifest.files {
        let bytes = stored
            .objects
            .get(&record.path)
            .unwrap_or_else(|| panic!("missing object {}", record.path));
        assert_eq!(
            bytes.len() as u64,
            record.bytes,
            "byte length for {}",
            record.path
        );
        assert_eq!(
            format!("{:016x}", xxh3_64(bytes)),
            record.digest,
            "digest for {}",
            record.path
        );
        let file_rows = parquet_rows(bytes);
        assert_eq!(file_rows, record.rows, "row count for {}", record.path);
        rows += file_rows;
    }
    assert_eq!(rows, manifest.total_rows, "manifest total rows");
    rows
}

fn parquet_rows(bytes: &[u8]) -> u64 {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("object.parquet");
    std::fs::write(&path, bytes).expect("write object");
    let reader = SerializedFileReader::new(std::fs::File::open(&path).expect("open"))
        .expect("parquet reader");
    reader.metadata().file_metadata().num_rows() as u64
}

async fn cleanup(env: &LiveEnv) {
    let prefix = ObjectPath::from(env.root.as_str());
    if let Ok(objects) = env.store.list(Some(&prefix)).try_collect::<Vec<_>>().await {
        for meta in objects {
            let _ = env.store.delete(&meta.location).await;
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn example(name: &str) -> String {
    workspace()
        .join("examples")
        .join(name)
        .display()
        .to_string()
}

fn atp_convert(args: &[&str]) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_atp"))
        .args(args)
        .output()
        .expect("run atp");
    assert!(
        output.status.success(),
        "atp {:?} failed\nstderr: {}\nstdout: {}",
        args,
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).expect("json summary on stdout")
}

#[test]
fn csv_unpartitioned_uploads_manifest_and_file() {
    let Some(env) = LiveEnv::from_env("csv") else {
        return;
    };
    let summary = atp_convert(&[
        "convert",
        &example("events.csv"),
        "--out",
        &env.url(),
        "--quiet",
        "--json",
    ]);
    assert_eq!(summary["rows"], 40);
    assert_eq!(summary["files"], 1);
    assert_eq!(
        summary["manifest"].as_str().expect("manifest string"),
        format!("{}/_manifest.json", env.url())
    );

    let rt = runtime();
    let stored = rt.block_on(load(&env));
    assert_eq!(stored.manifest.options.partition, Partition::None);
    assert_eq!(assert_objects_match_manifest(&stored, "complete"), 40);
    rt.block_on(cleanup(&env));
}

#[test]
fn day_partitions_upload_under_partition_prefixes() {
    let Some(env) = LiveEnv::from_env("partitions") else {
        return;
    };
    let dir = tempfile::tempdir().expect("tmp");
    let source = dir.path().join("two_days.csv");
    let mut body = String::from("@timestamp,service,status\n");
    for (day, hour) in [(20, 10), (20, 11), (20, 12), (21, 10), (21, 11), (21, 12)] {
        body.push_str(&format!(
            "2026-11-{day}T{hour}:00:00Z,svc-{hour},{}\n",
            200 + hour
        ));
    }
    std::fs::write(&source, body).expect("fixture");

    let summary = atp_convert(&[
        "convert",
        source.to_str().expect("utf8"),
        "--out",
        &env.url(),
        "--timestamp",
        "@timestamp",
        "--partition",
        "day",
        "--quiet",
        "--json",
    ]);
    assert_eq!(summary["rows"], 6);

    let rt = runtime();
    let stored = rt.block_on(load(&env));
    assert_eq!(stored.manifest.options.partition, Partition::Day);
    let days: BTreeSet<&str> = stored
        .manifest
        .files
        .iter()
        .map(|f| f.path.split('/').next().unwrap_or_default())
        .collect();
    assert_eq!(days, BTreeSet::from(["dt=2026-11-20", "dt=2026-11-21"]));
    assert_eq!(assert_objects_match_manifest(&stored, "complete"), 6);
    rt.block_on(cleanup(&env));
}

#[test]
fn schema_evolution_uploads_every_segment() {
    let Some(env) = LiveEnv::from_env("schema") else {
        return;
    };
    let dir = tempfile::tempdir().expect("tmp");
    let source = dir.path().join("evolving.jsonl");
    let mut body = String::new();
    for i in 0..6 {
        body.push_str(&format!("{{\"a\":{i},\"b\":\"first\"}}\n"));
    }
    for i in 6..12 {
        body.push_str(&format!("{{\"a\":{i},\"b\":\"second\",\"c\":true}}\n"));
    }
    std::fs::write(&source, body).expect("fixture");

    let summary = atp_convert(&[
        "convert",
        source.to_str().expect("utf8"),
        "--out",
        &env.url(),
        "--batch-rows",
        "4",
        "--sample-rows",
        "4",
        "--quiet",
        "--json",
    ]);
    assert_eq!(summary["rows"], 12);
    assert_eq!(
        summary["schema_hashes"].as_array().expect("hashes").len(),
        2
    );

    let rt = runtime();
    let stored = rt.block_on(load(&env));
    assert_eq!(stored.manifest.schemas.len(), 2);
    assert_eq!(stored.manifest.files.len(), 2);
    assert_eq!(assert_objects_match_manifest(&stored, "complete"), 12);
    rt.block_on(cleanup(&env));
}

#[test]
fn file_cap_splits_uploads_and_manifest_lists_all_parts() {
    let Some(env) = LiveEnv::from_env("file-cap") else {
        return;
    };
    let summary = atp_convert(&[
        "convert",
        &example("events.csv"),
        "--out",
        &env.url(),
        "--max-file-rows",
        "15",
        "--batch-rows",
        "15",
        "--quiet",
        "--json",
    ]);
    assert_eq!(summary["rows"], 40);
    assert_eq!(summary["files"], 3);

    let rt = runtime();
    let stored = rt.block_on(load(&env));
    let paths: Vec<&str> = stored
        .manifest
        .files
        .iter()
        .map(|f| f.path.as_str())
        .collect();
    assert_eq!(
        paths,
        vec![
            "part-00-00000.parquet",
            "part-00-00001.parquet",
            "part-00-00002.parquet"
        ]
    );
    let rows: Vec<u64> = stored.manifest.files.iter().map(|f| f.rows).collect();
    assert_eq!(rows, vec![15, 15, 10]);
    assert_eq!(assert_objects_match_manifest(&stored, "complete"), 40);
    rt.block_on(cleanup(&env));
}

/// Stops the engine after a fixed number of pages, then proves the bucket holds
/// exactly the files the `cancelled` manifest lists.
struct CancelAfter {
    inner: Box<dyn Source>,
    pages_left: usize,
    cancel: Arc<AtomicBool>,
}

#[async_trait]
impl Source for CancelAfter {
    fn describe(&self) -> SourceInfo {
        self.inner.describe()
    }

    async fn infer(&mut self) -> AtpResult<Schema> {
        self.inner.infer().await
    }

    async fn next_page(&mut self, max_rows: usize) -> AtpResult<Option<Page>> {
        if self.pages_left == 0 {
            self.cancel.store(true, Ordering::Relaxed);
        } else {
            self.pages_left -= 1;
        }
        self.inner.next_page(max_rows).await
    }

    fn timestamp_column(&self) -> Option<String> {
        self.inner.timestamp_column()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_run_uploads_completed_files_and_cancelled_manifest() {
    let Some(env) = LiveEnv::from_env("cancelled") else {
        return;
    };
    let dir = tempfile::tempdir().expect("tmp");
    let source_path = dir.path().join("wide.csv");
    let mut body = String::from("id,value\n");
    for i in 0..20_000 {
        body.push_str(&format!("{i},v{i}\n"));
    }
    std::fs::write(&source_path, body).expect("fixture");

    let config = SourceConfig::new(
        SourceKind::Csv,
        source_path.to_str().expect("utf8").to_string(),
    );
    let source = atp_core::source::open(&config).expect("open csv");
    let cancel = Arc::new(AtomicBool::new(false));
    let source: Box<dyn Source> = Box::new(CancelAfter {
        inner: source,
        pages_left: 0,
        cancel: Arc::clone(&cancel),
    });
    let mut opts = RunOptions::new(PathBuf::from(env.url()));
    opts.batch_rows = 1_000;
    opts.sink.max_file_rows = 1_000;

    let summary = engine::run(source, opts, None, cancel)
        .await
        .expect("cancelled run still returns a summary");
    assert_eq!(
        summary.local_manifest, None,
        "remote run has no local manifest"
    );
    assert_eq!(summary.manifest, format!("{}/_manifest.json", env.url()));

    let stored = load(&env).await;
    assert_eq!(stored.manifest.status, "cancelled");
    let rows = assert_objects_match_manifest(&stored, "cancelled");
    assert_eq!(rows, summary.rows);
    assert!(rows > 0, "at least one completed file must be durable");
    assert!(rows < 20_000, "the run really was cancelled early");
    cleanup(&env).await;
}
