#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! End-to-end conversion tests: write with the engine, read the bytes back with
//! an independent Parquet reader, and check the manifest against reality.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atp_core::engine::{self, RunOptions};
use atp_core::manifest;
use atp_core::sink::Partition;
use atp_core::source::{self, SourceConfig, SourceKind};
use atp_core::Error;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn local_manifest(summary: &atp_core::engine::Summary) -> PathBuf {
    summary.local_manifest.clone().expect("local manifest")
}

fn write(path: &Path, body: &str) {
    std::fs::write(path, body).expect("write fixture");
}

fn read_rows(path: &Path) -> (usize, Vec<String>) {
    let file = File::open(path).expect("open parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
    let names: Vec<String> = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let reader = builder.build().expect("build reader");
    let mut rows = 0;
    for batch in reader {
        rows += batch.expect("batch").num_rows();
    }
    (rows, names)
}

fn all_parquet(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry.expect("walk");
        if entry.path().extension().is_some_and(|e| e == "parquet") {
            out.push(entry.path().to_path_buf());
        }
    }
    out.sort();
    out
}

fn csv_fixture() -> String {
    let mut body = String::from("@timestamp,service,status,latency_ms,ok\n");
    for i in 0..60 {
        let day = if i < 30 { "21" } else { "22" };
        body.push_str(&format!(
            "2026-11-{day}T0{}:00:00Z,api,{},12.5,{}\n",
            i % 10,
            200 + i,
            i % 2 == 0
        ));
    }
    body
}

#[tokio::test]
async fn converts_csv_with_day_partitions_and_a_verifiable_manifest() {
    let dir = tempfile::tempdir().expect("tmp");
    let src = dir.path().join("events.csv");
    write(&src, &csv_fixture());

    let mut cfg = SourceConfig::new(SourceKind::Csv, src.display().to_string());
    cfg.timestamp_column = Some("@timestamp".to_string());
    let mut opts = RunOptions::new(dir.path().join("out"));
    opts.partition = Partition::Day;
    let summary = engine::run(
        source::open(&cfg).expect("source"),
        opts,
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("convert");

    assert_eq!(summary.rows, 60);
    assert!(summary.files >= 2, "expected one file per day");

    let files = all_parquet(local_manifest(&summary).parent().expect("parent"));
    assert_eq!(files.len(), summary.files);
    let mut total = 0;
    for file in &files {
        let (rows, names) = read_rows(file);
        total += rows;
        assert_eq!(
            names,
            vec!["@timestamp", "service", "status", "latency_ms", "ok"]
        );
        assert!(file.to_string_lossy().contains("dt=2026-11-"));
    }
    assert_eq!(total, 60);

    let report = manifest::verify(&local_manifest(&summary)).expect("verify");
    assert!(report.ok, "{}", report.summary());
    assert_eq!(report.rows_on_disk, 60);
    assert_eq!(report.files_checked, summary.files);

    // Partition directories are real Hive-style paths, not just file names.
    assert!(dir.path().join("out/dt=2026-11-21").is_dir());
    assert!(dir.path().join("out/dt=2026-11-22").is_dir());
}

#[tokio::test]
async fn rerunning_the_same_input_produces_identical_files() {
    let dir = tempfile::tempdir().expect("tmp");
    let src = dir.path().join("events.jsonl");
    let mut body = String::new();
    for i in 0..25 {
        body.push_str(&format!(
            "{{\"@timestamp\":\"2026-11-21T0{}:00:00Z\",\"n\":{i},\"s\":\"x{i}\",\"nested\":{{\"k\":{i}}}}}\n",
            i % 10
        ));
    }
    write(&src, &body);
    let mut cfg = SourceConfig::new(SourceKind::Jsonl, src.display().to_string());
    cfg.timestamp_column = Some("@timestamp".to_string());
    cfg.sample_rows = 5;

    let mut digests = Vec::new();
    for run in ["a", "b"] {
        let out = dir.path().join(run);
        let mut opts = RunOptions::new(&out);
        opts.partition = Partition::Hour;
        let summary = engine::run(
            source::open(&cfg).expect("source"),
            opts,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("convert");
        let manifest = manifest::Manifest::read(&local_manifest(&summary)).expect("read manifest");
        digests.push(
            manifest
                .files
                .iter()
                .map(|f| (f.path.clone(), f.digest.clone(), f.rows))
                .collect::<Vec<_>>(),
        );
        assert_eq!(summary.rows, 25);
    }
    assert_eq!(digests[0], digests[1], "same input must produce same bytes");
}

#[tokio::test]
async fn loose_mode_widens_and_strict_mode_refuses() {
    let dir = tempfile::tempdir().expect("tmp");
    let src = dir.path().join("mixed.jsonl");
    write(
        &src,
        "{\"n\":1}\n{\"n\":2}\n{\"n\":\"not-a-number\"}\n{\"n\":4.5}\n",
    );
    let mut cfg = SourceConfig::new(SourceKind::Jsonl, src.display().to_string());
    cfg.sample_rows = 1;

    // Loose: widens to f64 then to text; both segments are recorded.
    let out = dir.path().join("loose");
    let summary = engine::run(
        source::open(&cfg).expect("source"),
        RunOptions::new(&out),
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("loose convert");
    assert_eq!(summary.rows, 4);
    assert!(
        summary.schemas.len() >= 2,
        "expected schema segments, got {}",
        summary.schemas.len()
    );
    let manifest = manifest::Manifest::read(&local_manifest(&summary)).expect("manifest");
    let last = manifest.schemas.last().expect("schema");
    assert_eq!(
        last.fields.iter().find(|f| f.name == "n").expect("n").ty,
        atp_core::Ty::Str
    );
    assert!(
        manifest::verify(&local_manifest(&summary))
            .expect("verify")
            .ok
    );

    // Strict: refuses and still leaves a `failed` manifest with what it wrote.
    let mut strict = RunOptions::new(dir.path().join("strict"));
    strict.strict = true;
    let err = engine::run(
        source::open(&cfg).expect("source"),
        strict,
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect_err("strict must fail");
    assert!(matches!(err, Error::SchemaConflict { .. }), "{err}");
    assert_eq!(err.exit_code(), 3);
    let manifest = manifest::Manifest::read(&dir.path().join("strict/_manifest.json"))
        .expect("failed manifest exists");
    assert_eq!(manifest.status, "failed");
    assert!(manifest.total_rows < 4);
}

#[tokio::test]
async fn timestamps_in_rows_without_time_land_in_an_explicit_partition() {
    let dir = tempfile::tempdir().expect("tmp");
    let src = dir.path().join("nulls.jsonl");
    write(
        &src,
        "{\"@timestamp\":\"2026-11-21T00:00:00Z\"}\n{\"x\":1}\n",
    );
    let mut cfg = SourceConfig::new(SourceKind::Jsonl, src.display().to_string());
    cfg.timestamp_column = Some("@timestamp".to_string());
    let mut opts = RunOptions::new(dir.path().join("out"));
    opts.partition = Partition::Day;
    let summary = engine::run(
        source::open(&cfg).expect("source"),
        opts,
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("convert");
    assert_eq!(summary.rows, 2);
    assert!(dir.path().join("out/dt=_unknown").is_dir());
}

#[tokio::test]
async fn max_rows_limits_and_keeps_the_manifest_consistent() {
    let dir = tempfile::tempdir().expect("tmp");
    let src = dir.path().join("many.csv");
    let mut body = String::from("i,s\n");
    for i in 0..1000 {
        body.push_str(&format!("{i},v{i}\n"));
    }
    write(&src, &body);
    let cfg = SourceConfig::new(SourceKind::Csv, src.display().to_string());
    let mut opts = RunOptions::new(dir.path().join("out"));
    opts.max_rows = Some(100);
    opts.batch_rows = 16;
    let summary = engine::run(
        source::open(&cfg).expect("source"),
        opts,
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("convert");
    assert_eq!(summary.rows, 100);
    assert!(
        manifest::verify(&local_manifest(&summary))
            .expect("verify")
            .ok
    );
}
