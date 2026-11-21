#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! End-to-end: a real Prometheus block, through the shared engine, to
//! partitioned Parquet with a verifiable manifest.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atp_core::engine::{self, RunOptions};
use atp_core::manifest;
use atp_core::sink::Partition;
use atp_tsdb::{TsdbOptions, TsdbShape, TsdbSource};

const BLOCK_START_MS: i64 = 1_650_005_022_282;

fn fixture() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/testblock")
        .display()
        .to_string()
}

fn window(shape: TsdbShape) -> TsdbOptions {
    let mut options = TsdbOptions::new(fixture());
    options.shape = shape;
    options.from_ms = Some(BLOCK_START_MS);
    options.to_ms = Some(BLOCK_START_MS + 120_000);
    options
}

#[tokio::test]
async fn converts_a_block_window_to_hour_partitions() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut opts = RunOptions::new(dir.path().join("lake"));
    opts.partition = Partition::Hour;
    let summary = engine::run(
        Box::new(TsdbSource::open(window(TsdbShape::Wide)).expect("source")),
        opts,
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("convert");

    assert!(summary.rows > 10_000, "rows={}", summary.rows);
    assert_eq!(summary.source.kind, "tsdb");
    let report = manifest::verify(&summary.manifest).expect("verify");
    assert!(report.ok, "{}", report.summary());
    assert_eq!(report.rows_on_disk, summary.rows);

    let schema = summary.schemas.last().expect("schema");
    assert_eq!(schema.fields[0].name, "timestamp");
    assert_eq!(schema.fields[0].ty, atp_core::Ty::TimestampMillis);
    assert_eq!(schema.fields[1].name, "metric");
    assert_eq!(schema.fields[2].ty, atp_core::Ty::F64);
    assert_eq!(schema.fields[3].name, "series_fingerprint");
    assert!(
        schema.fields.iter().any(|f| f.name == "job"),
        "label columns missing"
    );

    let manifest = manifest::Manifest::read(&summary.manifest).expect("manifest");
    let options = manifest.options;
    assert_eq!(options.partition, Partition::Hour);
    assert_eq!(options.timestamp_column.as_deref(), Some("timestamp"));
    assert!(
        manifest
            .files
            .iter()
            .all(|f| f.path.contains("dt=") && f.path.contains("hour=")),
        "expected Hive hour partitions: {:?}",
        manifest.files.iter().map(|f| &f.path).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn long_shape_writes_one_json_labels_column() {
    let dir = tempfile::tempdir().expect("tmp");
    let summary = engine::run(
        Box::new(TsdbSource::open(window(TsdbShape::Long)).expect("source")),
        RunOptions::new(dir.path().join("lake")),
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("convert");

    assert!(summary.rows > 10_000);
    let names: Vec<String> = summary
        .schemas
        .last()
        .expect("schema")
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(
        names,
        vec![
            "timestamp".to_string(),
            "metric".to_string(),
            "value".to_string(),
            "series_fingerprint".to_string(),
            "labels".to_string(),
        ]
    );
    assert!(manifest::verify(&summary.manifest).expect("verify").ok);
}

#[tokio::test]
async fn a_matcher_limits_which_series_are_written() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut options = window(TsdbShape::Wide);
    options.matchers = vec!["__name__=\"ALERTS\"".to_string()];
    let all = engine::run(
        Box::new(TsdbSource::open(window(TsdbShape::Wide)).expect("source")),
        RunOptions::new(dir.path().join("all")),
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("all");
    let filtered = engine::run(
        Box::new(TsdbSource::open(options).expect("source")),
        RunOptions::new(dir.path().join("filtered")),
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("filtered");
    assert!(
        filtered.rows < all.rows,
        "matcher did not reduce rows: {} vs {}",
        filtered.rows,
        all.rows
    );
    assert!(filtered.rows > 0);
}
