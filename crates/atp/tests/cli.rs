#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! CLI contract tests: exit codes, stdout/stderr split, and the manifest that
//! `verify` consumes. These run the real binary.

use std::path::Path;
use std::process::Command;

fn atp() -> Command {
    Command::new(env!("CARGO_BIN_EXE_atp"))
}

fn fixture(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("logs.csv");
    let mut body = String::from("@timestamp,service,status\n");
    for i in 0..40 {
        body.push_str(&format!(
            "2026-11-2{}T0{}:00:00Z,svc-{},2{:02}\n",
            1 + (i % 2),
            i % 10,
            i % 3,
            i % 100
        ));
    }
    std::fs::write(&path, body).expect("fixture");
    path
}

#[test]
fn convert_then_verify_round_trips_and_keeps_stdout_clean() {
    let dir = tempfile::tempdir().expect("tmp");
    let src = fixture(dir.path());
    let out = dir.path().join("lake");
    let result = atp()
        .args([
            "convert",
            src.to_str().expect("utf8"),
            "--out",
            out.to_str().expect("utf8"),
            "--timestamp",
            "@timestamp",
            "--partition",
            "day",
            "--quiet",
            "--json",
        ])
        .output()
        .expect("run");
    assert!(
        result.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stdout = String::from_utf8_lossy(&result.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json on stdout");
    assert_eq!(parsed["rows"], 40);
    assert!(parsed["files"].as_u64().unwrap_or(0) >= 2);

    let verify = atp()
        .args(["verify", out.to_str().expect("utf8"), "--json"])
        .output()
        .expect("verify");
    assert!(verify.status.success());
    let report: serde_json::Value =
        serde_json::from_str(&String::from_utf8_lossy(&verify.stdout)).expect("json");
    assert_eq!(report["ok"], true);
    assert_eq!(report["rows_on_disk"], 40);
}

#[test]
fn missing_manifest_is_an_error_not_a_silent_pass() {
    let dir = tempfile::tempdir().expect("tmp");
    let result = atp()
        .args(["verify", dir.path().to_str().expect("utf8")])
        .output()
        .expect("run");
    assert!(!result.status.success());
}

#[test]
fn unknown_format_is_reported_as_an_error() {
    let result = atp()
        .args(["inspect", "/tmp/whatever.xyz", "--format", "not-a-format"])
        .output()
        .expect("run");
    assert_eq!(result.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&result.stderr).contains("unknown --format"));
}

#[test]
fn inspect_prints_schema_without_writing_output() {
    let _dir = tempfile::tempdir().expect("tmp");
    let src = fixture(_dir.path());
    let result = atp()
        .args(["inspect", src.to_str().expect("utf8"), "--limit", "3"])
        .output()
        .expect("run");
    assert!(result.status.success());
    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(stdout.contains("@timestamp"), "{stdout}");
    assert!(stdout.contains("first 3 row(s)"), "{stdout}");
    assert!(!_dir.path().join("lake").exists());
}

#[test]
fn strict_mode_exits_with_3() {
    let dir = tempfile::tempdir().expect("tmp");
    let src = dir.path().join("mixed.jsonl");
    std::fs::write(&src, "{\"n\":1}\n{\"n\":2}\n{\"n\":\"oops\"}\n").expect("fixture");
    let result = atp()
        .args([
            "convert",
            src.to_str().expect("utf8"),
            "--out",
            dir.path().join("out").to_str().expect("utf8"),
            "--strict",
            "--sample-rows",
            "1",
            "--quiet",
        ])
        .output()
        .expect("run");
    assert_eq!(result.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&result.stderr).contains("strict"));
}
