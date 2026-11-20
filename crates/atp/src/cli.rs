//! Command line surface. Everything here is argument parsing plus a thin
//! presentation layer over `atp_core`.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use atp_core::engine::{self, Event, RunOptions};
use atp_core::manifest::{self, MANIFEST_NAME};
use atp_core::sink::{Compression, Partition, SinkOptions};
use atp_core::source::{self, EsConfig, SourceConfig, SourceKind};
use atp_core::{Error, Result};
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "atp",
    version,
    about = "Turn Elasticsearch indices, CSV dumps and JSONL files into partitioned Parquet",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Show schema, estimates and a few rows without writing anything
    Inspect(InspectArgs),
    /// Convert the source into partitioned Parquet plus a manifest
    Convert(ConvertArgs),
    /// Re-read the output and check it against the manifest
    Verify(VerifyArgs),
    /// List supported sources
    Sources,
    /// Interactive terminal UI (source, preview, options, live progress)
    Tui(TuiArgs),
}

#[derive(Args, Clone)]
pub struct SourceArgs {
    /// File path (csv/tsv/jsonl/ndjson, optionally .gz) or Elasticsearch URL,
    /// e.g. https://es:9200/logs-*
    pub source: String,
    /// Force the source format instead of detecting it
    #[arg(long, value_name = "csv|jsonl|es")]
    pub format: Option<String>,
    /// Column to treat as the event timestamp (RFC 3339 / ES default format)
    #[arg(long)]
    pub timestamp: Option<String>,
    /// CSV delimiter (default: sniffed from the first line)
    #[arg(long)]
    pub delimiter: Option<char>,
    /// Leading rows used to infer column types (later rows may widen a column)
    #[arg(long, default_value_t = 10_000)]
    pub sample_rows: usize,
    #[command(flatten)]
    pub es: EsArgs,
}

#[derive(Args, Clone, Default)]
pub struct EsArgs {
    /// Elasticsearch base URL (alternative to putting it in the source argument)
    #[arg(long)]
    pub url: Option<String>,
    /// Index or pattern, when the URL does not carry it
    #[arg(long)]
    pub index: Option<String>,
    /// Query DSL JSON body, e.g. '{"range":{"@timestamp":{"gte":"now-30d"}}}'
    #[arg(long)]
    pub query: Option<String>,
    /// API key (sent as `Authorization: ApiKey …`)
    #[arg(long, env = "ATP_ES_API_KEY")]
    pub api_key: Option<String>,
    #[arg(long, env = "ATP_ES_USER")]
    pub user: Option<String>,
    #[arg(long, env = "ATP_ES_PASSWORD")]
    pub password: Option<String>,
    /// Scroll keep-alive
    #[arg(long, default_value = "5m")]
    pub scroll: String,
    /// Documents per scroll request
    #[arg(long, default_value_t = 1000)]
    pub page_size: usize,
    /// Slices to walk sequentially (one checkpoint per slice)
    #[arg(long, default_value_t = 1)]
    pub slices: usize,
    /// Continue from the checkpoint in the output directory
    #[arg(long)]
    pub resume: bool,
    /// Accept self-signed certificates
    #[arg(long)]
    pub insecure: bool,
}

#[derive(Args)]
pub struct InspectArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    /// Rows to preview
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    /// Machine-readable output
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct ConvertArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    /// Output directory (created if missing)
    #[arg(long, short)]
    pub out: PathBuf,
    /// Partition layout: none, day or hour
    #[arg(long, default_value = "none")]
    pub partition: String,
    /// Compression codec
    #[arg(long, default_value = "zstd")]
    pub compression: String,
    /// Rows per Parquet row group
    #[arg(long, default_value_t = 100_000)]
    pub row_group_rows: usize,
    /// Cap on simultaneously open output files
    #[arg(long, default_value_t = 16)]
    pub max_open_files: usize,
    /// Rows per output file before a new one is started
    #[arg(long, default_value_t = 5_000_000)]
    pub max_file_rows: u64,
    /// Rows held in memory per batch
    #[arg(long, default_value_t = 8192)]
    pub batch_rows: usize,
    /// Fail instead of widening a column type
    #[arg(long)]
    pub strict: bool,
    /// Stop after N rows (sampling)
    #[arg(long)]
    pub max_rows: Option<u64>,
    /// Print the summary as JSON on stdout
    #[arg(long)]
    pub json: bool,
    /// Suppress progress lines on stderr
    #[arg(long, short)]
    pub quiet: bool,
}

#[derive(Args)]
pub struct VerifyArgs {
    /// Output directory or path to _manifest.json
    pub target: PathBuf,
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct TuiArgs {
    /// Optional source to prefill
    pub source: Option<String>,
}

pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Inspect(args) => inspect(args).await,
        Command::Convert(args) => convert(args).await,
        Command::Verify(args) => verify(args),
        Command::Sources => {
            sources();
            Ok(())
        }
        Command::Tui(args) => crate::ui::run(args.source).await,
    }
}

pub fn checkpoint_dir(out: &Path) -> PathBuf {
    out.join(".atp")
}

/// Build a source config from CLI arguments. A URL of the form
/// `https://host:9200/index` splits into base URL + index; `--url`/`--index`
/// override it.
pub fn config_from(source_args: &SourceArgs, out: &Path) -> Result<SourceConfig> {
    let raw = source_args.source.trim();
    if raw.is_empty() {
        return Err(Error::msg("source is empty"));
    }
    let (location, url_index) = split_url(raw);
    let kind = match source_args.format.as_deref() {
        Some(f) => SourceKind::parse(f)
            .ok_or_else(|| Error::msg(format!("unknown --format `{f}` (csv, jsonl, es)")))?,
        None => {
            if source_args.es.url.is_some() || source_args.es.index.is_some() {
                SourceKind::Es
            } else {
                source::detect_kind(&location).ok_or_else(|| {
                    Error::msg(format!(
                        "cannot tell the format of `{raw}`; pass --format csv|jsonl|es"
                    ))
                })?
            }
        }
    };
    let mut cfg = SourceConfig::new(kind, location.clone());
    cfg.timestamp_column = source_args.timestamp.clone();
    cfg.sample_rows = source_args.sample_rows.max(1);
    if let Some(delimiter) = source_args.delimiter {
        let mut buf = [0u8; 4];
        let text = delimiter.encode_utf8(&mut buf);
        let bytes = text.as_bytes();
        if bytes.len() != 1 {
            return Err(Error::msg("--delimiter must be a single ASCII character"));
        }
        cfg.csv_delimiter = Some(bytes[0]);
    }
    if kind == SourceKind::Es {
        let base = source_args
            .es
            .url
            .clone()
            .unwrap_or_else(|| location.clone());
        let index = source_args.es.index.clone().or(url_index).ok_or_else(|| {
            Error::msg(
                "Elasticsearch source needs an index: use https://host:9200/<index> or --index",
            )
        })?;
        cfg.location = base;
        cfg.es = EsConfig {
            index,
            query: source_args.es.query.clone(),
            api_key: source_args.es.api_key.clone(),
            user: source_args.es.user.clone(),
            password: source_args.es.password.clone(),
            scroll: source_args.es.scroll.clone(),
            page_size: source_args.es.page_size.max(1),
            slices: source_args.es.slices.max(1),
            checkpoint_dir: checkpoint_dir(out),
            resume: source_args.es.resume,
            insecure: source_args.es.insecure,
        };
    }
    Ok(cfg)
}

fn split_url(raw: &str) -> (String, Option<String>) {
    let Some(scheme_end) = raw.find("://") else {
        return (raw.to_string(), None);
    };
    let after_scheme = &raw[scheme_end + 3..];
    match after_scheme.find('/') {
        Some(slash) => {
            let base = format!("{}{}", &raw[..scheme_end + 3], &after_scheme[..slash]);
            let index = after_scheme[slash + 1..].trim_matches('/').to_string();
            (base, if index.is_empty() { None } else { Some(index) })
        }
        None => (raw.to_string(), None),
    }
}

async fn inspect(args: InspectArgs) -> Result<()> {
    let mut cfg = config_from(&args.source, Path::new("."))?;
    // A preview must not leave scroll checkpoints behind in the caller's cwd.
    cfg.es.checkpoint_dir = std::env::temp_dir().join("atp-inspect");
    let preview = engine::preview(cfg, args.limit).await?;
    if args.json {
        let payload = serde_json::json!({
            "source": preview.info,
            "schema": preview.schema,
            "columns": preview.columns,
            "rows": preview.rows,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }
    println!("source   {} ({})", preview.info.location, preview.info.kind);
    if let Some(rows) = preview.info.rows_estimate {
        println!("rows     ~{rows}");
    }
    if let Some(bytes) = preview.info.bytes_estimate {
        println!("bytes    {bytes}");
    }
    for note in &preview.info.notes {
        println!("note     {note}");
    }
    println!("\nschema ({} columns)", preview.schema.fields.len());
    for line in atp_core::tui::schema_lines(&preview.schema) {
        println!("  {line}");
    }
    if !preview.rows.is_empty() {
        println!("\nfirst {} row(s)", preview.rows.len());
        println!("  {}", preview.columns.join(" | "));
        for row in &preview.rows {
            println!("  {}", row.join(" | "));
        }
    }
    Ok(())
}

async fn convert(args: ConvertArgs) -> Result<()> {
    let partition = Partition::parse(&args.partition)
        .ok_or_else(|| Error::msg(format!("unknown --partition `{}`", args.partition)))?;
    let compression = Compression::parse(&args.compression)
        .ok_or_else(|| Error::msg(format!("unknown --compression `{}`", args.compression)))?;
    let cfg = config_from(&args.source, &args.out)?;
    if !matches!(partition, Partition::None) && cfg.timestamp_column.is_none() {
        return Err(Error::msg(
            "time partitioning needs --timestamp <column>; pass --partition none to skip",
        ));
    }
    let opts = RunOptions {
        out: args.out.clone(),
        partition,
        strict: args.strict,
        batch_rows: args.batch_rows.max(1),
        sink: SinkOptions {
            compression,
            row_group_rows: args.row_group_rows.max(1),
            max_open_files: args.max_open_files.max(1),
            max_file_rows: args.max_file_rows.max(1),
        },
        max_rows: args.max_rows,
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let quiet = args.quiet;
    let printer = tokio::spawn(async move {
        let mut last = Instant::now() - Duration::from_secs(5);
        while let Some(event) = rx.recv().await {
            if quiet {
                continue;
            }
            match event {
                Event::Opened { info } => {
                    eprintln!("atp: reading {} from {}", info.kind, info.location);
                    if let Some(rows) = info.rows_estimate {
                        eprintln!("atp: source reports ~{rows} rows");
                    }
                }
                Event::SchemaReady { schema } => {
                    eprintln!(
                        "atp: schema {} ({} columns)",
                        schema.hash(),
                        schema.fields.len()
                    );
                }
                Event::SchemaWidened { schema, reason } => {
                    eprintln!("atp: schema widened to {} ({reason})", schema.hash());
                }
                Event::Progress {
                    rows,
                    bytes,
                    files,
                    elapsed_ms,
                } => {
                    if last.elapsed() >= Duration::from_secs(1) {
                        let secs = (elapsed_ms as f64 / 1000.0).max(0.001);
                        eprintln!(
                            "atp: {rows} rows, {} written, {files} file(s), {:.0} rows/s",
                            human_bytes(bytes),
                            rows as f64 / secs
                        );
                        last = Instant::now();
                    }
                }
                Event::Finished { .. } => {}
            }
        }
    });
    let result = engine::run(cfg, opts, Some(tx), cancel).await;
    let _ = printer.await;
    let summary = result?;
    if args.json {
        let payload = serde_json::json!({
            "rows": summary.rows,
            "files": summary.files,
            "bytes": summary.bytes,
            "manifest": summary.manifest,
            "elapsed_ms": summary.elapsed.as_millis() as u64,
            "columns": summary.schemas.last().map(|s| s.fields.len()),
            "schema_hashes": summary.schemas.iter().map(|s| s.hash()).collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!(
            "wrote {} rows to {} file(s), {}, in {:.2}s",
            summary.rows,
            summary.files,
            human_bytes(summary.bytes),
            summary.elapsed.as_secs_f64()
        );
        println!("manifest {}", summary.manifest.display());
        println!("verify   atp verify {}", args.out.display());
    }
    Ok(())
}

fn verify(args: VerifyArgs) -> Result<()> {
    let manifest_path = if args.target.is_dir() {
        args.target.join(MANIFEST_NAME)
    } else {
        args.target.clone()
    };
    let report = manifest::verify(&manifest_path)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("status   {}", report.status);
        println!("files    {}", report.files_checked);
        println!("rows     {}", report.rows_on_disk);
        println!("bytes    {}", human_bytes(report.bytes_manifest));
        println!("result   {}", if report.ok { "ok" } else { "MISMATCH" });
        for problem in &report.problems {
            println!("problem  {}: {}", problem.path, problem.detail);
        }
    }
    if report.ok {
        Ok(())
    } else {
        Err(Error::Verify(report.summary()))
    }
}

fn sources() {
    println!("csv     delimited text: .csv .tsv, optional .gz, delimiter sniffed");
    println!("jsonl   one JSON object per line: .jsonl .ndjson .json, optional .gz");
    println!("es      Elasticsearch/OpenSearch via scroll, mapping-driven schema");
    println!();
    println!("planned sqlite, mysql, parquet passthrough (see docs/RATIONALE.md)");
}

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn url_split_keeps_base_and_index() {
        let (base, index) = split_url("https://es.example:9200/logs-*");
        assert_eq!(base, "https://es.example:9200");
        assert_eq!(index.as_deref(), Some("logs-*"));
        let (base, index) = split_url("http://localhost:9200");
        assert_eq!(base, "http://localhost:9200");
        assert_eq!(index, None);
        let (base, index) = split_url("/data/events.csv");
        assert_eq!(base, "/data/events.csv");
        assert_eq!(index, None);
    }

    #[test]
    fn bytes_are_human_readable() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.00 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.00 MiB");
    }

    #[test]
    fn config_requires_an_index_for_es() {
        let args = SourceArgs {
            source: "https://localhost:9200".into(),
            format: None,
            timestamp: None,
            delimiter: None,
            sample_rows: 10_000,
            es: EsArgs::default(),
        };
        let err = config_from(&args, Path::new("/tmp")).expect_err("needs index");
        assert!(err.to_string().contains("index"));
    }
}
