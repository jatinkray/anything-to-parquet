# anything-to-parquet

**Get your data out, in the format every analytics engine already reads.**

`atp` turns data that is trapped in operational systems — an Elasticsearch or
OpenSearch index, a 40 GB CSV dump, a directory of JSONL — into partitioned
Apache Parquet with a manifest you can verify. One static Rust binary. No JVM,
no Python, no Spark, no cluster.

```bash
atp inspect https://es.example:9200/logs-2026.11.* --timestamp @timestamp
atp convert https://es.example:9200/logs-2026.11.* \
  --out ./lake --timestamp @timestamp --partition day   # local disk in v0.1
atp verify ./lake          # re-reads every file, compares rows + xxh3 digests
atp tui                    # same engine, four screens, live progress
```

## Why this exists

The destination format is settled: Parquet in object storage is what DuckDB,
Athena, BigQuery, Snowflake, Spark, Trino and even Elasticsearch's own
[ES|QL Data Federation](https://www.elastic.co/docs/reference/query-languages/esql/esql-data-federation-file-formats)
read. Getting *existing* data there is still hand-rolled.

| Evidence (fetched 2026-11-21) | What it means |
|---|---|
| Elastic's ES|QL Data Federation **reads** Parquet from external object storage (Stack 9.5 experimental, Serverless preview) | Elastic is a Parquet consumer with no first-party export path |
| Elastic Cloud **Serverless** has no user-initiated snapshot/restore ("Planned") and **no Elasticsearch-Hadoop** ([comparison table](https://www.elastic.co/docs/deploy-manage/deploy/elastic-cloud/differences-from-other-elasticsearch-offerings)) | On Serverless the only bulk egress is scroll/PIT/ES\|QL, and the Spark route is unavailable |
| `elasticsearch-dump` has 7.9k stars and exports **JSON only** | The de-facto ES exporter cannot produce columnar files |
| The only dedicated ES→Parquet repo found is a 1-star Python script last pushed 2023-09 | No maintained product exists for this |
| `csv2parquet`-style tools are single-purpose; the most-starred one is archived | Nothing unifies sources or adds partitions, manifests or verification |
| DuckDB's `elasticsearch` community extension (MIT, 23 stars) exists | SQL users have a path — but it needs SQL, materialises through DuckDB, and has no partition layout, manifest or verification |

Full sources, dates and the competitive scan: [docs/RATIONALE.md](docs/RATIONALE.md).
Measured results: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## What it does

| Command | Purpose |
|---|---|
| `atp inspect <source>` | Schema, estimates and sample rows without writing anything |
| `atp convert <source> --out <dir>` | Partitioned Parquet + `_manifest.json` |
| `atp verify <dir>` | Recompute every file's xxh3 digest and row count against the manifest |
| `atp sources` | Supported inputs and the roadmap |
| `atp tui` | Interactive: source → schema → options → live progress |

Sources: CSV/TSV (delimiter sniffing, header detection, optional `.gz`),
JSONL/NDJSON/`.json` (nested objects flattened to `a.b.c`, arrays kept as JSON
text, optional `.gz`), and Elasticsearch/OpenSearch via the scroll API with
mapping-driven typing and sequential slices. (Crash-safe resume is not in v0.1: see
[docs/LIMITATIONS.md](docs/LIMITATIONS.md).)

## Guarantees

- **Bounded memory.** Streaming pages, one open row group per partition, capped
  open files. 5M rows converted in 41 MB RSS (see benchmarks).
- **No partial files.** Files are written as `*.parquet.tmp` and renamed only
  when complete.
- **No silent type loss.** A column that does not fit is widened
  (`i64 → f64 → str`, `timestamp ↔ epoch millis`) and the manifest lists every
  schema segment that was written. `--strict` fails instead, with exit code 3.
- **Provable output.** Every file's xxh3-64 digest and row count are recorded at
  write time; `atp verify` recomputes them.
- **Deterministic.** Same input + options → same file names and the same bytes.

## Install

```bash
cargo install --path crates/atp        # or: cargo build --release
./target/release/atp --help
```

Linux is what CI builds and tests (including a static x86_64-musl binary); macOS
and Windows build from source with the same toolchain and are not exercised in
CI yet. No runtime dependencies.

## A five minute tour

```bash
# 1. Look before you leap: inferred types, row counts, sample rows.
atp inspect examples/events.csv --timestamp @timestamp

# 2. Convert with day partitions. zstd 3, 100k-row row groups by default.
atp convert examples/events.csv --out ./lake --timestamp @timestamp --partition day

# 3. Prove it.
atp verify ./lake

# 4. Query it with whatever you already have.
duckdb -c "SELECT dt, count(*) FROM read_parquet('./lake/**/*.parquet', hive_partitioning=true) GROUP BY 1 ORDER BY 1"
```

Flattened JSON columns keep their dots (`http.status`), so quote them in SQL:
`SELECT sum("http.status") FROM read_parquet(…)`.

Sample output:

```
wrote 40 rows to 1 file(s), 2.31 KiB, in 0.00s
manifest /…/lake/_manifest.json
verify   atp verify ./lake
```

## Elasticsearch specifics

```bash
atp convert https://es:9200/logs-2026.11.* \
  --query '{"range":{"@timestamp":{"gte":"now-7d"}}}' \
  --api-key "$ATP_ES_API_KEY" \
  --timestamp @timestamp --partition hour \
  --out ./lake
```

- Schema comes from `GET /{index}/_mapping` (mapping wins over sampling:
  `keyword` stays text even if every value looks numeric).
- Rows come from the scroll API; `--slices N` splits the index into N slices and
  walks them sequentially, each with its own (shorter) scroll context.
- If Elasticsearch expires the scroll context mid-run the conversion fails with
  the slice number, rather than silently skipping data. For very large indices,
  narrow the work with `--query` time windows and give each window its own output
  directory. One run = one output directory: file names are deterministic, so a
  rerun into the same directory replaces same-named files and rewrites
  `_manifest.json`.
- `--page-size` (default 1000) and `--scroll` (default `5m`) are the throughput
  knobs.

## Layout of an output directory

```
lake/
  _manifest.json
  dt=2026-11-21/hour=10/part-00-00000.parquet
  dt=2026-11-21/hour=11/part-00-00001.parquet
```

`_manifest.json` records: tool version, status (`complete`/`cancelled`/`failed`),
source description, options, every schema written, every file with rows, bytes,
xxh3 digest and schema hash, and the totals. If a run fails or is cancelled the
completed files are still listed, so partial work remains verifiable.

## Related

Prometheus/Thanos **TSDB blocks** are handled by the sibling project
[`tsdb2parqtel`](https://github.com/jatinkray/tsdb-parquet-convert): TSDB index
v1/v2, XOR/histogram chunks, tombstones, per-series verification. This repository
covers what TSDB readers cannot: Elasticsearch/OpenSearch indices, CSV dumps and
JSONL exports. Folding TSDB blocks into this binary is on the roadmap once the
source trait has a second non-HTTP implementation to generalise against.

## Non-goals (v0.1)

No streaming ingestion (use Vector), no query engine (use DuckDB/Athena), no
import into Elasticsearch (one-way export), no list/map Parquet columns for
arbitrary JSON (arrays are preserved as JSON text). Details in
[docs/LIMITATIONS.md](docs/LIMITATIONS.md).

## Development

```bash
make lint    # fmt check + clippy -D warnings
make test    # unit + integration + CLI tests
make bench   # reproducible 5M-row CSV benchmark
make tui-smoke  # drives the TUI through a PTY and asserts it renders/exits
```

Design notes: [docs/DESIGN.md](docs/DESIGN.md). Apache-2.0.
