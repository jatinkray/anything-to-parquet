<div align="center">
  <img src="docs/assets/hero.svg" alt="anything-to-parquet — one static binary from CSV, JSONL, Elasticsearch and Prometheus TSDB to lake-ready Parquet" width="100%">
</div>

<div align="center">

[**Quickstart**](#quickstart) · [**Sources**](#sources) · [**Output contract**](#the-output-contract) · [**Benchmarks**](#benchmarks) · [**CLI**](#cli-reference) · [**Docs**](#documentation-map) · [**Roadmap**](#roadmap--non-goals)

[![CI](https://github.com/jatinkray/anything-to-parquet/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jatinkray/anything-to-parquet/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/tests-84%20passing-brightgreen.svg)](#development)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.89%2B-orange.svg)](https://www.rust-lang.org)
[![Platforms](https://img.shields.io/badge/platforms-linux%20(ci)%20%C2%B7%20build%20from%20source-lightgrey.svg)](#install)

</div>

---

**`atp` gets operational data out of the systems that trap it** — an
Elasticsearch index, a 40 GB CSV dump, a directory of JSONL, or a
Prometheus/Thanos block — and writes partitioned Parquet with a manifest you can
verify. One static Rust binary. No JVM, no Python, no Spark, no cluster.

```bash
atp convert https://es:9200/logs-2026.11.* --out ./lake --timestamp @timestamp --partition day
atp convert ./prometheus/blocks          --out ./metrics --partition hour --tsdb-shape series
atp verify ./lake        # re-reads every file, compares rows + xxh3 digests
atp tui                  # same engine, four screens, live progress
```

---

## Why this exists

The destination format is settled. Parquet in object storage is what DuckDB,
Athena, BigQuery, Snowflake, Spark, Trino — and Elasticsearch's own
[ES|QL Data Federation](https://www.elastic.co/docs/reference/query-languages/esql/esql-data-federation-file-formats)
— read. Getting *existing* data there is still hand-rolled.

| Evidence (fetched 2026-11-21) | What it means |
|---|---|
| Elastic ES\|QL Data Federation **reads** external Parquet; nothing exports to it | Elastic is a Parquet consumer, not a producer |
| Elastic Cloud **Serverless** has no user snapshots and no ES-Hadoop | On Serverless, scroll/PIT/ES\|QL is the only egress |
| `elasticsearch-dump` (7.9k★) exports **JSON only**; the only ES→Parquet repo found is a 1★ script from 2023 | No maintained ES→Parquet product exists |
| `csv2parquet`-class tools are single-file, unpartitioned; the most-starred is archived | No lake-aware CSV tooling |
| DuckDB's `elasticsearch` extension exists | A SQL path, but no layout, manifest, resume or single binary |
| `elastic/elasticsearch#166095` prototypes a Parquet **write engine** (new indices only, flagged off) | Direction validated; not an exporter |

Full sources and dates: [docs/RATIONALE.md](docs/RATIONALE.md).

---

## Sources

| Source | Detect | Highlights |
|---|---|---|
| **CSV / TSV** | `.csv`, `.tsv`, `.csv.gz` | delimiter sniffing (through gzip), header detection, 10k-row type inference, leading-zero IDs kept as text, streaming |
| **JSONL / NDJSON** | `.jsonl`, `.ndjson`, `.json`, `.gz` | nested objects flipped to `a.b.c`, arrays kept as JSON text, late keys become new columns |
| **Elasticsearch / OpenSearch** | `http(s)://host:9200/index` | mapping-driven types (no sampling guesswork), scroll API, sequential slices, API key or basic auth, `--query` DSL |
| **Prometheus / Thanos TSDB** | a block directory, a directory of blocks, `s3://`/`gs://`/`az://` | index v1/v2, XOR/XOR2/histogram chunks, tombstones, Thanos external labels, selectors, time windows, replica dedup, three row shapes — merged from [`tsdb2parqtel`](https://github.com/jatinkray/tsdb-parquet-convert) ([details](docs/TSDB.md)) |

All four share one engine: one schema model, one partitioner, one manifest
format, one verification command.

---

## Quickstart

### Install

```bash
cargo install --path crates/atp        # or: cargo build --release
./target/release/atp --help
```

CI builds and tests a static Linux x86_64 (musl) binary; release archives are
not published yet. macOS and Windows build from source with the same toolchain
and are not yet exercised in CI. No runtime dependencies.

### 1. Look before you leap

```bash
atp inspect examples/events.csv --timestamp @timestamp
```

```
source   examples/events.csv (csv)
bytes    1985
note     delimiter ',', 6 columns, types from up to 10000 rows

schema (6 columns)
  @timestamp   timestamp_ms
  service      str
  host         str
  status       i64
  latency_ms   f64
  ok           bool
```

### 2. Convert

```bash
atp convert examples/events.csv --out ./lake --timestamp @timestamp --partition day
```

```
wrote 40 rows to 1 file(s), 2.31 KiB, in 0.00s
manifest /…/lake/_manifest.json
verify   atp verify ./lake
```

### 3. Prove it

```bash
atp verify ./lake        # rows, bytes and xxh3-64 digests vs the manifest
```

### 4. Query it with what you already have

```bash
duckdb -c "SELECT dt, count(*) FROM read_parquet('./lake/**/*.parquet', hive_partitioning=true) GROUP BY 1"
```

Flattened JSON names keep their dots (`http.status`), so quote them in SQL:
`SELECT sum("http.status") FROM read_parquet(…)`.

### Prometheus blocks

```bash
atp convert /var/lib/prometheus/blocks --out ./metrics \
  --partition hour --tsdb-shape wide --tsdb-from 2026-11-20T00:00:00Z
atp verify ./metrics
```

```
wrote 5550882 rows to 2 file(s), 3.78 MiB, in 3.80s
```

See [docs/TSDB.md](docs/TSDB.md) for shapes, selectors and edge cases.

---

## The output contract

```
lake/
  _manifest.json
  dt=2026-11-21/hour=10/part-00-00000.parquet
  dt=2026-11-21/hour=11/part-00-00001.parquet
```

| Guarantee | How |
|---|---|
| **No partial files** | written as `*.parquet.tmp`, renamed only when the footer is complete |
| **Bounded memory** | streaming pages, one row group per partition, capped open files |
| **No silent type loss** | `i64 → f64 → str`, `timestamp ↔ epoch millis`; every schema segment is recorded; `--strict` fails with exit 3 instead |
| **Provable output** | every file's xxh3-64 digest and row count is hashed while writing and rechecked by `atp verify` |
| **Deterministic** | same input + options → same file names and bytes (one run per `--out`) |

`_manifest.json` records the tool version, status (`complete`/`cancelled`/`failed`),
source description, options, every schema written, and every file with rows,
bytes, digest and schema hash.

Exit codes: `0` ok · `1` general · `2` usage · `3` schema conflict/bad timestamp ·
`4` verification mismatch · `5` source/network · `130` cancelled.

---

## Benchmarks

Measured with the scripts in [`bench/`](bench); full method and caveats in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md).

| Workload | Result |
|---|---|
| **5,000,000-row CSV** (385 MB) → Parquet | **42.9 MB**, 8.97× vs CSV, **2.07× smaller than `gzip -6`**, 7.8–8.6 s, **~640k rows/s**, **41 MB peak RSS** |
| **Prometheus block, 5.55M samples** | **3.78 MB** Parquet in 3.8 s, ~90 MB peak RSS |
| **TSDB wide shape**, 142k rows × 206 label columns | 888 KB in 1.6 s, 173 MB peak RSS |
| **GitHub Archive JSONL hour** (615 flattened columns) | 5.28× vs raw JSON, but 1.24× *larger* than gzip — published as a caveat, not hidden |
| **Real Elasticsearch 9.1.0**, 20k docs | 20k rows, verify ok; filtered `--query` matched `_count` exactly |

---

## CLI reference

```
atp inspect <source> [--limit N] [--json]
atp convert <source> --out <dir> [output and source options]
atp verify <dir> [--json]
atp sources
atp tui [source]
```

Output options: `--partition none|day|hour` · `--compression zstd|snappy|uncompressed`
· `--row-group-rows` · `--max-open-files` · `--max-file-rows` · `--batch-rows`
· `--strict` · `--max-rows` · `--json` · `--quiet`.

Source options:

- CSV/JSONL: `--timestamp`, `--delimiter`, `--sample-rows`.
- Elasticsearch: `--url`, `--index`, `--query`, `--api-key`, `--user`/`--password`,
  `--scroll`, `--page-size`, `--slices`, `--insecure`.
- TSDB: `--tsdb-shape`, `--tsdb-from`/`--tsdb-to`, `--tsdb-match`, `--tsdb-block`,
  `--tsdb-stale`, `--tsdb-on-error`, `--tsdb-external-labels`,
  `--tsdb-downsample`, `--tsdb-dedup-replica-labels`.

Sample output above; `--json` prints a machine-readable summary on stdout and
keeps progress on stderr.

### TUI

`atp tui` walks source → schema → options → live progress, using the same engine
and the same guarantees as the CLI. `↑/↓` move, `←/→`/space change a value,
`Enter` advance, `Esc` back, `q` cancel a run, `Ctrl-C` quit. It refuses to start
without a TTY (`atp convert` is the scriptable path).

---

## Architecture

```
source (csv | jsonl | elasticsearch | tsdb)
   │  pages of rows, values shared as Arc<str>
   ▼
engine (atp-core)
   │  schema by segment: coerce, widen, flush at 8,192 rows
   ▼
sink (atp-core)
   │  partition by dt=/hour=, zstd-3, 100k-row groups, hash while writing
   ▼
_manifest.json  →  atp verify  →  DuckDB / Athena / Spark / Trino
```

`crates/atp-core` holds the engine, schema model, sink, manifest and TUI state
machine. `crates/atp-tsdb` adds Prometheus/Thanos reading (vendored reader, see
[PROVENANCE](crates/atp-tsdb/PROVENANCE.md)). `crates/atp` is the CLI and
ratatui front-end. Details: [docs/DESIGN.md](docs/DESIGN.md).

---

## Documentation map

| Document | Contents |
|---|---|
| [docs/RATIONALE.md](docs/RATIONALE.md) | Market evidence, prior art with star counts, demand signals, the closest platform threat, honest economics |
| [docs/DESIGN.md](docs/DESIGN.md) | Schema lattice, segment evolution, sink atomicity, manifest contract, exit codes, test strategy |
| [docs/TSDB.md](docs/TSDB.md) | Prometheus/Thanos blocks: shapes, flags, bounded memory, native histograms, reconciliation with `tsdb2parqtel` |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | Measured results, datasets with hashes, method, what is not measured |
| [docs/LIMITATIONS.md](docs/LIMITATIONS.md) | Everything v0.2 does not do, and why |
| `plan/` (local, gitignored) | Raw research log, market plan, build plan, resume analysis, session log |

---

## Roadmap / non-goals

**Planned:** PIT + `search_after` for crash-safe Elasticsearch resume · parallel
slices · Parquet `LIST` columns for JSON arrays · file compaction · `s3://`
output with multipart and per-file checksums · per-series TSDB reconciliation
inside `atp verify`.

**Not planned:** streaming ingestion (that is Vector's job), a query engine (use
DuckDB/Athena), importing into Elasticsearch (one-way export), being a log
platform.

Nothing here is stubbed: each row of the feature table above is implemented and
covered by tests. The gaps are listed in
[docs/LIMITATIONS.md](docs/LIMITATIONS.md) rather than hidden behind flags.

---

## Development

```bash
make lint        # cargo fmt --check + clippy -D warnings (workspace-wide)
make test        # 84 tests: unit, stub-server, engine integration, CLI contract
make docs-check  # required docs and brand assets exist
make tui-smoke   # drives the real TUI through a pty and asserts its screens
make bench       # reproducible 5M-row CSV benchmark
```

CI runs the same plus a static musl build, `cargo deny` and `cargo audit`.
The test suite includes real fixture blocks: a 35,354-series Prometheus block
and a version-1 index block.

---

## License

Apache-2.0. The TSDB reader is vendored from `tsdb2parqtel` under the same
license; test fixtures retain their MIT provenance (see
[crates/atp-tsdb/PROVENANCE.md](crates/atp-tsdb/PROVENANCE.md)).
