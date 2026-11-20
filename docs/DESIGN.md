# Design

## One engine, two front-ends

```
source (csv | jsonl | elasticsearch)
   │  pages of Vec<Value> aligned with the source's own column list
   ▼
engine::run
   │  translate by column name -> current schema
   │  BatchBuilder: coerce, signal promotion, flush at 8,192 rows
   ▼
ParquetSink
   │  partition rows by dt=/hour=, one ArrowWriter per open partition
   │  hash bytes while writing, .tmp -> rename on close
   ▼
_manifest.json  (schemas, files, rows, bytes, xxh3 digests)
```

The CLI and the TUI both call `atp_core::engine::run`. The TUI owns no
conversion logic: `atp_core::tui::App` is a state machine with unit-tested
transitions, and `crates/atp/src/ui.rs` only renders it and shuttles messages.
Anything the TUI can do, the CLI can do with flags, and vice versa.

## Schema model

Six logical types: `bool`, `i64`, `f64`, `str`, `timestamp_ms`, plus `null`
during inference. Arrow mapping: `Boolean`, `Int64`, `Float64`, `Utf8`,
`Timestamp(Millisecond, None)`.

Promotion is a symmetric lattice, never a demotion:

```
null < any
i64  <-> f64            => f64
timestamp <-> i64       => timestamp (epoch millis)
bool + number           => str   (true is not 1 in a lakehouse)
anything else           => str
```

`str` is total: every value can be rendered as text, so widening to `str` never
loses data. That is why the lattice's top is `str` and why a column that sees one
bad value in 40 million rows degrades to text instead of dropping the value.

### Schema evolution is visible

A batch never mixes two physical types in one column. When a row does not fit:

1. the `BatchBuilder` returns `Promote { field, to }` and does not buffer the row;
2. the engine flushes the current batch, closes the current Parquet segment;
3. the sink records the new schema and opens a new segment (`part-01-*.parquet`);
4. the row is pushed again.

Consequences, all tested in `crates/atp-core/tests/convert.rs`:

- files already written keep their original schema and stay valid;
- the manifest lists every schema written (`schemas[]`) and each file's
  `schema_hash`;
- a reader that unions schemas across files (DuckDB, Athena, Spark) sees a
  consistent, widening column;
- `--strict` turns step 1 into an error with exit code 3 and a manifest whose
  `status` is `failed`, listing what was written before the conflict.

New columns discovered late (JSONL key on line 900k, unmapped ES field) are
added the same way, typed from the first non-null value seen.

## Sources

### CSV (`crates/atp-core/src/source/csv_source.rs`)

- Delimiter sniffed from the first line among `, ; \t |`, quote-aware; override
  with `--delimiter`.
- First record is the header; empty names become `column_N`.
- Type inference over the first `--sample-rows` (default 10,000) records, which
  are buffered and replayed to the writer (bounded memory, single pass).
- `0123` stays text: leading zeros signal an identifier, not a number.
- `.gz` is decompressed transparently (`flate2`), via a 256 KiB buffered reader.

### JSONL (`.../jsonl.rs`)

- One JSON document per line; blank lines skipped; parse errors carry
  `path:line`.
- Nested objects flattened to dotted names; arrays kept as JSON text.
- Column order is first-seen order; late keys are appended.

### Elasticsearch (`.../es.rs`)

- Schema from `GET /{index}/_mapping`, merged across indices in the pattern.
  Type map: `long/integer/short/byte/unsigned_long → i64`,
  `float/double/half_float/scaled_float → f64`, `boolean → bool`,
  `date/date_nanos → timestamp_ms`, `object/nested → recurse`,
  everything else → `str`. Conflicts between indices resolve through the lattice.
- `GET /{index}/_count` and `GET /{index}/_stats/store` fill the estimates shown
  by `inspect`, `describe()` and the TUI progress bar. Nothing is invented: if
  Elasticsearch does not report a number, the UI shows an indeterminate bar.
- Scroll: initial `POST /{index}/_search?scroll=…` (sorted by `_doc`, optional
  `slice`), then `POST /_search/scroll`. **The initial page is used**, not
  discarded — a bug the monkey-patched stub test caught during development.
- Slices are walked sequentially in v0.1; each slice is a separate scrolled walk
  with its own context, which keeps context lifetimes short.
- An expired scroll context is a hard error naming the slice; silently restarting
  would duplicate or skip rows.
- **No crash-safe resume in v0.1.** A scroll checkpoint may only be advanced once
  every row it covers is durable, which requires a file barrier (flush the batch,
  close and fsync the partition files, then record `scroll_id`). Without that
  barrier a checkpoint advances past rows that live only in an unflushed
  ArrowWriter, so a crash would lose them while claiming progress. The barrier
  design, with an output-directory sequence offset so resumed runs never
  overwrite earlier files, is the v0.2 plan; until it ships the flag does not
  exist rather than existing unsafely.

## Sink

- Writer properties: zstd level 3 (default), 100,000 rows per row group,
  dictionary encoding, page statistics, `created_by = anything-to-parquet <ver>`.
- File naming: `<partition>/part-<segment:02>-<seq:05>.parquet`, where `segment`
  is the schema-segment index. Deterministic across reruns.
- Row-group and file limits: `--row-group-rows`, `--max-file-rows`. Open-file
  cap: `--max-open-files` (LRU close). Closing a file at the cap produces
  additional files in the same partition, never an unwritten row.
- Atomicity: write `<name>.parquet.tmp`, then `rename(2)`. A crash leaves at
  most `.tmp` files, which `verify` ignores and a rerun overwrites.
- Hashing: bytes flow through `HashingWriter` (xxh3-64) as Parquet writes them, so
  the digest describes exactly the file on disk, and no second pass is needed.

## Manifest

`_manifest.json` in the output root:

```json
{
  "tool": "anything-to-parquet 0.1.0",
  "run_id": "…",
  "status": "complete",
  "error": null,
  "started_at": "…", "finished_at": "…", "elapsed_ms": 8280,
  "source": { "kind": "csv", "location": "/tmp/big.csv", "rows_estimate": null,
              "bytes_estimate": 385342339, "notes": ["…"] },
  "options": { "compression": "zstd", "partition": "day", "row_group_rows": 100000,
               "strict": false, "timestamp_column": "@timestamp" },
  "schemas": [ { "fields": [ { "name": "…", "ty": "i64" } ] } ],
  "files": [ { "path": "dt=…/part-00-00000.parquet", "rows": 4320000,
               "bytes": 37131644, "digest": "…", "schema_hash": "…" } ],
  "total_rows": 5000000,
  "total_bytes": 42947091
}
```

`status` is one of `complete`, `cancelled`, `failed`. Completed files are listed
in all three cases, so a cancelled backfill can be inspected and re-run into a
fresh directory without guessing what exists.

`atp verify` recomputes, for every listed file, the xxh3-64 digest of the bytes,
the byte count, and the row count from the Parquet footer, then compares the sum
against `total_rows`. Any divergence is a problem and exits 4.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | general error (bad arguments to the source, io, …) |
| 2 | clap usage error |
| 3 | schema conflict / unparseable timestamp (strict mode, bad timestamp column) |
| 4 | verification mismatch |
| 5 | source/network error (Elasticsearch, …) |
| 130 | cancelled |

## Concurrency and memory

The engine is a single async task; sources are `async` so Elasticsearch I/O does
not block. Memory is bounded by: one page (≤ 8,192 rows), one `BatchBuilder`
buffer per schema segment, one Arrow row group per open partition, and
`max_open_files` × writer buffers. Measured: 41 MB peak RSS for 5M rows; 211 MB
for a 615-column JSONL schema.

No `unsafe`, no `unwrap`/`expect`/`panic` in library or binary code
(`clippy::unwrap_used`, `expect_used`, `panic` are denied workspace-wide; tests
opt out explicitly).

## Testing strategy

- Unit: lattice symmetry, coercion, schema hashing, flattening, CSV inference
  quirks (leading zeros), JSONL late keys, sink partitioning and file caps, TUI
  state transitions, scroll URL construction.
- Stub-server tests: `EsSource` runs against a scripted HTTP server implementing
  `_mapping`, `_count`, `_stats/store`, `_search`, `_search/scroll` and scroll
  deletion, including a URL-shape assertion (no `//_search`) added after a real
  Elasticsearch run rejected the malformed path that the stub had tolerated.
- Integration (`crates/atp-core/tests/convert.rs`): conversions are read back
  with an independent Parquet reader; row counts, column names, partition dirs,
  byte-identical reruns, strict/loose widening, `dt=_unknown`, `--max-rows`.
- CLI (`crates/atp/tests/cli.rs`): exit codes, JSON on stdout with progress on
  stderr, verify round-trip, missing manifest, strict failure.
- TUI: state machine unit tests plus `make tui-smoke`, which drives the real
  binary through a PTY (`script`), types a path, presses Enter and Ctrl-C, and
  asserts the rendered frames contain the expected screens.
