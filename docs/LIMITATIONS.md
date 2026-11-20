# Limitations (v0.1)

Honest list. Anything here can be a future change; nothing here is hidden behind a
flag that silently does something else.

## Sources

- **Local files and Elasticsearch only.** No `s3://`/`gs://`/`az://` input or
  output yet. Writing to object storage needs multipart, retries and a
  different atomicity story; it is deliberately not faked.
- **No SQLite, MySQL, Postgres, Kafka, S3 log buckets.** Listed as roadmap in
  `atp sources`. Each new source must pass the same conformance tests (schema,
  pagination and cancellation) before it ships.
- **CSV needs a header.** Headerless files are rejected rather than guessed.
- **CSV inference samples the first `--sample-rows` rows** (default 10,000).
  Later rows can widen a column (and that is recorded per file), but the first
  files may have a narrower type than the last.
- **gzip only.** No zstd/xz/bzip2 input; `.gz` is transparent.
- **No stdin.** `atp -` is not supported; use a named pipe if you must.

## JSON shape

- Nested objects become dotted columns; **arrays become JSON text**, not Parquet
  `LIST`. That is lossless but not queryable as a list.
- Very heterogeneous JSONL produces very wide tables: a GitHub Archive hour
  produced **615 columns**, most null for any given event type. Correct, but
  wasteful for analytics; pre-flatten or split by event type if that matters.
- Field names are used verbatim; there is no sanitising pass for names that
  conflict after flattening (`a.b` as a literal key vs `a: {b}`).
- Map/struct columns are flattened; Parquet `MAP`/`STRUCT` are not emitted.

## Elasticsearch

- **Scroll API only** (no PIT/search_after). Works against 7.x–9.x and OpenSearch.
- **Slices run sequentially.** No parallel fetching, so a single large index is
  bounded by one connection.
- **Auth is basic or API key.** No OIDC/SAML/cloud-signed requests.
- **Dynamic fields are discovered per page.** If a field appears only after
  millions of documents, earlier files lack it (schema segments record this).
- **No crash-safe resume, and scroll cannot support one.** Rows must be durable
  before a checkpoint moves, *and* the scroll cursor advances when the request is
  served, not when the page is written — so a crash loses the in-flight page even
  with a file barrier (measured: 20 of 50,000 rows missing, none duplicated).
  v0.1 ships without the flag. Use `--query` time windows and one output directory
  per window for large backfills; the PIT + `search_after` design is in
  `docs/DESIGN.md`.
- **An expired scroll context aborts the run** with the slice number; the error
  is not retried automatically.
- **No `_id`, `_index`, `_score` columns** unless they appear in `_source`.
- **Nested documents are not un-nested** into separate rows (one row per
  `_source`, arrays as JSON text).
- **No delta/incremental sync.** Each run is an independent query; there is no
  "everything since last run" tracking.

## Output

- **Local filesystem only.** No Iceberg/Delta commit, no catalog. Files are
  Hive-style partitioned so `read_parquet(..., hive_partitioning=true)` works.
- **Partitioning is by one timestamp column**, `day` or `hour`. No multi-column
  partitioning, no bucketing.
- **`dt=_unknown`** collects rows whose timestamp is null. They are not dropped;
  they are also not silently assigned a date.
- **One schema per file, several schemas per run.** Consumers must union schemas
  across files (DuckDB, Athena and Spark do this automatically). A consumer that
  assumes one schema for the whole prefix will be surprised by the manifest's
  `schemas[]`.
- **`--max-file-rows` and the open-file cap can produce more, smaller files**
  than a perfect planner would. No compaction command yet.
- **One run per output directory.** Names are deterministic (`part-<segment>-<seq>`),
  so a second run into the same directory replaces same-named files and rewrites
  `_manifest.json`. Use a fresh `--out` per run when the inputs differ.
- **No encryption at rest / column-level redaction.** PII handling is the
  operator's job; a redaction pass is a candidate feature, not present.

## Operations

- **Cancellation is cooperative**: it takes effect after the current page/batch.
  Ctrl-C in the TUI sets the flag; SIGINT handling in the CLI is not wired (the
  process exits and leaves completed files plus, possibly, `.tmp` files).
- **No progress output for the final in-flight bytes**: bytes count when a file
  closes or a row group flushes, so progress can look flat for the first 100k
  rows of a partition.
- **No retry/resume for any source.** Rerun from scratch; the manifest and hashes
  let you tell whether the previous run was complete.
- **No backpressure to Elasticsearch beyond page size.** Very small pages and a
  high slice count will hammer a cluster; use `--page-size`.
- **Windows** is compiled in CI but only Linux/macOS are exercised by the TUI
  smoke test.

## Correctness caveats

- **Floating point** columns are written as IEEE-754 doubles; integers wider than
  `i64` (unsigned 64-bit) degrade to text.
- **Timestamps without a zone** are interpreted as UTC; the ES default format is
  accepted, but a naive local-time string is not.
- **Digits with leading zeros stay text**, so `count` columns that are
  zero-padded (`007`) stay text rather than becoming `7`.
- **`--strict` is about widening, not about schema quality**: a column that is
  `str` from the first sample is accepted forever, even if it looks numeric.
