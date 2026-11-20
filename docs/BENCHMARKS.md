# Benchmarks

All numbers below were produced by scripts in `bench/` on the machine described
here, at the commit that introduced this file. Re-run them; if your numbers
differ, treat yours as the truth for your hardware.

Machine: AMD Ryzen 7 5800H, 16 threads, 46 GB RAM, Linux 7.2.15, local NVMe.
Binary: `cargo build --release` (default release profile: thin LTO, 4 CGUs).
Codec: zstd level 3, 100,000-row row groups.

## Synthetic CSV — 5,000,000 rows, 8 columns

Command: `ROWS=5000000 bench/run.sh`

| Metric | Value |
|---|---|
| Rows written | 5,000,000 |
| Output files | 2 (day partitions: 4.32M + 0.68M rows) |
| Input CSV | 385,342,339 B |
| Input SHA-256 | `d4476ae371225f0fefdd9a2bcd31cf53d462e2631174b9a559d565b3656ec005` |
| `gzip -6` of the input | 88,970,893 B |
| Parquet (zstd 3) | 42,947,091 B |
| Ratio vs raw CSV | **8.97×** |
| Ratio vs `gzip -6` | **2.07×** |
| Wall clock | 7.83 s |
| Throughput | **638,000 rows/s** (49 MB/s of CSV) |
| Peak RSS | **40,924 KiB** |

Peak RSS is flat in the number of rows because the engine streams 8,192-row
batches: memory is a function of row width, not of input size.

Run-to-run spread on this machine is roughly 7.8–8.6 s for the same binary and
input (CPU frequency and page cache); treat the throughput as ~600k rows/s, not
as a single fixed number. The quoted run is the one after the per-cell coercion
fix (each value is coerced once and moved, not cloned twice).

Reproduce and verify:

```bash
ROWS=5000000 bench/run.sh          # prints the same JSON block
atp verify ./out                    # 5,000,000 rows, digests match
```

## Real JSONL — GitHub Archive, 2015-01-01 hour 0

Command: `bench/real_jsonl.sh` (downloads `https://data.gharchive.org/2015-01-01-0.json.gz`)

| Metric | Value |
|---|---|
| Rows | 7,702 events |
| Columns after flattening | **615** (heterogeneous event payloads) |
| gzip input | 2,744,253 B |
| Decompressed JSON | 18,023,797 B |
| Parquet (zstd 3) | 3,411,197 B |
| Ratio vs raw JSON | 5.28× |
| Ratio vs gzip | **0.80×** (Parquet is 1.24× *larger* than gzip here) |
| Wall clock | 0.54 s |
| gzip SHA-256 | `a863f2aaf8e3c36fcfd120f7187f218e54a0f9630f16b6e41dc4f6ee903a5273` |

The gzip comparison is deliberately not flattering. This dataset is the worst
case for a columnar file: 615 columns of which any single row populates a
handful, so Parquet pays per-column-chunk overhead and zstd has little
per-column repetition to exploit, while gzip sees one long text stream. Parquet
still wins where it matters — you can read one column, filter on statistics, and
run SQL over it — but it does not always win on bytes. If a workload only needs
an archive, `gzip` is a legitimate answer; if it needs analysis, it is not.

An Elasticsearch index of the same events would be larger than the JSON text
because it stores postings, doc values and `_source` for search. That comparison
is not measured here and is not claimed.

## Real Elasticsearch verification (correctness, not throughput)

Run on 2026-11-21 against Elasticsearch **9.1.0** in Docker (single node, no
security), to check the scroll path against a real server rather than the
scripted stub.

Fixtures and results:

| Check | Result |
|---|---|
| 20,000 documents bulk-indexed into `logs-2026` with an explicit mapping (`date`, `keyword`, `integer`, `long`, `double`, `boolean`, `text`, nested `http` object), nulls every 1,000th doc, `service` missing every 777th | `_count` = 20,000 |
| `atp convert http://localhost:19200/logs-2026 --out … --timestamp @timestamp --partition day --slices 4 --page-size 500` | 20,000 rows, 1 file, `atp verify` ok |
| Types derived from the mapping | `@timestamp` timestamp_ms, `http.status` i64, `http.bytes` i64, `latency_ms` f64, `ok` bool, `service`/`message`/`http.path`/`tags` str |
| Filtered run with `--query '{"range":{"@timestamp":{"gte":"2026-11-21T00:30:00Z"}}}'` | `atp` 12,800 rows; Elasticsearch `_count` with the same query 12,800 |
| Multi-valued `keyword` (`tags`) | preserved as JSON text, matching the documented array behaviour |

Two defects were found by this run and fixed before release:

1. **Double slash in the search URL.** The initial request went to
   `/logs-2026//_search`, which Elasticsearch rejects with 400; the stub had
   accepted it. Fixed, and the stub test now asserts the URL shape.
2. **Resume could not be made safe at the time.** A scroll checkpoint advanced
   after each page, but rows only become durable when a Parquet file is closed,
   so a crash would lose rows while claiming progress. The flag was removed
   instead of shipped broken; the barrier design is written down in
   `docs/DESIGN.md` and listed in `docs/LIMITATIONS.md`.

## What is *not* measured

- Elasticsearch scroll throughput (depends on cluster, shard count, network and
  `--page-size`; adding a fake-server benchmark would measure the fake server).
- Object storage upload throughput (not implemented).
- Concurrency: the engine converts in one task; there is no parallel scan yet.
- Compaction: the sink bounds file size; it does not merge existing files.

## Reproducing on your machine

```bash
cargo build --release
ROWS=5000000 bench/run.sh
bench/real_jsonl.sh
```

Both scripts print JSON and clean up after themselves (`KEEP=1` / `WORK=…` to
keep artifacts).
