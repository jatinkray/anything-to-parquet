# Prometheus / Thanos TSDB blocks

`atp` reads Prometheus and Thanos blocks directly — index format v1 and v2, XOR /
XOR2 / histogram chunk encodings, tombstones and Thanos external labels — and
writes them as Parquet rows. This capability is merged from
[`tsdb2parqtel`](https://github.com/jatinkray/tsdb-parquet-convert); the reader is
vendored in `crates/atp-tsdb` (see its `PROVENANCE.md`).

```bash
# A single block, a directory of blocks, or an object-store prefix.
atp inspect /var/lib/prometheus/blocks/01G0P9P6T0VSA5Z484BRGH2N68 --limit 5
atp convert /var/lib/prometheus/blocks --out ./metrics \
  --partition day --tsdb-shape series
atp verify ./metrics

# One metric family, last week, labels as columns.
atp convert s3://thanos/prod --out ./metrics \
  --tsdb-match '{__name__=~"http_.*"}' \
  --tsdb-from 2026-11-14T00:00:00Z --tsdb-to 2026-11-21T00:00:00Z \
  --tsdb-shape wide --partition hour
```

## What a row is

One row per sample, always with the same four key columns:

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | timestamp (ms, UTC) | sample time |
| `metric` | string | `__name__` label |
| `value` | float64 | sample value (stale markers dropped by default) |
| `series_fingerprint` | string (16 hex) | xxh3-64 of the label set, byte-identical to `tsdb2parqtel` |

`--tsdb-shape` controls how labels are carried:

| Shape | Extra columns | Use when |
|---|---|---|
| `series` (default) | none | You only need time, metric, value and series identity |
| `wide` | one string column per label name, sorted; null when absent | You want `GROUP BY job`/`pod` in SQL. Real blocks can expose 200+ label names, most null per row |
| `long` | `labels`: one JSON object per row | Full fidelity; unnest or `json_extract` downstream |

The fingerprint lets a dataset converted by either tool be reconciled by series:
`atp` writes it, and `tsdb2parqtel verify` recomputes it from the source block.

## Options

| Flag | Default | Meaning |
|---|---|---|
| `--tsdb-shape` | `series` | `series`, `wide` or `long` |
| `--tsdb-from` / `--tsdb-to` | whole block | Sample window, RFC 3339 or Unix seconds; `to` is exclusive |
| `--tsdb-match` | none | Series selector, repeatable: `job="api"` or `{job="api",env!="dev"}`; regex with `=~` |
| `--tsdb-block` | all blocks | Restrict to block ULIDs, repeatable |
| `--tsdb-stale` | `drop` | `keep` to write Prometheus stale NaN markers as NaN |
| `--tsdb-on-error` | `fail` | `skip` to omit unreadable series/blocks and continue |
| `--tsdb-external-labels` | `include` | `ignore` to drop Thanos external labels from rows |
| `--tsdb-downsample` | `prefer-raw` | `skip`, `prefer-raw`, `include`, `only` |
| `--tsdb-dedup-replica-labels` | none | Comma-separated label names (e.g. `replica`) merged across HA pairs |

The source is detected automatically for a directory containing `meta.json` (or
one level of block directories) and for `s3://`, `gs://`, `az://` prefixes.
`--format tsdb` forces it.

## How it stays bounded

The reader walks one series at a time; the adapter hands records to the engine
through a two-slot channel, so memory is bounded by one series' samples plus one
conversion batch — not by the block set or the number of series. A block with
5.5 million samples converts in 3.8 s with ~90 MB peak RSS
(see [BENCHMARKS.md](BENCHMARKS.md)).

## Behaviour worth knowing

- **Series without `__name__` are skipped**, matching `tsdb2parqtel`. Rows are
  only meaningful with a metric name.
- **Native histogram chunks cannot be represented** (a histogram is not one
  float). They fail the run; `--tsdb-on-error skip` omits those series and the
  run completes. The manifest's `source.notes` and the log say what was skipped.
- **Tombstones are applied**; deleted samples never reach Parquet.
- **Stale markers** (the reserved NaN payload Prometheus writes when a series
  disappears) are dropped by default so `value` stays a clean float column.
- **Remote prefixes are staged**: `s3://`/`gs://`/`az://` blocks are downloaded to
  a temporary directory before conversion, using the standard provider
  credential chains. Large prefixes therefore need local disk equal to the block
  set. Streaming directly from object storage is a roadmap item.
- **One run, one output directory.** File names are deterministic; use a fresh
  `--out` per run.
- **No per-series verification yet.** `atp verify` proves the Parquet matches the
  manifest (row counts, xxh3 digests). Comparing samples back against the source
  block is still `tsdb2parqtel verify`'s job; the shared `series_fingerprint`
  makes that possible.

## Downsamples and Thanos

`prefer-raw` keeps raw blocks and skips downsampled duplicates; `only` converts
downsampled blocks (useful for cheap long-range history); `include` converts
both. External labels are folded into every row by default, so a Thanos sidecar's
`cluster`/`replica` labels survive the trip.

## Reconciliation with tsdb2parqtel

| Aspect | `tsdb2parqtel` | `atp` (this repository) |
|---|---|---|
| Output | Parqtel metric blocks or schema-mode Parquet | Plain Parquet, Hive `dt=`/`hour=` partitions, manifest |
| Schema | User-supplied YAML schema or presets | Fixed TSDB shapes (`series`/`wide`/`long`) |
| Rollups / compaction | Yes (`--rollup`, `compact`) | No (roadmap) |
| Per-series verification against source | Yes (`verify`) | Not yet (roadmap) |
| Other sources | Prometheus/Thanos only | CSV, JSONL, Elasticsearch too |

Both tools write the same `series_fingerprint`, so a side-by-side comparison is
one join.
