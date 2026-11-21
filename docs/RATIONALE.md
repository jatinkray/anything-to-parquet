# Why anything-to-parquet

This document is the evidence behind the product: what already exists, what the
platform vendors say about Parquet in object storage, what it costs, and which
specific gaps `atp` fills. Every claim is either cited (URL + date fetched) or
produced by a script in this repository.

Research date: **2026-11-21**. Prices and repository statistics move; re-check
them before quoting this document in public.

## 1. The destination format is no longer contested

Parquet in object storage is the interchange format of the analytics stack:
DuckDB, Polars, Athena, BigQuery, Snowflake, Spark, Trino, ClickHouse and the
lakehouse table formats (Iceberg, Delta) all read it, and every serious
observability product now stores telemetry that way.

Most telling is that Elasticsearch — the system most of this data currently
lives in — now *reads* Parquet from external storage:

> "ES|QL Data Federation reads Parquet, newline-delimited JSON (NDJSON),
> comma-separated values (CSV), and tab-separated values (TSV) files from
> external storage."
> — Elastic docs, *File formats for ES|QL Data Federation*; applies to
> Elastic Stack 9.5 (experimental) and Elastic Cloud Serverless (preview).
> https://www.elastic.co/docs/reference/query-languages/esql/esql-data-federation-file-formats

That page specifies Parquet schema discovery from file metadata and the internal
codecs Elastic can read (`UNCOMPRESSED`, `SNAPPY`, `ZSTD`, `GZIP`, `LZ4_RAW`).
It does not describe any export path. Elastic has become a Parquet consumer
while remaining a closed producer.

Independent corroboration that the archive format is Parquet-in-S3:

| Vendor | Statement | Source |
|---|---|---|
| Datadog | The Observability Pipelines Amazon S3 destination "send[s] logs in JSON or **Parquet** format to Amazon S3" | https://docs.datadoghq.com/observability_pipelines/destinations/amazon_s3 |
| Datadog | Log Archives keep logs in the customer's bucket in a "storage-optimized archive format", queryable in place by Archive Search | https://docs.datadoghq.com/logs/log_configuration/archives |
| OpenObserve | Converts ingested data to Parquet in object storage (S3, GCS, MinIO, RustFS, Azure Blob) and queries it with DataFusion | https://openobserve.ai/docs/architecture |
| Parseable | Positions "S3-native log analytics" as object storage + Apache Parquet at "$0.023/GB/month" | https://www.parseable.com/blog/s3-native-log-analytics-future |

Marketing claims from vendors about savings (for example OpenObserve's "1/140th
the storage") are **not** repeated here: they are unverifiable from outside.
The measured numbers in [BENCHMARKS.md](BENCHMARKS.md) are what this project
stands behind.

## 2. The gap: getting existing data out

### 2.1 Elasticsearch cannot export its own indices to a columnar file

- **Serverless removed the snapshot escape hatch.** Elastic's own comparison
  table lists Snapshot/restore as "✅ User-managed" for Elastic Cloud Hosted and
  "Managed by Elastic / **Planned** for user-initiated snapshots" for Serverless,
  and "Elasticsearch for Apache Hadoop … ❌ Not available in Serverless".
  https://www.elastic.co/docs/deploy-manage/deploy/elastic-cloud/differences-from-other-elasticsearch-offerings
  On Serverless, therefore, there is no snapshot to hand to a Parquet writer and
  no ES-Hadoop/Spark path. What is left is scroll, PIT and ES|QL.
- **Kibana CSV export is explicitly for "moderate amounts of data"**, and CSV is
  not columnar or typed.
  https://www.elastic.co/docs/explore-analyze/report-and-share/reporting-troubleshooting-csv
- **ES|QL is a reader.** Data Federation reads external Parquet; the issue
  tracker in `elastic/elasticsearch` shows work on reading external Parquet
  (schema resolution, statistics pushdown, `STATS` performance) — not writing it.
  Example open issues observed 2026-11-21: #158077 ("query latency scales
  linearly with file count due to per-file schema resolution"), #161909
  ("STATS on external Parquet is up to 2.6x slower since #160937").

So the only sanctioned bulk egress is programmatic paging, and the only
sanctioned columnar format is a read-only dependency.

### 2.2 What the ecosystem offers instead

Repository statistics below come from the GitHub API (`gh api repos/<r>`), read
on 2026-11-21; "last push" is the field `pushed_at`.

| Tool | Stars | Last push | What it is | Why it does not close the gap |
|---|---:|---|---|---|
| `elasticsearch-dump/elasticsearch-dump` | 7,947 | 2026-11-01 | JS exporter/importer | JSON lines only; no Parquet, no schema, no partitions |
| `vectordotdev/vector` | 22,922 | 2026-11-20 | Streaming agent | Converts events that flow through it. Parquet encoding for the `aws_s3` sink landed only on 2026-06-01 (issue #1374, open since 2019-12-14). No backfill of an existing index |
| `duckdb/duckdb` | 43,211 | 2026-11-20 | Analytical engine | `COPY … TO (FORMAT parquet)` is the standard answer for CSV/JSON, but requires SQL and a running engine |
| `tlinhart/duckdb-elasticsearch` | 23 | 2026-04-29 | DuckDB community extension | Real ES reader (scroll, filter/limit pushdown). Requires SQL; materialises through DuckDB; no partition layout, manifest or verification |
| `domoritz/arrow-tools` | 210 | 2026-10-01 | `csv2parquet`, `json2parquet` | Single file in, single file out; no partitions, manifest or ES |
| `domoritz/csv2parquet` (standalone) | 77 | 2023-02-02 | Rust CSV→Parquet | **Archived** |
| `cldellow/csv2parquet` | 65 | 2022-12-08 | Python CSV→Parquet | Abandoned in practice |
| `rayyildiz/cc2p` | 7 | 2026-11-04 | Rust CSV→Parquet with a TUI | CSV only, folder-level, no manifest/verify/partitions |
| `bisoftbilgi/elastic-to-parquet` | 1 | 2023-09-08 | Python ES→Parquet script | The only dedicated ES→Parquet tool found; unmaintained, GPL-3.0, no releases |
| `YelpArchive/aws_logs_to_parquet_converter` | 11 | 2023-03-24 | Spark batch job | Requires a Spark cluster |
| `jatinkray/tsdb-parquet-convert` (`tsdb2parqtel`) | — | 2026-11-21 | Prometheus/Thanos → Parquet/Parqtel | Excellent for metrics, single-purpose; **merged into this tool** as the `tsdb` source |

Search method: `gh search repos` over `parquet converter`, `elasticsearch
parquet`, `parquet export logs`, `ndjson parquet`, `sqlite parquet export`,
`csv parquet tui`, plus targeted repository lookups. The TUI hits
(`tabiew` 3.2k stars, `datasight`, `tabler`) are all **viewers**; none writes
Parquet, so a conversion TUI is not a crowded space.

### 2.3 The gap in one sentence

There are many ways to *read* Parquet and several single-format *converters*, but
no maintained tool that takes an operational source (ES index, CSV dump, JSONL
export, Prometheus block), streams it with bounded memory, writes lake-shaped
Parquet (partitions + zstd + row-group limits), and emits a manifest that lets you
prove afterwards what you got. The metrics half of that sentence already existed
in `tsdb2parqtel`; it is now a source in this tool rather than a separate binary.

## 3. Economics, honestly

It is tempting to argue "S3 is cheaper than Elasticsearch storage". Don't:
Elastic's Serverless retention list price is in the same band as S3 Standard.

Elastic Cloud Serverless Observability list prices, read 2026-11-21 from
https://www.elastic.co/pricing/serverless-observability (the page shows two
tiers, both prefixed "as low as"):

| Dimension | Standard tier | Complete tier |
|---|---|---|
| Ingest, logs/traces | $0.07/GB | $0.09/GB |
| Ingest, metrics | $0.07/GB | $0.023/GB |
| Retention, logs/traces | $0.017/GB-month | $0.019/GB-month |
| Retention, metrics | $0.017/GB-month | $0.005/GB-month |
| Egress | 50 GB free, then $0.05/GB | same |

Amazon S3 Standard storage is $0.023/GB-month for the first 50 TB
(https://aws.amazon.com/s3/pricing/; the page injects prices client-side, the
same figure is quoted independently by Parseable's S3-native article above).

The defensible arguments are therefore about **footprint, portability and
compute**, not the sticker price per raw gigabyte:

1. **Footprint.** Retention is billed on stored bytes. The same events as
   Parquet+zstd occupy far fewer bytes than a search-indexed copy (postings, doc
   values, stored `_source`, replicas). Measured in this repo: a 5,000,000-row,
   385 MB CSV becomes 43 MB of Parquet (8.97×), and 89.0 MB with `gzip -6`
   (2.07× larger than Parquet). Elasticsearch's indexed footprint is larger than
   the raw text, so the effective ratio only improves — but that comparison is
   not measured here and is deliberately not claimed.
2. **Portability.** A Parquet file is readable by every engine, forever, without
   a vendor's snapshot format. Elastic itself now reads it through ES|QL Data
   Federation.
3. **Compute.** Cold data queried through DuckDB/Athena/Trino costs query time,
   not a cluster sized for peak ingest.
4. **No exit tax.** Egress from a SaaS is $0.05/GB and up; S3 GETs are $0.0004
   per 1,000 requests.

## 4. Who needs this

| Persona | Trigger | Today's workaround |
|---|---|---|
| Platform/observability engineer on Elastic Cloud Serverless | 12–24 months of logs to retain, no snapshot API, finance asks why the retention bill grows | Scroll scripts in Python, Kibana CSV for small slices |
| Data engineer landing operational data | Vendor CSV dumps and service JSONL need to be queryable next to warehouse data | Spark job or a one-file-at-a-time `csv2parquet`, then a home-grown manifest |
| SRE doing a cross-system postmortem | "Pull last week's logs into DuckDB next to metrics" | Stand up a pipeline; or the DuckDB ES extension plus SQL |
| Cost/compliance owner | Prove the archive is complete | No tool emits row counts + hashes for an export |

Common constraint: they want a *file*, not a platform. Every server-side option
(Logstash, Vector, Datadog pipelines) converts only what flows through it from
now on; backfill is the missing half.

### 4.1 Demand signals

Counts fetched 2026-11-21; treat them as order-of-magnitude, not precise.

Stack Overflow (`search/advanced`, all terms must match, `filter=total`):

| Query | Questions |
|---|---:|
| `csv to parquet` | 1,337 |
| `export parquet` | 268 |
| `elasticsearch parquet` | 30 |
| `jsonl parquet` | 13 |

GitHub issue search (`search/issues`):

| Query | Issues |
|---|---:|
| `repo:elastic/elasticsearch parquet` | 708 |
| `repo:elastic/elasticsearch parquet export` | 21 |
| `repo:opensearch-project/OpenSearch parquet` | 505 |

Reading: CSV demand is an order of magnitude larger than any single vendor
question, which is why CSV/JSONL ships in v0.1 alongside Elasticsearch. The
Elasticsearch counts are inflated by Elastic's own internal Parquet work (below),
so they overstate end-user questions; the 21 "parquet export" issues are the
closest signal and they are all engineering traffic, not a product.

### 4.2 The closest thing to a platform threat, and why it is not the same tool

`elastic/elasticsearch#166095` — **"Add Parquet write engine prototype"**, opened
2026-11-16, still open on 2026-11-21, zero comments at the time of reading:

> "This is a prototype. It is a draft to show the design and to get early
> feedback. It is not ready to merge." … "A new x-pack plugin,
> `x-pack/plugin/parquet-write-engine`. An index with `index.storage_format:
> parquet` gets an engine that stores its documents only as Parquet files in a
> snapshot repository (`fs` or `s3`). The files are listed as an Apache Iceberg
> table … The engine implements `Engine#indexBatch`. It needs the `batch_indexing`
> feature flag, which is off in release builds. The index has no refresh and no
> search."

Its own constraints: columnar index mode only, no replicas, no indexed fields, no
multi-fields, no `date_nanos`; field types limited to long/integer/short/byte/
date/keyword/double/float/boolean; `multi_value: true` becomes a Parquet `LIST`.

Why this does not make the tool unnecessary:

1. It is **write-side for indices created with a Parquet storage format**, not an
   export path for data already indexed in Lucene.
2. It is a **storage engine behind an off-by-default feature flag**, with no
   refresh and no search — not something an operator adopts for existing data.
3. It is **Elasticsearch-only**; CSV/JSONL dumps, other stores, and estates
   leaving Elasticsearch get nothing.
4. It has **no manifest/verify** story for a backfill and no `dt=` partition
   layout (it relies on Iceberg commits instead).

What it does establish is direction: Elastic is building Parquet + Iceberg as a
first-class storage shape. That increases the amount of data in Parquet and makes
"get my history out" a routine request rather than an exotic one.

## 5. Positioning against the realistic alternatives

| Alternative | Where it wins | Where `atp` wins |
|---|---|---|
| DuckDB + `elasticsearch` extension | SQL users, ad-hoc joins | No SQL required; per-window partitions; manifest + verify; single binary |
| `elasticdump` + DuckDB | Re-import workflows | One step; typed columns from the ES mapping; no intermediate JSON |
| Vector | Continuous streaming | Backfill of existing data; lake layout; verification |
| Spark / ES-Hadoop | Very large estates with an existing cluster | No cluster; unavailable on Serverless anyway; bounded memory on a laptop |
| `csv2parquet`-class tools | One file, one shot | Partitions, manifest, schema evolution, multiple sources |

## 6. Risks to this thesis

| Risk | Signal to watch | Response |
|---|---|---|
| Elastic ships first-party Parquet export | Prototype for *new* indexed data exists (issue #166095, flagged off, not an exporter) | Ship the export path for existing data and non-ES sources; keep the engine source-agnostic |
| Iceberg becomes the required commit protocol | Elastic's prototype writes Iceberg tables | Keep Hive-style partitions (readable by every engine); treat an Iceberg commit as an optional later output |
| DuckDB extension becomes "good enough" | Adoption of the extension, issue traffic | Document the SQL path; differentiate on verification, layout and multi-source support |
| "Anything" scope creep dilutes quality | Issues asking for 10 new sources | New sources are plugins behind one trait with a conformance test |
| Compression claims challenged | Anyone re-running `bench/` | Every number reproducible from a script with a pinned dataset hash |

## 7. What would make this document wrong

- Elastic publishing a supported `_export` to Parquet with partitioning.
- A maintained, lake-aware ES→Parquet CLI appearing with more traction than this
  project.
- Parquet losing to a successor format for lakehouse storage (Iceberg v3 with
  Vortex/Lance is the thing to watch; Iceberg still defaults to Parquet today).

Re-check this document at each release; it is dated for a reason.
