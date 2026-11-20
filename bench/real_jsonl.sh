#!/usr/bin/env bash
# Convert a real public JSONL dataset (GitHub Archive, one hour) and report the
# ratio. The gzip checksum is pinned so a changed dataset is visible.
#
#   bench/real_jsonl.sh
set -euo pipefail

url=${URL:-https://data.gharchive.org/2015-01-01-0.json.gz}
sha_expected=${SHA:-a863f2aaf8e3c36fcfd120f7187f218e54a0f9630f16b6e41dc4f6ee903a5273}
bin=${ATP_BIN:-./target/release/atp}
work=${WORK:-$(mktemp -d)}

mkdir -p "$work"
file="$work/$(basename "$url")"
if [[ ! -f "$file" ]]; then
  curl -fsSL -o "$file" "$url"
fi
sha_actual=$(sha256sum "$file" | cut -d' ' -f1)
if [[ "$sha_actual" != "$sha_expected" ]]; then
  echo "dataset checksum changed: $sha_actual (expected $sha_expected)" >&2
fi
gz_bytes=$(stat -c %s "$file")
raw_bytes=$(gzip -dc "$file" | wc -c)

out="$work/out"
rm -rf "$out"
"$bin" convert "$file" --out "$out" --timestamp created_at --partition day \
  --sample-rows 20000 --quiet --json > "$work/summary.json"

python3 - "$work/summary.json" "$gz_bytes" "$raw_bytes" "$sha_actual" <<'PY'
import json, sys
summary = json.load(open(sys.argv[1]))
gz, raw, sha = (int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
print(json.dumps({
    "dataset": "gharchive 2015-01-01 hour 0",
    "rows_written": summary["rows"],
    "columns": summary["columns"],
    "gzip_bytes": gz,
    "json_bytes": raw,
    "parquet_bytes": summary["bytes"],
    "ratio_vs_json": round(raw / summary["bytes"], 2),
    "ratio_vs_gzip": round(gz / summary["bytes"], 2),
    "wall_seconds": round(summary["elapsed_ms"] / 1000, 2),
    "sha256": sha,
}, indent=2))
PY

rm -rf "$work"
