#!/usr/bin/env bash
# Reproducible CSV conversion benchmark.
#
#   ROWS=5000000 bench/run.sh
#
# Prints one JSON object with the inputs and the measured result. Uses
# /usr/bin/time for wall clock and peak RSS; falls back to `time` when GNU time
# is unavailable (RSS then reported as null).
set -euo pipefail

rows=${ROWS:-5000000}
work=${WORK:-$(mktemp -d)}
bin=${ATP_BIN:-./target/release/atp}
keep=${KEEP:-0}

if [[ ! -x "$bin" ]]; then
  echo "building release binary" >&2
  cargo build --release >/dev/null
fi

mkdir -p "$work"
input="$work/in.csv"
python3 "$(dirname "$0")/gen_csv.py" "$rows" "$input"
input_bytes=$(stat -c %s "$input")
input_sha=$(sha256sum "$input" | cut -d' ' -f1)

gzip -6 -c "$input" > "$work/in.csv.gz"
gzip_bytes=$(stat -c %s "$work/in.csv.gz")

out="$work/out"
rm -rf "$out"

wall=null
rss=null
if command -v /usr/bin/time >/dev/null 2>&1; then
  timing="$work/time.txt"
  /usr/bin/time -f '%e %M' -o "$timing" \
    "$bin" convert "$input" --out "$out" --timestamp @timestamp --partition day \
    --quiet --json > "$work/summary.json"
  read -r wall rss < "$timing"
else
  start=$(date +%s%N)
  "$bin" convert "$input" --out "$out" --timestamp @timestamp --partition day \
    --quiet --json > "$work/summary.json"
  end=$(date +%s%N)
  wall=$(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.3f", (b-a)/1e9}')
fi

python3 - "$work/summary.json" "$input_bytes" "$gzip_bytes" "$input_sha" "$rows" \
  "$wall" "$rss" <<'PY'
import json, os, sys
summary_path, input_bytes, gzip_bytes, sha, rows, wall, rss = sys.argv[1:8]
summary = json.load(open(summary_path))
out_bytes = summary["bytes"]
print(json.dumps({
    "rows_requested": int(rows),
    "rows_written": summary["rows"],
    "files": summary["files"],
    "input_bytes": int(input_bytes),
    "gzip6_bytes": int(gzip_bytes),
    "parquet_bytes": out_bytes,
    "ratio_vs_input": round(int(input_bytes) / out_bytes, 2),
    "ratio_vs_gzip6": round(int(gzip_bytes) / out_bytes, 2),
    "wall_seconds": float(wall),
    "rows_per_second": int(summary["rows"] / float(wall)),
    "peak_rss_kb": None if rss == "null" else int(rss),
    "input_sha256": sha,
    "schema_hashes": summary["schema_hashes"],
    "compression": "zstd-3",
}, indent=2))
PY

if [[ "$keep" != "1" ]]; then
  rm -rf "$work"
else
  echo "kept work dir: $work" >&2
fi
