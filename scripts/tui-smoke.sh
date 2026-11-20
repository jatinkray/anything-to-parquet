#!/usr/bin/env bash
# Drive the real TUI through a PTY and assert it renders and exits cleanly.
#
#   scripts/tui-smoke.sh
#
# `script` gives the binary a pty; `stty` sizes it (a pty without a controlling
# terminal reports 0x0 and ratatui would render nothing). The script types a
# fixture path, presses Enter (inspect -> preview) and Ctrl-C (quit).
set -euo pipefail

bin=${ATP_BIN:-./target/release/atp}
if [[ ! -x "$bin" ]]; then
  cargo build --release >/dev/null
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fixture="$work/events.csv"
{
  echo "@timestamp,service,status,latency_ms"
  for i in $(seq 1 50); do
    printf '2026-11-2%dT0%d:00:00Z,svc-%d,%d,%d.5\n' \
      "$((1 + i % 2))" "$((i % 10))" "$((i % 3))" "$((200 + i))" "$i"
  done
} > "$fixture"

capture="$work/frames.txt"
(
  sleep 0.4
  printf '%s' "$fixture"
  sleep 1.2
  printf '\r'
  sleep 1.2
  printf '\003'
) | timeout 20 script -qec "stty rows 40 cols 120; $bin tui" /dev/null > "$capture" 2>&1
status=$?

if [[ $status -ne 0 ]]; then
  echo "tui exited with $status" >&2
  exit 1
fi

plain=$(sed 's/\x1b\[[0-9;?]*[a-zA-Z]//g' "$capture")
for needle in "anything-to-parquet" "source" "detected: csv" "schema"; do
  if ! grep -q -- "$needle" <<<"$plain"; then
    echo "missing expected TUI text: $needle" >&2
    echo "--- captured frames ---" >&2
    head -c 2000 "$plain" >&2
    exit 1
  fi
done
echo "tui smoke ok"
