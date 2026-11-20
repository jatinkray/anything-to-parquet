#!/usr/bin/env python3
"""Generate a deterministic log-shaped CSV for the conversion benchmark.

The shape is deliberately boring (timestamp, service, host, status, latency,
bytes, user id, path) because the point is to measure the converter, not to win
a compression contest with a degenerate dataset.

Usage: gen_csv.py <rows> <out.csv>
"""

import datetime
import random
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    rows = int(sys.argv[1])
    out = sys.argv[2]
    rng = random.Random(7)
    base = datetime.datetime(2026, 11, 21, tzinfo=datetime.timezone.utc)
    statuses = (200, 200, 200, 404, 500)
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write("@timestamp,service,host,status,latency_ms,bytes,user_id,path\n")
        write = f.write
        for i in range(rows):
            ts = (base + datetime.timedelta(seconds=i // 50)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            write(
                f"{ts},svc-{i % 40},host-{i % 500},{rng.choice(statuses)},"
                f"{rng.randint(1, 900)}.{i % 10},{rng.randint(100, 90000)},"
                f"u{i % 100000},/api/v1/items/{i % 1000}\n"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
