.PHONY: fmt test lint build release bench bench-jsonl tui-smoke live-s3 docs-check ci

fmt:
	cargo fmt --all -- --check

test:
	cargo test --workspace --locked

lint:
	cargo clippy --workspace --all-targets --locked -- -D warnings
	cargo fmt --all -- --check

build:
	cargo build --locked

release:
	cargo build --release --locked

bench:
	ROWS=$${ROWS:-5000000} ./bench/run.sh

bench-jsonl:
	./bench/real_jsonl.sh

tui-smoke:
	bash scripts/tui-smoke.sh

# Real bucket, real credentials. Reads aws.env when present; never runs in CI.
live-s3:
	@test -n "$$ATP_LIVE_S3_BUCKET" || (echo "ATP_LIVE_S3_BUCKET is not set" >&2; exit 2)
	set -a; if [ -f aws.env ]; then . ./aws.env; fi; set +a; \
	prefix="$${ATP_LIVE_S3_PREFIX:-atp-live}/$$(date +%s)"; \
	ATP_LIVE_S3=1 ATP_LIVE_S3_PREFIX="$$prefix" \
		cargo test -p anything-to-parquet --test live_s3 -- --test-threads=1; \
	status=$$?; \
	aws s3 rm --recursive "s3://$$ATP_LIVE_S3_BUCKET/$$prefix/" >/dev/null 2>&1 || true; \
	exit $$status

docs-check:
	@test -f README.md
	@test -f docs/RATIONALE.md
	@test -f docs/DESIGN.md
	@test -f docs/LIMITATIONS.md
	@test -f docs/BENCHMARKS.md
	@test -f docs/TSDB.md
	@test -f docs/assets/logo.svg
	@test -f docs/assets/hero.svg
	@test -f LICENSE
	@echo "documentation files present"

ci: lint test docs-check
