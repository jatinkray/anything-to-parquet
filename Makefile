.PHONY: fmt test lint build release bench bench-jsonl tui-smoke docs-check ci

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
