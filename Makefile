.PHONY: fmt clippy test nextest check deny release verify-spec verify-migrations ci

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test --all-features

nextest:
	cargo nextest run --all-features

check:
	cargo check --all-targets --all-features

deny:
	cargo deny check

release:
	cargo build --release --locked

verify-spec:
	./scripts/verify-spec-digests.sh

verify-migrations:
	./scripts/verify-migration-checksums.sh

ci: fmt clippy test deny release verify-spec verify-migrations

