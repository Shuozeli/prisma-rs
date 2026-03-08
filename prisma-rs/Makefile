.PHONY: test test-unit test-integration check fmt clippy

# Run all tests
test:
	cargo test

# Run only unit tests (no database required)
test-unit:
	cargo test -p prisma-driver-core -p prisma-schema -p prisma-compiler \
		-p prisma-driver-sqlite --lib \
		-p prisma-driver-pg --lib \
		-p prisma-driver-mysql --lib

# Run integration tests (requires Docker: make docker-up)
test-integration:
	cargo test -p prisma-driver-pg --test integration
	cargo test -p prisma-driver-mysql --test integration
	cargo test -p prisma-driver-sqlite --test integration

# Format check
fmt:
	cargo fmt --all -- --check

# Clippy lint
clippy:
	cargo clippy --all-targets

# Full CI check
check: fmt clippy test

# Start test databases
docker-up:
	docker compose up -d --wait

# Stop test databases
docker-down:
	docker compose down
