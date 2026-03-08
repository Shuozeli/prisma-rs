# Prisma TS-to-Rust Migration Phases

Last updated: 2026-03-08

## Current State

Cargo workspace set up under `prisma-rs/` with 20 crates. Phases 0-7 done.

| Phase | Name                        | Priority | Status      | Tests |
|-------|-----------------------------|----------|-------------|-------|
| 0     | Foundation & Consolidation  | P0       | Done        | 38    |
| 1     | Database Driver Layer       | P0       | Done        | 69    |
| 2     | Query Executor              | P0       | Done        | 34    |
| 3     | Client Runtime & Codegen    | P1       | Done        | 70    |
| 4     | CLI                         | P1       | Done        | 33    |
| 5     | Advanced Features & Polish  | P2       | Done        | 65    |
| 6     | Test Coverage Hardening     | P1       | Done        | 323   |
| 7     | DuckDB, ADBC & Flight SQL   | P2       | Done        | 73    |
| **Total** |                         |          |             | **705**|

---

## Phase 0: Foundation & Consolidation

Import existing Rust crates from `prisma-engines`, set up Cargo workspace,
establish cross-compat test infrastructure.

| # | Task | Status |
|---|------|--------|
| 0.1 | Import PSL parser crate | Done (`prisma-schema` wraps `prisma-fmt` + `psl` via git dep) |
| 0.2 | Import query compiler crate | Done (`prisma-compiler` wraps `query-compiler` via git dep) |
| 0.3 | Import schema engine crate | Done (`prisma-migrate` wraps `schema-core` via git dep) |
| 0.4 | Cargo workspace + dependency graph | Done (6 crates, git deps to prisma-engines@94a226b) |
| 0.5 | Shared error types (`prisma-error`) | Done (bridges driver-core <-> user-facing-errors) |
| 0.6 | Shared schema types (`prisma-schema`) | Done (re-exports `psl` crate) |
| 0.7 | CI setup (test, clippy, fmt) | Done (GitHub Actions + Makefile) |
| 0.8 | Cross-compat test harness | Done (`cross-compat` crate, 4 unit + 18 golden tests) |
| 0.9 | Docker-based DB provisioning | Done (docker-compose.yml with PG + MySQL) |
| 0.10 | Golden test framework | Done (insta snapshots for 6 operations x 3 providers) |

**Exit criteria:** All prisma-engines tests pass. Cross-compat harness runs a
simple query through the TS path.

---

## Phase 1: Database Driver Layer

Replace TypeScript driver adapters with native Rust database drivers.
See [`phase1-database-drivers.md`](phase1-database-drivers.md) for full details.

| # | Task | Status |
|---|------|--------|
| 1.1 | Core types & traits (SqlQuery, SqlResultSet, ColumnType) | Done |
| 1.2 | Trait definitions (SqlQueryable, Transaction, SqlDriverAdapter) | Done |
| 1.3 | Error types (MappedError, DriverError, P2xxx mapping) | Done |
| 1.4 | PostgreSQL driver (tokio-postgres + deadpool) | Done |
| 1.5 | MySQL/MariaDB driver (mysql_async) | Done |
| 1.6 | SQLite driver (rusqlite + spawn_blocking) | Done |
| 1.7 | Cross-compat test suite (3 DBs via Docker) | Done (all 3 DBs have integration tests) |

Done: DuckDB (`duckdb`), ADBC (`adbc_core`), Arrow Flight SQL (`arrow-flight`).
Future: CockroachDB (uses PG wire protocol, should work with `driver-pg`).

**Exit criteria:** All three drivers execute raw SQL and return identical results
as the TypeScript adapters.

---

## Phase 2: Query Executor

Replace the TypeScript `QueryInterpreter` with a Rust implementation.

| # | Task | Status |
|---|------|--------|
| 2.1 | Query plan execution | Done (`query-executor` crate, 15 unit + 4 integration tests) |
| 2.2 | Transaction management | Done (commit/rollback via SqlDriverAdapter) |
| 2.3 | Nested transactions (savepoints) | Done (create/rollback/release savepoint) |
| 2.4 | Batch operations | Done (multi-insert + findMany verification) |
| 2.5 | CRUD cross-compat (find/create/update/delete) | Done (findMany/findUnique/findFirst/createOne/updateMany/deleteMany + relations) |
| 2.6 | Aggregation cross-compat (aggregate/groupBy/count) | Done (aggregate count via query executor) |
| 2.7 | Raw query cross-compat ($executeRaw/$queryRaw) | Done (executeRaw/queryRaw via driver adapter) |
| 2.8 | Transaction cross-compat (sequential + interactive) | Done (sequential ops + interactive multi-op + nested savepoints) |

**Exit criteria:** Full operation coverage. Cross-compat tests pass on all
supported databases.

---

## Phase 3: Client Runtime & Code Generation

| # | Task | Status |
|---|------|--------|
| 3.1 | Rust client library (query builders) | Done (`prisma-client` crate, 28 tests) |
| 3.2 | TypeScript client generator | Done (`prisma-codegen` TS generator, 6 tests) |
| 3.3 | Rust client generator | Done (`prisma-codegen` Rust generator, 8 tests) |
| 3.4 | Schema IR (language-neutral intermediate representation) | Done (`prisma-codegen` schema_ir, 5 tests) |
| 3.5 | Generated type parity with TS client | Done (model types, input types, delegates, enums) |

**Exit criteria:** Generated TS client has identical API surface. Rust client
provides type-safe access to all operations.

---

## Phase 4: CLI

| # | Task | Status |
|---|------|--------|
| 4.1 | `prisma generate` | Done (TS + Rust codegen, --language flag) |
| 4.2 | `prisma validate` | Done (via prisma-fmt validation) |
| 4.3 | `prisma format` | Done (format + --check mode) |
| 4.4 | `prisma db push` / `prisma db pull` | Done (schema_push + introspect via schema engine) |
| 4.5 | `prisma migrate dev/deploy/reset/resolve/diff` | Done (full migration lifecycle) |
| 4.6 | `prisma db execute` | Done (--stdin and --file modes) |
| 4.7 | Config loading (schema + URL resolution) | Done (--schema flag, --url flag, DATABASE_URL env) |

**Exit criteria:** All CLI commands produce identical behavior to the TS CLI.

---

## Phase 5: Advanced Features & Polish

| # | Task | Status |
|---|------|--------|
| 5.1 | Prisma Accelerate / Data Proxy | Done (`AccelerateClient`, cache strategy, 5 tests) |
| 5.2 | `$extends` API + middleware pipeline | Done (middleware pipeline, result extensions, 10 tests) |
| 5.3 | SQL Commenter (query tags, trace context) | Done (`SqlComment` builder, OTel traceparent, 9 tests) |
| 5.4 | OpenTelemetry + query logging | Done (`tracing` spans, `QueryEvent`, `LogConfig`, 5 tests) |
| 5.5 | Neon adapter (serverless PostgreSQL) | Done (`driver-neon` crate, HTTP API, 9 tests) |
| 5.6 | PlanetScale adapter (serverless MySQL) | Done (`driver-planetscale` crate, HTTP API, 12 tests) |
| 5.7 | D1 adapter (Cloudflare) | Done (`driver-d1` crate, HTTP API, 10 tests) |
| 5.8 | Prisma Postgres adapter | Done (`driver-prisma-postgres` crate, HTTP API, 5 tests) |

**Exit criteria:** Feature parity with the TypeScript implementation.

---

## Execution Order

```
Phase 0 (foundation)
  |
  v
Phase 1 (drivers) -----> Phase 2 (executor)
                            |
                            v
                    Phase 3 (client + codegen)
                            |
                            v
                    Phase 4 (CLI)
                            |
                            v
                    Phase 5 (advanced)
```

---

## Phase 6: Test Coverage Hardening

See [`phase6-test-hardening.md`](phase6-test-hardening.md) for full details.

| # | Task | Status |
|---|------|--------|
| 6.1 | Error rendering/mapping tests | Done (+29 tests) |
| 6.2 | SQL commenter test coverage | Done (+21 tests) |
| 6.3 | Transaction manager edge cases | Done (+11 tests) |
| 6.4 | JSON protocol serde tests | Done (+43 tests) |
| 6.5 | Query parameterization tests | Done (+108 tests) |
| 6.6 | Driver adapter error mapping | Done (+40 tests) |
| 6.7 | E2E: schema -> codegen -> execute -> verify (SQLite) | Done (+9 tests) |
| 6.8 | Cross-database operation matrix (SQLite) | Done (+2 tests) |
| 6.9 | Transaction integration tests (SQLite) | Done (+1 test) |
| 6.10 | Raw query integration tests (SQLite) | Done (+8 tests) |
| 6.11 | Batch $transaction tests (SQLite) | Done (+5 tests) |
| 6.12 | Cross-provider golden tests | Done (+27 tests) |
| 6.13 | PG end-to-end integration tests | Done (+18 tests) |
| 6.14 | MySQL end-to-end integration tests | Done (+15 tests, 3 ignored) |

**Exit criteria:** Test coverage gap significantly closed. 632 total tests.

---

## Phase 7: DuckDB, ADBC & Arrow Flight SQL

| # | Task | Status |
|---|------|--------|
| 7.1 | Fix 3 ignored MySQL tests (updateOne, upsertOne, transaction_rollback) | Done (scope placeholder resolution + transaction nesting) |
| 7.2 | DuckDB driver (`driver-duckdb`) | Done (20 tests: adapter, conversion, error) |
| 7.3 | ADBC driver (`driver-adbc`) | Done (20 tests: 12 arrow conversion + 8 DuckDB integration) |
| 7.4 | Arrow Flight SQL driver (`driver-flightsql`) | Done (4 tests: factory, error mapping) |
| 7.5 | Shared Arrow conversion layer | Done (`driver-adbc::arrow` module, reused by Flight SQL) |
| 7.6 | `Provider::DuckDb` support across crates | Done (driver-core, prisma-client, prisma-error) |

**Key fixes:**
- `Expression::Transaction` nesting: Added `is_transaction()` to `SqlQueryable` trait, skip nested tx when already in one
- `InitializeRecord` scope resolution: Pass `scope` to `initialize_record`/`apply_field_operations` to resolve `Placeholder` values
- DuckDB `column_type()` requires execution: Use `Rows::as_ref()` to access metadata post-execution

**Exit criteria:** DuckDB works as embedded test database. ADBC and Flight SQL adapters functional. 705 total tests.

---

See `docs/migration-plan.md` for detailed sub-task descriptions, design
decisions, verification strategy, and risk analysis.
