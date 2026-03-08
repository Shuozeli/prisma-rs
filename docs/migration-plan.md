# Prisma TypeScript to Rust Migration Plan

Last updated: 2026-03-07

## Overview

Migrate the Prisma ORM from TypeScript to Rust. The TypeScript implementation is a
46-package monorepo. The Rust implementation consolidates into a single Cargo workspace.

### Migration Scope

**Already in Rust** (in prisma-engines, to be consolidated):
- PSL parser (schema parsing, compiled to Wasm)
- Query compiler/planner (compiled to Wasm)
- Schema engine (migrations, native binary)

**To migrate** (currently TypeScript):
- Query interpreter / executor (`client-engine-runtime`)
- Client runtime (`client`)
- Code generators (`client-generator-ts`, `client-generator-js`)
- CLI (`cli`)
- Configuration loading (`config`)
- Driver adapters (`adapter-pg`, `adapter-mysql`, `adapter-libsql`, etc.)
- Driver adapter utilities (`driver-adapter-utils`)
- Migration CLI commands (`migrate`)
- Shared internals (`internals`, `engines`, `get-platform`, `fetch-engine`)

### Architecture Layers (bottom-up)

```
Layer 0: Schema Types & Config
Layer 1: PSL Parser (already Rust)
Layer 2: Query Compiler / Planner (already Rust)
Layer 3: Database Drivers (trait + impls)
Layer 4: Query Executor / Interpreter
Layer 5: Client Runtime (API surface)
Layer 6: Code Generators
Layer 7: CLI
Layer 8: Migration Engine (already Rust)
```

---

## Phase 0: Foundation -- Consolidate Existing Rust

**Priority:** P0 | **Status:** Not started

Bring the existing Rust components from `prisma-engines` into this workspace.

### 0.1: Import prisma-engines crates

- Import PSL parser crate
- Import query compiler crate
- Import schema engine crate
- Verify they build and pass existing tests in the new workspace layout

### 0.2: Define workspace structure

- Set up Cargo workspace with proper dependency graph
- Establish shared error types (`prisma-error`)
- Establish shared schema types (`prisma-schema`)
- Set up CI (cargo test, cargo clippy, cargo fmt)

### 0.3: Cross-compat test infrastructure

- Create test harness that can run queries through both TS and Rust paths
- Docker-based database provisioning (PostgreSQL, MySQL, SQLite, SQL Server)
- Golden test framework: schema + operations + expected output

**Exit criteria:** All existing prisma-engines tests pass in the new workspace.
Cross-compat test harness can execute a simple query through the TS path.

---

## Phase 1: Database Driver Layer

**Priority:** P0 | **Status:** Not started

Replace TypeScript driver adapters with Rust database drivers.

### 1.1: Driver trait definition

Define the core trait matching `SqlQueryable` / `SqlDriverAdapter` interfaces:
- `execute(sql, params) -> Result<ResultSet>`
- `query(sql, params) -> Result<ResultSet>`
- Transaction support (begin, commit, rollback, savepoints)
- Connection pooling interface
- `ConnectionInfo` (provider, server version, etc.)

### 1.2: PostgreSQL driver

- Use `tokio-postgres` or `sqlx` for the implementation
- Map PostgreSQL error codes to Prisma error types (P2xxx)
- Connection pooling (deadpool or bb8)
- TLS support
- Cross-compat: identical results vs `adapter-pg` for all query types

### 1.3: MySQL driver

- `sqlx::mysql` or `mysql_async`
- MySQL/MariaDB error code mapping
- Cross-compat vs `adapter-mariadb`

### 1.4: SQLite driver

- `rusqlite` or `sqlx::sqlite`
- Cross-compat vs `adapter-better-sqlite3`

### 1.5: SQL Server driver

- `tiberius` crate
- Cross-compat vs `adapter-mssql`

### 1.6: Error mapping

Port error mapping from TypeScript adapters:
- Each adapter's `errors.ts` -> Rust `MappedError` enum
- `rethrowAsUserFacing()` -> Rust equivalent producing P2xxx codes
- Match the `MappedError` kind taxonomy exactly

**Exit criteria:** All four drivers can execute raw SQL and return identical
`ResultSet` structures as the TypeScript adapters for the same queries.

---

## Phase 2: Query Executor

**Priority:** P0 | **Status:** Not started

Replace the TypeScript query interpreter with a Rust implementation.

### 2.1: Query plan execution

Port `QueryInterpreter` from `client-engine-runtime`:
- Execute query plans produced by the query compiler
- Map result sets back to Prisma response format
- Handle nested reads (joins, relation loading)

### 2.2: Transaction management

Port `TransactionManager`:
- Interactive transactions with ID tracking
- Nested transactions via savepoints
- Provider-specific savepoint SQL (PostgreSQL, MySQL, SQLite, SQL Server)
- Transaction timeout and cleanup

### 2.3: Batch operations

- Batch query execution (compacted queries)
- Transaction batching

### 2.4: Cross-compat validation

For each of the Prisma client operations:
- `findUnique`, `findFirst`, `findMany`
- `create`, `createMany`, `createManyAndReturn`
- `update`, `updateMany`, `updateManyAndReturn`
- `delete`, `deleteMany`
- `upsert`
- `aggregate`, `groupBy`, `count`
- `$executeRaw`, `$queryRaw`
- `$transaction` (sequential and interactive)

Verify identical results for each operation against all four databases.

**Exit criteria:** Full Prisma client operation coverage. Cross-compat tests pass
for all operations on all supported databases.

---

## Phase 3: Client Runtime & Code Generation

**Priority:** P1 | **Status:** Not started

### 3.1: Rust client library

Create a Rust Prisma client library:
- Type-safe query builders generated from schema
- Fluent API matching the TypeScript client surface
- Result types matching the schema model definitions

### 3.2: TypeScript client generator

Port `client-generator-ts`:
- Generate TypeScript client that calls into the Rust backend
- Same API surface as existing Prisma Client
- Generated types for models, enums, input types

### 3.3: Rust client generator

New: generate a native Rust Prisma client (no Wasm/FFI overhead):
- Derive-macro or build.rs based schema loading
- Compile-time validated queries
- Zero-cost abstractions over the query executor

**Exit criteria:** Generated TypeScript client produces identical API surface.
Rust client provides type-safe access to all Prisma operations.

---

## Phase 4: CLI

**Priority:** P1 | **Status:** Not started

### 4.1: Core CLI commands

Port from `packages/cli`:
- `prisma generate` -- invoke code generators
- `prisma validate` -- schema validation
- `prisma format` -- schema formatting
- `prisma db push` / `prisma db pull`
- `prisma studio` (or defer to separate tool)

### 4.2: Migrate commands

Consolidate with existing schema engine:
- `prisma migrate dev`
- `prisma migrate deploy`
- `prisma migrate reset`
- `prisma migrate resolve`
- `prisma migrate diff`
- `prisma db execute`

### 4.3: Config loading

Port `packages/config`:
- Load `prisma.config.ts` (or Rust equivalent config format)
- Datasource resolution
- Generator configuration

**Exit criteria:** All CLI commands produce identical behavior to the TypeScript CLI.

---

## Phase 5: Advanced Features & Polish

**Priority:** P2 | **Status:** Not started

### 5.1: Prisma Accelerate / Data Proxy

- Remote executor for edge deployments
- Connection pooling service compatibility

### 5.2: Middleware and extensions

- `$extends` API support
- Query middleware pipeline
- Result extensions and computed fields

### 5.3: SQL Commenter

Port sqlcommenter packages:
- Query tags
- Trace context propagation
- Query insights

### 5.4: Metrics and tracing

- OpenTelemetry integration
- Query logging
- Performance metrics

### 5.5: Additional driver adapters

- Neon (serverless PostgreSQL)
- PlanetScale (serverless MySQL)
- D1 (Cloudflare)
- Prisma Postgres (PPG)

---

## Execution Order

```
Phase 0 (consolidate)  -->  Phase 1 (drivers)  -->  Phase 2 (executor)
                                                 -->  Phase 3 (client + codegen)
                                                 -->  Phase 4 (CLI)
                                                 -->  Phase 5 (advanced features)
```

Phase 0 must complete first. Phases 1 and 2 are the critical path -- they
replace the performance-critical runtime. Phases 3-5 can proceed incrementally
once the executor is working.

---

## Verification Strategy

Following the flatbuffers migration pattern:

1. **Cross-compat tests:** Run identical Prisma schemas and operations through
   both the TypeScript and Rust implementations. Assert identical query results.
2. **Golden tests:** Capture expected SQL output and result sets. Detect
   regressions automatically.
3. **Integration tests:** Real databases via Docker. No mocked drivers for
   correctness validation.
4. **Benchmark suite:** Track query latency and throughput vs TypeScript baseline
   to quantify the migration benefit.

## Key Risks

| Risk | Mitigation |
|------|------------|
| prisma-engines internal APIs are undocumented | Read source, build incrementally, validate with cross-compat tests |
| TypeScript client API surface is large | Prioritize by usage frequency; cover core CRUD first |
| Driver behavior differences across DBs | Per-database integration test suites, match existing adapter error maps |
| Config format compatibility (prisma.config.ts) | Consider supporting both TS config (via deno_core/v8) and native Rust config |
| Wasm compilation target still needed for edge | Keep Wasm compilation support as a build target alongside native |
