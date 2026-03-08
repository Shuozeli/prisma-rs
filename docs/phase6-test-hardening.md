# Phase 6: Test Coverage Hardening & Integration Testing

Last updated: 2026-03-07

## Goal

Close the test coverage gap between our Rust implementation and the original
Prisma TS codebase. Focus on porting high-value unit tests and building
end-to-end integration tests against real databases.

**Progress: 632 tests (up from 309 at start of phase 6)**

---

## Track 1: Unit Test Coverage Hardening

| # | Task | Source (TS) | Status |
|---|------|-------------|--------|
| 6.1 | Error rendering/mapping tests | client: 94 tests (errorRendering/) | Done (+29 tests) |
| 6.2 | SQL commenter test coverage | sqlcommenter-*: 276 tests | Done (+21 tests) |
| 6.3 | Transaction manager edge cases | client-engine-runtime: 26 tests | Done (+11 tests) |
| 6.4 | JSON protocol serde tests | client: 60 tests (jsonProtocol/) | Done (+43 tests) |
| 6.5 | Query parameterization tests | client: 102 tests (parameterization/) | Done (+108 tests) |
| 6.6 | Driver adapter error mapping | adapter-pg: 19 + adapter-ppg: 25 | Done (+40 tests) |

## Track 2: Integration Testing (Real Databases)

| # | Task | Databases | Status |
|---|------|-----------|--------|
| 6.7 | End-to-end: schema -> codegen -> execute -> verify | SQLite (in-memory) | Done (+9 tests) |
| 6.8 | Cross-database operation matrix | SQLite (in-memory) | Done (+2 tests) |
| 6.9 | Transaction integration tests | SQLite (in-memory) | Done (+1 test) |
| 6.10 | Raw query integration tests | SQLite (in-memory) | Done (+8 tests) |

## Track 3: Missing Operations & Cross-Database E2E

| # | Task | Databases | Status |
|---|------|-----------|--------|
| 6.11 | Batch $transaction tests (compile+execute in tx) | SQLite | Done (+5 tests) |
| 6.12 | Cross-provider golden tests for missing operations | All (compile-only) | Done (+27 tests) |
| 6.13 | PG end-to-end: compile -> execute -> verify | PostgreSQL (Docker) | Done (+18 tests) |
| 6.14 | MySQL end-to-end: compile -> execute -> verify | MySQL (Docker) | Done (+15 tests, 3 ignored) |

Note: PG/MySQL tests require Docker (docker-compose.yml: PG on 15432, MySQL on 13306).
MySQL `updateOne`/`upsertOne` update path and transaction rollback for compiled queries
are known limitations (MySQL lacks RETURNING; scope resolution for follow-up SELECT needs work).

---

## Task Details

### 6.1 Error Rendering/Mapping Tests

Port error rendering tests from `packages/client/src/runtime/core/errorRendering/`.
TS has 94 tests covering:
- mutuallyExclusiveFields
- includeOnScalar
- EmptySelection
- UnknownSelectionField, InvalidSelectionValue
- UnknownArgument, UnknownInputField
- RequiredArgumentMissing
- InvalidArgumentType, InvalidArgumentValue
- ValueTooLarge
- Union
- SomeFieldsMissing, TooManyFieldsGiven

Target crate: `prisma-error` or `prisma-client`

### 6.2 SQL Commenter Test Coverage

Expand from 9 tests toward coverage of TS's 276 tests across 3 packages:
- `sqlcommenter-query-tags` (18 tests): withQueryTags, withMergedQueryTags, multiple plugins
- `sqlcommenter-trace-context` (12 tests): traceparent parsing, plugin factory, edge cases
- `sqlcommenter-query-insights` (180 tests): parameterized query shapes, base64 encoding,
  filter operators, nested queries, data context, edge cases

Target crate: `driver-core` (sql_commenter module)

### 6.3 Transaction Manager Edge Cases

Port from `packages/client-engine-runtime/src/transaction-manager/`:
- Interactive transaction lifecycle (begin/query/commit/rollback)
- Savepoint syntax per provider (PG: ROLLBACK TO SAVEPOINT, MySQL/SQLite: ROLLBACK TO,
  SQL Server: ROLLBACK TRANSACTION)
- Nested transactions (savepoint within savepoint)
- Transaction timeout handling
- Rollback on error
- Concurrent transaction conflicts

Target crate: `query-executor`

### 6.4 JSON Protocol Serialization/Deserialization

Port from `packages/client/src/runtime/core/jsonProtocol/`:
- deserializeJsonObject round-tripping
- All scalar types: Int, BigInt, Float, Decimal, String, Boolean, DateTime, Json, Bytes, Uuid
- Null handling
- Nested objects and arrays
- Edge cases (empty objects, missing fields)

Target crate: `prisma-client` or `query-executor`

### 6.5 Query Parameterization Tests

Port from `packages/client/src/runtime/core/engines/client/parameterization/`:
- classifyValue (null, primitives, arrays, tagged scalars, structural, plain objects)
- parameterizeQuery (basic, filter operators, null handling, structural values)
- Placeholder naming and reuse
- Batch parameterization
- Cache key consistency
- Nested queries
- Tagged values and enum membership

Target crate: `prisma-client` or `prisma-compiler`

### 6.6 Driver Adapter Error Mapping

Add per-driver error mapping tests:
- **PostgreSQL** (19 mappings from TS adapter-pg): unique violation (23505),
  foreign key (23503), null constraint (23502), table not found (42P01),
  column not found (42703), syntax error (42601), etc.
- **MySQL**: duplicate entry (1062), foreign key (1452), table not found (1146),
  column not found (1054), access denied (1045), etc.
- **SQLite**: constraint violations (UNIQUE, FOREIGN KEY, NOT NULL),
  busy database, table not found, etc.
- **Type conversion/null handling** (25 tests from TS adapter-ppg):
  normalize_timestamp, normalize_money, normalize_bool, convertBytes, null in arrays

Target crates: `driver-pg`, `driver-mysql`, `driver-sqlite`

### 6.7 End-to-End Integration Tests

Full pipeline test:
1. Parse a Prisma schema
2. Run codegen (Rust client)
3. Apply schema to database (db push)
4. Execute queries via generated client
5. Verify results

Cover: CRUD, relations, filtering, ordering, pagination, aggregations.

Target crate: new `integration-tests` crate or extend `cross-compat`

### 6.8 Cross-Database Operation Matrix

Verify identical behavior across PG/MySQL/SQLite for:
- findMany with where/orderBy/skip/take
- findUnique with compound keys
- createMany
- updateMany with filters
- deleteMany with filters
- upsert
- aggregate (count/sum/avg/min/max)
- groupBy
- Nested creates/connects/disconnects

Target crate: `cross-compat`

### 6.9 Transaction Integration Tests

Against real databases:
- Sequential transactions (multiple ops in one tx)
- Interactive transactions (begin/query/commit)
- Nested transactions with savepoints
- Explicit rollback
- Error-triggered rollback
- Isolation levels (where supported)
- Concurrent transaction conflicts

Target crate: `cross-compat` or `query-executor` (integration tests)

### 6.10 Raw Query Integration Tests

Against real databases:
- $queryRaw with typed results
- $executeRaw returning affected row counts
- Parameterized raw queries with all scalar types
- Raw queries inside transactions
- Provider-specific SQL syntax
- Error handling for invalid SQL

Target crate: `cross-compat` or `query-executor`
