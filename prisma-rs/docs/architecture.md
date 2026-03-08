# Architecture

## Overview

prisma-rs is a 16-crate Cargo workspace implementing the Prisma ORM in Rust.
The architecture follows a layered design: database drivers at the bottom,
query execution in the middle, and the CLI/client at the top.

## Crate Dependency Graph

```
                          prisma-cli
                         /     |     \
                        /      |      \
              prisma-client  prisma-migrate  prisma-codegen
                    |            |               |
               query-executor   |          prisma-schema
                /    |    \     |
               /     |     \   |
          driver-pg  |  driver-sqlite
                     |
               driver-mysql
                     |
                driver-core  <-- driver-duckdb, driver-adbc, driver-flightsql
                     |
                prisma-error
```

## Layer Description

### Layer 1: Database Drivers

The driver layer provides a unified `DatabaseDriver` trait that abstracts
database-specific behavior. Each driver translates between the database's
native wire protocol and Prisma's internal `ResultSet` / `PrismaValue` types.

- **driver-core**: Defines the `DatabaseDriver` trait, `ResultSet`, `PrismaValue`,
  `DriverError`, and `MappedError`. All drivers depend on this crate.
- **driver-pg**: PostgreSQL via `tokio-postgres` with `deadpool-postgres` pooling
  and `rustls` TLS.
- **driver-mysql**: MySQL via `mysql_async` with built-in pooling.
- **driver-sqlite**: SQLite via `rusqlite` with bundled SQLite (no system dependency).
- **driver-duckdb**: DuckDB via the ADBC layer.
- **driver-adbc**: Arrow Database Connectivity, converts Arrow columnar data to rows.
- **driver-flightsql**: Arrow Flight SQL for remote database access over gRPC.

### Layer 2: Schema and Compilation

- **prisma-schema**: Defines the schema AST types (`DataModel`, `Model`, `Field`,
  `Relation`, `Enum`). This is the canonical representation of a Prisma schema.
- **prisma-compiler**: Compiles Prisma Client operations into query plans.
- **prisma-error**: Shared error types across all crates.

### Layer 3: Query Execution

- **query-executor**: Executes compiled query plans against a database driver.
  Implements in-memory operations: filtering (where clauses), sorting (orderBy),
  pagination (skip/take/cursor), aggregation (count/sum/avg/min/max/groupBy),
  relation traversal (include/select), and mutations (create/update/upsert/delete).

### Layer 4: Client and CLI

- **prisma-client**: High-level client runtime. Integrates with Prisma Accelerate
  (HTTP-based query forwarding) and supports interactive transactions.
- **prisma-codegen**: Generates typed client code from a Prisma schema. Builds
  a schema IR and validates identifiers and field types.
- **prisma-migrate**: RPC bridge to the Prisma schema engine for migration
  operations (dev, deploy, reset, resolve, diff).
- **prisma-cli**: CLI binary exposing all commands: `generate`, `validate`,
  `format`, `db push`, `db pull`, `db execute`, `migrate dev/deploy/reset/resolve/diff`.

### Cross-Cutting

- **cross-compat**: Test framework that runs identical queries through both the
  Rust and TypeScript Prisma implementations and asserts identical results.

## Error Handling

Errors flow upward through the layers using `thiserror`-derived types:

1. **Driver errors** (`DriverError`): Wraps database-specific errors with a
   `MappedError` variant that classifies the error (unique constraint violation,
   null constraint violation, foreign key violation, auth failure, etc.).
2. **Query errors**: Execution-level errors from invalid operations.
3. **Schema errors**: Parsing and validation errors from malformed schemas.
4. **CLI errors**: User-facing errors with actionable messages.

All production paths return `Result` types. Panics are prohibited.

## Connection Management

- PostgreSQL uses `deadpool-postgres` for async connection pooling with
  configurable pool size, timeouts, and TLS settings.
- MySQL uses `mysql_async`'s built-in connection pool.
- SQLite uses a single connection (file-based, no pooling needed).

## Security

- Path traversal prevention: All file paths validated against the working directory.
- Database URL scheme validation: Only known schemes accepted.
- Migration name validation: Rejects path separators and `..` sequences.
- Pagination bounds: Skip/take capped at 100,000 to prevent DoS.
- Array parameter limits: PostgreSQL IN-clause arrays capped at 32,768.
- Error body truncation: External API error responses truncated to prevent
  memory exhaustion.
