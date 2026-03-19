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
               /         \                       |
       prisma-compiler  query-executor      prisma-schema
            |    \          |
         prisma-ir  \   prisma-driver-core  <--+--+--+--+--+
            |    prisma-engines   ^             |  |  |  |  |
            |    (git deps)      |         driver-pg  |  |  |  |
            |                    |     driver-mysql  |  |  |
            |                    |    driver-sqlite  |  |
            |                    |   driver-duckdb   |
            |                    |   driver-adbc <-- driver-flightsql
            |                    |
          prisma-error ----------+
```

Notes:
- `prisma-error` depends on `prisma-driver-core` (not the reverse)
- `query-executor` depends on `prisma-ir` and `prisma-driver-core` (no prisma-engines dependency)
- `prisma-compiler` depends on prisma-engines crates (git) and `prisma-ir`
- `prisma-client` depends on `prisma-compiler`, `query-executor`, and `prisma-driver-core`
- `driver-flightsql` depends on `driver-adbc` for Arrow type conversion

## Layer Description

### Layer 1: Database Drivers

The driver layer provides a unified trait hierarchy (`SqlQueryable`, `SqlDriverAdapter`,
`SqlDriverAdapterFactory`) that abstracts database-specific behavior. Each driver
translates between the database's native wire protocol and Prisma's internal
`SqlResultSet` / `QueryValue` types.

- **driver-core**: Defines the `SqlQueryable`, `Transaction`, `SqlDriverAdapter`,
  `SqlDriverAdapterFactory` traits, plus `SqlResultSet`, `QueryValue`, `DriverError`,
  `MappedError`, `DatabaseUrl`, `SafeMessage`, and `StaticSql`. All drivers depend
  on this crate.
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

All production execution paths return `Result` types. Panics are reserved for
programmer errors and unsupported configurations in utility/test code.

## Connection Management

- PostgreSQL uses `deadpool-postgres` for async connection pooling with
  configurable pool size, timeouts, and TLS settings.
- MySQL uses `mysql_async`'s built-in connection pool.
- SQLite uses a single connection (file-based, no pooling needed).

## Security

- Path traversal prevention: All file paths validated against the working directory.
- Database URL scheme validation: Only known schemes accepted.
- Migration name validation: Rejects path separators and `..` sequences.
- Bind parameter limits: Each provider enforces a maximum bind parameter count
  per query (PostgreSQL: 32,766, MySQL: 65,535, SQLite: 999) via
  `Provider::max_bind_values()`.
- Error body truncation: External API error responses truncated to prevent
  memory exhaustion.

## Query Execution Flow

```
1. Client builds query
   PrismaClient.query("User", FindMany) + Selection

2. Compiler produces IR
   JSON request -> prisma-compiler -> prisma_ir::Expression tree

3. Executor interprets IR
   Expression tree -> query-executor walks the tree:
     - Let bindings set up scope variables
     - Query/Execute nodes run SQL via the driver
     - Seq nodes run expressions in order
     - If nodes evaluate DataRules on results
     - Join nodes perform application-level relation joining
     - DataMap nodes shape results via ResultNode rules
     - Process nodes apply in-memory pagination, distinct, and reverse

4. SQL rendering
   DbQuery (template) -> render_query() -> SqlQuery (concrete SQL + bound params)

5. Driver execution
   SqlQuery -> DatabaseDriver.query() -> ResultSet (rows + columns)

6. Result shaping
   ResultSet -> ResultNode rules -> IValue (JSON-like output)
```

## Serialization Boundary

The `prisma-ir` crate creates a clean separation between compilation and execution:

```
prisma-engines types  -->  JSON  -->  prisma-ir types
   (prisma-compiler)    roundtrip    (query-executor)
```

This means the `query-executor` crate has zero dependency on prisma-engines.
The only contract is the JSON serialization format, which is tested via
cross-compat snapshot tests.

## Component Documentation

Detailed documentation for each crate is in [`docs/components/`](components/):

| Crate | Purpose |
|-------|---------|
| [driver-core](components/driver-core.md) | SqlQueryable/SqlDriverAdapter traits and shared types |
| [driver-pg](components/driver-pg.md) | PostgreSQL driver (tokio-postgres + deadpool) |
| [driver-mysql](components/driver-mysql.md) | MySQL driver (mysql_async) |
| [driver-sqlite](components/driver-sqlite.md) | SQLite driver (rusqlite bundled) |
| [driver-duckdb](components/driver-duckdb.md) | DuckDB driver (bundled duckdb crate) |
| [driver-adbc](components/driver-adbc.md) | ADBC protocol driver (Arrow-based) |
| [driver-flightsql](components/driver-flightsql.md) | Arrow Flight SQL driver (gRPC) |
| [prisma-schema](components/prisma-schema.md) | Schema parsing, validation, DMMF |
| [prisma-compiler](components/prisma-compiler.md) | Query compiler (wraps prisma-engines) |
| [prisma-ir](components/prisma-ir.md) | Owned IR types for query plans |
| [query-executor](components/query-executor.md) | Query plan interpreter and executor |
| [prisma-client](components/prisma-client.md) | Client runtime (PrismaClient, middleware, Accelerate) |
| [prisma-codegen](components/prisma-codegen.md) | TypeScript and Rust code generator |
| [prisma-cli](components/prisma-cli.md) | CLI binary (validate, generate, migrate, db) |
| [prisma-error](components/prisma-error.md) | Error type bridge (driver <-> prisma-engines) |
| [prisma-migrate](components/prisma-migrate.md) | Schema migration engine (RPC bridge) |
| [cross-compat](components/cross-compat.md) | Cross-compatibility testing harness |

Last updated: 2026-03-19
