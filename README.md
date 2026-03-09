# prisma-rs

A pure Rust implementation of the [Prisma](https://www.prisma.io/) ORM.
Drop-in replacement: same Prisma schema input, same client API behavior,
same query results, same migration workflow.

## Features

- Full Prisma schema parsing and code generation
- Query execution engine with filtering, sorting, pagination, and aggregation
- Database drivers: PostgreSQL, MySQL, SQLite, DuckDB, ADBC, Flight SQL
- Connection pooling (deadpool for PostgreSQL)
- Migration engine with dev/deploy/reset/resolve workflows
- CLI with `migrate`, `generate`, `db push`, `db pull`, `db execute` commands
- Cross-compatibility testing framework (Rust vs TypeScript output verification)
- Prisma Accelerate client support

## Installation

```bash
# Install directly from GitHub
cargo install --git https://github.com/Shuozeli/prisma-rs.git prisma-cli

# Verify installation
prisma --help
```

This installs the `prisma` binary to `~/.cargo/bin/`.

To install a specific branch or commit:

```bash
# From a specific branch
cargo install --git https://github.com/Shuozeli/prisma-rs.git --branch main prisma-cli

# From a specific commit
cargo install --git https://github.com/Shuozeli/prisma-rs.git --rev <commit-sha> prisma-cli
```

To update to the latest version:

```bash
cargo install --git https://github.com/Shuozeli/prisma-rs.git prisma-cli --force
```

## Quick Start

```bash
# Install
cargo install --git https://github.com/Shuozeli/prisma-rs.git prisma-cli

# Validate a schema
prisma validate --schema schema.prisma

# Generate client code
prisma generate --schema schema.prisma

# Push schema to database
export DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
prisma db push --schema schema.prisma

# Run migrations
prisma migrate dev --schema schema.prisma
```

## Building from Source

```bash
# Clone and build
git clone https://github.com/Shuozeli/prisma-rs.git
cd prisma-rs
cargo build --workspace

# Run tests
cargo test --workspace

# Run the CLI from source
cargo run -p prisma-cli -- --help
```

## Architecture

```
driver-core/        Database driver trait and common types (DriverError, ResultSet, etc.)
driver-pg/          PostgreSQL driver (tokio-postgres + deadpool connection pooling)
driver-mysql/       MySQL driver (mysql_async)
driver-sqlite/      SQLite driver (rusqlite, bundled)
driver-duckdb/      DuckDB driver (via ADBC)
driver-adbc/        Arrow Database Connectivity driver
driver-flightsql/   Arrow Flight SQL driver
prisma-schema/      Schema types mirroring the Prisma schema AST
prisma-compiler/    Query planning and compilation
prisma-migrate/     Migration engine (RPC bridge to schema engine)
prisma-error/       Shared error types
cross-compat/       Cross-compatibility tests (Rust vs TypeScript)
query-executor/     In-memory query execution (filter, sort, paginate, aggregate)
prisma-client/      Client runtime (Accelerate integration, transactions)
prisma-codegen/     Client code generation
prisma-cli/         CLI binary
```

Dependency chain:

```
driver-core  <--  driver-pg, driver-mysql, driver-sqlite, driver-duckdb, driver-adbc, driver-flightsql
     |
     v
query-executor  -->  prisma-compiler  -->  prisma-schema
     |
     v
prisma-client  -->  prisma-codegen
     |
     v
prisma-cli  -->  prisma-migrate  -->  prisma-error
```

## Database Support

| Database | Driver | Connection Pooling | Status |
|----------|--------|--------------------|--------|
| PostgreSQL | `tokio-postgres` | `deadpool-postgres` | Implemented |
| MySQL | `mysql_async` | Built-in | Implemented |
| SQLite | `rusqlite` (bundled) | N/A | Implemented |
| DuckDB | ADBC | N/A | Implemented |

## CLI Commands

| Command | Description |
|---------|-------------|
| `generate` | Generate client code from Prisma schema |
| `validate` | Validate a Prisma schema file |
| `format` | Format a Prisma schema file |
| `db push` | Push schema changes to the database |
| `db pull` | Pull schema from existing database |
| `db execute` | Execute raw SQL against the database |
| `migrate dev` | Create and apply migrations (development) |
| `migrate deploy` | Apply pending migrations (production) |
| `migrate reset` | Reset database and re-apply all migrations |
| `migrate resolve` | Resolve migration issues |
| `migrate diff` | Show diff between migration states |

## Testing

```bash
# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p query-executor
cargo test -p prisma-codegen

# Regenerate golden files after intentional output changes
UPDATE_GOLDEN=1 cargo test --workspace
```

## Minimum Supported Rust Version

Rust 1.85+ (edition 2024)

## License

Apache-2.0
