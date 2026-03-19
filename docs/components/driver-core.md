# driver-core

Database driver trait and common types shared by all database adapters.

## Purpose

Defines the core trait hierarchy that all database-specific drivers implement.
Provides shared types (`SqlResultSet`, `QueryValue`, `SqlQuery`) and error types
(`DriverError`, `MappedError`) used throughout the stack.

## Public API

### Traits

| Trait | Description |
|-------|-------------|
| `SqlQueryable` | Core async trait for executing SQL queries (`query_raw`, `execute_raw`, `start_transaction`) |
| `Transaction` | Active database transaction with `commit`, `rollback`, and savepoint support |
| `SqlDriverAdapter` | Extends `SqlQueryable` with `execute_script`, `connection_info`, and `dispose` |
| `SqlDriverAdapterFactory` | Factory for creating `SqlDriverAdapter` instances from connection URLs |
| `SqlMigrationAwareDriverAdapterFactory` | Extends the factory with `connect_to_shadow_db` for migrations |

### Types

| Export | Description |
|--------|-------------|
| `Provider` | Database provider enum (`Postgres`, `Mysql`, `Sqlite`, `DuckDb`) with `max_bind_values()` |
| `IsolationLevel` | Transaction isolation levels (`ReadUncommitted`, `ReadCommitted`, `RepeatableRead`, `Snapshot`, `Serializable`) |
| `ConnectionInfo` | Connection metadata (`schema_name`, `max_bind_values`, `supports_relation_joins`) |
| `SqlQuery` | SQL string + bound parameters + argument types (with `validate()`) |
| `QueryValue` | Typed parameter value (Null, Boolean, Int32, Int64, Float, Double, Numeric, Text, Bytes, Uuid, DateTime, Date, Time, Json, Array) |
| `ColumnType` | Column type enum with `u16` discriminants matching TypeScript `ColumnTypeEnum` |
| `ResultValue` | Typed value in a result row (Null, Boolean, Int32, Int64, Float, Double, Numeric, Text, Date, Time, DateTime, Json, Enum, Uuid, Bytes, Array) |
| `SqlResultSet` | Row-oriented query result with column names, column types, rows, and `last_insert_id` |
| `TransactionOptions` | Transaction configuration (`use_phantom_query`) |
| `ArgType`, `ArgScalarType`, `Arity` | Parameter type metadata |

### Error Types

| Export | Description |
|--------|-------------|
| `DriverError` | Top-level error wrapping `MappedError` with original code and safe message |
| `MappedError` | Semantic error classification (26 variants including DB-specific fallbacks) |
| `ConstraintTarget` | Target of a constraint violation (fields, index, or foreign key) |
| `UserFacingError` | Prisma error with P-code and human-readable message |
| `to_user_facing_error` | Convert `DriverError` to `UserFacingError` for normal queries |
| `to_user_facing_raw_error` | Convert `DriverError` to `UserFacingError` for raw queries (P2010) |

### Security

| Export | Description |
|--------|-------------|
| `DatabaseUrl` | Parsed database URL with credential redaction in `Display`/`Debug` |
| `SafeMessage` | Parameterized error message with secret redaction (secrets only via `expose()`) |
| `StaticSql` / `static_sql!` | Compile-time SQL string builder rejecting runtime values (prevents SQL injection) |
| `SqlComment` | SQL Commenter spec implementation with OpenTelemetry `traceparent`/`tracestate` support |

## Error Classification

`MappedError` classifies database errors semantically:
- Unique constraint violation, null constraint violation, foreign key violation
- Authentication failure, TLS errors, connection closed
- Database not reachable, does not exist, access denied
- Table/column not found, too many connections
- Transaction conflicts, value out of range
- Database-specific fallbacks (Postgres, MySQL, SQLite, DuckDB)

This allows the upper layers to handle errors generically without
knowing which database is in use.

## Dependencies

None (leaf crate). Uses `async-trait`, `thiserror`, `serde`, `chrono`, `uuid`,
`rust_decimal`, `base64`, `url`.

Last updated: 2026-03-19
