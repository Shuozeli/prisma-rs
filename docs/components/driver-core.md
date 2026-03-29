# driver-core

Database driver trait and common types shared by all database adapters.

## Purpose

Defines the `DatabaseDriver` trait abstraction that all database-specific drivers
implement. Provides shared types (`ResultSet`, `QueryValue`, `SqlQuery`) and error
types (`DriverError`, `MappedError`) used throughout the stack.

## Public API

| Export | Description |
|--------|-------------|
| `DatabaseDriver` trait | Async trait for executing SQL queries and managing transactions |
| `DriverAdapterFactory` trait | Creates driver adapter instances from connection URLs |
| `ResultSet` | Row-oriented query result with column metadata |
| `QueryValue` | Typed parameter value (Text, Int, Float, Boolean, Bytes, etc.) |
| `SqlQuery` | SQL string + bound parameters + argument types |
| `ArgType`, `ArgScalarType`, `Arity` | Parameter type metadata |
| `DriverError`, `MappedError` | Error types with semantic classification |
| `DatabaseUrl` | Parsed and validated database URL |
| `SqlComment`, `SqlCommenter` | SQL comment injection for tracing |

## Error Classification

`MappedError` classifies database errors semantically:
- Unique constraint violation
- Null constraint violation
- Foreign key violation
- Authentication failure
- Connection errors
- Table/column not found

This allows the upper layers to handle errors generically without
knowing which database is in use.

## Dependencies

None (leaf crate). Uses `async-trait`, `thiserror`, `serde`, `chrono`, `uuid`.
