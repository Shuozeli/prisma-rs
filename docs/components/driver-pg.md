# driver-pg

PostgreSQL driver implementation.

## Purpose

Implements the `DatabaseDriver` trait for PostgreSQL using `tokio-postgres`
with `deadpool-postgres` connection pooling and `rustls` TLS support.

## Public API

| Export | Description |
|--------|-------------|
| `PgDriverAdapter` | Driver adapter wrapping a pooled PostgreSQL connection |
| `PgDriverAdapterFactory` | Creates `PgDriverAdapter` instances from PostgreSQL URLs |

## Configuration

Connection pooling is handled by `deadpool-postgres`:
- Configurable pool size
- Connection timeouts
- TLS via `rustls` with native certificate loading

## Type Mapping

Maps PostgreSQL native types to `QueryValue`:
- `INT2/INT4/INT8` -> `QueryValue::Int`
- `FLOAT4/FLOAT8` -> `QueryValue::Float`
- `TEXT/VARCHAR` -> `QueryValue::Text`
- `BOOL` -> `QueryValue::Boolean`
- `BYTEA` -> `QueryValue::Bytes`
- `UUID` -> `QueryValue::Uuid`
- `TIMESTAMP/TIMESTAMPTZ` -> `QueryValue::DateTime`
- `JSON/JSONB` -> `QueryValue::Json`
- `NUMERIC` -> `QueryValue::Numeric`

## Dependencies

`prisma-driver-core`, `tokio-postgres`, `deadpool-postgres`, `rustls`
