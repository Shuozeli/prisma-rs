# driver-mysql

MySQL driver implementation.

## Purpose

Implements the `SqlQueryable` / `SqlDriverAdapter` traits for MySQL using `mysql_async`
with its built-in connection pooling.

## Public API

| Export | Description |
|--------|-------------|
| `MySqlDriverAdapter` | Driver adapter wrapping a MySQL connection pool |
| `MySqlDriverAdapterFactory` | Creates `MySqlDriverAdapter` instances from MySQL URLs |

## Type Mapping

Maps MySQL native types to `QueryValue`:
- `TINYINT/SMALLINT/INT/BIGINT` -> `QueryValue::Int`
- `FLOAT/DOUBLE` -> `QueryValue::Float`
- `VARCHAR/TEXT/CHAR` -> `QueryValue::Text`
- `TINYINT(1)` -> `QueryValue::Boolean`
- `BLOB/BINARY` -> `QueryValue::Bytes`
- `DATETIME/TIMESTAMP` -> `QueryValue::DateTime`
- `JSON` -> `QueryValue::Json`
- `DECIMAL` -> `QueryValue::Numeric`

## Dependencies

`prisma-driver-core`, `mysql_async`

Last updated: 2026-03-19
