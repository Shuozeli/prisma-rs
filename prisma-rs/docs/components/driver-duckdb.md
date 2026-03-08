# driver-duckdb

DuckDB driver implementation.

## Purpose

Implements the `DatabaseDriver` trait for DuckDB, targeting analytics
and OLAP workloads. Uses DuckDB's bundled C library.

## Public API

| Export | Description |
|--------|-------------|
| `DuckDbDriverAdapter` | Driver adapter wrapping a DuckDB connection |
| `DuckDbDriverAdapterFactory` | Creates `DuckDbDriverAdapter` instances |

## Dependencies

`prisma-driver-core`, `duckdb` (bundled)
