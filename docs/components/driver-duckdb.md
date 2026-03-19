# driver-duckdb

DuckDB driver implementation.

## Purpose

Implements the `SqlQueryable` / `SqlDriverAdapter` traits for DuckDB,
targeting analytics and OLAP workloads. Uses the `duckdb` crate with
bundled DuckDB C library (direct binding, not via ADBC).

## Public API

| Export | Description |
|--------|-------------|
| `DuckDbDriverAdapter` | Driver adapter wrapping a DuckDB connection |
| `DuckDbDriverAdapterFactory` | Creates `DuckDbDriverAdapter` instances |

## Dependencies

`prisma-driver-core`, `duckdb` 1.4 (bundled)

Last updated: 2026-03-19
