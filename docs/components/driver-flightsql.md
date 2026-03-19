# driver-flightsql

Arrow Flight SQL driver for remote database access over gRPC.

## Purpose

Implements the `SqlQueryable` / `SqlDriverAdapter` traits using Apache Arrow Flight SQL protocol,
enabling SQL execution against remote databases through gRPC transport.

## Public API

| Export | Description |
|--------|-------------|
| `FlightSqlDriverAdapter` | Driver adapter wrapping a Flight SQL client |
| `FlightSqlDriverAdapterFactory` | Creates `FlightSqlDriverAdapter` instances |

## Notes

- Builds on `driver-adbc` for Arrow-to-Prisma type conversion.
- Uses `tonic` for gRPC transport.
- Suitable for distributed database access scenarios.

## Dependencies

`prisma-driver-core`, `prisma-driver-adbc`, `arrow-flight` 56, `tonic` 0.13, `prost` 0.13

Last updated: 2026-03-19
