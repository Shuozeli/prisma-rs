# driver-adbc

Arrow Database Connectivity (ADBC) driver.

## Purpose

Implements the `DatabaseDriver` trait using the ADBC protocol, converting
Arrow columnar data into Prisma's row-oriented `ResultSet` format.

## Public API

| Export | Description |
|--------|-------------|
| `AdbcDriverAdapter` | Driver adapter wrapping an ADBC connection |
| `AdbcDriverAdapterFactory` | Creates `AdbcDriverAdapter` instances |
| `arrow` module | Arrow-to-Prisma type conversion utilities |

## Notes

- Converts Apache Arrow `RecordBatch` columnar results to row-oriented `ResultSet`.
- Used as a foundation for other Arrow-based drivers (e.g., Flight SQL).
- Arrow crate versions pinned to 56 to match `adbc_core` compatibility.

## Dependencies

`prisma-driver-core`, `adbc_core`, `arrow-array`, `arrow-schema`
