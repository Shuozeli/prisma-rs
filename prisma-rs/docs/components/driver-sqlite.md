# driver-sqlite

SQLite driver implementation.

## Purpose

Implements the `DatabaseDriver` trait for SQLite using `rusqlite` with
bundled SQLite (no system SQLite dependency required).

## Public API

| Export | Description |
|--------|-------------|
| `SqliteDriverAdapter` | Driver adapter wrapping a single SQLite connection |
| `SqliteDriverAdapterFactory` | Creates `SqliteDriverAdapter` instances from file paths |

## Notes

- Uses bundled SQLite via `rusqlite`'s `bundled` feature -- no system dependency.
- Single connection (file-based, no connection pooling).
- Async operations are wrapped with `tokio::task::spawn_blocking` since
  `rusqlite` is synchronous.

## Dependencies

`prisma-driver-core`, `rusqlite` (bundled)
