# prisma-error

Error type bridge between driver errors and prisma-engines user-facing errors.

## Purpose

Converts between the driver layer's `DriverError` / `MappedError` types and
prisma-engines' `KnownError` type (P1xxx, P2xxx error codes). This ensures
that error messages shown to users match the standard Prisma error format.

## Public API

| Export | Description |
|--------|-------------|
| `driver_error_to_known(error: &DriverError) -> Option<KnownError>` | Convert a driver error to a prisma-engines KnownError |
| `known_to_driver_error(known: &KnownError) -> DriverError` | Convert a KnownError back to a DriverError |
| `user_facing_errors` (re-export) | Full prisma-engines user-facing-errors crate |

## Error Codes

Standard Prisma error codes are preserved:
- `P1xxx` -- Common errors (auth, connection, timeout)
- `P2xxx` -- Query errors (unique constraint, null constraint, not found)

## Dependencies

`prisma-driver-core`, `user-facing-errors` (from prisma-engines)
