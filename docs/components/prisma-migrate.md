# prisma-migrate

Schema migration engine.

## Purpose

RPC bridge to the prisma-engines `schema-core` for database schema migration
operations. Wraps the upstream migration engine with a simpler API surface.

## Public API

| Export | Description |
|--------|-------------|
| `create_engine(schema, url, shadow_url)` | Create a schema engine instance |
| `MigrateError` | Error type for migration failures |
| `GenericApi` (re-export) | Schema engine API trait |
| `CoreError`, `CoreResult` (re-export) | Upstream error types |
| `rpc_types` (re-export) | RPC request/response types for all migration operations |

## Migration Operations

The engine supports (via `GenericApi`):
- `createMigration` -- Generate a new migration SQL file
- `applyMigrations` -- Apply pending migrations
- `reset` -- Drop all tables and re-apply migrations
- `markMigrationApplied` -- Mark a migration as applied without running it
- `schemaPush` -- Push schema directly (no migration history)
- `introspect` -- Introspect database and generate schema
- `diff` -- Diff between schema states
- `evaluateDataLoss` -- Check if a migration would cause data loss

## Shadow Database

Some operations (e.g., `migrate dev`) require a shadow database to simulate
migrations. The shadow database URL can be configured separately.

## Dependencies

`schema-core`, `schema-commands`, `schema-connector`, `psl` (all from prisma-engines)
