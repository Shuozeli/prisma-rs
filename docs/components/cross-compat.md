# cross-compat

Cross-compatibility testing framework.

## Purpose

Validates that the Rust query compiler produces correct SQL across all supported
providers (PostgreSQL, MySQL, SQLite). Runs identical queries through both the Rust
and TypeScript Prisma implementations and asserts identical results.

## Public API

| Export | Description |
|--------|-------------|
| `compiler_for_provider(provider, schema)` | Create a QueryCompiler configured for a specific provider |
| `compile_operation(compiler, request)` | Compile a Prisma request and return the JSON plan |
| `extract_sql_queries(plan)` | Extract SQL strings from a compiled query plan |

## Test Structure

Tests use `insta` for snapshot testing. Each test:
1. Defines a Prisma schema
2. Compiles a Prisma client request
3. Extracts the generated SQL
4. Asserts it matches the snapshot

## Golden Files

Update snapshots after intentional changes:

```bash
UPDATE_GOLDEN=1 cargo test -p cross-compat
```

## Dependencies

`prisma-compiler`, `prisma-schema`, `prisma-driver-core`, `insta` (dev)
