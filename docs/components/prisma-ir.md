# prisma-ir

Owned intermediate representation types for Prisma query plans.

## Purpose

Mirrors the serialization format of prisma-engines `query-compiler` output as owned
Rust types. Creates a clean boundary between the compiler (which depends on
prisma-engines) and the executor (which depends only on this crate).

## Public API

| Module | Key Types |
|--------|-----------|
| `expression` | `Expression` enum -- the top-level query plan tree (Get, Let, Seq, If, MapGet, etc.) |
| `query` | `DbQuery` enum (RawSql, TemplateSql), `Fragment`, `PlaceholderFormat` |
| `result_node` | `ResultNode` -- how to shape query results |
| `rule` | `DataRule` enum (RowCountEq, RowCountNeq, AffectedRowCountEq, Never) |
| `value` | `PrismaValue`, `PrismaValueType`, `Placeholder`, `GeneratorCall` |

## Serde Conventions

These must match the upstream prisma-engines serialization exactly:

- **Expression**: `#[serde(tag = "type", content = "args", rename_all = "camelCase")]`
- **DataRule**: `#[serde(tag = "type", content = "args", rename_all = "camelCase")]`
- **PrismaValue**: Custom Serialize/Deserialize with `prisma__type`/`prisma__value` wrapper
- **PrismaValueType**: `#[serde(tag = "type", content = "inner")]` -- PascalCase variants
- **Placeholder**: serialized as `prisma__type: "param"`

## Design

The crate uses no `#[deny(missing_docs)]` -- it intentionally mirrors upstream types
without adding redundant documentation. If upstream changes its serialization format,
this crate must be updated to match.

## Dependencies

`serde`, `serde_json`, `indexmap`, `base64` (no prisma-engines dependency)
