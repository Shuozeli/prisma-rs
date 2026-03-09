# prisma-schema

Schema parsing, validation, formatting, and DMMF generation.

## Purpose

Thin wrapper around the upstream `prisma-fmt` and `psl` crates from prisma-engines.
Provides the core schema operations needed by the CLI and code generator.

## Public API

| Export | Description |
|--------|-------------|
| `validate(schema: &str)` | Validate a Prisma schema, returning errors if any |
| `format(schema: &str)` | Format a Prisma schema string |
| `get_config(schema: &str)` | Extract datasource and generator configuration |
| `get_dmmf(schema: &str)` | Generate the DMMF (Data Model Meta Format) JSON |
| `lint(schema: &str)` | Lint a schema for warnings |
| `SchemaError` | Error type for schema operations |
| `psl` (re-export) | The full PSL parser crate |

## DMMF

DMMF (Data Model Meta Format) is a JSON representation of the Prisma schema
used by code generators. It contains:
- Data model (models, fields, relations, enums)
- Schema (query/mutation types for the query engine)
- Mappings (model name to query engine operation mappings)

## Dependencies

`prisma-fmt`, `psl` (from prisma-engines)
