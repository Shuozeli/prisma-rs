# prisma-codegen

Code generator for Prisma TypeScript and Rust clients.

## Purpose

Reads a Prisma schema and generates typed client code. Supports both TypeScript
and Rust output targets through a language-neutral schema IR.

## Public API

| Export | Description |
|--------|-------------|
| `RustGenerator` | Generates Rust client code from a schema |
| `TypeScriptGenerator` | Generates TypeScript client code from a schema |
| `CodegenError` | Error type for code generation failures |
| `SchemaIR` | Language-neutral intermediate representation of a Prisma schema |
| `ModelIR` | IR for a single model |
| `ModelField` | Union of scalar and relation fields |
| `ScalarField` | Scalar field with type, arity, defaults |
| `RelationField` | Relation field with relation kind and references |
| `ScalarKind` | Enum of scalar types (String, Int, Float, Boolean, DateTime, etc.) |
| `FieldArity` | Required, Optional, or List |
| `FieldDefault` | Default value specification (autoincrement, now, uuid, cuid, value) |
| `RelationKind` | OneToOne, OneToMany, ManyToMany |

## Generated Output

### Rust
- Model structs with serde derives
- Model delegate structs with query methods (find_many, find_first, create, update, delete)
- PrismaClient struct with model accessors

### TypeScript
- Model interfaces
- Type-safe query builder types

## Dependencies

`prisma-schema`, `psl`
