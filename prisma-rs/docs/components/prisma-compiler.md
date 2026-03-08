# prisma-compiler

Query planning and compilation.

## Purpose

Wraps the `query-compiler` crate from prisma-engines, providing a native (non-Wasm)
API for compiling Prisma client operations into SQL query plan expressions. The
`compile_to_ir()` method produces owned `prisma_ir::Expression` types, creating a
clean serialization boundary between the compiler (prisma-engines) and the executor.

## Public API

| Export | Description |
|--------|-------------|
| `QueryCompiler::new(datamodel, connection_info)` | Create compiler from schema string and connection info |
| `QueryCompiler::compile(request)` | Compile a JSON request into a prisma-engines `Expression` |
| `QueryCompiler::compile_to_json(request)` | Compile and return as `serde_json::Value` |
| `QueryCompiler::compile_to_ir(request)` | Compile and return as owned `prisma_ir::Expression` |
| `CompilerError` | Error enum (compile, request parsing, serialization) |
| `quaint` (re-export) | Quaint query builder crate |
| `query_compiler` (re-export) | Upstream query compiler |

## Request Format

Accepts Prisma JSON protocol requests:

```json
{
  "modelName": "User",
  "action": "findMany",
  "query": {
    "selection": { "$scalars": true }
  }
}
```

## Serialization Boundary

`compile_to_ir()` roundtrips through JSON to decouple from prisma-engines types.
This means the `query-executor` crate can depend only on `prisma-ir`, not on
prisma-engines directly.

## Dependencies

`query-compiler`, `query-core`, `quaint`, `psl`, `prisma-ir` (all from prisma-engines except prisma-ir)
