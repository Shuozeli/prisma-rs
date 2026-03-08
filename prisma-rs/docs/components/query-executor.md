# query-executor

In-memory query execution engine.

## Purpose

Interprets `prisma_ir::Expression` trees (compiled query plans) by executing SQL
queries through database adapters and performing in-memory data transformation.

## Public API

| Export | Description |
|--------|-------------|
| `QueryExecutor` | Main executor struct -- takes a `DatabaseDriver` and executes expressions |
| `ExecutorError` | Error type for execution failures |
| `IValue` | Internal value type used during execution (JSON-like with Prisma semantics) |

## Capabilities

The executor handles:

- **SQL Execution**: Renders `DbQuery` templates into concrete `SqlQuery` values and sends them to the database driver
- **Expression Interpretation**: Walks the expression tree (Get, Let, Seq, If, MapGet, etc.)
- **In-Memory Filtering**: Evaluates `where` clauses on result sets
- **Sorting**: `orderBy` with multi-field and nested relation sorting
- **Pagination**: `skip`, `take`, cursor-based pagination
- **Aggregation**: `count`, `sum`, `avg`, `min`, `max`, `groupBy`
- **Relation Traversal**: `include` and `select` with nested relations
- **Mutations**: `create`, `update`, `upsert`, `delete` with nested writes
- **GeneratorCall Resolution**: Resolves `now()` to current timestamp at execution time
- **Placeholder Resolution**: Resolves variable references from the expression scope

## Execution Flow

```
Expression tree (from prisma-ir)
    |
    v
QueryExecutor.execute()
    |
    +--> render_query()  -- template SQL -> concrete SQL with bound params
    |
    +--> driver.query()  -- send to database
    |
    +--> shape results   -- apply ResultNode rules to format output
    |
    +--> in-memory ops   -- filter, sort, paginate, aggregate
    |
    v
IValue (JSON-like result)
```

## Dependencies

`prisma-driver-core`, `prisma-ir`, `chrono`, `uuid`, `indexmap`
