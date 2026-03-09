# prisma-client

Runtime library for generated Prisma clients.

## Purpose

Ties together the compiler, executor, and database adapters into a high-level
client API. This is what generated client code uses at runtime.

## Public API

| Export | Description |
|--------|-------------|
| `PrismaClient` | Main client struct -- build queries and execute them |
| `PrismaClientBuilder` | Builder for configuring and constructing a `PrismaClient` |
| `QueryBuilder` | Fluent query builder for constructing Prisma operations |
| `Operation` | Enum of all Prisma operations (FindMany, FindFirst, Create, Update, Delete, Count, etc.) |
| `Selection` | Field selection tree (scalars, relations, nested selects) |
| `TransactionClient` | Client handle for interactive transactions |
| `AccelerateClient` | HTTP client for Prisma Accelerate (edge caching) |
| `CacheStrategy` | Cache TTL and SWR configuration for Accelerate |
| `Middleware`, `MiddlewareNext`, `MiddlewareParams` | Middleware pipeline types |
| `LogConfig`, `LogEmit`, `LogLevel`, `QueryEvent` | Query logging configuration |
| `ResultExtension` | Extension methods on query results |

## Usage Pattern

```rust
let client = PrismaClientBuilder::new()
    .schema(schema_str)
    .database_url("postgresql://...")
    .build()
    .await?;

let users = client
    .query("User", Operation::FindMany)
    .select(Selection::scalars())
    .exec()
    .await?;
```

## Middleware

Supports a middleware pipeline for intercepting queries:

```rust
client.use_middleware(|params, next| async {
    println!("Query: {:?}", params.operation);
    let result = next.run(params).await;
    result
});
```

## Accelerate

Optional HTTP-based query forwarding to Prisma Accelerate for edge caching:

```rust
let client = PrismaClientBuilder::new()
    .accelerate_url("https://accelerate.prisma-data.net")
    .api_key("your-key")
    .build()
    .await?;
```

## Dependencies

`prisma-compiler`, `prisma-query-executor`, `prisma-driver-core`, `reqwest`
