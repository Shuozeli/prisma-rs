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
use prisma_client::{PrismaClient, PrismaClientBuilder, QueryBuilder, Operation};

// Basic construction
let client = PrismaClient::new(schema_str, &factory).await?;

// With middleware and logging
let client = PrismaClientBuilder::new(schema_str, &factory)
    .middleware(my_middleware)
    .log(LogConfig::new().level(LogLevel::Query))
    .sql_comment(SqlComment::new().tag("app", "my-app"))
    .build()
    .await?;

// Execute a query
let query = QueryBuilder::new("User", Operation::FindMany);
let users = client.execute(&query).await?;

// Disconnect
client.disconnect().await?;
```

## Middleware

Supports a middleware pipeline for intercepting queries. Middleware is added
via the builder, not on the client directly:

```rust
use prisma_client::{Middleware, MiddlewareParams, MiddlewareNext, ClientError};

struct TimingMiddleware;

#[async_trait::async_trait]
impl Middleware for TimingMiddleware {
    async fn resolve(
        &self,
        params: MiddlewareParams,
        next: MiddlewareNext<'_>,
    ) -> Result<serde_json::Value, ClientError> {
        let start = std::time::Instant::now();
        let result = next.run(params).await;
        println!("Query took {:?}", start.elapsed());
        result
    }
}

let client = PrismaClientBuilder::new(schema, &factory)
    .middleware(TimingMiddleware)
    .build()
    .await?;
```

## Query Logging

Structured query event logging with callback and tracing support:

```rust
use prisma_client::logging::{LogConfig, LogLevel, LogEmit};

let config = LogConfig::new()
    .level(LogLevel::Query)                        // Print to stderr
    .log(LogLevel::Query, LogEmit::Event)          // Emit as tracing event
    .on_query(|event| {                            // Programmatic callback
        println!("{}: {}ms", event.action, event.duration_ms);
    });
```

## Accelerate

Optional HTTP-based query forwarding to Prisma Accelerate for edge caching.
The `AccelerateClient` is available as a separate type.

## Dependencies

`prisma-compiler`, `prisma-query-executor`, `prisma-driver-core`, `reqwest`, `tokio`, `tracing`, `chrono`

Last updated: 2026-03-19
