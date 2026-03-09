//! Runtime library for generated Prisma clients (Rust and TypeScript).
//!
//! Provides `PrismaClient` which ties together the query compiler, executor,
//! and database adapter. Generated code (from `prisma-codegen`) builds on
//! these types to provide model-specific typed APIs.

pub mod accelerate;
mod client;
mod error;
pub mod extensions;
pub mod logging;
pub mod middleware;
mod query;
mod selection;
mod transaction;

pub use accelerate::{AccelerateClient, CacheStrategy};
pub use client::{PrismaClient, PrismaClientBuilder};
pub use error::{AccelerateErrorDetail, ClientError};
pub use extensions::ResultExtension;
pub use logging::{LogConfig, LogEmit, LogLevel, QueryEvent};
pub use middleware::{Middleware, MiddlewareNext, MiddlewareParams};
pub use query::{Operation, QueryBuilder};
pub use selection::Selection;
pub use transaction::TransactionClient;
