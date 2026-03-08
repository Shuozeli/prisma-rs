//! Native Rust query plan executor for Prisma.
//!
//! Interprets the `Expression` tree produced by the query compiler,
//! executing SQL queries through `prisma-driver-core` adapters and
//! performing data transformation, joining, and validation in Rust.

mod data_map;
mod error;
mod interpret;
mod render;
mod scope;
mod value;

pub use error::ExecutorError;
pub use interpret::QueryExecutor;
pub use value::IValue;
