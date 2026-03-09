#![recursion_limit = "512"]
//! Owned intermediate representation for Prisma query plans.
//!
//! These types mirror the JSON serialization format of prisma-engines
//! query-compiler output. They implement `Deserialize` to receive plans
//! from the compiler and `Serialize` for testing and inspection.
//!
//! This crate creates a clean serialization boundary between the
//! query compiler (which produces plans) and the executor (which
//! interprets them), allowing the executor to operate without any
//! direct dependency on prisma-engines crates.

mod expression;
mod query;
mod result_node;
mod rule;
mod value;

pub use expression::*;
pub use query::*;
pub use result_node::*;
pub use rule::*;
pub use value::*;
