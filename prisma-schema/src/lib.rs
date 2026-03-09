//! Prisma Schema Language (PSL) operations.
//!
//! Thin wrapper around `prisma-fmt` and `psl` from prisma-engines,
//! providing schema parsing, validation, formatting, and DMMF generation.

pub use psl;

mod error;
mod ops;

pub use error::SchemaError;
pub use ops::{format, get_config, get_dmmf, lint, validate};
