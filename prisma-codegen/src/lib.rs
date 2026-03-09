//! Code generation for Prisma TypeScript and Rust clients.
//!
//! Reads a Prisma schema via the PSL parser and emits typed client code
//! for both TypeScript and Rust targets.
//!
//! Architecture:
//! 1. `schema_ir` parses the PSL schema into a language-neutral intermediate representation
//! 2. `gen_typescript` emits TypeScript client code from the IR
//! 3. `gen_rust` emits Rust client code from the IR

mod error;
mod gen_rust;
mod gen_typescript;
mod schema_ir;

pub use error::CodegenError;
pub use gen_rust::RustGenerator;
pub use gen_typescript::TypeScriptGenerator;
pub use schema_ir::{
    FieldArity, FieldDefault, ModelField, ModelIR, RelationField, RelationKind, ScalarField, ScalarKind, SchemaIR,
};
