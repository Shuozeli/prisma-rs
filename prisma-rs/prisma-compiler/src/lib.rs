//! Native Rust query compiler for Prisma.
//!
//! Wraps the `query-compiler` crate from prisma-engines, providing a native
//! (non-Wasm) API for compiling Prisma client operations into SQL query plans.

use std::sync::Arc;

use psl::{ConnectorRegistry, parser_database::NoExtensionTypes};
use quaint::connector::ConnectionInfo;
use query_compiler::Expression;
use query_core::{QueryDocument, protocol::EngineProtocol, with_sync_unevaluated_request_context};
use request_handlers::RequestBody;
use thiserror::Error;

pub use quaint;
pub use query_compiler;
pub use query_core;

const CONNECTOR_REGISTRY: ConnectorRegistry<'_> = &[
    psl::builtin_connectors::POSTGRES,
    psl::builtin_connectors::MYSQL,
    psl::builtin_connectors::SQLITE,
];

#[derive(Debug, Error)]
pub enum CompilerError {
    #[error("Query compilation failed: {0}")]
    Compile(#[from] query_compiler::CompileError),

    #[error("Request parsing failed: {0}")]
    Request(String),

    #[error("Unexpected batch request (expected single query)")]
    UnexpectedBatch,

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// A native Prisma query compiler.
///
/// Created from a Prisma schema string and connection info. Compiles
/// Prisma client JSON requests into SQL query plan expressions.
pub struct QueryCompiler {
    schema: Arc<schema::QuerySchema>,
    connection_info: ConnectionInfo,
}

impl QueryCompiler {
    /// Create a new query compiler from a Prisma schema and provider info.
    ///
    /// `datamodel` is the raw Prisma schema string (without `url` in datasource).
    /// `connection_info` is the database connection info (from `quaint`).
    pub fn new(datamodel: &str, connection_info: ConnectionInfo) -> Self {
        let psl_schema = Arc::new(psl::parse_without_validation(
            datamodel.into(),
            CONNECTOR_REGISTRY,
            &NoExtensionTypes,
        ));
        let query_schema = Arc::new(schema::build(psl_schema, true));

        Self {
            schema: query_schema,
            connection_info,
        }
    }

    /// Compile a single Prisma client JSON request into a query plan expression.
    ///
    /// The request is in Prisma JSON protocol format, e.g.:
    /// ```json
    /// {
    ///   "modelName": "User",
    ///   "action": "findMany",
    ///   "query": { "selection": { "$scalars": true } }
    /// }
    /// ```
    pub fn compile(&self, request: &str) -> Result<Expression, CompilerError> {
        with_sync_unevaluated_request_context(|| {
            let body = RequestBody::try_from_str(request, EngineProtocol::Json)
                .map_err(|e| CompilerError::Request(e.to_string()))?;
            let QueryDocument::Single(op) = body
                .into_doc(&self.schema)
                .map_err(|e| CompilerError::Request(e.to_string()))?
            else {
                return Err(CompilerError::UnexpectedBatch);
            };
            let plan = query_compiler::compile(&self.schema, op, &self.connection_info)?;
            Ok(plan)
        })
    }

    /// Compile a request and return the expression serialized as JSON.
    pub fn compile_to_json(&self, request: &str) -> Result<serde_json::Value, CompilerError> {
        let expr = self.compile(request)?;
        Ok(serde_json::to_value(&expr)?)
    }

    /// Compile a request and return owned IR types.
    ///
    /// This roundtrips through JSON to decouple from prisma-engines types,
    /// producing owned `prisma_ir::Expression` values that the executor
    /// can interpret without any prisma-engines dependency.
    pub fn compile_to_ir(&self, request: &str) -> Result<prisma_ir::Expression, CompilerError> {
        let json = self.compile_to_json(request)?;
        let ir = serde_json::from_value(json)?;
        Ok(ir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quaint::prelude::ExternalConnectionInfo;

    fn test_compiler(provider: &str) -> QueryCompiler {
        let schema = format!(
            r#"
            datasource db {{
                provider = "{provider}"
            }}

            model User {{
                id    Int    @id @default(autoincrement())
                email String @unique
                name  String?
                posts Post[]
            }}

            model Post {{
                id       Int    @id @default(autoincrement())
                title    String
                authorId Int
                author   User   @relation(fields: [authorId], references: [id])
            }}
        "#
        );

        let conn_info = ConnectionInfo::External(ExternalConnectionInfo::new(
            quaint::prelude::SqlFamily::Postgres,
            None,
            None,
            true,
        ));
        QueryCompiler::new(&schema, conn_info)
    }

    #[test]
    fn compile_find_many() {
        let compiler = test_compiler("postgresql");
        let request = r#"{
            "modelName": "User",
            "action": "findMany",
            "query": {
                "selection": { "$scalars": true }
            }
        }"#;
        let result = compiler.compile(request);
        assert!(result.is_ok(), "Failed to compile: {:?}", result.err());
    }

    #[test]
    fn compile_find_many_json() {
        let compiler = test_compiler("postgresql");
        let request = r#"{
            "modelName": "User",
            "action": "findMany",
            "query": {
                "selection": { "$scalars": true }
            }
        }"#;
        let json = compiler.compile_to_json(request).unwrap();
        // The result should contain SQL query info
        assert!(
            json.is_object() || json.is_array(),
            "Expected object or array, got: {json}"
        );
    }

    #[test]
    fn compile_create() {
        let compiler = test_compiler("postgresql");
        let request = r#"{
            "modelName": "User",
            "action": "createOne",
            "query": {
                "arguments": {
                    "data": {
                        "email": "test@example.com",
                        "name": "Test"
                    }
                },
                "selection": { "$scalars": true }
            }
        }"#;
        let result = compiler.compile(request);
        assert!(result.is_ok(), "Failed to compile: {:?}", result.err());
    }

    #[test]
    fn compile_invalid_request() {
        let compiler = test_compiler("postgresql");
        let request = r#"{ "invalid": true }"#;
        let result = compiler.compile(request);
        assert!(result.is_err());
    }
}
