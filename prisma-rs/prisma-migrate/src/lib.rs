//! Prisma schema engine (migration) operations.
//!
//! Wraps `schema-core` from prisma-engines, providing native access to
//! migration, introspection, and schema push operations.

pub use schema_core::json_rpc::types as rpc_types;
pub use schema_core::{self, CoreError, CoreResult, GenericApi};

use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MigrateError {
    #[error("Schema engine error: {0}")]
    Core(#[from] CoreError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Create a schema engine API instance from a Prisma schema and connection URL.
///
/// The schema engine connects to the database using quaint's native drivers
/// and provides migration, introspection, and schema push operations.
pub fn create_engine(
    schema: Option<String>,
    url: Option<String>,
    shadow_url: Option<String>,
) -> Result<Box<dyn GenericApi>, MigrateError> {
    let datasource_urls = schema_core::DatasourceUrls {
        url,
        shadow_database_url: shadow_url,
    };
    let host = Arc::new(schema_core::schema_connector::EmptyHost);
    Ok(schema_core::schema_api_without_extensions(
        schema,
        datasource_urls,
        Some(host),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_types_accessible() {
        let input = rpc_types::SchemaPushInput {
            schema: rpc_types::SchemasContainer {
                files: vec![rpc_types::SchemaContainer {
                    path: "schema.prisma".to_string(),
                    content: "datasource db { provider = \"sqlite\" }".to_string(),
                }],
            },
            force: false,
            filters: rpc_types::SchemaFilter {
                external_tables: vec![],
                external_enums: vec![],
            },
        };
        assert_eq!(input.schema.files.len(), 1);
    }

    #[test]
    fn generic_api_trait_is_object_safe() {
        fn _assert_object_safe(_: &dyn GenericApi) {}
    }

    #[test]
    fn create_engine_with_sqlite() {
        let schema = r#"
            datasource db {
                provider = "sqlite"
            }

            model User {
                id   Int    @id @default(autoincrement())
                name String
            }
        "#;
        let engine = create_engine(Some(schema.to_string()), Some("file:test.db".to_string()), None);
        assert!(engine.is_ok(), "Failed to create engine: {:?}", engine.err());
    }
}
