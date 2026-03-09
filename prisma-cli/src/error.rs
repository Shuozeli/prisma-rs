use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Schema(#[from] prisma_schema::SchemaError),

    #[error("{0}")]
    Codegen(#[from] prisma_codegen::CodegenError),

    #[error("{0}")]
    Migrate(#[from] prisma_migrate::MigrateError),

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Config(String),
}
