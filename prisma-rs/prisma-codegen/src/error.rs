//! Code generation error types.

#[derive(Debug, thiserror::Error)]
pub enum CodegenError {
    #[error("Schema parsing error: {0}")]
    SchemaParse(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Code generation error: {0}")]
    Generation(String),
}
