use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("Schema validation failed: {0}")]
    Validation(String),

    #[error("DMMF generation failed: {0}")]
    Dmmf(String),

    #[error("Config extraction failed: {0}")]
    Config(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
