use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Query execution failed: {0}")]
    Driver(#[from] prisma_driver_core::DriverError),

    #[error("Variable not found in scope: {0}")]
    VariableNotFound(String),

    #[error("Unique constraint violated: expected at most 1 record, got {0}")]
    UniqueViolation(usize),

    #[error("Required record not found: {context}")]
    RequiredNotFound { context: String },

    #[error("Validation failed: {message}")]
    Validation { message: String },

    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
