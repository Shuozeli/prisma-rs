//! Client error types.

use prisma_compiler::CompilerError;
use prisma_driver_core::DriverError;
use prisma_query_executor::ExecutorError;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Compilation error: {0}")]
    Compiler(#[from] CompilerError),

    #[error("Execution error: {0}")]
    Executor(#[from] ExecutorError),

    #[error("Driver error: {0}")]
    Driver(#[from] DriverError),

    #[error("Record not found")]
    NotFound,

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Accelerate error: {}", format_accelerate_errors(.errors))]
    Accelerate { errors: Vec<AccelerateErrorDetail> },

    #[error("Transaction failed: {0}")]
    TransactionFailed(String),

    #[error("Connection lost: {0}")]
    ConnectionLost(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Transport error: {0}")]
    TransportError(String),
}

/// A single error returned by the Accelerate proxy.
#[derive(Debug, Clone)]
pub struct AccelerateErrorDetail {
    pub message: String,
}

fn format_accelerate_errors(errors: &[AccelerateErrorDetail]) -> String {
    errors.iter().map(|e| e.message.as_str()).collect::<Vec<_>>().join("; ")
}
