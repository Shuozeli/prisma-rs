use crate::safe_message::SafeMessage;
use serde::{Deserialize, Serialize};

/// Target of a constraint violation (fields, index name, or foreign key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConstraintTarget {
    Fields { fields: Vec<String> },
    Index { index: String },
    ForeignKey { foreign_key: String },
}

impl ConstraintTarget {
    pub fn render(&self) -> String {
        match self {
            ConstraintTarget::Fields { fields } => {
                let quoted: Vec<String> = fields.iter().map(|f| format!("`{f}`")).collect();
                format!("fields: ({})", quoted.join(", "))
            }
            ConstraintTarget::Index { index } => {
                format!("constraint: `{index}`")
            }
            ConstraintTarget::ForeignKey { .. } => "foreign key".to_string(),
        }
    }
}

/// A mapped database error, classifying the underlying driver error into a
/// known taxonomy that can be translated to Prisma error codes (P1xxx/P2xxx).
///
/// Variants mirror the TypeScript `MappedError` discriminated union from
/// `@prisma/driver-adapter-utils`.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MappedError {
    #[error("Generic JS error (id {id})")]
    GenericJs { id: u32 },

    #[error("Unsupported native data type: {type}")]
    UnsupportedNativeDataType { r#type: String },

    #[error("Invalid isolation level: {level}")]
    InvalidIsolationLevel { level: String },

    #[error("Length mismatch on column: {}", column.as_deref().unwrap_or("(not available)"))]
    LengthMismatch { column: Option<String> },

    #[error("Unique constraint violation")]
    UniqueConstraintViolation { constraint: Option<ConstraintTarget> },

    #[error("Null constraint violation")]
    NullConstraintViolation { constraint: Option<ConstraintTarget> },

    #[error("Foreign key constraint violation")]
    ForeignKeyConstraintViolation { constraint: Option<ConstraintTarget> },

    #[error("Database not reachable at {host:?}:{port:?}")]
    DatabaseNotReachable { host: Option<String>, port: Option<u16> },

    #[error("Database does not exist: {}", db.as_deref().unwrap_or("(not available)"))]
    DatabaseDoesNotExist { db: Option<String> },

    #[error("Database already exists: {}", db.as_deref().unwrap_or("(not available)"))]
    DatabaseAlreadyExists { db: Option<String> },

    #[error("Database access denied: {}", db.as_deref().unwrap_or("(not available)"))]
    DatabaseAccessDenied { db: Option<String> },

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("TLS connection error: {reason}")]
    TlsConnectionError { reason: String },

    #[error("Authentication failed for user: {}", user.as_deref().unwrap_or("(not available)"))]
    AuthenticationFailed { user: Option<String> },

    #[error("Transaction write conflict")]
    TransactionWriteConflict,

    #[error("Table does not exist: {}", table.as_deref().unwrap_or("(not available)"))]
    TableDoesNotExist { table: Option<String> },

    #[error("Column not found: {}", column.as_deref().unwrap_or("(not available)"))]
    ColumnNotFound { column: Option<String> },

    #[error("Too many connections: {cause}")]
    TooManyConnections { cause: String },

    #[error("Value out of range: {cause}")]
    ValueOutOfRange { cause: String },

    #[error("Invalid input value: {message}")]
    InvalidInputValue { message: String },

    #[error("Missing full text search index")]
    MissingFullTextSearchIndex,

    #[error("Socket timeout")]
    SocketTimeout,

    #[error("Inconsistent column data: {cause}")]
    InconsistentColumnData { cause: String },

    #[error("Transaction already closed: {cause}")]
    TransactionAlreadyClosed { cause: String },

    // Raw database-specific errors (fallback when no mapped variant applies).
    #[error("PostgreSQL error {code}: {message}")]
    Postgres {
        code: String,
        severity: String,
        message: String,
        detail: Option<String>,
        column: Option<String>,
        hint: Option<String>,
    },

    #[error("MySQL error {code}: {message}")]
    Mysql {
        code: u32,
        message: String,
        state: String,
        cause: Option<String>,
    },

    #[error("SQLite error {extended_code}: {message}")]
    Sqlite { extended_code: i32, message: String },

    #[error("DuckDB error: {message}")]
    DuckDb { message: String },
}

/// A driver error wrapping a [`MappedError`] with the original database error
/// code and message preserved for raw query error reporting.
///
/// The `original_message` uses [`SafeMessage`] so that any secrets (passwords,
/// connection URLs) are structurally separated from the template and redacted
/// in `Display`/`Debug` output. This prevents credential leakage by
/// construction -- no regex sanitization needed.
#[derive(Debug, thiserror::Error)]
#[error("{mapped}")]
pub struct DriverError {
    pub mapped: MappedError,
    pub original_code: Option<String>,
    pub original_message: Option<SafeMessage>,
}

impl DriverError {
    pub fn new(mapped: MappedError) -> Self {
        Self {
            mapped,
            original_code: None,
            original_message: None,
        }
    }

    /// Attach the original error code and a safe (non-secret) message.
    pub fn with_original(mut self, code: impl Into<String>, message: impl Into<String>) -> Self {
        self.original_code = Some(code.into());
        self.original_message = Some(SafeMessage::new(message.into()));
        self
    }

    /// Attach the original error code and a structured message that may
    /// contain secrets (redacted in Display/Debug, only exposed via `expose()`).
    pub fn with_safe_message(mut self, code: impl Into<String>, message: SafeMessage) -> Self {
        self.original_code = Some(code.into());
        self.original_message = Some(message);
        self
    }
}

impl From<MappedError> for DriverError {
    fn from(mapped: MappedError) -> Self {
        Self::new(mapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_target_render_fields() {
        let target = ConstraintTarget::Fields {
            fields: vec!["email".into(), "name".into()],
        };
        assert_eq!(target.render(), "fields: (`email`, `name`)");
    }

    #[test]
    fn constraint_target_render_index() {
        let target = ConstraintTarget::Index {
            index: "User_email_key".into(),
        };
        assert_eq!(target.render(), "constraint: `User_email_key`");
    }

    #[test]
    fn constraint_target_render_foreign_key() {
        let target = ConstraintTarget::ForeignKey {
            foreign_key: String::new(),
        };
        assert_eq!(target.render(), "foreign key");
    }

    #[test]
    fn driver_error_with_original() {
        let err = DriverError::new(MappedError::SocketTimeout).with_original("ETIMEDOUT", "Connection timed out");
        assert_eq!(err.original_code.as_deref(), Some("ETIMEDOUT"));
        assert_eq!(
            err.original_message.as_ref().map(|m| m.to_string()),
            Some("Connection timed out".to_string())
        );
    }

    #[test]
    fn driver_error_with_safe_message_redacts() {
        let msg = SafeMessage::new("Failed to connect to {0} with password {1}")
            .param("localhost:5432")
            .secret("hunter2");
        let err = DriverError::new(MappedError::SocketTimeout).with_safe_message("CONN", msg);

        let displayed = err.original_message.as_ref().unwrap().to_string();
        assert!(!displayed.contains("hunter2"), "password leaked: {displayed}");
        assert!(displayed.contains("***"));

        let exposed = err.original_message.as_ref().unwrap().expose();
        assert!(exposed.contains("hunter2"));
    }
}
