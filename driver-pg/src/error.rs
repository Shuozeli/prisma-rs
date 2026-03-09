use prisma_driver_core::{ConstraintTarget, DriverError, MappedError, SafeMessage};

/// Convert a `tokio_postgres::Error` into a Prisma `DriverError`.
pub fn convert_pg_error(err: &tokio_postgres::Error) -> DriverError {
    // Try to extract a DbError with a SQLSTATE code.
    if let Some(db_err) = err.as_db_error() {
        let code = db_err.code().code();
        let message = db_err.message().to_string();
        let original_code = code.to_string();
        let original_message = message.clone();

        let mapped = match code {
            "22001" => MappedError::LengthMismatch {
                column: db_err.column().map(|c| c.to_string()),
            },
            "22003" => MappedError::ValueOutOfRange { cause: message },
            "22P02" => MappedError::InvalidInputValue { message },
            "23505" => MappedError::UniqueConstraintViolation {
                constraint: parse_unique_constraint(db_err),
            },
            "23502" => MappedError::NullConstraintViolation {
                constraint: parse_null_constraint(db_err),
            },
            "23503" => MappedError::ForeignKeyConstraintViolation {
                constraint: db_err.constraint().map(|c| ConstraintTarget::ForeignKey {
                    foreign_key: c.to_string(),
                }),
            },
            "3D000" => MappedError::DatabaseDoesNotExist {
                db: parse_db_name_from_message(&message),
            },
            "28000" => MappedError::DatabaseAccessDenied {
                db: parse_db_name_from_message(&message),
            },
            "28P01" => MappedError::AuthenticationFailed {
                user: parse_user_from_message(&message),
            },
            "40001" => MappedError::TransactionWriteConflict,
            "42P01" => MappedError::TableDoesNotExist {
                table: parse_table_from_message(&message),
            },
            "42703" => MappedError::ColumnNotFound {
                column: parse_column_from_message(&message),
            },
            "42P04" => MappedError::DatabaseAlreadyExists {
                db: parse_db_name_from_message(&message),
            },
            "53300" => MappedError::TooManyConnections { cause: message },
            // Fallback to raw PostgreSQL error
            _ => MappedError::Postgres {
                code: code.to_string(),
                severity: db_err.severity().to_string(),
                message: message.clone(),
                detail: db_err.detail().map(|d| d.to_string()),
                column: db_err.column().map(|c| c.to_string()),
                hint: db_err.hint().map(|h| h.to_string()),
            },
        };

        return DriverError::new(mapped).with_original(original_code, original_message);
    }

    // Connection-level errors (no DbError).
    // These raw messages may contain connection URLs with credentials.
    // We classify into known categories with safe descriptions, and for
    // unknown errors use SafeMessage to structurally mark the raw message
    // as a secret (it may contain credentials).
    let message = err.to_string();
    let mapped = if is_connection_refused(&message) {
        MappedError::DatabaseNotReachable { host: None, port: None }
    } else if is_connection_reset(&message) {
        MappedError::ConnectionClosed
    } else if is_timeout(&message) {
        MappedError::SocketTimeout
    } else if is_tls_error(&message) {
        MappedError::TlsConnectionError {
            // TLS errors are safe to expose (no credentials)
            reason: message.clone(),
        }
    } else {
        MappedError::Postgres {
            code: String::new(),
            severity: "ERROR".to_string(),
            message: "connection error".to_string(),
            detail: None,
            column: None,
            hint: None,
        }
    };

    // The raw driver message is marked as a secret -- it may contain
    // connection URLs with passwords. Only accessible via .expose().
    let safe_msg = SafeMessage::new("PostgreSQL connection error: {0}").secret(message);
    DriverError::new(mapped).with_safe_message("", safe_msg)
}

fn parse_unique_constraint(db_err: &tokio_postgres::error::DbError) -> Option<ConstraintTarget> {
    // Try constraint name first
    if let Some(constraint) = db_err.constraint() {
        return Some(ConstraintTarget::Index {
            index: constraint.to_string(),
        });
    }
    // Try to parse fields from detail message
    if let Some(detail) = db_err.detail() {
        if let Some(fields) = parse_fields_from_detail(detail) {
            return Some(ConstraintTarget::Fields { fields });
        }
    }
    None
}

fn parse_null_constraint(db_err: &tokio_postgres::error::DbError) -> Option<ConstraintTarget> {
    db_err.column().map(|col| ConstraintTarget::Fields {
        fields: vec![col.to_string()],
    })
}

/// Parse field names from a PostgreSQL detail message like:
/// "Key (email)=(foo@bar.com) already exists."
fn parse_fields_from_detail(detail: &str) -> Option<Vec<String>> {
    let start = detail.find("Key (")?;
    let after_key = &detail[start + 5..];
    let end = after_key.find(")=(")?;
    let fields_str = &after_key[..end];
    let fields: Vec<String> = fields_str.split(", ").map(|s| s.to_string()).collect();
    if fields.is_empty() { None } else { Some(fields) }
}

fn parse_db_name_from_message(message: &str) -> Option<String> {
    // Pattern: 'database "foo" does not exist'
    let start = message.find('"')?;
    let rest = &message[start + 1..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_user_from_message(message: &str) -> Option<String> {
    // Pattern: 'password authentication failed for user "foo"'
    let marker = "user \"";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_table_from_message(message: &str) -> Option<String> {
    // Pattern: 'relation "foo" does not exist'
    let marker = "relation \"";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn parse_column_from_message(message: &str) -> Option<String> {
    // Pattern: 'column "foo" does not exist'
    let marker = "column \"";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn is_connection_refused(message: &str) -> bool {
    message.contains("Connection refused") || message.contains("No such file or directory")
}

fn is_connection_reset(message: &str) -> bool {
    message.contains("connection reset") || message.contains("Connection reset")
}

fn is_timeout(message: &str) -> bool {
    message.contains("timed out") || message.contains("timeout")
}

fn is_tls_error(message: &str) -> bool {
    message.contains("TLS") || message.contains("SSL") || message.contains("certificate")
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_fields_from_detail ---

    #[test]
    fn parse_fields_from_detail_single() {
        let detail = "Key (email)=(foo@bar.com) already exists.";
        let fields = parse_fields_from_detail(detail).unwrap();
        assert_eq!(fields, vec!["email"]);
    }

    #[test]
    fn parse_fields_from_detail_multiple() {
        let detail = "Key (email, name)=(foo@bar.com, Bob) already exists.";
        let fields = parse_fields_from_detail(detail).unwrap();
        assert_eq!(fields, vec!["email", "name"]);
    }

    #[test]
    fn parse_fields_from_detail_no_key() {
        assert!(parse_fields_from_detail("Something else happened").is_none());
    }

    #[test]
    fn parse_fields_from_detail_triple_compound() {
        let detail = "Key (a, b, c)=(1, 2, 3) already exists.";
        let fields = parse_fields_from_detail(detail).unwrap();
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    // --- parse_db_name_from_message ---

    #[test]
    fn parse_db_name() {
        let msg = "database \"mydb\" does not exist";
        assert_eq!(parse_db_name_from_message(msg), Some("mydb".into()));
    }

    #[test]
    fn parse_db_name_with_special_chars() {
        let msg = "database \"my-db_123\" does not exist";
        assert_eq!(parse_db_name_from_message(msg), Some("my-db_123".into()));
    }

    #[test]
    fn parse_db_name_no_quotes() {
        assert!(parse_db_name_from_message("database mydb does not exist").is_none());
    }

    // --- parse_user_from_message ---

    #[test]
    fn parse_user() {
        let msg = "password authentication failed for user \"admin\"";
        assert_eq!(parse_user_from_message(msg), Some("admin".into()));
    }

    #[test]
    fn parse_user_special_name() {
        let msg = "password authentication failed for user \"db-user_01\"";
        assert_eq!(parse_user_from_message(msg), Some("db-user_01".into()));
    }

    #[test]
    fn parse_user_no_match() {
        assert!(parse_user_from_message("authentication failed").is_none());
    }

    // --- parse_table_from_message ---

    #[test]
    fn parse_table() {
        let msg = "relation \"users\" does not exist";
        assert_eq!(parse_table_from_message(msg), Some("users".into()));
    }

    #[test]
    fn parse_table_schema_qualified() {
        let msg = "relation \"public.users\" does not exist";
        assert_eq!(parse_table_from_message(msg), Some("public.users".into()));
    }

    #[test]
    fn parse_table_no_match() {
        assert!(parse_table_from_message("something else happened").is_none());
    }

    // --- parse_column_from_message ---

    #[test]
    fn parse_column() {
        let msg = "column \"age\" does not exist";
        assert_eq!(parse_column_from_message(msg), Some("age".into()));
    }

    #[test]
    fn parse_column_underscore() {
        let msg = "column \"first_name\" does not exist";
        assert_eq!(parse_column_from_message(msg), Some("first_name".into()));
    }

    #[test]
    fn parse_column_no_match() {
        assert!(parse_column_from_message("something else").is_none());
    }

    // --- Connection-level error detection ---

    #[test]
    fn connection_refused_detection() {
        assert!(is_connection_refused("Connection refused (os error 111)"));
        assert!(is_connection_refused("No such file or directory"));
        assert!(!is_connection_refused("timeout waiting for connection"));
    }

    #[test]
    fn connection_reset_detection() {
        assert!(is_connection_reset("connection reset by peer"));
        assert!(is_connection_reset("Connection reset by server"));
        assert!(!is_connection_reset("Connection refused"));
    }

    #[test]
    fn timeout_detection() {
        assert!(is_timeout("connection timed out"));
        assert!(is_timeout("query timeout expired"));
        assert!(!is_timeout("Connection refused"));
    }

    #[test]
    fn tls_error_detection() {
        assert!(is_tls_error("TLS handshake failed"));
        assert!(is_tls_error("SSL connection required"));
        assert!(is_tls_error("invalid certificate chain"));
        assert!(!is_tls_error("Connection refused"));
    }
}
