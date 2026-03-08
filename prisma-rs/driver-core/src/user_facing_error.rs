use crate::{DriverError, MappedError};

/// A user-facing Prisma error with an error code (P1xxx/P2xxx) and human-readable message.
#[derive(Debug, Clone)]
pub struct UserFacingError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for UserFacingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for UserFacingError {}

/// Convert a [`DriverError`] into a [`UserFacingError`] for normal (non-raw) queries.
///
/// Returns `None` if the error is a raw database-specific error that has no
/// Prisma error code mapping (Postgres/Mysql/Sqlite fallback variants).
pub fn to_user_facing_error(error: &DriverError) -> Option<UserFacingError> {
    let code = error_code(&error.mapped)?;
    let message = error_message(&error.mapped)?;
    Some(UserFacingError { code, message })
}

/// Convert a [`DriverError`] into a [`UserFacingError`] for raw queries.
/// Always produces P2010 with the original error code/message.
pub fn to_user_facing_raw_error(error: &DriverError) -> UserFacingError {
    let original_code = error.original_code.as_deref().unwrap_or("N/A");
    // Use the redacted form of the message -- secrets are replaced with ***
    let original_message = match &error.original_message {
        Some(msg) => msg.redacted(),
        None => error_message(&error.mapped).unwrap_or_else(|| "Unknown error".into()),
    };

    UserFacingError {
        code: "P2010".to_string(),
        message: format!("Raw query failed. Code: `{original_code}`. Message: `{original_message}`"),
    }
}

fn error_code(mapped: &MappedError) -> Option<String> {
    let code = match mapped {
        MappedError::AuthenticationFailed { .. } => "P1000",
        MappedError::DatabaseNotReachable { .. } => "P1001",
        MappedError::DatabaseDoesNotExist { .. } => "P1003",
        MappedError::SocketTimeout => "P1008",
        MappedError::DatabaseAlreadyExists { .. } => "P1009",
        MappedError::DatabaseAccessDenied { .. } => "P1010",
        MappedError::TlsConnectionError { .. } => "P1011",
        MappedError::ConnectionClosed => "P1017",
        MappedError::TransactionAlreadyClosed { .. } => "P1018",
        MappedError::LengthMismatch { .. } => "P2000",
        MappedError::UniqueConstraintViolation { .. } => "P2002",
        MappedError::ForeignKeyConstraintViolation { .. } => "P2003",
        MappedError::InvalidInputValue { .. } => "P2007",
        MappedError::UnsupportedNativeDataType { .. } => "P2010",
        MappedError::NullConstraintViolation { .. } => "P2011",
        MappedError::ValueOutOfRange { .. } => "P2020",
        MappedError::TableDoesNotExist { .. } => "P2021",
        MappedError::ColumnNotFound { .. } => "P2022",
        MappedError::InvalidIsolationLevel { .. } | MappedError::InconsistentColumnData { .. } => "P2023",
        MappedError::MissingFullTextSearchIndex => "P2030",
        MappedError::TransactionWriteConflict => "P2034",
        MappedError::GenericJs { .. } => "P2036",
        MappedError::TooManyConnections { .. } => "P2037",
        // Raw DB errors have no mapped Prisma code.
        MappedError::Postgres { .. }
        | MappedError::Mysql { .. }
        | MappedError::Sqlite { .. }
        | MappedError::DuckDb { .. } => {
            return None;
        }
    };
    Some(code.to_string())
}

fn error_message(mapped: &MappedError) -> Option<String> {
    let msg = match mapped {
        MappedError::AuthenticationFailed { user } => {
            let user = user.as_deref().unwrap_or("(not available)");
            format!("Authentication failed against the database server, the provided database credentials for `{user}` are not valid")
        }
        MappedError::DatabaseNotReachable { host, port } => {
            let address = match (host.as_deref(), port) {
                (Some(h), Some(p)) => format!(" at {h}:{p}"),
                (Some(h), None) => format!(" at {h}"),
                _ => String::new(),
            };
            format!("Can't reach database server{address}")
        }
        MappedError::DatabaseDoesNotExist { db } => {
            let db = db.as_deref().unwrap_or("(not available)");
            format!("Database `{db}` does not exist on the database server")
        }
        MappedError::SocketTimeout => "Operation has timed out".to_string(),
        MappedError::DatabaseAlreadyExists { db } => {
            let db = db.as_deref().unwrap_or("(not available)");
            format!("Database `{db}` already exists on the database server")
        }
        MappedError::DatabaseAccessDenied { db } => {
            let db = db.as_deref().unwrap_or("(not available)");
            format!("User was denied access on the database `{db}`")
        }
        MappedError::TlsConnectionError { reason } => {
            format!("Error opening a TLS connection: {reason}")
        }
        MappedError::ConnectionClosed => "Server has closed the connection.".to_string(),
        MappedError::TransactionAlreadyClosed { cause } => cause.clone(),
        MappedError::LengthMismatch { column } => {
            let column = column.as_deref().unwrap_or("(not available)");
            format!("The provided value for the column is too long for the column's type. Column: {column}")
        }
        MappedError::UniqueConstraintViolation { constraint } => {
            let target = render_constraint(constraint.as_ref());
            format!("Unique constraint failed on the {target}")
        }
        MappedError::ForeignKeyConstraintViolation { constraint } => {
            let target = render_constraint(constraint.as_ref());
            format!("Foreign key constraint violated on the {target}")
        }
        MappedError::UnsupportedNativeDataType { r#type } => {
            format!(
                "Failed to deserialize column of type '{}'. If you're using $queryRaw and this column is explicitly marked as `Unsupported` in your Prisma schema, try casting this column to any supported Prisma type such as `String`.",
                r#type
            )
        }
        MappedError::NullConstraintViolation { constraint } => {
            let target = render_constraint(constraint.as_ref());
            format!("Null constraint violation on the {target}")
        }
        MappedError::ValueOutOfRange { cause } => {
            format!("Value out of range for the type: {cause}")
        }
        MappedError::TableDoesNotExist { table } => {
            let table = table.as_deref().unwrap_or("(not available)");
            format!("The table `{table}` does not exist in the current database.")
        }
        MappedError::ColumnNotFound { column } => {
            let column = column.as_deref().unwrap_or("(not available)");
            format!("The column `{column}` does not exist in the current database.")
        }
        MappedError::InvalidIsolationLevel { level } => {
            format!("Error in connector: Conversion error: {level}")
        }
        MappedError::InconsistentColumnData { cause } => {
            format!("Inconsistent column data: {cause}")
        }
        MappedError::MissingFullTextSearchIndex => {
            "Cannot find a fulltext index to use for the native search, try adding a @@fulltext([Fields...]) to your schema".to_string()
        }
        MappedError::TransactionWriteConflict => {
            "Transaction failed due to a write conflict or a deadlock. Please retry your transaction".to_string()
        }
        MappedError::GenericJs { id } => {
            format!("Error in external connector (id {id})")
        }
        MappedError::TooManyConnections { cause } => {
            format!("Too many database connections opened: {cause}")
        }
        MappedError::InvalidInputValue { message } => {
            format!("Invalid input value: {message}")
        }
        MappedError::Postgres { .. }
        | MappedError::Mysql { .. }
        | MappedError::Sqlite { .. }
        | MappedError::DuckDb { .. } => {
            return None;
        }
    };
    Some(msg)
}

use crate::ConstraintTarget;

fn render_constraint(constraint: Option<&ConstraintTarget>) -> String {
    match constraint {
        Some(target) => target.render(),
        None => "(not available)".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_match_ts() {
        let cases: Vec<(MappedError, &str)> = vec![
            (MappedError::AuthenticationFailed { user: None }, "P1000"),
            (MappedError::DatabaseNotReachable { host: None, port: None }, "P1001"),
            (MappedError::DatabaseDoesNotExist { db: None }, "P1003"),
            (MappedError::SocketTimeout, "P1008"),
            (MappedError::DatabaseAlreadyExists { db: None }, "P1009"),
            (MappedError::DatabaseAccessDenied { db: None }, "P1010"),
            (MappedError::TlsConnectionError { reason: String::new() }, "P1011"),
            (MappedError::ConnectionClosed, "P1017"),
            (MappedError::TransactionAlreadyClosed { cause: String::new() }, "P1018"),
            (MappedError::LengthMismatch { column: None }, "P2000"),
            (MappedError::UniqueConstraintViolation { constraint: None }, "P2002"),
            (MappedError::ForeignKeyConstraintViolation { constraint: None }, "P2003"),
            (MappedError::InvalidInputValue { message: String::new() }, "P2007"),
            (
                MappedError::UnsupportedNativeDataType { r#type: String::new() },
                "P2010",
            ),
            (MappedError::NullConstraintViolation { constraint: None }, "P2011"),
            (MappedError::ValueOutOfRange { cause: String::new() }, "P2020"),
            (MappedError::TableDoesNotExist { table: None }, "P2021"),
            (MappedError::ColumnNotFound { column: None }, "P2022"),
            (MappedError::InvalidIsolationLevel { level: String::new() }, "P2023"),
            (MappedError::InconsistentColumnData { cause: String::new() }, "P2023"),
            (MappedError::MissingFullTextSearchIndex, "P2030"),
            (MappedError::TransactionWriteConflict, "P2034"),
            (MappedError::GenericJs { id: 0 }, "P2036"),
            (MappedError::TooManyConnections { cause: String::new() }, "P2037"),
        ];

        for (mapped, expected_code) in cases {
            let code = error_code(&mapped);
            assert_eq!(code.as_deref(), Some(expected_code), "Mismatch for {mapped:?}");
        }
    }

    #[test]
    fn raw_db_errors_have_no_code() {
        assert!(
            error_code(&MappedError::Postgres {
                code: "42P01".into(),
                severity: "ERROR".into(),
                message: "table not found".into(),
                detail: None,
                column: None,
                hint: None,
            })
            .is_none()
        );

        assert!(
            error_code(&MappedError::Mysql {
                code: 1146,
                message: "table not found".into(),
                state: "42S02".into(),
                cause: None,
            })
            .is_none()
        );

        assert!(
            error_code(&MappedError::Sqlite {
                extended_code: 1,
                message: "table not found".into(),
            })
            .is_none()
        );
    }

    #[test]
    fn user_facing_error_messages() {
        let err = DriverError::new(MappedError::UniqueConstraintViolation {
            constraint: Some(ConstraintTarget::Fields {
                fields: vec!["email".into()],
            }),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2002");
        assert_eq!(uf.message, "Unique constraint failed on the fields: (`email`)");
    }

    #[test]
    fn raw_error_formatting() {
        let err = DriverError::new(MappedError::SocketTimeout).with_original("ETIMEDOUT", "Connection timed out");
        let uf = to_user_facing_raw_error(&err);
        assert_eq!(uf.code, "P2010");
        assert!(uf.message.contains("ETIMEDOUT"));
        assert!(uf.message.contains("Connection timed out"));
    }

    #[test]
    fn raw_error_without_original() {
        let err = DriverError::new(MappedError::SocketTimeout);
        let uf = to_user_facing_raw_error(&err);
        assert_eq!(uf.code, "P2010");
        assert!(uf.message.contains("N/A"));
        assert!(uf.message.contains("timed out"));
    }

    #[test]
    fn raw_error_from_raw_db_error() {
        let err = DriverError::new(MappedError::Postgres {
            code: "42P01".into(),
            severity: "ERROR".into(),
            message: "relation \"users\" does not exist".into(),
            detail: None,
            column: None,
            hint: None,
        })
        .with_original("42P01", "relation \"users\" does not exist");
        let uf = to_user_facing_raw_error(&err);
        assert_eq!(uf.code, "P2010");
        assert!(uf.message.contains("42P01"));
    }

    #[test]
    fn error_messages_with_details() {
        // Authentication with user
        let err = DriverError::new(MappedError::AuthenticationFailed {
            user: Some("admin".into()),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P1000");
        assert!(uf.message.contains("admin"));

        // Authentication without user
        let err = DriverError::new(MappedError::AuthenticationFailed { user: None });
        let uf = to_user_facing_error(&err).unwrap();
        assert!(uf.message.contains("(not available)"));
    }

    #[test]
    fn error_messages_database_not_reachable() {
        let err = DriverError::new(MappedError::DatabaseNotReachable {
            host: Some("db.example.com".into()),
            port: Some(5432),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P1001");
        assert!(uf.message.contains("db.example.com:5432"));

        // Host only
        let err = DriverError::new(MappedError::DatabaseNotReachable {
            host: Some("db.example.com".into()),
            port: None,
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert!(uf.message.contains("db.example.com"));
        assert!(!uf.message.contains(':'));

        // No info
        let err = DriverError::new(MappedError::DatabaseNotReachable { host: None, port: None });
        let uf = to_user_facing_error(&err).unwrap();
        assert!(uf.message.contains("Can't reach database server"));
    }

    #[test]
    fn error_messages_constraint_violations() {
        // Unique with index constraint
        let err = DriverError::new(MappedError::UniqueConstraintViolation {
            constraint: Some(ConstraintTarget::Index {
                index: "User_email_key".into(),
            }),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2002");
        assert!(uf.message.contains("User_email_key"));

        // Unique with no constraint info
        let err = DriverError::new(MappedError::UniqueConstraintViolation { constraint: None });
        let uf = to_user_facing_error(&err).unwrap();
        assert!(uf.message.contains("(not available)"));

        // Foreign key with constraint name (renders as generic "foreign key")
        let err = DriverError::new(MappedError::ForeignKeyConstraintViolation {
            constraint: Some(ConstraintTarget::ForeignKey {
                foreign_key: "Post_authorId_fkey".into(),
            }),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2003");
        assert!(uf.message.contains("foreign key"));

        // Null constraint with field
        let err = DriverError::new(MappedError::NullConstraintViolation {
            constraint: Some(ConstraintTarget::Fields {
                fields: vec!["name".into()],
            }),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2011");
        assert!(uf.message.contains("name"));
    }

    #[test]
    fn error_messages_table_and_column() {
        let err = DriverError::new(MappedError::TableDoesNotExist {
            table: Some("users".into()),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2021");
        assert!(uf.message.contains("`users`"));

        let err = DriverError::new(MappedError::ColumnNotFound {
            column: Some("age".into()),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2022");
        assert!(uf.message.contains("`age`"));
    }

    #[test]
    fn error_messages_miscellaneous() {
        let err = DriverError::new(MappedError::TooManyConnections {
            cause: "max_connections exceeded".into(),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2037");
        assert!(uf.message.contains("max_connections exceeded"));

        let err = DriverError::new(MappedError::TransactionWriteConflict);
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2034");
        assert!(uf.message.contains("retry"));

        let err = DriverError::new(MappedError::ValueOutOfRange {
            cause: "integer overflow".into(),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2020");
        assert!(uf.message.contains("integer overflow"));

        let err = DriverError::new(MappedError::LengthMismatch {
            column: Some("bio".into()),
        });
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2000");
        assert!(uf.message.contains("bio"));

        let err = DriverError::new(MappedError::MissingFullTextSearchIndex);
        let uf = to_user_facing_error(&err).unwrap();
        assert_eq!(uf.code, "P2030");
        assert!(uf.message.contains("fulltext"));
    }

    #[test]
    fn error_display_formatting() {
        let uf = UserFacingError {
            code: "P2002".into(),
            message: "Unique constraint failed".into(),
        };
        assert_eq!(uf.to_string(), "P2002: Unique constraint failed");
    }

    #[test]
    fn all_mapped_variants_have_codes() {
        let all_variants: Vec<MappedError> = vec![
            MappedError::GenericJs { id: 0 },
            MappedError::UnsupportedNativeDataType { r#type: String::new() },
            MappedError::InvalidIsolationLevel { level: String::new() },
            MappedError::LengthMismatch { column: None },
            MappedError::UniqueConstraintViolation { constraint: None },
            MappedError::NullConstraintViolation { constraint: None },
            MappedError::ForeignKeyConstraintViolation { constraint: None },
            MappedError::DatabaseNotReachable { host: None, port: None },
            MappedError::DatabaseDoesNotExist { db: None },
            MappedError::DatabaseAlreadyExists { db: None },
            MappedError::DatabaseAccessDenied { db: None },
            MappedError::ConnectionClosed,
            MappedError::TlsConnectionError { reason: String::new() },
            MappedError::AuthenticationFailed { user: None },
            MappedError::TransactionWriteConflict,
            MappedError::TableDoesNotExist { table: None },
            MappedError::ColumnNotFound { column: None },
            MappedError::TooManyConnections { cause: String::new() },
            MappedError::ValueOutOfRange { cause: String::new() },
            MappedError::InvalidInputValue { message: String::new() },
            MappedError::MissingFullTextSearchIndex,
            MappedError::SocketTimeout,
            MappedError::InconsistentColumnData { cause: String::new() },
            MappedError::TransactionAlreadyClosed { cause: String::new() },
        ];

        for variant in &all_variants {
            assert!(
                error_code(variant).is_some(),
                "MappedError variant {variant:?} should have a P-code"
            );
            assert!(
                error_message(variant).is_some(),
                "MappedError variant {variant:?} should have a message"
            );
        }
    }

    use crate::DriverError;
}
