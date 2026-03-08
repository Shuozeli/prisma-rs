use prisma_driver_core::{ConstraintTarget, DriverError, MappedError};
use rusqlite::ffi;

/// Convert a `rusqlite::Error` into a Prisma `DriverError`.
pub fn convert_sqlite_error(err: &rusqlite::Error) -> DriverError {
    match err {
        rusqlite::Error::SqliteFailure(sqlite_err, msg) => {
            let extended_code = sqlite_err.extended_code;
            let message = msg.as_deref().unwrap_or(&sqlite_err.to_string()).to_string();
            let original_code = extended_code.to_string();

            let mapped = match extended_code {
                ffi::SQLITE_BUSY => MappedError::SocketTimeout,

                ffi::SQLITE_CONSTRAINT_UNIQUE | ffi::SQLITE_CONSTRAINT_PRIMARYKEY => {
                    MappedError::UniqueConstraintViolation {
                        constraint: parse_sqlite_constraint(&message),
                    }
                }

                ffi::SQLITE_CONSTRAINT_NOTNULL => MappedError::NullConstraintViolation {
                    constraint: parse_sqlite_null_field(&message),
                },

                ffi::SQLITE_CONSTRAINT_FOREIGNKEY | ffi::SQLITE_CONSTRAINT_TRIGGER => {
                    MappedError::ForeignKeyConstraintViolation { constraint: None }
                }

                _ => {
                    // Check message patterns for additional mappings
                    if message.contains("no such table") {
                        MappedError::TableDoesNotExist {
                            table: parse_sqlite_table(&message),
                        }
                    } else if message.contains("no such column") || message.contains("has no column named") {
                        MappedError::ColumnNotFound {
                            column: parse_sqlite_column(&message),
                        }
                    } else {
                        MappedError::Sqlite {
                            extended_code,
                            message: message.clone(),
                        }
                    }
                }
            };

            DriverError::new(mapped).with_original(original_code, message)
        }

        rusqlite::Error::QueryReturnedNoRows => DriverError::new(MappedError::Sqlite {
            extended_code: 0,
            message: "Query returned no rows".to_string(),
        }),

        other => {
            let message = other.to_string();
            // Check for table/column errors in non-failure errors too
            let mapped = if message.contains("no such table") {
                MappedError::TableDoesNotExist {
                    table: parse_sqlite_table(&message),
                }
            } else if message.contains("no such column") || message.contains("has no column named") {
                MappedError::ColumnNotFound {
                    column: parse_sqlite_column(&message),
                }
            } else {
                MappedError::Sqlite {
                    extended_code: 0,
                    message: message.clone(),
                }
            };

            DriverError::new(mapped).with_original("", message)
        }
    }
}

/// Parse constraint info from SQLite unique constraint message:
/// "UNIQUE constraint failed: User.email"
fn parse_sqlite_constraint(message: &str) -> Option<ConstraintTarget> {
    let marker = "UNIQUE constraint failed: ";
    if let Some(start) = message.find(marker) {
        let rest = &message[start + marker.len()..];
        let fields: Vec<String> = rest
            .split(", ")
            .map(|s| {
                // "Table.column" -> "column"
                s.rsplit('.').next().unwrap_or(s).to_string()
            })
            .collect();
        return Some(ConstraintTarget::Fields { fields });
    }
    None
}

/// Parse field from SQLite NOT NULL message:
/// "NOT NULL constraint failed: User.name"
fn parse_sqlite_null_field(message: &str) -> Option<ConstraintTarget> {
    let marker = "NOT NULL constraint failed: ";
    if let Some(start) = message.find(marker) {
        let rest = &message[start + marker.len()..];
        let field = rest.rsplit('.').next().unwrap_or(rest).to_string();
        return Some(ConstraintTarget::Fields { fields: vec![field] });
    }
    None
}

/// Parse table name from "no such table: foo"
fn parse_sqlite_table(message: &str) -> Option<String> {
    let marker = "no such table: ";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    Some(rest.trim().to_string())
}

/// Parse column name from "no such column: foo" or "has no column named foo"
fn parse_sqlite_column(message: &str) -> Option<String> {
    if let Some(start) = message.find("no such column: ") {
        let rest = &message[start + "no such column: ".len()..];
        return Some(rest.trim().to_string());
    }
    if let Some(start) = message.find("has no column named ") {
        let rest = &message[start + "has no column named ".len()..];
        return Some(rest.trim().to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_sqlite_constraint ---

    #[test]
    fn parse_unique_constraint() {
        let msg = "UNIQUE constraint failed: User.email";
        let target = parse_sqlite_constraint(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["email"]));
    }

    #[test]
    fn parse_unique_constraint_composite() {
        let msg = "UNIQUE constraint failed: User.email, User.name";
        let target = parse_sqlite_constraint(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["email", "name"]));
    }

    #[test]
    fn parse_unique_constraint_triple() {
        let msg = "UNIQUE constraint failed: Idx.a, Idx.b, Idx.c";
        let target = parse_sqlite_constraint(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["a", "b", "c"]));
    }

    #[test]
    fn parse_unique_constraint_no_match() {
        assert!(parse_sqlite_constraint("some other error").is_none());
    }

    // --- parse_sqlite_null_field ---

    #[test]
    fn parse_not_null() {
        let msg = "NOT NULL constraint failed: User.name";
        let target = parse_sqlite_null_field(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["name"]));
    }

    #[test]
    fn parse_not_null_different_table() {
        let msg = "NOT NULL constraint failed: Post.title";
        let target = parse_sqlite_null_field(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["title"]));
    }

    #[test]
    fn parse_not_null_no_match() {
        assert!(parse_sqlite_null_field("something else").is_none());
    }

    // --- parse_sqlite_table ---

    #[test]
    fn parse_table_name() {
        assert_eq!(parse_sqlite_table("no such table: users"), Some("users".into()));
    }

    #[test]
    fn parse_table_name_with_spaces() {
        assert_eq!(parse_sqlite_table("no such table:  my_table "), Some("my_table".into()));
    }

    #[test]
    fn parse_table_no_match() {
        assert!(parse_sqlite_table("something else").is_none());
    }

    // --- parse_sqlite_column ---

    #[test]
    fn parse_column_name() {
        assert_eq!(parse_sqlite_column("no such column: age"), Some("age".into()));
    }

    #[test]
    fn parse_column_has_no_column_named() {
        assert_eq!(
            parse_sqlite_column("table foo has no column named bar"),
            Some("bar".into())
        );
    }

    #[test]
    fn parse_column_no_match() {
        assert!(parse_sqlite_column("something else").is_none());
    }

    // --- convert_sqlite_error integration ---

    #[test]
    fn convert_unique_constraint_error() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::ConstraintViolation,
            extended_code: ffi::SQLITE_CONSTRAINT_UNIQUE,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("UNIQUE constraint failed: User.email".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::UniqueConstraintViolation { constraint: Some(ConstraintTarget::Fields { ref fields }) } if fields == &["email"]),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_not_null_constraint_error() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::ConstraintViolation,
            extended_code: ffi::SQLITE_CONSTRAINT_NOTNULL,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("NOT NULL constraint failed: User.name".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::NullConstraintViolation { constraint: Some(ConstraintTarget::Fields { ref fields }) } if fields == &["name"]),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_foreign_key_error() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::ConstraintViolation,
            extended_code: ffi::SQLITE_CONSTRAINT_FOREIGNKEY,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("FOREIGN KEY constraint failed".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(
                driver_err.mapped,
                MappedError::ForeignKeyConstraintViolation { constraint: None }
            ),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_primary_key_error() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::ConstraintViolation,
            extended_code: ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("UNIQUE constraint failed: User.id".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::UniqueConstraintViolation { .. }),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_busy_error() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::DatabaseBusy,
            extended_code: ffi::SQLITE_BUSY,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("database is locked".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::SocketTimeout),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_no_such_table_error() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::Unknown,
            extended_code: 1,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("no such table: users".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::TableDoesNotExist { table: Some(ref t) } if t == "users"),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_no_such_column_error() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::Unknown,
            extended_code: 1,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("no such column: age".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::ColumnNotFound { column: Some(ref c) } if c == "age"),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_query_returned_no_rows() {
        let err = rusqlite::Error::QueryReturnedNoRows;
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::Sqlite { extended_code: 0, .. }),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_non_failure_table_error() {
        let err = rusqlite::Error::SqlInputError {
            error: rusqlite::ffi::Error {
                code: rusqlite::ffi::ErrorCode::Unknown,
                extended_code: 1,
            },
            msg: "no such table: posts".into(),
            sql: "SELECT * FROM posts".into(),
            offset: 0,
        };
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::TableDoesNotExist { table: Some(ref t) } if t.contains("posts")),
            "got: {:?}",
            driver_err.mapped
        );
    }

    #[test]
    fn convert_unknown_error_fallback() {
        let sqlite_err = rusqlite::ffi::Error {
            code: rusqlite::ffi::ErrorCode::InternalMalfunction,
            extended_code: 11,
        };
        let err = rusqlite::Error::SqliteFailure(sqlite_err, Some("internal error".into()));
        let driver_err = convert_sqlite_error(&err);
        assert!(
            matches!(driver_err.mapped, MappedError::Sqlite { extended_code: 11, .. }),
            "got: {:?}",
            driver_err.mapped
        );
    }
}
