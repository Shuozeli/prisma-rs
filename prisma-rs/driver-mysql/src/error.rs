use mysql_async::Error as MysqlError;
use prisma_driver_core::{ConstraintTarget, DriverError, MappedError, SafeMessage};

/// Convert a `mysql_async::Error` into a Prisma `DriverError`.
pub fn convert_mysql_error(err: &MysqlError) -> DriverError {
    match err {
        MysqlError::Server(server_err) => {
            let code = server_err.code;
            let message = server_err.message.clone();
            let state = server_err.state.clone();

            let mapped = match code {
                1062 => MappedError::UniqueConstraintViolation {
                    constraint: parse_mysql_index_from_message(&message),
                },
                1451 | 1452 => MappedError::ForeignKeyConstraintViolation {
                    constraint: parse_mysql_fk_from_message(&message),
                },
                1263 => MappedError::NullConstraintViolation { constraint: None },
                1364 | 1048 => MappedError::NullConstraintViolation {
                    constraint: parse_mysql_field_from_message(&message),
                },
                1264 => MappedError::ValueOutOfRange { cause: message.clone() },
                1049 => MappedError::DatabaseDoesNotExist {
                    db: parse_quoted_value(&message),
                },
                1007 => MappedError::DatabaseAlreadyExists {
                    db: parse_quoted_value(&message),
                },
                1044 => MappedError::DatabaseAccessDenied {
                    db: parse_quoted_value(&message),
                },
                1045 => MappedError::AuthenticationFailed {
                    user: parse_mysql_user(&message),
                },
                1146 => MappedError::TableDoesNotExist {
                    table: parse_quoted_value(&message),
                },
                1054 => MappedError::ColumnNotFound {
                    column: parse_quoted_value(&message),
                },
                1406 => MappedError::LengthMismatch {
                    column: parse_quoted_value(&message),
                },
                1191 => MappedError::MissingFullTextSearchIndex,
                1213 => MappedError::TransactionWriteConflict,
                1040 | 1203 => MappedError::TooManyConnections { cause: message.clone() },
                _ => MappedError::Mysql {
                    code: code as u32,
                    message: message.clone(),
                    state: state.clone(),
                    cause: None,
                },
            };

            DriverError::new(mapped).with_original(code.to_string(), message)
        }
        MysqlError::Io(io_err) => {
            let message = io_err.to_string();
            let mapped = if message.contains("Connection refused") {
                MappedError::DatabaseNotReachable { host: None, port: None }
            } else if message.contains("timed out") {
                MappedError::SocketTimeout
            } else {
                MappedError::ConnectionClosed
            };
            DriverError::new(mapped).with_original("IO", message)
        }
        other => {
            // Catch-all: raw driver messages may contain connection URLs
            // with credentials. Mark the entire message as a secret.
            let raw = other.to_string();
            let safe_msg = SafeMessage::new("MySQL error: {0}").secret(raw);
            DriverError::new(MappedError::Mysql {
                code: 0,
                message: "connection error".to_string(),
                state: String::new(),
                cause: None,
            })
            .with_safe_message("", safe_msg)
        }
    }
}

/// Parse index name from MySQL duplicate key message:
/// "Duplicate entry 'foo' for key 'User_email_key'"
fn parse_mysql_index_from_message(message: &str) -> Option<ConstraintTarget> {
    let marker = "for key '";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('\'')?;
    Some(ConstraintTarget::Index {
        index: rest[..end].to_string(),
    })
}

/// Parse foreign key constraint from MySQL FK violation message.
fn parse_mysql_fk_from_message(message: &str) -> Option<ConstraintTarget> {
    let marker = "CONSTRAINT `";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('`')?;
    Some(ConstraintTarget::ForeignKey {
        foreign_key: rest[..end].to_string(),
    })
}

/// Parse field name from MySQL null constraint message:
/// "Field 'name' doesn't have a default value"
fn parse_mysql_field_from_message(message: &str) -> Option<ConstraintTarget> {
    let marker = "Field '";
    let start = message.find(marker).or_else(|| {
        // "Column 'name' cannot be null"
        message.find("Column '")
    })?;
    let offset = if message[start..].starts_with("Field '") { 7 } else { 8 };
    let rest = &message[start + offset..];
    let end = rest.find('\'')?;
    Some(ConstraintTarget::Fields {
        fields: vec![rest[..end].to_string()],
    })
}

/// Parse a single-quoted value from a message.
fn parse_quoted_value(message: &str) -> Option<String> {
    let start = message.find('\'')?;
    let rest = &message[start + 1..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

/// Parse user from MySQL auth failure message:
/// "Access denied for user 'root'@'localhost'"
fn parse_mysql_user(message: &str) -> Option<String> {
    let marker = "user '";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_mysql_index_from_message ---

    #[test]
    fn parse_unique_index() {
        let msg = "Duplicate entry 'foo@bar.com' for key 'User_email_key'";
        let target = parse_mysql_index_from_message(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Index { index } if index == "User_email_key"));
    }

    #[test]
    fn parse_unique_index_primary() {
        let msg = "Duplicate entry '1' for key 'PRIMARY'";
        let target = parse_mysql_index_from_message(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Index { index } if index == "PRIMARY"));
    }

    #[test]
    fn parse_unique_index_compound() {
        let msg = "Duplicate entry 'foo-bar' for key 'User_email_name_key'";
        let target = parse_mysql_index_from_message(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Index { index } if index == "User_email_name_key"));
    }

    #[test]
    fn parse_unique_index_no_match() {
        assert!(parse_mysql_index_from_message("some other error").is_none());
    }

    // --- parse_mysql_fk_from_message ---

    #[test]
    fn parse_fk_constraint() {
        let msg = "Cannot add or update a child row: a foreign key constraint fails (`db`.`Post`, CONSTRAINT `Post_authorId_fkey` FOREIGN KEY (`authorId`) REFERENCES `User` (`id`))";
        let target = parse_mysql_fk_from_message(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::ForeignKey { foreign_key } if foreign_key == "Post_authorId_fkey"));
    }

    #[test]
    fn parse_fk_constraint_delete() {
        let msg = "Cannot delete or update a parent row: a foreign key constraint fails (`db`.`Comment`, CONSTRAINT `Comment_postId_fkey` FOREIGN KEY (`postId`) REFERENCES `Post` (`id`))";
        let target = parse_mysql_fk_from_message(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::ForeignKey { foreign_key } if foreign_key == "Comment_postId_fkey"));
    }

    #[test]
    fn parse_fk_no_match() {
        assert!(parse_mysql_fk_from_message("some other error").is_none());
    }

    // --- parse_mysql_field_from_message ---

    #[test]
    fn parse_null_field() {
        let msg = "Field 'name' doesn't have a default value";
        let target = parse_mysql_field_from_message(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["name"]));
    }

    #[test]
    fn parse_null_column() {
        let msg = "Column 'email' cannot be null";
        let target = parse_mysql_field_from_message(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["email"]));
    }

    #[test]
    fn parse_null_field_no_match() {
        assert!(parse_mysql_field_from_message("some other error").is_none());
    }

    // --- parse_quoted_value ---

    #[test]
    fn parse_quoted_single() {
        assert_eq!(parse_quoted_value("Unknown database 'mydb'"), Some("mydb".into()));
    }

    #[test]
    fn parse_quoted_special_chars() {
        assert_eq!(
            parse_quoted_value("Unknown database 'my-db_123'"),
            Some("my-db_123".into())
        );
    }

    #[test]
    fn parse_quoted_no_quotes() {
        assert!(parse_quoted_value("no quotes here").is_none());
    }

    #[test]
    fn parse_quoted_empty() {
        assert_eq!(parse_quoted_value("value ''"), Some("".into()));
    }

    // --- parse_mysql_user ---

    #[test]
    fn parse_user_from_auth_error() {
        let msg = "Access denied for user 'root'@'localhost'";
        assert_eq!(parse_mysql_user(msg), Some("root".into()));
    }

    #[test]
    fn parse_user_with_host() {
        let msg = "Access denied for user 'admin'@'192.168.1.1'";
        assert_eq!(parse_mysql_user(msg), Some("admin".into()));
    }

    #[test]
    fn parse_user_no_match() {
        assert!(parse_mysql_user("connection refused").is_none());
    }
}
