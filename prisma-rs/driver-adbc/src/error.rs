use prisma_driver_core::{DriverError, MappedError};

pub fn convert_adbc_error(err: &adbc_core::error::Error) -> DriverError {
    let message = err.message.clone();
    let sqlstate = sqlstate_to_string(&err.sqlstate);

    // Map common SQLSTATE classes to MappedError variants.
    let mapped = match sqlstate.as_str() {
        "23505" => MappedError::UniqueConstraintViolation { constraint: None },
        "23502" => MappedError::NullConstraintViolation { constraint: None },
        "23503" => MappedError::ForeignKeyConstraintViolation { constraint: None },
        "42P01" => MappedError::TableDoesNotExist {
            table: extract_quoted(&message),
        },
        "42703" => MappedError::ColumnNotFound {
            column: extract_quoted(&message),
        },
        "28P01" | "28000" => MappedError::AuthenticationFailed { user: None },
        "08001" | "08006" => MappedError::ConnectionClosed,
        _ => {
            // Fallback: try to detect from message text
            if message.contains("unique constraint") || message.contains("Duplicate key") {
                MappedError::UniqueConstraintViolation { constraint: None }
            } else if message.contains("not-null constraint") || message.contains("NOT NULL") {
                MappedError::NullConstraintViolation { constraint: None }
            } else if message.contains("foreign key constraint") {
                MappedError::ForeignKeyConstraintViolation { constraint: None }
            } else {
                MappedError::DuckDb {
                    message: message.clone(),
                }
            }
        }
    };

    let code = if sqlstate.is_empty() {
        err.vendor_code.to_string()
    } else {
        sqlstate
    };
    DriverError::new(mapped).with_original(code, message)
}

fn sqlstate_to_string(sqlstate: &[std::ffi::c_char; 5]) -> String {
    sqlstate.iter().take_while(|&&c| c != 0).map(|&c| c as char).collect()
}

fn extract_quoted(msg: &str) -> Option<String> {
    let start = msg.find('"')?;
    let rest = &msg[start + 1..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_quoted_basic() {
        assert_eq!(extract_quoted(r#"table "users" not found"#), Some("users".into()),);
        assert_eq!(extract_quoted("no quotes here"), None);
    }

    #[test]
    fn sqlstate_conversion() {
        let state: [std::ffi::c_char; 5] = [b'2' as _, b'3' as _, b'5' as _, b'0' as _, b'5' as _];
        assert_eq!(sqlstate_to_string(&state), "23505");

        let empty: [std::ffi::c_char; 5] = [0; 5];
        assert_eq!(sqlstate_to_string(&empty), "");
    }
}
