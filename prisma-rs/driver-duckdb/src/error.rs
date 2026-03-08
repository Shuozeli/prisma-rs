use prisma_driver_core::{ConstraintTarget, DriverError, MappedError};

pub fn convert_duckdb_error(err: &duckdb::Error) -> DriverError {
    let message = err.to_string();

    let mapped = if message.contains("Constraint Error: Duplicate key") || message.contains("UNIQUE constraint failed")
    {
        MappedError::UniqueConstraintViolation {
            constraint: parse_unique_constraint(&message),
        }
    } else if message.contains("NOT NULL constraint failed") || message.contains("Constraint Error: NOT NULL") {
        MappedError::NullConstraintViolation {
            constraint: parse_null_field(&message),
        }
    } else if message.contains("violates foreign key constraint")
        || message.contains("Constraint Error: Violates foreign key")
    {
        MappedError::ForeignKeyConstraintViolation { constraint: None }
    } else if message.contains("Table with name") && message.contains("does not exist") {
        MappedError::TableDoesNotExist {
            table: parse_table_not_found(&message),
        }
    } else if message.contains("Referenced column") && message.contains("not found") {
        MappedError::ColumnNotFound {
            column: parse_column_not_found(&message),
        }
    } else {
        MappedError::DuckDb {
            message: message.clone(),
        }
    };

    DriverError::new(mapped).with_original("DUCKDB", message)
}

fn parse_unique_constraint(message: &str) -> Option<ConstraintTarget> {
    // DuckDB: "Constraint Error: Duplicate key \"email = alice@test.com\""
    // or: "UNIQUE constraint failed: User.email"
    if let Some(start) = message.find("UNIQUE constraint failed: ") {
        let rest = &message[start + "UNIQUE constraint failed: ".len()..];
        let fields: Vec<String> = rest
            .split(", ")
            .map(|s| s.rsplit('.').next().unwrap_or(s).to_string())
            .collect();
        return Some(ConstraintTarget::Fields { fields });
    }
    None
}

fn parse_null_field(message: &str) -> Option<ConstraintTarget> {
    if let Some(start) = message.find("NOT NULL constraint failed: ") {
        let rest = &message[start + "NOT NULL constraint failed: ".len()..];
        let field = rest.rsplit('.').next().unwrap_or(rest).to_string();
        return Some(ConstraintTarget::Fields { fields: vec![field] });
    }
    None
}

fn parse_table_not_found(message: &str) -> Option<String> {
    // "Table with name X does not exist"
    if let Some(start) = message.find("Table with name ") {
        let rest = &message[start + "Table with name ".len()..];
        if let Some(end) = rest.find(" does not exist") {
            return Some(rest[..end].trim().to_string());
        }
    }
    None
}

fn parse_column_not_found(message: &str) -> Option<String> {
    if let Some(start) = message.find("Referenced column \"") {
        let rest = &message[start + "Referenced column \"".len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_unique_constraint_sqlite_style() {
        let msg = "UNIQUE constraint failed: User.email";
        let target = parse_unique_constraint(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["email"]));
    }

    #[test]
    fn parse_null_field_test() {
        let msg = "NOT NULL constraint failed: User.name";
        let target = parse_null_field(msg).unwrap();
        assert!(matches!(target, ConstraintTarget::Fields { fields } if fields == vec!["name"]));
    }

    #[test]
    fn parse_table_not_found_test() {
        let msg = "Table with name users does not exist";
        assert_eq!(parse_table_not_found(msg), Some("users".into()));
    }

    #[test]
    fn parse_column_not_found_test() {
        let msg = r#"Referenced column "age" not found"#;
        assert_eq!(parse_column_not_found(msg), Some("age".into()));
    }
}
