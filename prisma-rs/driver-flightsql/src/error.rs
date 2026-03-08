use prisma_driver_core::{DriverError, MappedError};

pub fn convert_arrow_error(err: &arrow_schema::ArrowError) -> DriverError {
    map_error_message(&err.to_string())
}

pub fn convert_flight_error(err: &arrow_flight::error::FlightError) -> DriverError {
    map_error_message(&err.to_string())
}

fn map_error_message(message: &str) -> DriverError {
    let mapped = if message.contains("unique constraint") || message.contains("Duplicate key") {
        MappedError::UniqueConstraintViolation { constraint: None }
    } else if message.contains("not-null constraint") || message.contains("NOT NULL") {
        MappedError::NullConstraintViolation { constraint: None }
    } else if message.contains("foreign key constraint") {
        MappedError::ForeignKeyConstraintViolation { constraint: None }
    } else if message.contains("does not exist") {
        MappedError::TableDoesNotExist { table: None }
    } else if message.contains("authentication") || message.contains("Unauthenticated") {
        MappedError::AuthenticationFailed { user: None }
    } else if message.contains("connection") && message.contains("refused") {
        MappedError::DatabaseNotReachable { host: None, port: None }
    } else {
        MappedError::DuckDb {
            message: message.to_string(),
        }
    };

    DriverError::new(mapped).with_original("FLIGHT_SQL", message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_connection_refused() {
        let err = map_error_message("connection refused");
        assert!(matches!(err.mapped, MappedError::DatabaseNotReachable { .. }));
    }

    #[test]
    fn error_auth_failed() {
        let err = map_error_message("Unauthenticated");
        assert!(matches!(err.mapped, MappedError::AuthenticationFailed { .. }));
    }

    #[test]
    fn error_unique_constraint() {
        let err = map_error_message("unique constraint violation");
        assert!(matches!(err.mapped, MappedError::UniqueConstraintViolation { .. }));
    }
}
