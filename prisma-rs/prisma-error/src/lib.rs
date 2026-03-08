//! Shared error types bridging prisma-driver-core with prisma-engines.
//!
//! Provides conversions between our driver `DriverError`/`MappedError` types
//! and the prisma-engines `user_facing_errors` crate.

pub use user_facing_errors;

use prisma_driver_core::{DriverError, MappedError};
use user_facing_errors::KnownError;

/// Convert a `DriverError` from our driver layer into a prisma-engines `KnownError`.
pub fn driver_error_to_known(error: &DriverError) -> Option<KnownError> {
    match &error.mapped {
        MappedError::AuthenticationFailed { user } => Some(KnownError::new(
            user_facing_errors::common::IncorrectDatabaseCredentials {
                database_user: user.clone().unwrap_or_default(),
            },
        )),
        MappedError::DatabaseNotReachable { host, port } => {
            let location = match (host, port) {
                (Some(h), Some(p)) => format!("{h}:{p}"),
                (Some(h), None) => h.clone(),
                _ => "unknown".into(),
            };
            Some(KnownError::new(user_facing_errors::common::DatabaseNotReachable {
                database_location: location,
            }))
        }
        MappedError::DatabaseDoesNotExist { db } => {
            Some(KnownError::new(user_facing_errors::common::DatabaseDoesNotExist {
                database_name: db.clone().unwrap_or_default(),
            }))
        }
        MappedError::DatabaseAlreadyExists { db } => {
            Some(KnownError::new(user_facing_errors::common::DatabaseAlreadyExists {
                database_name: db.clone().unwrap_or_default(),
            }))
        }
        MappedError::DatabaseAccessDenied { db } => {
            Some(KnownError::new(user_facing_errors::common::DatabaseAccessDenied {
                database_name: db.clone().unwrap_or_default(),
            }))
        }
        MappedError::TlsConnectionError { reason } => {
            Some(KnownError::new(user_facing_errors::common::TlsConnectionError {
                message: reason.clone(),
            }))
        }
        MappedError::ConnectionClosed => Some(KnownError::new(user_facing_errors::common::ConnectionClosed)),
        _ => None,
    }
}

/// Convert a prisma-engines `KnownError` into a `DriverError`.
pub fn known_to_driver_error(known: &KnownError) -> DriverError {
    let code: &str = &known.error_code;
    let mapped = match code {
        "P1000" => MappedError::AuthenticationFailed { user: None },
        "P1001" => MappedError::DatabaseNotReachable { host: None, port: None },
        "P1003" => MappedError::DatabaseDoesNotExist { db: None },
        "P1008" => MappedError::SocketTimeout,
        "P1009" => MappedError::DatabaseAlreadyExists { db: None },
        "P1010" => MappedError::DatabaseAccessDenied { db: None },
        "P1011" => MappedError::TlsConnectionError {
            reason: known.message.clone(),
        },
        "P1017" => MappedError::ConnectionClosed,
        _ => MappedError::Postgres {
            code: known.error_code.to_string(),
            severity: String::new(),
            message: known.message.clone(),
            detail: None,
            column: None,
            hint: None,
        },
    };
    DriverError::new(mapped)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- driver_error_to_known ---

    #[test]
    fn driver_to_known_auth_error() {
        let err = DriverError::new(MappedError::AuthenticationFailed {
            user: Some("admin".into()),
        });
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1000");
        assert!(known.message.contains("admin"));
    }

    #[test]
    fn driver_to_known_auth_error_no_user() {
        let err = DriverError::new(MappedError::AuthenticationFailed { user: None });
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1000");
    }

    #[test]
    fn driver_to_known_not_reachable() {
        let err = DriverError::new(MappedError::DatabaseNotReachable {
            host: Some("db.example.com".into()),
            port: Some(5432),
        });
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1001");
    }

    #[test]
    fn driver_to_known_db_does_not_exist() {
        let err = DriverError::new(MappedError::DatabaseDoesNotExist {
            db: Some("mydb".into()),
        });
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1003");
    }

    #[test]
    fn driver_to_known_db_already_exists() {
        let err = DriverError::new(MappedError::DatabaseAlreadyExists {
            db: Some("mydb".into()),
        });
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1009");
    }

    #[test]
    fn driver_to_known_access_denied() {
        let err = DriverError::new(MappedError::DatabaseAccessDenied {
            db: Some("mydb".into()),
        });
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1010");
    }

    #[test]
    fn driver_to_known_tls_error() {
        let err = DriverError::new(MappedError::TlsConnectionError {
            reason: "cert expired".into(),
        });
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1011");
    }

    #[test]
    fn driver_to_known_connection_closed() {
        let err = DriverError::new(MappedError::ConnectionClosed);
        let known = driver_error_to_known(&err).unwrap();
        assert_eq!(known.error_code, "P1017");
    }

    // --- known_to_driver_error ---

    #[test]
    fn known_to_driver_roundtrip() {
        let known = KnownError::new(user_facing_errors::common::ConnectionClosed);
        assert_eq!(known.error_code, "P1017");

        let driver_err = known_to_driver_error(&known);
        assert!(matches!(driver_err.mapped, MappedError::ConnectionClosed));
    }

    #[test]
    fn known_to_driver_p1000() {
        let known = KnownError::new(user_facing_errors::common::IncorrectDatabaseCredentials {
            database_user: "root".into(),
        });
        let driver_err = known_to_driver_error(&known);
        assert!(matches!(driver_err.mapped, MappedError::AuthenticationFailed { .. }));
    }

    #[test]
    fn known_to_driver_p1001() {
        let known = KnownError::new(user_facing_errors::common::DatabaseNotReachable {
            database_location: "localhost:5432".into(),
        });
        let driver_err = known_to_driver_error(&known);
        assert!(matches!(driver_err.mapped, MappedError::DatabaseNotReachable { .. }));
    }

    #[test]
    fn known_to_driver_p1003() {
        let known = KnownError::new(user_facing_errors::common::DatabaseDoesNotExist {
            database_name: "mydb".into(),
        });
        let driver_err = known_to_driver_error(&known);
        assert!(matches!(driver_err.mapped, MappedError::DatabaseDoesNotExist { .. }));
    }

    #[test]
    fn known_to_driver_p1008() {
        let known = KnownError {
            error_code: std::borrow::Cow::Borrowed("P1008"),
            message: "timeout".into(),
            meta: serde_json::Value::Null,
        };
        let driver_err = known_to_driver_error(&known);
        assert!(matches!(driver_err.mapped, MappedError::SocketTimeout));
    }

    #[test]
    fn known_to_driver_p1009() {
        let known = KnownError::new(user_facing_errors::common::DatabaseAlreadyExists {
            database_name: "mydb".into(),
        });
        let driver_err = known_to_driver_error(&known);
        assert!(matches!(driver_err.mapped, MappedError::DatabaseAlreadyExists { .. }));
    }

    #[test]
    fn known_to_driver_p1010() {
        let known = KnownError::new(user_facing_errors::common::DatabaseAccessDenied {
            database_name: "mydb".into(),
        });
        let driver_err = known_to_driver_error(&known);
        assert!(matches!(driver_err.mapped, MappedError::DatabaseAccessDenied { .. }));
    }

    #[test]
    fn known_to_driver_unknown_code_fallback() {
        let known = KnownError {
            error_code: std::borrow::Cow::Borrowed("P9999"),
            message: "some unknown error".into(),
            meta: serde_json::Value::Null,
        };
        let driver_err = known_to_driver_error(&known);
        assert!(
            matches!(driver_err.mapped, MappedError::Postgres { ref code, .. } if code == "P9999"),
            "got: {:?}",
            driver_err.mapped
        );
    }

    // --- raw errors ---

    #[test]
    fn raw_errors_return_none() {
        let err = DriverError::new(MappedError::Postgres {
            code: "42P01".into(),
            severity: "ERROR".into(),
            message: "relation does not exist".into(),
            detail: None,
            column: None,
            hint: None,
        });
        assert!(driver_error_to_known(&err).is_none());
    }

    #[test]
    fn raw_mysql_errors_return_none() {
        let err = DriverError::new(MappedError::Mysql {
            code: 9999,
            message: "unknown error".into(),
            state: "HY000".into(),
            cause: None,
        });
        assert!(driver_error_to_known(&err).is_none());
    }

    #[test]
    fn raw_sqlite_errors_return_none() {
        let err = DriverError::new(MappedError::Sqlite {
            extended_code: 999,
            message: "unknown error".into(),
        });
        assert!(driver_error_to_known(&err).is_none());
    }

    #[test]
    fn unmapped_variants_return_none() {
        let unmapped = [
            MappedError::UniqueConstraintViolation { constraint: None },
            MappedError::TransactionWriteConflict,
            MappedError::SocketTimeout,
            MappedError::TableDoesNotExist { table: None },
        ];
        for mapped in &unmapped {
            let err = DriverError::new(mapped.clone());
            // These are P2xxx errors, not handled by driver_error_to_known
            // (which only maps P1xxx). They should return None.
            let result = driver_error_to_known(&err);
            assert!(result.is_none(), "Expected None for {mapped:?}, got {:?}", result);
        }
    }
}
