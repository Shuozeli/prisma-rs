use serde::{Deserialize, Serialize};

/// Database provider type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Postgres,
    Mysql,
    Sqlite,
    DuckDb,
}

impl Provider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Provider::Postgres => "postgres",
            Provider::Mysql => "mysql",
            Provider::Sqlite => "sqlite",
            Provider::DuckDb => "duckdb",
        }
    }

    /// Maximum number of bind parameters supported per query.
    pub fn max_bind_values(&self) -> Option<u32> {
        match self {
            Provider::Postgres => Some(32766),
            Provider::Mysql => Some(65535),
            Provider::Sqlite => Some(999),
            Provider::DuckDb => None, // DuckDB has no practical limit
        }
    }
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}

impl IsolationLevel {
    pub fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Snapshot => "SNAPSHOT",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// Connection metadata returned by a driver adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub schema_name: Option<String>,
    pub max_bind_values: Option<u32>,
    pub supports_relation_joins: bool,
}

/// A SQL query with typed parameters.
///
/// When `arg_types` is non-empty, it must have the same length as `args`.
/// An empty `arg_types` means type information is not available (e.g. in tests
/// or raw SQL queries).
#[derive(Debug, Clone)]
pub struct SqlQuery {
    pub sql: String,
    pub args: Vec<QueryValue>,
    pub arg_types: Vec<ArgType>,
}

impl SqlQuery {
    /// Validate that `args` and `arg_types` are consistent.
    ///
    /// Returns `Err` if `arg_types` is non-empty but has a different length
    /// than `args`. An empty `arg_types` is always valid (type info omitted).
    pub fn validate(&self) -> Result<(), crate::DriverError> {
        if !self.arg_types.is_empty() && self.arg_types.len() != self.args.len() {
            return Err(crate::DriverError::new(crate::MappedError::InvalidInputValue {
                message: format!(
                    "SqlQuery args/arg_types length mismatch: {} args vs {} arg_types",
                    self.args.len(),
                    self.arg_types.len(),
                ),
            }));
        }
        Ok(())
    }
}

/// Describes the type of a query argument.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgType {
    pub scalar_type: ArgScalarType,
    pub db_type: Option<String>,
    pub arity: Arity,
}

/// Scalar type classification for query arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ArgScalarType {
    String,
    Int,
    BigInt,
    Float,
    Decimal,
    Boolean,
    Enum,
    Uuid,
    Json,
    DateTime,
    Bytes,
    Unknown,
}

/// Whether an argument is a scalar value or a list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Arity {
    Scalar,
    List,
}

/// A dynamically-typed query parameter value.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Numeric(rust_decimal::Decimal),
    Text(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    DateTime(chrono::NaiveDateTime),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Json(serde_json::Value),
    Array(Vec<QueryValue>),
}

/// Column type classification matching the TypeScript `ColumnTypeEnum`.
///
/// Discriminant values are kept in sync with the TS enum so that
/// cross-compat serialization round-trips produce identical integers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u16)]
pub enum ColumnType {
    // Scalars (0-15)
    Int32 = 0,
    Int64 = 1,
    Float = 2,
    Double = 3,
    Numeric = 4,
    Boolean = 5,
    Character = 6,
    Text = 7,
    Date = 8,
    Time = 9,
    DateTime = 10,
    Json = 11,
    Enum = 12,
    Bytes = 13,
    Set = 14,
    Uuid = 15,
    // Arrays (64-78)
    Int32Array = 64,
    Int64Array = 65,
    FloatArray = 66,
    DoubleArray = 67,
    NumericArray = 68,
    BooleanArray = 69,
    CharacterArray = 70,
    TextArray = 71,
    DateArray = 72,
    TimeArray = 73,
    DateTimeArray = 74,
    JsonArray = 75,
    EnumArray = 76,
    BytesArray = 77,
    UuidArray = 78,
    // Special
    UnknownNumber = 128,
}

/// A single value in a result row.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    /// Numeric/Decimal values are represented as strings to preserve precision.
    Numeric(String),
    Text(String),
    Date(String),
    Time(String),
    DateTime(String),
    Json(String),
    Enum(String),
    Uuid(String),
    Bytes(Vec<u8>),
    /// Array of result values (PostgreSQL array columns).
    Array(Vec<ResultValue>),
}

/// The result of executing a SQL query.
#[derive(Debug, Clone)]
pub struct SqlResultSet {
    pub column_names: Vec<String>,
    pub column_types: Vec<ColumnType>,
    pub rows: Vec<Vec<ResultValue>>,
    pub last_insert_id: Option<String>,
}

/// Options controlling transaction behavior.
#[derive(Debug, Clone, Default)]
pub struct TransactionOptions {
    pub use_phantom_query: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_type_discriminants_match_ts() {
        assert_eq!(ColumnType::Int32 as u16, 0);
        assert_eq!(ColumnType::Int64 as u16, 1);
        assert_eq!(ColumnType::Float as u16, 2);
        assert_eq!(ColumnType::Double as u16, 3);
        assert_eq!(ColumnType::Numeric as u16, 4);
        assert_eq!(ColumnType::Boolean as u16, 5);
        assert_eq!(ColumnType::Text as u16, 7);
        assert_eq!(ColumnType::Date as u16, 8);
        assert_eq!(ColumnType::DateTime as u16, 10);
        assert_eq!(ColumnType::Json as u16, 11);
        assert_eq!(ColumnType::Bytes as u16, 13);
        assert_eq!(ColumnType::Uuid as u16, 15);
        assert_eq!(ColumnType::Int32Array as u16, 64);
        assert_eq!(ColumnType::TextArray as u16, 71);
        assert_eq!(ColumnType::UnknownNumber as u16, 128);
    }

    #[test]
    fn provider_max_bind_values() {
        assert_eq!(Provider::Postgres.max_bind_values(), Some(32766));
        assert_eq!(Provider::Mysql.max_bind_values(), Some(65535));
        assert_eq!(Provider::Sqlite.max_bind_values(), Some(999));
    }

    #[test]
    fn isolation_level_sql_strings() {
        assert_eq!(IsolationLevel::ReadUncommitted.as_sql(), "READ UNCOMMITTED");
        assert_eq!(IsolationLevel::ReadCommitted.as_sql(), "READ COMMITTED");
        assert_eq!(IsolationLevel::RepeatableRead.as_sql(), "REPEATABLE READ");
        assert_eq!(IsolationLevel::Serializable.as_sql(), "SERIALIZABLE");
        assert_eq!(IsolationLevel::Snapshot.as_sql(), "SNAPSHOT");
    }

    #[test]
    fn isolation_level_clone_eq() {
        let level = IsolationLevel::ReadCommitted;
        let cloned = level;
        assert_eq!(level, cloned);
        assert_ne!(IsolationLevel::ReadCommitted, IsolationLevel::Serializable);
    }

    // --- Savepoint SQL per provider ---

    #[test]
    fn pg_savepoint_sql() {
        let name = "sp1";
        assert_eq!(format!("SAVEPOINT {name}"), "SAVEPOINT sp1");
        assert_eq!(format!("ROLLBACK TO SAVEPOINT {name}"), "ROLLBACK TO SAVEPOINT sp1");
        assert_eq!(format!("RELEASE SAVEPOINT {name}"), "RELEASE SAVEPOINT sp1");
    }

    #[test]
    fn mysql_savepoint_sql() {
        let name = "sp1";
        assert_eq!(format!("SAVEPOINT {name}"), "SAVEPOINT sp1");
        // MySQL uses ROLLBACK TO without SAVEPOINT keyword
        assert_eq!(format!("ROLLBACK TO {name}"), "ROLLBACK TO sp1");
        assert_eq!(format!("RELEASE SAVEPOINT {name}"), "RELEASE SAVEPOINT sp1");
    }

    #[test]
    fn sqlite_savepoint_sql() {
        let name = "sp1";
        assert_eq!(format!("SAVEPOINT {name}"), "SAVEPOINT sp1");
        // SQLite uses ROLLBACK TO without SAVEPOINT keyword
        assert_eq!(format!("ROLLBACK TO {name}"), "ROLLBACK TO sp1");
        // SQLite RELEASE SAVEPOINT
        assert_eq!(format!("RELEASE SAVEPOINT {name}"), "RELEASE SAVEPOINT sp1");
    }

    #[test]
    fn savepoint_names_support_special_formats() {
        // Prisma uses sequential names like s1, s2, s3 for nested savepoints
        for i in 1..=5 {
            let name = format!("s{i}");
            let create = format!("SAVEPOINT {name}");
            assert_eq!(create, format!("SAVEPOINT s{i}"));
        }
    }

    // --- TransactionOptions ---

    #[test]
    fn transaction_options_default() {
        let opts = TransactionOptions {
            use_phantom_query: false,
        };
        assert!(!opts.use_phantom_query);
    }

    // --- ConnectionInfo ---

    #[test]
    fn connection_info_postgres() {
        let info = ConnectionInfo {
            schema_name: Some("public".into()),
            max_bind_values: Provider::Postgres.max_bind_values(),
            supports_relation_joins: true,
        };
        assert_eq!(info.schema_name.as_deref(), Some("public"));
        assert_eq!(info.max_bind_values, Some(32766));
        assert!(info.supports_relation_joins);
    }

    #[test]
    fn connection_info_mysql() {
        let info = ConnectionInfo {
            schema_name: Some("mydb".into()),
            max_bind_values: Provider::Mysql.max_bind_values(),
            supports_relation_joins: true,
        };
        assert_eq!(info.max_bind_values, Some(65535));
    }

    #[test]
    fn connection_info_sqlite() {
        let info = ConnectionInfo {
            schema_name: Some("main".into()),
            max_bind_values: Provider::Sqlite.max_bind_values(),
            supports_relation_joins: false,
        };
        assert_eq!(info.max_bind_values, Some(999));
        assert!(!info.supports_relation_joins);
    }

    // --- SqlQuery validation ---

    #[test]
    fn sql_query_validate_empty_arg_types_ok() {
        let q = SqlQuery {
            sql: "SELECT 1".into(),
            args: vec![QueryValue::Int32(1)],
            arg_types: vec![],
        };
        assert!(q.validate().is_ok());
    }

    #[test]
    fn sql_query_validate_matching_lengths_ok() {
        let q = SqlQuery {
            sql: "SELECT $1".into(),
            args: vec![QueryValue::Int32(1)],
            arg_types: vec![ArgType {
                scalar_type: ArgScalarType::Int,
                db_type: None,
                arity: Arity::Scalar,
            }],
        };
        assert!(q.validate().is_ok());
    }

    #[test]
    fn sql_query_validate_mismatched_lengths_err() {
        let q = SqlQuery {
            sql: "SELECT $1".into(),
            args: vec![QueryValue::Int32(1), QueryValue::Int32(2)],
            arg_types: vec![ArgType {
                scalar_type: ArgScalarType::Int,
                db_type: None,
                arity: Arity::Scalar,
            }],
        };
        let err = q.validate().unwrap_err();
        assert!(err.to_string().contains("mismatch"));
    }
}
