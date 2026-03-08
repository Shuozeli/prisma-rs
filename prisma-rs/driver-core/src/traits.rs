use async_trait::async_trait;

use crate::{
    ConnectionInfo, DriverError, IsolationLevel, MappedError, Provider, SqlQuery, SqlResultSet, TransactionOptions,
};

/// Core query execution interface, implemented by both adapters and transactions.
#[async_trait]
pub trait SqlQueryable: Send + Sync {
    fn provider(&self) -> Provider;
    fn adapter_name(&self) -> &str;

    /// Whether this queryable is an active transaction context.
    ///
    /// Returns `true` for `Transaction` implementations, `false` for adapters.
    /// Used by the query executor to avoid starting nested transactions on
    /// separate connections when already running inside a transaction.
    fn is_transaction(&self) -> bool {
        false
    }

    /// Execute a query and return the result set.
    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError>;

    /// Execute a mutation and return the number of affected rows.
    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError>;

    /// Begin a new transaction, optionally at a specific isolation level.
    ///
    /// Overridden by adapter implementations. The default returns an error,
    /// which is appropriate for transaction contexts (where `is_transaction()`
    /// returns `true` and this method is never called).
    async fn start_transaction(
        &mut self,
        _isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction + Send>, DriverError> {
        Err(DriverError::new(MappedError::InvalidInputValue {
            message: "start_transaction is not supported in this context".into(),
        }))
    }
}

/// An active database transaction with savepoint support.
#[async_trait]
pub trait Transaction: SqlQueryable {
    fn options(&self) -> &TransactionOptions;

    async fn commit(&mut self) -> Result<(), DriverError>;
    async fn rollback(&mut self) -> Result<(), DriverError>;

    async fn create_savepoint(&mut self, name: &'static str) -> Result<(), DriverError>;
    async fn rollback_to_savepoint(&mut self, name: &'static str) -> Result<(), DriverError>;
    async fn release_savepoint(&mut self, name: &'static str) -> Result<(), DriverError>;
}

/// A database connection that can execute queries and start transactions.
#[async_trait]
pub trait SqlDriverAdapter: SqlQueryable {
    /// Execute a multi-statement script (e.g. for migrations).
    async fn execute_script(&mut self, script: &str) -> Result<(), DriverError>;

    /// Return metadata about the connection.
    fn connection_info(&self) -> ConnectionInfo;

    /// Close the connection / release resources.
    async fn dispose(&mut self) -> Result<(), DriverError>;
}

/// Factory for creating database connections.
#[async_trait]
pub trait SqlDriverAdapterFactory: Send + Sync {
    fn provider(&self) -> Provider;
    fn adapter_name(&self) -> &str;

    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError>;
}

/// Extended factory that can also create shadow database connections for migrations.
#[async_trait]
pub trait SqlMigrationAwareDriverAdapterFactory: SqlDriverAdapterFactory {
    async fn connect_to_shadow_db(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError>;
}
