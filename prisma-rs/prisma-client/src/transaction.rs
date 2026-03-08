//! Interactive transaction support.
//!
//! `TransactionClient` wraps a database transaction and provides the same
//! compile-and-execute pipeline as `PrismaClient`, but all queries run
//! within the transaction boundary.

use prisma_compiler::QueryCompiler;
use prisma_driver_core::Transaction;
use prisma_query_executor::QueryExecutor;
use serde_json::Value;

use crate::error::ClientError;
use crate::query::QueryBuilder;

/// A client bound to an active database transaction.
///
/// Created via `PrismaClient::transaction()`. All queries executed through
/// this client run within the transaction. Call `commit()` or `rollback()`
/// to finalize.
pub struct TransactionClient<'a> {
    compiler: &'a QueryCompiler,
    tx: Box<dyn Transaction + Send>,
    finalized: bool,
}

impl<'a> TransactionClient<'a> {
    pub(crate) fn new(compiler: &'a QueryCompiler, tx: Box<dyn Transaction + Send>) -> Self {
        Self {
            compiler,
            tx,
            finalized: false,
        }
    }

    /// Execute a query builder within this transaction.
    pub async fn execute(&mut self, query: &QueryBuilder) -> Result<Value, ClientError> {
        let request = query.build();
        self.execute_json(&request).await
    }

    /// Execute a raw JSON protocol request within this transaction.
    pub async fn execute_json(&mut self, request: &Value) -> Result<Value, ClientError> {
        let request_str = serde_json::to_string(request)?;
        let expr = self.compiler.compile_to_ir(&request_str)?;
        let result = QueryExecutor::execute(&expr, self.tx.as_mut()).await?;
        Ok(result.to_json())
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), ClientError> {
        self.finalized = true;
        self.tx.commit().await?;
        Ok(())
    }

    /// Roll back the transaction.
    pub async fn rollback(mut self) -> Result<(), ClientError> {
        self.finalized = true;
        self.tx.rollback().await?;
        Ok(())
    }
}

impl Drop for TransactionClient<'_> {
    fn drop(&mut self) {
        if !self.finalized {
            tracing::warn!(
                "TransactionClient dropped without commit() or rollback(). \
                 The transaction will be rolled back by the driver."
            );
            // The inner Box<dyn Transaction + Send> will be dropped next, triggering
            // the driver-level Drop impl which handles the actual rollback.
        }
    }
}
