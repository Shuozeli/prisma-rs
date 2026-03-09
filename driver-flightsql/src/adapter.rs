use std::sync::Arc;

use arrow_flight::FlightInfo;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use prisma_driver_adbc::arrow::record_batches_to_result_set;
use prisma_driver_core::{
    ConnectionInfo, DriverError, IsolationLevel, MappedError, Provider, SqlDriverAdapter, SqlDriverAdapterFactory,
    SqlQuery, SqlQueryable, SqlResultSet, Transaction, TransactionOptions, static_sql,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::error::{convert_arrow_error, convert_flight_error};

/// Arrow Flight SQL driver adapter.
///
/// Connects to a Flight SQL server via gRPC and executes queries using
/// the Flight SQL protocol. Results are received as Arrow RecordBatches
/// and converted to Prisma types using the shared arrow conversion layer.
pub struct FlightSqlDriverAdapter {
    client: Arc<Mutex<FlightSqlServiceClient<Channel>>>,
    provider: Provider,
}

impl FlightSqlDriverAdapter {
    pub fn new(client: FlightSqlServiceClient<Channel>, provider: Provider) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            provider,
        }
    }
}

async fn flight_query(
    client: &Mutex<FlightSqlServiceClient<Channel>>,
    sql: &str,
    args: &[prisma_driver_core::QueryValue],
) -> Result<SqlResultSet, DriverError> {
    let mut client = client.lock().await;

    let flight_info = if args.is_empty() {
        client
            .execute(sql.to_string(), None)
            .await
            .map_err(|e| convert_arrow_error(&e))?
    } else {
        let mut prepared = client
            .prepare(sql.to_string(), None)
            .await
            .map_err(|e| convert_arrow_error(&e))?;
        let params = prisma_driver_adbc::arrow::query_values_to_record_batch(args);
        prepared.set_parameters(params).map_err(|e| convert_arrow_error(&e))?;
        prepared.execute().await.map_err(|e| convert_arrow_error(&e))?
    };

    let batches = collect_flight_batches(&mut client, flight_info).await?;
    Ok(record_batches_to_result_set(&batches))
}

async fn flight_execute(
    client: &Mutex<FlightSqlServiceClient<Channel>>,
    sql: &str,
    args: &[prisma_driver_core::QueryValue],
) -> Result<u64, DriverError> {
    let mut client = client.lock().await;

    let affected = if args.is_empty() {
        client
            .execute_update(sql.to_string(), None)
            .await
            .map_err(|e| convert_arrow_error(&e))?
    } else {
        let mut prepared = client
            .prepare(sql.to_string(), None)
            .await
            .map_err(|e| convert_arrow_error(&e))?;
        let params = prisma_driver_adbc::arrow::query_values_to_record_batch(args);
        prepared.set_parameters(params).map_err(|e| convert_arrow_error(&e))?;
        prepared.execute_update().await.map_err(|e| convert_arrow_error(&e))?
    };

    Ok(affected as u64)
}

async fn flight_exec_sql(client: &Mutex<FlightSqlServiceClient<Channel>>, sql: &str) -> Result<(), DriverError> {
    let mut client = client.lock().await;
    client
        .execute_update(sql.to_string(), None)
        .await
        .map_err(|e| convert_arrow_error(&e))?;
    Ok(())
}

async fn collect_flight_batches(
    client: &mut FlightSqlServiceClient<Channel>,
    flight_info: FlightInfo,
) -> Result<Vec<arrow_array::RecordBatch>, DriverError> {
    let mut batches = Vec::new();
    for endpoint in flight_info.endpoint {
        if let Some(ticket) = endpoint.ticket {
            let mut stream = client.do_get(ticket).await.map_err(|e| convert_arrow_error(&e))?;

            while let Some(batch) = stream.try_next().await.map_err(|e| convert_flight_error(&e))? {
                batches.push(batch);
            }
        }
    }
    Ok(batches)
}

#[async_trait]
impl SqlQueryable for FlightSqlDriverAdapter {
    fn provider(&self) -> Provider {
        self.provider
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-flightsql"
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        query.validate()?;
        flight_query(&self.client, &query.sql, &query.args).await
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        query.validate()?;
        flight_execute(&self.client, &query.sql, &query.args).await
    }

    async fn start_transaction(
        &mut self,
        _isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction + Send>, DriverError> {
        flight_exec_sql(&self.client, "BEGIN TRANSACTION").await?;
        Ok(Box::new(FlightSqlTransaction {
            client: self.client.clone(),
            provider: self.provider,
            options: TransactionOptions::default(),
            closed: false,
        }))
    }
}

#[async_trait]
impl SqlDriverAdapter for FlightSqlDriverAdapter {
    async fn execute_script(&mut self, script: &str) -> Result<(), DriverError> {
        for sql in script.split(';') {
            let trimmed = sql.trim();
            if trimmed.is_empty() {
                continue;
            }
            flight_exec_sql(&self.client, trimmed).await?;
        }
        Ok(())
    }

    fn connection_info(&self) -> ConnectionInfo {
        ConnectionInfo {
            schema_name: Some("main".into()),
            max_bind_values: self.provider.max_bind_values(),
            supports_relation_joins: false,
        }
    }

    async fn dispose(&mut self) -> Result<(), DriverError> {
        Ok(())
    }
}

struct FlightSqlTransaction {
    client: Arc<Mutex<FlightSqlServiceClient<Channel>>>,
    provider: Provider,
    options: TransactionOptions,
    closed: bool,
}

#[async_trait]
impl SqlQueryable for FlightSqlTransaction {
    fn provider(&self) -> Provider {
        self.provider
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-flightsql"
    }

    fn is_transaction(&self) -> bool {
        true
    }

    async fn query_raw(&mut self, query: SqlQuery) -> Result<SqlResultSet, DriverError> {
        query.validate()?;
        flight_query(&self.client, &query.sql, &query.args).await
    }

    async fn execute_raw(&mut self, query: SqlQuery) -> Result<u64, DriverError> {
        query.validate()?;
        flight_execute(&self.client, &query.sql, &query.args).await
    }
}

#[async_trait]
impl Transaction for FlightSqlTransaction {
    fn options(&self) -> &TransactionOptions {
        &self.options
    }

    async fn commit(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.closed = true;
            flight_exec_sql(&self.client, "COMMIT").await?;
        }
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), DriverError> {
        if !self.closed {
            self.closed = true;
            flight_exec_sql(&self.client, "ROLLBACK").await?;
        }
        Ok(())
    }

    async fn create_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        flight_exec_sql(&self.client, static_sql!("SAVEPOINT ", name).as_str()).await
    }

    async fn rollback_to_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        flight_exec_sql(&self.client, static_sql!("ROLLBACK TO SAVEPOINT ", name).as_str()).await
    }

    async fn release_savepoint(&mut self, name: &'static str) -> Result<(), DriverError> {
        flight_exec_sql(&self.client, static_sql!("RELEASE SAVEPOINT ", name).as_str()).await
    }
}

impl Drop for FlightSqlTransaction {
    fn drop(&mut self) {
        if !self.closed {
            eprintln!(
                "[prisma-driver-flightsql] WARNING: Transaction dropped without commit/rollback, \
                 attempting async rollback"
            );
            let client = self.client.clone();
            tokio::spawn(async move {
                let mut c = client.lock().await;
                let _ = c.execute_update("ROLLBACK".to_string(), None).await;
            });
        }
    }
}

/// Factory for creating Flight SQL connections.
pub struct FlightSqlDriverAdapterFactory {
    endpoint: String,
    provider: Provider,
}

impl FlightSqlDriverAdapterFactory {
    pub fn new(endpoint: impl Into<String>, provider: Provider) -> Self {
        Self {
            endpoint: endpoint.into(),
            provider,
        }
    }
}

#[async_trait]
impl SqlDriverAdapterFactory for FlightSqlDriverAdapterFactory {
    fn provider(&self) -> Provider {
        self.provider
    }

    fn adapter_name(&self) -> &str {
        "prisma-driver-flightsql"
    }

    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError> {
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| {
                DriverError::new(MappedError::DuckDb {
                    message: format!("invalid endpoint: {e}"),
                })
            })?
            .connect()
            .await
            .map_err(|e| {
                DriverError::new(MappedError::DatabaseNotReachable {
                    host: Some(self.endpoint.clone()),
                    port: None,
                })
                .with_original("CONNECT", e.to_string())
            })?;

        let client = FlightSqlServiceClient::new(channel);
        Ok(Box::new(FlightSqlDriverAdapter::new(client, self.provider)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_creation() {
        let factory = FlightSqlDriverAdapterFactory::new("http://localhost:8815", Provider::DuckDb);
        assert_eq!(factory.provider(), Provider::DuckDb);
        assert_eq!(factory.adapter_name(), "prisma-driver-flightsql");
    }
}
