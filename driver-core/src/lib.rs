mod database_url;
mod error;
mod safe_message;
pub mod sql_commenter;
mod static_sql;
mod traits;
mod types;
mod user_facing_error;

pub use database_url::{DatabaseUrl, DatabaseUrlError};
pub use error::*;
pub use safe_message::SafeMessage;
pub use sql_commenter::SqlComment;
pub use static_sql::StaticSql;
pub use traits::*;
pub use types::*;
pub use user_facing_error::*;

/// Log a warning when a transaction is dropped without an explicit commit or rollback.
///
/// All driver `Transaction` implementations should call this in their `Drop` impl
/// to provide consistent, structured logging.
pub fn warn_uncommitted_transaction(adapter_name: &str) {
    tracing::warn!(
        adapter = adapter_name,
        "Transaction dropped without commit/rollback, connection returned with implicit rollback"
    );
}
