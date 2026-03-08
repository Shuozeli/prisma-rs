//! Query event logging for the Prisma client.
//!
//! Emits structured `QueryEvent` records for each database query,
//! enabling observability and debugging. Events are emitted via the
//! `tracing` crate and can optionally be delivered to a callback.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

/// A structured event emitted for each database query.
#[derive(Debug, Clone, Serialize)]
pub struct QueryEvent {
    /// The Prisma model name (e.g., "User"), if applicable.
    pub model: Option<String>,
    /// The operation action (e.g., "findMany", "createOne").
    pub action: String,
    /// The SQL query string that was executed.
    pub query: String,
    /// The query parameters as a JSON string.
    pub params: String,
    /// How long the query took to execute.
    pub duration_ms: u64,
    /// ISO 8601 timestamp of when the query was executed.
    pub timestamp: String,
}

impl QueryEvent {
    pub(crate) fn new(
        model: Option<String>,
        action: String,
        query: String,
        params: String,
        duration: Duration,
    ) -> Self {
        Self {
            model,
            action,
            query,
            params,
            duration_ms: duration.as_millis() as u64,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}

/// Log level for Prisma client events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Log individual SQL queries with timing.
    Query,
    /// Informational messages (connection, disconnection).
    Info,
    /// Warnings (slow queries, deprecation notices).
    Warn,
    /// Errors.
    Error,
}

/// Where to emit log events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogEmit {
    /// Print to stdout.
    Stdout,
    /// Emit as a `tracing` event (for OpenTelemetry integration).
    Event,
}

/// Configuration for a single log level.
#[derive(Debug, Clone)]
pub struct LogDefinition {
    pub level: LogLevel,
    pub emit: LogEmit,
}

/// Callback type for receiving query events programmatically.
pub type QueryEventCallback = Arc<dyn Fn(QueryEvent) + Send + Sync>;

/// Logging configuration for the Prisma client.
#[derive(Clone, Default)]
pub struct LogConfig {
    pub(crate) levels: Vec<LogDefinition>,
    pub(crate) on_query: Option<QueryEventCallback>,
}

impl std::fmt::Debug for LogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogConfig")
            .field("levels", &self.levels)
            .field("on_query", &self.on_query.as_ref().map(|_| "Fn(..)"))
            .finish()
    }
}

impl LogConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable logging at the given level with stdout output.
    pub fn level(mut self, level: LogLevel) -> Self {
        self.levels.push(LogDefinition {
            level,
            emit: LogEmit::Stdout,
        });
        self
    }

    /// Enable logging at the given level with the specified emit mode.
    pub fn log(mut self, level: LogLevel, emit: LogEmit) -> Self {
        self.levels.push(LogDefinition { level, emit });
        self
    }

    /// Register a callback for query events.
    pub fn on_query(mut self, callback: impl Fn(QueryEvent) + Send + Sync + 'static) -> Self {
        self.on_query = Some(Arc::new(callback));
        self
    }

    pub(crate) fn is_query_enabled(&self) -> bool {
        self.levels.iter().any(|l| l.level == LogLevel::Query) || self.on_query.is_some()
    }

    pub(crate) fn emit_query_event(&self, event: QueryEvent) {
        // Deliver to callback
        if let Some(ref cb) = self.on_query {
            cb(event.clone());
        }

        // Emit via configured destinations
        for def in &self.levels {
            if def.level == LogLevel::Query {
                match def.emit {
                    LogEmit::Stdout => {
                        eprintln!("prisma:query {} [{}ms]", event.query, event.duration_ms);
                    }
                    LogEmit::Event => {
                        tracing::info!(
                            target: "prisma:query",
                            model = ?event.model,
                            action = %event.action,
                            query = %event.query,
                            params = %event.params,
                            duration_ms = event.duration_ms,
                            "query"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn log_config_default_is_empty() {
        let config = LogConfig::new();
        assert!(config.levels.is_empty());
        assert!(!config.is_query_enabled());
    }

    #[test]
    fn log_config_with_query_level() {
        let config = LogConfig::new().level(LogLevel::Query);
        assert!(config.is_query_enabled());
    }

    #[test]
    fn log_config_with_callback() {
        let events: Arc<Mutex<Vec<QueryEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();
        let config = LogConfig::new().on_query(move |e| {
            events_clone.lock().unwrap().push(e);
        });
        assert!(config.is_query_enabled());

        let event = QueryEvent::new(
            Some("User".into()),
            "findMany".into(),
            "SELECT * FROM User".into(),
            "[]".into(),
            Duration::from_millis(5),
        );
        config.emit_query_event(event);

        let captured = events.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].action, "findMany");
        assert_eq!(captured[0].duration_ms, 5);
    }

    #[test]
    fn query_event_serializes() {
        let event = QueryEvent::new(
            Some("User".into()),
            "findMany".into(),
            "SELECT * FROM User".into(),
            "[]".into(),
            Duration::from_millis(42),
        );
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["action"], "findMany");
        assert_eq!(json["duration_ms"], 42);
        assert_eq!(json["model"], "User");
    }
}
