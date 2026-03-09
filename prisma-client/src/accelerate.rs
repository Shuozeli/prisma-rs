//! Prisma Accelerate / Data Proxy support.
//!
//! Provides `AccelerateClient` that sends Prisma JSON protocol requests
//! to a Prisma Accelerate endpoint over HTTPS. This enables edge-compatible
//! database access through Prisma's managed proxy service.
//!
//! Unlike `PrismaClient` which compiles and executes queries locally,
//! `AccelerateClient` delegates all work to the Accelerate proxy,
//! which handles connection pooling, caching, and query execution.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{AccelerateErrorDetail, ClientError};
use crate::query::QueryBuilder;

/// Prisma Accelerate client.
///
/// Sends queries to a Prisma Accelerate endpoint over HTTPS.
/// The proxy handles compilation, execution, caching, and connection pooling.
pub struct AccelerateClient {
    client: reqwest::Client,
    endpoint: String,
    api_key: String,
}

/// Accelerate request body.
#[derive(Debug, Serialize)]
struct AccelerateRequest {
    action: String,
    #[serde(rename = "modelName", skip_serializing_if = "Option::is_none")]
    model_name: Option<String>,
    query: Value,
}

/// Accelerate response body.
#[derive(Debug, Deserialize)]
struct AccelerateResponse {
    data: Value,
    #[serde(default)]
    errors: Vec<AccelerateError>,
}

/// Accelerate error from the proxy response.
#[derive(Debug, Deserialize)]
struct AccelerateError {
    message: String,
}

/// Cache strategy configuration for Accelerate queries.
#[derive(Debug, Clone, Serialize)]
pub struct CacheStrategy {
    /// Time-to-live in seconds. How long the cached result is valid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
    /// Stale-while-revalidate in seconds. Serve stale data while refreshing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swr: Option<u64>,
}

impl AccelerateClient {
    /// Create a new Accelerate client.
    ///
    /// `endpoint` is the Accelerate proxy URL (e.g., `https://accelerate.prisma-data.net`).
    /// `api_key` is the Prisma Data Platform API key.
    pub fn new(endpoint: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
            api_key: api_key.into(),
        }
    }

    /// Execute a query built with `QueryBuilder`.
    pub async fn execute(&self, query: &QueryBuilder) -> Result<Value, ClientError> {
        let request = query.build();
        self.execute_json(&request).await
    }

    /// Execute a raw JSON protocol request through Accelerate.
    pub async fn execute_json(&self, request: &Value) -> Result<Value, ClientError> {
        let action = request
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ClientError::InvalidQuery("missing required field: action".into()))?
            .to_string();
        let model_name = request.get("modelName").and_then(|v| v.as_str()).map(String::from);
        let query = request
            .get("query")
            .cloned()
            .unwrap_or(Value::Object(Default::default()));

        let body = AccelerateRequest {
            action,
            model_name,
            query,
        };

        let resp = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| ClientError::InvalidQuery(format!("Accelerate request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let mut body = resp
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            // Truncate error body to prevent leaking secrets in long responses
            const MAX_ERROR_BODY: usize = 512;
            if body.len() > MAX_ERROR_BODY {
                body.truncate(MAX_ERROR_BODY);
                body.push_str("...[truncated]");
            }
            return Err(ClientError::InvalidQuery(format!(
                "Accelerate returned {status}: {body}"
            )));
        }

        let result: AccelerateResponse = resp
            .json()
            .await
            .map_err(|e| ClientError::InvalidQuery(format!("Invalid Accelerate response: {e}")))?;

        if !result.errors.is_empty() {
            return Err(ClientError::Accelerate {
                errors: result
                    .errors
                    .into_iter()
                    .map(|e| AccelerateErrorDetail { message: e.message })
                    .collect(),
            });
        }

        Ok(result.data)
    }

    /// Execute a query with cache strategy hints.
    ///
    /// The cache strategy tells Accelerate how to cache this query's results.
    pub async fn execute_cached(&self, query: &QueryBuilder, strategy: &CacheStrategy) -> Result<Value, ClientError> {
        let mut request = query.build();
        if let Value::Object(ref mut map) = request {
            if let Some(Value::Object(q)) = map.get_mut("query") {
                q.insert(
                    "cacheStrategy".into(),
                    serde_json::to_value(strategy)
                        .map_err(|e| ClientError::InvalidQuery(format!("Failed to serialize cache strategy: {e}")))?,
                );
            }
        }
        self.execute_json(&request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accelerate_request_serializes() {
        let req = AccelerateRequest {
            action: "findMany".into(),
            model_name: Some("User".into()),
            query: json!({"selection": {"$scalars": true}}),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["action"], "findMany");
        assert_eq!(json["modelName"], "User");
    }

    #[test]
    fn accelerate_request_omits_null_model() {
        let req = AccelerateRequest {
            action: "queryRaw".into(),
            model_name: None,
            query: json!({}),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("modelName"));
    }

    #[test]
    fn cache_strategy_serializes() {
        let strategy = CacheStrategy {
            ttl: Some(60),
            swr: Some(120),
        };
        let json = serde_json::to_value(&strategy).unwrap();
        assert_eq!(json["ttl"], 60);
        assert_eq!(json["swr"], 120);
    }

    #[test]
    fn cache_strategy_omits_none() {
        let strategy = CacheStrategy {
            ttl: Some(60),
            swr: None,
        };
        let json = serde_json::to_string(&strategy).unwrap();
        assert!(json.contains("ttl"));
        assert!(!json.contains("swr"));
    }

    #[test]
    fn accelerate_response_deserializes() {
        let json = json!({
            "data": [{"id": 1, "name": "Alice"}],
            "errors": []
        });
        let resp: AccelerateResponse = serde_json::from_value(json).unwrap();
        assert!(resp.data.is_array());
        assert_eq!(resp.data[0]["name"], "Alice");
    }

    #[test]
    fn accelerate_response_with_errors_deserializes() {
        let json = json!({
            "data": null,
            "errors": [
                {"message": "Table not found"},
                {"message": "Query failed"}
            ]
        });
        let resp: AccelerateResponse = serde_json::from_value(json).unwrap();
        assert_eq!(resp.errors.len(), 2);
        assert_eq!(resp.errors[0].message, "Table not found");
        assert_eq!(resp.errors[1].message, "Query failed");
    }

    #[test]
    fn accelerate_response_missing_errors_defaults_empty() {
        let json = json!({
            "data": {"id": 1}
        });
        let resp: AccelerateResponse = serde_json::from_value(json).unwrap();
        assert!(resp.errors.is_empty());
    }
}
