//! Middleware pipeline for intercepting Prisma operations.
//!
//! Middleware functions wrap the query execution pipeline, allowing
//! cross-cutting concerns like logging, caching, access control,
//! and query transformation.
//!
//! # Example
//!
//! ```rust,ignore
//! use prisma_client::{Middleware, MiddlewareParams, MiddlewareNext};
//!
//! struct TimingMiddleware;
//!
//! #[async_trait::async_trait]
//! impl Middleware for TimingMiddleware {
//!     async fn resolve(
//!         &self,
//!         params: MiddlewareParams,
//!         next: MiddlewareNext<'_>,
//!     ) -> Result<serde_json::Value, prisma_client::ClientError> {
//!         let start = std::time::Instant::now();
//!         let result = next.run(params).await;
//!         println!("Query took {:?}", start.elapsed());
//!         result
//!     }
//! }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::ClientError;

/// Parameters describing the intercepted Prisma operation.
#[derive(Debug, Clone)]
pub struct MiddlewareParams {
    /// The model name (e.g., "User"), or `None` for raw queries.
    pub model: Option<String>,
    /// The action name (e.g., "findMany", "createOne").
    pub action: String,
    /// The query arguments (where, data, orderBy, etc.) as JSON.
    pub args: Value,
}

/// Middleware trait for intercepting Prisma operations.
///
/// Implementations receive the operation parameters and a `next` handle
/// to continue execution. They can modify params before calling next,
/// transform the result after, or short-circuit execution entirely.
#[async_trait]
pub trait Middleware: Send + Sync {
    async fn resolve(&self, params: MiddlewareParams, next: MiddlewareNext<'_>) -> Result<Value, ClientError>;
}

/// The continuation of the middleware chain.
///
/// Call `run()` to pass execution to the next middleware or,
/// if this is the last middleware, to the actual query executor.
pub struct MiddlewareNext<'a> {
    stack: &'a [Arc<dyn Middleware>],
    executor: &'a dyn MiddlewareExecutor,
}

impl<'a> MiddlewareNext<'a> {
    /// Continue to the next middleware or execute the query.
    pub async fn run(self, params: MiddlewareParams) -> Result<Value, ClientError> {
        match self.stack.split_first() {
            Some((head, tail)) => {
                let next = MiddlewareNext {
                    stack: tail,
                    executor: self.executor,
                };
                head.resolve(params, next).await
            }
            None => self.executor.execute(params).await,
        }
    }
}

/// Internal trait for the final execution step after all middleware.
#[async_trait]
pub(crate) trait MiddlewareExecutor: Send + Sync {
    async fn execute(&self, params: MiddlewareParams) -> Result<Value, ClientError>;
}

/// A stack of middleware that wraps query execution.
#[derive(Clone, Default)]
pub(crate) struct MiddlewareStack {
    middlewares: Vec<Arc<dyn Middleware>>,
}

impl MiddlewareStack {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, middleware: Arc<dyn Middleware>) {
        self.middlewares.push(middleware);
    }

    pub fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }

    /// Run the middleware stack, falling through to the executor at the end.
    pub async fn execute(
        &self,
        params: MiddlewareParams,
        executor: &dyn MiddlewareExecutor,
    ) -> Result<Value, ClientError> {
        let next = MiddlewareNext {
            stack: &self.middlewares,
            executor,
        };
        next.run(params).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingMiddleware {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Middleware for CountingMiddleware {
        async fn resolve(&self, params: MiddlewareParams, next: MiddlewareNext<'_>) -> Result<Value, ClientError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            next.run(params).await
        }
    }

    struct ModifyingMiddleware;

    #[async_trait]
    impl Middleware for ModifyingMiddleware {
        async fn resolve(&self, mut params: MiddlewareParams, next: MiddlewareNext<'_>) -> Result<Value, ClientError> {
            // Add a tag to the args
            if let Value::Object(ref mut map) = params.args {
                map.insert("_modified".into(), Value::Bool(true));
            }
            next.run(params).await
        }
    }

    struct ShortCircuitMiddleware;

    #[async_trait]
    impl Middleware for ShortCircuitMiddleware {
        async fn resolve(&self, _params: MiddlewareParams, _next: MiddlewareNext<'_>) -> Result<Value, ClientError> {
            Ok(serde_json::json!({ "short_circuited": true }))
        }
    }

    struct EchoExecutor;

    #[async_trait]
    impl MiddlewareExecutor for EchoExecutor {
        async fn execute(&self, params: MiddlewareParams) -> Result<Value, ClientError> {
            Ok(serde_json::json!({
                "model": params.model,
                "action": params.action,
                "args": params.args,
            }))
        }
    }

    #[tokio::test]
    async fn empty_stack_calls_executor() {
        let stack = MiddlewareStack::new();
        let executor = EchoExecutor;
        let params = MiddlewareParams {
            model: Some("User".into()),
            action: "findMany".into(),
            args: serde_json::json!({}),
        };

        let result = stack.execute(params, &executor).await.unwrap();
        assert_eq!(result["model"], "User");
        assert_eq!(result["action"], "findMany");
    }

    #[tokio::test]
    async fn middleware_is_called() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut stack = MiddlewareStack::new();
        stack.push(Arc::new(CountingMiddleware { count: count.clone() }));

        let executor = EchoExecutor;
        let params = MiddlewareParams {
            model: Some("User".into()),
            action: "findMany".into(),
            args: serde_json::json!({}),
        };

        let _ = stack.execute(params, &executor).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn middleware_chain_order() {
        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        let mut stack = MiddlewareStack::new();
        stack.push(Arc::new(CountingMiddleware { count: count1.clone() }));
        stack.push(Arc::new(CountingMiddleware { count: count2.clone() }));

        let executor = EchoExecutor;
        let params = MiddlewareParams {
            model: Some("User".into()),
            action: "findMany".into(),
            args: serde_json::json!({}),
        };

        let _ = stack.execute(params, &executor).await.unwrap();
        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn middleware_can_modify_params() {
        let mut stack = MiddlewareStack::new();
        stack.push(Arc::new(ModifyingMiddleware));

        let executor = EchoExecutor;
        let params = MiddlewareParams {
            model: Some("User".into()),
            action: "findMany".into(),
            args: serde_json::json!({}),
        };

        let result = stack.execute(params, &executor).await.unwrap();
        assert_eq!(result["args"]["_modified"], true);
    }

    #[tokio::test]
    async fn middleware_can_short_circuit() {
        let count = Arc::new(AtomicUsize::new(0));
        let mut stack = MiddlewareStack::new();
        stack.push(Arc::new(ShortCircuitMiddleware));
        stack.push(Arc::new(CountingMiddleware { count: count.clone() }));

        let executor = EchoExecutor;
        let params = MiddlewareParams {
            model: Some("User".into()),
            action: "findMany".into(),
            args: serde_json::json!({}),
        };

        let result = stack.execute(params, &executor).await.unwrap();
        assert_eq!(result["short_circuited"], true);
        // Second middleware was never called
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
