use std::fmt;

/// A structured error message with parameterized placeholders, analogous to
/// SQL query bindings. Parameters are either public (safe to display) or
/// secret (redacted in `Display`/`Debug` output).
///
/// This prevents credential leakage by construction -- secrets are never
/// embedded in the template string and can only be revealed via the explicit
/// `expose()` method.
///
/// # Example
///
/// ```
/// use prisma_driver_core::SafeMessage;
///
/// let msg = SafeMessage::new("Failed to connect to {0} as user {1}")
///     .param("db.example.com:5432")   // {0} - public
///     .secret("hunter2");             // {1} - redacted
///
/// assert_eq!(msg.to_string(), "Failed to connect to db.example.com:5432 as user ***");
/// assert_eq!(msg.expose(), "Failed to connect to db.example.com:5432 as user hunter2");
/// ```
#[derive(Clone)]
pub struct SafeMessage {
    template: String,
    params: Vec<Param>,
}

#[derive(Clone)]
struct Param {
    value: String,
    secret: bool,
}

impl SafeMessage {
    /// Create a new message with a template string. Use `{0}`, `{1}`, etc.
    /// as placeholders for parameters added via `param()` and `secret()`.
    pub fn new(template: impl Into<String>) -> Self {
        Self {
            template: template.into(),
            params: Vec::new(),
        }
    }

    /// Add a public parameter (safe to display in logs/errors).
    pub fn param(mut self, value: impl Into<String>) -> Self {
        self.params.push(Param {
            value: value.into(),
            secret: false,
        });
        self
    }

    /// Add a secret parameter (redacted in Display/Debug, only shown via `expose()`).
    pub fn secret(mut self, value: impl Into<String>) -> Self {
        self.params.push(Param {
            value: value.into(),
            secret: true,
        });
        self
    }

    /// Render the message with all secrets redacted as `***`.
    pub fn redacted(&self) -> String {
        self.render(true)
    }

    /// Render the message with all values exposed, including secrets.
    /// Use only when you explicitly need the real values (e.g., for actual
    /// connection attempts).
    pub fn expose(&self) -> String {
        self.render(false)
    }

    fn render(&self, redact_secrets: bool) -> String {
        let mut result = self.template.clone();
        for (i, param) in self.params.iter().enumerate() {
            let placeholder = format!("{{{i}}}");
            let value = if redact_secrets && param.secret {
                "***"
            } else {
                &param.value
            };
            result = result.replace(&placeholder, value);
        }
        result
    }
}

impl fmt::Display for SafeMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.redacted())
    }
}

impl fmt::Debug for SafeMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SafeMessage(\"{}\")", self.redacted())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_redacts_secrets() {
        let msg = SafeMessage::new("connect to {0} with password {1}")
            .param("localhost:5432")
            .secret("hunter2");

        assert_eq!(msg.to_string(), "connect to localhost:5432 with password ***");
    }

    #[test]
    fn expose_shows_secrets() {
        let msg = SafeMessage::new("connect to {0} with password {1}")
            .param("localhost:5432")
            .secret("hunter2");

        assert_eq!(msg.expose(), "connect to localhost:5432 with password hunter2");
    }

    #[test]
    fn debug_redacts_secrets() {
        let msg = SafeMessage::new("url={0}").secret("postgres://user:pass@host/db");

        let debug = format!("{msg:?}");
        assert!(!debug.contains("pass"), "debug leaked secret: {debug}");
        assert!(debug.contains("***"));
    }

    #[test]
    fn no_params() {
        let msg = SafeMessage::new("connection refused");
        assert_eq!(msg.to_string(), "connection refused");
        assert_eq!(msg.expose(), "connection refused");
    }

    #[test]
    fn all_public_params() {
        let msg = SafeMessage::new("table {0} column {1}").param("users").param("email");

        assert_eq!(msg.to_string(), "table users column email");
        assert_eq!(msg.expose(), "table users column email");
    }

    #[test]
    fn multiple_secrets() {
        let msg = SafeMessage::new("{0}://{1}:{2}@{3}/{4}")
            .param("postgres")
            .secret("admin")
            .secret("s3cret")
            .param("db.example.com")
            .param("mydb");

        assert_eq!(msg.to_string(), "postgres://***:***@db.example.com/mydb");
        assert_eq!(msg.expose(), "postgres://admin:s3cret@db.example.com/mydb");
    }

    #[test]
    fn unused_placeholders_preserved() {
        let msg = SafeMessage::new("error {0} {1} {2}").param("only-one");

        assert_eq!(msg.to_string(), "error only-one {1} {2}");
    }
}
