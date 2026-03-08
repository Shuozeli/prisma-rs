//! Parsed database URL with credential redaction.
//!
//! Follows the sqlx pattern: the raw connection string is parsed immediately
//! on construction. The password is stored separately and never appears in
//! `Display` or `Debug` output. This prevents credential leakage through
//! error messages, logs, or stack traces.

use url::Url;

/// A parsed database connection URL that redacts credentials in all output.
///
/// # Security
///
/// - `Display` prints the URL with the password replaced by `***`.
/// - `Debug` prints the same redacted form.
/// - The raw password is only accessible via [`expose_password`](Self::expose_password).
/// - The original URL (with password) is only accessible via [`expose_url`](Self::expose_url).
///
/// # Examples
///
/// ```
/// use prisma_driver_core::DatabaseUrl;
///
/// let url = DatabaseUrl::parse("postgres://user:s3cret@localhost:5432/mydb").unwrap();
///
/// // Display/Debug never show the password
/// assert_eq!(url.to_string(), "postgres://user:***@localhost:5432/mydb");
/// assert_eq!(format!("{url:?}"), "DatabaseUrl(\"postgres://user:***@localhost:5432/mydb\")");
///
/// // Explicit opt-in to access the password
/// assert_eq!(url.expose_password(), Some("s3cret"));
/// ```
#[derive(Clone)]
pub struct DatabaseUrl {
    parsed: Url,
    password: Option<String>,
}

impl DatabaseUrl {
    /// Parse a database URL string.
    ///
    /// The password is extracted and stored separately. The internal `Url`
    /// has its password replaced with `***` so it can never leak.
    pub fn parse(raw: &str) -> Result<Self, DatabaseUrlError> {
        let mut parsed = Url::parse(raw).map_err(|e| DatabaseUrlError::InvalidUrl { message: e.to_string() })?;

        let password = parsed.password().map(|p| p.to_string());

        // Replace password in the parsed URL so Display/Debug are always safe
        if password.is_some() {
            parsed
                .set_password(Some("***"))
                .map_err(|_| DatabaseUrlError::InvalidUrl {
                    message: "cannot set password on URL".to_string(),
                })?;
        }

        Ok(Self { parsed, password })
    }

    /// The database scheme (e.g., `"postgres"`, `"mysql"`, `"sqlite"`).
    pub fn scheme(&self) -> &str {
        self.parsed.scheme()
    }

    /// The username, if present.
    pub fn username(&self) -> &str {
        self.parsed.username()
    }

    /// The host, if present.
    pub fn host(&self) -> Option<&str> {
        self.parsed.host_str()
    }

    /// The port, if present.
    pub fn port(&self) -> Option<u16> {
        self.parsed.port()
    }

    /// The database name (path component without leading `/`).
    pub fn database(&self) -> Option<&str> {
        let path = self.parsed.path();
        if path.len() > 1 { Some(&path[1..]) } else { None }
    }

    /// The full query string, if present.
    pub fn query(&self) -> Option<&str> {
        self.parsed.query()
    }

    /// Expose the password. Call this only when you actually need it
    /// (e.g., to pass to the database driver).
    pub fn expose_password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// Reconstruct the full URL with the real password for passing to
    /// database drivers that require a URL string.
    ///
    /// The returned string contains the plaintext password. Do not log it,
    /// include it in error messages, or store it beyond the immediate use.
    pub fn expose_url(&self) -> String {
        let mut url = self.parsed.clone();
        match &self.password {
            Some(pw) => {
                let _ = url.set_password(Some(pw));
            }
            None => {
                let _ = url.set_password(None);
            }
        }
        url.to_string()
    }

    /// The redacted URL string (password replaced with `***`).
    /// This is the same as `to_string()` / `Display`.
    pub fn redacted(&self) -> String {
        self.parsed.to_string()
    }
}

impl std::fmt::Display for DatabaseUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Always shows the redacted form
        write!(f, "{}", self.parsed)
    }
}

impl std::fmt::Debug for DatabaseUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never leaks password in debug output
        write!(f, "DatabaseUrl(\"{}\")", self.parsed)
    }
}

/// Errors from parsing a database URL.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DatabaseUrlError {
    #[error("Invalid database URL: {message}")]
    InvalidUrl { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_postgres_url() {
        let url = DatabaseUrl::parse("postgres://user:secret@localhost:5432/mydb").unwrap();
        assert_eq!(url.scheme(), "postgres");
        assert_eq!(url.username(), "user");
        assert_eq!(url.host(), Some("localhost"));
        assert_eq!(url.port(), Some(5432));
        assert_eq!(url.database(), Some("mydb"));
        assert_eq!(url.expose_password(), Some("secret"));
    }

    #[test]
    fn parse_mysql_url() {
        let url = DatabaseUrl::parse("mysql://root:p@ssw0rd@db.example.com:3306/app").unwrap();
        assert_eq!(url.scheme(), "mysql");
        assert_eq!(url.username(), "root");
        assert_eq!(url.expose_password(), Some("p@ssw0rd"));
        assert_eq!(url.database(), Some("app"));
    }

    #[test]
    fn display_redacts_password() {
        let url = DatabaseUrl::parse("postgres://admin:hunter2@prod.db:5432/main").unwrap();
        let display = url.to_string();
        assert!(!display.contains("hunter2"), "password leaked in Display");
        assert!(display.contains("***"), "redaction marker missing");
        assert_eq!(display, "postgres://admin:***@prod.db:5432/main");
    }

    #[test]
    fn debug_redacts_password() {
        let url = DatabaseUrl::parse("postgres://admin:hunter2@prod.db:5432/main").unwrap();
        let debug = format!("{url:?}");
        assert!(!debug.contains("hunter2"), "password leaked in Debug");
        assert!(debug.contains("***"), "redaction marker missing");
    }

    #[test]
    fn expose_url_restores_password() {
        let url = DatabaseUrl::parse("postgres://user:secret@localhost:5432/mydb").unwrap();
        assert_eq!(url.expose_url(), "postgres://user:secret@localhost:5432/mydb");
    }

    #[test]
    fn no_password() {
        let url = DatabaseUrl::parse("postgres://user@localhost:5432/mydb").unwrap();
        assert_eq!(url.expose_password(), None);
        assert_eq!(url.to_string(), "postgres://user@localhost:5432/mydb");
        assert_eq!(url.expose_url(), "postgres://user@localhost:5432/mydb");
    }

    #[test]
    fn with_query_params() {
        let url = DatabaseUrl::parse("postgres://user:pw@host:5432/db?sslmode=require&connect_timeout=10").unwrap();
        assert_eq!(url.query(), Some("sslmode=require&connect_timeout=10"));
        assert!(url.to_string().contains("sslmode=require"));
        assert!(!url.to_string().contains("pw"));
    }

    #[test]
    fn sqlite_url() {
        let url = DatabaseUrl::parse("file:./dev.db").unwrap();
        assert_eq!(url.scheme(), "file");
        assert_eq!(url.expose_password(), None);
    }

    #[test]
    fn invalid_url() {
        let result = DatabaseUrl::parse("not a url at all");
        assert!(result.is_err());
    }

    #[test]
    fn empty_password() {
        let url = DatabaseUrl::parse("postgres://user:@localhost:5432/db").unwrap();
        assert_eq!(url.expose_password(), Some(""));
        // Even empty password gets redacted
        assert!(url.to_string().contains("***"));
    }

    #[test]
    fn password_with_special_chars() {
        let url = DatabaseUrl::parse("postgres://user:p%40ss%23word@localhost:5432/db").unwrap();
        assert_eq!(url.expose_password(), Some("p%40ss%23word"));
        assert!(!url.to_string().contains("p%40ss"));
    }

    #[test]
    fn clone_preserves_password() {
        let url = DatabaseUrl::parse("postgres://user:secret@localhost/db").unwrap();
        let cloned = url.clone();
        assert_eq!(cloned.expose_password(), Some("secret"));
        assert!(!cloned.to_string().contains("secret"));
    }

    #[test]
    fn redacted_matches_display() {
        let url = DatabaseUrl::parse("postgres://u:p@h:5432/d").unwrap();
        assert_eq!(url.redacted(), url.to_string());
    }
}
