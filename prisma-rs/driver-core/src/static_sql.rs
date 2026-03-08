/// A SQL string guaranteed to contain only compile-time-known content.
///
/// `StaticSql` can only be constructed via the `static_sql!` macro, which
/// enforces that every fragment is `&'static str`. Runtime strings (`String`,
/// `&str` from user input) cannot be used, making SQL injection a compile error.
///
/// # Why not just use `format!()`?
///
/// `format!()` accepts any `Display` type, including runtime `String` values.
/// This means user input can be interpolated into SQL:
///
/// ```ignore
/// // DANGEROUS: user_input could be "sp1; DROP TABLE users; --"
/// let sql = format!("SAVEPOINT {}", user_input);
/// ```
///
/// `static_sql!` rejects this at compile time:
///
/// ```compile_fail
/// use prisma_driver_core::static_sql;
/// let user_input = String::from("malicious");
/// let sql = static_sql!("SAVEPOINT ", user_input.as_str()); // ERROR
/// ```
#[derive(Debug, Clone)]
pub struct StaticSql {
    value: String,
}

impl StaticSql {
    /// Build the final SQL string.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Construct from static string parts. Only called by `static_sql!`.
    #[doc(hidden)]
    #[inline]
    pub fn from_static_parts(parts: &[&'static str]) -> Self {
        let mut value = String::new();
        for part in parts {
            value.push_str(part);
        }
        Self { value }
    }
}

impl std::fmt::Display for StaticSql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value)
    }
}

/// Build a SQL string from compile-time-known `&'static str` fragments only.
///
/// Every argument must be `&'static str`. This includes:
/// - String literals: `"SAVEPOINT"`
/// - `const` values: `const NAME: &str = "sp1";`
/// - Methods returning `&'static str`: `IsolationLevel::as_sql()`
///
/// Runtime values are rejected at compile time:
/// - `String` -- not `'static`
/// - `&str` from function args -- lifetime not `'static`
/// - `format!()` results -- heap-allocated, not `'static`
///
/// # Examples
///
/// ```
/// use prisma_driver_core::static_sql;
///
/// let sql = static_sql!("SAVEPOINT ", "sp1");
/// assert_eq!(sql.as_str(), "SAVEPOINT sp1");
/// ```
#[macro_export]
macro_rules! static_sql {
    ($($part:expr),+ $(,)?) => {
        $crate::StaticSql::from_static_parts(&[
            $({
                // Force each part to be &'static str at compile time.
                // A runtime &str or String will fail this binding.
                let _part: &'static str = $part;
                _part
            }),+
        ])
    };
}

#[cfg(test)]
mod tests {

    #[test]
    fn single_literal() {
        let sql = static_sql!("BEGIN");
        assert_eq!(sql.as_str(), "BEGIN");
    }

    #[test]
    fn multiple_literals() {
        let sql = static_sql!("SAVEPOINT ", "sp1");
        assert_eq!(sql.as_str(), "SAVEPOINT sp1");
    }

    #[test]
    fn three_parts() {
        let sql = static_sql!("ROLLBACK TO SAVEPOINT ", "sp2", ";");
        assert_eq!(sql.as_str(), "ROLLBACK TO SAVEPOINT sp2;");
    }

    #[test]
    fn from_const() {
        const NAME: &str = "my_savepoint";
        let sql = static_sql!("SAVEPOINT ", NAME);
        assert_eq!(sql.as_str(), "SAVEPOINT my_savepoint");
    }

    #[test]
    fn from_method_returning_static_str() {
        // IsolationLevel::as_sql() returns &'static str, so this compiles.
        let level = crate::IsolationLevel::ReadCommitted;
        let sql = static_sql!("BEGIN ISOLATION LEVEL ", level.as_sql());
        assert_eq!(sql.as_str(), "BEGIN ISOLATION LEVEL READ COMMITTED");
    }

    #[test]
    fn display_impl() {
        let sql = static_sql!("RELEASE SAVEPOINT ", "sp1");
        assert_eq!(format!("{sql}"), "RELEASE SAVEPOINT sp1");
    }
}
