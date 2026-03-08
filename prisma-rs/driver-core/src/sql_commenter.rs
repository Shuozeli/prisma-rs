//! SQL Commenter support.
//!
//! Implements the [SQL Commenter](https://google.github.io/sqlcommenter/) specification
//! for appending key-value metadata to SQL queries as comments. This enables
//! correlation between application-level operations and database queries.
//!
//! Supports OpenTelemetry trace context propagation via `traceparent` and
//! `tracestate` keys.

use std::collections::BTreeMap;

/// Builds a SQL comment from key-value tags following the SQL Commenter spec.
///
/// Keys are sorted alphabetically. Values are URL-encoded.
#[derive(Debug, Clone, Default)]
pub struct SqlComment {
    tags: BTreeMap<String, String>,
}

impl SqlComment {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a key-value tag.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Add OpenTelemetry traceparent header value.
    pub fn traceparent(self, value: impl Into<String>) -> Self {
        self.tag("traceparent", value)
    }

    /// Add OpenTelemetry tracestate header value.
    pub fn tracestate(self, value: impl Into<String>) -> Self {
        self.tag("tracestate", value)
    }

    /// Build the comment string (without `/* */` delimiters).
    ///
    /// Returns `None` if no tags are set.
    pub fn build(&self) -> Option<String> {
        if self.tags.is_empty() {
            return None;
        }

        let pairs: Vec<String> = self
            .tags
            .iter()
            .map(|(k, v)| format!("{}='{}'", url_encode(k), url_encode(v)))
            .collect();

        Some(pairs.join(","))
    }

    /// Prepend the comment to a SQL query string.
    ///
    /// Returns the original query unchanged if no tags are set.
    pub fn prepend_to(&self, sql: &str) -> String {
        match self.build() {
            Some(comment) => format!("/* {comment} */ {sql}"),
            None => sql.to_string(),
        }
    }

    /// Append the comment to a SQL query string.
    ///
    /// Returns the original query unchanged if no tags are set.
    pub fn append_to(&self, sql: &str) -> String {
        match self.build() {
            Some(comment) => format!("{sql} /* {comment} */"),
            None => sql.to_string(),
        }
    }
}

/// URL-encode a string per the SQL Commenter spec.
///
/// Encodes characters that could break SQL comment parsing.
fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            ' ' => result.push_str("%20"),
            '\'' => result.push_str("%27"),
            '*' => result.push_str("%2A"),
            '/' => result.push_str("%2F"),
            '%' => result.push_str("%25"),
            _ => result.push(c),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- build ---

    #[test]
    fn empty_comment_returns_none() {
        let comment = SqlComment::new();
        assert!(comment.build().is_none());
    }

    #[test]
    fn single_tag() {
        let comment = SqlComment::new().tag("action", "findMany");
        assert_eq!(comment.build().unwrap(), "action='findMany'");
    }

    #[test]
    fn multiple_tags_sorted() {
        let comment = SqlComment::new().tag("model", "User").tag("action", "findMany");
        assert_eq!(comment.build().unwrap(), "action='findMany',model='User'");
    }

    #[test]
    fn three_tags_sorted_lexicographically() {
        let comment = SqlComment::new().tag("z", "1").tag("a", "2").tag("m", "3");
        assert_eq!(comment.build().unwrap(), "a='2',m='3',z='1'");
    }

    #[test]
    fn duplicate_key_overwrites() {
        let comment = SqlComment::new().tag("key", "first").tag("key", "second");
        assert_eq!(comment.build().unwrap(), "key='second'");
    }

    #[test]
    fn empty_string_values() {
        let comment = SqlComment::new().tag("key", "");
        assert_eq!(comment.build().unwrap(), "key=''");
    }

    #[test]
    fn numeric_like_values() {
        let comment = SqlComment::new().tag("version", "123");
        assert_eq!(comment.build().unwrap(), "version='123'");
    }

    // --- URL encoding ---

    #[test]
    fn url_encodes_spaces_in_values() {
        let comment = SqlComment::new().tag("query", "SELECT * FROM users");
        assert_eq!(comment.build().unwrap(), "query='SELECT%20%2A%20FROM%20users'");
    }

    #[test]
    fn url_encodes_spaces_in_keys() {
        let comment = SqlComment::new().tag("key with spaces", "value");
        assert_eq!(comment.build().unwrap(), "key%20with%20spaces='value'");
    }

    #[test]
    fn encodes_single_quotes() {
        let comment = SqlComment::new().tag("name", "O'Brien");
        assert_eq!(comment.build().unwrap(), "name='O%27Brien'");
    }

    #[test]
    fn encodes_forward_slashes() {
        let comment = SqlComment::new().tag("path", "a/b/c");
        assert_eq!(comment.build().unwrap(), "path='a%2Fb%2Fc'");
    }

    #[test]
    fn encodes_asterisks() {
        let comment = SqlComment::new().tag("query", "SELECT *");
        assert_eq!(comment.build().unwrap(), "query='SELECT%20%2A'");
    }

    #[test]
    fn encodes_percent_signs() {
        let comment = SqlComment::new().tag("like", "50%");
        assert_eq!(comment.build().unwrap(), "like='50%25'");
    }

    #[test]
    fn preserves_safe_characters() {
        let comment = SqlComment::new().tag("safe", "abc-123_def.xyz");
        assert_eq!(comment.build().unwrap(), "safe='abc-123_def.xyz'");
    }

    // --- traceparent/tracestate ---

    #[test]
    fn traceparent_tag() {
        let comment = SqlComment::new().traceparent("00-abc123-def456-01");
        assert_eq!(comment.build().unwrap(), "traceparent='00-abc123-def456-01'");
    }

    #[test]
    fn traceparent_full_w3c_format() {
        let comment = SqlComment::new().traceparent("00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01");
        assert_eq!(
            comment.build().unwrap(),
            "traceparent='00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01'"
        );
    }

    #[test]
    fn tracestate_tag() {
        let comment = SqlComment::new().tracestate("congo=congos5,rojo=rojospg");
        assert_eq!(comment.build().unwrap(), "tracestate='congo=congos5,rojo=rojospg'");
    }

    #[test]
    fn traceparent_and_tracestate_together() {
        let comment = SqlComment::new()
            .traceparent("00-abc-def-01")
            .tracestate("vendor=value");
        let built = comment.build().unwrap();
        assert!(built.contains("traceparent='00-abc-def-01'"));
        assert!(built.contains("tracestate='vendor=value'"));
    }

    // --- realistic usage ---

    #[test]
    fn multiple_realistic_tags() {
        let comment = SqlComment::new()
            .traceparent("00-abc-def-01")
            .tag("application", "my-app")
            .tag("prisma-query", "eyJtb2RlbCI6IlVzZXIifQ==");
        let built = comment.build().unwrap();
        // Keys sorted: application, prisma-query, traceparent
        assert!(built.starts_with("application='my-app'"));
        assert!(built.contains("traceparent='00-abc-def-01'"));
    }

    #[test]
    fn prisma_style_tags() {
        let comment = SqlComment::new()
            .tag("prisma-model", "User")
            .tag("prisma-action", "findMany")
            .tag("prisma-version", "5.0.0");
        let built = comment.build().unwrap();
        assert!(built.contains("prisma-action='findMany'"));
        assert!(built.contains("prisma-model='User'"));
        assert!(built.contains("prisma-version='5.0.0'"));
    }

    // --- prepend/append ---

    #[test]
    fn prepend_to_query() {
        let comment = SqlComment::new().tag("action", "findMany").tag("model", "User");
        let sql = "SELECT * FROM \"User\"";
        assert_eq!(
            comment.prepend_to(sql),
            "/* action='findMany',model='User' */ SELECT * FROM \"User\""
        );
    }

    #[test]
    fn append_to_query() {
        let comment = SqlComment::new().tag("action", "findMany");
        let sql = "SELECT * FROM \"User\"";
        assert_eq!(comment.append_to(sql), "SELECT * FROM \"User\" /* action='findMany' */");
    }

    #[test]
    fn prepend_empty_returns_original() {
        let comment = SqlComment::new();
        let sql = "SELECT 1";
        assert_eq!(comment.prepend_to(sql), "SELECT 1");
    }

    #[test]
    fn append_empty_returns_original() {
        let comment = SqlComment::new();
        let sql = "SELECT 1";
        assert_eq!(comment.append_to(sql), "SELECT 1");
    }

    #[test]
    fn append_to_complex_sql() {
        let comment = SqlComment::new().tag("app", "test");
        let sql = "SELECT \"id\", \"name\" FROM \"User\" WHERE \"active\" = true ORDER BY \"name\"";
        let result = comment.append_to(sql);
        assert!(result.starts_with("SELECT \"id\""));
        assert!(result.ends_with("/* app='test' */"));
    }

    // --- url_encode function ---

    #[test]
    fn url_encode_empty() {
        assert_eq!(url_encode(""), "");
    }

    #[test]
    fn url_encode_no_special_chars() {
        assert_eq!(url_encode("simple"), "simple");
    }

    #[test]
    fn url_encode_all_special() {
        assert_eq!(url_encode(" '*/% "), "%20%27%2A%2F%25%20");
    }

    // --- Clone/Default ---

    #[test]
    fn clone_preserves_tags() {
        let comment = SqlComment::new().tag("a", "1");
        let cloned = comment.clone();
        assert_eq!(comment.build(), cloned.build());
    }

    #[test]
    fn default_is_empty() {
        let comment = SqlComment::default();
        assert!(comment.build().is_none());
    }
}
