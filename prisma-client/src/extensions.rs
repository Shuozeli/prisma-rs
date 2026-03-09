//! Extension system for the Prisma client.
//!
//! Provides `ResultExtension` for adding computed fields to query results,
//! similar to Prisma's `$extends({ result: ... })` API.

use serde_json::Value;

/// A result extension that computes derived fields from query results.
///
/// Implementations specify which model they apply to, which fields they
/// need from the query result, and a compute function that produces
/// additional fields.
///
/// # Example
///
/// ```rust
/// use prisma_client::ResultExtension;
/// use serde_json::{json, Value};
///
/// struct FullNameExtension;
///
/// impl ResultExtension for FullNameExtension {
///     fn model(&self) -> &str { "User" }
///
///     fn needs(&self) -> &[&str] { &["firstName", "lastName"] }
///
///     fn compute(&self, row: &Value) -> Vec<(String, Value)> {
///         let first = row["firstName"].as_str().unwrap_or("");
///         let last = row["lastName"].as_str().unwrap_or("");
///         vec![("fullName".into(), json!(format!("{first} {last}")))]
///     }
/// }
/// ```
pub trait ResultExtension: Send + Sync {
    /// The model this extension applies to.
    fn model(&self) -> &str;

    /// Fields required from the query result to compute the extension.
    fn needs(&self) -> &[&str];

    /// Compute additional fields from a query result row.
    ///
    /// Returns a list of (field_name, value) pairs to add to the result.
    fn compute(&self, row: &Value) -> Vec<(String, Value)>;
}

/// Apply result extensions to a query result.
///
/// Walks the result value, applying matching extensions to each record.
pub fn apply_result_extensions(value: &mut Value, model: &str, extensions: &[Box<dyn ResultExtension>]) {
    let matching: Vec<&dyn ResultExtension> = extensions
        .iter()
        .filter(|ext| ext.model() == model)
        .map(|ext| ext.as_ref())
        .collect();

    if matching.is_empty() {
        return;
    }

    match value {
        Value::Array(items) => {
            for item in items.iter_mut() {
                apply_to_record(item, &matching);
            }
        }
        Value::Object(_) => {
            apply_to_record(value, &matching);
        }
        _ => {}
    }
}

fn apply_to_record(record: &mut Value, extensions: &[&dyn ResultExtension]) {
    // Compute extensions from an immutable snapshot, then mutate
    let computed_fields: Vec<Vec<(String, Value)>> = extensions.iter().map(|ext| ext.compute(record)).collect();

    if let Value::Object(map) = record {
        for fields in computed_fields {
            for (key, val) in fields {
                map.insert(key, val);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct FullNameExt;

    impl ResultExtension for FullNameExt {
        fn model(&self) -> &str {
            "User"
        }

        fn needs(&self) -> &[&str] {
            &["firstName", "lastName"]
        }

        fn compute(&self, row: &Value) -> Vec<(String, Value)> {
            let first = row["firstName"].as_str().unwrap_or("");
            let last = row["lastName"].as_str().unwrap_or("");
            vec![("fullName".into(), json!(format!("{first} {last}").trim()))]
        }
    }

    struct UpperEmailExt;

    impl ResultExtension for UpperEmailExt {
        fn model(&self) -> &str {
            "User"
        }

        fn needs(&self) -> &[&str] {
            &["email"]
        }

        fn compute(&self, row: &Value) -> Vec<(String, Value)> {
            let email = row["email"].as_str().unwrap_or("");
            vec![("upperEmail".into(), json!(email.to_uppercase()))]
        }
    }

    #[test]
    fn applies_extension_to_single_record() {
        let extensions: Vec<Box<dyn ResultExtension>> = vec![Box::new(FullNameExt)];
        let mut value = json!({"firstName": "Alice", "lastName": "Smith"});

        apply_result_extensions(&mut value, "User", &extensions);

        assert_eq!(value["fullName"], "Alice Smith");
    }

    #[test]
    fn applies_extension_to_array() {
        let extensions: Vec<Box<dyn ResultExtension>> = vec![Box::new(FullNameExt)];
        let mut value = json!([
            {"firstName": "Alice", "lastName": "Smith"},
            {"firstName": "Bob", "lastName": "Jones"}
        ]);

        apply_result_extensions(&mut value, "User", &extensions);

        assert_eq!(value[0]["fullName"], "Alice Smith");
        assert_eq!(value[1]["fullName"], "Bob Jones");
    }

    #[test]
    fn skips_non_matching_model() {
        let extensions: Vec<Box<dyn ResultExtension>> = vec![Box::new(FullNameExt)];
        let mut value = json!({"title": "Post 1"});

        apply_result_extensions(&mut value, "Post", &extensions);

        assert!(value["fullName"].is_null());
    }

    #[test]
    fn multiple_extensions_on_same_model() {
        let extensions: Vec<Box<dyn ResultExtension>> = vec![Box::new(FullNameExt), Box::new(UpperEmailExt)];
        let mut value = json!({
            "firstName": "Alice",
            "lastName": "Smith",
            "email": "alice@example.com"
        });

        apply_result_extensions(&mut value, "User", &extensions);

        assert_eq!(value["fullName"], "Alice Smith");
        assert_eq!(value["upperEmail"], "ALICE@EXAMPLE.COM");
    }

    #[test]
    fn handles_null_value() {
        let extensions: Vec<Box<dyn ResultExtension>> = vec![Box::new(FullNameExt)];
        let mut value = json!(null);

        apply_result_extensions(&mut value, "User", &extensions);

        assert!(value.is_null());
    }
}
