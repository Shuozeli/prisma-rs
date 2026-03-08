//! Selection builder for controlling which fields to return.
//!
//! Prisma supports three mutually exclusive modes:
//! - `select`: explicit list of fields (only these are returned)
//! - `include`: all scalars + specified relations
//! - `omit`: all scalars minus specified fields

use serde_json::{Map, Value};

/// Builds the `selection` object for a Prisma JSON protocol request.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    mode: SelectionMode,
    fields: Vec<(String, Value)>,
}

#[derive(Debug, Clone, Default)]
enum SelectionMode {
    /// Default: `{ "$scalars": true }` (all scalar fields)
    #[default]
    Scalars,
    /// Explicit select: only listed fields
    Select,
    /// Include: all scalars + listed relations with sub-selections
    Include,
    /// Omit: all scalars except listed fields
    Omit,
}

impl Selection {
    pub fn new() -> Self {
        Self::default()
    }

    /// Select all scalar fields (the default).
    pub fn scalars() -> Self {
        Self {
            mode: SelectionMode::Scalars,
            fields: Vec::new(),
        }
    }

    /// Start a `select` builder. Only the fields you add will be returned.
    pub fn select() -> Self {
        Self {
            mode: SelectionMode::Select,
            fields: Vec::new(),
        }
    }

    /// Start an `include` builder. All scalars are included, plus the
    /// relations you add.
    pub fn include() -> Self {
        Self {
            mode: SelectionMode::Include,
            fields: Vec::new(),
        }
    }

    /// Start an `omit` builder. All scalars minus the fields you list.
    pub fn omit() -> Self {
        Self {
            mode: SelectionMode::Omit,
            fields: Vec::new(),
        }
    }

    /// Add a scalar field to the selection.
    pub fn field(mut self, name: impl Into<String>) -> Self {
        self.fields.push((name.into(), Value::Bool(true)));
        self
    }

    /// Add a relation field with a nested selection.
    pub fn relation(mut self, name: impl Into<String>, nested: Selection) -> Self {
        let nested_obj = nested.build_nested();
        self.fields.push((name.into(), nested_obj));
        self
    }

    /// Build the selection JSON for a top-level query.
    pub fn build(&self) -> Value {
        match &self.mode {
            SelectionMode::Scalars => {
                let mut map = Map::new();
                map.insert("$scalars".into(), Value::Bool(true));
                for (k, v) in &self.fields {
                    map.insert(k.clone(), v.clone());
                }
                Value::Object(map)
            }
            SelectionMode::Select => {
                let mut map = Map::new();
                for (k, v) in &self.fields {
                    map.insert(k.clone(), v.clone());
                }
                Value::Object(map)
            }
            SelectionMode::Include => {
                let mut map = Map::new();
                map.insert("$scalars".into(), Value::Bool(true));
                map.insert("$composites".into(), Value::Bool(true));
                for (k, v) in &self.fields {
                    map.insert(k.clone(), v.clone());
                }
                Value::Object(map)
            }
            SelectionMode::Omit => {
                let mut map = Map::new();
                map.insert("$scalars".into(), Value::Bool(true));
                map.insert("$composites".into(), Value::Bool(true));
                for (name, _) in &self.fields {
                    map.insert(name.clone(), Value::Bool(false));
                }
                Value::Object(map)
            }
        }
    }

    /// Build the selection as a nested object (for relation sub-selections).
    fn build_nested(&self) -> Value {
        let selection = self.build();
        let mut wrapper = Map::new();
        wrapper.insert("selection".into(), selection);
        Value::Object(wrapper)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_selects_scalars() {
        let sel = Selection::scalars();
        let json = sel.build();
        assert_eq!(json, serde_json::json!({ "$scalars": true }));
    }

    #[test]
    fn explicit_select_fields() {
        let sel = Selection::select().field("id").field("email");
        let json = sel.build();
        assert_eq!(json, serde_json::json!({ "id": true, "email": true }));
    }

    #[test]
    fn include_with_relation() {
        let nested = Selection::scalars();
        let sel = Selection::include().relation("posts", nested);
        let json = sel.build();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["$scalars"], Value::Bool(true));
        assert_eq!(obj["$composites"], Value::Bool(true));
        assert!(obj["posts"].is_object());
        assert_eq!(obj["posts"]["selection"], serde_json::json!({ "$scalars": true }));
    }

    #[test]
    fn omit_fields() {
        let sel = Selection::omit().field("password");
        let json = sel.build();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["$scalars"], Value::Bool(true));
        assert_eq!(obj["password"], Value::Bool(false));
    }
}
