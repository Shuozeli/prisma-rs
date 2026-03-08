//! Lexical scope for variable bindings during expression interpretation.

use std::collections::HashMap;
use std::sync::Arc;

use crate::value::{IValue, IntermediateValue};

/// A lexical scope that holds variable bindings.
///
/// Scopes are nested: a child scope can see all bindings from its parent.
/// Parent scopes are shared via `Arc` so that creating a child scope is O(1)
/// instead of cloning the entire ancestor chain.
#[derive(Debug, Clone)]
pub struct Scope {
    bindings: HashMap<String, IntermediateValue>,
    parent: Option<Arc<Scope>>,
}

impl Scope {
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
            parent: None,
        }
    }

    /// Create a child scope that inherits from this scope.
    ///
    /// The current scope is snapshot into an `Arc` so that parent lookups
    /// share memory instead of deep-cloning the entire chain.
    pub fn child(&self) -> Self {
        Self {
            bindings: HashMap::new(),
            parent: Some(Arc::new(self.clone())),
        }
    }

    /// Set a binding in this scope.
    pub fn set(&mut self, name: impl Into<String>, value: IntermediateValue) {
        self.bindings.insert(name.into(), value);
    }

    /// Look up a binding, walking up the scope chain.
    pub fn get(&self, name: &str) -> Option<&IValue> {
        self.get_intermediate(name).map(|iv| &iv.value)
    }

    /// Look up a full IntermediateValue (includes last_insert_id).
    pub fn get_intermediate(&self, name: &str) -> Option<&IntermediateValue> {
        self.bindings
            .get(name)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get_intermediate(name)))
    }

    /// Get the first non-empty value from a list of binding names.
    /// A value is "empty" if it's Null or an empty List.
    pub fn get_first_non_empty(&self, names: &[impl AsRef<str>]) -> IValue {
        for name in names {
            if let Some(val) = self.get(name.as_ref()) {
                match val {
                    IValue::Null => continue,
                    IValue::List(v) if v.is_empty() => continue,
                    _ => return val.clone(),
                }
            }
        }
        IValue::List(vec![])
    }
}

impl Default for Scope {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_basic_get_set() {
        let mut scope = Scope::new();
        scope.set("x", IntermediateValue::new(IValue::Int(42)));
        assert!(matches!(scope.get("x"), Some(IValue::Int(42))));
        assert!(scope.get("y").is_none());
    }

    #[test]
    fn scope_child_inherits_parent() {
        let mut parent = Scope::new();
        parent.set("a", IntermediateValue::new(IValue::String("hello".into())));

        let mut child = parent.child();
        child.set("b", IntermediateValue::new(IValue::Int(10)));

        // Child sees both
        assert!(child.get("a").is_some());
        assert!(child.get("b").is_some());

        // Parent doesn't see child bindings
        assert!(parent.get("b").is_none());
    }

    #[test]
    fn scope_child_shadows_parent() {
        let mut parent = Scope::new();
        parent.set("x", IntermediateValue::new(IValue::Int(1)));

        let mut child = parent.child();
        child.set("x", IntermediateValue::new(IValue::Int(2)));

        assert!(matches!(child.get("x"), Some(IValue::Int(2))));
        assert!(matches!(parent.get("x"), Some(IValue::Int(1))));
    }

    #[test]
    fn get_first_non_empty() {
        let mut scope = Scope::new();
        scope.set("empty", IntermediateValue::new(IValue::List(vec![])));
        scope.set("null", IntermediateValue::new(IValue::Null));
        scope.set("val", IntermediateValue::new(IValue::Int(5)));

        let names = ["empty", "null", "val"];
        let result = scope.get_first_non_empty(&names);
        assert!(matches!(result, IValue::Int(5)));
    }

    #[test]
    fn get_first_non_empty_all_empty() {
        let mut scope = Scope::new();
        scope.set("a", IntermediateValue::new(IValue::Null));

        let names = ["a", "missing"];
        let result = scope.get_first_non_empty(&names);
        assert!(matches!(result, IValue::List(v) if v.is_empty()));
    }
}
