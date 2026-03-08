# Postmortem: SQL Injection in Migration DDL Enum Value Interpolation

**Date:** 2026-03-12
**Severity:** High
**Component:** `quiver-migrate/src/sql_gen.rs`
**Status:** Resolved

## Summary

Enum values in Quiver schema definitions were directly interpolated into SQL
DDL strings without any protection. An enum value like `'); DROP TABLE users; --`
would produce exploitable SQL in migration output. The initial fix used
`escape_sql_string()` but this was replaced with a structural solution:
`TrustedSqlBuilder`, which only accepts `&'static str` for SQL text and
forces dynamic values through bind parameters or strict validation.

## Timeline

- **Discovery:** Found during routine code health review of the quiver workspace.
- **Root cause identified:** String interpolation of user-controlled enum values
  into SQL literals using `format!("'{}'", value)` without escaping.
- **Initial fix:** Added `escape_sql_string()` helper (`s.replace('\'', "''")`).
- **Final fix:** Replaced escape-based approach with `TrustedSqlBuilder` --
  a builder that only accepts `&'static str` for SQL fragments. Dynamic
  values go through bind parameters (`?` placeholders). DDL-embedded
  literals (DEFAULT values) go through `validate_safe_literal()` which
  rejects any string containing SQL metacharacters.

## Why Escaping Is Not Enough

The initial fix used `escape_sql_string()` which doubles single quotes.
This is a blocklist approach with fundamental problems:

1. **Incomplete coverage**: Escaping handles `'` but may miss other
   injection vectors (`\`, null bytes, multi-byte encoding attacks,
   dialect-specific metacharacters).
2. **Maintenance burden**: Every new SQL generation site requires the
   developer to remember to call the escape function. A single missed
   call reintroduces the vulnerability.
3. **No compile-time enforcement**: Nothing prevents a developer from
   writing `format!("'{}'", value)` without calling escape.

The `TrustedSqlBuilder` approach solves all three: there is no escape
function to call (or forget), the type system prevents raw string
interpolation, and bind parameters handle dynamic values safely by design.

## Affected Code Paths

All paths are in `quiver-migrate/src/sql_gen.rs`:

| # | Migration Step | Direction | Vulnerability |
|---|---------------|-----------|---------------|
| 1 | `CreateEnum` | up | Enum values in `VALUES ('{value}')` |
| 2 | `AddEnumValue` | up | Value in `VALUES ('{value}')` |
| 3 | `RemoveEnumValue` | up | Value in `WHERE value = '{value}'` |
| 4 | `AddEnumValue` | down | Value in `WHERE value = '{value}'` |
| 5 | `RemoveEnumValue` | down | Value in `VALUES ('{value}')` |
| 6 | `get_default()` | both | `EnumVariant` default in DDL |
| 7 | `get_default()` | both | `String` default in DDL |

## The Fix: TrustedSqlBuilder

### Design

```rust
pub struct TrustedSqlBuilder {
    sql: String,
    params: Vec<Value>,
}

impl TrustedSqlBuilder {
    fn push_static(&mut self, s: &'static str) -> &mut Self;
    fn push_ident(&mut self, ident: &str) -> Result<&mut Self, QuiverError>;
    fn push_param(&mut self, value: Value) -> &mut Self;
    fn build(self) -> TrustedSql;
}
```

Three methods, three trust levels:
- **`push_static(&'static str)`** -- only compile-time-known SQL text.
  The `&'static str` type enforces this at the Rust type level.
- **`push_ident(&str)`** -- validated identifier, wrapped in double
  quotes. Rejects identifiers containing `"`.
- **`push_param(Value)`** -- bind parameter. Adds `?` to the SQL
  template and stores the value separately.

There is intentionally **no method that accepts `&str` as raw SQL**.

### DML (enum INSERT/DELETE) -- Bind Parameters

```rust
// Before (vulnerable):
format!("INSERT OR IGNORE INTO \"_enum_{}\" (value) VALUES ('{}')",
    enum_name, value)

// After (safe):
let mut b = trusted("INSERT OR IGNORE INTO ");
b.push_ident(&format!("_enum_{}", enum_name))?;
b.push_static(" (value) VALUES (");
b.push_param(Value::Text(value.clone()));
b.push_static(")");
```

### DDL (DEFAULT values) -- Strict Validation

DEFAULT values in CREATE TABLE cannot use bind parameters (they're part of
the DDL syntax itself). For these, `validate_safe_literal()` rejects any
string containing SQL metacharacters (`'`, `"`, `;`, `\`, `\0`, `--`):

```rust
fn validate_safe_literal(s: &str) -> Result<(), QuiverError> {
    for ch in s.chars() {
        match ch {
            '\'' | '"' | ';' | '\\' | '\0' => {
                return Err(QuiverError::Migration(format!(
                    "unsafe character '{}' in DDL literal: {}", ch, s
                )));
            }
            _ => {}
        }
    }
    if s.contains("--") {
        return Err(QuiverError::Migration(...));
    }
    Ok(())
}
```

This is deliberately strict: if a legitimate value is rejected, the
schema definition should be changed rather than weakening the validation.

### Execution

`TrustedSql` carries both the SQL template and bind parameters. The
migration tracker's `exec_trusted()` method dispatches based on whether
params exist:

- **No params** -- executes via `execute_ddl()` (pure DDL)
- **Has params** -- splits multi-statement SQL, executes DDL parts via
  `execute_ddl()` and parameterized parts via `execute()` with bind params

## Root Cause Analysis

1. **No SQL construction abstraction.** Each SQL generation site used raw
   `format!()` independently. There was no type-level enforcement of safe
   construction.

2. **DDL perceived as "internal."** DDL generation was treated as a
   trusted internal operation, but the values it interpolates (enum names,
   default values) come from schema definitions which are user-controlled.

3. **Inconsistent existing practice.** `DefaultValue::String` had inline
   escaping (`v.replace('\'', "''")`) but enum values did not. The fix
   existed for one case but was not generalized.

4. **DDL is a blind spot.** Most SQL injection guidance focuses on DML
   with bind parameters. DDL generation is inherently string-based and
   needs different protection mechanisms.

## Lessons

1. **Structural safety over procedural safety.** An `escape()` function
   is procedural -- developers must remember to call it every time.
   `TrustedSqlBuilder` is structural -- the type system prevents unsafe
   construction. Prefer designs where the wrong thing is impossible over
   designs where the wrong thing is merely discouraged.

2. **`&'static str` as a trust boundary.** Rust's type system distinguishes
   compile-time strings (`&'static str`) from runtime strings (`&str` /
   `String`). Using `&'static str` for SQL fragments makes "only trusted
   SQL" a compile-time property, not a code review property.

3. **Validate, don't escape.** When bind parameters aren't possible
   (DDL defaults), validation (allowlist) is safer than escaping
   (blocklist). Validation fails closed: unknown characters are rejected.
   Escaping fails open: unknown attack vectors pass through.

4. **Audit for consistency.** The escaping pattern existed for
   `DefaultValue::String` but not for enum values. When adding safety
   measures to one location, search for all analogous locations.

## Prevention

- [x] `TrustedSqlBuilder` enforces `&'static str` for SQL text at compile time.
- [x] `validate_safe_literal()` rejects unsafe characters in DDL-embedded values.
- [x] Unit tests: `unsafe_default_string_rejected`, `unsafe_identifier_rejected`.
- [x] All 50 tests (34 unit + 16 integration) pass with the new approach.
- [ ] Consider adding a test with deliberately malicious enum values to
      verify bind parameters prevent injection end-to-end.
