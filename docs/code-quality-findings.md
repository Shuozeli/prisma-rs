# Code Quality Findings

## 1. Dead Code

### 1.1 `SchemaError::Config` variant never constructed
- **File:** `prisma-schema/src/error.rs:11`
- **Problem:** `SchemaError::Config` is defined but never instantiated anywhere in the codebase.
- **Fix:** Remove the variant.
- **Status:** DONE

### 1.2 `#[allow(dead_code)]` on `IntermediateValue::with_last_insert_id`
- **File:** `query-executor/src/value.rs:25-26`
- **Problem:** `#[allow(dead_code)]` suppresses a real warning. The method is used only in tests.
- **Fix:** Replace `#[allow(dead_code)]` with `#[cfg(test)]`.
- **Status:** DONE

### 1.3 `PrismaClient::disposed` field is never set to `true`
- **File:** `prisma-client/src/client.rs:39`
- **Problem:** The `disposed` field is initialized to `false` but never set to `true`. The `Drop` impl checks it but does nothing even when `!self.disposed`. This is a no-op field and a no-op Drop.
- **Fix:** Remove the `disposed` field and the empty `Drop` impl.
- **Status:** DONE

## 2. Duplication

### 2.1 Duplicated `parse_unique_constraint` / `parse_null_field` in DuckDB and SQLite error modules
- **File:** `driver-duckdb/src/error.rs:36-57` and `driver-sqlite/src/error.rs:81-107`
- **Problem:** `parse_unique_constraint` and `parse_null_field` in `driver-duckdb/src/error.rs` are near-identical to `parse_sqlite_constraint` and `parse_sqlite_null_field` in `driver-sqlite/src/error.rs`. Both parse the same SQLite-style constraint messages.
- **Fix:** Accept as-is -- these are in separate crates with different dependencies. Extracting to driver-core would add coupling. Noted but not fixed.
- **Status:** SKIPPED (acceptable cross-crate duplication)

### 2.2 `MappedError::DuckDb` used as catch-all in ADBC and FlightSQL
- **File:** `driver-adbc/src/adapter.rs` (lines 39,53,86,etc), `driver-adbc/src/error.rs:29`, `driver-flightsql/src/error.rs:25`, `driver-flightsql/src/adapter.rs:284`
- **Problem:** ADBC and Flight SQL use `MappedError::DuckDb` as a generic fallback error, even when the underlying database is not DuckDB. This is semantically misleading.
- **Fix:** Accept as-is for now -- these drivers are primarily used with DuckDB backends. A proper fix would add a `MappedError::Generic { message }` variant, but that's a larger refactor.
- **Status:** SKIPPED (design debt, not a bug)

### 2.3 Duplicate enum types `ArgScalarType` / `QueryArity` across `driver-core` and `prisma-ir`
- **File:** `driver-core/src/types.rs:104-127` and `prisma-ir/src/query.rs:96-126`
- **Problem:** `prisma_ir::ArgScalarType` and `prisma_driver_core::ArgScalarType` are identical enums with different serde configurations. The `convert_scalar_type` function in `query-executor/src/render.rs:214-228` is a mechanical 1:1 match mapping between them. Same for `Arity` / `QueryArity`.
- **Fix:** Accept as-is -- `prisma-ir` types use camelCase serde for JSON IR deserialization, while `driver-core` types use lowercase serde for wire compat. The duplication serves a real purpose (decoupling serialization formats). The boilerplate mapping is the correct tradeoff.
- **Status:** SKIPPED (intentional architectural boundary)

## 3. No-op Code

### 3.1 Empty `Drop` impl for `PrismaClient`
- **File:** `prisma-client/src/client.rs:229-237`
- **Problem:** The `Drop` impl checks `self.disposed` but the if-body is empty (just a comment). This is dead code.
- **Fix:** Remove the `Drop` impl entirely.
- **Status:** DONE (merged with 1.3)

## 4. Noise / Comments

### 4.1 Savepoint SQL tests that test `format!` macro
- **File:** `driver-core/src/types.rs:278-315`
- **Problem:** Tests `pg_savepoint_sql`, `mysql_savepoint_sql`, `sqlite_savepoint_sql`, and `savepoint_names_support_special_formats` only verify that `format!("SAVEPOINT {name}")` produces `"SAVEPOINT sp1"`. They test the Rust `format!` macro, not any project code.
- **Fix:** Remove these tests -- they add noise without testing real behavior.
- **Status:** DONE

## 5. Unsafe `unwrap()` Patterns

### 5.1 `RecordBatch::try_new().unwrap()` in ADBC arrow conversion
- **File:** `driver-adbc/src/arrow.rs:340`
- **Problem:** `RecordBatch::try_new(schema, columns).unwrap()` could panic if schema/column construction becomes inconsistent in a future refactor. The schema and columns are built in lockstep in the same function, so the unwrap is currently safe, but it lacks a diagnostic message.
- **Fix:** Replace `.unwrap()` with `.expect("schema and columns built in lockstep must be consistent")`.
- **Status:** DONE

### 5.2 `from_ymd_opt().unwrap()` for epoch date constants
- **Files:** `driver-adbc/src/arrow.rs:148`, `driver-duckdb/src/conversion.rs:121`
- **Problem:** `chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()` is logically infallible (1970-01-01 is always valid), but the bare `unwrap()` provides no diagnostic context if something goes wrong.
- **Fix:** Replace with `.expect("1970-01-01 is a valid date")`.
- **Status:** DONE

### 5.3 `downcast_ref().unwrap()` in ADBC arrow value extraction
- **File:** `driver-adbc/src/arrow.rs:86-239` (20+ occurrences)
- **Problem:** Each `downcast_ref::<T>().unwrap()` in `arrow_value_to_result` is guarded by a match on `array.data_type()`, so the downcast is always correct. However, bare `unwrap()` provides no diagnostic context.
- **Fix:** Accept as-is -- these are all protected by the data-type match guard. Adding `expect` to 20+ lines would be noise. The pattern is idiomatic for Arrow type dispatch.
- **Status:** SKIPPED (logically safe, idiomatic Arrow pattern)

## 6. Silent Failures

### 6.1 Decimal parse failure defaults to 0.0
- **File:** `query-executor/src/value.rs:221-224`
- **Problem:** `PrismaValue::Float(s) => s.parse::<f64>().unwrap_or_else(|| { tracing::error!(...); 0.0 })` silently replaces unparseable decimal strings with 0.0 and logs an error. This violates fail-fast principles -- an invalid decimal should not produce `0.0`.
- **Fix:** Requires changing `prisma_value_to_ivalue` to return `Result<IValue, _>`, which is a large refactor affecting many call sites. Noted as design debt.
- **Status:** SKIPPED (requires signature change across many call sites)

## 7. Bugs

### 7.1 `DatabaseUrl::expose_password()` returns percent-encoded password
- **File:** `driver-core/src/database_url.rs:47`
- **Problem:** `Url::password()` returns the percent-encoded form (e.g., `"p%40ssw0rd"` instead of `"p@ssw0rd"`). `expose_password()` is meant to return the actual password for passing to database drivers, but was returning the encoded form. Additionally, the `empty_password` test expected `Some("")` for `user:@host`, but the `url` crate correctly returns `None` per RFC 3986.
- **Fix:** Decode the password using `percent_encoding::percent_decode_str()` before storing. Fix the `empty_password` test to expect `None`. Fix `password_with_special_chars` test to expect the decoded password.
- **Status:** DONE

## Summary

| # | Category | Issue | Status |
|---|----------|-------|--------|
| 1.1 | Dead code | Unused `SchemaError::Config` | DONE |
| 1.2 | Dead code | `#[allow(dead_code)]` suppressing real warning | DONE |
| 1.3 | Dead code | Unused `disposed` field + no-op Drop | DONE |
| 2.1 | Duplication | DuckDB/SQLite error parsing | SKIPPED |
| 2.2 | Duplication | `MappedError::DuckDb` as generic fallback | SKIPPED |
| 2.3 | Duplication | Duplicate `ArgScalarType`/`QueryArity` enums | SKIPPED |
| 3.1 | No-op | Empty `PrismaClient` Drop impl | DONE |
| 4.1 | Noise | Tests that test `format!` macro | DONE |
| 5.1 | Unsafe unwrap | `RecordBatch::try_new().unwrap()` | DONE |
| 5.2 | Unsafe unwrap | `from_ymd_opt().unwrap()` without message | DONE |
| 5.3 | Unsafe unwrap | Arrow `downcast_ref().unwrap()` | SKIPPED |
| 6.1 | Silent failure | Decimal parse defaults to 0.0 | SKIPPED |
| 7.1 | Bug | `DatabaseUrl` returns percent-encoded password | DONE |
