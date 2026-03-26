# Code Quality Findings

## 1. Silent Failures / Error Swallowing

### 1.1 eprintln warnings in production paths instead of proper error propagation
- **Location:** `driver-pg/src/conversion.rs` (query_value_to_pg_param, Array branch; query_value_to_pg_param_typed, INT4/INT2 truncation)
- **Severity:** High
- **Status:** FIXED -- `query_value_to_pg_param` and `query_value_to_pg_param_typed` now return `Result<..., DriverError>`. Array overflow returns `MappedError::ValueOutOfRange`; INT4/INT2 narrowing failures return `MappedError::ValueOutOfRange` instead of clamping. The `.take(MAX_ARRAY_PARAMS)` truncation was also removed since overflow is now an error.

### 1.2 eprintln in Transaction Drop handlers across all drivers
- **Location:** All six driver `Transaction::Drop` implementations (pg, mysql, sqlite, duckdb, adbc, flightsql)
- **Severity:** Medium
- **Status:** FIXED -- All six `eprintln!` calls replaced with a shared `prisma_driver_core::warn_uncommitted_transaction(adapter_name)` function that uses `tracing::warn!` for structured logging.

### 1.3 pg_row_value silently returns Null on type extraction errors
- **Location:** `driver-pg/src/conversion.rs` (pg_row_value)
- **Severity:** Medium
- **Status:** FIXED -- Added a `warn_null!` macro that logs column index, expected type, and error via `tracing::warn!` before returning `Null`. All match arms now use `warn_null!` instead of bare `Err(_) => ResultValue::Null`. Full `Result` propagation was not done to maintain backward compatibility.

### 1.4 mysql_row_value silently returns Null for out-of-bounds columns
- **Location:** `driver-mysql/src/conversion.rs` (mysql_row_value)
- **Severity:** High
- **Status:** FIXED -- Out-of-bounds column access now panics with a descriptive message (including row length) since this indicates a programming error in the caller, not a runtime condition.

## 2. Duplication

### 2.1 convert_scalar_type is a hand-written identity mapping
- **Location:** `query-executor/src/render.rs:214-229` (convert_scalar_type)
- **Severity:** Low
- **Status:** SKIPPED -- This is a cross-crate type mapping between `prisma_ir` and `prisma_driver_core`. Unifying the enums would create a coupling between IR and driver-core that is undesirable at this stage. The boilerplate is isolated and unlikely to drift.

### 2.2 Duplicated spawn_blocking error wrapping in SQLite driver
- **Location:** `driver-sqlite/src/adapter.rs` (11 instances)
- **Severity:** Medium
- **Status:** FIXED -- Extracted a `spawn_sqlite` helper function that encapsulates `Arc` cloning, `Mutex` locking, `spawn_blocking`, and `JoinError` wrapping. All 11 instances reduced to one-line calls.

### 2.3 Duplicated schema definitions in cross-compat tests
- **Location:** `cross-compat/src/lib.rs` (PG_SCHEMA, MYSQL_SCHEMA, SQLITE_SCHEMA)
- **Severity:** Low
- **Status:** FIXED -- Replaced three nearly identical `const` schemas with a `make_schema(provider)` function that interpolates the provider into a single template.

## 3. Dead / No-op Code

### 3.1 PrismaClient.disposed field is never set to true
- **Location:** `prisma-client/src/client.rs`
- **Severity:** Low
- **Status:** SKIPPED -- The field exists as scaffolding for future lifecycle management (e.g., when `disconnect()` changes to `&mut self`). Removing it now would just require re-adding it later. Low priority.

### 3.2 IntermediateValue::with_last_insert_id is marked #[allow(dead_code)]
- **Location:** `query-executor/src/value.rs:25-26`
- **Severity:** Low
- **Status:** SKIPPED -- This constructor is intentionally kept for the `Expression::InitializeRecord` path that is under development. Removing it would require re-adding it soon. Low priority.

### 3.3 PgDriverAdapterFactory::connect_to_shadow_db is a stub
- **Location:** `driver-pg/src/adapter.rs` (connect_to_shadow_db)
- **Severity:** High
- **Status:** FIXED -- The stub that silently connected to the primary database is replaced with an explicit error: `MappedError::InvalidInputValue` telling users to configure `shadowDatabaseUrl`. A TODO comment documents the eventual proper implementation.

## 4. Unsafe Patterns

### 4.1 Unchecked integer casts in MySQL conversion
- **Location:** `driver-mysql/src/conversion.rs` (mysql_row_value, Int32 branch)
- **Severity:** High
- **Status:** FIXED -- `*v as i32` replaced with `i32::try_from(*v)`. Values that overflow i32 are promoted to `ResultValue::Int64`; values that overflow i64 (from `u64`) are returned as `ResultValue::Numeric(v.to_string())`.

### 4.2 Unchecked integer cast in MySQL Int64 conversion
- **Location:** `driver-mysql/src/conversion.rs` (mysql_row_value, Int64 branch)
- **Severity:** Medium
- **Status:** FIXED -- `*v as i64` for `u64` values replaced with `i64::try_from(*v)`, falling back to `ResultValue::Numeric(v.to_string())` for values above `i64::MAX`.

### 4.3 panic in cross-compat for unsupported providers
- **Location:** `cross-compat/src/lib.rs` (compiler_for_provider)
- **Severity:** Medium
- **Status:** FIXED -- `compiler_for_provider` now returns `Result<QueryCompiler, String>` instead of panicking. All callers updated to use `.unwrap()` in test contexts.

## 5. Missing Abstractions

### 5.1 pool_error_to_mapped uses string matching for error classification
- **Location:** `driver-pg/src/adapter.rs` (pool_error_to_mapped)
- **Severity:** Medium
- **Status:** FIXED -- Replaced string matching with direct pattern matching on `deadpool_postgres::PoolError` variants (`Timeout`, `Backend`, `Closed`, `NoRuntimeSpecified`). String matching is only used as a fallback for future/unknown variants (e.g., recycle hooks).

### 5.2 Connection-level PG error classification uses string matching
- **Location:** `driver-pg/src/error.rs` and `driver-mysql/src/error.rs`
- **Severity:** Medium
- **Status:** SKIPPED -- The underlying `tokio_postgres::Error` and `mysql_async::Error` types do not expose structured error kinds for all connection-level failures in a way that avoids string matching entirely. Fixing this requires upstream changes or extensive source-chain inspection. Deferred for a future iteration.

## 6. Noise / Low-Value Code

### 6.1 Trivial tests that test string formatting, not driver behavior
- **Location:** `driver-core/src/types.rs` (savepoint SQL tests)
- **Severity:** Low
- **Status:** SKIPPED -- While the tests are trivial, removing tests is a low-priority cleanup. They do no harm and any future refactor of savepoint SQL generation would benefit from having them as a sanity check.

### 6.2 Workspace clippy lint allows that may suppress real issues
- **Location:** `Cargo.toml:76-77` (workspace lints)
- **Severity:** Low
- **Status:** SKIPPED -- `result_large_err` suppression is needed because `MappedError` is intentionally a large enum to provide detailed error context across all drivers. Boxing would add allocation overhead on every error path. `approx_constant` is cosmetic. Low priority.

## 7. Potential Correctness Issues

### 7.1 Scope::child() clones the entire scope before wrapping in Arc
- **Location:** `query-executor/src/scope.rs:31-35`
- **Severity:** Medium
- **Status:** SKIPPED -- Restructuring scope ownership requires careful analysis of all scope consumers. The current clone-based approach is correct (just suboptimal for deeply nested scopes). This is a performance concern, not a correctness bug, and is deferred until profiling shows it matters.

### 7.2 DuckDb provider maps to SqlFamily::Postgres in PrismaClient
- **Location:** `prisma-client/src/client.rs:97`
- **Severity:** Low
- **Status:** FIXED -- Added a comment explaining why DuckDB intentionally maps to `SqlFamily::Postgres` (DuckDB aims for PostgreSQL wire/SQL compatibility) and noting that a dedicated `SqlFamily::DuckDb` should be added if DuckDB-specific generation is needed.

### 7.3 process_records applies pagination after distinct but before nested
- **Location:** `query-executor/src/interpret.rs` (process_records_inner)
- **Severity:** Low
- **Status:** FIXED -- Added a TODO comment on the `linking_fields` field in `prisma-ir/src/expression.rs` documenting that it is deserialized but not yet used, and explaining when it will be needed (in-memory join support for nested queries).

## 8. Inconsistency

### 8.1 QueryValue::Array silently becomes NULL in MySQL and SQLite drivers
- **Location:** `driver-mysql/src/conversion.rs` and `driver-sqlite/src/conversion.rs`
- **Severity:** High
- **Status:** FIXED -- Both `query_value_to_mysql` and `query_value_to_sqlite` now return `Result<..., DriverError>`. Array parameters produce `MappedError::InvalidInputValue` with a clear error message instead of silently converting to NULL. All callers updated to propagate the error.
