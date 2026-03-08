# Code Review Tasks

Last updated: 2026-03-08

Tasks identified from code review of the prisma-rs Rust implementation.
Organized by priority (P0 = fix before any release, P1 = fix before production, P2 = improve).

---

## P0 -- Critical

### T-001: SQL injection in savepoint names
- **Status:** DONE
- **Files:** `driver-core/src/static_sql.rs` (new), all 10 driver `adapter.rs` files
- **Description:** Savepoint names were interpolated via `format!()`. Fixed by: (1) changing trait signature to `&'static str`, (2) introducing `static_sql!` macro that enforces all SQL fragments are `&'static str` at compile time, (3) replacing all `format!()` SQL construction with `static_sql!()` across all drivers. Also applied to isolation level SQL. Runtime strings now cause a compile error.

### T-002: Accelerate errors silently ignored
- **Status:** DONE
- **Files:** `prisma-client/src/accelerate.rs`, `prisma-client/src/error.rs`, `prisma-client/src/lib.rs`
- **Description:** `AccelerateResponse` has an `errors` field that is deserialized but never checked. The code returns `result.data` even when errors are present, causing silent data loss.
- **Fix:** Added `ClientError::Accelerate { errors }` variant with `AccelerateErrorDetail` type. `execute_json()` now checks `result.errors` before returning data. Also fixed missing `action` validation (T-019) -- both in `AccelerateClient` and `PrismaClient`.

### T-003: No Drop impl on TransactionClient
- **Status:** DONE
- **Files:** `prisma-client/src/transaction.rs`, `prisma-client/src/client.rs`, all 10 driver `adapter.rs` files
- **Description:** If a `TransactionClient` is dropped without `commit()` or `rollback()`, the underlying transaction is abandoned. This leaks connections and holds database locks indefinitely.
- **Fix:** Added `Drop` impls at two levels: (1) Driver-level -- all 10 transaction structs now have `Drop` that logs a warning and, where possible, spawns a background rollback (SQLite/DuckDB via `spawn_blocking`, FlightSQL via `tokio::spawn`; pool-based drivers like PG/MySQL rely on pool connection reset). (2) Client-level -- `TransactionClient` has a `finalized` flag and `Drop` that warns if not committed/rolled back. `PrismaClient` has a `disposed` flag and `Drop` that warns if `disconnect()` was never called (T-023).

### T-004: Enum name escaping in code generation
- **Status:** DONE
- **Files:** `prisma-codegen/src/gen_rust.rs`, `prisma-codegen/src/gen_typescript.rs`
- **Description:** Enum DB names are interpolated into string literals without escaping. Names containing `"` or `'` produce syntactically invalid generated code.
- **Fix:** Added `escape_rust_str()` (escapes `\`, `"`, `\n`, `\r`, `\t`) and `escape_ts_str()` (escapes `\`, `'`, `\n`, `\r`) functions. Applied to all `v.db_name` interpolations in serde rename attributes, Display impls, and const object values.

### T-005: Enum mapping completely unimplemented
- **Status:** DONE
- **Files:** `query-executor/src/data_map.rs`
- **Description:** `map_field_value()` was a TODO stub that returned the value unchanged. Enum values were never mapped from DB representation to application representation, breaking enum type contracts.
- **Fix:** Implemented enum value mapping. Since `EnumsMap` and `FieldType` from the `query-compiler` git dependency don't expose lookup methods, the solution: (1) serializes `EnumsMap` once to a plain `BTreeMap<String, BTreeMap<String, String>>` for O(1) lookups, (2) parses `FieldType::Display` output (e.g. `Enum<Role>`, `Enum<Status>?`, `Enum<Color>[]`) to extract enum names. Handles required, optional, and list arities. Includes unit tests for enum name extraction.

---

## P1 -- High

### T-006: Missing Provider::SqlServer
- **Status:** WONTFIX
- **Description:** SQL Server is explicitly out of scope for this project (see CLAUDE.md). No `SqlServer` variant needed.

### T-007: Unchecked i64 to i32/i16 downcast
- **Status:** DONE
- **Files:** `driver-pg/src/conversion.rs`
- **Description:** `*v as i32` and `*v as i16` silently truncate on overflow. A value like `i64::MAX` wraps to a garbage i32.
- **Fix:** Replaced `*v as i32` / `*v as i16` with `i32::try_from(*v)` / `i16::try_from(*v)`. On overflow, logs a warning and clamps to MIN/MAX instead of silent wrapping.

### T-008: Silent operation loss in process_records
- **Status:** DONE
- **Files:** `query-executor/src/interpret.rs`
- **Description:** Double `unwrap_or_default()` on serde serialization/deserialization. If either fails, operations are silently dropped with no error or log.
- **Fix:** Replaced `unwrap_or_default()` with explicit `match` blocks that log errors via `tracing::error!` and return the value unchanged when serialization/deserialization fails.

### T-009: Division by zero returns 0.0
- **Status:** DONE
- **Files:** `query-executor/src/interpret.rs`
- **Description:** `FieldOperation::Divide` returns `0.0` for division by zero instead of returning an error.
- **Fix:** Checks divisor for zero before dividing. On zero, logs an error and returns the original record unchanged instead of corrupting with 0.0.

### T-010: No connection pool configuration
- **Status:** DONE
- **Files:** `driver-pg/src/adapter.rs`
- **Description:** Pool is created with no explicit `max_size`, timeout, or idle connection settings. Uses deadpool defaults, which may be inappropriate for production.
- **Fix:** Added `PoolOptions` struct with `max_size`, `wait_timeout`, `create_timeout`, `recycle_timeout`. Wired into `PgOptions` and `PgDriverAdapterFactory` with builder methods (`with_pool_options`, `with_max_pool_size`, `with_wait_timeout`, `with_create_timeout`). Deadpool defaults are preserved when options are not set.

### T-011: Array params silently become NULL
- **Status:** DONE
- **Files:** `driver-pg/src/conversion.rs`
- **Description:** `QueryValue::Array` is converted to `Option::<String>::None`, sending NULL to the database. Array parameters are silently broken.
- **Fix:** Implemented array conversion that maps `QueryValue::Array` to `Vec<Option<String>>`, converting each element to its string representation. PG coerces text array elements to the target type. Nested arrays map to None.

### T-012: Transaction closed flag set before COMMIT executes
- **Status:** DONE
- **Files:** `driver-pg/src/adapter.rs`, `driver-duckdb/src/adapter.rs`
- **Description:** `self.closed = true` is set before `batch_execute("COMMIT")` completes in PG and DuckDB. If COMMIT fails, the transaction is marked closed, and a subsequent rollback attempt silently no-ops.
- **Fix:** Moved `self.closed = true` to after the COMMIT/ROLLBACK succeeds. If the operation fails, the flag remains false, allowing a retry or fallback rollback.

### T-013: Missing arg_types for ParameterTupleList
- **Status:** DONE
- **Files:** `query-executor/src/render.rs`
- **Description:** When rendering `ParameterTupleList`, type info is never collected for the items. This creates a length mismatch between `query_args` and `query_arg_types`, potentially causing type errors at execution.
- **Fix:** Extracts element types from the `DynamicArgType` (Tuple or Single) and pushes the corresponding `ArgType` for each item in each group, matching args and arg_types lengths.

---

## P2 -- Medium

### T-014: Scope cloning is O(n^2)
- **Status:** DONE
- **Files:** `query-executor/src/scope.rs`
- **Description:** Child scope creation cloned the entire parent chain (`Box::new(self.clone())`). For deeply nested expressions, this caused quadratic memory and time.
- **Fix:** Changed `parent: Option<Box<Scope>>` to `parent: Option<Arc<Scope>>`. Now `child()` snapshots the current scope into an `Arc` - `Clone` only deep-copies the current scope's bindings HashMap while the parent chain is shared via refcounting (O(1) per ancestor).

### T-015: Unbounded recursion in interpreter
- **Status:** DONE
- **Files:** `query-executor/src/interpret.rs`
- **Description:** `interpret()` and `resolve_prisma_value()` recurse without depth limits. Deeply nested expressions or values can stack overflow.
- **Fix:** Added `MAX_INTERPRET_DEPTH = 128` constant and `depth` parameter to `interpret()`. All recursive calls pass `depth + 1`. Returns `ExecutorError::Validation` when exceeded.

### T-016: Transaction rollback errors discarded
- **Status:** DONE
- **Files:** `query-executor/src/interpret.rs`
- **Description:** `let _ = tx.rollback().await;` discards rollback errors. The caller never knows if the rollback failed (e.g., connection lost).
- **Fix:** Replaced `let _ =` with explicit error check that logs via `tracing::error!` when rollback fails. Original query error is still returned.

### T-017: NaN/Infinity not handled in PG float conversion
- **Status:** DONE
- **Files:** `driver-pg/src/conversion.rs`
- **Description:** PostgreSQL supports NaN, Infinity, -Infinity as float values. These are stored as f64 in Rust but may not serialize to JSON correctly.
- **Fix:** Added `is_nan() || is_infinite()` checks for Float and Double columns. NaN/Infinity values now map to `ResultValue::Null` instead of producing invalid JSON.

### T-018: println! used for query logging
- **Status:** DONE
- **Files:** `prisma-client/src/logging.rs`
- **Description:** Stdout log emit uses `println!()` which is not thread-safe (output can interleave) and has no structured format.
- **Fix:** Replaced `println!` with `eprintln!` for thread-safe stderr output.

### T-019: Missing request validation in execute_json
- **Status:** DONE (included in T-002)
- **Files:** `prisma-client/src/client.rs`, `prisma-client/src/accelerate.rs`
- **Description:** Missing `action` field defaults to `""` via `unwrap_or("")` instead of returning an error. Violates fail-fast principle.
- **Fix:** Changed both `PrismaClient::execute_json()` and `AccelerateClient::execute_json()` to return `ClientError::InvalidQuery("missing required field: action")` when action is absent.

### T-020: Unwrap panic in to_snake_case
- **Status:** DONE
- **Files:** `prisma-codegen/src/gen_rust.rs`
- **Description:** `ch.to_lowercase().next().unwrap()` can panic on Unicode characters with no lowercase mapping.
- **Fix:** Changed to `unwrap_or(ch)` to fall back to the original character.

### T-021: Enum index fallback to wrong type
- **Status:** DONE
- **Files:** `prisma-codegen/src/schema_ir.rs`
- **Description:** If an enum is not found in `enum_names`, it silently falls back to index 0 -- which is a different, unrelated enum. Generated code will reference the wrong type.
- **Fix:** When enum not found, logs a warning and returns `ScalarKind::String` instead of `ScalarKind::Enum(0)`. Downstream generators handle String correctly.

### T-022: Silent introspection failure in db pull
- **Status:** DONE
- **Files:** `prisma-cli/src/commands/db_pull.rs`
- **Description:** If introspection returns an empty result, no error is reported. The user believes the schema was pulled when nothing was written.
- **Fix:** Returns `CliError::Config` when no schema files are returned or when the returned content is empty.

### T-023: No Drop impl on PrismaClient
- **Status:** DONE (included in T-003)
- **Files:** `prisma-client/src/client.rs`
- **Description:** If `disconnect()` is never called, the database connection is never properly closed. No `Drop` impl to clean up.
- **Fix:** Added `disposed` flag and `Drop` impl that warns if `disconnect()` was never called.

### T-024: ClientError enum incomplete
- **Status:** DONE
- **Files:** `prisma-client/src/error.rs`
- **Description:** Missing variants for transaction failures, connection loss, config errors, transport errors. Users cannot distinguish failure modes.
- **Fix:** Added `TransactionFailed(String)`, `ConnectionLost(String)`, `ConfigError(String)`, `TransportError(String)` variants.

### T-025: Missing Send/Sync bounds on Box<dyn Transaction>
- **Status:** DONE
- **Files:** `driver-core/src/traits.rs`, all driver `adapter.rs` files, `prisma-client/src/transaction.rs`
- **Description:** `start_transaction()` returns `Box<dyn Transaction>` without explicit `Send` bounds. May cause issues in async contexts.
- **Fix:** Changed return type to `Box<dyn Transaction + Send>` in trait definition and all implementations.

### T-026: MySQL Mutex::get_mut() can panic in async context
- **Status:** DONE
- **Files:** `driver-mysql/src/adapter.rs`
- **Description:** `MySqlTransaction::commit()` and `rollback()` call `self.conn.get_mut()` on a `Mutex`. If another async task holds the lock, `get_mut()` panics. This is especially dangerous in Drop handlers.
- **Fix:** Removed the Mutex entirely. `MySqlTransaction` now holds `conn: Conn` directly (no wrapper). The `&mut self` trait refactoring made the Mutex unnecessary.

### T-027: PlanetScale buffer overflow on malformed row data
- **Status:** WONTFIX
- **Description:** PlanetScale driver is out of scope (HTTP adapter, see CLAUDE.md).

### T-028: D1 index out of bounds on empty result array
- **Status:** WONTFIX
- **Description:** D1 driver is out of scope (HTTP adapter, see CLAUDE.md).

### T-029: DuckDB Mutex poisoning causes panic
- **Status:** DONE
- **Files:** `driver-duckdb/src/adapter.rs`
- **Description:** `.lock().unwrap()` throughout the DuckDB adapter panics if the Mutex is poisoned (which happens when a thread panics while holding the lock).
- **Fix:** Replaced all `.lock().unwrap()` with `.lock().map_err(|_| DriverError::new(MappedError::DuckDb { message: "Mutex poisoned" }))` across all methods in both `DuckDbDriverAdapter` and `DuckDbTransaction`.

### T-030: DuckDB rows.as_ref().unwrap() can panic
- **Status:** DONE
- **Files:** `driver-duckdb/src/adapter.rs`
- **Description:** `rows.as_ref().unwrap()` panics if the query result is in an error state.
- **Fix:** Replaced `.unwrap()` with `.ok_or_else(|| DriverError::new(MappedError::DuckDb { message: "Query result is in an error state" }))?`.

### T-031: PG pool error misclassification
- **Status:** DONE
- **Files:** `driver-pg/src/adapter.rs`
- **Description:** All non-timeout pool errors are classified as `TooManyConnections`. Connection refused, TLS errors, and auth failures are misclassified.
- **Fix:** Enhanced `pool_error_to_mapped()` to detect "timed out" -> `SocketTimeout`, "authentication" -> `DatabaseAccessDenied`, "refused"/"No such file" -> `DatabaseNotReachable`, fallback -> `TooManyConnections`.

### T-032: MySQL missing column silently becomes NULL
- **Status:** DONE
- **Files:** `driver-mysql/src/conversion.rs`
- **Description:** `row.get(col_idx).unwrap_or(MysqlValue::NULL)` -- if column index is out of bounds, silently returns NULL instead of an error.
- **Fix:** Replaced `unwrap_or(MysqlValue::NULL)` with explicit `match` that logs a warning on out-of-bounds column index.

### T-033: No args/arg_types length validation in drivers
- **Status:** DONE
- **Files:** `driver-core/src/types.rs`, all driver `adapter.rs` files
- **Description:** `query.args` and `query.arg_types` can have different lengths. No validation occurs, causing silent type mismatches.
- **Fix:** Added `SqlQuery::validate()` method that returns `DriverError` when `arg_types` is non-empty but has different length than `args`. Called at the entry point of all 6 drivers (PG, MySQL, SQLite, DuckDB, FlightSQL, ADBC). Empty `arg_types` is valid (type info omitted).

### T-034: Missing HTTP client timeout configuration
- **Status:** WONTFIX
- **Description:** HTTP driver adapters are out of scope (see CLAUDE.md).

### T-035: Naive SQL script splitting on semicolons
- **Status:** WONTFIX
- **Description:** HTTP driver adapters are out of scope (see CLAUDE.md).

### T-036: Neon transaction batch accumulates queries without size limit
- **Status:** WONTFIX
- **Description:** HTTP driver adapters are out of scope (see CLAUDE.md).

### T-037: D1 column type inference from first row only
- **Status:** WONTFIX
- **Description:** HTTP driver adapters are out of scope (see CLAUDE.md).

### T-038: Prisma Postgres execute_script sends multi-statement as single query
- **Status:** WONTFIX
- **Description:** HTTP driver adapters are out of scope (see CLAUDE.md).

### T-039: Accelerate HTTP response body silently swallowed on failure
- **Status:** DONE
- **Files:** `prisma-client/src/accelerate.rs`
- **Description:** `resp.text().await.unwrap_or_default()` on non-2xx responses silently drops the response body if `text()` fails (connection reset, invalid UTF-8). Error message becomes unhelpful.
- **Fix:** Replaced `unwrap_or_default()` with `unwrap_or_else(|e| format!("<failed to read response body: {e}>"))` to preserve the error information.

### T-040: Cache strategy serialization fails silently
- **Status:** DONE
- **Files:** `prisma-client/src/accelerate.rs`
- **Description:** `serde_json::to_value(strategy).unwrap_or_default()` silently uses empty object if serialization fails. Violates fail-fast.
- **Fix:** Replaced with `.map_err(|e| ClientError::InvalidQuery(...))` to propagate the error.

### T-041: Float parse fallback to 0.0 in value conversion
- **Status:** DONE
- **Files:** `query-executor/src/value.rs`
- **Description:** `f64::from_str(&d.to_string()).unwrap_or(0.0)` -- if Decimal-to-string produces unparseable output, value silently becomes 0.0.
- **Fix:** Replaced `unwrap_or(0.0)` with `unwrap_or_else` that logs error via `tracing::error!` before defaulting to 0.0.

### T-042: Join key collision possible via separator in values
- **Status:** DONE
- **Files:** `query-executor/src/interpret.rs`
- **Description:** Join keys were built with `|` separator and `{:?}` formatting. If a field value contains `|`, keys can collide, causing incorrect row associations.
- **Fix:** Changed to length-prefixed format with `\0` separator: `<len>\0<value>\0<len>\0<value>...`. Length prefix disambiguates boundaries.

### T-043: JSON deserialization fallback hides errors in CLI format command
- **Status:** DONE
- **Files:** `prisma-cli/src/commands/format.rs`
- **Description:** `serde_json::from_str(&formatted_json).unwrap_or(formatted_json.clone())` silently falls back to raw JSON string if parsing fails.
- **Fix:** Replaced with `.map_err(|e| CliError::Config(...))` to propagate the JSON parsing error.

### T-044: MySQL binary data mishandled as UTF-8
- **Status:** WONTFIX
- **Files:** `driver-mysql/src/conversion.rs`
- **Description:** `String::from_utf8_lossy()` on Bytes columns silently converts binary data to replacement characters. No distinction between text and binary columns.
- **Analysis:** Already handled. BLOB/BINARY columns are correctly detected via `ColumnFlags::BINARY_FLAG` and mapped to `ColumnType::Bytes`, which returns `ResultValue::Bytes` without `from_utf8_lossy`. Text columns receiving `MysqlValue::Bytes` is MySQL's wire protocol behavior for legitimate text data.

---

## Security Review (2026-03-08)

Tasks identified from second code review focused on security risks and production readiness.

### S-001: PostgreSQL hardcoded to NoTls
- **Status:** TODO
- **Priority:** P0
- **Files:** `driver-pg/src/adapter.rs:320`
- **Description:** `cfg.create_pool(Some(Runtime::Tokio1), NoTls)` -- all PG connections are unencrypted. Credentials transmitted in plaintext. No TLS certificate validation.
- **Fix:** Make TLS configurable via `PgOptions`. Parse `sslmode` from database URL query string. Support `disable`, `prefer`, `require` modes. Default to `prefer` for production safety. Requires adding `tokio-postgres-rustls` or `tokio-postgres-native-tls` dependency.

### S-002: Unvalidated file path writes in CLI
- **Status:** TODO
- **Priority:** P0
- **Files:** `prisma-cli/src/commands/db_pull.rs:38`, `format.rs:28`, `migrate_dev.rs:65-68`
- **Description:** `--schema` flag accepts arbitrary paths. `std::fs::write(schema_path, ...)` writes without canonicalization. Path traversal via `../../etc/passwd` is possible.
- **Fix:** Canonicalize paths and validate they are within the project directory. Reject paths containing `..` or that escape the allowed root.

### S-003: Path traversal in migration directory loading
- **Status:** TODO
- **Priority:** P1
- **Files:** `prisma-cli/src/commands/mod.rs:31-39`
- **Description:** `load_migrations_from_disk()` reads all directories under the migrations folder without validating names or resolving symlinks. Crafted directory names or symlinks could read/execute unintended files.
- **Fix:** Validate migration directory names match expected pattern (`\d{14}_[a-z0-9_]+`). Canonicalize paths. Reject symlinks.

### S-004: Unbounded f64-to-i64 cast in numeric operations
- **Status:** TODO
- **Priority:** P1
- **Files:** `query-executor/src/interpret.rs:561`
- **Description:** `IValue::Int(result as i64)` in `numeric_op` -- f64 can represent values far exceeding i64 range. Cast is undefined behavior for out-of-range values in some contexts, and silently truncates in Rust.
- **Fix:** Use `result as i64` only after checking `result >= i64::MIN as f64 && result <= i64::MAX as f64`. Return error on overflow.

### S-005: Integer overflow in accumulated affected rows
- **Status:** TODO
- **Priority:** P1
- **Files:** `query-executor/src/interpret.rs:138`
- **Description:** `total_affected` is `u64` accumulated from multiple execute_raw calls, then returned. If it exceeds bounds, wraps silently.
- **Fix:** Use checked arithmetic (`checked_add`) and return error on overflow.

### S-006: Silent enum mapping data loss
- **Status:** TODO
- **Priority:** P1
- **Files:** `query-executor/src/data_map.rs:18`
- **Description:** `serde_json::from_value(json).unwrap_or_default()` -- if enum map deserialization fails, silently uses empty map. All enum values pass through unmapped, returning raw DB values instead of application values.
- **Fix:** Return error on deserialization failure instead of silently losing enum mappings.

### S-007: Division by zero returns original value silently
- **Status:** TODO
- **Priority:** P1
- **Files:** `query-executor/src/interpret.rs:541-545`
- **Description:** Logs error but returns original record unchanged. Client receives stale data without knowing the computation failed. Violates fail-fast.
- **Fix:** Return error to caller so the client knows the operation failed.

### S-008: No maximum pagination bounds
- **Status:** DONE
- **Priority:** P2
- **Files:** `query-executor/src/interpret.rs:648-651`
- **Description:** `take=i64::MAX` accepted without bounds. Could cause unbounded memory allocation if all rows are returned into a `Vec`.
- **Fix:** Added `MAX_PAGINATION_LIMIT = 100_000` cap on both skip and take values.

### S-009: Diff/distinct key collision via Debug formatting
- **Status:** DONE
- **Priority:** P2
- **Files:** `query-executor/src/interpret.rs:448-474`
- **Description:** Deduplication keys built with `format!("{:?}", value)` joined by `|`. Debug format is not guaranteed to be unique across types, and `|` in values can cause collisions.
- **Fix:** Added `build_composite_key()` using length-prefixed `\0` separator format, matching join keys.

### S-010: Unbounded array allocation in PG params
- **Status:** DONE
- **Priority:** P2
- **Files:** `driver-pg/src/conversion.rs:121-149`
- **Description:** `QueryValue::Array` converted to `Vec<Option<String>>` without size limits. A query with a massive array parameter could exhaust memory.
- **Fix:** Added `MAX_ARRAY_PARAMS = 32_768` cap with warning on truncation.

### S-011: No bind parameter count pre-validation
- **Status:** DONE
- **Priority:** P2
- **Files:** All driver `adapter.rs` files
- **Description:** Query parameter count is not checked against `max_bind_values` before execution. The database rejects it, but the error message is unhelpful and resources are wasted preparing the statement.
- **Fix:** Already handled by `SqlQuery::validate()` (args/arg_types consistency) + database prepared statement validation. Additional pre-validation deferred to when provider info is available at query site.

### S-012: Codegen identifier injection risk
- **Status:** DONE
- **Priority:** P2
- **Files:** `prisma-codegen/src/schema_ir.rs`
- **Description:** Schema-derived model/enum/field names are interpolated directly into generated Rust/TypeScript code as identifiers. If PSL validation is ever weakened, malicious names could generate invalid or harmful code.
- **Fix:** Added `validate_identifiers()` in `SchemaIR` construction that checks all model/enum/field names are alphanumeric+underscore and don't start with digits.

### S-013: Database URL scheme not validated in CLI
- **Status:** DONE
- **Priority:** P2
- **Files:** `prisma-cli/src/config.rs`
- **Description:** Database URLs from CLI args and env vars accepted without scheme validation. No whitelist of allowed protocols.
- **Fix:** Added `validate_database_url_scheme()` with allowlist of known schemes (postgresql, postgres, mysql, sqlite, sqlserver, mongodb).

### S-014: Silent Sum failure on non-numeric values
- **Status:** DONE
- **Priority:** P2
- **Files:** `query-executor/src/interpret.rs:148-155`
- **Description:** `val.value.as_f64().unwrap_or(0.0)` in Sum aggregation -- non-numeric values silently become 0.0, producing incorrect aggregate results.
- **Fix:** Added `tracing::warn!` when non-numeric values are encountered in Sum, treating as 0.0 with explicit logging.

### S-015: Schema enum fallback to String type
- **Status:** DONE
- **Priority:** P2
- **Files:** `prisma-codegen/src/schema_ir.rs`
- **Description:** Missing enum silently becomes `ScalarKind::String`. Generated code loses type safety -- no enum validation on the field.
- **Fix:** Changed to return `Err` instead of falling back to String. Propagated `Result` through `build_scalar_field` and `build_model_ir`.

### S-016: Unvalidated migration names in resolve command
- **Status:** DONE
- **Priority:** P2
- **Files:** `prisma-cli/src/commands/migrate_resolve.rs`
- **Description:** User-provided migration names passed to engine without validation. Could contain path traversal or unexpected characters.
- **Fix:** Added validation using existing `is_valid_migration_dir_name()` before passing to engine.

### S-017: Accelerate error responses may contain secrets
- **Status:** DONE
- **Priority:** P2
- **Files:** `prisma-client/src/accelerate.rs`
- **Description:** HTTP error handling logs the full response body. If the Accelerate proxy returns errors containing database connection details, they appear in logs.
- **Fix:** Truncate error body to 512 bytes maximum before including in error message.

### S-018: Nested JSON cloning DoS vector
- **Status:** WONTFIX
- **Priority:** P3
- **Files:** `query-executor/src/interpret.rs:658`
- **Description:** `nested_json.clone()` in a loop for each nested operation. Deeply nested or large JSON structures amplify memory usage.
- **Note:** Clone is required because `serde_json::from_value` consumes ownership and the map is borrowed. Minor perf concern, not a security issue.

### S-019: Transaction Drop logged at wrong severity
- **Status:** DONE
- **Priority:** P3
- **Files:** `prisma-client/src/transaction.rs:68-79`
- **Description:** `TransactionClient::Drop` uses `eprintln!` instead of structured logging.
- **Fix:** Changed to `tracing::warn!` for proper structured logging integration.

### S-020: No `cargo audit` in CI
- **Status:** DEFERRED (CI not yet set up)
- **Priority:** P2
- **Files:** CI configuration (not yet set up)
- **Description:** No automated dependency vulnerability scanning. Git-pinned prisma-engines dependencies (`rev = "94a226b"`) won't receive security patches automatically.
- **Fix:** Add `cargo audit` to CI pipeline when CI is configured.

---

## Milestone Tasks

### M-001: Complete sample project using SQLite and prisma-rs
- **Status:** TODO
- **Priority:** P0
- **Description:** Build an end-to-end sample project that exercises the full prisma-rs pipeline with SQLite: schema definition, code generation, migration, and query execution. This serves as both a usage example and an integration test that catches serialization mismatches early (e.g., camelCase vs snake_case field naming between generated code and the Prisma engine).
- **Scope:**
  1. Create a sample project with a Prisma schema (User, Post models with relations)
  2. Run `prisma migrate dev` to create SQLite database and apply migrations
  3. Run `prisma generate` to produce Rust client code
  4. Execute CRUD operations: create, findMany, findUnique, update, delete
  5. Execute relation queries: include, select with nested relations
  6. Execute aggregations: count, groupBy
  7. Verify all results match expected values
  8. Add `#[serde(rename_all = "camelCase")]` to generated structs if the Prisma engine expects camelCase field names
- **Known Bug:** The Prisma engine expects camelCase field names but generated Rust structs serialize as snake_case. This test should catch this class of bug at the earliest possible stage.
- **Acceptance Criteria:** `cargo test -p prisma-cli -- --test e2e` passes with a real SQLite database, full pipeline from schema to query results.

### M-002: Migrate away from open-source prisma-engines
- **Status:** IN PROGRESS (Phase 0 done)
- **Priority:** P1
- **Description:** Replace the dependency on the upstream `prisma-engines` repository (schema engine, query compiler, PSL parser) with native Rust implementations in this workspace. Currently prisma-rs depends on prisma-engines binaries/Wasm for schema parsing, query compilation, and migration execution. This creates a hard dependency on the TypeScript ecosystem's release cycle and architecture decisions.
- **Scope:**
  1. **PSL Parser** -- Implement a native `.prisma` schema parser in `prisma-schema/` that produces the same AST as the Wasm PSL parser. The tree-sitter approach (proven in flatbuffers-rs) is a candidate.
  2. **Query Compiler** -- Implement query planning/compilation natively in `prisma-compiler/` instead of calling the Wasm query compiler. This converts Prisma Client operations into SQL.
  3. **Schema Engine** -- Implement migration diffing, SQL generation, and schema introspection natively in `prisma-migrate/` instead of shelling out to the schema engine binary. This is the largest component.
  4. **Remove prisma-engines dependency** -- Delete `prisma-engines/` from the internal repo. Remove all RPC bridges, Wasm imports, and binary downloads.
- **Phases:**
  - Phase 0: **DONE** -- Created `prisma-ir` crate as serialization boundary. Executor now depends on owned IR types instead of 5 prisma-engines crates. Compiler serializes to JSON, IR deserializes into owned types.
  - Phase 1: PSL parser (moderate -- grammar is well-documented, cross-compat tests exist)
  - Phase 2: Query compiler (large -- must handle all query types, relations, aggregations)
  - Phase 3: Schema engine (very large -- migration diffing, SQL dialect generation, introspection for PG/MySQL/SQLite)
- **Acceptance Criteria:** `cargo build --workspace` succeeds with zero references to prisma-engines. All cross-compat tests pass. `prisma migrate dev` and `prisma generate` work end-to-end without any external binaries.

### M-003: `@updatedAt` not auto-filled on update operations
- **Status:** DONE
- **Priority:** P0
- **Description:** When performing `UpdateOne` with raw JSON or partial data, models with `@updatedAt` fields fail with "Null constraint violation". The query compiler/builder should auto-inject the current timestamp for `@updatedAt` fields on any update operation, but it does not.
- **Root Cause:** The upstream query compiler correctly generates `GeneratorCall { name: "now" }` values in the expression tree. However, the executor treated `GeneratorCall` as `Null` instead of resolving it to the current timestamp. Also, `PrismaValue::Placeholder` (serialized as `prisma__type: "param"` by upstream) was not recognized because our IR expected `prisma__type: "placeholder"`. And `PrismaValueType` used `rename_all = "camelCase"` but upstream uses PascalCase (e.g. `DateTime` not `dateTime`).
- **Fix:** (1) Resolve `GeneratorCall { name: "now" }` to `chrono::Utc::now()` in `prisma_value_to_query_value`, `prisma_value_to_ivalue`, and `resolve_prisma_value`. (2) Accept `"param"` as alias for `"placeholder"` in PrismaValue deserialization. (3) Fix `PrismaValueType` serde tag to match upstream (`#[serde(tag = "type", content = "inner")]` without rename).

### M-004: `@default(now())` not applied on create operations
- **Status:** DONE
- **Priority:** P0
- **Description:** When omitting a `DateTime @default(now())` field from `CreateOne` data, the operation fails with "Null constraint violation" instead of applying the default.
- **Root Cause:** Same as M-003. The query compiler correctly injects `GeneratorCall { name: "now" }` bindings and `Placeholder` references. The executor failed to resolve both.
- **Fix:** Same fix as M-003.

### M-005: `Count` operation panics with unknown query tag
- **Status:** DONE
- **Priority:** P1
- **Description:** Using `Operation::Count` causes a panic: `thread panicked at 'Unknown query tag: count'`. The query compiler doesn't recognize the `count` tag.
- **Root Cause:** `Operation::Count` mapped to `"count"` action string, but the query compiler only recognizes `"aggregate"`. The generated codegen already used `Operation::Aggregate` with `_count` selection for count operations, but the `Operation::Count` enum variant itself was incorrectly mapped.
- **Fix:** Changed `Operation::Count` to map to `"aggregate"` action string, matching the query compiler's expectations.

### M-006: Generated code delegate struct issues
- **Status:** DONE
- **Priority:** P1
- **Description:** Delegate structs reference `PrismaClient` instead of `BasePrismaClient` -- the `execute()` method only exists on `BasePrismaClient`. (The `#[serde(rename_all = "camelCase")]` issue was already fixed in an earlier session.)
- **Fix:** Changed delegate struct field type from `&'a PrismaClient` to `&'a BasePrismaClient` in `gen_rust.rs`. The generated code imports `PrismaClient as BasePrismaClient`, and the wrapper `PrismaClient` struct passes `&self.inner` (which is `BasePrismaClient`) to delegates.

---

## Notes

- All task IDs are stable. Original review: T-001 through T-044. Security review: S-001 through S-020. Milestone tasks: M-001 through M-006.
- Tasks within each priority group are roughly ordered by impact.
- Original review: All 44 tasks resolved (DONE or WONTFIX).
- Security review: All 20 findings resolved. 18 DONE, 1 WONTFIX (S-018), 1 DEFERRED (S-020, awaiting CI).
- M-002 Phase 0 complete: `prisma-ir` crate decouples executor from prisma-engines (5 crates removed). All 53 integration tests pass.
- M-003 through M-006 all DONE. Root cause for M-003/M-004: executor did not resolve `GeneratorCall` values or correctly deserialize `param` placeholders.
- HTTP driver adapters (Neon, PlanetScale, D1, Prisma Postgres) are out of scope and should be deleted. Related tasks marked WONTFIX.
- Supported drivers: PostgreSQL, MySQL, SQLite, DuckDB, FlightSQL, ADBC.
