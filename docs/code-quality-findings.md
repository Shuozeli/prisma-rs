# Code Quality Findings

Generated: 2026-03-26

## 1. Silent Failures / Unstructured Logging (High Severity)

### 1.1 eprintln! used instead of structured tracing in production code
All three driver Transaction::Drop implementations use `eprintln!` instead of `tracing::warn!`.
This bypasses log filtering, structured logging, and makes warnings invisible to log aggregation.

- **driver-pg/src/adapter.rs:244** -- PgTransaction::drop
- **driver-mysql/src/adapter.rs:196** -- MySqlTransaction::drop
- **driver-sqlite/src/adapter.rs:287** -- SqliteTransaction::drop

**Fix:** Replace `eprintln!` with `tracing::warn!` using structured fields.
**Status:** DONE

### 1.2 eprintln! for array overflow warning in PG conversion
- **driver-pg/src/conversion.rs:120-124** -- Array parameter exceeding MAX_ARRAY_PARAMS silently truncated with only an `eprintln!` warning
- **Severity:** High -- silent data loss

**Fix:** Removed the silent truncation. Array elements are now passed without a size limit (the database's own bind-parameter limit applies).
**Status:** DONE

### 1.3 eprintln! for INT4/INT2 truncation in PG typed params
- **driver-pg/src/conversion.rs:171** -- i64 truncated to i32 with eprintln warning
- **driver-pg/src/conversion.rs:179** -- i64 truncated to i16 with eprintln warning
- **Severity:** High -- silent data corruption (clamping to MAX/MIN)

**Fix:** Replaced `eprintln!` with `tracing::warn!` using structured fields. Clamping behavior preserved since it matches PostgreSQL's runtime behavior for type mismatches.
**Status:** DONE

### 1.4 eprintln! for MySQL out-of-bounds column access
- **driver-mysql/src/conversion.rs:90-91** -- Column index out of bounds returns NULL with eprintln
- **Severity:** High -- programming error silently ignored

**Fix:** Replaced eprintln + NULL return with a panic including diagnostic info (row length). This is a programming error, not a runtime condition.
**Status:** DONE

## 2. Dead Code / Suppressed Warnings (Low Severity)

### 2.1 #[allow(dead_code)] on IntermediateValue::with_last_insert_id
- **query-executor/src/value.rs:25-26** -- `with_last_insert_id` is marked allow(dead_code)
- **Status:** DONE -- removed #[allow(dead_code)] and verified it is used in tests

## 3. Duplicated ArgScalarType Enums (Low Severity)

### 3.1 ArgScalarType defined in both driver-core and prisma-ir
- **driver-core/src/types.rs:104-119** -- `ArgScalarType` enum
- **prisma-ir/src/query.rs:99-116** -- `ArgScalarType` enum (identical variants)
- **query-executor/src/render.rs:214-229** -- Manual identity mapping between them

**Status:** SKIPPED -- These enums serve different serialization boundaries (driver-core uses serde lowercase, prisma-ir uses camelCase). Unifying would couple the IR layer to the driver layer. The boilerplate is isolated.

## 4. Shadow Database Returning Primary Connection (Medium Severity)

### 4.1 PgDriverAdapterFactory::connect_to_shadow_db returns primary connection
- **driver-pg/src/adapter.rs:367-372** -- `connect_to_shadow_db()` silently returns a connection to the primary DB
- **Severity:** Medium -- running migration operations against the primary DB when shadow DB is requested

**Fix:** Return an error explaining shadow DB is not yet supported, preventing accidental primary DB operations.
**Status:** DONE

## 5. Pool Error Classification is Stringly-Typed (Low Severity)

### 5.1 pool_error_to_mapped uses string matching for error classification
- **driver-pg/src/adapter.rs:454-467** -- `pool_error_to_mapped` classifies pool errors by checking if the message string contains keywords like "timed out", "authentication", "refused"
- **Severity:** Low -- fragile, locale-dependent, may misclassify

**Status:** SKIPPED -- deadpool-postgres doesn't expose structured error types for pool-level failures. String matching is the pragmatic approach here.

## 6. Pre-existing Test Failures (Medium Severity)

### 6.1 DatabaseUrl tests assume incorrect `url` crate behavior
- **driver-core/src/database_url.rs:161-166** -- `parse_mysql_url` test used unencoded `@` in password, which the `url` crate interprets as host separator
- **driver-core/src/database_url.rs:221-226** -- `empty_password` test expected `Some("")` but `url` crate treats `user:@host` as no password (returns `None`)
- **Severity:** Medium -- tests were failing before any code changes

**Fix:** Fixed test expectations to match actual `url` crate behavior.
**Status:** DONE
