# Phase 1: Database Driver Layer

Last updated: 2026-03-07

## Goal

Replace the TypeScript driver adapters (`adapter-pg`, `adapter-better-sqlite3`,
`adapter-mariadb`) with native Rust database drivers that produce identical
`SqlResultSet` output for the same queries.

Initial scope: **PostgreSQL, MySQL/MariaDB, SQLite**. SQL Server is deferred.

## Architecture Overview

```
                        SqlDriverAdapter (trait)
                        /        |        \
               PgDriver   MySqlDriver  SqliteDriver
                  |           |            |
            tokio-postgres  mysql_async  rusqlite
```

Each driver implements a common trait hierarchy mirroring the TypeScript
interfaces. Error mapping translates database-specific errors into Prisma's
`MappedError` taxonomy (P2xxx codes).

---

## 1.1: Core Types & Traits

**Status:** Not started

Port the foundational types from `driver-adapter-utils/src/types.ts`.

### 1.1.1: SqlQuery and ArgType

```rust
struct SqlQuery {
    sql: String,
    args: Vec<QueryValue>,
    arg_types: Vec<ArgType>,
}

struct ArgType {
    scalar_type: ArgScalarType,
    db_type: Option<String>,
    arity: Arity,
}

enum ArgScalarType {
    String, Int, BigInt, Float, Decimal,
    Boolean, Enum, Uuid, Json, DateTime, Bytes, Unknown,
}

enum Arity { Scalar, List }
```

### 1.1.2: SqlResultSet and ColumnType

```rust
struct SqlResultSet {
    column_names: Vec<String>,
    column_types: Vec<ColumnType>,
    rows: Vec<Vec<ResultValue>>,
    last_insert_id: Option<String>,
}

#[repr(u16)]
enum ColumnType {
    // Scalars (0-15)
    Int32 = 0, Int64 = 1, Float = 2, Double = 3, Numeric = 4,
    Boolean = 5, Character = 6, Text = 7, Date = 8, Time = 9,
    DateTime = 10, Json = 11, Enum = 12, Bytes = 13, Set = 14, Uuid = 15,
    // Arrays (64-78)
    Int32Array = 64, Int64Array = 65, FloatArray = 66, DoubleArray = 67,
    NumericArray = 68, BooleanArray = 69, CharacterArray = 70, TextArray = 71,
    DateArray = 72, TimeArray = 73, DateTimeArray = 74, JsonArray = 75,
    EnumArray = 76, BytesArray = 77, UuidArray = 78,
    // Special
    UnknownNumber = 128,
}

enum ResultValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Numeric(String),
    Boolean(bool),
    Text(String),
    Bytes(Vec<u8>),
    // ... complete set matching TS `ResultValue`
}
```

### 1.1.3: ConnectionInfo and Provider

```rust
struct ConnectionInfo {
    schema_name: Option<String>,
    max_bind_values: Option<u32>,
    supports_relation_joins: bool,
}

enum Provider { Postgres, Mysql, Sqlite }
```

### 1.1.4: IsolationLevel

```rust
enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}
```

**Exit criteria:** All core types compile. Serialization round-trip tests pass
(Rust types -> JSON -> match TS JSON output).

---

## 1.2: Trait Definitions

**Status:** Not started

Port the interface hierarchy from `driver-adapter-utils`.

### 1.2.1: SqlQueryable trait

```rust
#[async_trait]
trait SqlQueryable: Send + Sync {
    fn provider(&self) -> Provider;
    fn adapter_name(&self) -> &str;
    async fn query_raw(&self, query: SqlQuery) -> Result<SqlResultSet, DriverError>;
    async fn execute_raw(&self, query: SqlQuery) -> Result<u64, DriverError>;
}
```

### 1.2.2: Transaction trait

```rust
#[async_trait]
trait Transaction: SqlQueryable {
    fn options(&self) -> &TransactionOptions;
    async fn commit(self: Box<Self>) -> Result<(), DriverError>;
    async fn rollback(self: Box<Self>) -> Result<(), DriverError>;
    async fn create_savepoint(&self, name: &str) -> Result<(), DriverError>;
    async fn rollback_to_savepoint(&self, name: &str) -> Result<(), DriverError>;
    async fn release_savepoint(&self, name: &str) -> Result<(), DriverError>;
}

struct TransactionOptions {
    use_phantom_query: bool,
}
```

### 1.2.3: SqlDriverAdapter trait

```rust
#[async_trait]
trait SqlDriverAdapter: SqlQueryable {
    async fn execute_script(&self, script: &str) -> Result<(), DriverError>;
    async fn start_transaction(
        &self,
        isolation_level: Option<IsolationLevel>,
    ) -> Result<Box<dyn Transaction>, DriverError>;
    fn connection_info(&self) -> ConnectionInfo;
    async fn dispose(&self) -> Result<(), DriverError>;
}
```

### 1.2.4: SqlDriverAdapterFactory trait

```rust
#[async_trait]
trait SqlDriverAdapterFactory: Send + Sync {
    fn provider(&self) -> Provider;
    fn adapter_name(&self) -> &str;
    async fn connect(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError>;
}

#[async_trait]
trait SqlMigrationAwareDriverAdapterFactory: SqlDriverAdapterFactory {
    async fn connect_to_shadow_db(&self) -> Result<Box<dyn SqlDriverAdapter>, DriverError>;
}
```

**Exit criteria:** Trait definitions compile. A mock implementation compiles
against all traits.

---

## 1.3: Error Types

**Status:** Not started

Port the error mapping system from `driver-adapter-utils/src/types.ts` and each
adapter's `errors.ts`.

### 1.3.1: MappedError enum

All 27+ error variants from the TypeScript `MappedError` discriminated union:

```rust
enum MappedError {
    GenericJs { id: u32 },
    UnsupportedNativeDataType { r#type: String },
    InvalidIsolationLevel { level: String },
    LengthMismatch { column: Option<String> },
    UniqueConstraintViolation { constraint: Option<ConstraintTarget> },
    NullConstraintViolation { constraint: Option<ConstraintTarget> },
    ForeignKeyConstraintViolation { constraint: Option<ConstraintTarget> },
    DatabaseNotReachable { host: Option<String>, port: Option<u16> },
    DatabaseDoesNotExist { db: Option<String> },
    DatabaseAlreadyExists { db: Option<String> },
    DatabaseAccessDenied { db: Option<String> },
    ConnectionClosed,
    TlsConnectionError { reason: String },
    AuthenticationFailed { user: Option<String> },
    TransactionWriteConflict,
    TableDoesNotExist { table: Option<String> },
    ColumnNotFound { column: Option<String> },
    TooManyConnections { cause: String },
    ValueOutOfRange { cause: String },
    InvalidInputValue { message: String },
    MissingFullTextSearchIndex,
    SocketTimeout,
    InconsistentColumnData { cause: String },
    TransactionAlreadyClosed { cause: String },
    // Raw database-specific errors
    Postgres { code: String, severity: String, message: String,
               detail: Option<String>, column: Option<String>, hint: Option<String> },
    Mysql { code: u32, message: String, state: String, cause: Option<String> },
    Sqlite { extended_code: i32, message: String },
}

enum ConstraintTarget {
    Fields(Vec<String>),
    Index(String),
    ForeignKey(String),
}
```

### 1.3.2: DriverError wrapper

```rust
struct DriverError {
    mapped: MappedError,
    original_code: Option<String>,
    original_message: Option<String>,
}
```

### 1.3.3: User-facing error mapping

Port `rethrowAsUserFacing()` from `client-engine-runtime/src/user-facing-error.ts`:
- Map `MappedError` variants to Prisma error codes (P2000-P2034)
- Render human-readable error messages
- Separate path for raw queries (always P2010)

**Exit criteria:** All error variants constructible. Round-trip serialization
matches TS JSON format. Unit tests for each `MappedError` -> P2xxx mapping.

---

## 1.4: PostgreSQL Driver

**Status:** Not started

Port `adapter-pg`. Recommended crate: `tokio-postgres` + `deadpool-postgres`.

### 1.4.1: Type mapping (OID -> ColumnType)

Port `conversion.ts:fieldToColumnType()`. Map PostgreSQL type OIDs:

| OID Range | ColumnType |
|-----------|-----------|
| INT2(21), INT4(23) | Int32 |
| INT8(20) | Int64 |
| FLOAT4(700) | Float |
| FLOAT8(701) | Double |
| NUMERIC(1700), MONEY(790) | Numeric |
| BOOL(16) | Boolean |
| CHAR(18), BPCHAR(1042), VARCHAR(1043), TEXT(25), XML(142), NAME(19) | Text |
| DATE(1082) | Date |
| TIME(1083), TIMETZ(1266) | Time |
| TIMESTAMP(1114), TIMESTAMPTZ(1184) | DateTime |
| JSON(114), JSONB(3802) | Json |
| UUID(2950) | Uuid |
| BYTEA(17) | Bytes |
| OID >= 16384 (custom types) | Text |
| Array variants (1000-2951) | corresponding array ColumnType |

### 1.4.2: Argument mapping

Port `conversion.ts:mapArg()`:
- DateTime -> `YYYY-MM-DD HH:mm:ss.fff` format
- Bytes -> raw `Vec<u8>` (no base64 in Rust)
- Arrays -> recursive mapping
- BigInt -> i64

### 1.4.3: PgQueryable implementation

- Execute queries via `tokio-postgres::Client::query_raw()`
- Custom type parsers for NUMERIC, TIMESTAMP, MONEY, JSON, BYTEA, BIT
- Array row mode (return `Vec<Vec<ResultValue>>`)

### 1.4.4: PgTransaction

- `SAVEPOINT {name}` / `ROLLBACK TO SAVEPOINT {name}` / `RELEASE SAVEPOINT {name}`
- Commit and rollback release the connection back to pool

### 1.4.5: PgDriverAdapter

- Connection pooling via `deadpool-postgres`
- `start_transaction()`: acquire connection, `BEGIN`, optional isolation level
- `execute_script()`: split by `;`, execute each statement
- `connection_info()`: schema name, `supports_relation_joins: true`

### 1.4.6: PgDriverAdapterFactory

- Accept connection string or pool config
- `connect()`: create pool, return adapter
- `connect_to_shadow_db()`: create temporary DB, return adapter with cleanup

### 1.4.7: Error mapping

Port `errors.ts:convertDriverError()`:

| PG Code | MappedError |
|---------|-------------|
| 22001 | LengthMismatch |
| 22003 | ValueOutOfRange |
| 22P02 | InvalidInputValue |
| 23505 | UniqueConstraintViolation (parse detail for fields) |
| 23502 | NullConstraintViolation (parse detail for fields) |
| 23503 | ForeignKeyConstraintViolation |
| 3D000 | DatabaseDoesNotExist |
| 28000 | DatabaseAccessDenied |
| 28P01 | AuthenticationFailed |
| 40001 | TransactionWriteConflict |
| 42P01 | TableDoesNotExist |
| 42703 | ColumnNotFound |
| 42P04 | DatabaseAlreadyExists |
| 53300 | TooManyConnections |
| ENOTFOUND/ECONNREFUSED | DatabaseNotReachable |
| ECONNRESET | ConnectionClosed |
| ETIMEDOUT | SocketTimeout |

**Exit criteria:** Cross-compat tests pass: same schema, same queries, identical
`SqlResultSet` output as `adapter-pg` across all PostgreSQL types.

---

## 1.5: MySQL/MariaDB Driver

**Status:** Not started

Port `adapter-mariadb`. Recommended crate: `sqlx::mysql` or `mysql_async`.

### 1.5.1: Type mapping

Port `conversion.ts:mapColumnType()`:

| MariaDB Type | ColumnType |
|-------------|-----------|
| TINY, SHORT, INT24, YEAR | Int32 |
| INT (unsigned) | Int64 |
| INT (signed) | Int32 |
| LONG, BIGINT | Int64 |
| FLOAT | Float |
| DOUBLE | Double |
| TIMESTAMP, DATETIME | DateTime |
| DATE | Date |
| TIME | Time |
| DECIMAL, NEWDECIMAL | Numeric |
| VARCHAR, STRING, BLOB (json format) | Json |
| VARCHAR, STRING, BLOB (binary collation) | Bytes |
| VARCHAR, STRING, BLOB (other) | Text |
| ENUM | Enum |
| BIT, GEOMETRY | Bytes |
| NULL | Int32 |

### 1.5.2: Argument mapping

Port `conversion.ts:mapArg()`:
- BigInt -> i64 / `BigInt`
- DateTime -> `YYYY-MM-DD HH:mm:ss.fff` or time/date variants
- Bytes -> raw `Vec<u8>`

### 1.5.3: MySqlQueryable implementation

- Query with `rows_as_array`, `date_strings`, `auto_json_map: false` equivalents
- Type casting for GEOMETRY types

### 1.5.4: MySqlTransaction

- `SAVEPOINT {name}` / `ROLLBACK TO {name}` / `RELEASE SAVEPOINT {name}`
- Commit/rollback close the connection

### 1.5.5: MySqlDriverAdapter

- Connection pooling
- `start_transaction()`: acquire connection, optional isolation level, `BEGIN`
- Version detection: MariaDB vs MySQL >= 8.0.13 for `supports_relation_joins`
- `connection_info()`: schema name from config

### 1.5.6: MySqlDriverAdapterFactory

- Accept connection string (handle `mysql://` -> driver connection)
- `connect()`: create pool, detect capabilities via `SELECT VERSION()`

### 1.5.7: Error mapping

Port `errors.ts:convertDriverError()`:

| MySQL Code | MappedError |
|-----------|-------------|
| 1062 | UniqueConstraintViolation |
| 1451, 1452 | ForeignKeyConstraintViolation |
| 1263, 1364, 1048 | NullConstraintViolation |
| 1264 | ValueOutOfRange |
| 1049 | DatabaseDoesNotExist |
| 1007 | DatabaseAlreadyExists |
| 1044 | DatabaseAccessDenied |
| 1045 | AuthenticationFailed |
| 1146 | TableDoesNotExist |
| 1054 | ColumnNotFound |
| 1406 | LengthMismatch |
| 1191 | MissingFullTextSearchIndex |
| 1213 | TransactionWriteConflict |
| 1040, 1203 | TooManyConnections |

**Exit criteria:** Cross-compat tests pass against both MySQL 8+ and MariaDB.

---

## 1.6: SQLite Driver

**Status:** Not started

Port `adapter-better-sqlite3`. Recommended crate: `rusqlite` (with `bundled`
feature) + `tokio::task::spawn_blocking` for async.

### 1.6.1: Type mapping

Port `conversion.ts:mapDeclType()` and `getColumnTypes()`:
- Map SQLite declared type strings to ColumnType
- Infer column types from actual data values when declared type is unknown
- NULL columns default to Int32

### 1.6.2: Argument mapping

Port `conversion.ts:mapArg()`:
- Int/Float/Decimal string -> parsed number
- BigInt -> i64
- Boolean -> 1/0
- DateTime -> ISO string or unixepoch-ms (configurable)
- Bytes -> raw `Vec<u8>`

### 1.6.3: SqliteQueryable implementation

- `prepare()`, `bind()`, raw array results
- Distinguish reader vs writer statements (`stmt.readonly()`)
- Return `changes` count for mutations

### 1.6.4: SqliteTransaction

- `SAVEPOINT {name}` / `ROLLBACK TO {name}` / `RELEASE SAVEPOINT {name}`
- Mutex serialization (SQLite is single-writer)
- Only supports SERIALIZABLE isolation level

### 1.6.5: SqliteDriverAdapter

- `start_transaction()`: acquire mutex, `BEGIN`
- `execute_script()`: `conn.execute_batch()`
- Single connection (no pool needed for SQLite)

### 1.6.6: SqliteDriverAdapterFactory

- Accept file path or `:memory:`
- `connect()`: open database, `PRAGMA journal_mode=WAL`
- `connect_to_shadow_db()`: use shadow URL or `:memory:`
- Options: `timestamp_format` (iso8601 or unixepoch-ms)

### 1.6.7: Error mapping

Port `errors.ts:convertDriverError()`:

| SQLite Code | MappedError |
|------------|-------------|
| SQLITE_BUSY | SocketTimeout |
| SQLITE_CONSTRAINT_UNIQUE, SQLITE_CONSTRAINT_PRIMARYKEY | UniqueConstraintViolation |
| SQLITE_CONSTRAINT_NOTNULL | NullConstraintViolation |
| SQLITE_CONSTRAINT_FOREIGNKEY, SQLITE_CONSTRAINT_TRIGGER | ForeignKeyConstraintViolation |
| message "no such table" | TableDoesNotExist |
| message "no such column" / "has no column named" | ColumnNotFound |

**Exit criteria:** Cross-compat tests pass. Identical results for both
`:memory:` and file-based databases.

---

## 1.7: Cross-Compat Test Suite

**Status:** Not started

Verification that Rust drivers produce identical results to TypeScript adapters.

### 1.7.1: Test infrastructure

- Docker Compose with PostgreSQL, MySQL, SQLite
- Shared test schemas (subset of `prisma/packages/client/tests/functional`)
- Test runner executes same query through both TS adapter and Rust driver
- Diff `SqlResultSet` output field-by-field

### 1.7.2: Type round-trip tests

For each database, verify every column type:
- Insert typed value via Rust driver
- Read via TS adapter (and vice versa)
- Assert identical `ColumnType` and value representation

### 1.7.3: Transaction tests

- Begin / commit / rollback
- Nested transactions (savepoints)
- Isolation levels
- Timeout behavior
- Concurrent transactions

### 1.7.4: Error mapping tests

For each `MappedError` variant:
- Trigger the corresponding database error
- Assert Rust driver produces the same `MappedError` as TS adapter
- Assert same P2xxx code from user-facing error mapping

### 1.7.5: Edge case tests

- NULL handling across all types
- Empty result sets
- Large result sets (pagination behavior)
- Unicode in column names and values
- Binary data round-trip
- Maximum bind parameter counts per provider

**Exit criteria:** Full green cross-compat suite across all three databases.

---

## Execution Order

```
1.1 (core types) --> 1.2 (traits) --> 1.3 (errors)
                                        |
                        +---------------+---------------+
                        |               |               |
                       1.4 (pg)       1.5 (mysql)    1.6 (sqlite)
                        |               |               |
                        +---------------+---------------+
                                        |
                                       1.7 (cross-compat)
```

Tasks 1.1-1.3 are sequential (each depends on the previous). Tasks 1.4-1.6 are
independent and can be developed in parallel. Task 1.7 runs against all drivers.

## Crate Dependencies (Recommended)

| Crate | Purpose |
|-------|---------|
| `tokio-postgres` | PostgreSQL async client |
| `deadpool-postgres` | PostgreSQL connection pool |
| `mysql_async` | MySQL/MariaDB async client |
| `rusqlite` (bundled) | SQLite (with `spawn_blocking`) |
| `async-trait` | Async trait support |
| `thiserror` | Error type derivation |
| `chrono` | Date/time handling |
| `uuid` | UUID type |
| `rust_decimal` | Decimal/Numeric type |
| `base64` | Binary data encoding |

---

## Long-Term: Future Driver Backends

These are not in scope for the initial migration but worth exploring once the
core trait layer is stable.

| Backend | Crate | Notes |
|---------|-------|-------|
| SQL Server | `tiberius` | Deferred from initial scope. Port `adapter-mssql` when needed. |
| ADBC (Arrow Database Connectivity) | `adbc_core` | Columnar result sets via Arrow. Could enable zero-copy analytics queries and interop with DataFusion, Polars, DuckDB. Requires a `SqlResultSet`-from-`RecordBatch` adapter layer. |
| Arrow Flight SQL | `arrow-flight` | gRPC-based protocol for distributed query execution. Enables Prisma to talk to Flight SQL servers (Dremio, DuckDB remote, etc.). Interesting for federated query scenarios. |

The trait abstraction (`SqlDriverAdapter`) is designed to accommodate these --
each backend would be a new crate implementing the same trait hierarchy.
