# Quiver: Implementation Phases

**Status: Active Development**
**Repo: prisma-rs/quiver/ (incubating), synced to Shuozeli/quiver**

---

## Phase 0: Project Bootstrap

**Goal:** Set up the repo structure, Cargo workspace, CI, and a minimal
end-to-end proof that the schema language -> codegen -> query pipeline works.

**Status: DONE**

### Completed

- [x] Create `quiver/` directory inside prisma-rs with Cargo workspace
- [x] Scaffold crate stubs: quiver-schema, quiver-codegen, quiver-error, quiver-cli
- [x] Pre-commit hooks (reuse prisma-rs pattern, repo-layout agnostic)
- [x] Write a hardcoded "hello world" schema parse -> print AST roundtrip
- [x] Rust 2024 edition with MSRV 1.85

### Deliverable

`cargo build --workspace` succeeds. All crate stubs compile. **DONE.**

---

## Phase 1: Schema Language Parser

**Goal:** Parse `.quiver` files into a typed AST. No codegen, no drivers.
Just the parser.

**Status: DONE (32 tests)**

### Completed

- [x] Tokenizer: keywords, identifiers, literals (int, float, string, bool)
- [x] Symbols: `{`, `}`, `(`, `)`, `[`, `]`, `,`, `?`, `@`, `@@`
- [x] Comments: `//` line comments
- [x] Source position tracking (line, column) for error messages
- [x] Parse `config { provider "postgresql" ... }`
- [x] Parse `generate { flatbuffers "./generated/fb" ... }`
- [x] Parse `enum Role { User Admin Moderator }`
- [x] All scalar types: Int8-64, UInt8-64, Float16/32/64, Decimal128/256, Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary, Boolean, Date32/64, Time32/64, Timestamp
- [x] Nested types: List<T>, LargeList<T>, Map<K,V>, Struct<{...}>
- [x] Array sugar: `Type[]` as alias for `List<Type>`
- [x] Nullability: `Type?`
- [x] Model definitions with all field attributes (@id, @autoincrement, @unique, @default, @updatedAt, @ignore, @map, @relation with onDelete/onUpdate)
- [x] Model attributes: @@id, @@unique, @@index, @@map
- [x] Enum field references and relation fields
- [x] Validation pass: duplicate names, enum/model resolution, FK targets, relation consistency
- [x] Arrow type mapping: TypeExpr -> arrow_schema::DataType
- [x] ReferentialAction enum (Cascade, Restrict, SetNull, SetDefault, NoAction)

### Deliverable

`quiver parse schema.quiver` prints the parsed AST. All reference schemas parse. **DONE.**

---

## Phase 2: FlatBuffers Codegen

**Goal:** Generate `.fbs` files from the parsed AST and compile them to
Rust types using `flatbuffers-rs`.

**Status: DONE**

### Completed

- [x] `FbsGenerator::generate(schema, namespace)` -> `.fbs` string
- [x] Enum, model table, scalar type mapping, nullable, list, struct, map
- [x] Input types: CreateInput, UpdateInput, WhereUniqueInput
- [x] `RustFbsGenerator` -- pipeline: .fbs -> flatc-rs parse -> codegen -> Rust source
- [x] `quiver generate --target flatbuffers` and `--target rust-fbs`

---

## Phase 3: Protobuf Codegen

**Goal:** Generate `.proto` files from the parsed AST and compile them to
Rust types using `protobuf-rs`.

**Status: DONE**

### Completed

- [x] `ProtoGenerator::generate(schema, package)` -> `.proto` string
- [x] Proto3 enums with UNSPECIFIED sentinel, prefixed variant names
- [x] Message generation with optional fields, repeated, maps
- [x] Input types: CreateInput, UpdateInput
- [x] `RustProtoGenerator` -- pipeline: .proto -> protoc-rs parse -> codegen -> Rust modules
- [x] `quiver generate --target protobuf` and `--target rust-proto`

---

## Phase 4: Rust Serde Codegen

**Goal:** Generate plain Rust structs with serde derives.

**Status: DONE**

### Completed

- [x] `RustSerdeGenerator::generate(schema)` -> Rust source with `#[derive(Serialize, Deserialize)]`
- [x] Arrow type -> Rust type mapping
- [x] Enum and input type generation
- [x] `RustClientGenerator` -- generates typed client delegates per model
- [x] `quiver generate --target rust-serde`

---

## Phase 5: Driver Core + Database Drivers

**Goal:** Define the driver trait and implement database drivers.

**Status: DONE (23 tests)**

### Completed

- [x] `Driver` trait with `connect(url) -> Connection`
- [x] `Connection` trait with `execute`, `query`, `execute_ddl`
- [x] `Transactional` trait with `begin() -> Transaction`
- [x] `Transaction` trait with `commit`, `rollback`, auto-rollback on drop
- [x] `Value` enum (Null, Bool, Int, UInt, Float, Text, Blob)
- [x] `Row`, `Column`, `Statement`, `DdlStatement` types
- [x] Arrow RecordBatch conversion (Value -> Arrow arrays)
- [x] **SQLite driver** (quiver-driver-sqlite): rusqlite + re-exports adbc-sqlite (12 tests)
- [x] **PostgreSQL driver** (quiver-driver-postgres): postgres crate + re-exports adbc-postgres (3 tests)
- [x] **MySQL driver** (quiver-driver-mysql): mysql crate + re-exports adbc-mysql
- [x] PostgreSQL placeholder rewriting (`?` -> `$1, $2, ...`)
- [x] UnsafeCell interior mutability for `&self` trait methods (safe: types are !Sync)
- [x] UInt overflow protection for PostgreSQL (values > i64::MAX sent as NUMERIC text)
- [x] ADBC re-exported from each driver crate for Arrow-native access

---

## Phase 6: Query Builder

**Goal:** Type-safe SQL query builder with SQL injection prevention.

**Status: DONE (113 unit + 42 integration = 155 tests)**

### Completed

- [x] `SafeIdent` -- compile-time validated identifiers (`&'static str` only)
- [x] `Query::table("name")` entry point -> fluent builder API
- [x] `FindManyBuilder` -- SELECT with filters, ordering, limit, offset, DISTINCT
- [x] `FindFirstBuilder` -- SELECT ... LIMIT 1
- [x] `CreateBuilder` -- INSERT single row
- [x] `CreateManyBuilder` -- INSERT multiple rows (batch)
- [x] `UpdateBuilder` -- UPDATE with filters
- [x] `DeleteBuilder` -- DELETE with filters
- [x] `UpsertBuilder` -- INSERT ... ON CONFLICT DO UPDATE
- [x] `AggregateBuilder` -- COUNT, SUM, AVG, MIN, MAX, STDDEV_POP/SAMP
- [x] `Filter` -- Eq, Neq, Gt, Gte, Lt, Lte, In, NotIn, Like, IsNull, IsNotNull, Between, And, Or, Not, Raw
- [x] `Order` -- ASC/DESC/NULLS FIRST/NULLS LAST
- [x] `Join` -- INNER, LEFT, RIGHT, FULL with ON conditions
- [x] `Include` + `RelationDef` -- eager-load 1:1 and 1:N relations via separate queries
- [x] `ChildWrite` + `create_with_children` -- nested inserts in transactions
- [x] `PageRequest`/`PageResponse` -- AIP-132 cursor-based pagination with Base64 tokens
- [x] `SchemaValidator` -- validate queries against schema at build time
- [x] `Expr` -- SQL expressions: Column, StringFn, MathFn, DateFn, Subquery
- [x] `WindowFn` -- ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, SUM, COUNT, AVG, MIN, MAX
- [x] `WindowSpec` -- PARTITION BY + ORDER BY with builder pattern
- [x] `Cte` -- Common Table Expressions (WITH clauses)
- [x] `RawQueryBuilder` -- raw SQL with `&'static str` + bind params
- [x] `Filter::raw()` accepts `&'static str` only (injection prevention)
- [x] 42 SQLite integration tests covering full CRUD + pagination + aggregation + joins

---

## Phase 7: TypeScript Codegen

**Goal:** Generate TypeScript client types from `.quiver` schema.

**Status: DONE (9 tests)**

### Completed

- [x] `TypeScriptGenerator::generate(schema)` -> TypeScript source
- [x] Model interfaces (column fields only, no relations)
- [x] CreateInput (excludes @id @autoincrement fields, optional for nullable/default)
- [x] UpdateInput (all non-auto fields optional)
- [x] WhereUniqueInput (@id and @unique fields only)
- [x] Field constants object (`as const`)
- [x] Enum generation (string enums)
- [x] Type mapping: Int8-32 -> number, Int64/UInt64 -> bigint, Decimal -> string, Binary -> Uint8Array, temporal -> string, List -> T[], Map -> Map<K,V>, Struct -> inline object
- [x] `quiver generate --target typescript`

---

## Phase 8: SQL DDL Codegen

**Goal:** Generate database-specific DDL from `.quiver` schema.

**Status: DONE (12 tests in codegen + 35 unit in migrate)**

### Completed

- [x] `SqlGenerator::generate(schema, dialect)` -> SQL DDL string
- [x] `SqlDialect` enum: Sqlite, Postgres, Mysql
- [x] Dialect-specific type mapping (Arrow -> SQL for each database)
- [x] Foreign key constraints with ON DELETE/ON UPDATE referential actions
- [x] Boolean defaults (TRUE/FALSE for PG, 1/0 for SQLite)
- [x] `quiver generate --target sql-sqlite`, `sql-postgres`, `sql-mysql`

---

## Phase 9: Schema Migration Engine

**Goal:** Diff two schema versions and generate SQL DDL migrations.

**Status: DONE (35 unit + 24 integration = 59 tests)**

### Completed

- [x] `diff_schemas(old, new)` -> `Vec<MigrationStep>`
- [x] MigrationStep: CreateModel, DropModel, AddField, DropField, AlterField, CreateIndex, DropIndex, CreateEnum, DropEnum, AddEnumValue, RemoveEnumValue
- [x] `MigrationSqlGenerator` -- converts steps to DDL per dialect
- [x] `TrustedSqlBuilder` -- security-first DDL generation (`&'static str` fragments only)
- [x] `MigrationTracker` -- tracks applied migrations in database (_quiver_migrations table)
- [x] SQLite table rebuild (for ALTER COLUMN on SQLite)
- [x] PostgreSQL CREATE TYPE for enums, ALTER COLUMN support
- [x] MySQL ENUM inline types, AUTO_INCREMENT, ALTER COLUMN
- [x] FK referential actions (CASCADE, RESTRICT, SET NULL, SET DEFAULT, NO ACTION)
- [x] `introspect(conn, dialect)` -> Schema (PG + MySQL via information_schema)
- [x] `schema_to_quiver(schema)` -> `.quiver` source text
- [x] Schema snapshots saved with each migration for diffing

### CLI Commands

- [x] `quiver migrate create <schema> <name>` -- diff + create migration files
- [x] `quiver migrate apply <schema>` -- apply pending migrations
- [x] `quiver migrate status <schema>` -- show applied/pending
- [x] `quiver migrate rollback <schema>` -- rollback last migration
- [x] `quiver db push <schema>` -- push schema directly (no migration files)
- [x] `quiver db pull <schema> -o output.quiver` -- introspect database
- [x] `quiver db execute <schema> <sql>` -- run raw SQL

---

## Phase 10: End-to-End Integration

**Goal:** Full pipeline tests: schema -> codegen -> driver -> query -> result.

**Status: DONE (24 tests)**

### Completed

- [x] `quiver-e2e` crate with SQLite integration tests
- [x] Parse schema -> generate DDL -> create tables -> CRUD -> verify results
- [x] All codegen targets produce valid output for reference schemas

---

## Test Summary

| Crate | Unit Tests | Integration Tests | Total |
|-------|-----------|-------------------|-------|
| quiver-schema | 32 | -- | 32 |
| quiver-codegen | 63 | -- | 63 |
| quiver-driver-core | 8 | -- | 8 |
| quiver-driver-sqlite | 12 | -- | 12 |
| quiver-driver-postgres | 3 | -- | 3 |
| quiver-e2e | 24 | -- | 24 |
| quiver-query | 113 | 42 | 155 |
| quiver-migrate | 35 | 24 | 59 |
| **Total** | **290** | **66** | **356** |

---

## Dependency Graph

```
Phase 0 (bootstrap)
  |
  v
Phase 1 (parser)
  |
  +--------+--------+--------+
  |        |        |        |
  v        v        v        v
Phase 2  Phase 3  Phase 4  Phase 7
(FBS)    (Proto)  (Serde)  (TypeScript)
  |        |        |
  +--------+--------+
           |
           v
       Phase 5 (drivers)
           |
           v
       Phase 6 (query builder)
           |
  +--------+--------+
  |        |        |
  v        v        v
Phase 8  Phase 9  Phase 10
(SQL DDL)(migrate)(e2e tests)
```

---

## What's Next

Future phases (not yet started):

- **Client runtime** -- Generated delegates that combine codegen + query builder + driver
- **Arrow-native result path** -- RecordBatch passthrough from ADBC without row-level conversion
- **FlightSQL driver** -- Quiver traits on top of adbc-flightsql for distributed queries
- **Connection pooling** -- Pool management for multi-threaded applications
- **Async API** -- Async versions of Connection/Transaction traits
