# Quiver: An Arrow-Native ORM

## What Is Quiver

Quiver is a Rust ORM built on the Apache Arrow ecosystem:

- **Arrow type system** as the schema language -- no lossy type mappings
- **ADBC** for database connectivity -- one interface, any database
- **FlightSQL** for distributed/remote queries -- Arrow over gRPC
- **FlatBuffers** as the native wire format -- Arrow already is FlatBuffers
- **Protobuf** as an interchange option -- for systems that speak proto

Quiver replaces the traditional ORM stack (custom types + database drivers +
JSON serialization) with Arrow-native primitives that the entire data ecosystem
already speaks.

## Why

Traditional ORMs have an impedance mismatch problem:

```
Application types  <->  ORM types  <->  SQL types  <->  Wire format  <->  Database types
     String              String          VARCHAR         JSON/text           TEXT
     i32                 Int             INTEGER         JSON number         INT4
     f64                 Float           DOUBLE          JSON number         FLOAT8
     DateTime            DateTime        TIMESTAMP       ISO 8601 string     TIMESTAMPTZ
     ???                 ???             NUMERIC(10,2)   string              NUMERIC
     ???                 ???             INT2            JSON number         SMALLINT
```

Every `<->` is a lossy conversion. An `Int` could be 16-bit or 64-bit. A
`DateTime` loses timezone and precision info. `Decimal` becomes a string
somewhere in the pipeline. Nested types (arrays, maps, structs) are
shoehorned into JSON.

Arrow eliminates this:

```
Quiver schema types  ==  Arrow types  ==  ADBC wire types  ==  Database types
     Int16                 Int16             Int16                SMALLINT
     Int32                 Int32             Int32                INTEGER
     Decimal128(10,2)      Decimal128(10,2)  Decimal128(10,2)     NUMERIC(10,2)
     Timestamp(us, UTC)    Timestamp(us,UTC) Timestamp(us,UTC)    TIMESTAMPTZ
     List<Utf8>            List<Utf8>        List<Utf8>           TEXT[]
     Map<Utf8,Int32>       Map<Utf8,Int32>   Map<Utf8,Int32>      JSONB / MAP
```

Zero lossy conversions. The type is the same from schema to wire to database.

## Architecture

```
                      quiver-schema
                     (parser + AST)
                          |
            +-------------+--------------------+
            |             |                    |
      quiver-codegen  quiver-migrate     quiver-query
       |  |  |  |     (DDL + diff +      (query builder)
       v  v  v  v      introspect)            |
    .fbs .proto .rs .ts   |                   |
     |    |               v                   v
     v    v          SQL DDL / info_schema   SQL + params
  flatbuffers-rs          |                   |
  protobuf-rs             |                   |
     |    |               |                   |
     v    v               +---+---+-----------+
  Rust types                  |   |
                              v   v
                        quiver-driver-core
                        (Driver, Connection, Transaction traits)
                              |
              +---------------+---------------+
              |               |               |
    quiver-driver-sqlite  quiver-driver-  quiver-driver-
    (rusqlite + adbc)     postgres         mysql
                          (postgres +      (mysql +
                           adbc)            adbc)
```

## Crate Layout

```
quiver/
  arrow-adbc-rs/           Git submodule: clean-room ADBC core + drivers (Shuozeli/arrow-adbc-rs)
  quiver-schema/           Schema language parser, AST, Arrow type mapping
  quiver-codegen/          Code generation (FlatBuffers, Protobuf, Rust, TypeScript, SQL DDL)
  quiver-query/            Type-safe query builder (SQL injection prevention, JOINs, CTEs, window functions)
  quiver-driver-core/      Driver trait + common types (Value, Row, Statement)
  quiver-driver-sqlite/    SQLite driver (rusqlite, re-exports adbc-sqlite)
  quiver-driver-postgres/  PostgreSQL driver (postgres crate, re-exports adbc-postgres)
  quiver-driver-mysql/     MySQL driver (mysql crate, re-exports adbc-mysql)
  quiver-migrate/          Migration engine (schema diff, DDL gen, tracking, introspection)
  quiver-e2e/              End-to-end integration tests
  quiver-cli/              CLI binary (parse, generate, migrate, db push/pull/execute)
  quiver-error/            Shared error types
```

## Schema Language

File extension: `.quiver`

### Syntax

```
// database configuration
config {
  provider  "postgresql"
  database  "myapp"
  url       env("DATABASE_URL")
}

// codegen targets
generate {
  flatbuffers  "./generated/fb"
  protobuf     "./generated/proto"
  rust         "./generated/rs"
}

// enum definition
enum Role {
  User
  Admin
  Moderator
}

// model definition
model User {
  // field      Type                          Attributes

  id            Int32                         @id @autoincrement
  email         Utf8                          @unique
  name          Utf8?                                              // nullable
  age           Int16?
  balance       Decimal128(10, 2)             @default(0)
  score         Float64                       @default(0.0)
  avatar        Binary?
  active        Boolean                       @default(true)
  created       Timestamp(Microsecond, UTC)   @default(now())
  updated       Timestamp(Microsecond, UTC)   @updatedAt
  tags          List<Utf8>                    @default([])
  metadata      Map<Utf8, Utf8>?
  role          Role                          @default(User)

  // relations
  posts         Post[]                        @relation
  profile       Profile?                      @relation

  // model-level attributes
  @@index([email])
  @@map("users")
}

model Post {
  id            Int32                         @id @autoincrement
  title         Utf8
  content       LargeUtf8?
  published     Boolean                       @default(false)
  views         UInt32                        @default(0)
  createdAt     Timestamp(Microsecond, UTC)   @default(now())

  // foreign key + relation
  authorId      Int32
  author        User                          @relation(fields: [authorId], references: [id])

  tags          List<Utf8>                    @default([])
  ratings       List<Float32>

  @@index([authorId])
  @@index([published, createdAt])
  @@map("posts")
}

model Profile {
  id            Int32                         @id @autoincrement
  bio           LargeUtf8?
  location      Struct<{
                  lat   Float64
                  lng   Float64
                  city  Utf8
                }>?
  socials       Map<Utf8, Utf8>               @default({})

  userId        Int32                         @unique
  user          User                          @relation(fields: [userId], references: [id])
}
```

### Type System

The type system is a 1:1 mapping to `arrow_schema::DataType`:

**Integers (signed):**
| Type | Arrow | Rust | Bytes |
|------|-------|------|-------|
| Int8 | DataType::Int8 | i8 | 1 |
| Int16 | DataType::Int16 | i16 | 2 |
| Int32 | DataType::Int32 | i32 | 4 |
| Int64 | DataType::Int64 | i64 | 8 |

**Integers (unsigned):**
| Type | Arrow | Rust | Bytes |
|------|-------|------|-------|
| UInt8 | DataType::UInt8 | u8 | 1 |
| UInt16 | DataType::UInt16 | u16 | 2 |
| UInt32 | DataType::UInt32 | u32 | 4 |
| UInt64 | DataType::UInt64 | u64 | 8 |

**Floating point:**
| Type | Arrow | Rust |
|------|-------|------|
| Float16 | DataType::Float16 | half::f16 |
| Float32 | DataType::Float32 | f32 |
| Float64 | DataType::Float64 | f64 |

**Decimal:**
| Type | Arrow | Rust |
|------|-------|------|
| Decimal128(p, s) | DataType::Decimal128(p, s) | i128 (with scale) |
| Decimal256(p, s) | DataType::Decimal256(p, s) | i256 (with scale) |

**String:**
| Type | Arrow | Rust | Max size |
|------|-------|------|----------|
| Utf8 | DataType::Utf8 | String | 2 GB |
| LargeUtf8 | DataType::LargeUtf8 | String | 8 EB |

**Binary:**
| Type | Arrow | Rust |
|------|-------|------|
| Binary | DataType::Binary | Vec<u8> |
| LargeBinary | DataType::LargeBinary | Vec<u8> |
| FixedSizeBinary(n) | DataType::FixedSizeBinary(n) | [u8; N] |

**Boolean:**
| Type | Arrow | Rust |
|------|-------|------|
| Boolean | DataType::Boolean | bool |

**Temporal:**
| Type | Arrow | Rust |
|------|-------|------|
| Date32 | DataType::Date32 | chrono::NaiveDate |
| Date64 | DataType::Date64 | chrono::NaiveDate |
| Time32(Second) | DataType::Time32(Second) | chrono::NaiveTime |
| Time32(Millisecond) | DataType::Time32(Millisecond) | chrono::NaiveTime |
| Time64(Microsecond) | DataType::Time64(Microsecond) | chrono::NaiveTime |
| Time64(Nanosecond) | DataType::Time64(Nanosecond) | chrono::NaiveTime |
| Timestamp(unit, tz) | DataType::Timestamp(unit, tz) | chrono::DateTime<Tz> |

Timestamp units: `Second`, `Millisecond`, `Microsecond`, `Nanosecond`
Timezone: `UTC`, `"America/New_York"`, or omitted for naive.

**Nested:**
| Type | Arrow | Rust |
|------|-------|------|
| List<T> | DataType::List(T) | Vec<T> |
| LargeList<T> | DataType::LargeList(T) | Vec<T> |
| Map<K, V> | DataType::Map(K, V) | HashMap<K, V> |
| Struct<{...}> | DataType::Struct(fields) | generated struct |

**Nullability:**
Append `?` to any type to make it nullable: `Utf8?`, `Int32?`, `List<Utf8>?`

### Attributes

| Attribute | Level | Meaning |
|-----------|-------|---------|
| @id | field | Primary key |
| @autoincrement | field | Auto-increment (database-generated) |
| @unique | field | Unique constraint |
| @default(value) | field | Default value (literal, now(), uuid(), cuid()) |
| @updatedAt | field | Auto-set on update |
| @map("name") | field | Database column name |
| @relation(...) | field | Foreign key relation |
| @ignore | field | Exclude from codegen |
| @@id([fields]) | model | Composite primary key |
| @@unique([fields]) | model | Composite unique constraint |
| @@index([fields]) | model | Database index |
| @@map("name") | model | Database table name |

### Grammar (EBNF sketch)

```ebnf
schema       = (config | generate | enum_def | model_def)*

config       = "config" "{" config_entry* "}"
config_entry = IDENT STRING

generate     = "generate" "{" gen_entry* "}"
gen_entry    = IDENT STRING

enum_def     = "enum" IDENT "{" enum_value* "}"
enum_value   = IDENT

model_def    = "model" IDENT "{" (field_def | model_attr)* "}"
field_def    = IDENT type_expr field_attr*

type_expr    = base_type "?"?
             | base_type "[]"          // sugar for List<base_type>
base_type    = scalar_type
             | "List" "<" type_expr ">"
             | "LargeList" "<" type_expr ">"
             | "Map" "<" type_expr "," type_expr ">"
             | "Struct" "<" "{" struct_field ("," struct_field)* "}" ">"
             | IDENT                   // enum or model reference

scalar_type  = "Int8" | "Int16" | "Int32" | "Int64"
             | "UInt8" | "UInt16" | "UInt32" | "UInt64"
             | "Float16" | "Float32" | "Float64"
             | "Decimal128" "(" INT "," INT ")"
             | "Decimal256" "(" INT "," INT ")"
             | "Utf8" | "LargeUtf8"
             | "Binary" | "LargeBinary" | "FixedSizeBinary" "(" INT ")"
             | "Boolean"
             | "Date32" | "Date64"
             | "Time32" "(" time_unit ")"
             | "Time64" "(" time_unit ")"
             | "Timestamp" "(" time_unit ("," timezone)? ")"

time_unit    = "Second" | "Millisecond" | "Microsecond" | "Nanosecond"
timezone     = "UTC" | STRING

struct_field = IDENT type_expr

field_attr   = "@id" | "@autoincrement" | "@unique" | "@updatedAt" | "@ignore"
             | "@default" "(" default_val ")"
             | "@map" "(" STRING ")"
             | "@relation" "(" relation_args ")"

model_attr   = "@@id" "(" "[" ident_list "]" ")"
             | "@@unique" "(" "[" ident_list "]" ")"
             | "@@index" "(" "[" ident_list "]" ")"
             | "@@map" "(" STRING ")"

default_val  = INT | FLOAT | STRING | BOOL | "now()" | "uuid()" | "cuid()"
             | "[" (default_val ("," default_val)*)? "]"
             | "{" "}"

relation_args = "fields" ":" "[" ident_list "]" "," "references" ":" "[" ident_list "]"
```

## Codegen Targets

Quiver compiles the schema to multiple targets. Each target produces typed
struct definitions. The user chooses which backend to use.

### Target 1: FlatBuffers (native Arrow format)

Since Arrow's wire format IS FlatBuffers, this is the most natural target.
`flatbuffers-rs` compiles the `.fbs` to zero-copy Rust types.

```
schema.quiver --> quiver-codegen --> schema.fbs --> flatbuffers-rs --> Rust types
```

Generated `.fbs`:
```flatbuffers
namespace Quiver.Models;

enum Role : byte { User = 0, Admin = 1, Moderator = 2 }

table User {
  id: int32;
  email: string (required);
  name: string;
  age: int16;
  balance: string (required);        // Decimal as string for precision
  score: float64 = 0.0;
  avatar: [ubyte];
  active: bool = true;
  created: int64;                    // epoch microseconds
  updated: int64;
  tags: [string];
  role: Role = User;
}

table UserCreateInput {
  email: string (required);
  name: string;
  age: int16;
  balance: string;
  score: float64;
  avatar: [ubyte];
  active: bool;
  tags: [string];
  role: Role;
}
```

**Advantage:** Zero-copy reads. A query result can be accessed without
deserialization -- just pointer arithmetic into the buffer.

### Target 2: Protobuf (interchange format)

For systems that speak protobuf. `protobuf-rs` compiles the `.proto` to
prost-compatible Rust types.

```
schema.quiver --> quiver-codegen --> schema.proto --> protobuf-rs --> Rust types
```

Generated `.proto`:
```protobuf
syntax = "proto3";
package quiver.models;

enum Role {
  ROLE_UNSPECIFIED = 0;
  ROLE_USER = 1;
  ROLE_ADMIN = 2;
  ROLE_MODERATOR = 3;
}

message User {
  int32 id = 1;
  string email = 2;
  optional string name = 3;
  optional int32 age = 4;
  string balance = 5;               // Decimal as string
  double score = 6;
  optional bytes avatar = 7;
  bool active = 8;
  int64 created_us = 9;             // epoch microseconds
  int64 updated_us = 10;
  repeated string tags = 11;
  Role role = 12;
}
```

### Target 3: Rust serde structs (current prisma-rs approach)

For backward compatibility and simplicity.

```
schema.quiver --> quiver-codegen --> client.rs (serde-derived structs)
```

### Target 4: TypeScript types

```
schema.quiver --> quiver-codegen --> client.ts (interfaces + delegates)
```

### Target 5: SQL DDL

For migrations. Maps Arrow types to database-specific SQL types.

```
schema.quiver --> quiver-migrate --> CREATE TABLE ... (PostgreSQL/MySQL/SQLite/DuckDB)
```

Arrow to SQL mapping (PostgreSQL):
| Arrow Type | PostgreSQL |
|-----------|-----------|
| Int16 | SMALLINT |
| Int32 | INTEGER |
| Int64 | BIGINT |
| Float32 | REAL |
| Float64 | DOUBLE PRECISION |
| Decimal128(p,s) | NUMERIC(p,s) |
| Utf8 | TEXT |
| LargeUtf8 | TEXT |
| Binary | BYTEA |
| Boolean | BOOLEAN |
| Date32 | DATE |
| Timestamp(us, UTC) | TIMESTAMPTZ |
| Time64(us) | TIME |
| List<Utf8> | TEXT[] |
| List<Int32> | INTEGER[] |
| Map<Utf8, Utf8> | JSONB |
| Struct<...> | JSONB |

## Query Execution

### Query Path 1: ADBC (standard)

```
quiver-client (query builder)
  |
  v
SQL generation (from query AST)
  |
  v
quiver-driver-adbc
  |
  v
ADBC connection (any database)
  |
  v
Arrow RecordBatch (result)
  |
  v
Zero-copy or convert to typed structs
```

ADBC returns `RecordBatch` (Arrow columnar format). Since our schema types
ARE Arrow types, the result maps directly to our structs with no conversion
overhead.

### Query Path 2: FlightSQL (distributed)

```
quiver-client (query builder)
  |
  v
FlightSQL request (Arrow Flight RPC)
  |
  v
quiver-driver-flightsql
  |
  v
Arrow Flight server (remote database)
  |
  v
Arrow RecordBatch stream (result)
  |
  v
Zero-copy or convert to typed structs
```

FlightSQL sends queries and receives results as Arrow RecordBatch streams
over gRPC. Same Arrow types, same zero conversion. This enables querying
remote databases, data lakes, or federated query engines (e.g., Dremio,
Ballista, DataFusion) with the same client API.

### Query Path 3: Native drivers (optional, for raw performance)

```
quiver-client (query builder)
  |
  v
quiver-driver-pg / quiver-driver-sqlite
  |
  v
Database protocol (PostgreSQL wire / SQLite C API)
  |
  v
Convert to Arrow types
```

Native drivers avoid the ADBC abstraction layer for maximum performance.
The conversion to Arrow types happens in the driver.

## Client API

### Generated Code

```rust
// From schema.quiver:
//   model User { id Int32 @id, email Utf8 @unique, name Utf8?, ... }

use quiver::client::{QuiverClient, ClientError};

// Generated model struct (backed by FlatBuffers, Protobuf, or serde)
pub struct User {
    pub id: i32,
    pub email: String,
    pub name: Option<String>,
    pub active: bool,
    pub balance: rust_decimal::Decimal,
    pub created: chrono::DateTime<chrono::Utc>,
    pub tags: Vec<String>,
    pub role: Role,
}

pub struct UserCreateInput { ... }
pub struct UserUpdateInput { ... }

// Query builder (fluent API)
pub struct UserDelegate<'a> { ... }

impl<'a> UserDelegate<'a> {
    // Read
    pub fn find_many(&self) -> UserFindMany<'a>;
    pub fn find_unique(&self, id: i32) -> UserFindUnique<'a>;
    pub fn find_first(&self) -> UserFindFirst<'a>;
    pub fn count(&self) -> UserCount<'a>;

    // Write
    pub fn create(&self, data: UserCreateInput) -> UserCreate<'a>;
    pub fn update(&self, id: i32, data: UserUpdateInput) -> UserUpdate<'a>;
    pub fn upsert(&self, id: i32, create: UserCreateInput, update: UserUpdateInput) -> UserUpsert<'a>;
    pub fn delete(&self, id: i32) -> UserDelete<'a>;

    // Bulk
    pub fn create_many(&self, data: Vec<UserCreateInput>) -> UserCreateMany<'a>;
    pub fn update_many(&self) -> UserUpdateMany<'a>;
    pub fn delete_many(&self) -> UserDeleteMany<'a>;
}

// Fluent query builder (chainable)
pub struct UserFindMany<'a> { ... }

impl<'a> UserFindMany<'a> {
    pub fn r#where(self, filter: UserWhereInput) -> Self;
    pub fn order_by(self, field: UserOrderBy) -> Self;
    pub fn skip(self, n: i64) -> Self;
    pub fn take(self, n: i64) -> Self;
    pub fn include_posts(self) -> Self;
    pub fn include_profile(self) -> Self;
    pub async fn exec(self) -> Result<Vec<User>, ClientError>;

    // Arrow-native: return raw RecordBatch instead of deserialized structs
    pub async fn exec_arrow(self) -> Result<RecordBatch, ClientError>;
}
```

### Usage

```rust
use quiver::generated::{PrismaClient, User, UserCreateInput, Role};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect via ADBC (works with any database)
    let client = QuiverClient::connect("postgresql://localhost/myapp").await?;

    // Create
    let user = client.user().create(UserCreateInput {
        email: "alice@example.com".into(),
        name: Some("Alice".into()),
        role: Role::Admin,
        tags: vec!["rust".into(), "arrow".into()],
        ..Default::default()
    }).exec().await?;

    // Query with fluent builder
    let admins = client.user()
        .find_many()
        .r#where(UserWhereInput {
            role: Some(Role::Admin),
            active: Some(true),
            ..Default::default()
        })
        .order_by(UserOrderBy::CreatedDesc)
        .take(10)
        .include_posts()
        .exec()
        .await?;

    // Arrow-native: get raw RecordBatch for analytics
    let batch = client.user()
        .find_many()
        .exec_arrow()
        .await?;

    // Use with DataFusion, Polars, or any Arrow-compatible tool
    println!("columns: {:?}", batch.schema().fields());
    println!("rows: {}", batch.num_rows());

    // FlightSQL: query a remote data lake with the same API
    let remote = QuiverClient::connect("flightsql://datalake.company.com").await?;
    let results = remote.user().find_many().exec().await?;

    // Transactions
    let tx = client.transaction().await?;
    tx.user().create(input1).exec().await?;
    tx.user().create(input2).exec().await?;
    tx.commit().await?;

    Ok(())
}
```

### Arrow-Native Features (not possible in traditional ORMs)

```rust
// 1. Zero-copy result access (FlatBuffers backend)
let result = client.user().find_many().exec_fb().await?;
let first_email = result.get(0).email();  // no deserialization

// 2. Columnar access (Arrow RecordBatch)
let batch = client.user().find_many().exec_arrow().await?;
let emails: &StringArray = batch.column(1).as_string();
let ages: &Int16Array = batch.column(3).as_primitive();

// 3. Direct interop with analytics tools
let df = polars::DataFrame::try_from(batch)?;
let ctx = datafusion::prelude::SessionContext::new();
ctx.register_batch("users", batch)?;
let analytics = ctx.sql("SELECT role, AVG(score) FROM users GROUP BY role").await?;

// 4. Bulk insert from Arrow (e.g., from Parquet file)
let parquet_batch = read_parquet("users.parquet")?;
client.user().insert_arrow(parquet_batch).await?;

// 5. Stream large results
let mut stream = client.user().find_many().exec_stream().await?;
while let Some(batch) = stream.next().await {
    process_batch(batch?);
}
```

## Introspection

Quiver can introspect an existing database and generate a `.quiver` schema:

```bash
quiver introspect --url "postgresql://localhost/myapp" --output schema.quiver
quiver introspect --url "duckdb:///analytics.db" --output schema.quiver
quiver introspect --url "flightsql://datalake.corp.com" --output schema.quiver
```

The introspection uses ADBC `get_objects()` which returns Arrow metadata.
Since Quiver's type system IS Arrow types, the introspected schema is
lossless -- no type guessing or approximation.

## Migration

```bash
quiver migrate dev --name "add_user_tags"     # create + apply migration
quiver migrate deploy                          # apply pending migrations
quiver migrate reset                           # reset database
quiver migrate diff                            # show pending changes
```

Migrations are SQL DDL files generated from schema diffs. The Arrow-to-SQL
mapping is database-specific (PostgreSQL, MySQL, SQLite, DuckDB).

## Project Dependencies

| Crate | Source | Purpose |
|-------|--------|---------|
| flatbuffers-rs | Shuozeli/flatbuffers-rs | FlatBuffers compiler + codegen |
| protobuf-rs | Shuozeli/protobuf-rs | Protobuf compiler + codegen |
| arrow-adbc-rs | Shuozeli/arrow-adbc-rs | Clean-room ADBC core + drivers (submodule) |
| arrow-schema | apache/arrow-rs | Arrow type definitions |
| arrow-array | apache/arrow-rs | RecordBatch + array types |
| rusqlite | rusqlite/rusqlite | SQLite driver |
| postgres | sfackler/rust-postgres | PostgreSQL driver |
| mysql | blackbeam/rust-mysql-simple | MySQL driver |
| clap | clap-rs/clap | CLI argument parsing |
| serde + serde_json | serde-rs/serde | Serialization |
| thiserror | dtolnay/thiserror | Error derive macros |
| base64 | marshallpierce/rust-base64 | Page token encoding |

## Comparison

| Feature | Prisma (TS) | Diesel | SQLx | SeaORM | **Quiver** |
|---------|-------------|--------|------|--------|-----------|
| Type system | Custom (lossy) | SQL-mapped | SQL-mapped | Custom (lossy) | **Arrow (lossless)** |
| Schema language | .prisma | Rust macros | None (SQL) | Rust macros | **.quiver (Arrow types)** |
| Wire format | JSON | DB protocol | DB protocol | DB protocol | **Arrow (columnar)** |
| Zero-copy results | No | No | No | No | **Yes (FlatBuffers)** |
| Columnar access | No | No | No | No | **Yes (RecordBatch)** |
| Analytics interop | No | No | No | No | **Yes (DataFusion/Polars)** |
| Multi-database | Yes (adapters) | No | Limited | Limited | **Yes (ADBC)** |
| Remote/distributed | No | No | No | No | **Yes (FlightSQL)** |
| Nested types | JSON blob | Limited | Limited | JSON blob | **First-class (List/Map/Struct)** |
| Code generation | TS only | Rust macros | None | Rust macros | **FlatBuf + Proto + Rust + TS** |

## What Makes This Different

1. **Arrow IS the type system.** Not a mapping layer. The schema types are
   Arrow types. The wire format is Arrow. The query results are Arrow.
   There is no conversion.

2. **Multiple serialization backends from one schema.** Write `.quiver` once,
   get FlatBuffers (zero-copy), Protobuf (interchange), serde (simple),
   and TypeScript (frontend) -- all from the same source of truth.

3. **Analytics-ready by default.** Every query result is a `RecordBatch`.
   Pipe it into DataFusion, Polars, or any Arrow-compatible tool. No ETL.

4. **FlightSQL as a first-class driver.** Query remote databases, data lakes,
   or federated query engines with the same ORM API you use for local databases.

5. **Lossless database introspection.** ADBC returns Arrow metadata. Quiver's
   type system is Arrow. Introspection is lossless -- no "best guess" type mapping.
