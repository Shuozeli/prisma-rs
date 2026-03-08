# Design: ADBC Schema to Protobuf/FlatBuffers Codegen

## Problem

The current Prisma codegen pipeline uses JSON (serde_json) as the serialization
format for query results and inputs. This works but has limitations:

1. **No schema enforcement at the wire level** -- JSON is untyped, so
   deserialization errors are caught at runtime, not compile time.
2. **Serialization overhead** -- JSON encoding/decoding is slower than binary
   formats, especially for large result sets.
3. **No zero-copy access** -- Every query result requires full deserialization
   into owned Rust structs.

ADBC (Arrow Database Connectivity) provides Arrow-format schema metadata from
database introspection. We can use this as a schema source, independent of
Prisma `.prisma` files, and generate protobuf or flatbuffers definitions that
enable typed, efficient serialization.

## Architecture

### Current Pipeline

```
Prisma Schema (.prisma)
  |
  v
PSL Parser (psl crate)
  |
  v
SchemaIR (prisma-codegen)
  |
  +---> RustGenerator   --> generated Rust code (serde_json types)
  +---> TypeScriptGenerator --> generated TS code
```

### New Pipeline

```
                          Prisma Schema (.prisma)          ADBC Arrow Schema
                                  |                               |
                                  v                               v
                          PSL Parser (psl)              ArrowSchemaConverter
                                  |                               |
                                  +---------- SchemaIR -----------+
                                                 |
                    +----------------------------+----------------------------+
                    |              |              |              |             |
                    v              v              v              v             v
              RustGenerator  TSGenerator   ProtoGenerator  FbsGenerator  RustProtoGenerator
                    |              |              |              |             |
                    v              v              v              v             v
              Rust client    TS client     .proto files    .fbs files    Rust client
              (serde_json)                       |              |        (protobuf types)
                                                 v              v
                                          protobuf-rs     flatbuffers-rs
                                          (compiler)       (compiler)
                                                 |              |
                                                 v              v
                                          Rust proto      Rust flatbuf
                                          types            types
```

### Two Schema Sources

| Source | Entry Point | Use Case |
|--------|-------------|----------|
| `.prisma` file | `SchemaIR::from_schema()` (existing) | Schema-first development |
| ADBC Arrow metadata | `SchemaIR::from_arrow_schema()` (new) | Database-first / introspection |

Both produce the same `SchemaIR`, so all downstream generators work with either.

## Components

### 1. Arrow Schema Converter (`prisma-codegen/src/arrow_schema.rs`)

Converts ADBC-provided Arrow schema into SchemaIR. This enables database-first
workflows where the schema is discovered at introspection time, not written by hand.

```rust
use arrow_schema::{Schema, DataType, Field};

pub struct ArrowSchemaConverter;

impl ArrowSchemaConverter {
    /// Convert Arrow table schemas (from ADBC get_objects) into SchemaIR.
    ///
    /// Each Arrow Schema represents one database table. The converter:
    /// 1. Maps Arrow DataType to ScalarKind
    /// 2. Infers nullability from Field.is_nullable()
    /// 3. Extracts primary key / unique info from Arrow metadata
    /// 4. Builds ModelIR for each table
    pub fn convert(
        tables: &[(String, Schema)],  // (table_name, arrow_schema) pairs
        provider: &str,
    ) -> Result<SchemaIR, CodegenError>;

    /// Convert a single Arrow DataType to ScalarKind.
    pub fn arrow_to_scalar_kind(dt: &DataType) -> ScalarKind;
}
```

**Type mapping (Arrow -> ScalarKind):**

| Arrow DataType | ScalarKind | Notes |
|---------------|------------|-------|
| Int8, Int16, Int32, UInt8, UInt16 | Int | |
| Int64, UInt32, UInt64 | BigInt | |
| Float16, Float32, Float64 | Float | |
| Decimal128, Decimal256 | Decimal | |
| Utf8, LargeUtf8, Utf8View | String | |
| Binary, LargeBinary, FixedSizeBinary | Bytes | |
| Boolean | Boolean | |
| Date32, Date64 | DateTime | |
| Timestamp | DateTime | |
| Time32, Time64 | DateTime | Could be separate Time kind |

**Metadata extraction:**

ADBC's `get_objects()` returns Arrow metadata that includes:
- Column names and types (from `Field`)
- Nullable flag (from `Field::is_nullable()`)
- Primary keys (from ADBC catalog metadata)
- Foreign keys (for relation inference)

The converter reads this metadata to populate `ModelIR.primary_key`,
`ModelIR.unique_constraints`, and `RelationField` references.

### 2. Proto Generator (`prisma-codegen/src/gen_proto.rs`)

Generates `.proto` files from SchemaIR. These can be compiled by `protobuf-rs`.

```rust
pub struct ProtoGenerator;

impl ProtoGenerator {
    /// Generate a .proto file from the SchemaIR.
    pub fn generate(ir: &SchemaIR, options: &ProtoGenOptions) -> Result<String, CodegenError>;
}

pub struct ProtoGenOptions {
    /// Proto package name (e.g., "prisma.models")
    pub package: String,
    /// Proto syntax version ("proto3")
    pub syntax: String,
    /// Generate service definitions for CRUD operations
    pub gen_services: bool,
    /// Generate well-known type wrappers for nullable fields
    pub use_wrappers: bool,
}
```

**Output format:**

```protobuf
syntax = "proto3";

package prisma.models;

import "google/protobuf/wrappers.proto";
import "google/protobuf/timestamp.proto";

// -- Enums --

enum Role {
  ROLE_UNSPECIFIED = 0;
  ROLE_USER = 1;
  ROLE_ADMIN = 2;
}

// -- Model messages --

message User {
  int32 id = 1;
  string email = 2;
  google.protobuf.StringValue name = 3;        // Optional<String> -> wrapper
  bool is_active = 4;
  google.protobuf.Timestamp created_at = 5;    // DateTime -> Timestamp
  Role role = 6;
}

message Post {
  int32 id = 1;
  string title = 2;
  string content = 3;
  bool published = 4;
  int32 author_id = 5;
}

// -- Input messages --

message UserCreateInput {
  string email = 1;
  google.protobuf.StringValue name = 2;
  optional bool is_active = 3;                  // has @default
  optional Role role = 4;
}

message UserUpdateInput {
  optional string email = 1;
  optional google.protobuf.StringValue name = 2;
  optional bool is_active = 3;
  optional Role role = 4;
}

message UserWhereUniqueInput {
  oneof key {
    int32 id = 1;
    string email = 2;
  }
}

// -- Query/Response wrappers --

message FindManyUsersRequest {
  optional UserWhereInput where = 1;
  optional OrderBy order_by = 2;
  optional int32 skip = 3;
  optional int32 take = 4;
}

message FindManyUsersResponse {
  repeated User users = 1;
}

// -- CRUD Service (optional) --

service UserService {
  rpc FindMany(FindManyUsersRequest) returns (FindManyUsersResponse);
  rpc FindUnique(UserWhereUniqueInput) returns (User);
  rpc Create(UserCreateInput) returns (User);
  rpc Update(UserUpdateRequest) returns (User);
  rpc Delete(UserWhereUniqueInput) returns (User);
}
```

**Scalar type mapping (ScalarKind -> proto type):**

| ScalarKind | Proto3 Type | Nullable Version |
|-----------|-------------|------------------|
| Int | int32 | google.protobuf.Int32Value |
| BigInt | int64 | google.protobuf.Int64Value |
| Float | float | google.protobuf.FloatValue |
| Decimal | string | google.protobuf.StringValue |
| String | string | google.protobuf.StringValue |
| Boolean | bool | google.protobuf.BoolValue |
| DateTime | google.protobuf.Timestamp | google.protobuf.Timestamp (already nullable) |
| Json | bytes | google.protobuf.BytesValue |
| Bytes | bytes | google.protobuf.BytesValue |
| Uuid | string | google.protobuf.StringValue |
| Enum(idx) | EnumName | optional EnumName (proto3 optional) |

### 3. FlatBuffers Generator (`prisma-codegen/src/gen_fbs.rs`)

Generates `.fbs` files from SchemaIR. These can be compiled by `flatbuffers-rs`.

```rust
pub struct FbsGenerator;

impl FbsGenerator {
    /// Generate a .fbs file from the SchemaIR.
    pub fn generate(ir: &SchemaIR, options: &FbsGenOptions) -> Result<String, CodegenError>;
}

pub struct FbsGenOptions {
    /// FlatBuffers namespace (e.g., "Prisma.Models")
    pub namespace: String,
    /// Generate root_type for the primary model
    pub root_type: Option<String>,
}
```

**Output format:**

```flatbuffers
namespace Prisma.Models;

// -- Enums --

enum Role : byte {
  User = 0,
  Admin = 1,
}

// -- Model tables --

table User {
  id: int;
  email: string (required);
  name: string;                   // nullable by default in flatbuffers
  is_active: bool = true;
  created_at: string (required);  // ISO 8601
  role: Role = User;
  posts: [Post];                  // relation
}

table Post {
  id: int;
  title: string (required);
  content: string;
  published: bool = false;
  author_id: int;
}

// -- Input tables --

table UserCreateInput {
  email: string (required);
  name: string;
  is_active: bool = true;
  role: Role;
}

table UserUpdateInput {
  email: string;
  name: string;
  is_active: bool;
  role: Role;
}

// -- Result set wrapper --

table UserResultSet {
  rows: [User];
  count: long;
}

root_type UserResultSet;
```

**Scalar type mapping (ScalarKind -> flatbuffers type):**

| ScalarKind | FBS Type | Notes |
|-----------|----------|-------|
| Int | int | 32-bit signed |
| BigInt | long | 64-bit signed |
| Float | float | 32-bit |
| Decimal | double | 64-bit, or string for exact precision |
| String | string | |
| Boolean | bool | |
| DateTime | string | ISO 8601 (no native timestamp in FBS) |
| Json | string | JSON-encoded |
| Bytes | [ubyte] | byte vector |
| Uuid | string | |
| Enum(idx) | EnumName | |

### 4. Rust Proto Client Generator (`prisma-codegen/src/gen_rust_proto.rs`)

Generates a Rust client that uses protobuf types directly instead of serde_json.
This is the key API change -- the generated code imports types from the compiled
`.proto` definitions and uses them for query results and inputs.

```rust
pub struct RustProtoGenerator;

impl RustProtoGenerator {
    /// Generate Rust client code that uses protobuf-compiled types.
    ///
    /// Prerequisites: The .proto file must already be compiled by protobuf-rs
    /// and the generated Rust types available as a module.
    pub fn generate(
        ir: &SchemaIR,
        options: &RustProtoGenOptions,
    ) -> Result<String, CodegenError>;
}

pub struct RustProtoGenOptions {
    /// Module path to the compiled proto types (e.g., "crate::proto::models")
    pub proto_module: String,
    /// Whether to generate conversion impls between proto and domain types
    pub gen_conversions: bool,
}
```

**Generated API (key difference from current serde-based API):**

```rust
// Current API (serde_json):
pub async fn find_many(&self) -> Result<Vec<User>, ClientError>
// where User is: #[derive(Serialize, Deserialize)] pub struct User { ... }

// New API (protobuf):
pub async fn find_many(&self) -> Result<Vec<proto::User>, ClientError>
// where proto::User is the protobuf-rs compiled type with:
//   - .id() -> i32
//   - .email() -> &str
//   - .encode_to_vec() -> Vec<u8>
//   - proto::User::decode(bytes) -> Result<proto::User, DecodeError>
```

The delegate methods remain the same signature, but the types they return are
protobuf-generated rather than serde-derived.

### 5. Integration with protobuf-rs and flatbuffers-rs

Both compilers are available as libraries with programmatic APIs:

**protobuf-rs integration:**
```rust
use protoc_rs_compiler::compile_single;
use protoc_rs_codegen::generate_rust;

fn compile_proto_schema(proto_source: &str) -> Result<String, Error> {
    let result = compile_single(proto_source)?;
    let rust_code = generate_rust(&result.schema, &Default::default())?;
    Ok(rust_code)
}
```

**flatbuffers-rs integration:**
```rust
use flatc_rs_compiler::compile_single;
use flatc_rs_codegen::generate_rust;

fn compile_fbs_schema(fbs_source: &str) -> Result<String, Error> {
    let result = compile_single(fbs_source)?;
    let rust_code = generate_rust(&result.schema, &CodeGenOptions::default())?;
    Ok(rust_code)
}
```

### 6. End-to-End Pipeline

The full pipeline for the `prisma generate` command:

```
Step 1: Schema Input
  a) From .prisma file:  SchemaIR::from_schema(prisma_content)
  b) From ADBC:          ArrowSchemaConverter::convert(arrow_tables, provider)

Step 2: Schema Definition Generation
  ProtoGenerator::generate(&ir, &opts)   -> user.proto
  FbsGenerator::generate(&ir, &opts)     -> user.fbs

Step 3: Type Compilation (using our own compilers)
  protobuf-rs::compile_single(proto_src) -> Rust proto types
  flatbuffers-rs::compile_single(fbs_src) -> Rust flatbuf types

Step 4: Client Code Generation
  RustProtoGenerator::generate(&ir, &opts) -> Rust client using proto types
  RustGenerator::generate(&ir)             -> Rust client using serde types (existing)

Step 5: Output
  Write generated files to output directory
```

## API Surface Changes

### Current Generated API

```rust
// Model type (serde-derived)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: i32,
    pub email: String,
    pub name: Option<String>,
}

// Input type (serde-derived)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserCreateInput {
    pub email: String,
    pub name: Option<String>,
}

// Delegate
impl<'a> UserDelegate<'a> {
    pub async fn find_many(&self) -> Result<Vec<User>, ClientError>;
    pub async fn create(&self, data: UserCreateInput) -> Result<User, ClientError>;
}
```

### New Generated API (Proto Mode)

```rust
// Re-export compiled proto types
pub use proto::User;
pub use proto::UserCreateInput;

// Conversion trait for query results
pub trait FromResultSet {
    fn from_result_set(rs: &SqlResultSet) -> Result<Vec<Self>, ClientError>
    where
        Self: Sized;
}

impl FromResultSet for proto::User {
    fn from_result_set(rs: &SqlResultSet) -> Result<Vec<Self>, ClientError> {
        rs.rows.iter().map(|row| {
            let mut user = proto::User::default();
            user.id = row.get_i32("id")?;
            user.email = row.get_string("email")?.to_string();
            user.name = row.get_optional_string("name")?.map(|s| s.to_string());
            Ok(user)
        }).collect()
    }
}

// Delegate (same interface, different types)
impl<'a> UserDelegate<'a> {
    pub async fn find_many(&self) -> Result<Vec<proto::User>, ClientError>;
    pub async fn create(&self, data: proto::UserCreateInput) -> Result<proto::User, ClientError>;
}

// Binary serialization methods on the client
impl PrismaClient {
    /// Serialize query results to protobuf bytes.
    pub fn encode_results<T: prost::Message>(results: &[T]) -> Vec<u8>;

    /// Deserialize protobuf bytes to query results.
    pub fn decode_results<T: prost::Message + Default>(bytes: &[u8]) -> Result<Vec<T>, ClientError>;
}
```

### New Generated API (FlatBuffers Mode)

```rust
// FlatBuffers types are zero-copy references
pub use fb::User;
pub use fb::UserArgs;

// Delegate returns owned FlatBuffer bytes
impl<'a> UserDelegate<'a> {
    /// Returns a FlatBuffer-encoded result set.
    /// Access individual rows via fb::root_as_user_result_set(bytes).
    pub async fn find_many_fb(&self) -> Result<FlatBufferResult, ClientError>;

    /// Returns deserialized objects (Object API).
    pub async fn find_many(&self) -> Result<Vec<fb::UserT>, ClientError>;
}

pub struct FlatBufferResult {
    bytes: Vec<u8>,
}

impl FlatBufferResult {
    /// Zero-copy access to results.
    pub fn root(&self) -> fb::UserResultSet<'_>;
    /// Owned/unpacked access.
    pub fn unpack(&self) -> Vec<fb::UserT>;
}
```

## Implementation Plan

### Phase 1: Schema Generators (proto + fbs output)

**Goal:** From a Prisma schema or ADBC Arrow schema, produce valid `.proto` and
`.fbs` definition files.

1. Add `ArrowSchemaConverter` to prisma-codegen
2. Add `ProtoGenerator` to prisma-codegen
3. Add `FbsGenerator` to prisma-codegen
4. Add `--format proto|fbs` flag to `prisma generate` CLI
5. Unit tests: roundtrip SchemaIR -> .proto -> protobuf-rs compile
6. Unit tests: roundtrip SchemaIR -> .fbs -> flatbuffers-rs compile

**Deliverables:**
- `prisma generate --format proto` outputs a `.proto` file
- `prisma generate --format fbs` outputs a `.fbs` file
- Both files compile cleanly with the respective Rust compilers

### Phase 2: Type Compilation Integration

**Goal:** Integrate protobuf-rs and flatbuffers-rs as library dependencies so
the full pipeline (schema -> compiled Rust types) runs in one step.

1. Add `protoc-rs-schema`, `protoc-rs-parser`, `protoc-rs-codegen` as dependencies
2. Add `flatc-rs-schema`, `flatc-rs-parser`, `flatc-rs-codegen` as dependencies
3. Build a `SchemaCompiler` that orchestrates: SchemaIR -> proto/fbs -> Rust types
4. Output compiled types to a generated module file

**Deliverables:**
- `prisma generate --format proto-rust` outputs both `.proto` and compiled Rust
- `prisma generate --format fbs-rust` outputs both `.fbs` and compiled Rust

### Phase 3: Client Codegen with Proto/FBS Types

**Goal:** Generate a Prisma client that uses protobuf or flatbuffers types
instead of serde_json types.

1. Add `RustProtoGenerator` that generates delegate methods using proto types
2. Add `FromResultSet` trait and impl generation for proto types
3. Add `RustFbsGenerator` for flatbuffers variant
4. Modify `BasePrismaClient` to support pluggable serialization
5. Integration tests against real databases

**Deliverables:**
- Full working Prisma client with protobuf serialization
- Full working Prisma client with flatbuffers serialization
- Performance benchmarks vs serde_json baseline

### Phase 4: ADBC Schema Introspection

**Goal:** Introspect a database via ADBC, discover schema, and generate
proto/fbs definitions without a `.prisma` file.

1. Add `prisma introspect --format proto` to CLI
2. Use ADBC `get_objects()` to discover tables, columns, keys
3. Convert Arrow metadata to SchemaIR via `ArrowSchemaConverter`
4. Pipe through existing generators

**Deliverables:**
- Database -> `.proto`/`.fbs` in one command
- Supports PostgreSQL, MySQL, SQLite, DuckDB via ADBC

## Open Questions

1. **Relation handling in proto/fbs:** Protobuf messages can nest, but this
   implies eagerly loading relations. Should we use `int32 author_id` (FK only)
   or `User author` (nested message) for relations? Likely both: FK field
   always present, nested message available via `Include` semantics.

2. **Optional semantics:** Proto3 `optional` keyword vs wrapper types
   (`google.protobuf.StringValue`). Wrapper types are more explicit but verbose.
   Proto3 `optional` with `has_*()` methods may be cleaner.

3. **DateTime representation:** Proto has `google.protobuf.Timestamp` (seconds +
   nanos). FlatBuffers has no built-in timestamp. Use string (ISO 8601) for
   FlatBuffers, Timestamp for proto? Or use int64 (epoch millis) for both for
   consistency?

4. **Decimal precision:** Proto has no decimal type. Use `string` for exact
   representation (current approach) or `double` for speed? Configurable?

5. **Backward compatibility:** Should the proto-based client implement the same
   `PrismaClient` trait as the serde-based client? Or is it a separate type?
   Using a trait would allow generic code over serialization format.

6. **Build integration:** Should the proto/fbs compilation happen at build time
   (`build.rs`) or at `prisma generate` time? Build-time is more Rust-idiomatic
   but requires the compilers as build dependencies.

## Dependencies

### New Crate Dependencies

```toml
# In prisma-codegen/Cargo.toml

[dependencies]
# Arrow schema types (for ArrowSchemaConverter)
arrow-schema = "56"

# Protobuf compiler (Shuozeli/protobuf-rs)
protoc-rs-schema = { git = "https://github.com/Shuozeli/protobuf-rs" }
protoc-rs-parser = { git = "https://github.com/Shuozeli/protobuf-rs" }
protoc-rs-codegen = { git = "https://github.com/Shuozeli/protobuf-rs" }

# FlatBuffers compiler (Shuozeli/flatbuffers-rs)
flatc-rs-schema = { git = "https://github.com/Shuozeli/flatbuffers-rs" }
flatc-rs-parser = { git = "https://github.com/Shuozeli/flatbuffers-rs" }
flatc-rs-codegen = { git = "https://github.com/Shuozeli/flatbuffers-rs" }

# Runtime serialization
prost = "0.13"                     # For protobuf message encoding/decoding
flatbuffers = "25"                 # For flatbuffers runtime
```

### Runtime Dependencies (in generated code)

```toml
# User's Cargo.toml (added by prisma generate)
[dependencies]
prost = "0.13"           # if using proto mode
flatbuffers = "25"       # if using fbs mode
```
