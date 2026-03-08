# Protobuf Integration: Implementation Phases

## Overview

Add protobuf as an alternative struct definition format for prisma-rs codegen,
using `Shuozeli/protobuf-rs` as the proto compiler. Protobuf replaces serde-derived
structs with proto-compiled message types for models, inputs, and enums.

No gRPC. No services. Just typed struct definitions via `.proto` schema.

**End state:** `prisma generate --format proto` produces:
1. A `.proto` file defining all models, enums, and input types
2. Compiled Rust structs (via protobuf-rs codegen)
3. A Prisma client whose delegates return proto message types

---

## Phase 1: ProtoGenerator -- SchemaIR to .proto

**Goal:** Generate valid `.proto` files from any Prisma schema. No new
dependencies. No runtime changes. Just string output, same as RustGenerator.

### Tasks

1. **Add `gen_proto.rs` module to prisma-codegen**
   - `ProtoGenerator::generate(ir: &SchemaIR, options: &ProtoGenOptions) -> Result<String, CodegenError>`
   - `ProtoGenOptions { package: String }`

2. **Enum generation**
   - Map each `EnumIR` to a proto3 enum
   - Add `_UNSPECIFIED = 0` sentinel (proto3 requirement)
   - Prefix variant names with enum name to avoid proto namespace collisions
     (e.g., `Role::USER` -> `ROLE_USER = 1`)

3. **Model message generation**
   - Map each `ModelIR` to a proto3 `message`
   - Scalar fields: `ScalarKind` -> proto type (see mapping table below)
   - Optional fields: `optional` keyword (proto3 explicit presence)
   - List fields: `repeated`
   - Relation fields: FK scalar only (no nested messages in Phase 1)
   - Auto-assign field numbers sequentially

4. **Input message generation**
   - `{Model}CreateInput`: exclude auto fields (id+autoincrement, updatedAt),
     fields with defaults become `optional`
   - `{Model}UpdateInput`: all fields `optional`
   - `{Model}WhereUniqueInput`: `oneof key` with primary key + unique fields

5. **Scalar type mapping**

   | ScalarKind | Proto3 Type | Notes |
   |-----------|-------------|-------|
   | Int | int32 | |
   | BigInt | int64 | |
   | Float | double | f64 precision |
   | Decimal | string | exact representation |
   | String | string | |
   | Boolean | bool | |
   | DateTime | string | ISO 8601 (avoid well-known type dep for Phase 1) |
   | Json | string | JSON-encoded |
   | Bytes | bytes | |
   | Uuid | string | |
   | Enum(idx) | EnumName | |

   All optional variants use the `optional` keyword (proto3 explicit presence).
   List variants use `repeated`.

6. **Unit tests**
   - Parse a multi-model Prisma schema -> generate .proto -> assert valid syntax
   - Test all ScalarKind mappings
   - Test Optional/List/Required arity
   - Test enum generation with UNSPECIFIED sentinel
   - Test CreateInput excludes auto fields
   - Test UpdateInput has all fields optional
   - Test WhereUniqueInput oneof with composite keys

7. **Wire to CLI**
   - Add `--format proto` option to `prisma generate` command
   - Write output to `{output_dir}/schema.proto`

### Dark Launch Validation

Run against these reference schemas:
- Simple: single model with all scalar types
- Relations: User/Post/Profile with 1:1, 1:N, N:M
- Enums: model with enum fields
- Defaults: model with autoincrement, now(), uuid(), literal defaults

Manually verify each `.proto` output is valid proto3 syntax.

### Deliverable

`prisma generate --schema test.prisma --format proto` outputs a syntactically
valid `.proto` file. No compilation, no runtime. Just struct definitions.

---

## Phase 2: protobuf-rs Integration -- .proto to Rust Types

**Goal:** Compile the generated `.proto` into Rust struct types using
protobuf-rs as a library dependency. One-step pipeline: SchemaIR -> .proto
-> Rust source.

### Tasks

1. **Add protobuf-rs dependencies to prisma-codegen**
   ```toml
   [dependencies]
   protoc-rs-schema = { git = "https://github.com/Shuozeli/protobuf-rs" }
   protoc-rs-parser = { git = "https://github.com/Shuozeli/protobuf-rs" }
   protoc-rs-analyzer = { git = "https://github.com/Shuozeli/protobuf-rs" }
   protoc-rs-codegen = { git = "https://github.com/Shuozeli/protobuf-rs" }
   ```

2. **Add `proto_compiler.rs` module**
   ```rust
   pub struct ProtoCompiler;
   impl ProtoCompiler {
       /// Generate .proto from SchemaIR, then compile to Rust via protobuf-rs.
       pub fn compile(ir: &SchemaIR, options: &ProtoCompileOptions)
           -> Result<ProtoCompileOutput, CodegenError>;
   }

   pub struct ProtoCompileOutput {
       pub proto_source: String,   // the .proto content
       pub rust_source: String,    // compiled Rust structs
   }
   ```

3. **Compilation pipeline**
   - Step 1: `ProtoGenerator::generate()` -> proto source string
   - Step 2: `protoc_rs_parser::parse()` -> parsed proto AST
   - Step 3: `protoc_rs_analyzer::analyze()` -> validated + resolved schema
   - Step 4: `protoc_rs_codegen::generate_rust()` -> Rust source string

4. **Unit tests**
   - Roundtrip: Prisma schema -> .proto -> protobuf-rs compile -> valid Rust
   - Verify generated Rust compiles (use `syn::parse_file` to syntax-check)
   - Test with all scalar types, enums, optional/list fields

5. **Wire to CLI**
   - `prisma generate --format proto` now also writes compiled `schema.pb.rs`
   - Output: `{output_dir}/schema.proto` + `{output_dir}/schema.pb.rs`

### Deliverable

`prisma generate --format proto` outputs both `.proto` and compiled `.pb.rs`.
The Rust file contains prost-compatible struct definitions for all models,
enums, and inputs.

---

## Phase 3: Proto-Typed Client Generator

**Goal:** Generate a Prisma client where delegate methods accept and return
proto message types instead of serde-derived structs. This phase combines
the result-set conversion layer and the client generator since they are
tightly coupled.

### Tasks

1. **Add `gen_rust_proto.rs` module to prisma-codegen**
   ```rust
   pub struct RustProtoGenerator;
   impl RustProtoGenerator {
       pub fn generate(ir: &SchemaIR, options: &RustProtoGenOptions)
           -> Result<String, CodegenError>;
   }

   pub struct RustProtoGenOptions {
       pub proto_module: String,  // e.g., "super::proto"
   }
   ```

2. **ResultSet -> proto conversion (generated per model)**

   For each model, generate a conversion function from `SqlResultSet` rows
   to proto message types:
   ```rust
   fn user_from_row(columns: &[String], values: &[ResultValue])
       -> Result<proto::User, ClientError>
   {
       let mut msg = proto::User::default();
       for (i, col) in columns.iter().enumerate() {
           match col.as_str() {
               "id" => msg.id = values[i].as_i32()?,
               "email" => msg.email = values[i].as_string()?,
               "name" => msg.name = values[i].as_optional_string()?,
               _ => {}
           }
       }
       Ok(msg)
   }
   ```

3. **Proto input -> QueryValue conversion (generated per input)**

   For each input type, generate conversion to query parameters:
   ```rust
   fn user_create_to_params(input: &proto::UserCreateInput)
       -> Vec<(String, QueryValue)>
   {
       let mut params = Vec::new();
       params.push(("email".into(), QueryValue::String(input.email.clone())));
       if let Some(ref name) = input.name {
           params.push(("name".into(), QueryValue::String(name.clone())));
       }
       params
   }
   ```

4. **Delegate methods using proto types**
   Same method names as the serde-based generator, different types:
   ```rust
   impl<'a> UserDelegate<'a> {
       pub async fn find_many(&self) -> Result<Vec<proto::User>, ClientError>;
       pub async fn find_unique(&self, r#where: Value)
           -> Result<Option<proto::User>, ClientError>;
       pub async fn create(&self, data: proto::UserCreateInput)
           -> Result<proto::User, ClientError>;
       pub async fn update(&self, r#where: Value, data: proto::UserUpdateInput)
           -> Result<proto::User, ClientError>;
       pub async fn delete(&self, r#where: Value)
           -> Result<proto::User, ClientError>;
       // ... same full set of CRUD methods
   }
   ```

5. **Generated output structure**
   ```
   generated/
     schema.proto          # proto3 struct definitions
     schema.pb.rs          # compiled Rust types (from protobuf-rs)
     client.rs             # PrismaClient + delegates using proto types
     mod.rs                # re-exports
   ```

6. **Integration tests**
   - Generate proto client from a test schema
   - Compile the generated code (verify it builds)
   - Test against SQLite (in-memory) with real queries:
     - create -> find_unique roundtrip
     - find_many with multiple rows
     - update + verify changed fields
     - delete + verify count
   - Verify proto encode/decode roundtrip on results

### Deliverable

`prisma generate --format proto` produces a fully functional Prisma client
using protobuf struct types. The client compiles, connects to a database,
and executes queries returning proto messages.

---

## Phase 4: ADBC Schema Introspection

**Goal:** Discover database schema via ADBC Arrow metadata, convert to
SchemaIR, then feed into the proto pipeline. No `.prisma` file needed.

### Tasks

1. **Add `arrow_schema.rs` module to prisma-codegen**
   ```rust
   pub struct ArrowSchemaConverter;
   impl ArrowSchemaConverter {
       pub fn convert(
           tables: &[(String, arrow_schema::Schema)],
           provider: &str,
       ) -> Result<SchemaIR, CodegenError>;

       pub fn arrow_to_scalar_kind(dt: &arrow_schema::DataType) -> ScalarKind;
   }
   ```

2. **ADBC introspection in driver-adbc**
   - Add `introspect_schema()` method to `AdbcDriverAdapter`
   - Use ADBC `get_objects()` to list tables, columns, types, keys
   - Return `Vec<(String, arrow_schema::Schema)>` with table metadata

3. **Relation inference from foreign keys**
   - ADBC metadata includes foreign key constraints
   - Infer `RelationField` entries from FK -> PK references
   - Determine relation kind (1:1 vs 1:N) from unique constraints on FK

4. **Primary key and unique constraint extraction**
   - Read primary key metadata from ADBC catalog
   - Read unique index metadata
   - Populate `ModelIR.primary_key` and `ModelIR.unique_constraints`

5. **CLI command: `prisma introspect`**
   ```bash
   prisma introspect --url "postgresql://..." --format proto
   prisma introspect --url "duckdb:///data.db" --format proto
   ```
   - Connect to database via appropriate ADBC driver
   - Introspect schema -> SchemaIR -> .proto + compiled Rust types

6. **Tests**
   - Introspect a PostgreSQL database with known schema -> verify SchemaIR
   - Introspect DuckDB -> verify Arrow type mappings
   - Roundtrip: create table (SQL) -> introspect -> generate proto -> verify

### Deliverable

`prisma introspect --url <db-url> --format proto` connects to any
ADBC-supported database, discovers the schema, and generates protobuf
struct definitions + compiled Rust types.

---

## Dependency Graph

```
Phase 1 (ProtoGenerator: SchemaIR -> .proto)
   |
   v
Phase 2 (protobuf-rs: .proto -> Rust structs)
   |
   v
Phase 3 (Proto client: delegates using proto types)
   |
Phase 2 -------> Phase 4 (ADBC introspect -> SchemaIR -> proto)
```

Phases 1-2 are pure codegen (no runtime changes).
Phase 3 introduces the proto-typed client.
Phase 4 adds the ADBC entry point (independent of Phase 3, needs only 1-2).

---

## Success Criteria

| Phase | Metric |
|-------|--------|
| 1 | Generated .proto parses successfully via protobuf-rs parser |
| 2 | Generated Rust structs compile without errors |
| 3 | Full CRUD test suite passes with proto-typed client against SQLite |
| 4 | Introspect real DB -> generate -> compile -> query succeeds |

## Risk Mitigation

- **protobuf-rs API stability:** Pin to a specific git rev in Cargo.toml
- **Proto field numbering:** Sequential assignment is safe for generated code
  since we control both producer and consumer. Document that field numbers
  are NOT stable across schema changes (regenerate both sides).
- **Backward compatibility:** The serde-based generator (`--format rust`) is
  completely unchanged. Proto is opt-in via `--format proto`.
- **DateTime representation:** Use ISO 8601 string in Phase 1 to avoid
  well-known type dependency. Can upgrade to `google.protobuf.Timestamp`
  later if needed.
