//! Discrepancy verification: checks our generated code against the
//! reference Prisma TypeScript client API surface.
//!
//! The reference spec is taken from the official Prisma TS packages:
//!   - packages/dmmf/src/dmmf.ts (ModelAction enum)
//!   - packages/client-generator-js/src/utils.ts (naming conventions)
//!   - packages/json-protocol/src/index.ts (JSON protocol format)
//!   - packages/client-generator-js/src/utils/common.ts (scalar mapping)
//!
//! Any intentional deviations from the reference are documented inline.

use prisma_codegen::{RustGenerator, SchemaIR, TypeScriptGenerator};

const FULL_SCHEMA: &str = r#"
    datasource db {
        provider = "postgresql"
    }

    enum Role {
        USER
        ADMIN
        MODERATOR
    }

    model User {
        id        Int      @id @default(autoincrement())
        email     String   @unique
        name      String?
        role      Role     @default(USER)
        age       Int?
        balance   Float?
        isActive  Boolean  @default(true)
        bio       String?
        createdAt DateTime @default(now())
        metadata  Json?
        posts     Post[]
        profile   Profile?
    }

    model Post {
        id        Int      @id @default(autoincrement())
        title     String
        content   String?
        published Boolean  @default(false)
        authorId  Int
        author    User     @relation(fields: [authorId], references: [id])
        tags      Tag[]
    }

    model Profile {
        id     Int    @id @default(autoincrement())
        bio    String
        userId Int    @unique
        user   User   @relation(fields: [userId], references: [id])
    }

    model Tag {
        id    Int    @id @default(autoincrement())
        name  String @unique
        posts Post[]
    }
"#;

fn ir() -> SchemaIR {
    SchemaIR::from_schema(FULL_SCHEMA).unwrap()
}

// ============================================================================
// Reference spec: the canonical Prisma TS client surface
// ============================================================================

/// All delegate methods the reference TS client generates for each model.
/// Source: packages/dmmf/src/dmmf.ts ModelAction enum
const REFERENCE_TS_DELEGATE_METHODS: &[&str] = &[
    "findUnique",
    "findUniqueOrThrow",
    "findFirst",
    "findFirstOrThrow",
    "findMany",
    "create",
    "createMany",
    "createManyAndReturn",
    "update",
    "updateMany",
    "updateManyAndReturn",
    "upsert",
    "delete",
    "deleteMany",
    "aggregate",
    "groupBy",
    "count",
];

/// Delegate methods the reference Rust client generates.
/// Same core set as TS, minus TS-only convenience (AndReturn variants).
const REFERENCE_RUST_DELEGATE_METHODS: &[&str] = &[
    "find_unique",
    "find_unique_or_throw",
    "find_first",
    "find_first_or_throw",
    "find_many",
    "create",
    "create_many",
    "update",
    "update_many",
    "upsert",
    "delete",
    "delete_many",
    "aggregate",
    "group_by",
    "count",
];

/// Global client methods on PrismaClient.
const REFERENCE_GLOBAL_METHODS: &[&str] = &["$connect", "$disconnect", "$transaction", "$queryRaw", "$executeRaw"];

/// The JSON protocol action strings (what gets sent over the wire).
/// Source: packages/json-protocol/src/index.ts
const REFERENCE_JSON_ACTIONS: &[(&str, &str)] = &[
    // (TS delegate method, JSON protocol action)
    ("findUnique", "findUnique"),
    ("findUniqueOrThrow", "findUniqueOrThrow"),
    ("findFirst", "findFirst"),
    ("findFirstOrThrow", "findFirstOrThrow"),
    ("findMany", "findMany"),
    ("create", "createOne"),
    ("createMany", "createMany"),
    ("createManyAndReturn", "createManyAndReturn"),
    ("update", "updateOne"),
    ("updateMany", "updateMany"),
    ("updateManyAndReturn", "updateManyAndReturn"),
    ("upsert", "upsertOne"),
    ("delete", "deleteOne"),
    ("deleteMany", "deleteMany"),
    ("aggregate", "aggregate"),
    ("groupBy", "groupBy"),
    ("executeRaw", "executeRaw"),
    ("queryRaw", "queryRaw"),
];

/// Input type naming patterns generated per model.
/// Source: packages/client-generator-js/src/utils.ts
const REFERENCE_TS_INPUT_TYPES: &[&str] = &[
    "WhereInput",
    "WhereUniqueInput",
    "CreateInput",
    "UpdateInput",
    "OrderByInput",
    "Select",
    "Include",
    "FindManyArgs",
];

/// Scalar type mapping: (Prisma type, expected TS type).
/// Source: packages/client-generator-js/src/utils/common.ts
const REFERENCE_SCALAR_MAPPING_TS: &[(&str, &str)] = &[
    ("Int", "number"),
    ("String", "string"),
    ("Boolean", "boolean"),
    ("DateTime", "Date"),
    ("Float", "number"),
    ("Json", "JsonValue"),
];

/// Scalar type mapping for Rust output.
const REFERENCE_SCALAR_MAPPING_RS: &[(&str, &str)] = &[
    ("Int", "i32"),
    ("BigInt", "i64"),
    ("Float", "f64"),
    ("Decimal", "f64"),
    ("String", "String"),
    ("Boolean", "bool"),
    ("DateTime", "String"),
    ("Json", "Value"),
    ("Bytes", "Vec<u8>"),
];

// ============================================================================
// 1. Schema IR verification
// ============================================================================

#[test]
fn ir_parses_all_models() {
    let schema = ir();
    let model_names: Vec<&str> = schema.models.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(model_names, vec!["User", "Post", "Profile", "Tag"]);
}

#[test]
fn ir_parses_enums() {
    let schema = ir();
    assert_eq!(schema.enums.len(), 1);
    assert_eq!(schema.enums[0].name, "Role");
    let values: Vec<&str> = schema.enums[0].values.iter().map(|v| v.name.as_str()).collect();
    assert_eq!(values, vec!["USER", "ADMIN", "MODERATOR"]);
}

#[test]
fn ir_parses_all_scalar_types() {
    let schema = ir();
    let user = schema.models.iter().find(|m| m.name == "User").unwrap();

    let expected_scalars = vec![
        ("id", "Int", false),
        ("email", "String", false),
        ("name", "String", true),
        ("role", "Enum", false),
        ("age", "Int", true),
        ("balance", "Float", true),
        ("isActive", "Boolean", false),
        ("bio", "String", true),
        ("createdAt", "DateTime", false),
        ("metadata", "Json", true),
    ];

    for (name, kind_str, optional) in &expected_scalars {
        let field = user
            .fields
            .iter()
            .find_map(|f| match f {
                prisma_codegen::ModelField::Scalar(sf) if sf.name == *name => Some(sf),
                _ => None,
            })
            .unwrap_or_else(|| panic!("Missing scalar field: {}", name));

        let is_optional = field.arity == prisma_codegen::FieldArity::Optional;
        assert_eq!(
            is_optional, *optional,
            "Field '{}' optional mismatch: got {}, expected {}",
            name, is_optional, optional
        );

        // Verify kind matches (loose check -- Enum is special)
        let kind_matches = matches!(
            (*kind_str, field.scalar_kind),
            ("Int", prisma_codegen::ScalarKind::Int)
                | ("String", prisma_codegen::ScalarKind::String)
                | ("Float", prisma_codegen::ScalarKind::Float)
                | ("Boolean", prisma_codegen::ScalarKind::Boolean)
                | ("DateTime", prisma_codegen::ScalarKind::DateTime)
                | ("Json", prisma_codegen::ScalarKind::Json)
                | ("Enum", prisma_codegen::ScalarKind::Enum(_))
        );
        assert!(
            kind_matches,
            "Field '{}' kind mismatch: expected {}, got {:?}",
            name, kind_str, field.scalar_kind
        );
    }
}

#[test]
fn ir_parses_relations() {
    let schema = ir();

    // User -> Post[] (one-to-many)
    let user = schema.models.iter().find(|m| m.name == "User").unwrap();
    let posts_rel = user
        .fields
        .iter()
        .find_map(|f| match f {
            prisma_codegen::ModelField::Relation(rf) if rf.name == "posts" => Some(rf),
            _ => None,
        })
        .expect("User should have 'posts' relation");
    assert_eq!(posts_rel.related_model, "Post");
    assert_eq!(posts_rel.arity, prisma_codegen::FieldArity::List);

    // User -> Profile? (one-to-one)
    let profile_rel = user
        .fields
        .iter()
        .find_map(|f| match f {
            prisma_codegen::ModelField::Relation(rf) if rf.name == "profile" => Some(rf),
            _ => None,
        })
        .expect("User should have 'profile' relation");
    assert_eq!(profile_rel.related_model, "Profile");
    assert_eq!(profile_rel.arity, prisma_codegen::FieldArity::Optional);

    // Post -> User (many-to-one with FK)
    let post = schema.models.iter().find(|m| m.name == "Post").unwrap();
    let author_rel = post
        .fields
        .iter()
        .find_map(|f| match f {
            prisma_codegen::ModelField::Relation(rf) if rf.name == "author" => Some(rf),
            _ => None,
        })
        .expect("Post should have 'author' relation");
    assert_eq!(author_rel.related_model, "User");
    assert_eq!(author_rel.fk_fields, vec!["authorId"]);
    assert_eq!(author_rel.references, vec!["id"]);
}

#[test]
fn ir_parses_unique_constraints() {
    let schema = ir();
    let user = schema.models.iter().find(|m| m.name == "User").unwrap();
    assert!(
        user.unique_constraints.iter().any(|c| c == &["email"]),
        "User should have @unique on email"
    );

    let profile = schema.models.iter().find(|m| m.name == "Profile").unwrap();
    assert!(
        profile.unique_constraints.iter().any(|c| c == &["userId"]),
        "Profile should have @unique on userId"
    );
}

// ============================================================================
// 2. TypeScript generator: delegate method coverage
// ============================================================================

#[test]
fn ts_delegate_methods_present() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    let mut missing = Vec::new();
    for method in REFERENCE_TS_DELEGATE_METHODS {
        // Check the UserDelegate for each method
        let pattern = format!("{}(", method);
        if !code.contains(&pattern) {
            missing.push(*method);
        }
    }

    // Document known gaps
    let known_missing: Vec<&str> = vec![];

    let unexpected_missing: Vec<&&str> = missing.iter().filter(|m| !known_missing.contains(m)).collect();

    assert!(
        unexpected_missing.is_empty(),
        "Unexpected missing TS delegate methods: {:?}\n\
         Known missing (tracked): {:?}",
        unexpected_missing,
        known_missing
    );

    // Verify the known missing list is accurate
    for method in &known_missing {
        assert!(
            missing.contains(method),
            "Method '{}' is in known_missing but is now present -- remove from known_missing",
            method,
        );
    }
}

// ============================================================================
// 3. Rust generator: delegate method coverage
// ============================================================================

#[test]
fn rs_delegate_methods_present() {
    let schema = ir();
    let code = RustGenerator::generate(&schema).unwrap();

    let mut missing = Vec::new();
    for method in REFERENCE_RUST_DELEGATE_METHODS {
        let pattern = format!("fn {}(", method);
        if !code.contains(&pattern) {
            missing.push(*method);
        }
    }

    let known_missing: Vec<&str> = vec![];

    let unexpected_missing: Vec<&&str> = missing.iter().filter(|m| !known_missing.contains(m)).collect();

    assert!(
        unexpected_missing.is_empty(),
        "Unexpected missing Rust delegate methods: {:?}\n\
         Known missing (tracked): {:?}",
        unexpected_missing,
        known_missing
    );

    for method in &known_missing {
        assert!(
            missing.contains(method),
            "Method '{}' is in known_missing but is now present -- remove from known_missing",
            method,
        );
    }
}

// ============================================================================
// 4. TypeScript generator: input types per model
// ============================================================================

#[test]
fn ts_input_types_per_model() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    for model in &schema.models {
        for suffix in REFERENCE_TS_INPUT_TYPES {
            let type_name = format!("{}{}", model.name, suffix);
            assert!(code.contains(&type_name), "Missing TS input type: {}", type_name);
        }
    }
}

// ============================================================================
// 5. TypeScript generator: global client methods
// ============================================================================

#[test]
fn ts_global_client_methods() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    let mut missing = Vec::new();
    for method in REFERENCE_GLOBAL_METHODS {
        // Use method name without trailing '(' because some methods have
        // generic parameters, e.g. $transaction<T>(...).
        if !code.contains(method) {
            missing.push(*method);
        }
    }

    let known_missing: Vec<&str> = vec![];

    let unexpected_missing: Vec<&&str> = missing.iter().filter(|m| !known_missing.contains(m)).collect();

    assert!(
        unexpected_missing.is_empty(),
        "Unexpected missing global methods: {:?}\n\
         Known missing (tracked): {:?}",
        unexpected_missing,
        known_missing
    );

    for method in &known_missing {
        assert!(
            missing.contains(method),
            "'{}' is in known_missing but now present -- remove from known_missing",
            method,
        );
    }
}

// ============================================================================
// 6. TypeScript scalar type mapping
// ============================================================================

#[test]
fn ts_scalar_type_mapping() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    // Check User model interface for correct type mappings
    let user_section = code
        .split("export interface User {")
        .nth(1)
        .and_then(|s| s.split('}').next())
        .expect("User interface not found");

    for (prisma_type, expected_ts) in REFERENCE_SCALAR_MAPPING_TS {
        // Find a field with this type and verify its TS mapping
        let field_name = match *prisma_type {
            "Int" => "id",
            "String" => "email",
            "Boolean" => "isActive",
            "DateTime" => "createdAt",
            "Float" => "balance",
            "Json" => "metadata",
            _ => continue,
        };

        let field_line = user_section.lines().find(|l| l.contains(&format!("{}:", field_name)));

        if let Some(line) = field_line {
            assert!(
                line.contains(expected_ts),
                "Scalar mapping for {} (field '{}'): expected '{}', got line: {}",
                prisma_type,
                field_name,
                expected_ts,
                line.trim()
            );
        }
    }
}

// ============================================================================
// 7. Rust scalar type mapping
// ============================================================================

#[test]
fn rs_scalar_type_mapping() {
    let schema = ir();
    let code = RustGenerator::generate(&schema).unwrap();

    let user_section = code
        .split("pub struct User {")
        .nth(1)
        .and_then(|s| s.split('}').next())
        .expect("User struct not found");

    for (prisma_type, expected_rs) in REFERENCE_SCALAR_MAPPING_RS {
        let field_name = match *prisma_type {
            "Int" => "id",
            "BigInt" => continue, // no BigInt field in test schema
            "Float" => "balance",
            "Decimal" => continue, // no Decimal field in test schema
            "String" => "email",
            "Boolean" => "is_active",
            "DateTime" => "created_at",
            "Json" => "metadata",
            "Bytes" => continue, // no Bytes field in test schema
            _ => continue,
        };

        let field_line = user_section.lines().find(|l| l.contains(&format!("{}:", field_name)));

        if let Some(line) = field_line {
            assert!(
                line.contains(expected_rs),
                "Rust scalar mapping for {} (field '{}'): expected '{}', got line: {}",
                prisma_type,
                field_name,
                expected_rs,
                line.trim()
            );
        }
    }
}

// ============================================================================
// 8. JSON protocol action names
// ============================================================================

#[test]
fn json_protocol_action_names() {
    use prisma_client::{Operation, QueryBuilder};

    // Verify our Operation enum produces the correct JSON protocol action strings
    let checks: Vec<(Operation, &str)> = vec![
        (Operation::FindMany, "findMany"),
        (Operation::FindUnique, "findUnique"),
        (Operation::FindFirst, "findFirst"),
        (Operation::FindFirstOrThrow, "findFirstOrThrow"),
        (Operation::FindUniqueOrThrow, "findUniqueOrThrow"),
        (Operation::CreateOne, "createOne"),
        (Operation::CreateMany, "createMany"),
        (Operation::CreateManyAndReturn, "createManyAndReturn"),
        (Operation::UpdateOne, "updateOne"),
        (Operation::UpdateMany, "updateMany"),
        (Operation::UpdateManyAndReturn, "updateManyAndReturn"),
        (Operation::DeleteOne, "deleteOne"),
        (Operation::DeleteMany, "deleteMany"),
        (Operation::UpsertOne, "upsertOne"),
        (Operation::Aggregate, "aggregate"),
        (Operation::GroupBy, "groupBy"),
        (Operation::ExecuteRaw, "executeRaw"),
        (Operation::QueryRaw, "queryRaw"),
    ];

    for (op, expected_action) in &checks {
        let qb = QueryBuilder::new("Test", *op);
        let json = qb.build();
        let actual_action = json["action"].as_str().unwrap();
        assert_eq!(
            actual_action, *expected_action,
            "Operation {:?} should produce action '{}'",
            op, expected_action
        );
    }

    // Verify against the reference mapping -- all actions should be covered
    for (ts_method, json_action) in REFERENCE_JSON_ACTIONS {
        let found = checks.iter().any(|(_, a)| a == json_action);
        assert!(
            found,
            "Reference JSON action '{}' (from TS method '{}') not in our Operation enum",
            json_action, ts_method
        );
    }
}

// ============================================================================
// 9. TS generator: model delegates on PrismaClient
// ============================================================================

#[test]
fn ts_client_has_model_delegates() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    // Each model should appear as a camelCase property on PrismaClient
    assert!(code.contains("user: UserDelegate;"), "Missing user delegate");
    assert!(code.contains("post: PostDelegate;"), "Missing post delegate");
    assert!(code.contains("profile: ProfileDelegate;"), "Missing profile delegate");
    assert!(code.contains("tag: TagDelegate;"), "Missing tag delegate");
}

// ============================================================================
// 10. Rust generator: model delegates on PrismaClient
// ============================================================================

#[test]
fn rs_client_has_model_delegates() {
    let schema = ir();
    let code = RustGenerator::generate(&schema).unwrap();

    assert!(
        code.contains("pub fn user(&self) -> UserDelegate"),
        "Missing user() method"
    );
    assert!(
        code.contains("pub fn post(&self) -> PostDelegate"),
        "Missing post() method"
    );
    assert!(
        code.contains("pub fn profile(&self) -> ProfileDelegate"),
        "Missing profile() method"
    );
    assert!(
        code.contains("pub fn tag(&self) -> TagDelegate"),
        "Missing tag() method"
    );
}

// ============================================================================
// 11. TS generator: enum types
// ============================================================================

#[test]
fn ts_enum_types_generated() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    assert!(code.contains("export const Role = {"));
    assert!(code.contains("USER: 'USER',"));
    assert!(code.contains("ADMIN: 'ADMIN',"));
    assert!(code.contains("MODERATOR: 'MODERATOR',"));
    assert!(code.contains("export type Role = (typeof Role)[keyof typeof Role];"));
}

// ============================================================================
// 12. Rust generator: enum types
// ============================================================================

#[test]
fn rs_enum_types_generated() {
    let schema = ir();
    let code = RustGenerator::generate(&schema).unwrap();

    assert!(code.contains("pub enum Role {"));
    assert!(code.contains("USER,"));
    assert!(code.contains("ADMIN,"));
    assert!(code.contains("MODERATOR,"));
    assert!(code.contains("impl std::fmt::Display for Role {"));
}

// ============================================================================
// 13. TS generator: WhereInput has AND/OR/NOT
// ============================================================================

#[test]
fn ts_where_input_has_logical_operators() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    let user_where = code
        .split("export interface UserWhereInput {")
        .nth(1)
        .and_then(|s| s.split('}').next())
        .expect("UserWhereInput not found");

    assert!(user_where.contains("AND?:"), "WhereInput missing AND");
    assert!(user_where.contains("OR?:"), "WhereInput missing OR");
    assert!(user_where.contains("NOT?:"), "WhereInput missing NOT");
}

// ============================================================================
// 14. TS generator: CreateInput excludes auto-generated fields
// ============================================================================

#[test]
fn ts_create_input_excludes_auto_fields() {
    let schema = ir();
    let code = TypeScriptGenerator::generate(&schema).unwrap();

    let user_create = code
        .split("export interface UserCreateInput {")
        .nth(1)
        .and_then(|s| s.split('}').next())
        .expect("UserCreateInput not found");

    assert!(
        !user_create.lines().any(|l| l.trim().starts_with("id")),
        "id should not be in CreateInput (autoincrement)"
    );
    assert!(user_create.contains("email:"), "email should be in CreateInput");
}

// ============================================================================
// 15. Rust generator: CreateInput excludes auto-generated fields
// ============================================================================

#[test]
fn rs_create_input_excludes_auto_fields() {
    let schema = ir();
    let code = RustGenerator::generate(&schema).unwrap();

    let user_create = code
        .split("pub struct UserCreateInput {")
        .nth(1)
        .and_then(|s| s.split('}').next())
        .expect("UserCreateInput not found");

    assert!(
        !user_create.lines().any(|l| l.trim().starts_with("pub id")),
        "id should not be in CreateInput (autoincrement)"
    );
    assert!(
        user_create.contains("pub email: String"),
        "email should be in CreateInput"
    );
}

// ============================================================================
// 16. Selection modes
// ============================================================================

#[test]
fn selection_modes_work() {
    use prisma_client::Selection;

    // select mode: explicit fields only, no $scalars
    let sel = Selection::select().field("id").field("email");
    let json = sel.build();
    assert_eq!(json["id"], true);
    assert_eq!(json["email"], true);
    assert!(json["$scalars"].is_null(), "select mode should not have $scalars");

    // include mode: $scalars + $composites + explicit relations
    let nested = Selection::scalars();
    let sel = Selection::include().relation("posts", nested);
    let json = sel.build();
    assert_eq!(json["$scalars"], true);
    assert_eq!(json["$composites"], true);
    assert!(json["posts"].is_object());

    // omit mode: $scalars + $composites, named fields set to false
    let sel = Selection::omit().field("password");
    let json = sel.build();
    assert_eq!(json["$scalars"], true);
    assert_eq!(json["password"], false);

    // default scalars mode
    let sel = Selection::scalars();
    let json = sel.build();
    assert_eq!(json["$scalars"], true);
}

// ============================================================================
// 17. Cross-generator consistency: both generators produce same models
// ============================================================================

#[test]
fn both_generators_cover_same_models() {
    let schema = ir();
    let ts_code = TypeScriptGenerator::generate(&schema).unwrap();
    let rs_code = RustGenerator::generate(&schema).unwrap();

    for model in &schema.models {
        // TS has interface
        assert!(
            ts_code.contains(&format!("export interface {} {{", model.name)),
            "TS missing model interface: {}",
            model.name
        );
        // Rust has struct
        assert!(
            rs_code.contains(&format!("pub struct {} {{", model.name)),
            "Rust missing model struct: {}",
            model.name
        );
        // TS has delegate
        assert!(
            ts_code.contains(&format!("export interface {}Delegate {{", model.name)),
            "TS missing delegate: {}",
            model.name
        );
        // Rust has delegate
        assert!(
            rs_code.contains(&format!("pub struct {}Delegate", model.name)),
            "Rust missing delegate: {}",
            model.name
        );
    }
}

// ============================================================================
// 18. Cross-generator consistency: both generators produce same enums
// ============================================================================

#[test]
fn both_generators_cover_same_enums() {
    let schema = ir();
    let ts_code = TypeScriptGenerator::generate(&schema).unwrap();
    let rs_code = RustGenerator::generate(&schema).unwrap();

    for e in &schema.enums {
        assert!(
            ts_code.contains(&format!("export const {} = {{", e.name)),
            "TS missing enum: {}",
            e.name
        );
        assert!(
            rs_code.contains(&format!("pub enum {} {{", e.name)),
            "Rust missing enum: {}",
            e.name
        );

        for v in &e.values {
            assert!(
                ts_code.contains(&v.name),
                "TS missing enum value: {}.{}",
                e.name,
                v.name
            );
            assert!(
                rs_code.contains(&v.name),
                "Rust missing enum value: {}.{}",
                e.name,
                v.name
            );
        }
    }
}

// ============================================================================
// 19. Discrepancy summary: comprehensive gap report
// ============================================================================

#[test]
fn print_discrepancy_summary() {
    let schema = ir();
    let ts_code = TypeScriptGenerator::generate(&schema).unwrap();
    let rs_code = RustGenerator::generate(&schema).unwrap();

    let mut gaps: Vec<String> = Vec::new();

    // Check TS delegate methods
    for method in REFERENCE_TS_DELEGATE_METHODS {
        if !ts_code.contains(&format!("{}(", method)) {
            gaps.push(format!("TS delegate missing: {}", method));
        }
    }

    // Check Rust delegate methods
    for method in REFERENCE_RUST_DELEGATE_METHODS {
        if !rs_code.contains(&format!("fn {}(", method)) {
            gaps.push(format!("Rust delegate missing: {}", method));
        }
    }

    // Check global methods (don't require trailing '(' due to generics like $transaction<T>)
    for method in REFERENCE_GLOBAL_METHODS {
        if !ts_code.contains(method) {
            gaps.push(format!("TS global missing: {}", method));
        }
    }

    // Check Operation enum coverage against reference JSON actions
    {
        use prisma_client::Operation;
        let all_ops = vec![
            Operation::FindMany,
            Operation::FindUnique,
            Operation::FindFirst,
            Operation::CreateOne,
            Operation::CreateMany,
            Operation::CreateManyAndReturn,
            Operation::UpdateOne,
            Operation::UpdateMany,
            Operation::UpdateManyAndReturn,
            Operation::DeleteOne,
            Operation::DeleteMany,
            Operation::UpsertOne,
            Operation::Aggregate,
            Operation::GroupBy,
            Operation::Count,
            Operation::FindFirstOrThrow,
            Operation::FindUniqueOrThrow,
            Operation::ExecuteRaw,
            Operation::QueryRaw,
        ];
        let op_actions: Vec<&str> = all_ops
            .iter()
            .map(|op| {
                let qb = prisma_client::QueryBuilder::new("_", *op);
                let json = qb.build();
                // Leak is fine in tests
                let s: &str = Box::leak(json["action"].as_str().unwrap().to_string().into_boxed_str());
                s
            })
            .collect();
        for (_ts_method, json_action) in REFERENCE_JSON_ACTIONS {
            if !op_actions.contains(json_action) {
                gaps.push(format!("Operation enum missing: {}", json_action));
            }
        }
    }

    // This test always passes -- it prints the gap report for visibility.
    // When all gaps are closed, the list will be empty.
    if !gaps.is_empty() {
        eprintln!("\n=== DISCREPANCY REPORT ({} gaps) ===", gaps.len());
        for (i, gap) in gaps.iter().enumerate() {
            eprintln!("  {}. {}", i + 1, gap);
        }
        eprintln!("=== END REPORT ===\n");
    }

    // Fail if there are UNEXPECTED gaps (not in known list)
    let known_gaps: Vec<&str> = vec![];

    let unexpected: Vec<&String> = gaps.iter().filter(|g| !known_gaps.contains(&g.as_str())).collect();

    assert!(
        unexpected.is_empty(),
        "Unexpected discrepancies found: {:?}",
        unexpected
    );
}
