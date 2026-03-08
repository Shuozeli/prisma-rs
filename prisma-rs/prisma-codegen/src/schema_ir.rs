//! Schema intermediate representation.
//!
//! Parses a Prisma schema into a language-neutral IR that code generators
//! consume. Decouples PSL walker API from code emission.

use psl::parser_database::{ScalarFieldType, ScalarType, walkers};

/// The top-level schema IR containing all models and enums.
#[derive(Debug, Clone)]
pub struct SchemaIR {
    pub provider: String,
    pub models: Vec<ModelIR>,
    pub enums: Vec<EnumIR>,
}

/// A single model in the schema.
#[derive(Debug, Clone)]
pub struct ModelIR {
    pub name: String,
    pub db_name: String,
    pub fields: Vec<ModelField>,
    pub primary_key: Vec<String>,
    pub unique_constraints: Vec<Vec<String>>,
}

/// A model field: either scalar or relation.
#[derive(Debug, Clone)]
pub enum ModelField {
    Scalar(ScalarField),
    Relation(RelationField),
}

impl ModelField {
    pub fn name(&self) -> &str {
        match self {
            ModelField::Scalar(f) => &f.name,
            ModelField::Relation(f) => &f.name,
        }
    }
}

/// A scalar (non-relation) field.
#[derive(Debug, Clone)]
pub struct ScalarField {
    pub name: String,
    pub db_name: String,
    pub scalar_kind: ScalarKind,
    pub arity: FieldArity,
    pub is_id: bool,
    pub is_unique: bool,
    pub is_updated_at: bool,
    pub is_autoincrement: bool,
    pub default: Option<FieldDefault>,
    pub native_type: Option<String>,
}

/// A relation field (reference to another model).
#[derive(Debug, Clone)]
pub struct RelationField {
    pub name: String,
    pub related_model: String,
    pub relation_kind: RelationKind,
    pub arity: FieldArity,
    /// Foreign key fields on this side (e.g., `authorId` in `@relation(fields: [authorId])`).
    pub fk_fields: Vec<String>,
    /// Referenced fields on the other side (e.g., `id` in `references: [id]`).
    pub references: Vec<String>,
}

/// Scalar type classification (language-neutral).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarKind {
    Int,
    BigInt,
    Float,
    Decimal,
    String,
    Boolean,
    DateTime,
    Json,
    Bytes,
    Uuid,
    Enum(usize),
    Unsupported,
}

/// Field cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldArity {
    Required,
    Optional,
    List,
}

/// Default value expression.
#[derive(Debug, Clone)]
pub enum FieldDefault {
    Autoincrement,
    Now,
    Uuid,
    Cuid,
    DbGenerated(String),
    Value(serde_json::Value),
}

/// Relation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationKind {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

/// An enum definition.
#[derive(Debug, Clone)]
pub struct EnumIR {
    pub name: String,
    pub db_name: String,
    pub values: Vec<EnumValueIR>,
}

/// A single enum value.
#[derive(Debug, Clone)]
pub struct EnumValueIR {
    pub name: String,
    pub db_name: String,
}

impl SchemaIR {
    /// Parse a Prisma schema string into the intermediate representation.
    pub fn from_schema(schema_str: &str) -> Result<Self, crate::CodegenError> {
        let connector_registry: psl::ConnectorRegistry<'_> = &[
            psl::builtin_connectors::POSTGRES,
            psl::builtin_connectors::MYSQL,
            psl::builtin_connectors::SQLITE,
        ];

        let validated = psl::parse_without_validation(
            schema_str.into(),
            connector_registry,
            &psl::parser_database::NoExtensionTypes,
        );

        let db = &validated.db;

        // Extract provider from datasource
        let provider = validated
            .configuration
            .datasources
            .first()
            .map(|ds| ds.active_provider.to_string())
            .unwrap_or_else(|| "postgresql".into());

        // Build enum index (name -> index) for ScalarKind::Enum references
        let enums: Vec<EnumIR> = db
            .walk_enums()
            .map(|e| EnumIR {
                name: e.name().to_string(),
                db_name: e.database_name().to_string(),
                values: e
                    .values()
                    .map(|v| EnumValueIR {
                        name: v.name().to_string(),
                        db_name: v.database_name().to_string(),
                    })
                    .collect(),
            })
            .collect();

        let enum_names: Vec<String> = enums.iter().map(|e| e.name.clone()).collect();

        // Build models
        let models: Vec<ModelIR> = db
            .walk_models()
            .map(|model| build_model_ir(model, &enum_names))
            .collect::<Result<Vec<_>, _>>()
            .map_err(crate::CodegenError::Generation)?;

        let ir = SchemaIR {
            provider,
            models,
            enums,
        };
        ir.validate_identifiers().map_err(crate::CodegenError::Generation)?;
        Ok(ir)
    }

    /// Validate that all identifiers are safe for code generation.
    ///
    /// While the PSL parser already validates identifiers, this provides
    /// defense-in-depth against code injection through malformed names.
    fn validate_identifiers(&self) -> Result<(), String> {
        fn check(name: &str, context: &str) -> Result<(), String> {
            if name.is_empty() {
                return Err(format!("Empty identifier in {context}"));
            }
            if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(format!(
                    "Invalid identifier '{name}' in {context}: must be alphanumeric or underscore"
                ));
            }
            if name.starts_with(|c: char| c.is_ascii_digit()) {
                return Err(format!(
                    "Invalid identifier '{name}' in {context}: must not start with a digit"
                ));
            }
            Ok(())
        }
        for model in &self.models {
            check(&model.name, "model name")?;
            for field in &model.fields {
                match field {
                    ModelField::Scalar(sf) => check(&sf.name, &format!("field in {}", model.name))?,
                    ModelField::Relation(rf) => check(&rf.name, &format!("relation in {}", model.name))?,
                }
            }
        }
        for e in &self.enums {
            check(&e.name, "enum name")?;
            for v in &e.values {
                check(&v.name, &format!("enum value in {}", e.name))?;
            }
        }
        Ok(())
    }
}

fn build_model_ir(model: walkers::ModelWalker<'_>, enum_names: &[String]) -> Result<ModelIR, String> {
    let mut fields = Vec::new();

    // Scalar fields
    for sf in model.scalar_fields() {
        fields.push(ModelField::Scalar(build_scalar_field(sf, enum_names)?));
    }

    // Relation fields
    for rf in model.relation_fields() {
        fields.push(ModelField::Relation(build_relation_field(rf)));
    }

    // Primary key
    let primary_key: Vec<String> = model
        .primary_key()
        .map(|pk| pk.fields().map(|f| f.name().to_string()).collect())
        .unwrap_or_default();

    // Unique constraints
    let unique_constraints: Vec<Vec<String>> = model
        .indexes()
        .filter(|idx| idx.is_unique())
        .map(|idx| {
            idx.scalar_field_attributes()
                .filter_map(|f| f.as_index_field().as_scalar_field())
                .map(|f| f.name().to_string())
                .collect()
        })
        .collect();

    Ok(ModelIR {
        name: model.name().to_string(),
        db_name: model.database_name().to_string(),
        fields,
        primary_key,
        unique_constraints,
    })
}

fn build_scalar_field(sf: walkers::ScalarFieldWalker<'_>, enum_names: &[String]) -> Result<ScalarField, String> {
    let scalar_kind = match sf.scalar_field_type() {
        ScalarFieldType::BuiltInScalar(st) => match st {
            ScalarType::Int => ScalarKind::Int,
            ScalarType::BigInt => ScalarKind::BigInt,
            ScalarType::Float => ScalarKind::Float,
            ScalarType::Decimal => ScalarKind::Decimal,
            ScalarType::String => ScalarKind::String,
            ScalarType::Boolean => ScalarKind::Boolean,
            ScalarType::DateTime => ScalarKind::DateTime,
            ScalarType::Json => ScalarKind::Json,
            ScalarType::Bytes => ScalarKind::Bytes,
        },
        ScalarFieldType::Enum(enum_id) => {
            // Find the enum index
            let enum_walker = sf.walk(enum_id);
            let enum_name = enum_walker.name();
            match enum_names.iter().position(|n| n == enum_name) {
                Some(idx) => ScalarKind::Enum(idx),
                None => {
                    return Err(format!(
                        "Enum '{}' referenced by field but not found in enum list",
                        enum_name
                    ));
                }
            }
        }
        _ => ScalarKind::Unsupported,
    };

    let arity = if sf.is_optional() {
        FieldArity::Optional
    } else if sf.ast_field().arity.is_list() {
        FieldArity::List
    } else {
        FieldArity::Required
    };

    let default = sf.default_value().and_then(|d| {
        if d.is_autoincrement() {
            Some(FieldDefault::Autoincrement)
        } else if d.is_now() {
            Some(FieldDefault::Now)
        } else if d.is_uuid() {
            Some(FieldDefault::Uuid)
        } else if d.is_cuid() {
            Some(FieldDefault::Cuid)
        } else if d.is_dbgenerated() {
            Some(FieldDefault::DbGenerated(String::new()))
        } else {
            None
        }
    });

    let native_type = sf.raw_native_type().map(|(_, name, _, _)| name.to_string());

    Ok(ScalarField {
        name: sf.name().to_string(),
        db_name: sf.database_name().to_string(),
        scalar_kind,
        arity,
        is_id: sf.is_single_pk(),
        is_unique: sf.is_unique(),
        is_updated_at: sf.is_updated_at(),
        is_autoincrement: sf.default_value().map(|d| d.is_autoincrement()).unwrap_or(false),
        default,
        native_type,
    })
}

fn build_relation_field(rf: walkers::RelationFieldWalker<'_>) -> RelationField {
    let arity = if rf.ast_field().arity.is_optional() {
        FieldArity::Optional
    } else if rf.ast_field().arity.is_list() {
        FieldArity::List
    } else {
        FieldArity::Required
    };

    let relation_kind = if rf.ast_field().arity.is_list() {
        if rf
            .opposite_relation_field()
            .map(|o| o.ast_field().arity.is_list())
            .unwrap_or(false)
        {
            RelationKind::ManyToMany
        } else {
            RelationKind::OneToMany
        }
    } else if rf
        .opposite_relation_field()
        .map(|o| o.ast_field().arity.is_list())
        .unwrap_or(false)
    {
        RelationKind::ManyToOne
    } else {
        RelationKind::OneToOne
    };

    let fk_fields: Vec<String> = rf
        .fields()
        .into_iter()
        .flat_map(|fields| fields.map(|f| f.name().to_string()))
        .collect();

    let references: Vec<String> = rf
        .referenced_fields()
        .into_iter()
        .flat_map(|fields| fields.map(|f| f.name().to_string()))
        .collect();

    RelationField {
        name: rf.name().to_string(),
        related_model: rf.related_model().name().to_string(),
        relation_kind,
        arity,
        fk_fields,
        references,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SCHEMA: &str = r#"
        datasource db {
            provider = "postgresql"
        }

        model User {
            id    Int    @id @default(autoincrement())
            email String @unique
            name  String?
            posts Post[]
        }

        model Post {
            id       Int    @id @default(autoincrement())
            title    String
            authorId Int
            author   User   @relation(fields: [authorId], references: [id])
        }
    "#;

    #[test]
    fn parse_schema_ir() {
        let ir = SchemaIR::from_schema(TEST_SCHEMA).unwrap();
        assert_eq!(ir.provider, "postgresql");
        assert_eq!(ir.models.len(), 2);

        let user = &ir.models[0];
        assert_eq!(user.name, "User");
        assert_eq!(user.primary_key, vec!["id"]);
    }

    #[test]
    fn scalar_fields_parsed() {
        let ir = SchemaIR::from_schema(TEST_SCHEMA).unwrap();
        let user = &ir.models[0];

        let scalars: Vec<&ScalarField> = user
            .fields
            .iter()
            .filter_map(|f| match f {
                ModelField::Scalar(sf) => Some(sf),
                _ => None,
            })
            .collect();

        assert_eq!(scalars.len(), 3); // id, email, name

        let id_field = scalars.iter().find(|f| f.name == "id").unwrap();
        assert!(id_field.is_id);
        assert!(id_field.is_autoincrement);
        assert_eq!(id_field.scalar_kind, ScalarKind::Int);
        assert_eq!(id_field.arity, FieldArity::Required);

        let email_field = scalars.iter().find(|f| f.name == "email").unwrap();
        assert!(email_field.is_unique);
        assert_eq!(email_field.scalar_kind, ScalarKind::String);

        let name_field = scalars.iter().find(|f| f.name == "name").unwrap();
        assert_eq!(name_field.arity, FieldArity::Optional);
    }

    #[test]
    fn relation_fields_parsed() {
        let ir = SchemaIR::from_schema(TEST_SCHEMA).unwrap();

        // User has a relation to Post[]
        let user = &ir.models[0];
        let user_rels: Vec<&RelationField> = user
            .fields
            .iter()
            .filter_map(|f| match f {
                ModelField::Relation(rf) => Some(rf),
                _ => None,
            })
            .collect();
        assert_eq!(user_rels.len(), 1);
        assert_eq!(user_rels[0].name, "posts");
        assert_eq!(user_rels[0].related_model, "Post");
        assert_eq!(user_rels[0].arity, FieldArity::List);

        // Post has a relation to User
        let post = &ir.models[1];
        let post_rels: Vec<&RelationField> = post
            .fields
            .iter()
            .filter_map(|f| match f {
                ModelField::Relation(rf) => Some(rf),
                _ => None,
            })
            .collect();
        assert_eq!(post_rels.len(), 1);
        assert_eq!(post_rels[0].name, "author");
        assert_eq!(post_rels[0].related_model, "User");
        assert_eq!(post_rels[0].fk_fields, vec!["authorId"]);
        assert_eq!(post_rels[0].references, vec!["id"]);
    }

    #[test]
    fn unique_constraints_parsed() {
        let ir = SchemaIR::from_schema(TEST_SCHEMA).unwrap();
        let user = &ir.models[0];
        // email has @unique, so there should be a single-field unique constraint
        assert!(user.unique_constraints.iter().any(|c| c == &["email"]));
    }

    #[test]
    fn enum_schema() {
        let schema = r#"
            datasource db {
                provider = "postgresql"
            }

            enum Role {
                USER
                ADMIN
            }

            model User {
                id   Int  @id
                role Role
            }
        "#;

        let ir = SchemaIR::from_schema(schema).unwrap();
        assert_eq!(ir.enums.len(), 1);
        assert_eq!(ir.enums[0].name, "Role");
        assert_eq!(ir.enums[0].values.len(), 2);
        assert_eq!(ir.enums[0].values[0].name, "USER");
        assert_eq!(ir.enums[0].values[1].name, "ADMIN");

        let user = &ir.models[0];
        let role_field = user.fields.iter().find(|f| f.name() == "role").unwrap();
        match role_field {
            ModelField::Scalar(sf) => {
                assert_eq!(sf.scalar_kind, ScalarKind::Enum(0));
            }
            _ => panic!("Expected scalar field"),
        }
    }
}
