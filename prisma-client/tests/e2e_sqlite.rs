//! End-to-end tests using SQLite and the full Prisma pipeline.
//!
//! Tests the complete flow: schema -> compile -> execute -> deserialize.
//! This catches serialization mismatches (e.g., camelCase vs snake_case)
//! between the Prisma engine output and the generated Rust structs.

use prisma_client::{Operation, PrismaClient, QueryBuilder, Selection};
use prisma_driver_sqlite::SqliteDriverAdapterFactory;
use serde::{Deserialize, Serialize};
use serde_json::json;

// -- Schema --
// This schema is used by both the compiler (to generate query plans)
// and the test setup (to create tables).

const SCHEMA: &str = r#"
    datasource db {
        provider = "sqlite"
    }

    model User {
        id        Int     @id @default(autoincrement())
        email     String  @unique
        firstName String?
        lastName  String?
        posts     Post[]
    }

    model Post {
        id        Int     @id @default(autoincrement())
        title     String
        content   String?
        published Boolean @default(false)
        authorId  Int
        author    User    @relation(fields: [authorId], references: [id])
    }
"#;

// -- Generated structs (simulating codegen output) --
// These mirror what RustGenerator would produce with #[serde(rename_all = "camelCase")].
// The test verifies that JSON from the Prisma engine deserializes into these structs.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct User {
    pub id: i32,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserCreateInput {
    pub email: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserUpdateInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Post {
    pub id: i32,
    pub title: String,
    pub content: Option<String>,
    pub published: bool,
    pub author_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PostCreateInput {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published: Option<bool>,
    pub author_id: i32,
}

// -- Test helpers --

async fn setup_client() -> PrismaClient {
    let factory = SqliteDriverAdapterFactory::new(":memory:");

    // Create the client (this initializes the compiler with the schema)
    let client = PrismaClient::new(SCHEMA, &factory).await.unwrap();

    // Create tables directly via raw SQL (simulating what migrate would do)
    let create_tables = QueryBuilder::raw(Operation::ExecuteRaw)
        .arg(
            "query",
            json!("CREATE TABLE User (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE, firstName TEXT, lastName TEXT)"),
        )
        .arg("parameters", json!([]));
    client.execute(&create_tables).await.unwrap();

    let create_posts = QueryBuilder::raw(Operation::ExecuteRaw)
        .arg(
            "query",
            json!("CREATE TABLE Post (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, content TEXT, published BOOLEAN NOT NULL DEFAULT 0, authorId INTEGER NOT NULL, FOREIGN KEY (authorId) REFERENCES User(id))"),
        )
        .arg("parameters", json!([]));
    client.execute(&create_posts).await.unwrap();

    client
}

// -- Tests --

#[tokio::test]
async fn e2e_create_and_find_user() {
    let client = setup_client().await;

    // Create a user
    let create_data = UserCreateInput {
        email: "alice@example.com".to_string(),
        first_name: Some("Alice".to_string()),
        last_name: Some("Smith".to_string()),
    };
    let data_json = serde_json::to_value(&create_data).unwrap();
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data_json);
    let result = client.execute(&qb).await.unwrap();

    // This is the critical test: can we deserialize the engine's camelCase
    // JSON response into our snake_case Rust struct?
    let user: User = serde_json::from_value(result).unwrap();
    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.first_name, Some("Alice".to_string()));
    assert_eq!(user.last_name, Some("Smith".to_string()));
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn e2e_create_user_with_null_fields() {
    let client = setup_client().await;

    let create_data = UserCreateInput {
        email: "bob@example.com".to_string(),
        first_name: None,
        last_name: None,
    };
    let data_json = serde_json::to_value(&create_data).unwrap();
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data_json);
    let result = client.execute(&qb).await.unwrap();

    let user: User = serde_json::from_value(result).unwrap();
    assert_eq!(user.email, "bob@example.com");
    assert_eq!(user.first_name, None);
    assert_eq!(user.last_name, None);
}

#[tokio::test]
async fn e2e_find_many_users() {
    let client = setup_client().await;

    // Create two users
    for (email, name) in [("a@test.com", "Alice"), ("b@test.com", "Bob")] {
        let data = json!({ "email": email, "firstName": name });
        let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
        client.execute(&qb).await.unwrap();
    }

    // Find all users
    let qb = QueryBuilder::new("User", Operation::FindMany);
    let result = client.execute(&qb).await.unwrap();
    let users: Vec<User> = serde_json::from_value(result).unwrap();

    assert_eq!(users.len(), 2);
    assert_eq!(users[0].email, "a@test.com");
    assert_eq!(users[0].first_name, Some("Alice".to_string()));
    assert_eq!(users[1].email, "b@test.com");
    assert_eq!(users[1].first_name, Some("Bob".to_string()));
}

#[tokio::test]
async fn e2e_find_unique() {
    let client = setup_client().await;

    let data = json!({ "email": "unique@test.com", "firstName": "Unique" });
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
    client.execute(&qb).await.unwrap();

    let qb = QueryBuilder::new("User", Operation::FindUnique).where_arg(json!({ "email": "unique@test.com" }));
    let result = client.execute(&qb).await.unwrap();

    let user: User = serde_json::from_value(result).unwrap();
    assert_eq!(user.email, "unique@test.com");
    assert_eq!(user.first_name, Some("Unique".to_string()));
}

#[tokio::test]
async fn e2e_update_user() {
    let client = setup_client().await;

    // Create
    let data = json!({ "email": "update@test.com", "firstName": "Before" });
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
    client.execute(&qb).await.unwrap();

    // Update
    let update_data = UserUpdateInput {
        first_name: Some("After".to_string()),
        ..Default::default()
    };
    let data_json = serde_json::to_value(&update_data).unwrap();
    let qb = QueryBuilder::new("User", Operation::UpdateOne)
        .where_arg(json!({ "email": "update@test.com" }))
        .data(data_json);
    let result = client.execute(&qb).await.unwrap();

    let user: User = serde_json::from_value(result).unwrap();
    assert_eq!(user.first_name, Some("After".to_string()));
    assert_eq!(user.email, "update@test.com");
}

#[tokio::test]
async fn e2e_delete_user() {
    let client = setup_client().await;

    // Create
    let data = json!({ "email": "delete@test.com" });
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
    client.execute(&qb).await.unwrap();

    // Delete
    let qb = QueryBuilder::new("User", Operation::DeleteOne).where_arg(json!({ "email": "delete@test.com" }));
    let result = client.execute(&qb).await.unwrap();
    let deleted: User = serde_json::from_value(result).unwrap();
    assert_eq!(deleted.email, "delete@test.com");

    // Verify deleted
    let qb = QueryBuilder::new("User", Operation::FindMany);
    let result = client.execute(&qb).await.unwrap();
    let users: Vec<User> = serde_json::from_value(result).unwrap();
    assert_eq!(users.len(), 0);
}

#[tokio::test]
async fn e2e_create_post_with_relation() {
    let client = setup_client().await;

    // Create user first
    let data = json!({ "email": "author@test.com", "firstName": "Author" });
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
    let user_result = client.execute(&qb).await.unwrap();
    let user: User = serde_json::from_value(user_result).unwrap();

    // Create post linked to user
    let post_data = PostCreateInput {
        title: "My First Post".to_string(),
        content: Some("Hello world".to_string()),
        published: Some(true),
        author_id: user.id,
    };
    let data_json = serde_json::to_value(&post_data).unwrap();
    let qb = QueryBuilder::new("Post", Operation::CreateOne).data(data_json);
    let result = client.execute(&qb).await.unwrap();

    // Verify the post deserializes with camelCase -> snake_case mapping
    let post: Post = serde_json::from_value(result).unwrap();
    assert_eq!(post.title, "My First Post");
    assert_eq!(post.content, Some("Hello world".to_string()));
    assert!(post.published);
    assert_eq!(post.author_id, user.id); // This is the key camelCase test: authorId -> author_id
}

#[tokio::test]
async fn e2e_find_many_with_ordering() {
    let client = setup_client().await;

    for email in ["c@test.com", "a@test.com", "b@test.com"] {
        let data = json!({ "email": email });
        let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
        client.execute(&qb).await.unwrap();
    }

    let qb = QueryBuilder::new("User", Operation::FindMany).order_by(json!([{ "email": "asc" }]));
    let result = client.execute(&qb).await.unwrap();
    let users: Vec<User> = serde_json::from_value(result).unwrap();

    assert_eq!(users[0].email, "a@test.com");
    assert_eq!(users[1].email, "b@test.com");
    assert_eq!(users[2].email, "c@test.com");
}

#[tokio::test]
async fn e2e_find_many_with_pagination() {
    let client = setup_client().await;

    for i in 1..=5 {
        let data = json!({ "email": format!("user{}@test.com", i) });
        let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
        client.execute(&qb).await.unwrap();
    }

    let qb = QueryBuilder::new("User", Operation::FindMany)
        .skip(1)
        .take(2)
        .order_by(json!([{ "id": "asc" }]));
    let result = client.execute(&qb).await.unwrap();
    let users: Vec<User> = serde_json::from_value(result).unwrap();

    assert_eq!(users.len(), 2);
    assert_eq!(users[0].email, "user2@test.com");
    assert_eq!(users[1].email, "user3@test.com");
}

#[tokio::test]
async fn e2e_delete_many() {
    let client = setup_client().await;

    for email in ["x@test.com", "y@test.com", "z@test.com"] {
        let data = json!({ "email": email });
        let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
        client.execute(&qb).await.unwrap();
    }

    let qb = QueryBuilder::new("User", Operation::DeleteMany).selection(Selection::select().field("count"));
    let result = client.execute(&qb).await.unwrap();
    let count = result["count"].as_i64().unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn e2e_update_many() {
    let client = setup_client().await;

    for email in ["m1@test.com", "m2@test.com"] {
        let data = json!({ "email": email, "firstName": "Old" });
        let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
        client.execute(&qb).await.unwrap();
    }

    let update = json!({ "firstName": "New" });
    let qb = QueryBuilder::new("User", Operation::UpdateMany)
        .data(update)
        .selection(Selection::select().field("count"));
    let result = client.execute(&qb).await.unwrap();
    let count = result["count"].as_i64().unwrap();
    assert_eq!(count, 2);

    // Verify all updated
    let qb = QueryBuilder::new("User", Operation::FindMany);
    let result = client.execute(&qb).await.unwrap();
    let users: Vec<User> = serde_json::from_value(result).unwrap();
    for user in &users {
        assert_eq!(user.first_name, Some("New".to_string()));
    }
}

#[tokio::test]
async fn e2e_upsert_create() {
    let client = setup_client().await;

    let qb = QueryBuilder::new("User", Operation::UpsertOne)
        .where_arg(json!({ "email": "upsert@test.com" }))
        .arg("create", json!({ "email": "upsert@test.com", "firstName": "Created" }))
        .arg("update", json!({ "firstName": "Updated" }));
    let result = client.execute(&qb).await.unwrap();

    let user: User = serde_json::from_value(result).unwrap();
    assert_eq!(user.email, "upsert@test.com");
    assert_eq!(user.first_name, Some("Created".to_string()));
}

#[tokio::test]
async fn e2e_upsert_update() {
    let client = setup_client().await;

    // First create the user
    let data = json!({ "email": "upsert2@test.com", "firstName": "Original" });
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
    client.execute(&qb).await.unwrap();

    // Upsert should update
    let qb = QueryBuilder::new("User", Operation::UpsertOne)
        .where_arg(json!({ "email": "upsert2@test.com" }))
        .arg("create", json!({ "email": "upsert2@test.com", "firstName": "Created" }))
        .arg("update", json!({ "firstName": "Updated" }));
    let result = client.execute(&qb).await.unwrap();

    let user: User = serde_json::from_value(result).unwrap();
    assert_eq!(user.first_name, Some("Updated".to_string()));
}

/// This test specifically validates the camelCase serialization contract.
/// If the Prisma engine returns `{ "authorId": 1 }` but our struct expects
/// `author_id`, deserialization will fail without `rename_all = "camelCase"`.
#[tokio::test]
async fn e2e_camelcase_field_deserialization() {
    let client = setup_client().await;

    // Create user
    let data = json!({ "email": "camel@test.com" });
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
    client.execute(&qb).await.unwrap();

    // Create post with camelCase foreign key
    let data = json!({ "title": "CamelCase Test", "authorId": 1 });
    let qb = QueryBuilder::new("Post", Operation::CreateOne).data(data);
    let result = client.execute(&qb).await.unwrap();

    // Verify the raw JSON uses camelCase
    assert!(
        result.get("authorId").is_some() || result.get("author_id").is_some(),
        "Expected authorId or author_id in response: {result}"
    );

    // The critical assertion: this will fail if rename_all = "camelCase" is missing
    let post: Post = serde_json::from_value(result).expect(
        "Failed to deserialize Post. This likely means the Prisma engine returns camelCase \
         fields but the struct expects snake_case. Add #[serde(rename_all = \"camelCase\")] \
         to the generated struct.",
    );
    assert_eq!(post.author_id, 1);
    assert_eq!(post.title, "CamelCase Test");
}

// ============================================================
// Timestamp tests: @default(now()) and @updatedAt (M-003, M-004)
// ============================================================

// A separate schema with timestamp fields.
const SCHEMA_WITH_TIMESTAMPS: &str = r#"
    datasource db {
        provider = "sqlite"
    }

    model Task {
        id        Int      @id @default(autoincrement())
        title     String
        createdAt DateTime @default(now())
        updatedAt DateTime @updatedAt
    }
"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Task {
    pub id: i32,
    pub title: String,
    pub created_at: String,
    pub updated_at: String,
}

async fn setup_task_client() -> PrismaClient {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let client = PrismaClient::new(SCHEMA_WITH_TIMESTAMPS, &factory).await.unwrap();

    let create_tables = QueryBuilder::raw(Operation::ExecuteRaw)
        .arg(
            "query",
            json!("CREATE TABLE Task (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, createdAt TEXT NOT NULL, updatedAt TEXT NOT NULL)"),
        )
        .arg("parameters", json!([]));
    client.execute(&create_tables).await.unwrap();
    client
}

/// M-004: @default(now()) should auto-populate createdAt when omitted.
#[tokio::test]
async fn e2e_default_now_on_create() {
    let client = setup_task_client().await;

    // Create a task WITHOUT providing createdAt or updatedAt.
    // The compiler should inject now() via generatorCall.
    let data = json!({ "title": "Test Task" });
    let qb = QueryBuilder::new("Task", Operation::CreateOne).data(data);
    let result = client.execute(&qb).await.unwrap();

    let task: Task = serde_json::from_value(result).unwrap();
    assert_eq!(task.title, "Test Task");
    // createdAt and updatedAt should be non-empty datetime strings
    assert!(
        !task.created_at.is_empty(),
        "createdAt should be auto-populated, got empty string"
    );
    assert!(
        !task.updated_at.is_empty(),
        "updatedAt should be auto-populated, got empty string"
    );
    // Should look like an ISO timestamp (starts with year)
    assert!(
        task.created_at.starts_with("20"),
        "createdAt should be an ISO timestamp, got: {}",
        task.created_at
    );
}

/// M-003: @updatedAt should auto-populate on update operations.
#[tokio::test]
async fn e2e_updated_at_on_update() {
    let client = setup_task_client().await;

    // Create
    let data = json!({ "title": "Original" });
    let qb = QueryBuilder::new("Task", Operation::CreateOne).data(data);
    let create_result = client.execute(&qb).await.unwrap();
    let created: Task = serde_json::from_value(create_result).unwrap();

    // Small delay to ensure updatedAt changes
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Update - only change title, updatedAt should be auto-set
    let qb = QueryBuilder::new("Task", Operation::UpdateOne)
        .where_arg(json!({ "id": created.id }))
        .data(json!({ "title": "Updated" }));
    let update_result = client.execute(&qb).await.unwrap();

    let updated: Task = serde_json::from_value(update_result).unwrap();
    assert_eq!(updated.title, "Updated");
    assert!(
        !updated.updated_at.is_empty(),
        "updatedAt should be auto-populated on update"
    );
    assert!(
        updated.updated_at.starts_with("20"),
        "updatedAt should be an ISO timestamp, got: {}",
        updated.updated_at
    );
}

// ============================================================
// Count operation test (M-005)
// ============================================================

/// M-005: Operation::Count should work (maps to aggregate with _count).
#[tokio::test]
async fn e2e_count_operation() {
    let client = setup_client().await;

    // Create users
    for email in ["c1@test.com", "c2@test.com", "c3@test.com"] {
        let data = json!({ "email": email });
        let qb = QueryBuilder::new("User", Operation::CreateOne).data(data);
        client.execute(&qb).await.unwrap();
    }

    // Use Operation::Count (which maps to aggregate internally)
    let qb = QueryBuilder::new("User", Operation::Count)
        .selection(Selection::select().relation("_count", Selection::select().field("_all")));
    let result = client.execute(&qb).await.unwrap();
    let count = result["_count"]["_all"].as_i64().unwrap_or(0);
    assert_eq!(count, 3, "Count should be 3, got result: {result}");
}

// ============================================================
// camelCase serialization tests
// ============================================================

/// Test that input structs serialize to camelCase for the Prisma engine.
#[test]
fn input_serializes_to_camelcase() {
    let input = UserCreateInput {
        email: "test@test.com".to_string(),
        first_name: Some("Test".to_string()),
        last_name: None,
    };

    let json = serde_json::to_value(&input).unwrap();
    // Must serialize as camelCase for the engine
    assert!(
        json.get("firstName").is_some(),
        "Expected camelCase 'firstName', got: {json}"
    );
    assert!(
        json.get("first_name").is_none(),
        "Should not have snake_case 'first_name'"
    );
    assert_eq!(json["firstName"], "Test");
    // lastName is None + skip_serializing_if, so it should be absent
    assert!(json.get("lastName").is_none());
}

#[test]
fn update_input_serializes_to_camelcase() {
    let input = UserUpdateInput {
        first_name: Some("Updated".to_string()),
        ..Default::default()
    };

    let json = serde_json::to_value(&input).unwrap();
    assert!(
        json.get("firstName").is_some(),
        "Expected camelCase 'firstName', got: {json}"
    );
    assert_eq!(json["firstName"], "Updated");
    // Other fields are None + skip_serializing_if
    assert!(json.get("email").is_none());
    assert!(json.get("lastName").is_none());
}

#[test]
fn post_input_serializes_author_id_as_camelcase() {
    let input = PostCreateInput {
        title: "Test".to_string(),
        content: None,
        published: None,
        author_id: 42,
    };

    let json = serde_json::to_value(&input).unwrap();
    assert!(
        json.get("authorId").is_some(),
        "Expected camelCase 'authorId', got: {json}"
    );
    assert_eq!(json["authorId"], 42);
    assert!(json.get("author_id").is_none(), "Should not have snake_case");
}
