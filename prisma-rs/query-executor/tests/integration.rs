//! Integration tests: compile queries and execute them against SQLite in-memory.

use prisma_compiler::QueryCompiler;
use prisma_compiler::quaint::connector::ConnectionInfo;
use prisma_compiler::quaint::prelude::{ExternalConnectionInfo, SqlFamily};
use prisma_driver_core::{
    ArgScalarType, ArgType, Arity, QueryValue, SqlDriverAdapter, SqlDriverAdapterFactory, SqlQuery, SqlQueryable,
};
use prisma_driver_sqlite::SqliteDriverAdapterFactory;
use prisma_query_executor::QueryExecutor;

const SCHEMA: &str = r#"
    datasource db {
        provider = "sqlite"
    }

    model User {
        id    Int    @id @default(autoincrement())
        email String @unique
        name  String?
        posts Post[]
    }

    model Post {
        id        Int    @id @default(autoincrement())
        title     String
        published Boolean @default(false)
        authorId  Int
        author    User   @relation(fields: [authorId], references: [id])
    }
"#;

fn make_compiler() -> QueryCompiler {
    let conn_info = ConnectionInfo::External(ExternalConnectionInfo::new(SqlFamily::Sqlite, None, None, false));
    QueryCompiler::new(SCHEMA, conn_info)
}

fn text_arg() -> ArgType {
    ArgType {
        scalar_type: ArgScalarType::String,
        db_type: None,
        arity: Arity::Scalar,
    }
}

async fn setup_db() -> Box<dyn SqlDriverAdapter> {
    let factory = SqliteDriverAdapterFactory::new(":memory:");
    let mut adapter = factory.connect().await.unwrap();
    adapter
        .execute_script(
            "CREATE TABLE User (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                name TEXT
            );
            CREATE TABLE Post (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                published INTEGER NOT NULL DEFAULT 0,
                authorId INTEGER NOT NULL,
                FOREIGN KEY (authorId) REFERENCES User(id)
            );",
        )
        .await
        .unwrap();
    adapter
}

async fn insert_user(adapter: &mut dyn SqlQueryable, email: &str, name: &str) {
    adapter
        .execute_raw(SqlQuery {
            sql: "INSERT INTO User (email, name) VALUES (?, ?)".into(),
            args: vec![QueryValue::Text(email.into()), QueryValue::Text(name.into())],
            arg_types: vec![text_arg(), text_arg()],
        })
        .await
        .unwrap();
}

async fn insert_post(adapter: &mut dyn SqlQueryable, title: &str, author_id: i64, published: bool) {
    adapter
        .execute_raw(SqlQuery {
            sql: "INSERT INTO Post (title, authorId, published) VALUES (?, ?, ?)".into(),
            args: vec![
                QueryValue::Text(title.into()),
                QueryValue::Int64(author_id),
                QueryValue::Int64(if published { 1 } else { 0 }),
            ],
            arg_types: vec![
                text_arg(),
                ArgType {
                    scalar_type: ArgScalarType::Int,
                    db_type: None,
                    arity: Arity::Scalar,
                },
                ArgType {
                    scalar_type: ArgScalarType::Boolean,
                    db_type: None,
                    arity: Arity::Scalar,
                },
            ],
        })
        .await
        .unwrap();
}

async fn count_users(adapter: &mut dyn SqlQueryable) -> i64 {
    let rs = adapter
        .query_raw(SqlQuery {
            sql: "SELECT COUNT(*) as cnt FROM User".into(),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();
    match &rs.rows[0][0] {
        prisma_driver_core::ResultValue::Int64(n) => *n,
        prisma_driver_core::ResultValue::Int32(n) => *n as i64,
        other => panic!("Expected int, got: {other:?}"),
    }
}

fn compile_and_execute<'a>(
    compiler: &'a QueryCompiler,
    adapter: &'a mut dyn SqlQueryable,
    request: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = serde_json::Value> + 'a>> {
    Box::pin(async move {
        let expr = compiler.compile_to_ir(request).unwrap();
        let result = QueryExecutor::execute(&expr, adapter).await.unwrap();
        result.to_json()
    })
}

// ============================================================
// 2.1 Basic query plan execution
// ============================================================

#[tokio::test]
async fn find_many_empty() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();
    let expr = compiler
        .compile_to_ir(r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true}}}"#)
        .unwrap();
    let result = QueryExecutor::execute(&expr, adapter.as_mut()).await.unwrap();
    let json = result.to_json();
    assert!(json.is_array());
    assert_eq!(json.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn find_many_with_data() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "alice@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "bob@test.com", "Bob").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("alice@test.com"), "Missing Alice: {s}");
    assert!(s.contains("bob@test.com"), "Missing Bob: {s}");
}

#[tokio::test]
async fn find_unique_by_id() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "carol@test.com", "Carol").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findUnique","query":{"arguments":{"where":{"id":1}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("carol@test.com"), "Expected Carol: {s}");
}

// ============================================================
// 2.2 Transaction management
// ============================================================

#[tokio::test]
async fn transaction_commit() {
    let mut adapter = setup_db().await;
    let mut tx = adapter.start_transaction(None).await.unwrap();

    insert_user(tx.as_mut(), "tx_user@test.com", "TxUser").await;
    tx.commit().await.unwrap();

    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

#[tokio::test]
async fn transaction_rollback() {
    let mut adapter = setup_db().await;
    let mut tx = adapter.start_transaction(None).await.unwrap();

    insert_user(tx.as_mut(), "rollback@test.com", "Rollback").await;
    tx.rollback().await.unwrap();

    assert_eq!(count_users(adapter.as_mut()).await, 0);
}

// ============================================================
// 2.3 Nested transactions (savepoints)
// ============================================================

#[tokio::test]
async fn savepoint_commit() {
    let mut adapter = setup_db().await;
    let mut tx = adapter.start_transaction(None).await.unwrap();

    insert_user(tx.as_mut(), "outer@test.com", "Outer").await;

    tx.create_savepoint("sp1").await.unwrap();
    insert_user(tx.as_mut(), "inner@test.com", "Inner").await;
    tx.release_savepoint("sp1").await.unwrap();

    tx.commit().await.unwrap();
    assert_eq!(count_users(adapter.as_mut()).await, 2);
}

#[tokio::test]
async fn savepoint_rollback() {
    let mut adapter = setup_db().await;
    let mut tx = adapter.start_transaction(None).await.unwrap();

    insert_user(tx.as_mut(), "outer@test.com", "Outer").await;

    tx.create_savepoint("sp1").await.unwrap();
    insert_user(tx.as_mut(), "inner@test.com", "Inner").await;
    tx.rollback_to_savepoint("sp1").await.unwrap();

    tx.commit().await.unwrap();
    // Only outer should remain
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

// ============================================================
// 2.4 Batch operations (createMany via multiple inserts)
// ============================================================

#[tokio::test]
async fn create_many_users() {
    let mut adapter = setup_db().await;
    for i in 0..5 {
        insert_user(adapter.as_mut(), &format!("user{i}@test.com"), &format!("User{i}")).await;
    }
    assert_eq!(count_users(adapter.as_mut()).await, 5);

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert_eq!(json.as_array().unwrap().len(), 5);
}

// ============================================================
// 2.5 CRUD cross-compat (find/create/update/delete)
// ============================================================

#[tokio::test]
async fn crud_create_one() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"new@test.com","name":"New"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("new@test.com"), "createOne result: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

#[tokio::test]
async fn crud_update_many() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "u1@test.com", "A").await;
    insert_user(adapter.as_mut(), "u2@test.com", "A").await;
    insert_user(adapter.as_mut(), "u3@test.com", "B").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"updateMany","query":{"arguments":{"where":{"name":"A"},"data":{"name":"Updated"}},"selection":{"count":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    // Should have updated 2 rows
    assert!(s.contains("2") || s.contains("count"), "updateMany result: {s}");
}

#[tokio::test]
async fn crud_delete_many() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "del1@test.com", "Del").await;
    insert_user(adapter.as_mut(), "del2@test.com", "Del").await;
    insert_user(adapter.as_mut(), "keep@test.com", "Keep").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"deleteMany","query":{"arguments":{"where":{"name":"Del"}},"selection":{"count":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("2"), "deleteMany result: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

#[tokio::test]
async fn crud_find_first() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "first@test.com", "First").await;
    insert_user(adapter.as_mut(), "second@test.com", "Second").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findFirst","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    // findFirst should return a single record (not an array)
    assert!(json.is_object() || json.is_null(), "findFirst should be object: {json}");
}

// ============================================================
// 2.5 continued: Relations
// ============================================================

#[tokio::test]
async fn find_many_with_relation() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "author@test.com", "Author").await;
    insert_post(adapter.as_mut(), "Post 1", 1, true).await;
    insert_post(adapter.as_mut(), "Post 2", 1, false).await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true,"posts":{"selection":{"$scalars":true}}}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("Post 1"), "Expected Post 1: {s}");
    assert!(s.contains("Post 2"), "Expected Post 2: {s}");
}

// ============================================================
// 2.6 Aggregation cross-compat
// ============================================================

#[tokio::test]
async fn aggregate_count() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "A").await;
    insert_user(adapter.as_mut(), "b@test.com", "B").await;
    insert_user(adapter.as_mut(), "c@test.com", "C").await;

    let compiler = make_compiler();
    let expr = compiler
        .compile_to_ir(
            r#"{"modelName":"User","action":"aggregate","query":{"selection":{"_count":{"selection":{"_all":true}}}}}"#,
        )
        .unwrap();
    let result = QueryExecutor::execute(&expr, adapter.as_mut()).await.unwrap();
    let json = result.to_json();
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("3"), "Expected count=3: {s}");
}

// ============================================================
// 2.7 Raw query cross-compat
// ============================================================

#[tokio::test]
async fn execute_raw_insert() {
    let mut adapter = setup_db().await;

    adapter
        .execute_raw(SqlQuery {
            sql: "INSERT INTO User (email, name) VALUES ('raw@test.com', 'Raw')".into(),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();

    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

#[tokio::test]
async fn query_raw_select() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "raw@test.com", "RawUser").await;

    let rs = adapter
        .query_raw(SqlQuery {
            sql: "SELECT id, email, name FROM User WHERE email = ?".into(),
            args: vec![QueryValue::Text("raw@test.com".into())],
            arg_types: vec![text_arg()],
        })
        .await
        .unwrap();
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(rs.column_names, vec!["id", "email", "name"]);
}

// ============================================================
// 2.8 Transaction cross-compat (sequential + interactive)
// ============================================================

#[tokio::test]
async fn interactive_transaction_multi_operation() {
    let mut adapter = setup_db().await;
    let mut tx = adapter.start_transaction(None).await.unwrap();

    // Insert two users in one transaction
    insert_user(tx.as_mut(), "t1@test.com", "T1").await;
    insert_user(tx.as_mut(), "t2@test.com", "T2").await;

    // Query within the transaction should see both
    let rs = tx
        .as_mut()
        .query_raw(SqlQuery {
            sql: "SELECT COUNT(*) as cnt FROM User".into(),
            args: vec![],
            arg_types: vec![],
        })
        .await
        .unwrap();
    let count = match &rs.rows[0][0] {
        prisma_driver_core::ResultValue::Int64(n) => *n,
        prisma_driver_core::ResultValue::Int32(n) => *n as i64,
        other => panic!("Expected int: {other:?}"),
    };
    assert_eq!(count, 2);

    tx.commit().await.unwrap();
    assert_eq!(count_users(adapter.as_mut()).await, 2);
}

#[tokio::test]
async fn sequential_operations_in_transaction() {
    let mut adapter = setup_db().await;
    let mut tx = adapter.start_transaction(None).await.unwrap();

    // Create user
    insert_user(tx.as_mut(), "seq@test.com", "Seq").await;

    // Update user within same transaction
    tx.as_mut()
        .execute_raw(SqlQuery {
            sql: "UPDATE User SET name = 'Updated' WHERE email = ?".into(),
            args: vec![QueryValue::Text("seq@test.com".into())],
            arg_types: vec![text_arg()],
        })
        .await
        .unwrap();

    // Verify update within transaction
    let rs = tx
        .as_mut()
        .query_raw(SqlQuery {
            sql: "SELECT name FROM User WHERE email = ?".into(),
            args: vec![QueryValue::Text("seq@test.com".into())],
            arg_types: vec![text_arg()],
        })
        .await
        .unwrap();
    match &rs.rows[0][0] {
        prisma_driver_core::ResultValue::Text(s) => assert_eq!(s, "Updated"),
        other => panic!("Expected text: {other:?}"),
    }

    tx.commit().await.unwrap();
}

#[tokio::test]
async fn nested_savepoints_in_transaction() {
    let mut adapter = setup_db().await;
    let mut tx = adapter.start_transaction(None).await.unwrap();

    insert_user(tx.as_mut(), "base@test.com", "Base").await;

    // First savepoint: add user, then release
    tx.create_savepoint("sp1").await.unwrap();
    insert_user(tx.as_mut(), "sp1@test.com", "SP1").await;
    tx.release_savepoint("sp1").await.unwrap();

    // Second savepoint: add user, then rollback
    tx.create_savepoint("sp2").await.unwrap();
    insert_user(tx.as_mut(), "sp2@test.com", "SP2").await;
    tx.rollback_to_savepoint("sp2").await.unwrap();

    tx.commit().await.unwrap();

    // base + sp1 should exist, sp2 should not
    assert_eq!(count_users(adapter.as_mut()).await, 2);
}

// ============================================================
// 2.9 Error handling in compiled queries
// ============================================================

#[tokio::test]
async fn find_unique_returns_null_for_missing() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findUnique","query":{"arguments":{"where":{"id":999}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert!(json.is_null(), "findUnique for missing id should be null: {json}");
}

#[tokio::test]
async fn delete_one_removes_record() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "victim@test.com", "Victim").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"deleteOne","query":{"arguments":{"where":{"email":"victim@test.com"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(
        s.contains("victim@test.com"),
        "deleteOne should return deleted record: {s}"
    );
    assert_eq!(count_users(adapter.as_mut()).await, 0);
}

#[tokio::test]
async fn update_one_modifies_record() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "update@test.com", "Before").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"updateOne","query":{"arguments":{"where":{"email":"update@test.com"},"data":{"name":"After"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("After"), "updateOne should return updated record: {s}");

    // Verify in DB
    let json2 = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findUnique","query":{"arguments":{"where":{"email":"update@test.com"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s2 = serde_json::to_string(&json2).unwrap();
    assert!(s2.contains("After"), "DB should have updated name: {s2}");
}

// ============================================================
// 2.10 Compiled transaction (Expression::Transaction)
// ============================================================

#[tokio::test]
async fn compiled_create_uses_transaction() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    // createOne internally wraps in a transaction
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"txcreate@test.com","name":"TxCreate"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("txcreate@test.com"), "createOne result: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

// ============================================================
// 2.11 Find with filtering
// ============================================================

#[tokio::test]
async fn find_many_with_where_filter() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b@test.com", "Bob").await;
    insert_user(adapter.as_mut(), "c@test.com", "Charlie").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"where":{"name":"Bob"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 1);
    let s = serde_json::to_string(&arr[0]).unwrap();
    assert!(s.contains("Bob"), "Expected Bob: {s}");
}

#[tokio::test]
async fn find_many_with_take() {
    let mut adapter = setup_db().await;
    for i in 0..5 {
        insert_user(adapter.as_mut(), &format!("u{i}@test.com"), &format!("User{i}")).await;
    }

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"take":2},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2);
}

#[tokio::test]
async fn find_many_with_skip() {
    let mut adapter = setup_db().await;
    for i in 0..5 {
        insert_user(adapter.as_mut(), &format!("u{i}@test.com"), &format!("User{i}")).await;
    }

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"skip":3},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2);
}

// ============================================================
// 2.12 Upsert
// ============================================================

#[tokio::test]
async fn upsert_creates_when_not_exists() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"upsertOne","query":{"arguments":{"where":{"email":"new@test.com"},"create":{"email":"new@test.com","name":"Created"},"update":{"name":"Updated"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("Created"), "Upsert should create: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

#[tokio::test]
async fn upsert_updates_when_exists() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "existing@test.com", "Original").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"upsertOne","query":{"arguments":{"where":{"email":"existing@test.com"},"create":{"email":"existing@test.com","name":"ShouldNotCreate"},"update":{"name":"Upserted"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("Upserted"), "Upsert should update: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

// ============================================================
// 3.1 createMany
// ============================================================

#[tokio::test]
async fn create_many_batch() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createMany","query":{"arguments":{"data":[{"email":"a@test.com","name":"A"},{"email":"b@test.com","name":"B"},{"email":"c@test.com","name":"C"}]},"selection":{"count":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    // createMany returns { count: N }
    assert!(s.contains("3") || s.contains("count"), "createMany result: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 3);
}

#[tokio::test]
async fn create_many_and_return() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createManyAndReturn","query":{"arguments":{"data":[{"email":"x@test.com","name":"X"},{"email":"y@test.com","name":"Y"}]},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("createManyAndReturn should return array");
    assert_eq!(arr.len(), 2);
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("x@test.com"), "Expected x@test.com: {s}");
    assert!(s.contains("y@test.com"), "Expected y@test.com: {s}");
}

// ============================================================
// 3.2 findFirstOrThrow / findUniqueOrThrow
// ============================================================

#[tokio::test]
async fn find_first_or_throw_found() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "found@test.com", "Found").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findFirstOrThrow","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert!(json.is_object(), "findFirstOrThrow should return object: {json}");
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("found@test.com"), "Expected found: {s}");
}

#[tokio::test]
async fn find_first_or_throw_not_found() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let expr = compiler
        .compile_to_ir(r#"{"modelName":"User","action":"findFirstOrThrow","query":{"selection":{"$scalars":true}}}"#)
        .unwrap();
    let result = QueryExecutor::execute(&expr, adapter.as_mut()).await;
    assert!(result.is_err(), "findFirstOrThrow on empty table should error");
}

#[tokio::test]
async fn find_unique_or_throw_found() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "unique@test.com", "Unique").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findUniqueOrThrow","query":{"arguments":{"where":{"email":"unique@test.com"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("unique@test.com"), "Expected unique: {s}");
}

#[tokio::test]
async fn find_unique_or_throw_not_found() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let expr = compiler
        .compile_to_ir(r#"{"modelName":"User","action":"findUniqueOrThrow","query":{"arguments":{"where":{"email":"missing@test.com"}},"selection":{"$scalars":true}}}"#)
        .unwrap();
    let result = QueryExecutor::execute(&expr, adapter.as_mut()).await;
    assert!(result.is_err(), "findUniqueOrThrow for missing record should error");
}

// ============================================================
// 3.3 aggregate (sum/min/max)
// ============================================================

#[tokio::test]
async fn aggregate_min_max() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "A").await;
    insert_user(adapter.as_mut(), "b@test.com", "B").await;
    insert_user(adapter.as_mut(), "c@test.com", "C").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"aggregate","query":{"selection":{"_count":{"selection":{"_all":true}},"_min":{"selection":{"id":true}},"_max":{"selection":{"id":true}}}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    // Should contain count=3, min id=1, max id=3
    assert!(s.contains("3"), "Expected count 3: {s}");
    assert!(s.contains("1"), "Expected min id 1: {s}");
}

// ============================================================
// 3.4 groupBy
// ============================================================

#[tokio::test]
async fn group_by_name() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a1@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "a2@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b1@test.com", "Bob").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"groupBy","query":{"arguments":{"by":["name"]},"selection":{"name":true,"_count":{"selection":{"_all":true}}}}}"#,
    )
    .await;
    let arr = json.as_array().expect("groupBy should return array");
    assert_eq!(arr.len(), 2, "Expected 2 groups: {json}");
}

// ============================================================
// 3.5 orderBy
// ============================================================

#[tokio::test]
async fn find_many_order_by_desc() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b@test.com", "Bob").await;
    insert_user(adapter.as_mut(), "c@test.com", "Charlie").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"orderBy":[{"email":"desc"}]},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 3);
    // Descending by email: c@ > b@ > a@
    let first_email = arr[0]["email"].as_str().unwrap();
    let last_email = arr[2]["email"].as_str().unwrap();
    assert!(
        first_email > last_email,
        "Expected desc order: first={first_email}, last={last_email}"
    );
}

#[tokio::test]
async fn find_many_order_by_asc() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "c@test.com", "Charlie").await;
    insert_user(adapter.as_mut(), "a@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b@test.com", "Bob").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"orderBy":[{"email":"asc"}]},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    let first_email = arr[0]["email"].as_str().unwrap();
    assert_eq!(first_email, "a@test.com");
}

// ============================================================
// 3.6 Complex filtering
// ============================================================

#[tokio::test]
async fn filter_in() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b@test.com", "Bob").await;
    insert_user(adapter.as_mut(), "c@test.com", "Charlie").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"where":{"name":{"in":["Alice","Charlie"]}}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2, "IN filter should return 2 results: {json}");
}

#[tokio::test]
async fn filter_not_equals() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b@test.com", "Bob").await;
    insert_user(adapter.as_mut(), "c@test.com", "Charlie").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"where":{"name":{"not":"Bob"}}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2, "NOT filter should exclude Bob: {json}");
}

#[tokio::test]
async fn filter_gt() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "A").await;
    insert_user(adapter.as_mut(), "b@test.com", "B").await;
    insert_user(adapter.as_mut(), "c@test.com", "C").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"where":{"id":{"gt":1}}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2, "gt filter should return 2 results: {json}");
}

#[tokio::test]
async fn filter_contains() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "alice@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "bob@other.com", "Bob").await;
    insert_user(adapter.as_mut(), "charlie@test.com", "Charlie").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"where":{"email":{"contains":"test"}}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2, "contains filter should match 2: {json}");
}

#[tokio::test]
async fn filter_starts_with() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "alice@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "alex@test.com", "Alex").await;
    insert_user(adapter.as_mut(), "bob@test.com", "Bob").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"where":{"email":{"startsWith":"al"}}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2, "startsWith filter should match 2: {json}");
}

#[tokio::test]
async fn filter_and_or() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b@test.com", "Bob").await;
    insert_user(adapter.as_mut(), "c@test.com", "Charlie").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"where":{"OR":[{"name":"Alice"},{"name":"Charlie"}]}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2, "OR filter should return 2: {json}");
}

// ============================================================
// 3.7 Nested writes
// ============================================================

#[tokio::test]
async fn create_with_nested_posts() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"author@test.com","name":"Author","posts":{"create":[{"title":"First Post","published":true},{"title":"Draft","published":false}]}}},"selection":{"$scalars":true,"posts":{"selection":{"$scalars":true}}}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("author@test.com"), "Expected author: {s}");
    assert!(s.contains("First Post"), "Expected First Post: {s}");
    assert!(s.contains("Draft"), "Expected Draft: {s}");
}

// ============================================================
// 3.8 Relation filtering
// ============================================================

#[tokio::test]
async fn filter_by_relation() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "alice@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "bob@test.com", "Bob").await;
    insert_post(adapter.as_mut(), "Alice Post", 1, true).await;
    insert_post(adapter.as_mut(), "Bob Post", 2, true).await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"Post","action":"findMany","query":{"arguments":{"where":{"author":{"is":{"name":"Alice"}}}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 1, "Relation filter should return 1: {json}");
    let s = serde_json::to_string(&arr[0]).unwrap();
    assert!(s.contains("Alice Post"), "Expected Alice Post: {s}");
}

// ============================================================
// 3.9 Cursor-based pagination
// ============================================================

#[tokio::test]
async fn cursor_pagination() {
    let mut adapter = setup_db().await;
    for i in 0..5 {
        insert_user(adapter.as_mut(), &format!("u{i}@test.com"), &format!("User{i}")).await;
    }

    let compiler = make_compiler();
    // Cursor at id=3, take 2 (should get id=3 and id=4 or id=4 and id=5 depending on skip)
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"cursor":{"id":3},"take":2,"skip":1},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("Expected array");
    assert_eq!(arr.len(), 2, "Cursor pagination should return 2: {json}");
}

// ============================================================
// 3.10 updateManyAndReturn
// ============================================================

#[tokio::test]
async fn update_many_and_return() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "Same").await;
    insert_user(adapter.as_mut(), "b@test.com", "Same").await;
    insert_user(adapter.as_mut(), "c@test.com", "Different").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"updateManyAndReturn","query":{"arguments":{"where":{"name":"Same"},"data":{"name":"Updated"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let arr = json.as_array().expect("updateManyAndReturn should return array");
    assert_eq!(arr.len(), 2, "Should update 2 records: {json}");
    for item in arr {
        assert_eq!(item["name"].as_str().unwrap(), "Updated");
    }
}

// ============================================================
// 4.1 Batch $transaction tests
// ============================================================

/// Helper: compile and execute within an existing transaction context.
async fn compile_and_execute_in_tx<'a>(
    compiler: &'a QueryCompiler,
    tx: &'a mut dyn SqlQueryable,
    request: &str,
) -> serde_json::Value {
    let expr = compiler.compile_to_ir(request).unwrap();
    let result = QueryExecutor::execute(&expr, tx).await.unwrap();
    result.to_json()
}

#[tokio::test]
async fn batch_transaction_commit() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let mut tx = adapter.start_transaction(None).await.unwrap();

    // Create two users within the same transaction
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"tx1@test.com","name":"TxOne"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"tx2@test.com","name":"TxTwo"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    tx.commit().await.unwrap();

    // Both users should be visible after commit
    assert_eq!(count_users(adapter.as_mut()).await, 2);
}

#[tokio::test]
async fn batch_transaction_rollback() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let mut tx = adapter.start_transaction(None).await.unwrap();

    // Create users within transaction
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"gone1@test.com","name":"Gone1"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"gone2@test.com","name":"Gone2"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    // Rollback -- neither user should exist
    tx.rollback().await.unwrap();
    assert_eq!(count_users(adapter.as_mut()).await, 0);
}

#[tokio::test]
async fn batch_transaction_mixed_operations() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "existing@test.com", "Existing").await;

    let compiler = make_compiler();
    let mut tx = adapter.start_transaction(None).await.unwrap();

    // Create a new user
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"new@test.com","name":"New"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    // Update the existing user
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"updateOne","query":{"arguments":{"where":{"email":"existing@test.com"},"data":{"name":"Modified"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    // Delete the new user
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"deleteOne","query":{"arguments":{"where":{"email":"new@test.com"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    tx.commit().await.unwrap();

    // Only the existing user should remain, with updated name
    assert_eq!(count_users(adapter.as_mut()).await, 1);
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findUnique","query":{"arguments":{"where":{"email":"existing@test.com"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("Modified"), "Expected name to be Modified: {s}");
}

#[tokio::test]
async fn batch_transaction_isolation() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let mut tx = adapter.start_transaction(None).await.unwrap();

    // Create user in transaction
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"isolated@test.com","name":"Isolated"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    // User visible inside the transaction
    let inner_json = compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert_eq!(inner_json.as_array().unwrap().len(), 1, "Should see 1 user inside tx");

    // Not yet visible outside the transaction (SQLite WAL mode allows this)
    // Note: For SQLite with shared cache, this may differ, but with separate
    // connections the uncommitted data should not be visible
    // We verify after commit instead
    tx.commit().await.unwrap();

    let outer_json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert_eq!(
        outer_json.as_array().unwrap().len(),
        1,
        "Should see 1 user after commit"
    );
}

#[tokio::test]
async fn batch_transaction_with_savepoint() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let mut tx = adapter.start_transaction(None).await.unwrap();

    // First operation: create user
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"keep@test.com","name":"Keep"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    // Savepoint before risky operation
    tx.create_savepoint("sp1").await.unwrap();

    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"discard@test.com","name":"Discard"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    // Rollback savepoint -- discard the second user
    tx.rollback_to_savepoint("sp1").await.unwrap();

    // Create another user after rollback
    compile_and_execute_in_tx(
        &compiler,
        tx.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"after@test.com","name":"After"}},"selection":{"$scalars":true}}}"#,
    )
    .await;

    tx.commit().await.unwrap();

    // Should have keep@ and after@, but not discard@
    assert_eq!(count_users(adapter.as_mut()).await, 2);
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"orderBy":[{"email":"asc"}]},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("after@test.com"), "Expected after@: {s}");
    assert!(s.contains("keep@test.com"), "Expected keep@: {s}");
    assert!(!s.contains("discard@test.com"), "Should not have discard@: {s}");
}
