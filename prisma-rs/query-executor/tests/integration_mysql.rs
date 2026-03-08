//! End-to-end integration tests: compile + execute against MySQL.
//!
//! Requires a running MySQL instance (see docker-compose.yml).
//! Set MYSQL_TEST_URL to override the default connection string.

use prisma_compiler::QueryCompiler;
use prisma_compiler::quaint::connector::ConnectionInfo;
use prisma_compiler::quaint::prelude::{ExternalConnectionInfo, SqlFamily};
use prisma_driver_core::{
    ArgScalarType, ArgType, Arity, DatabaseUrl, QueryValue, SqlDriverAdapter, SqlDriverAdapterFactory, SqlQuery,
    SqlQueryable,
};
use prisma_driver_mysql::MySqlDriverAdapterFactory;
use prisma_query_executor::QueryExecutor;
use serial_test::serial;

const MYSQL_SCHEMA: &str = r#"
    datasource db {
        provider = "mysql"
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

fn connection_url() -> DatabaseUrl {
    let raw =
        std::env::var("MYSQL_TEST_URL").unwrap_or_else(|_| "mysql://prisma:prisma@127.0.0.1:13306/prisma_test".into());
    DatabaseUrl::parse(&raw).expect("invalid MYSQL_TEST_URL")
}

fn make_compiler() -> QueryCompiler {
    let conn_info = ConnectionInfo::External(ExternalConnectionInfo::new(SqlFamily::Mysql, None, None, true));
    QueryCompiler::new(MYSQL_SCHEMA, conn_info)
}

async fn setup_db() -> Box<dyn SqlDriverAdapter> {
    let factory = MySqlDriverAdapterFactory::new(connection_url());
    let mut adapter = factory.connect().await.unwrap();
    adapter.execute_script("DROP TABLE IF EXISTS `Post`").await.unwrap();
    adapter.execute_script("DROP TABLE IF EXISTS `User`").await.unwrap();
    adapter
        .execute_script(
            "CREATE TABLE `User` (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 email VARCHAR(255) NOT NULL UNIQUE,
                 name VARCHAR(255)
             )",
        )
        .await
        .unwrap();
    adapter
        .execute_script(
            "CREATE TABLE `Post` (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 title VARCHAR(255) NOT NULL,
                 published BOOLEAN NOT NULL DEFAULT FALSE,
                 authorId INT NOT NULL,
                 FOREIGN KEY (authorId) REFERENCES `User`(id)
             )",
        )
        .await
        .unwrap();
    adapter
}

fn text_arg() -> ArgType {
    ArgType {
        scalar_type: ArgScalarType::String,
        db_type: None,
        arity: Arity::Scalar,
    }
}

async fn insert_user(adapter: &mut dyn SqlQueryable, email: &str, name: &str) {
    adapter
        .execute_raw(SqlQuery {
            sql: "INSERT INTO `User` (email, name) VALUES (?, ?)".into(),
            args: vec![QueryValue::Text(email.into()), QueryValue::Text(name.into())],
            arg_types: vec![text_arg(), text_arg()],
        })
        .await
        .unwrap();
}

async fn count_users(adapter: &mut dyn SqlQueryable) -> i64 {
    let rs = adapter
        .query_raw(SqlQuery {
            sql: "SELECT COUNT(*) as cnt FROM `User`".into(),
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

async fn compile_and_execute(
    compiler: &QueryCompiler,
    adapter: &mut dyn SqlQueryable,
    request: &str,
) -> serde_json::Value {
    let expr = compiler.compile_to_ir(request).unwrap();
    let result = QueryExecutor::execute(&expr, adapter).await.unwrap();
    result.to_json()
}

// -- CRUD --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_find_many_empty() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert!(json.is_array());
    assert_eq!(json.as_array().unwrap().len(), 0);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_create_one() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"my@test.com","name":"MyUser"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("my@test.com"), "createOne: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_find_many_with_data() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "a@test.com", "Alice").await;
    insert_user(adapter.as_mut(), "b@test.com", "Bob").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert_eq!(json.as_array().unwrap().len(), 2);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_find_unique() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "unique@test.com", "Unique").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findUnique","query":{"arguments":{"where":{"email":"unique@test.com"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("unique@test.com"), "findUnique: {s}");
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_update_one() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "upd@test.com", "Before").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"updateOne","query":{"arguments":{"where":{"email":"upd@test.com"},"data":{"name":"After"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("After"), "updateOne: {s}");
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_delete_one() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "del@test.com", "Delete").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"deleteOne","query":{"arguments":{"where":{"email":"del@test.com"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("del@test.com"), "deleteOne: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 0);
}

// -- createMany --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_create_many() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createMany","query":{"arguments":{"data":[{"email":"a@test.com","name":"A"},{"email":"b@test.com","name":"B"},{"email":"c@test.com","name":"C"}]},"selection":{"count":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("3") || s.contains("count"), "createMany: {s}");
    assert_eq!(count_users(adapter.as_mut()).await, 3);
}

// -- upsert --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_upsert_create() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"upsertOne","query":{"arguments":{"where":{"email":"new@test.com"},"create":{"email":"new@test.com","name":"Created"},"update":{"name":"Updated"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("Created"), "upsert create: {s}");
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_upsert_update() {
    let mut adapter = setup_db().await;
    insert_user(adapter.as_mut(), "existing@test.com", "Original").await;

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"upsertOne","query":{"arguments":{"where":{"email":"existing@test.com"},"create":{"email":"existing@test.com","name":"ShouldNot"},"update":{"name":"Upserted"}},"selection":{"$scalars":true}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("Upserted"), "upsert update: {s}");
}

// -- Filtering --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_filter_where() {
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
    assert_eq!(json.as_array().unwrap().len(), 1);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_filter_in() {
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
    assert_eq!(json.as_array().unwrap().len(), 2);
}

// -- orderBy and pagination --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_order_by() {
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
    let arr = json.as_array().unwrap();
    assert_eq!(arr[0]["email"].as_str().unwrap(), "a@test.com");
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_skip_take() {
    let mut adapter = setup_db().await;
    for i in 0..5 {
        insert_user(adapter.as_mut(), &format!("u{i}@test.com"), &format!("U{i}")).await;
    }

    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"findMany","query":{"arguments":{"skip":2,"take":2},"selection":{"$scalars":true}}}"#,
    )
    .await;
    assert_eq!(json.as_array().unwrap().len(), 2);
}

// -- Aggregate and groupBy --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_aggregate() {
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
    assert!(s.contains("3"), "aggregate count: {s}");
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_group_by() {
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
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 2, "groupBy: {json}");
}

// -- Nested writes --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_create_with_nested() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();
    let json = compile_and_execute(
        &compiler,
        adapter.as_mut(),
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"author@test.com","name":"Author","posts":{"create":[{"title":"First","published":true},{"title":"Draft","published":false}]}}},"selection":{"$scalars":true,"posts":{"selection":{"$scalars":true}}}}}"#,
    )
    .await;
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("author@test.com"), "nested create: {s}");
    assert!(s.contains("First"), "nested post: {s}");
}

// -- Transaction --

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_transaction_commit() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let expr = compiler.compile_to_ir(
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"tx@test.com","name":"TxUser"}},"selection":{"$scalars":true}}}"#,
    ).unwrap();
    QueryExecutor::execute(&expr, tx.as_mut()).await.unwrap();

    tx.commit().await.unwrap();
    assert_eq!(count_users(adapter.as_mut()).await, 1);
}

#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_transaction_rollback() {
    let mut adapter = setup_db().await;
    let compiler = make_compiler();

    let mut tx = adapter.start_transaction(None).await.unwrap();

    let expr = compiler.compile_to_ir(
        r#"{"modelName":"User","action":"createOne","query":{"arguments":{"data":{"email":"gone@test.com","name":"Gone"}},"selection":{"$scalars":true}}}"#,
    ).unwrap();
    QueryExecutor::execute(&expr, tx.as_mut()).await.unwrap();

    tx.rollback().await.unwrap();
    assert_eq!(count_users(adapter.as_mut()).await, 0);
}
