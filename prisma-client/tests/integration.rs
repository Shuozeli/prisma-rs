//! Integration tests for PrismaClient query builder and JSON protocol output.

use prisma_client::{Operation, QueryBuilder, Selection};

// --- Query Builder Tests ---

#[test]
fn find_many_builds_correct_json() {
    let qb = QueryBuilder::new("User", Operation::FindMany);
    let request = qb.build();

    assert_eq!(request["modelName"], "User");
    assert_eq!(request["action"], "findMany");
    assert_eq!(request["query"]["selection"]["$scalars"], true);
}

#[test]
fn create_one_with_data() {
    let qb = QueryBuilder::new("User", Operation::CreateOne).data(serde_json::json!({
        "email": "test@example.com",
        "name": "Test User"
    }));

    let request = qb.build();
    assert_eq!(request["action"], "createOne");
    assert_eq!(request["query"]["arguments"]["data"]["email"], "test@example.com");
}

#[test]
fn find_unique_with_where() {
    let qb = QueryBuilder::new("User", Operation::FindUnique)
        .where_arg(serde_json::json!({ "id": 1 }))
        .selection(Selection::select().field("id").field("email"));

    let request = qb.build();
    assert_eq!(request["action"], "findUnique");
    assert_eq!(request["query"]["arguments"]["where"]["id"], 1);
    assert_eq!(request["query"]["selection"]["id"], true);
    assert_eq!(request["query"]["selection"]["email"], true);
}

#[test]
fn update_many_builds_correct_json() {
    let qb = QueryBuilder::new("User", Operation::UpdateMany)
        .where_arg(serde_json::json!({ "name": { "contains": "test" } }))
        .data(serde_json::json!({ "name": "Updated" }))
        .selection(Selection::select().field("count"));

    let request = qb.build();
    assert_eq!(request["action"], "updateMany");
    assert_eq!(request["query"]["arguments"]["data"]["name"], "Updated");
    assert_eq!(request["query"]["selection"]["count"], true);
}

#[test]
fn delete_many_builds_correct_json() {
    let qb = QueryBuilder::new("User", Operation::DeleteMany)
        .where_arg(serde_json::json!({}))
        .selection(Selection::select().field("count"));

    let request = qb.build();
    assert_eq!(request["action"], "deleteMany");
    assert_eq!(request["query"]["selection"]["count"], true);
}

// --- Selection Tests ---

#[test]
fn include_with_nested_relation() {
    let nested_sel = Selection::scalars();
    let sel = Selection::include().relation("posts", nested_sel);

    let qb = QueryBuilder::new("User", Operation::FindMany).selection(sel);
    let request = qb.build();

    let selection = &request["query"]["selection"];
    assert_eq!(selection["$scalars"], true);
    assert_eq!(selection["$composites"], true);
    assert!(selection["posts"].is_object());
    assert_eq!(selection["posts"]["selection"]["$scalars"], true);
}

#[test]
fn omit_fields() {
    let sel = Selection::omit().field("password");
    let qb = QueryBuilder::new("User", Operation::FindMany).selection(sel);
    let request = qb.build();

    assert_eq!(request["query"]["selection"]["$scalars"], true);
    assert_eq!(request["query"]["selection"]["password"], false);
}

#[test]
fn deeply_nested_selection() {
    let comment_sel = Selection::select().field("id").field("body");
    let post_sel = Selection::include().relation("comments", comment_sel);
    let user_sel = Selection::include().relation("posts", post_sel);

    let qb = QueryBuilder::new("User", Operation::FindMany).selection(user_sel);
    let request = qb.build();

    let posts_sel = &request["query"]["selection"]["posts"]["selection"];
    assert_eq!(posts_sel["$scalars"], true);
    let comments_sel = &posts_sel["comments"]["selection"];
    assert_eq!(comments_sel["id"], true);
    assert_eq!(comments_sel["body"], true);
}

// --- Pagination Tests ---

#[test]
fn find_many_with_pagination() {
    let qb = QueryBuilder::new("User", Operation::FindMany)
        .skip(5)
        .take(10)
        .order_by(serde_json::json!([{ "id": "asc" }]));

    let request = qb.build();
    assert_eq!(request["query"]["arguments"]["skip"], 5);
    assert_eq!(request["query"]["arguments"]["take"], 10);
    assert_eq!(
        request["query"]["arguments"]["orderBy"],
        serde_json::json!([{ "id": "asc" }])
    );
}

#[test]
fn find_many_with_cursor() {
    let qb = QueryBuilder::new("User", Operation::FindMany)
        .cursor(serde_json::json!({ "id": 42 }))
        .take(10);

    let request = qb.build();
    assert_eq!(request["query"]["arguments"]["cursor"]["id"], 42);
    assert_eq!(request["query"]["arguments"]["take"], 10);
}

#[test]
fn find_many_with_distinct() {
    let qb = QueryBuilder::new("User", Operation::FindMany).distinct(vec!["email".into()]);

    let request = qb.build();
    assert_eq!(request["query"]["arguments"]["distinct"], serde_json::json!(["email"]));
}

// --- Aggregate / GroupBy ---

#[test]
fn aggregate_query() {
    let qb = QueryBuilder::new("User", Operation::Aggregate).selection(Selection::select().field("_count"));

    let request = qb.build();
    assert_eq!(request["action"], "aggregate");
    assert_eq!(request["query"]["selection"]["_count"], true);
}

#[test]
fn group_by_query() {
    let qb = QueryBuilder::new("Post", Operation::GroupBy)
        .arg("by", serde_json::json!(["authorId"]))
        .selection(Selection::select().field("authorId").field("_count"));

    let request = qb.build();
    assert_eq!(request["action"], "groupBy");
    assert_eq!(request["query"]["arguments"]["by"], serde_json::json!(["authorId"]));
}

// --- Raw SQL ---

#[test]
fn raw_query_no_model() {
    let qb = QueryBuilder::raw(Operation::QueryRaw)
        .arg("query", serde_json::json!("SELECT 1"))
        .arg("parameters", serde_json::json!([]));

    let request = qb.build();
    assert!(request["modelName"].is_null());
    assert_eq!(request["action"], "queryRaw");
    assert_eq!(request["query"]["arguments"]["query"], "SELECT 1");
}

#[test]
fn execute_raw_no_model() {
    let qb = QueryBuilder::raw(Operation::ExecuteRaw)
        .arg("query", serde_json::json!("UPDATE users SET name = 'x' WHERE 1=0"))
        .arg("parameters", serde_json::json!([]));

    let request = qb.build();
    assert!(request["modelName"].is_null());
    assert_eq!(request["action"], "executeRaw");
}

// --- Upsert ---

#[test]
fn upsert_query() {
    let qb = QueryBuilder::new("User", Operation::UpsertOne)
        .where_arg(serde_json::json!({ "email": "a@b.com" }))
        .arg("create", serde_json::json!({ "email": "a@b.com", "name": "Alice" }))
        .arg("update", serde_json::json!({ "name": "Alice Updated" }));

    let request = qb.build();
    assert_eq!(request["action"], "upsertOne");
    assert_eq!(request["query"]["arguments"]["where"]["email"], "a@b.com");
    assert_eq!(request["query"]["arguments"]["create"]["email"], "a@b.com");
}

// --- All Operations ---

#[test]
fn all_operations_have_correct_action_strings() {
    let ops = vec![
        (Operation::FindMany, "findMany"),
        (Operation::FindUnique, "findUnique"),
        (Operation::FindFirst, "findFirst"),
        (Operation::CreateOne, "createOne"),
        (Operation::CreateMany, "createMany"),
        (Operation::UpdateOne, "updateOne"),
        (Operation::UpdateMany, "updateMany"),
        (Operation::DeleteOne, "deleteOne"),
        (Operation::DeleteMany, "deleteMany"),
        (Operation::UpsertOne, "upsertOne"),
        (Operation::Aggregate, "aggregate"),
        (Operation::GroupBy, "groupBy"),
        (Operation::Count, "aggregate"),
        (Operation::FindFirstOrThrow, "findFirstOrThrow"),
        (Operation::FindUniqueOrThrow, "findUniqueOrThrow"),
        (Operation::ExecuteRaw, "executeRaw"),
        (Operation::QueryRaw, "queryRaw"),
    ];

    for (op, expected) in ops {
        let qb = QueryBuilder::new("Test", op);
        let json = qb.build();
        assert_eq!(
            json["action"].as_str().unwrap(),
            expected,
            "Operation {:?} should map to '{}'",
            op,
            expected
        );
    }
}
