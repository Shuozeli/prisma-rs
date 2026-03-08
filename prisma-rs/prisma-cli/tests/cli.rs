use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::sync::atomic::{AtomicU32, Ordering};
use tempfile::TempDir;

static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Generate a unique database name per test to avoid conflicts.
fn unique_db_name(prefix: &str) -> String {
    let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("{prefix}_{pid}_{n}")
}

const VALID_SCHEMA: &str = r#"
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
  id       Int     @id @default(autoincrement())
  title    String
  content  String?
  authorId Int
  author   User    @relation(fields: [authorId], references: [id])
}
"#;

const INVALID_SCHEMA: &str = r#"
datasource db {
  provider = "postgresql"
}

mode User {
  id Int @id
}
"#;

fn write_schema(dir: &TempDir, content: &str) -> String {
    let schema_path = dir.path().join("schema.prisma");
    fs::write(&schema_path, content).unwrap();
    schema_path.to_string_lossy().to_string()
}

fn prisma() -> Command {
    #[allow(deprecated)]
    Command::cargo_bin("prisma").unwrap()
}

// ============================================================================
// Help and version
// ============================================================================

#[test]
fn shows_help() {
    prisma()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Prisma CLI"))
        .stdout(predicate::str::contains("validate"))
        .stdout(predicate::str::contains("format"))
        .stdout(predicate::str::contains("generate"))
        .stdout(predicate::str::contains("db"))
        .stdout(predicate::str::contains("migrate"));
}

#[test]
fn shows_version() {
    prisma()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("prisma"));
}

// ============================================================================
// Validate
// ============================================================================

#[test]
fn validate_valid_schema() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);

    prisma()
        .args(["--schema", &path, "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("is valid"));
}

#[test]
fn validate_invalid_schema() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, INVALID_SCHEMA);

    prisma().args(["--schema", &path, "validate"]).assert().failure();
}

#[test]
fn validate_missing_file() {
    prisma()
        .args(["--schema", "/tmp/nonexistent.prisma", "validate"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}

// ============================================================================
// Format
// ============================================================================

#[test]
fn format_schema() {
    let dir = TempDir::new().unwrap();
    // Poorly formatted schema
    let ugly = "datasource db {\nprovider=\"postgresql\"\n}\nmodel User {\nid Int @id\nemail String @unique\n}";
    let path = write_schema(&dir, ugly);

    prisma()
        .args(["--schema", &path, "format"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Formatted"));

    let formatted = fs::read_to_string(&path).unwrap();
    assert!(
        formatted.contains("provider = \"postgresql\""),
        "Should have formatted provider line: {}",
        formatted
    );
}

#[test]
fn format_check_already_formatted() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);

    // First format the schema
    prisma().args(["--schema", &path, "format"]).assert().success();

    // Then check -- should pass
    prisma()
        .args(["--schema", &path, "format", "--check"])
        .assert()
        .success()
        .stdout(predicate::str::contains("already formatted"));
}

// ============================================================================
// Generate
// ============================================================================

#[test]
fn generate_typescript() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);
    let output_dir = dir.path().join("generated");

    prisma()
        .args([
            "--schema",
            &path,
            "generate",
            "--output",
            &output_dir.to_string_lossy(),
            "--language",
            "typescript",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Generated TypeScript client"));

    let ts_file = output_dir.join("index.ts");
    assert!(ts_file.exists(), "index.ts should be generated");
    let content = fs::read_to_string(&ts_file).unwrap();
    assert!(content.contains("export interface User {"));
    assert!(content.contains("export interface PrismaClient {"));
    assert!(content.contains("findMany("));
}

#[test]
fn generate_rust() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);
    let output_dir = dir.path().join("generated");

    prisma()
        .args([
            "--schema",
            &path,
            "generate",
            "--output",
            &output_dir.to_string_lossy(),
            "--language",
            "rust",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Generated Rust client"));

    let rs_file = output_dir.join("client.rs");
    assert!(rs_file.exists(), "client.rs should be generated");
    let content = fs::read_to_string(&rs_file).unwrap();
    assert!(content.contains("pub struct User {"));
    assert!(content.contains("pub struct PrismaClient {"));
    assert!(content.contains("fn find_many("));
}

#[test]
fn generate_invalid_language() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);

    prisma()
        .args(["--schema", &path, "generate", "--language", "python"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Unsupported language"));
}

#[test]
fn generate_short_flags() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);
    let output_dir = dir.path().join("gen");

    prisma()
        .args([
            "--schema",
            &path,
            "generate",
            "-o",
            &output_dir.to_string_lossy(),
            "-l",
            "ts",
        ])
        .assert()
        .success();
}

// ============================================================================
// DB / Migrate subcommand parsing
// ============================================================================

#[test]
fn db_push_requires_url() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);

    // No DATABASE_URL set, no --url flag
    prisma()
        .args(["--schema", &path, "db", "push"])
        .env_remove("DATABASE_URL")
        .assert()
        .failure()
        .stderr(predicate::str::contains("No database URL"));
}

#[test]
fn db_execute_requires_input() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);

    prisma()
        .args([
            "--schema",
            &path,
            "db",
            "execute",
            "--url",
            "postgresql://localhost/test",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--stdin or --file"));
}

#[test]
fn migrate_reset_requires_force() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);

    prisma()
        .args(["--schema", &path, "migrate", "reset"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--force"));
}

#[test]
fn migrate_resolve_requires_flag() {
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, VALID_SCHEMA);

    prisma()
        .args([
            "--schema",
            &path,
            "migrate",
            "resolve",
            "--url",
            "postgresql://localhost/test",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--applied or --rolled-back"));
}

// ============================================================================
// Generate with enum schema
// ============================================================================

#[test]
fn generate_with_enums() {
    let schema = r#"
datasource db {
  provider = "postgresql"
}

enum Role {
  USER
  ADMIN
}

model User {
  id   Int  @id @default(autoincrement())
  role Role
}
"#;

    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, schema);
    let output_dir = dir.path().join("generated");

    prisma()
        .args([
            "--schema",
            &path,
            "generate",
            "-o",
            &output_dir.to_string_lossy(),
            "-l",
            "ts",
        ])
        .assert()
        .success();

    let content = fs::read_to_string(output_dir.join("index.ts")).unwrap();
    assert!(content.contains("export const Role = {"));
    assert!(content.contains("USER: 'USER'"));
}

// ============================================================================
// SQLite database tests (no Docker needed)
// ============================================================================

#[test]
fn db_push_sqlite() {
    let dir = TempDir::new().unwrap();
    let schema = r#"
datasource db {
  provider = "sqlite"
}

model User {
  id    Int    @id @default(autoincrement())
  email String @unique
  name  String?
}
"#;
    let path = write_schema(&dir, schema);
    let db_path = dir.path().join("test.db");
    let url = format!("file:{}", db_path.to_string_lossy());

    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("pushed successfully"));
}

#[test]
fn db_execute_sqlite() {
    let dir = TempDir::new().unwrap();
    let schema = r#"
datasource db {
  provider = "sqlite"
}
"#;
    let path = write_schema(&dir, schema);
    let db_path = dir.path().join("exec.db");
    let url = format!("file:{}", db_path.to_string_lossy());

    let sql_file = dir.path().join("init.sql");
    fs::write(&sql_file, "CREATE TABLE test (id INTEGER PRIMARY KEY);").unwrap();

    prisma()
        .args([
            "--schema",
            &path,
            "db",
            "execute",
            "--url",
            &url,
            "--file",
            &sql_file.to_string_lossy(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("SQL executed successfully"));
}

#[test]
fn db_pull_sqlite() {
    let dir = TempDir::new().unwrap();
    let schema = r#"
datasource db {
  provider = "sqlite"
}

model User {
  id    Int    @id @default(autoincrement())
  email String @unique
}
"#;
    let path = write_schema(&dir, schema);
    let db_path = dir.path().join("pull.db");
    let url = format!("file:{}", db_path.to_string_lossy());

    // First push to create the database
    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success();

    // Then pull to introspect
    prisma()
        .args(["--schema", &path, "db", "pull", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("Introspected schema written"));

    let pulled = fs::read_to_string(&path).unwrap();
    assert!(pulled.contains("User"), "Pulled schema should contain User model");
}

#[test]
fn migrate_dev_sqlite() {
    let dir = TempDir::new().unwrap();
    let prisma_dir = dir.path().join("prisma");
    fs::create_dir_all(&prisma_dir).unwrap();

    let schema = r#"
datasource db {
  provider = "sqlite"
}

model User {
  id    Int    @id @default(autoincrement())
  email String @unique
}
"#;
    let schema_path = prisma_dir.join("schema.prisma");
    fs::write(&schema_path, schema).unwrap();

    let db_path = dir.path().join("migrate.db");
    let url = format!("file:{}", db_path.to_string_lossy());

    prisma()
        .args([
            "--schema",
            schema_path.to_string_lossy().as_ref(),
            "migrate",
            "dev",
            "--name",
            "init",
            "--url",
            &url,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("migration"));

    // Verify migration directory was created
    let migrations_dir = prisma_dir.join("migrations");
    assert!(migrations_dir.exists(), "migrations directory should exist");
}

#[test]
fn migrate_reset_sqlite() {
    let dir = TempDir::new().unwrap();
    let schema = r#"
datasource db {
  provider = "sqlite"
}

model User {
  id Int @id @default(autoincrement())
}
"#;
    let path = write_schema(&dir, schema);
    let db_path = dir.path().join("reset.db");
    let url = format!("file:{}", db_path.to_string_lossy());

    // Push first to create DB
    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success();

    // Reset
    prisma()
        .args(["--schema", &path, "migrate", "reset", "--force", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("reset successfully"));
}

#[test]
fn migrate_diff_empty_to_schema() {
    let dir = TempDir::new().unwrap();
    let schema = r#"
datasource db {
  provider = "sqlite"
}

model User {
  id    Int    @id @default(autoincrement())
  email String @unique
}
"#;
    let path = write_schema(&dir, schema);
    let db_path = dir.path().join("diff.db");
    let url = format!("file:{}", db_path.to_string_lossy());

    // Diff from empty to schema (pure schema-based diff)
    prisma()
        .env("DATABASE_URL", &url)
        .args(["--schema", &path, "migrate", "diff", "--from", "empty", "--to", &path])
        .assert()
        .success();
}

// ============================================================================
// PostgreSQL database tests (requires Docker: PG on port 15432)
// ============================================================================

const PG_BASE_URL: &str = "postgresql://prisma:prisma@localhost:15432";

fn pg_url(db_name: &str) -> String {
    format!("{PG_BASE_URL}/{db_name}")
}

fn pg_schema() -> String {
    r#"
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
  id       Int     @id @default(autoincrement())
  title    String
  content  String?
  authorId Int
  author   User    @relation(fields: [authorId], references: [id])
}
"#
    .to_string()
}

/// Returns true if the test should run. Panics if PG is unreachable
/// (unless SKIP_PG=1 is set, in which case returns false).
fn require_pg() -> bool {
    if std::env::var("SKIP_PG").as_deref() == Ok("1") {
        return false;
    }
    let reachable =
        std::net::TcpStream::connect_timeout(&"127.0.0.1:15432".parse().unwrap(), std::time::Duration::from_secs(2))
            .is_ok();
    assert!(
        reachable,
        "PostgreSQL not available on port 15432. Set SKIP_PG=1 to skip."
    );
    true
}

/// Create a fresh PG schema for a test using psql, returning the URL with search_path.
fn create_pg_schema(name: &str) -> String {
    let base = pg_url("prisma_test");
    // Use psql to create schema outside a transaction (DROP/CREATE SCHEMA is fine in a tx)
    let sql = format!("DROP SCHEMA IF EXISTS \"{name}\" CASCADE; CREATE SCHEMA \"{name}\";");
    let status = std::process::Command::new("psql")
        .args([&base, "-c", &sql])
        .output()
        .expect("psql must be available for PG tests");
    assert!(
        status.status.success(),
        "Failed to create PG schema: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    format!("{base}?schema={name}")
}

#[test]
fn pg_db_push() {
    if !require_pg() {
        return;
    }

    let db_name = unique_db_name("cli_push");
    let url = create_pg_schema(&db_name);
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, &pg_schema());

    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("pushed successfully"));
}

#[test]
fn pg_db_pull() {
    if !require_pg() {
        return;
    }

    let db_name = unique_db_name("cli_pull");
    let url = create_pg_schema(&db_name);
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, &pg_schema());

    // Push schema first
    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success();

    // Write a minimal schema for pull (just datasource)
    let minimal = "datasource db {\n  provider = \"postgresql\"\n}\n";
    fs::write(&path, minimal).unwrap();

    // Pull to introspect
    prisma()
        .args(["--schema", &path, "db", "pull", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("Introspected schema written"));

    let pulled = fs::read_to_string(&path).unwrap();
    assert!(pulled.contains("User"), "Pulled schema should contain User model");
    assert!(pulled.contains("Post"), "Pulled schema should contain Post model");
}

#[test]
fn pg_db_execute() {
    if !require_pg() {
        return;
    }

    let db_name = unique_db_name("cli_exec");
    let url = create_pg_schema(&db_name);
    let dir = TempDir::new().unwrap();
    let schema_path = dir.path().join("schema.prisma");
    fs::write(&schema_path, "datasource db {\n  provider = \"postgresql\"\n}\n").unwrap();

    let sql_file = dir.path().join("exec.sql");
    fs::write(&sql_file, "CREATE TABLE test_exec (id SERIAL PRIMARY KEY, value TEXT);").unwrap();

    prisma()
        .args([
            "--schema",
            &schema_path.to_string_lossy(),
            "db",
            "execute",
            "--url",
            &url,
            "--file",
            &sql_file.to_string_lossy(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("SQL executed successfully"));
}

#[test]
fn pg_migrate_dev() {
    if !require_pg() {
        return;
    }

    let db_name = unique_db_name("cli_mig");
    let url = create_pg_schema(&db_name);
    let dir = TempDir::new().unwrap();
    let prisma_dir = dir.path().join("prisma");
    fs::create_dir_all(&prisma_dir).unwrap();

    let schema = r#"
datasource db {
  provider = "postgresql"
}

model User {
  id    Int    @id @default(autoincrement())
  email String @unique
}
"#;
    let schema_path = prisma_dir.join("schema.prisma");
    fs::write(&schema_path, schema).unwrap();

    prisma()
        .args([
            "--schema",
            schema_path.to_string_lossy().as_ref(),
            "migrate",
            "dev",
            "--name",
            "init",
            "--url",
            &url,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("migration"));

    let migrations_dir = prisma_dir.join("migrations");
    assert!(migrations_dir.exists(), "migrations directory should exist");

    // Verify a migration SQL file was created
    let entries: Vec<_> = fs::read_dir(&migrations_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    assert!(!entries.is_empty(), "Should have at least one migration directory");

    let migration_sql = entries[0].path().join("migration.sql");
    assert!(migration_sql.exists(), "migration.sql should exist");
    let sql = fs::read_to_string(&migration_sql).unwrap();
    assert!(sql.contains("User"), "Migration SQL should reference User table");
}

#[test]
fn pg_migrate_deploy() {
    if !require_pg() {
        return;
    }

    let db_name = unique_db_name("cli_deploy");
    let url = create_pg_schema(&db_name);
    let dir = TempDir::new().unwrap();
    let prisma_dir = dir.path().join("prisma");
    fs::create_dir_all(&prisma_dir).unwrap();

    let schema = r#"
datasource db {
  provider = "postgresql"
}

model Item {
  id   Int    @id @default(autoincrement())
  name String
}
"#;
    let schema_path = prisma_dir.join("schema.prisma");
    fs::write(&schema_path, schema).unwrap();

    // Create migration without applying
    prisma()
        .args([
            "--schema",
            schema_path.to_string_lossy().as_ref(),
            "migrate",
            "dev",
            "--name",
            "init",
            "--create-only",
            "--url",
            &url,
        ])
        .assert()
        .success();

    // Deploy the pending migration
    prisma()
        .args([
            "--schema",
            schema_path.to_string_lossy().as_ref(),
            "migrate",
            "deploy",
            "--url",
            &url,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Applied 1 migration"));
}

#[test]
fn pg_migrate_reset() {
    if !require_pg() {
        return;
    }

    let db_name = unique_db_name("cli_reset");
    let url = create_pg_schema(&db_name);
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, &pg_schema());

    // Push first
    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success();

    // Reset
    prisma()
        .args(["--schema", &path, "migrate", "reset", "--force", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("reset successfully"));
}

// ============================================================================
// MySQL database tests (requires Docker: MySQL on port 13306)
// ============================================================================

const MYSQL_BASE_URL: &str = "mysql://prisma:prisma@localhost:13306";
const MYSQL_ROOT_URL: &str = "mysql://root:prisma@localhost:13306";

fn mysql_url(db_name: &str) -> String {
    format!("{MYSQL_BASE_URL}/{db_name}")
}

fn mysql_root_url(db_name: &str) -> String {
    format!("{MYSQL_ROOT_URL}/{db_name}")
}

fn mysql_schema() -> String {
    r#"
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
  id       Int     @id @default(autoincrement())
  title    String  @db.VarChar(255)
  content  String? @db.Text
  authorId Int
  author   User    @relation(fields: [authorId], references: [id])
}
"#
    .to_string()
}

/// Returns true if the test should run. Panics if MySQL is unreachable
/// (unless SKIP_MYSQL=1 is set, in which case returns false).
fn require_mysql() -> bool {
    if std::env::var("SKIP_MYSQL").as_deref() == Ok("1") {
        return false;
    }
    let reachable =
        std::net::TcpStream::connect_timeout(&"127.0.0.1:13306".parse().unwrap(), std::time::Duration::from_secs(2))
            .is_ok();
    assert!(
        reachable,
        "MySQL not available on port 13306. Set SKIP_MYSQL=1 to skip."
    );
    true
}

/// Create a fresh MySQL database for a test using root credentials,
/// then grant access to the prisma user. Returns the prisma-user URL.
fn create_mysql_db(name: &str) -> String {
    let root_url = mysql_root_url("prisma_test");
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("schema.prisma");
    fs::write(&path, "datasource db {\n  provider = \"mysql\"\n}\n").unwrap();
    let sql = format!(
        "DROP DATABASE IF EXISTS `{name}`; CREATE DATABASE `{name}`; GRANT ALL PRIVILEGES ON *.* TO 'prisma'@'%'; FLUSH PRIVILEGES;"
    );
    let sql_file = dir.path().join("create.sql");
    fs::write(&sql_file, &sql).unwrap();

    prisma()
        .args([
            "--schema",
            &path.to_string_lossy(),
            "db",
            "execute",
            "--url",
            &root_url,
            "--file",
            &sql_file.to_string_lossy(),
        ])
        .assert()
        .success();

    mysql_url(name)
}

#[test]
fn mysql_db_push() {
    if !require_mysql() {
        return;
    }

    let db_name = unique_db_name("cli_push");
    let url = create_mysql_db(&db_name);
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, &mysql_schema());

    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("pushed successfully"));
}

#[test]
fn mysql_db_pull() {
    if !require_mysql() {
        return;
    }

    let db_name = unique_db_name("cli_pull");
    let url = create_mysql_db(&db_name);
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, &mysql_schema());

    // Push first
    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success();

    // Write minimal schema for pull
    let minimal = "datasource db {\n  provider = \"mysql\"\n}\n";
    fs::write(&path, minimal).unwrap();

    // Pull
    prisma()
        .args(["--schema", &path, "db", "pull", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("Introspected schema written"));

    let pulled = fs::read_to_string(&path).unwrap();
    assert!(pulled.contains("User"), "Pulled schema should contain User model");
    assert!(pulled.contains("Post"), "Pulled schema should contain Post model");
}

#[test]
fn mysql_db_execute() {
    if !require_mysql() {
        return;
    }

    let db_name = unique_db_name("cli_exec");
    let url = create_mysql_db(&db_name);
    let dir = TempDir::new().unwrap();
    let schema_path = dir.path().join("schema.prisma");
    fs::write(&schema_path, "datasource db {\n  provider = \"mysql\"\n}\n").unwrap();

    let sql_file = dir.path().join("exec.sql");
    fs::write(
        &sql_file,
        "CREATE TABLE test_exec (id INT AUTO_INCREMENT PRIMARY KEY, value TEXT);",
    )
    .unwrap();

    prisma()
        .args([
            "--schema",
            &schema_path.to_string_lossy(),
            "db",
            "execute",
            "--url",
            &url,
            "--file",
            &sql_file.to_string_lossy(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("SQL executed successfully"));
}

#[test]
fn mysql_migrate_dev() {
    if !require_mysql() {
        return;
    }

    let db_name = unique_db_name("cli_mig");
    let url = create_mysql_db(&db_name);
    let dir = TempDir::new().unwrap();
    let prisma_dir = dir.path().join("prisma");
    fs::create_dir_all(&prisma_dir).unwrap();

    let schema = r#"
datasource db {
  provider = "mysql"
}

model User {
  id    Int    @id @default(autoincrement())
  email String @unique
}
"#;
    let schema_path = prisma_dir.join("schema.prisma");
    fs::write(&schema_path, schema).unwrap();

    prisma()
        .args([
            "--schema",
            schema_path.to_string_lossy().as_ref(),
            "migrate",
            "dev",
            "--name",
            "init",
            "--url",
            &url,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("migration"));

    let migrations_dir = prisma_dir.join("migrations");
    assert!(migrations_dir.exists());

    let entries: Vec<_> = fs::read_dir(&migrations_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    assert!(!entries.is_empty());
}

#[test]
fn mysql_migrate_reset() {
    if !require_mysql() {
        return;
    }

    let db_name = unique_db_name("cli_reset");
    let url = create_mysql_db(&db_name);
    let dir = TempDir::new().unwrap();
    let path = write_schema(&dir, &mysql_schema());

    // Push first
    prisma()
        .args(["--schema", &path, "db", "push", "--url", &url])
        .assert()
        .success();

    // Reset
    prisma()
        .args(["--schema", &path, "migrate", "reset", "--force", "--url", &url])
        .assert()
        .success()
        .stdout(predicate::str::contains("reset successfully"));
}
