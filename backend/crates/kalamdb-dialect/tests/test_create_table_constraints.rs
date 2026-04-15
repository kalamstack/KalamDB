//! Tests for CREATE TABLE IF NOT EXISTS with constraints.

use kalamdb_dialect::ddl::create_table::CreateTableStatement;

#[test]
fn test_if_not_exists_basic() {
    let sql = "CREATE TABLE IF NOT EXISTS test.users (id BIGINT, name TEXT)";
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();

    assert!(stmt.if_not_exists);
    assert_eq!(stmt.table_name.as_str(), "users");
    assert_eq!(stmt.namespace_id.as_str(), "test");
}

#[test]
fn test_if_not_exists_with_primary_key() {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS test.products (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL
        )
    "#;
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();

    assert!(stmt.if_not_exists);
    assert_eq!(stmt.primary_key_column.as_deref(), Some("id"));
    assert!(!stmt.schema.field_with_name("id").unwrap().is_nullable());
    assert!(!stmt.schema.field_with_name("name").unwrap().is_nullable());
}

#[test]
fn test_exact_user_query() {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS playing_with_neon(
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL
        )
    "#;
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();

    assert!(stmt.if_not_exists);
    assert_eq!(stmt.table_name.as_str(), "playing_with_neon");
    assert_eq!(stmt.primary_key_column.as_deref(), Some("id"));
    assert!(stmt.schema.field_with_name("id").is_ok());
    assert!(stmt.schema.field_with_name("name").is_ok());
    assert!(stmt.schema.field_with_name("value").is_ok());
    assert!(!stmt.schema.field_with_name("id").unwrap().is_nullable());
    assert!(!stmt.schema.field_with_name("name").unwrap().is_nullable());
    assert!(stmt.schema.field_with_name("value").unwrap().is_nullable());
}

#[test]
fn test_without_if_not_exists() {
    let sql = "CREATE TABLE test.orders (id BIGINT PRIMARY KEY, total REAL)";
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();

    assert!(!stmt.if_not_exists);
}

#[test]
fn test_multiple_not_null_columns() {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS test.employees (
            id BIGINT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            department TEXT
        )
    "#;
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();

    assert!(stmt.if_not_exists);
    assert_eq!(stmt.primary_key_column.as_deref(), Some("id"));
    assert!(!stmt.schema.field_with_name("id").unwrap().is_nullable());
    assert!(!stmt.schema.field_with_name("first_name").unwrap().is_nullable());
    assert!(!stmt.schema.field_with_name("last_name").unwrap().is_nullable());
    assert!(!stmt.schema.field_with_name("email").unwrap().is_nullable());
    assert!(stmt.schema.field_with_name("department").unwrap().is_nullable());
}

#[test]
fn test_primary_key_table_constraint() {
    let sql = r#"
        CREATE TABLE IF NOT EXISTS test.items (
            id BIGINT,
            name TEXT NOT NULL,
            PRIMARY KEY (id)
        )
    "#;
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();

    assert!(stmt.if_not_exists);
    assert_eq!(stmt.primary_key_column.as_deref(), Some("id"));
    assert!(!stmt.schema.field_with_name("id").unwrap().is_nullable());
}

#[test]
fn test_table_types_with_if_not_exists() {
    let sql = "CREATE USER TABLE IF NOT EXISTS test.user_data (id BIGINT PRIMARY KEY)";
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();
    assert!(stmt.if_not_exists);
    assert_eq!(stmt.table_type, kalamdb_commons::schemas::TableType::User);

    let sql = "CREATE SHARED TABLE IF NOT EXISTS test.shared_data (id BIGINT PRIMARY KEY)";
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();
    assert!(stmt.if_not_exists);
    assert_eq!(stmt.table_type, kalamdb_commons::schemas::TableType::Shared);

    let sql = r#"
        CREATE STREAM TABLE IF NOT EXISTS test.events (
            id BIGINT PRIMARY KEY
        ) WITH (
            TTL_SECONDS = 3600
        )
    "#;
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();
    assert!(stmt.if_not_exists);
    assert_eq!(stmt.table_type, kalamdb_commons::schemas::TableType::Stream);
}

#[test]
fn test_validation_primary_key_not_null() {
    let sql = "CREATE TABLE test.data (id BIGINT PRIMARY KEY)";
    let stmt = CreateTableStatement::parse(sql, "default").unwrap();

    assert!(!stmt.schema.field_with_name("id").unwrap().is_nullable());
}

#[test]
fn test_validation_multiple_primary_keys_rejected() {
    let sql = r#"
        CREATE TABLE test.invalid (
            id1 BIGINT PRIMARY KEY,
            id2 BIGINT PRIMARY KEY
        )
    "#;
    let result = CreateTableStatement::parse(sql, "default");

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Multiple PRIMARY KEY"));
}

#[test]
fn test_validation_composite_primary_key_not_supported() {
    let sql = r#"
        CREATE TABLE test.invalid (
            id1 BIGINT,
            id2 BIGINT,
            PRIMARY KEY (id1, id2)
        )
    "#;
    let result = CreateTableStatement::parse(sql, "default");

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Composite PRIMARY KEY"));
}

#[test]
#[ignore]
fn test_validation_invalid_column_names() {
    let sql = "CREATE TABLE test.invalid (`id-with-dash` BIGINT)";
    let result = CreateTableStatement::parse(sql, "default");

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("alphanumeric") || err.contains("Invalid column name"));
}

#[test]
fn test_validation_primary_key_must_exist() {
    let sql = r#"
        CREATE TABLE test.invalid (
            id BIGINT,
            name TEXT,
            PRIMARY KEY (nonexistent)
        )
    "#;
    let result = CreateTableStatement::parse(sql, "default");

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}
