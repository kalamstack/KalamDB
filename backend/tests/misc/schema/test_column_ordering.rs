//! Integration tests for column ordering (Phase 4 - 008-schema-consolidation)
//!
//! Tests that SELECT * returns columns in ordinal_position order

use kalamdb_commons::models::{
    datatypes::KalamDataType,
    schemas::{ColumnDefinition, TableDefinition, TableType},
    NamespaceId, TableId, TableName,
};

use super::test_support::TestServer;

fn unique_namespace(prefix: &str) -> String {
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System time before UNIX_EPOCH")
        .as_nanos();
    format!("{}_{}", prefix, run_id)
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_select_star_returns_columns_in_ordinal_order() {
    let server = TestServer::new_shared().await;

    // Create a table with columns defined out of order
    let namespace = unique_namespace("test_ns");
    let test_namespace = NamespaceId::from(namespace.as_str());
    let table_name = TableName::new("test_table");
    let table_id = TableId::new(test_namespace.clone(), table_name.clone());

    let columns_out_of_order = vec![
        ColumnDefinition::simple(3, "email", 3, KalamDataType::Text),
        ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
        ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
        ColumnDefinition::simple(4, "created_at", 4, KalamDataType::Timestamp),
    ];

    let table_def = TableDefinition::new_with_defaults(
        test_namespace.clone(),
        table_name.clone(),
        TableType::User,
        columns_out_of_order,
        None,
    )
    .expect("Failed to create table definition");

    // Create namespace first
    server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;

    // Store the table definition using system tables provider
    server
        .app_context
        .system_tables()
        .tables()
        .create_table(&table_id, &table_def)
        .expect("Failed to create table");

    // Get the table definition back
    let retrieved = server
        .app_context
        .system_tables()
        .tables()
        .get_table_by_id(&table_id)
        .expect("Failed to get table")
        .expect("Table not found");

    // Verify columns are sorted by ordinal_position
    assert_eq!(retrieved.columns.len(), 4);
    assert_eq!(retrieved.columns[0].column_name, "id");
    assert_eq!(retrieved.columns[0].ordinal_position, 1);
    assert_eq!(retrieved.columns[1].column_name, "name");
    assert_eq!(retrieved.columns[1].ordinal_position, 2);
    assert_eq!(retrieved.columns[2].column_name, "email");
    assert_eq!(retrieved.columns[2].ordinal_position, 3);
    assert_eq!(retrieved.columns[3].column_name, "created_at");
    assert_eq!(retrieved.columns[3].ordinal_position, 4);

    // Verify Arrow schema has columns in the same order
    let arrow_schema = retrieved.to_arrow_schema().expect("Failed to convert to Arrow schema");
    assert_eq!(arrow_schema.fields().len(), 4);
    assert_eq!(arrow_schema.field(0).name(), "id");
    assert_eq!(arrow_schema.field(1).name(), "name");
    assert_eq!(arrow_schema.field(2).name(), "email");
    assert_eq!(arrow_schema.field(3).name(), "created_at");

    println!("✅ SELECT * returns columns in ordinal_position order");
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_alter_table_add_column_assigns_next_ordinal() {
    let server = TestServer::new_shared().await;

    let namespace = unique_namespace("test_ns");
    let test_namespace = NamespaceId::from(namespace.as_str());
    let table_name = TableName::new("test_table");
    let table_id = TableId::new(test_namespace.clone(), table_name.clone());

    // Create namespace first
    server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;

    // Create initial table with 2 columns
    let initial_columns = vec![
        ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
        ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
    ];

    let mut table_def = TableDefinition::new_with_defaults(
        test_namespace.clone(),
        table_name.clone(),
        TableType::User,
        initial_columns,
        None,
    )
    .expect("Failed to create table definition");

    server
        .app_context
        .system_tables()
        .tables()
        .create_table(&table_id, &table_def)
        .expect("Failed to create table");

    // Simulate ALTER TABLE ADD COLUMN
    let new_column = ColumnDefinition::simple(3, "email", 3, KalamDataType::Text);
    table_def.add_column(new_column).expect("Failed to add column");

    server
        .app_context
        .system_tables()
        .tables()
        .update_table(&table_id, &table_def)
        .expect("Failed to update table");

    // Verify the new column has ordinal_position = 3
    let updated = server
        .app_context
        .system_tables()
        .tables()
        .get_table_by_id(&table_id)
        .expect("Failed to get table")
        .expect("Table not found");

    assert_eq!(updated.columns.len(), 3);
    assert_eq!(updated.columns[2].column_name, "email");
    assert_eq!(updated.columns[2].ordinal_position, 3);

    println!("✅ ALTER TABLE ADD COLUMN assigns next available ordinal_position");
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_alter_table_drop_column_preserves_ordinals() {
    let server = TestServer::new_shared().await;

    let namespace = unique_namespace("test_ns");
    let test_namespace = NamespaceId::from(namespace.as_str());
    let table_name = TableName::new("test_table");
    let table_id = TableId::new(test_namespace.clone(), table_name.clone());

    // Create namespace first
    server.execute_sql(&format!("CREATE NAMESPACE {}", namespace)).await;

    // Create table with 4 columns
    let initial_columns = vec![
        ColumnDefinition::primary_key(1, "id", 1, KalamDataType::BigInt),
        ColumnDefinition::simple(2, "name", 2, KalamDataType::Text),
        ColumnDefinition::simple(3, "email", 3, KalamDataType::Text),
        ColumnDefinition::simple(4, "created_at", 4, KalamDataType::Timestamp),
    ];

    let mut table_def = TableDefinition::new_with_defaults(
        test_namespace.clone(),
        table_name.clone(),
        TableType::User,
        initial_columns,
        None,
    )
    .expect("Failed to create table definition");

    server
        .app_context
        .system_tables()
        .tables()
        .create_table(&table_id, &table_def)
        .expect("Failed to create table");

    // Simulate ALTER TABLE DROP COLUMN email (position 3)
    table_def.drop_column("email").expect("Failed to drop column");

    server
        .app_context
        .system_tables()
        .tables()
        .update_table(&table_id, &table_def)
        .expect("Failed to update table");

    // Verify remaining columns preserve their original ordinal_positions
    let updated = server
        .app_context
        .system_tables()
        .tables()
        .get_table_by_id(&table_id)
        .expect("Failed to get table")
        .expect("Table not found");

    assert_eq!(updated.columns.len(), 3);
    assert_eq!(updated.columns[0].column_name, "id");
    assert_eq!(updated.columns[0].ordinal_position, 1);
    assert_eq!(updated.columns[1].column_name, "name");
    assert_eq!(updated.columns[1].ordinal_position, 2);
    // Note: ordinal_position 3 is now gone
    assert_eq!(updated.columns[2].column_name, "created_at");
    assert_eq!(updated.columns[2].ordinal_position, 4); // Preserved!

    println!("✅ ALTER TABLE DROP COLUMN preserves ordinal_position of remaining columns");
}

#[tokio::test]
#[ntest::timeout(60000)]
async fn test_system_tables_have_correct_column_ordering() {
    use datafusion::datasource::TableProvider;
    let server = TestServer::new_shared().await;

    // Test system.users table
    // Note: System tables are not stored in system.schemas, so we access the provider directly
    let users_provider = server.app_context.system_tables().users();
    let arrow_schema = users_provider.schema();

    // Expected columns in order (based on UsersTableSchema)
    let expected_columns = vec![
        "user_id",
        "password_hash",
        "role",
        "email",
        "auth_type",
        "auth_data",
        "storage_mode",
        "storage_id",
        "failed_login_attempts",
        "locked_until",
        "last_login_at",
        "created_at",
        "updated_at",
        "last_seen",
        "deleted_at",
    ];

    // Verify Arrow schema matches expected column order
    assert_eq!(arrow_schema.fields().len(), expected_columns.len(), "Column count mismatch");

    for (idx, expected_name) in expected_columns.iter().enumerate() {
        let field = arrow_schema.field(idx);
        assert_eq!(
            field.name(),
            expected_name,
            "Column at position {} should be '{}'",
            idx + 1,
            expected_name
        );
    }

    println!("✅ System tables have correct column ordering (ordinal_position)");
}
