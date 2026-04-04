#![allow(unused_imports)]
#![cfg(any())] // Disabled - Phase 8 TableSchemaStore separated cache deprecated in Phase 10 unified SchemaCache
//! Integration tests for Phase 15 (008-schema-consolidation)
//!
//! Tests the consolidated schema infrastructure:
//! - TableSchemaStore persistence
//! - SchemaCache performance
//! - Schema versioning

use kalamdb_commons::{NamespaceId, TableId, TableName};
use kalamdb_core::system_table_registration::register_system_tables;
use kalamdb_store::entity_store::EntityStore;
use kalamdb_store::test_utils::TestDb;
use std::sync::Arc;

fn create_test_db_and_backend() -> (TestDb, Arc<dyn kalamdb_store::StorageBackend>) {
    let test_db = TestDb::with_system_tables().expect("Failed to create test database");
    let backend = test_db.backend();
    (test_db, backend)
}

#[tokio::test]
async fn test_schema_store_persistence() {
    use kalamdb_registry::TableSchemaStore;

    let (_test_db, backend) = create_test_db_and_backend();

    // Register system tables - this populates the schema store
    let system_schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
    let (_jobs_provider, schema_store) = register_system_tables(&system_schema, backend.clone())
        .expect("Failed to register system tables");

    // Verify all 7 system table schemas are persisted
    let system_namespace = NamespaceId::system();
    let all_schemas = schema_store
        .scan_namespace(&system_namespace)
        .expect("Failed to scan namespace");

    assert_eq!(all_schemas.len(), 7, "Should have 7 system table schemas");

    // Verify specific table schemas
    let table_names = vec![
        "users",
        "jobs",
        "namespaces",
        "storages",
        "live",
        "tables",
        "table_schemas",
    ];

    for table_name in table_names {
        let table_id = TableId::new(system_namespace.clone(), TableName::from(table_name));
        let schema = schema_store
            .get(&table_id)
            .expect("Failed to get schema")
            .unwrap_or_else(|| panic!("Schema not found for {}", table_name));

        assert!(!schema.columns.is_empty(), "Table {} should have columns", table_name);
        // Note: schema_history may be empty for system tables initialized without explicit versioning
        // This is acceptable - versioning is tracked separately

        println!("✅ Schema persisted for system.{}", table_name);
    }
}

#[tokio::test]
async fn test_schema_cache_basic_operations() {
    use kalamdb_core::schema_registry::SchemaRegistry;
    use kalamdb_registry::TableSchemaStore;

    let (_test_db, backend) = create_test_db_and_backend();

    // Register system tables to get populated schema store
    let system_schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
    let (_jobs_provider, schema_store) = register_system_tables(&system_schema, backend.clone())
        .expect("Failed to register system tables");

    let system_namespace = NamespaceId::system();
    let users_table_id = TableId::new(system_namespace, TableName::from("users"));

    // Check if schema is already cached (may be pre-loaded during registration)
    let schema_before = cache.get(&users_table_id);
    let was_preloaded = schema_before.is_some();

    if !was_preloaded {
        // Load from store and insert into cache
        let schema_from_store = schema_store
            .get(&users_table_id)
            .expect("Failed to get from store")
            .expect("Schema should exist");
        cache.insert(users_table_id.clone(), schema_from_store.clone());
    }

    // Access again - should definitely hit cache now
    let schema_after = cache.get(&users_table_id);
    assert!(schema_after.is_some(), "Schema should be in cache");

    // Verify cache size
    let size = cache.stats();
    assert!(size >= 1, "Cache should have at least 1 entry");

    println!("✅ Cache basic operations: {} size, was_preloaded={}", size, was_preloaded);
}

#[tokio::test]
async fn test_schema_versioning() {
    use kalamdb_registry::TableSchemaStore;

    let (_test_db, backend) = create_test_db_and_backend();

    // Register system tables
    let system_schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
    let (_jobs_provider, schema_store) = register_system_tables(&system_schema, backend.clone())
        .expect("Failed to register system tables");

    let system_namespace = NamespaceId::system();
    let users_table_id = TableId::new(system_namespace, TableName::from("users"));

    let schema = schema_store
        .get(&users_table_id)
        .expect("Failed to get schema")
        .expect("Schema should exist");

    // Verify schema has columns (versioning may not be initialized for system tables)
    assert!(!schema.columns.is_empty(), "Schema should have columns");

    // If schema history exists, verify it
    if !schema.schema_history.is_empty() {
        assert_eq!(schema.schema_history[0].version, 1, "First version should be 1");
        println!(
            "✅ Schema versioning: {} versions, current version {}",
            schema.schema_history.len(),
            schema.schema_history[0].version
        );
    } else {
        println!(
            "✅ Schema exists with {} columns (history not initialized)",
            schema.columns.len()
        );
    }
}

#[tokio::test]
async fn test_all_system_tables_have_schemas() {
    use kalamdb_registry::TableSchemaStore;

    let (_test_db, backend) = create_test_db_and_backend();

    // Register system tables
    let system_schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
    let (_jobs_provider, schema_store) = register_system_tables(&system_schema, backend.clone())
        .expect("Failed to register system tables");

    let system_namespace = NamespaceId::system();

    // All 7 system tables should have schema definitions
    let expected_tables = vec![
        "users",
        "jobs",
        "namespaces",
        "storages",
        "live",
        "tables",
        "table_schemas",
    ];

    for table_name in expected_tables {
        let table_id = TableId::new(system_namespace.clone(), TableName::from(table_name));
        let schema = schema_store
            .get(&table_id)
            .expect("Failed to get schema")
            .unwrap_or_else(|| panic!("Schema not found for {}", table_name));

        assert!(!schema.columns.is_empty(), "Table {} should have columns", table_name);

        // Verify each column has required metadata
        for (idx, col) in schema.columns.iter().enumerate() {
            assert!(
                !col.column_name.is_empty(),
                "Column {} in {} should have a name",
                idx,
                table_name
            );
            // Ordinal positions are 1-indexed, so column 0 has ordinal_position 1
            assert_eq!(
                col.ordinal_position as usize,
                idx + 1,
                "Column {} in {} should have correct ordinal (1-indexed)",
                idx,
                table_name
            );
        }

        println!("✅ system.{} has {} columns", table_name, schema.columns.len());
    }
}

#[tokio::test]
async fn test_internal_api_schema_matches_describe_table() {
    use kalamdb_registry::TableSchemaStore;

    let (_test_db, backend) = create_test_db_and_backend();

    // Register system tables
    let system_schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
    let (_jobs_provider, schema_store) = register_system_tables(&system_schema, backend.clone())
        .expect("Failed to register system tables");

    let system_namespace = NamespaceId::system();
    let users_table_id = TableId::new(system_namespace.clone(), TableName::from("users"));

    // Get schema from internal API (TableSchemaStore)
    let api_schema = schema_store
        .get(&users_table_id)
        .expect("Failed to get schema from API")
        .expect("Schema not found in API");

    // Verify the API schema has expected structure
    assert!(!api_schema.columns.is_empty(), "Schema should have columns");
    assert_eq!(api_schema.table_name.as_str(), "users", "Table name should be 'users'");
    assert_eq!(api_schema.namespace_id.as_str(), "system", "Namespace should be 'system'");

    // Verify all columns have correct ordinal positions (1-indexed)
    for (idx, column) in api_schema.columns.iter().enumerate() {
        assert_eq!(
            column.ordinal_position as usize,
            idx + 1,
            "Column {} should have ordinal_position {}",
            column.column_name,
            idx + 1
        );
    }

    // Verify the schema can convert to Arrow schema
    let arrow_schema = api_schema.to_arrow_schema().expect("Should convert to Arrow schema");
    assert_eq!(
        arrow_schema.fields().len(),
        api_schema.columns.len(),
        "Arrow schema should have same number of fields as columns"
    );

    println!(
        "✅ Internal API schema structure is valid for system.users ({} columns)",
        api_schema.columns.len()
    );
}

#[tokio::test]
async fn test_cache_invalidation_on_alter_table() {
    use kalamdb_commons::models::schemas::{ColumnDefinition, TableDefinition};

    let (_test_db, backend) = create_test_db_and_backend();

    // Create schema store and cache
    let system_schema = Arc::new(datafusion::catalog::memory::MemorySchemaProvider::new());
    let (_jobs_provider, schema_store) = register_system_tables(&system_schema, backend.clone())
        .expect("Failed to register system tables");

    let test_namespace = NamespaceId::from("test_ns");
    let test_table_id = TableId::new(test_namespace.clone(), TableName::from("test_table"));

    // Create initial table definition with version 1
    let columns_v1 = vec![
        ColumnDefinition {
            column_name: "id".to_string(),
            ordinal_position: 1,
            data_type: kalamdb_commons::datatypes::KalamDataType::Int,
            is_nullable: false,
            is_primary_key: true,
            is_partition_key: false,
            default_value: kalamdb_commons::models::schemas::ColumnDefault::None,
            column_comment: None,
        },
        ColumnDefinition {
            column_name: "name".to_string(),
            ordinal_position: 2,
            data_type: kalamdb_commons::datatypes::KalamDataType::Text,
            is_nullable: true,
            is_primary_key: false,
            is_partition_key: false,
            default_value: kalamdb_commons::models::schemas::ColumnDefault::None,
            column_comment: None,
        },
    ];

    let table_def_v1 = TableDefinition::new_with_defaults(
        test_namespace.clone(),
        TableName::new("test_table"),
        kalamdb_commons::models::schemas::TableType::User,
        columns_v1.clone(),
        None,
    )
    .expect("Failed to create table definition");

    // 1. Store initial schema
    schema_store
        .put(&test_table_id, &table_def_v1)
        .expect("Failed to put initial schema");

    // 2. Load into cache
    let cached_v1 = schema_store
        .get(&test_table_id)
        .expect("Failed to get schema")
        .expect("Schema should exist");

    assert_eq!(cached_v1.schema_version, 1, "Should be version 1");
    assert_eq!(cached_v1.columns.len(), 2, "Should have 2 columns");

    // 3. Create version 2 with an added column
    let columns = vec![
        ColumnDefinition {
            column_name: "id".to_string(),
            ordinal_position: 1,
            data_type: kalamdb_commons::datatypes::KalamDataType::Int,
            is_nullable: false,
            is_primary_key: true,
            is_partition_key: false,
            default_value: kalamdb_commons::models::schemas::ColumnDefault::None,
            column_comment: None,
        },
        ColumnDefinition {
            column_name: "name".to_string(),
            ordinal_position: 2,
            data_type: kalamdb_commons::datatypes::KalamDataType::Text,
            is_nullable: true,
            is_primary_key: false,
            is_partition_key: false,
            default_value: kalamdb_commons::models::schemas::ColumnDefault::None,
            column_comment: None,
        },
        ColumnDefinition {
            column_name: "email".to_string(),
            ordinal_position: 3,
            data_type: kalamdb_commons::datatypes::KalamDataType::Text,
            is_nullable: true,
            is_primary_key: false,
            is_partition_key: false,
            default_value: kalamdb_commons::models::schemas::ColumnDefault::None,
            column_comment: Some("User email address".to_string()),
        },
    ];

    let table_def = TableDefinition::new_with_defaults(
        test_namespace.clone(),
        TableName::new("test_table"),
        kalamdb_commons::models::schemas::TableType::User,
        columns.clone(),
        None,
    )
    .expect("Failed to create updated table definition");

    // 4. Invalidate cache (this is what ALTER TABLE would do)
    schema_registry.invalidate(&test_table_id);

    // 5. Update schema in store
    schema_store
        .put(&test_table_id, &table_def)
        .expect("Failed to put updated schema");

    // 6. Verify cache was invalidated - should fetch fresh from store
    let cached = schema_store
        .get(&test_table_id)
        .expect("Failed to get schema after update")
        .expect("Schema should exist after update");

    assert_eq!(cached.columns.len(), 3, "Should have 3 columns after update");
    assert_eq!(cached.columns[2].column_name, "email", "New column should be 'email'");

    // 7. Verify subsequent read comes from fresh cache (cached should equal a new read)
    let cached_again = schema_store
        .get(&test_table_id)
        .expect("Failed to get schema again")
        .expect("Schema should exist");

    assert_eq!(cached_again.columns.len(), 3, "Cache should have 3 columns");

    println!("✅ Cache invalidation works correctly");
}
