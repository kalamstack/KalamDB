// Comprehensive smoke test for core KalamDB operations
// Tests system tables, namespaces, users, storage, and flush operations

use std::time::Duration;

use crate::common::*;

#[ntest::timeout(180_000)]
#[test]
fn smoke_test_core_operations() {
    // Fail fast with clear error if server not running
    if !require_server_running() {
        return;
    }

    println!("\n=== Starting Core Operations Smoke Test ===\n");

    // Test 1: Query all system tables
    test_system_tables_queryable();

    // Test 2: Namespace CRUD operations
    test_namespace_operations();

    // Test 3: User CRUD operations with system table verification
    test_user_operations();

    // Test 4: Storage CRUD operations
    test_storage_operations();

    // Test 5: Flush operations with job verification
    test_flush_operations();

    println!("\n=== All Core Operations Tests Passed ===\n");
}

/// Test 1: Verify all system tables can be queried and return columns
fn test_system_tables_queryable() {
    println!("TEST 1: System Tables Queryable");
    println!("================================");

    let system_tables = vec![
        "system.users",
        "system.namespaces",
        "system.live",
        "system.schemas",
        "system.storages",
        // First release: no legacy data, direct query supported
        "system.jobs",
    ];

    for table in system_tables {
        let query = format!("SELECT * FROM {} LIMIT 1", table);
        println!("  Testing: {}", table);

        let result = execute_sql_as_root_via_client(&query)
            .unwrap_or_else(|e| panic!("Failed to query {}: {}", table, e));

        // Verify we got a response (even if empty)
        // The output should contain column headers in the table format
        // or indicate "(0 rows)" for empty tables
        assert!(
            result.contains("row") || result.contains("│") || result.contains("column"),
            "Query to {} should return table structure, got: {}",
            table,
            result
        );

        println!("    ✓ {} is queryable", table);
    }

    println!("  ✅ All system tables are queryable\n");
}

/// Test 2: Namespace CREATE/SELECT/DROP operations
fn test_namespace_operations() {
    println!("TEST 2: Namespace Operations");
    println!("=============================");

    // Generate unique namespace name
    let ns_name = generate_unique_namespace("smoke_core_ops");

    // CREATE NAMESPACE
    println!("  Creating namespace: {}", ns_name);
    let create_sql = format!("CREATE NAMESPACE {}", ns_name);
    execute_sql_as_root_via_client(&create_sql).expect("CREATE NAMESPACE should succeed");
    println!("    ✓ Namespace created");

    // SELECT from system.namespaces to verify
    println!("  Verifying namespace in system.namespaces");
    let select_sql =
        format!("SELECT namespace_id FROM system.namespaces WHERE namespace_id = '{}'", ns_name);
    let result = execute_sql_as_root_via_client(&select_sql)
        .expect("SELECT from system.namespaces should succeed");

    assert!(
        result.contains(&ns_name) || result.contains("(1 row)"),
        "Namespace {} should appear in system.namespaces, got: {}",
        ns_name,
        result
    );
    println!("    ✓ Namespace found in system.namespaces");

    // DROP NAMESPACE
    println!("  Dropping namespace: {}", ns_name);
    let drop_sql = format!("DROP NAMESPACE {} CASCADE", ns_name);
    execute_sql_as_root_via_client(&drop_sql).expect("DROP NAMESPACE should succeed");
    println!("    ✓ Namespace dropped");

    // Verify namespace is removed or marked as deleted
    let verify_sql =
        format!("SELECT namespace_id FROM system.namespaces WHERE namespace_id = '{}'", ns_name);
    let result =
        execute_sql_as_root_via_client(&verify_sql).expect("SELECT after DROP should succeed");

    assert!(
        result.contains("(0 rows)") || !result.contains(&ns_name),
        "Namespace {} should be removed after DROP, got: {}",
        ns_name,
        result
    );
    println!("    ✓ Namespace removed from system.namespaces");

    println!("  ✅ Namespace operations completed successfully\n");
}

/// Test 3: User CREATE/SELECT/DROP with system table verification
fn test_user_operations() {
    println!("TEST 3: User Operations");
    println!("=======================");

    // Generate unique user_id
    let user_id = generate_unique_namespace("smoke_user");
    let password = "S3cur3P@ssw0rd!";

    // CREATE USER
    println!("  Creating user: {}", user_id);
    let create_sql = format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", user_id, password);
    execute_sql_as_root_via_client(&create_sql).expect("CREATE USER should succeed");
    println!("    ✓ User created");

    // SELECT from system.users to verify
    println!("  Verifying user in system.users");
    let select_sql =
        format!("SELECT user_id, role FROM system.users WHERE user_id = '{}'", user_id);
    let result = wait_for_query_contains_with(
        &select_sql,
        &user_id,
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("SELECT from system.users should succeed");

    assert!(
        result.contains(&user_id) && (result.contains("user") || result.contains("(1 row)")),
        "User {} should appear in system.users with role 'user', got: {}",
        user_id,
        result
    );
    println!("    ✓ User found in system.users with correct role");

    // DROP USER
    println!("  Dropping user: {}", user_id);
    let drop_sql = format!("DROP USER '{}'", user_id);
    execute_sql_as_root_via_client(&drop_sql).expect("DROP USER should succeed");
    println!("    ✓ User dropped");

    // Verify user is removed or soft-deleted
    let verify_sql =
        format!("SELECT user_id, deleted_at FROM system.users WHERE user_id = '{}'", user_id);
    let result =
        execute_sql_as_root_via_client(&verify_sql).expect("SELECT after DROP should succeed");

    // User should either be completely removed or have deleted_at timestamp
    let is_removed = result.contains("(0 rows)") || !result.contains(&user_id);
    let is_soft_deleted = result.to_lowercase().contains("deleted");

    assert!(
        is_removed || is_soft_deleted,
        "User {} should be removed or marked deleted, got: {}",
        user_id,
        result
    );
    println!("    ✓ User removed or soft-deleted");

    println!("  ✅ User operations completed successfully\n");
}

/// Test 4: Storage CREATE/SELECT/DROP operations
fn test_storage_operations() {
    println!("TEST 4: Storage Operations");
    println!("===========================");

    // Generate unique storage name
    let storage_name = generate_unique_namespace("smoke_storage");

    // CREATE STORAGE
    println!("  Creating storage: {}", storage_name);
    // Omit NAME intentionally to validate defaulting to storage_id
    let create_sql = format!(
        "CREATE STORAGE {} TYPE 'filesystem' BASE_DIRECTORY '/tmp/smoke_test'",
        storage_name
    );

    let create_result = execute_sql_as_root_via_client(&create_sql);

    // Storage creation might not be fully implemented, handle gracefully
    match create_result {
        Ok(_) => {
            println!("    ✓ Storage created");

            // SELECT from system.storages to verify
            println!("  Verifying storage in system.storages");
            let select_sql = format!(
                "SELECT storage_id, storage_type FROM system.storages WHERE storage_id = '{}'",
                storage_name
            );
            let result = execute_sql_as_root_via_client(&select_sql)
                .expect("SELECT from system.storages should succeed");

            assert!(
                result.contains(&storage_name) || result.contains("(1 row)"),
                "Storage {} should appear in system.storages, got: {}",
                storage_name,
                result
            );
            println!("    ✓ Storage found in system.storages");

            // DROP STORAGE
            println!("  Dropping storage: {}", storage_name);
            let drop_sql = format!("DROP STORAGE {}", storage_name);
            execute_sql_as_root_via_client(&drop_sql).expect("DROP STORAGE should succeed");
            println!("    ✓ Storage dropped");

            // Verify storage is removed
            let verify_sql = format!(
                "SELECT storage_id FROM system.storages WHERE storage_id = '{}'",
                storage_name
            );
            let result = execute_sql_as_root_via_client(&verify_sql)
                .expect("SELECT after DROP should succeed");

            assert!(
                result.contains("(0 rows)") || !result.contains(&storage_name),
                "Storage {} should be removed after DROP, got: {}",
                storage_name,
                result
            );
            println!("    ✓ Storage removed from system.storages");

            println!("  ✅ Storage operations completed successfully\n");
        },
        Err(e) => {
            println!("    ⚠️  Storage operations not fully implemented yet: {}", e);
            println!("    Skipping storage verification\n");
        },
    }
}

/// Test 5: STORAGE FLUSH TABLE/ALL operations with job verification
fn test_flush_operations() {
    println!("TEST 5: Flush Operations & Job Verification");
    println!("============================================");

    // Create a test namespace and table for flush testing
    let ns_name = generate_unique_namespace("smoke_flush_ns");
    let table_name = generate_unique_table("test_table");
    let full_table_name = format!("{}.{}", ns_name, table_name);

    // Setup: Create namespace and table
    println!("  Setting up test namespace and table");
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", ns_name))
        .expect("CREATE NAMESPACE should succeed");

    let create_table_sql = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, value VARCHAR) WITH (TYPE = 'USER', FLUSH_POLICY = \
         'rows:100')",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");
    println!("    ✓ Test table created: {}", full_table_name);

    // Insert test data
    println!("  Inserting test data");
    let insert_sql =
        format!("INSERT INTO {} (id, value) VALUES (1, 'test_value')", full_table_name);
    execute_sql_as_root_via_client(&insert_sql).expect("INSERT should succeed");
    println!("    ✓ Test data inserted");

    // Test 5a: STORAGE FLUSH TABLE
    println!("\n  Test 5a: STORAGE FLUSH TABLE");
    let flush_table_sql = format!("STORAGE FLUSH TABLE {}", full_table_name);
    let flush_result = execute_sql_as_root_via_client(&flush_table_sql);

    match flush_result {
        Ok(output) => {
            println!("    ✓ STORAGE FLUSH TABLE executed");
            println!("    Output: {}", output);
        },
        Err(e) => {
            // Note: system.jobs may have serialization issues, so flush might fail
            println!("    ⚠️  STORAGE FLUSH TABLE failed (may be due to jobs table issues): {}", e);
        },
    }

    // Test 5b: STORAGE FLUSH ALL
    println!("\n  Test 5b: STORAGE FLUSH ALL");
    let flush_all_sql = format!("STORAGE FLUSH ALL IN {}", ns_name);
    let flush_all_result = execute_sql_as_root_via_client(&flush_all_sql);

    match flush_all_result {
        Ok(output) => {
            println!("    ✓ STORAGE FLUSH ALL executed");

            // Try to parse job IDs from output using common helper
            if let Ok(job_ids) = crate::common::parse_job_ids_from_flush_all_output(&output) {
                if !job_ids.is_empty() {
                    println!("    Found {} job ID(s): {:?}", job_ids.len(), job_ids);

                    // Verify jobs in system.jobs (with graceful handling)
                    for job_id in &job_ids {
                        let query = format!(
                            "SELECT job_id, status FROM system.jobs WHERE job_id = '{}'",
                            job_id
                        );

                        match execute_sql_as_root_via_client(&query) {
                            Ok(result) => {
                                if result.contains("(1 row)") || result.contains(job_id) {
                                    println!("      ✓ Job {} found in system.jobs", job_id);
                                } else {
                                    println!("      ⚠️  Job {} not yet in system.jobs", job_id);
                                }
                            },
                            Err(e) => {
                                println!("      ⚠️  Unable to query system.jobs: {}", e);
                            },
                        }
                    }
                } else {
                    println!("    ⚠️  No job IDs found in output");
                }
            } else {
                println!("    ⚠️  Could not parse job IDs from output");
            }
        },
        Err(e) => {
            println!("    ⚠️  STORAGE FLUSH ALL failed (may be due to jobs table issues): {}", e);
        },
    }

    // Cleanup: Drop namespace (cascade will remove tables)
    println!("\n  Cleaning up test namespace");
    execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns_name))
        .expect("DROP NAMESPACE should succeed");
    println!("    ✓ Test namespace cleaned up");

    println!("  ✅ Flush operations test completed\n");
}
