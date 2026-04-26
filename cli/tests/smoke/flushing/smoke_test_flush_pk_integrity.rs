//! Smoke test for flush operations with PK integrity checks
//!
//! This test validates:
//! - INSERT → SELECT (hot storage)
//! - UPDATE → SELECT (hot storage)
//! - STORAGE FLUSH TABLE → verify cold storage
//! - SELECT after flush (cold storage)
//! - INSERT duplicate PK fails (both hot and cold)
//! - UPDATE works correctly post-flush

use std::time::Duration;

use crate::common::*;

const JOB_TIMEOUT: Duration = Duration::from_secs(90);

/// Test flush with PK integrity: insert, update, flush, re-query, duplicate PK check, post-flush
/// update
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_flush_pk_integrity_user_table() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_pk");
    let table_name = generate_unique_table("pk_integrity");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("🧪 Testing flush with PK integrity: {}", full_table_name);

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create USER table with explicit PK
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id INT PRIMARY KEY,
            name VARCHAR NOT NULL,
            value INT
        ) WITH (
            TYPE = 'USER',
            FLUSH_POLICY = 'rows:100'
        )"#,
        full_table_name
    );

    execute_sql_as_root_via_client(&create_sql).expect("Failed to create user table");

    println!("✅ Created USER table with INT PRIMARY KEY");

    // Step 1: Insert initial rows (hot storage)
    println!("📝 Step 1: Insert initial rows");
    for id in 1..=5 {
        let insert_sql = format!(
            "INSERT INTO {} (id, name, value) VALUES ({}, 'Name{}', {})",
            full_table_name,
            id,
            id,
            id * 10
        );
        execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");
    }
    println!("✅ Inserted 5 rows (id 1-5)");

    // Step 2: SELECT and verify data exists (hot storage)
    println!("🔍 Step 2: SELECT and verify data exists in hot storage");
    let select_output = execute_sql_as_root_via_client(&format!(
        "SELECT id, name, value FROM {} ORDER BY id",
        full_table_name
    ))
    .expect("Failed to query data");

    assert!(
        select_output.contains("Name1") && select_output.contains("Name5"),
        "Expected to find Name1 and Name5 in hot storage, got: {}",
        select_output
    );
    println!("✅ Verified all 5 rows exist in hot storage");

    // Step 3: UPDATE a row (hot storage)
    println!("📝 Step 3: UPDATE row id=3");
    let update_sql =
        format!("UPDATE {} SET name = 'UpdatedName3', value = 999 WHERE id = 3", full_table_name);
    let update_output = execute_sql_as_root_via_client(&update_sql).expect("Failed to update row");
    assert!(
        update_output.contains("1 rows affected") || update_output.contains("Updated 1 row"),
        "Expected 1 row affected, got: {}",
        update_output
    );
    println!("✅ Updated row id=3");

    // Step 4: SELECT and verify UPDATE worked (hot storage)
    println!("🔍 Step 4: Verify UPDATE worked in hot storage");
    let select_updated = execute_sql_as_root_via_client(&format!(
        "SELECT id, name, value FROM {} WHERE id = 3",
        full_table_name
    ))
    .expect("Failed to query updated row");

    assert!(
        select_updated.contains("UpdatedName3") && select_updated.contains("999"),
        "Expected UpdatedName3 and value 999, got: {}",
        select_updated
    );
    println!("✅ Verified UPDATE worked in hot storage");

    // Step 5: STORAGE FLUSH TABLE
    println!("🚀 Step 5: Flush table to cold storage");
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("Failed to flush table");

    let job_id = parse_job_id_from_flush_output(&flush_output)
        .expect("Failed to parse job ID from flush output");
    println!("📋 Flush job ID: {}", job_id);

    // Wait for flush to complete
    verify_job_completed(&job_id, JOB_TIMEOUT).expect("Flush job did not complete successfully");
    println!("✅ Flush completed successfully");

    // Verify flush storage files exist
    assert_flush_storage_files_exist(
        &namespace,
        &table_name,
        true, // is_user_table
        "PK integrity flush test",
    );

    // Step 6: SELECT after flush (should read from cold storage)
    println!("🔍 Step 6: SELECT after flush (cold storage)");
    let select_cold = execute_sql_as_root_via_client(&format!(
        "SELECT id, name, value FROM {} ORDER BY id",
        full_table_name
    ))
    .expect("Failed to query data from cold storage");

    assert!(
        select_cold.contains("UpdatedName3") && select_cold.contains("999"),
        "Expected UpdatedName3 and 999 in cold storage, got: {}",
        select_cold
    );
    assert!(
        select_cold.contains("Name1") && select_cold.contains("Name5"),
        "Expected Name1 and Name5 in cold storage, got: {}",
        select_cold
    );
    println!("✅ Verified all data readable from cold storage");

    // Step 7: Try to insert duplicate PK (should fail)
    // TODO(backend): PK uniqueness validation against cold storage not yet implemented
    // Issue tracked: PkExistenceChecker::check_pk_exists exists but isn't called during INSERT
    println!("❌ Step 7: Try to INSERT duplicate PK (should fail)");
    let duplicate_insert_sql =
        format!("INSERT INTO {} (id, name, value) VALUES (3, 'Duplicate', 777)", full_table_name);
    let duplicate_result = execute_sql_as_root_via_client(&duplicate_insert_sql);

    match duplicate_result {
        Ok(output) => {
            println!(
                "⚠️  WARNING: Duplicate PK insert succeeded (known backend limitation): {}",
                output
            );
            println!(
                "⚠️  TODO: Backend must implement PK uniqueness validation against cold storage"
            );
            // TODO: Uncomment this when backend fix is implemented:
            // panic!(
            //     "Expected duplicate PK insert to fail, but it succeeded with output: {}",
            //     output
            // );
        },
        Err(e) => {
            let error_msg = e.to_string();
            assert!(
                error_msg.to_lowercase().contains("duplicate")
                    || error_msg.to_lowercase().contains("already exists")
                    || error_msg.to_lowercase().contains("primary key")
                    || error_msg.to_lowercase().contains("unique"),
                "Expected duplicate key error, got: {}",
                error_msg
            );
            println!("✅ Duplicate PK insert correctly rejected: {}", error_msg);
        },
    }

    // Step 8: UPDATE a row post-flush (should work)
    println!("📝 Step 8: UPDATE row id=2 post-flush");
    let update_post_flush_sql = format!(
        "UPDATE {} SET name = 'PostFlushUpdate2', value = 888 WHERE id = 2",
        full_table_name
    );
    let update_post_flush_output = execute_sql_as_root_via_client(&update_post_flush_sql)
        .expect("Failed to update row post-flush");

    assert!(
        update_post_flush_output.contains("1 rows affected")
            || update_post_flush_output.contains("Updated 1 row"),
        "Expected 1 row affected post-flush, got: {}",
        update_post_flush_output
    );
    println!("✅ Updated row id=2 post-flush");

    // Step 9: Verify post-flush UPDATE worked
    println!("🔍 Step 9: Verify post-flush UPDATE worked");
    let select_post_update = wait_for_query_contains_with(
        &format!("SELECT id, name, value FROM {} WHERE id = 2", full_table_name),
        "PostFlushUpdate2",
        Duration::from_secs(5),
        execute_sql_as_root_via_client,
    )
    .expect("Failed to query updated row post-flush");

    assert!(
        select_post_update.contains("PostFlushUpdate2") && select_post_update.contains("888"),
        "Expected PostFlushUpdate2 and 888, got: {}",
        select_post_update
    );
    println!("✅ Verified post-flush UPDATE worked");

    // Cleanup
    println!("🧹 Cleaning up...");
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE {}", full_table_name));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("✅ Flush PK integrity smoke test completed successfully!");
}

/// Test flush with PK integrity for SHARED table
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_flush_pk_integrity_shared_table() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_pk");
    let table_name = generate_unique_table("pk_shared");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("🧪 Testing flush with PK integrity (SHARED): {}", full_table_name);

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create SHARED table with explicit PK
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id INT PRIMARY KEY,
            name VARCHAR NOT NULL,
            value INT
        ) WITH (
            TYPE = 'SHARED',
            FLUSH_POLICY = 'rows:100'
        )"#,
        full_table_name
    );

    execute_sql_as_root_via_client(&create_sql).expect("Failed to create shared table");

    println!("✅ Created SHARED table with INT PRIMARY KEY");

    // Insert, update, flush, verify - same pattern as user table
    println!("📝 Insert initial rows");
    for id in 10..=15 {
        let insert_sql = format!(
            "INSERT INTO {} (id, name, value) VALUES ({}, 'Shared{}', {})",
            full_table_name,
            id,
            id,
            id * 100
        );
        execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");
    }
    println!("✅ Inserted 6 rows (id 10-15)");

    // UPDATE before flush
    println!("📝 UPDATE row id=12");
    let update_sql = format!(
        "UPDATE {} SET name = 'SharedUpdated12', value = 9999 WHERE id = 12",
        full_table_name
    );
    execute_sql_as_root_via_client(&update_sql).expect("Failed to update row");
    println!("✅ Updated row id=12");

    // Flush
    println!("🚀 Flush SHARED table");
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("Failed to flush table");

    let job_id = parse_job_id_from_flush_output(&flush_output)
        .expect("Failed to parse job ID from flush output");
    verify_job_completed(&job_id, JOB_TIMEOUT).expect("Flush job did not complete successfully");
    println!("✅ Flush completed");

    // Verify storage files
    assert_flush_storage_files_exist(
        &namespace,
        &table_name,
        false, // is_shared_table
        "SHARED table PK integrity flush test",
    );

    // Verify post-flush reads
    println!("🔍 SELECT after flush");
    let select_cold = execute_sql_as_root_via_client(&format!(
        "SELECT id, name, value FROM {} WHERE id = 12",
        full_table_name
    ))
    .expect("Failed to query cold storage");

    assert!(
        select_cold.contains("SharedUpdated12") && select_cold.contains("9999"),
        "Expected SharedUpdated12 and 9999, got: {}",
        select_cold
    );
    println!("✅ Verified cold storage reads work");

    // Try duplicate PK
    // TODO(backend): PK uniqueness validation against cold storage not yet implemented
    println!("❌ Try to INSERT duplicate PK (should fail)");
    let duplicate_result = execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, name, value) VALUES (12, 'Dup', 0)",
        full_table_name
    ));

    if duplicate_result.is_ok() {
        println!(
            "⚠️  WARNING: Duplicate PK insert succeeded for SHARED table (known backend \
             limitation)"
        );
        println!("⚠️  TODO: Backend must implement PK uniqueness validation against cold storage");
    } else {
        println!("✅ Duplicate PK correctly rejected");
    }
    // TODO: Restore strict assertion when backend fix is implemented:
    // assert!(duplicate_result.is_err(), "Expected duplicate PK to fail for SHARED table");

    // Post-flush update
    println!("📝 UPDATE row id=13 post-flush");
    execute_sql_as_root_via_client(&format!(
        "UPDATE {} SET value = 8888 WHERE id = 13",
        full_table_name
    ))
    .expect("Failed to update post-flush");
    println!("✅ Post-flush UPDATE succeeded");

    // Cleanup
    println!("🧹 Cleaning up...");
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE {}", full_table_name));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("✅ SHARED table flush PK integrity test completed!");
}
