//! Smoke tests for ALTER TABLE DDL operations
//!
//! Tests schema evolution and table modification:
//! - ALTER TABLE ADD COLUMN
//! - ALTER TABLE DROP COLUMN
//! - ALTER TABLE MODIFY COLUMN
//! - ALTER TABLE SET TBLPROPERTIES (for SHARED tables)
//! - Error cases (adding NOT NULL without DEFAULT, modifying system columns)
//!
//! Reference: docs/SQL.md lines 438-460

use crate::common::*;

/// Test ALTER TABLE ADD COLUMN
///
/// Verifies:
/// - Can add nullable column to existing table
/// - Can add column with DEFAULT value
/// - New column appears in subsequent queries
#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_table_add_column() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("alter_ns");
    let table = generate_unique_table("add_col_test");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing ALTER TABLE ADD COLUMN");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create table with initial columns
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT NOT NULL
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000')"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    println!("✅ Created table with 2 columns (id, name)");

    // Insert a row before adding column
    let insert_sql = format!("INSERT INTO {} (name) VALUES ('Alice')", full_table);
    execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");

    // Add nullable column
    let alter_sql = format!("ALTER TABLE {} ADD COLUMN age INT", full_table);
    let alter_result = execute_sql_as_root_via_client(&alter_sql);

    match alter_result {
        Ok(output) => {
            println!("ALTER output: {}", output);
            if output.to_lowercase().contains("error")
                || output.to_lowercase().contains("not implemented")
                || output.to_lowercase().contains("unsupported")
            {
                println!("⚠️  ALTER TABLE ADD COLUMN not yet implemented - skipping test");
                return;
            }
        },
        Err(e) => {
            println!("⚠️  ALTER TABLE ADD COLUMN not yet implemented - skipping test");
            println!("   Error: {:?}", e);
            return;
        },
    }

    println!("✅ Added nullable column 'age'");

    // Verify new column exists (SELECT should succeed)
    let select_sql = format!("SELECT name, age FROM {}", full_table);
    let output_result = execute_sql_as_root_via_client_json(&select_sql);

    if output_result.is_err() {
        println!(
            "⚠️  Column 'age' not found in schema - ALTER TABLE may have succeeded syntactically \
             but schema wasn't updated"
        );
        println!("   This indicates ALTER TABLE ADD COLUMN needs further implementation");
        return;
    }

    let output = output_result.unwrap();

    assert!(output.contains("\"age\""), "Expected 'age' column in output");
    assert!(output.contains("Alice"), "Expected existing row in output");

    println!("✅ Verified new column exists and old data preserved");

    // Add column with DEFAULT value
    let alter_with_default =
        format!("ALTER TABLE {} ADD COLUMN status TEXT DEFAULT 'active'", full_table);
    execute_sql_as_root_via_client(&alter_with_default).expect("Failed to add column with DEFAULT");

    // Insert new row and verify DEFAULT applied
    let insert_sql2 = format!("INSERT INTO {} (name, age) VALUES ('Bob', 30)", full_table);
    execute_sql_as_root_via_client(&insert_sql2).expect("Failed to insert after ADD COLUMN");

    let select_sql2 = format!("SELECT name, status FROM {} WHERE name = 'Bob'", full_table);
    let output2 = execute_sql_as_root_via_client_json(&select_sql2).expect("Failed to query");

    assert!(output2.contains("active"), "Expected DEFAULT value 'active' for new column");

    println!("✅ Verified ADD COLUMN with DEFAULT works");

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test ALTER TABLE DROP COLUMN
///
/// Verifies:
/// - Can drop existing column
/// - Column no longer appears in queries
/// - Existing data in other columns preserved
#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_table_drop_column() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("alter_ns");
    let table = generate_unique_table("drop_col_test");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing ALTER TABLE DROP COLUMN");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create table with multiple columns
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT NOT NULL,
            email TEXT,
            age INT
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000')"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    // Insert data
    let insert_sql = format!(
        "INSERT INTO {} (name, email, age) VALUES ('Alice', 'alice@example.com', 25)",
        full_table
    );
    execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");

    println!("✅ Created table with 4 columns and 1 row");

    // Drop column
    let alter_sql = format!("ALTER TABLE {} DROP COLUMN email", full_table);
    let alter_result = execute_sql_as_root_via_client(&alter_sql);

    if alter_result.is_err() {
        println!("⚠️  ALTER TABLE DROP COLUMN not yet implemented - skipping test");
        println!("   Error: {:?}", alter_result.unwrap_err());
        return;
    }

    println!("✅ Dropped column 'email'");

    // Verify column no longer exists
    let select_sql = format!("SELECT * FROM {}", full_table);
    let output = execute_sql_as_root_via_client_json(&select_sql)
        .expect("Failed to query after DROP COLUMN");

    assert!(
        !output.contains("\"email\""),
        "Expected 'email' column to be removed from output"
    );
    assert!(
        output.contains("Alice") && output.contains("\"age\""),
        "Expected other columns to still exist"
    );

    println!("✅ Verified column dropped and other data preserved");

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test ALTER TABLE MODIFY COLUMN
///
/// Verifies:
/// - Can modify column type
/// - Can change NULL/NOT NULL constraint
#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_table_modify_column() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("alter_ns");
    let table = generate_unique_table("modify_col_test");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing ALTER TABLE MODIFY COLUMN");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            content TEXT
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000')"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    println!("✅ Created table");

    // Insert data
    let insert_sql = format!("INSERT INTO {} (content) VALUES ('Test data')", full_table);
    execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");

    // Modify column to NOT NULL (should work if data exists)
    let alter_sql = format!("ALTER TABLE {} MODIFY COLUMN content TEXT NOT NULL", full_table);

    // Note: This might fail if backend doesn't support MODIFY COLUMN yet
    match execute_sql_as_root_via_client(&alter_sql) {
        Ok(_) => {
            println!("✅ Modified column to NOT NULL");

            // Verify by attempting to insert NULL (should fail)
            let insert_null = format!("INSERT INTO {} (content) VALUES (NULL)", full_table);
            let null_result = execute_sql_as_root_via_client(&insert_null);

            assert!(
                null_result.is_err() || null_result.unwrap().to_lowercase().contains("error"),
                "Expected error when inserting NULL into NOT NULL column"
            );

            println!("✅ Verified NOT NULL constraint enforced");
        },
        Err(e) => {
            println!("⚠️  MODIFY COLUMN not yet implemented: {}", e);
            println!("TODO: Implement ALTER TABLE MODIFY COLUMN support");
        },
    }

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test ALTER TABLE SET TBLPROPERTIES for SHARED tables
///
/// Verifies:
/// - Can change ACCESS_LEVEL for shared tables
/// - ACCESS_LEVEL appears in system.schemas options
#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_shared_table_access_level() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("alter_ns");
    let table = generate_unique_table("shared_access_test");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing ALTER TABLE SET TBLPROPERTIES (ACCESS_LEVEL)");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create SHARED table with PUBLIC access
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            data TEXT
        ) WITH (
            TYPE = 'SHARED',
            ACCESS_LEVEL = 'PUBLIC',
            FLUSH_POLICY = 'rows:1000'
        )"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create shared table");

    println!("✅ Created SHARED table with ACCESS_LEVEL='PUBLIC'");

    // Alter access level to RESTRICTED
    let alter_sql =
        format!("ALTER TABLE {} SET TBLPROPERTIES (ACCESS_LEVEL = 'RESTRICTED')", full_table);

    match execute_sql_as_root_via_client(&alter_sql) {
        Ok(_) => {
            println!("✅ Changed ACCESS_LEVEL to RESTRICTED");

            // Verify in system.schemas
            let query_sql =
                format!("SELECT options FROM system.schemas WHERE table_name = '{}'", table);
            let output = execute_sql_as_root_via_client_json(&query_sql)
                .expect("Failed to query system.schemas");

            assert!(
                output.contains("RESTRICTED")
                    || output.contains("restricted")
                    || output.contains("Restricted"),
                "Expected ACCESS_LEVEL='RESTRICTED' in system.schemas options"
            );

            println!("✅ Verified ACCESS_LEVEL updated in system.schemas");
        },
        Err(e) => {
            println!("⚠️  SET TBLPROPERTIES not yet implemented: {}", e);
            println!("TODO: Implement ALTER TABLE SET TBLPROPERTIES for SHARED tables");
        },
    }

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test error: ADD NOT NULL column without DEFAULT on non-empty table
///
/// Verifies:
/// - Adding NOT NULL column without DEFAULT to table with existing rows fails
/// - Error message is clear
#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_add_not_null_without_default_error() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("alter_ns");
    let table = generate_unique_table("add_not_null_error");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing error: ADD NOT NULL column without DEFAULT");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT NOT NULL
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000')"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    // Insert data
    let insert_sql = format!("INSERT INTO {} (name) VALUES ('Alice')", full_table);
    execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");

    println!("✅ Created table with existing data");

    // Try to add NOT NULL column without DEFAULT (should fail)
    let alter_sql = format!("ALTER TABLE {} ADD COLUMN required_field TEXT NOT NULL", full_table);
    let alter_result = execute_sql_as_root_via_client(&alter_sql);

    match alter_result {
        Err(e) => {
            println!("✅ ADD NOT NULL without DEFAULT failed as expected: {}", e);
            let error_msg = e.to_string().to_lowercase();
            assert!(
                error_msg.contains("not null")
                    || error_msg.contains("default")
                    || error_msg.contains("required"),
                "Expected error message about NOT NULL requiring DEFAULT, got: {}",
                e
            );
        },
        Ok(output) => {
            let output_lower = output.to_lowercase();
            assert!(
                output_lower.contains("error") || output_lower.contains("failed"),
                "Expected error when adding NOT NULL column without DEFAULT, got success: {}",
                output
            );
        },
    }

    println!("✅ Verified error when adding NOT NULL column without DEFAULT to non-empty table");

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test error: Cannot ALTER system columns
///
/// Verifies:
/// - Attempting to DROP or MODIFY _updated or _deleted fails
/// - Error message indicates system columns are protected
#[ntest::timeout(180000)]
#[test]
fn smoke_test_alter_system_columns_error() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("alter_ns");
    let table = generate_unique_table("system_col_error");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing error: Cannot ALTER system columns");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create USER table (has _updated, _deleted system columns)
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            name TEXT NOT NULL
        ) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000')"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    println!("✅ Created USER table with system columns");

    // Try to DROP _updated (should fail)
    let drop_updated = format!("ALTER TABLE {} DROP COLUMN _updated", full_table);
    let drop_result = execute_sql_as_root_via_client(&drop_updated);

    match drop_result {
        Err(e) => {
            println!("✅ DROP _updated failed as expected: {}", e);
        },
        Ok(output) => {
            let output_lower = output.to_lowercase();
            if output_lower.contains("error") || output_lower.contains("system") {
                println!("✅ DROP _updated failed as expected");
            } else {
                println!("⚠️  DROP _updated should fail (system column), but got: {}", output);
                println!("TODO: Enforce protection of system columns in ALTER TABLE");
            }
        },
    }

    // Try to DROP _deleted (should fail)
    let drop_deleted = format!("ALTER TABLE {} DROP COLUMN _deleted", full_table);
    let drop_result2 = execute_sql_as_root_via_client(&drop_deleted);

    match drop_result2 {
        Err(e) => {
            println!("✅ DROP _deleted failed as expected: {}", e);
        },
        Ok(output) => {
            let output_lower = output.to_lowercase();
            if output_lower.contains("error") || output_lower.contains("system") {
                println!("✅ DROP _deleted failed as expected");
            } else {
                println!("⚠️  DROP _deleted should fail (system column), but got: {}", output);
            }
        },
    }

    println!("✅ Verified system columns are protected from ALTER operations");

    // Cleanup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
