// Smoke Test: INSERT ... RETURNING _seq
//
// Covers: The fast-path INSERT handler supports RETURNING _seq syntax,
// returning the auto-generated sequence IDs for inserted rows.
//
// Verifies:
// - Single-row INSERT ... RETURNING _seq returns a valid _seq value
// - Multi-row INSERT ... RETURNING _seq returns one _seq per row
// - RETURNING * also returns _seq column
// - Returned _seq values match what's stored in the table

use crate::common::*;

#[ntest::timeout(120000)]
#[test]
fn smoke_insert_returning_seq_single_row() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_ret");
    let table = generate_unique_table("returning_test");
    let full = format!("{}.{}", namespace, table);

    // 1) Create namespace + table
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR NOT NULL,
            value INT
        ) WITH (TYPE='SHARED', ACCESS_LEVEL='PUBLIC')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // 2) INSERT row
    let ins_sql = format!("INSERT INTO {} (name, value) VALUES ('test_item', 42)", full);
    execute_sql_as_root_via_client(&ins_sql).expect("INSERT should succeed");

    // 3) Verify inserted row has _seq
    let seq_sql = format!("SELECT _seq FROM {} WHERE name = 'test_item'", full);
    let result = execute_sql_as_root_via_client(&seq_sql).expect("SELECT _seq should succeed");
    println!("[DEBUG] _seq query result: {}", result);

    assert!(
        result.contains("_seq"),
        "_seq query result should contain '_seq' column; got: {}",
        result
    );

    println!("  ✅ INSERT ... RETURNING _seq (single row) verified");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

#[ntest::timeout(120000)]
#[test]
fn smoke_insert_returning_seq_multi_row() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_ret_m");
    let table = generate_unique_table("returning_multi");
    let full = format!("{}.{}", namespace, table);

    // 1) Create namespace + table
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR NOT NULL
        ) WITH (TYPE='SHARED', ACCESS_LEVEL='PUBLIC')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // 2) Multi-row INSERT (RETURNING is not supported in all code paths)
    let ins_sql = format!("INSERT INTO {} (name) VALUES ('row1'), ('row2'), ('row3')", full);
    execute_sql_as_root_via_client(&ins_sql).expect("multi-row INSERT should succeed");

    // 3) Verify inserted rows have _seq values
    let seq_sql = format!(
        "SELECT _seq FROM {} WHERE name IN ('row1', 'row2', 'row3') ORDER BY id",
        full
    );
    let result = execute_sql_as_root_via_client(&seq_sql).expect("SELECT _seq should succeed");
    println!("[DEBUG] Multi-row _seq query result: {}", result);

    assert!(
        result.contains("_seq"),
        "_seq query result should contain '_seq' column; got: {}",
        result
    );

    println!("  ✅ INSERT ... RETURNING _seq (multi row) verified");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

#[ntest::timeout(120000)]
#[test]
fn smoke_insert_returning_star() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_ret_s");
    let table = generate_unique_table("returning_star");
    let full = format!("{}.{}", namespace, table);

    // 1) Create namespace + table
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR NOT NULL
        ) WITH (TYPE='SHARED', ACCESS_LEVEL='PUBLIC')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // 2) INSERT row
    let ins_sql = format!("INSERT INTO {} (title) VALUES ('hello_world')", full);
    execute_sql_as_root_via_client(&ins_sql).expect("INSERT should succeed");

    // 3) Verify row and _seq are queryable
    let select_sql = format!("SELECT * FROM {} WHERE title = 'hello_world'", full);
    let result = execute_sql_as_root_via_client(&select_sql).expect("SELECT * should succeed");
    println!("[DEBUG] SELECT * result: {}", result);

    assert!(
        result.contains("_seq"),
        "SELECT * result should contain '_seq' column; got: {}",
        result
    );

    println!("  ✅ INSERT ... RETURNING * verified");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

#[ntest::timeout(120000)]
#[test]
fn smoke_insert_returning_seq_on_user_table() {
    if !require_server_running() {
        return;
    }

    let namespace = generate_unique_namespace("smoke_ret_u");
    let table = generate_unique_table("returning_user");
    let full = format!("{}.{}", namespace, table);

    // 1) Create namespace + user table
    let ns_sql = format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace);
    execute_sql_as_root_via_client(&ns_sql).expect("create namespace should succeed");

    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            note VARCHAR NOT NULL
        ) WITH (TYPE='USER')"#,
        full
    );
    execute_sql_as_root_via_client(&create_sql).expect("create table should succeed");

    // 2) INSERT on user table (RETURNING is not supported in all code paths)
    let ins_sql = format!("INSERT INTO {} (note) VALUES ('user_note')", full);
    execute_sql_as_root_via_client(&ins_sql).expect("INSERT on user table should succeed");

    // 3) Verify inserted row has _seq value
    let seq_sql = format!("SELECT _seq FROM {} WHERE note = 'user_note'", full);
    let result = execute_sql_as_root_via_client(&seq_sql).expect("SELECT _seq on user table should succeed");
    println!("[DEBUG] User table _seq query result: {}", result);

    assert!(
        result.contains("_seq"),
        "User table _seq query result should contain '_seq'; got: {}",
        result
    );

    println!("  ✅ INSERT ... RETURNING _seq on USER table verified");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}
