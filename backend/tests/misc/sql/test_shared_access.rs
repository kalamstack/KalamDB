//! Integration tests for User Story 4: Shared Table Access Control
//!
//! Tests Phase 6 requirements:
//! - T085: Public shared table read-only access for all authenticated users
//! - T086: Private shared table access for service/dba/system only
//! - T087: Default access level validation (defaults to private)
//! - T088: Access level modification authorization (only service/dba/system can modify)
//! - T089: Read-only enforcement for regular users on public tables

use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;

use super::test_support::{consolidated_helpers, TestServer};

#[tokio::test]
async fn test_public_table_read_only_for_users() {
    let server = TestServer::new_shared().await;
    let admin_username = "system";
    let admin_password = "SystemPass123!";
    let admin_id = server.create_user(admin_username, admin_password, Role::System).await;
    let admin_id_str = admin_id.as_str();

    // Create test namespace
    let namespace = consolidated_helpers::unique_namespace("shared_test_public_ro");
    let res = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", namespace), admin_id_str)
        .await;
    assert_eq!(
        res.status,
        ResponseStatus::Success,
        "Failed to create namespace: {:?}",
        res.error
    );

    // Create a service user to set up the table
    let service_username = "service_user";
    let service_password = "ServicePass123!";
    let service_id = server.create_user(service_username, service_password, Role::Service).await;
    let service_id_str = service_id.as_str();

    // Create a public shared table
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.messages (
            id BIGINT PRIMARY KEY,
            content TEXT NOT NULL
        ) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'public')
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Failed to create public table: {:?}",
        result.error
    );

    // Create a regular user
    let regular_username = "regular_user";
    let regular_password = "RegularPass123!";
    let regular_id = server.create_user(regular_username, regular_password, Role::User).await;
    let regular_id_str = regular_id.as_str();

    // Test 1: Regular user CAN read from public table
    let select_sql = format!("SELECT * FROM {}.messages", namespace);
    let result = server.execute_sql_as_user(&select_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Regular user should be able to read public table: {:?}",
        result.error
    );

    // Test 2: Regular user CANNOT insert into public table (read-only)
    let insert_sql = format!("INSERT INTO {}.messages (id, content) VALUES (1, 'test')", namespace);
    let result = server.execute_sql_as_user(&insert_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT be able to insert into public table"
    );

    // Test 3: Regular user CANNOT update public table
    let update_sql = format!("UPDATE {}.messages SET content = 'updated' WHERE id = 1", namespace);
    let result = server.execute_sql_as_user(&update_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT be able to update public table"
    );

    // Test 4: Regular user CANNOT delete from public table
    let delete_sql = format!("DELETE FROM {}.messages WHERE id = 1", namespace);
    let result = server.execute_sql_as_user(&delete_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT be able to delete from public table"
    );
}

#[tokio::test]
async fn test_private_table_service_dba_only() {
    let server = TestServer::new_shared().await;
    let admin_username = "system";
    let admin_password = "SystemPass123!";
    let admin_id = server.create_user(admin_username, admin_password, Role::System).await;
    let admin_id_str = admin_id.as_str();

    // Create test namespace
    let namespace = consolidated_helpers::unique_namespace("shared_test_private");
    let res = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", namespace), admin_id_str)
        .await;
    assert_eq!(res.status, ResponseStatus::Success);

    // Create users with different roles
    let service_username = "service_user";
    let service_password = "ServicePass123!";
    let service_id = server.create_user(service_username, service_password, Role::Service).await;
    let service_id_str = service_id.as_str();

    let dba_username = "dba_user";
    let dba_password = "DbaPass123!";
    let dba_id = server.create_user(dba_username, dba_password, Role::Dba).await;
    let dba_id_str = dba_id.as_str();

    let regular_username = "regular_user";
    let regular_password = "RegularPass123!";
    let regular_id = server.create_user(regular_username, regular_password, Role::User).await;
    let regular_id_str = regular_id.as_str();

    // Create a private shared table (as service user)
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.sensitive_data (
            id BIGINT PRIMARY KEY,
            secret TEXT NOT NULL
        ) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'private')
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Failed to create private table: {:?}",
        result.error
    );

    // Test 1: Service user CAN access private table
    let select_sql = format!("SELECT * FROM {}.sensitive_data", namespace);
    let result = server.execute_sql_as_user(&select_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Service user should access private table: {:?}",
        result.error
    );

    // Test 2: DBA user CAN access private table
    let result = server.execute_sql_as_user(&select_sql, dba_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "DBA user should access private table: {:?}",
        result.error
    );

    // Test 3: Regular user CANNOT access private table
    let result = server.execute_sql_as_user(&select_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT access private table"
    );

    // Test 4: Service user CAN modify private table
    let insert_sql = format!(
        "INSERT INTO {}.sensitive_data (id, secret) VALUES (1, 'confidential')",
        namespace
    );
    let result = server.execute_sql_as_user(&insert_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Service user should be able to modify private table: {:?}",
        result.error
    );
}

#[tokio::test]
async fn test_shared_table_defaults_to_private() {
    let server = TestServer::new_shared().await;
    let admin_username = "system";
    let admin_password = "SystemPass123!";
    let admin_id = server.create_user(admin_username, admin_password, Role::System).await;
    let admin_id_str = admin_id.as_str();

    // Create test namespace
    let namespace = consolidated_helpers::unique_namespace("shared_test_defaults");
    let res = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", namespace), admin_id_str)
        .await;
    assert_eq!(res.status, ResponseStatus::Success);

    // Create a service user
    let service_username = "service_user";
    let service_password = "ServicePass123!";
    let service_id = server.create_user(service_username, service_password, Role::Service).await;
    let service_id_str = service_id.as_str();

    // Create shared table WITHOUT specifying ACCESS LEVEL
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.default_access (
            id BIGINT PRIMARY KEY,
            data TEXT
        ) WITH (TYPE = 'SHARED')
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Failed to create table: {:?}",
        result.error
    );

    // Verify the table was created with default "private" access level
    // Query system.schemas to get the table metadata (requires admin privileges)
    let query_table_sql = format!(
        "SELECT access_level FROM system.schemas WHERE table_id = '{}:default_access'",
        namespace
    );
    let query_result = server.execute_sql_as_user(&query_table_sql, admin_id_str).await;

    assert_eq!(
        query_result.status,
        ResponseStatus::Success,
        "Failed to query system.schemas: {:?}",
        query_result.error
    );

    // Parse the result to check access_level
    assert!(!query_result.results.is_empty(), "Expected query results");
    let result = &query_result.results[0];

    let rows = result.rows_as_maps();
    assert!(!rows.is_empty(), "Table should exist in system.schemas");

    let row = &rows[0];
    let access_level = row
        .get("access_level")
        .and_then(|v| v.as_str())
        .expect("access_level should be present");

    assert_eq!(access_level, "private", "Default access level should be Private");

    // Create a regular user and verify they cannot access it
    let regular_username = "regular_user";
    let regular_password = "RegularPass123!";
    let regular_id = server.create_user(regular_username, regular_password, Role::User).await;
    let regular_id_str = regular_id.as_str();

    let select_sql = format!("SELECT * FROM {}.default_access", namespace);
    let result = server.execute_sql_as_user(&select_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT access table with default private access"
    );
}

#[tokio::test]
async fn test_change_access_level_requires_privileges() {
    let server = TestServer::new_shared().await;
    let admin_username = "system";
    let admin_password = "SystemPass123!";
    let admin_id = server.create_user(admin_username, admin_password, Role::System).await;
    let admin_id_str = admin_id.as_str();

    // Create test namespace
    let namespace = consolidated_helpers::unique_namespace("shared_test_alter");
    let res = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", namespace), admin_id_str)
        .await;
    assert_eq!(res.status, ResponseStatus::Success);

    // Create users with different roles
    let service_username = "service_user";
    let service_password = "ServicePass123!";
    let service_id = server.create_user(service_username, service_password, Role::Service).await;
    let service_id_str = service_id.as_str();

    let dba_username = "dba_user";
    let dba_password = "DbaPass123!";
    let dba_id = server.create_user(dba_username, dba_password, Role::Dba).await;
    let dba_id_str = dba_id.as_str();

    let regular_username = "regular_user";
    let regular_password = "RegularPass123!";
    let regular_id = server.create_user(regular_username, regular_password, Role::User).await;
    let regular_id_str = regular_id.as_str();

    // Create a private shared table
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.config (
            id BIGINT PRIMARY KEY,
            value TEXT
        ) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'private')
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Failed to create table: {:?}",
        result.error
    );

    // Test 1: Regular user CANNOT change access level
    let alter_sql = format!("ALTER TABLE {}.config SET ACCESS LEVEL public", namespace);
    let result = server.execute_sql_as_user(&alter_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT be able to change access level"
    );

    // Test 2: Service user CAN change access level
    let result = server.execute_sql_as_user(&alter_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Service user should be able to change access level: {:?}",
        result.error
    );

    // Test 3: DBA user CAN change access level back
    let alter_sql_private = format!("ALTER TABLE {}.config SET ACCESS LEVEL private", namespace);
    let result = server.execute_sql_as_user(&alter_sql_private, dba_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "DBA user should be able to change access level: {:?}",
        result.error
    );
}

#[tokio::test]
async fn test_user_cannot_modify_public_table() {
    let server = TestServer::new_shared().await;
    let admin_username = "system";
    let admin_password = "SystemPass123!";
    let admin_id = server.create_user(admin_username, admin_password, Role::System).await;
    let admin_id_str = admin_id.as_str();

    // Create test namespace
    let namespace = consolidated_helpers::unique_namespace("shared_test_modify");
    let res = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", namespace), admin_id_str)
        .await;
    assert_eq!(res.status, ResponseStatus::Success);

    // Create service user and public table
    let service_username = "service_user";
    let service_password = "ServicePass123!";
    let service_id = server.create_user(service_username, service_password, Role::Service).await;
    let service_id_str = service_id.as_str();

    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.announcements (
            id BIGINT PRIMARY KEY,
            message TEXT NOT NULL
        ) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'public')
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Failed to create table: {:?}",
        result.error
    );

    // Service user adds some data
    let insert_sql =
        format!("INSERT INTO {}.announcements (id, message) VALUES (1, 'Welcome')", namespace);
    let result = server.execute_sql_as_user(&insert_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Service user should insert data: {:?}",
        result.error
    );

    // Create regular user
    let regular_username = "regular_user";
    let regular_password = "RegularPass123!";
    let regular_id = server.create_user(regular_username, regular_password, Role::User).await;
    let regular_id_str = regular_id.as_str();

    // Test 1: Regular user CAN read
    let select_sql = format!("SELECT * FROM {}.announcements", namespace);
    let result = server.execute_sql_as_user(&select_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Regular user should be able to SELECT from public table: {:?}",
        result.error
    );

    // Test 2: Regular user CANNOT insert
    let insert_sql =
        format!("INSERT INTO {}.announcements (id, message) VALUES (2, 'Hacked')", namespace);
    let result = server.execute_sql_as_user(&insert_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT be able to INSERT into public table"
    );

    // Test 3: Regular user CANNOT update
    let update_sql =
        format!("UPDATE {}.announcements SET message = 'Modified' WHERE id = 1", namespace);
    let result = server.execute_sql_as_user(&update_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT be able to UPDATE public table"
    );

    // Test 4: Regular user CANNOT delete
    let delete_sql = format!("DELETE FROM {}.announcements WHERE id = 1", namespace);
    let result = server.execute_sql_as_user(&delete_sql, regular_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "Regular user should NOT be able to DELETE from public table"
    );

    // Test 5: Service user CAN still modify (verification)
    let update_sql = format!(
        "UPDATE {}.announcements SET message = 'Updated by service' WHERE id = 1",
        namespace
    );
    let result = server.execute_sql_as_user(&update_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Service user should be able to modify public table: {:?}",
        result.error
    );
}

#[tokio::test]
async fn test_access_level_only_on_shared_tables() {
    let server = TestServer::new_shared().await;
    let admin_username = "system";
    let admin_password = "SystemPass123!";
    let admin_id = server.create_user(admin_username, admin_password, Role::System).await;
    let admin_id_str = admin_id.as_str();

    // Create test namespace
    let namespace = consolidated_helpers::unique_namespace("shared_test_invalid");
    let res = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", namespace), admin_id_str)
        .await;
    assert_eq!(res.status, ResponseStatus::Success);

    let service_username = "service_user";
    let service_password = "ServicePass123!";
    let service_id = server.create_user(service_username, service_password, Role::Service).await;
    let service_id_str = service_id.as_str();

    // Test 1: USER table with ACCESS LEVEL should fail
    let create_user_table_sql = format!(
        r#"
        CREATE TABLE {}.my_data (
            id BIGINT PRIMARY KEY,
            data TEXT
        ) WITH (TYPE = 'USER', ACCESS_LEVEL = 'public')
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_user_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "ACCESS LEVEL should not be allowed on USER tables"
    );

    // Test 2: STREAM table with ACCESS LEVEL should fail
    let create_stream_table_sql = format!(
        r#"
        CREATE TABLE {}.events (
            id BIGINT PRIMARY KEY,
            event_type TEXT
        ) WITH (TYPE = 'STREAM', ACCESS_LEVEL = 'public', TTL_SECONDS = 3600)
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_stream_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "ACCESS LEVEL should not be allowed on STREAM tables"
    );
}

#[tokio::test]
async fn test_alter_access_level_only_on_shared_tables() {
    let server = TestServer::new_shared().await;
    let admin_username = "system";
    let admin_password = "SystemPass123!";
    let admin_id = server.create_user(admin_username, admin_password, Role::System).await;
    let admin_id_str = admin_id.as_str();

    // Create test namespace
    let namespace = consolidated_helpers::unique_namespace("shared_test_alter_invalid");
    let res = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", namespace), admin_id_str)
        .await;
    assert_eq!(res.status, ResponseStatus::Success);

    let service_username = "service_user";
    let service_password = "ServicePass123!";
    let service_id = server.create_user(service_username, service_password, Role::Service).await;
    let service_id_str = service_id.as_str();

    // Create a USER table
    let create_user_table_sql = format!(
        r#"
        CREATE TABLE {}.personal_notes (
            id BIGINT PRIMARY KEY,
            note TEXT
        ) WITH (TYPE = 'USER')
    "#,
        namespace
    );
    let result = server.execute_sql_as_user(&create_user_table_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Success,
        "Failed to create USER table: {:?}",
        result.error
    );

    // Try to set ACCESS LEVEL on USER table - should fail
    let alter_sql = format!("ALTER TABLE {}.personal_notes SET ACCESS LEVEL public", namespace);
    let result = server.execute_sql_as_user(&alter_sql, service_id_str).await;
    assert_eq!(
        result.status,
        ResponseStatus::Error,
        "ALTER TABLE SET ACCESS LEVEL should fail on USER tables"
    );
}
