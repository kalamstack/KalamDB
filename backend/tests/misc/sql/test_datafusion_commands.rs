//! DataFusion meta commands integration tests
//!
//! Verifies that DataFusion built-in commands (EXPLAIN, SET, SHOW, DESCRIBE)
//! work correctly for admin users and are blocked for non-admin users.

use kalam_client::models::ResponseStatus;
use kalamdb_commons::models::{Role, UserId};

use super::test_support::TestServer;

async fn insert_user(server: &TestServer, username: &str, role: Role) -> UserId {
    server.create_user(username, "TestPass123!", role).await
}

#[actix_web::test]
async fn test_explain_command_admin_allowed() {
    let server = TestServer::new_shared().await;
    let dba = insert_user(&server, "admin_user", Role::Dba).await;

    // Create namespace and table for testing
    server.execute_sql("CREATE NAMESPACE df_test").await;
    let create_sql =
        "CREATE TABLE df_test.test_table (id INT PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // EXPLAIN should work for admin
    let sql = "EXPLAIN SELECT * FROM df_test.test_table";
    let resp = server.execute_sql_as_user(sql, dba.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "EXPLAIN should work for DBA role: {:?}",
        resp.error
    );

    // Should return query plan
    assert!(!resp.results.is_empty(), "EXPLAIN should return query plan");
}

#[actix_web::test]
async fn test_explain_command_user_denied() {
    let server = TestServer::new_shared().await;
    let user = insert_user(&server, "regular_user", Role::User).await;

    // Create namespace and table for testing
    server.execute_sql("CREATE NAMESPACE df_test2").await;
    let create_sql =
        "CREATE TABLE df_test2.test_table (id INT PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // EXPLAIN should be denied for regular user
    let sql = "EXPLAIN SELECT * FROM df_test2.test_table";
    let resp = server.execute_sql_as_user(sql, user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Error, "EXPLAIN should be denied for User role");

    assert!(
        resp.error.as_ref().unwrap().message.contains("Admin privileges"),
        "Error message should mention admin privileges: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_set_command_admin_allowed() {
    let server = TestServer::new_shared().await;
    let dba = insert_user(&server, "admin_set", Role::Dba).await;

    // SET should work for admin
    let sql = "SET datafusion.execution.batch_size TO 8192";
    let resp = server.execute_sql_as_user(sql, dba.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "SET should work for DBA role: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_set_command_user_denied() {
    let server = TestServer::new_shared().await;
    let user = insert_user(&server, "regular_set_user", Role::User).await;

    // SET should be denied for regular user
    let sql = "SET datafusion.execution.batch_size TO 8192";
    let resp = server.execute_sql_as_user(sql, user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Error, "SET should be denied for User role");

    assert!(
        resp.error.as_ref().unwrap().message.contains("Admin privileges"),
        "Error message should mention admin privileges: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_show_all_admin_allowed() {
    let server = TestServer::new_shared().await;
    let dba = insert_user(&server, "admin_show", Role::Dba).await;

    // SHOW ALL should work for admin
    let sql = "SHOW ALL";
    let resp = server.execute_sql_as_user(sql, dba.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "SHOW ALL should work for DBA role: {:?}",
        resp.error
    );

    // Should return configuration options
    assert!(!resp.results.is_empty(), "SHOW ALL should return configuration");
}

#[actix_web::test]
async fn test_show_all_user_denied() {
    let server = TestServer::new_shared().await;
    let user = insert_user(&server, "regular_show_user", Role::User).await;

    // SHOW ALL should be denied for regular user
    let sql = "SHOW ALL";
    let resp = server.execute_sql_as_user(sql, user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Error, "SHOW ALL should be denied for User role");

    assert!(
        resp.error.as_ref().unwrap().message.contains("Admin privileges"),
        "Error message should mention admin privileges: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_show_columns_admin_allowed() {
    let server = TestServer::new_shared().await;
    let dba = insert_user(&server, "admin_cols", Role::Dba).await;

    // Create namespace and table
    server.execute_sql("CREATE NAMESPACE df_cols_test").await;
    let create_sql = "CREATE TABLE df_cols_test.test_table (id INT PRIMARY KEY, name TEXT, age \
                      INT) WITH (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // SHOW COLUMNS should work for admin
    let sql = "SHOW COLUMNS FROM df_cols_test.test_table";
    let resp = server.execute_sql_as_user(sql, dba.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "SHOW COLUMNS should work for DBA role: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_show_columns_user_denied() {
    let server = TestServer::new_shared().await;
    let user = insert_user(&server, "regular_cols_user", Role::User).await;

    // Create namespace and table
    server.execute_sql("CREATE NAMESPACE df_cols_test2").await;
    let create_sql = "CREATE TABLE df_cols_test2.test_table (id INT PRIMARY KEY, name TEXT) WITH \
                      (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // SHOW COLUMNS should be denied for regular user
    let sql = "SHOW COLUMNS FROM df_cols_test2.test_table";
    let resp = server.execute_sql_as_user(sql, user.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "SHOW COLUMNS should be denied for User role"
    );

    assert!(
        resp.error.as_ref().unwrap().message.contains("Admin privileges"),
        "Error message should mention admin privileges: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_describe_datafusion_style_admin_allowed() {
    let server = TestServer::new_shared().await;
    let dba = insert_user(&server, "admin_desc", Role::Dba).await;

    // Create namespace and table
    server.execute_sql("CREATE NAMESPACE df_desc_test").await;
    let create_sql = "CREATE TABLE df_desc_test.test_table (id INT PRIMARY KEY, name TEXT) WITH \
                      (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // DESCRIBE (DataFusion style without TABLE keyword) should work for admin
    let sql = "DESCRIBE df_desc_test.test_table";
    let resp = server.execute_sql_as_user(sql, dba.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "DESCRIBE should work for DBA role: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_describe_datafusion_style_user_denied() {
    let server = TestServer::new_shared().await;
    let user = insert_user(&server, "regular_desc_user", Role::User).await;

    // Create namespace and table
    server.execute_sql("CREATE NAMESPACE df_desc_test2").await;
    let create_sql = "CREATE TABLE df_desc_test2.test_table (id INT PRIMARY KEY, name TEXT) WITH \
                      (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // DESCRIBE should be denied for regular user
    let sql = "DESCRIBE df_desc_test2.test_table";
    let resp = server.execute_sql_as_user(sql, user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Error, "DESCRIBE should be denied for User role");

    assert!(
        resp.error.as_ref().unwrap().message.contains("Admin privileges"),
        "Error message should mention admin privileges: {:?}",
        resp.error
    );
}

#[actix_web::test]
async fn test_system_role_datafusion_commands() {
    let server = TestServer::new_shared().await;
    let system = insert_user(&server, "system_user", Role::System).await;

    // Create namespace and table
    server.execute_sql("CREATE NAMESPACE df_sys_test").await;
    let create_sql = "CREATE TABLE df_sys_test.test_table (id INT PRIMARY KEY, name TEXT) WITH \
                      (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // System role should also be able to use DataFusion commands
    let explain = "EXPLAIN SELECT * FROM df_sys_test.test_table";
    let resp = server.execute_sql_as_user(explain, system.as_str()).await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "System role should be allowed to use EXPLAIN"
    );

    let set = "SET datafusion.execution.batch_size TO 4096";
    let resp = server.execute_sql_as_user(set, system.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Success, "System role should be allowed to use SET");

    let show = "SHOW ALL";
    let resp = server.execute_sql_as_user(show, system.as_str()).await;
    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "System role should be allowed to use SHOW ALL"
    );
}

#[actix_web::test]
async fn test_service_role_datafusion_commands_denied() {
    let server = TestServer::new_shared().await;
    let service = insert_user(&server, "service_user", Role::Service).await;

    // Create namespace and table
    server.execute_sql("CREATE NAMESPACE df_svc_test").await;
    let create_sql = "CREATE TABLE df_svc_test.test_table (id INT PRIMARY KEY, name TEXT) WITH \
                      (TYPE = 'SHARED')";
    server.execute_sql(create_sql).await;

    // Service role should NOT be able to use DataFusion commands (not admin)
    let explain = "EXPLAIN SELECT * FROM df_svc_test.test_table";
    let resp = server.execute_sql_as_user(explain, service.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Error, "Service role should be denied EXPLAIN");

    let set = "SET datafusion.execution.batch_size TO 4096";
    let resp = server.execute_sql_as_user(set, service.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Error, "Service role should be denied SET");

    let show = "SHOW ALL";
    let resp = server.execute_sql_as_user(show, service.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Error, "Service role should be denied SHOW ALL");
}
