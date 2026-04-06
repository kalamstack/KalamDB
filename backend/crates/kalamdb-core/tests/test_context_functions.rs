//! Integration tests for SQL context functions: KDB_CURRENT_USER(), KDB_CURRENT_USER_ID(), KDB_CURRENT_ROLE()
//!
//! These tests verify that the context functions work end-to-end with ExecutionContext.

use datafusion::prelude::SessionContext;
use kalamdb_commons::{NodeId, Role, UserId, UserName};
use kalamdb_configs::ServerConfig;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::context::ExecutionResult;
use kalamdb_core::sql::datafusion_session::DataFusionSessionFactory;
use kalamdb_core::sql::executor::handler_registry::HandlerRegistry;
use kalamdb_core::sql::executor::SqlExecutor;
use kalamdb_session::AuthSession;
use kalamdb_store::test_utils::TestDb;
use std::sync::Arc;

fn create_executor(app_context: Arc<AppContext>) -> SqlExecutor {
    let registry = Arc::new(HandlerRegistry::new());
    kalamdb_handlers::register_all_handlers(&registry, app_context.clone(), false);
    SqlExecutor::new(app_context, registry)
}

/// Helper to create a simple test session with custom functions registered
fn create_test_session() -> Arc<SessionContext> {
    // Use DataFusionSessionFactory to get a session with all custom functions registered
    let factory =
        DataFusionSessionFactory::new().expect("Failed to create DataFusionSessionFactory");
    Arc::new(factory.create_session())
}

fn create_test_app_context() -> Arc<AppContext> {
    let test_db = TestDb::with_system_tables().expect("Failed to create test database");
    let storage_base_path = test_db.storage_dir().expect("Failed to create storage directory");
    let app_context = AppContext::create_isolated(
        test_db.backend(),
        NodeId::new(1),
        storage_base_path.to_string_lossy().into_owned(),
        ServerConfig::default(),
    );

    std::mem::forget(test_db);
    app_context
}

#[tokio::test]
async fn test_current_user_with_username() {
    let username = UserName::new("alice");
    let user_id = UserId::new("u_alice");
    let role = Role::User;

    // Create AuthSession with username
    let auth_session = AuthSession::with_username_and_auth_details(
        user_id.clone(),
        username.clone(),
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    // Create ExecutionContext
    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());

    // Create session with user
    let session = exec_ctx.create_session_with_user();

    // Execute KDB_CURRENT_USER() - should return username
    let result = session.sql("SELECT KDB_CURRENT_USER() AS username").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);

    // Verify the returned value is the username
    let column = batch.column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "alice");
}

#[tokio::test]
async fn test_current_user_without_username_fails() {
    let user_id = UserId::new("u_bob");
    let role = Role::User;

    // Create ExecutionContext without username
    let exec_ctx = ExecutionContext::new(user_id, role, create_test_session());

    // Create session with user
    let session = exec_ctx.create_session_with_user();

    // Execute KDB_CURRENT_USER() - should fail because username is not set
    let result = session.sql("SELECT KDB_CURRENT_USER() AS username").await;
    assert!(result.is_ok(), "Query parsing failed");

    let df = result.unwrap();
    let batches_result = df.collect().await;
    assert!(batches_result.is_err(), "Expected error when username is not set");
}

#[tokio::test]
async fn test_current_user_id_with_dba_role() {
    let username = UserName::new("admin");
    let user_id = UserId::new("u_admin");
    let role = Role::Dba;

    // Create AuthSession with username
    let auth_session = AuthSession::with_username_and_auth_details(
        user_id.clone(),
        username.clone(),
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    // Create ExecutionContext
    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());

    // Create session with user
    let session = exec_ctx.create_session_with_user();

    // Execute KDB_CURRENT_USER_ID() - should return user_id (DBA role is authorized)
    let result = session.sql("SELECT KDB_CURRENT_USER_ID() AS user_id").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify the returned value is the user_id
    let column = batch.column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "u_admin");
}

#[tokio::test]
async fn test_current_user_id_with_system_role() {
    let user_id = UserId::system();
    let role = Role::System;

    // Create ExecutionContext
    let exec_ctx = ExecutionContext::new(user_id.clone(), role, create_test_session());

    // Create session with user
    let session = exec_ctx.create_session_with_user();

    // Execute KDB_CURRENT_USER_ID() - should work (System role is authorized)
    let result = session.sql("SELECT KDB_CURRENT_USER_ID() AS user_id").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify the returned value is the user_id
    let column = batch.column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "system");
}

#[tokio::test]
async fn test_current_user_id_with_service_role() {
    let user_id = UserId::new("u_service");
    let role = Role::Service;

    // Create ExecutionContext
    let exec_ctx = ExecutionContext::new(user_id.clone(), role, create_test_session());

    // Create session with user
    let session = exec_ctx.create_session_with_user();

    // Execute KDB_CURRENT_USER_ID() - should work (Service role is authorized)
    let result = session.sql("SELECT KDB_CURRENT_USER_ID() AS user_id").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let column = batches[0].column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "u_service");
}

#[tokio::test]
async fn test_current_user_id_with_user_role_fails() {
    let user_id = UserId::new("u_regular");
    let role = Role::User;

    // Create ExecutionContext
    let exec_ctx = ExecutionContext::new(user_id, role, create_test_session());

    // Create session with user
    let session = exec_ctx.create_session_with_user();

    // Execute KDB_CURRENT_USER_ID() - should fail (User role not authorized)
    let result = session.sql("SELECT KDB_CURRENT_USER_ID() AS user_id").await;
    assert!(result.is_ok(), "Query parsing failed");

    let df = result.unwrap();
    let batches_result = df.collect().await;
    assert!(batches_result.is_err(), "Expected error for unauthorized User role");

    let err = batches_result.unwrap_err();
    assert!(
        err.to_string().contains("system, dba, or service"),
        "Error message should mention required roles: {}",
        err
    );
}

#[tokio::test]
async fn test_current_role_user() {
    let username = UserName::new("regular");
    let user_id = UserId::new("u_regular");
    let role = Role::User;

    // Create AuthSession
    let auth_session = AuthSession::with_username_and_auth_details(
        user_id,
        username,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    // Create ExecutionContext
    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());

    // Create session
    let session = exec_ctx.create_session_with_user();

    // Execute KDB_CURRENT_ROLE()
    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify the returned value is "user"
    let column = batch.column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "user");
}

#[tokio::test]
async fn test_current_role_dba() {
    let user_id = UserId::new("u_admin");
    let role = Role::Dba;

    let exec_ctx = ExecutionContext::new(user_id, role, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await.unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let column = batches[0].column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "dba");
}

#[tokio::test]
async fn test_current_role_system() {
    let user_id = UserId::system();
    let role = Role::System;

    let exec_ctx = ExecutionContext::new(user_id, role, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await.unwrap();
    let batches = result.collect().await.unwrap();

    let column = batches[0].column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "system");
}

#[tokio::test]
async fn test_current_role_service() {
    let user_id = UserId::new("u_service");
    let role = Role::Service;

    let exec_ctx = ExecutionContext::new(user_id, role, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await.unwrap();
    let batches = result.collect().await.unwrap();

    let column = batches[0].column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "service");
}

#[tokio::test]
async fn test_all_three_functions_together() {
    let username = UserName::new("testuser");
    let user_id = UserId::new("u_testuser");
    let role = Role::Dba;

    // Create AuthSession
    let auth_session = AuthSession::with_username_and_auth_details(
        user_id,
        username,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    // Query all three functions at once
    let result = session
        .sql("SELECT KDB_CURRENT_USER() AS username, KDB_CURRENT_USER_ID() AS user_id, KDB_CURRENT_ROLE() AS role")
        .await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);

    // Verify username
    let username_col = batch.column(0);
    let username_array = username_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(username_array.value(0), "testuser");

    // Verify user_id
    let user_id_col = batch.column(1);
    let user_id_array = user_id_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(user_id_array.value(0), "u_testuser");

    // Verify role
    let role_col = batch.column(2);
    let role_array = role_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(role_array.value(0), "dba");
}

#[tokio::test]
async fn test_sql_standard_context_function_aliases() {
    let username = UserName::new("admin");
    let user_id = UserId::new("u_admin");
    let role = Role::Dba;
    let app_context = create_test_app_context();

    let auth_session = AuthSession::with_username_and_auth_details(
        user_id,
        username,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, app_context.base_session_context());
    let executor = create_executor(app_context);

    let result = executor
        .execute(
            "SELECT CURRENT_USER() AS username, CURRENT_USER_ID() AS user_id, CURRENT_ROLE() AS role",
            &exec_ctx,
            vec![],
        )
        .await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let batches = match result.unwrap() {
        ExecutionResult::Rows { batches, .. } => batches,
        other => panic!("Expected row result, got {:?}", other),
    };
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let username_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let user_id_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    let role_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();

    assert_eq!(username_col.value(0), "admin");
    assert_eq!(user_id_col.value(0), "u_admin");
    assert_eq!(role_col.value(0), "dba");
}

#[tokio::test]
async fn test_functions_in_where_clause() {
    let username = UserName::new("admin");
    let user_id = UserId::new("u_admin");
    let role = Role::Dba;

    let auth_session = AuthSession::with_username_and_auth_details(
        user_id,
        username,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    // Test functions in WHERE clause
    let result = session
        .sql("SELECT 1 WHERE KDB_CURRENT_USER() = 'admin' AND KDB_CURRENT_ROLE() = 'dba'")
        .await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    // Should return one row (conditions are true)
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_functions_with_no_arguments() {
    let username = UserName::new("test");
    let user_id = UserId::new("u_test");
    let role = Role::User;

    let auth_session = AuthSession::with_username_and_auth_details(
        user_id,
        username,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    // All three functions should work without arguments
    let result = session.sql("SELECT KDB_CURRENT_USER(), KDB_CURRENT_ROLE()").await;
    assert!(result.is_ok(), "Query should succeed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}
