//! Integration tests for SQL context functions: KDB_CURRENT_USER(), KDB_CURRENT_USER_ID(), KDB_CURRENT_ROLE()

use datafusion::prelude::SessionContext;
use kalamdb_commons::{Role, UserId};
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_core::sql::datafusion_session::DataFusionSessionFactory;
use kalamdb_session::AuthSession;
use std::sync::Arc;

fn create_test_session() -> Arc<SessionContext> {
    let factory =
        DataFusionSessionFactory::new().expect("Failed to create DataFusionSessionFactory");
    Arc::new(factory.create_session())
}

#[tokio::test]
async fn test_current_user_returns_user_id() {
    let user_id = UserId::new("u_alice");
    let role = Role::User;

    let auth_session = AuthSession::with_auth_details(
        user_id.clone(),
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    // KDB_CURRENT_USER() now returns user_id
    let result = session.sql("SELECT KDB_CURRENT_USER() AS current_user").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);

    let column = batch.column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "u_alice");
}

#[tokio::test]
async fn test_current_user_id_with_dba_role() {
    let user_id = UserId::new("u_admin");
    let role = Role::Dba;

    let auth_session = AuthSession::with_auth_details(
        user_id.clone(),
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER_ID() AS user_id").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let column = batch.column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "u_admin");
}

#[tokio::test]
async fn test_current_user_id_with_system_role() {
    let user_id = UserId::system();
    let role = Role::System;

    let exec_ctx = ExecutionContext::new(user_id.clone(), role, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER_ID() AS user_id").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let column = batches[0].column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "system");
}

#[tokio::test]
async fn test_current_user_id_with_service_role() {
    let user_id = UserId::new("u_service");
    let role = Role::Service;

    let exec_ctx = ExecutionContext::new(user_id.clone(), role, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER_ID() AS user_id").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let column = batches[0].column(0);
    let string_array =
        column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(string_array.value(0), "u_service");
}

#[tokio::test]
async fn test_current_user_id_with_user_role_fails() {
    let user_id = UserId::new("u_regular");
    let role = Role::User;

    let exec_ctx = ExecutionContext::new(user_id, role, create_test_session());
    let session = exec_ctx.create_session_with_user();

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
    let user_id = UserId::new("u_regular");
    let role = Role::User;

    let auth_session = AuthSession::with_auth_details(
        user_id,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let column = batches[0].column(0);
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
    let user_id = UserId::new("u_testuser");
    let role = Role::Dba;

    let auth_session = AuthSession::with_auth_details(
        user_id,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT KDB_CURRENT_USER() AS current_user, KDB_CURRENT_USER_ID() AS user_id, KDB_CURRENT_ROLE() AS role")
        .await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);

    // current_user now returns user_id
    let current_user_col = batch.column(0);
    let current_user_array = current_user_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(current_user_array.value(0), "u_testuser");

    let user_id_col = batch.column(1);
    let user_id_array = user_id_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(user_id_array.value(0), "u_testuser");

    let role_col = batch.column(2);
    let role_array = role_col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(role_array.value(0), "dba");
}

#[tokio::test]
async fn test_context_function_execution_uses_rewritten_aliases() {
    let user_id = UserId::new("u_admin");
    let role = Role::Dba;
    let session_context = create_test_session();

    let auth_session = AuthSession::with_auth_details(
        user_id,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, session_context);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql(
            "SELECT KDB_CURRENT_USER() AS current_user, KDB_CURRENT_USER_ID() AS user_id, KDB_CURRENT_ROLE() AS role",
        )
        .await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let batches = result.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    let current_user_col = batches[0]
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

    assert_eq!(current_user_col.value(0), "u_admin");
    assert_eq!(user_id_col.value(0), "u_admin");
    assert_eq!(role_col.value(0), "dba");
}

#[tokio::test]
async fn test_functions_in_where_clause() {
    let user_id = UserId::new("u_admin");
    let role = Role::Dba;

    let auth_session = AuthSession::with_auth_details(
        user_id,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT 1 WHERE KDB_CURRENT_USER() = 'u_admin' AND KDB_CURRENT_ROLE() = 'dba'")
        .await;
    assert!(result.is_ok(), "Query failed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_functions_with_no_arguments() {
    let user_id = UserId::new("u_test");
    let role = Role::User;

    let auth_session = AuthSession::with_auth_details(
        user_id,
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );

    let exec_ctx = ExecutionContext::from_session(auth_session, create_test_session());
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER(), KDB_CURRENT_ROLE()").await;
    assert!(result.is_ok(), "Query should succeed: {:?}", result.err());

    let df = result.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}
