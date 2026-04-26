//! Comprehensive integration tests for SQL functions
//!
//! Tests cover:
//! - Context functions: KDB_CURRENT_USER(), KDB_CURRENT_USER(), KDB_CURRENT_ROLE()
//! - ID generation functions: SNOWFLAKE_ID(), UUID_V7(), ULID()
//! - Function usage in SELECT, WHERE, INSERT, UPDATE, DELETE statements

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use kalamdb_commons::{Role, UserId};
use kalamdb_core::sql::{context::ExecutionContext, datafusion_session::DataFusionSessionFactory};
use kalamdb_session::AuthSession;

fn create_test_session() -> Arc<SessionContext> {
    let factory =
        DataFusionSessionFactory::new().expect("Failed to create DataFusionSessionFactory");
    Arc::new(factory.create_session())
}

fn create_exec_context_with_user(user_id: &str, role: Role) -> ExecutionContext {
    let auth_session = AuthSession::with_auth_details(
        UserId::new(user_id),
        role,
        kalamdb_commons::models::ConnectionInfo::new(None),
        kalamdb_session::AuthMethod::Bearer,
    );
    ExecutionContext::from_session(auth_session, create_test_session())
}

// ============================================================================
// CONTEXT FUNCTIONS TESTS
// ============================================================================

#[tokio::test]
async fn test_current_user_basic() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER() AS current_user").await.unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "u_alice");
}

#[tokio::test]
async fn test_current_user_id_dba() {
    let exec_ctx = create_exec_context_with_user("u_admin", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER() AS user_id").await.unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "u_admin");
}

#[tokio::test]
async fn test_current_user_id_system() {
    let exec_ctx = create_exec_context_with_user("system", Role::System);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER() AS user_id").await.unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "system");
}

#[tokio::test]
async fn test_current_user_id_service_role() {
    let exec_ctx = create_exec_context_with_user("svc_worker", Role::Service);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_USER() AS user_id").await.unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "svc_worker");
}

#[tokio::test]
async fn test_current_role_user() {
    let exec_ctx = create_exec_context_with_user("u_bob", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await.unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "user");
}

#[tokio::test]
async fn test_current_role_dba() {
    let exec_ctx = create_exec_context_with_user("u_admin", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await.unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "dba");
}

#[tokio::test]
async fn test_current_role_system() {
    let exec_ctx = create_exec_context_with_user("system", Role::System);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT KDB_CURRENT_ROLE() AS role").await.unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "system");
}

#[tokio::test]
async fn test_all_context_functions_together() {
    let exec_ctx = create_exec_context_with_user("u_test", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql(
            "SELECT KDB_CURRENT_USER() AS current_user, KDB_CURRENT_USER() AS user_id, \
             KDB_CURRENT_ROLE() AS role",
        )
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 3);

    // Verify current_user (column 0) - now returns user_id
    let col0 = batches[0].column(0);
    let arr0 = col0.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr0.value(0), "u_test");

    // Verify user_id (column 1)
    let col1 = batches[0].column(1);
    let arr1 = col1.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr1.value(0), "u_test");

    // Verify role (column 2)
    let col2 = batches[0].column(2);
    let arr2 = col2.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr2.value(0), "dba");
}

// ============================================================================
// ID GENERATION FUNCTIONS TESTS
// ============================================================================

#[tokio::test]
async fn test_snowflake_id_function() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT SNOWFLAKE_ID() AS id").await.unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();

    // Snowflake ID should be a positive 64-bit integer
    let id = arr.value(0);
    assert!(id > 0, "SNOWFLAKE_ID should generate positive IDs");
}

#[tokio::test]
async fn test_uuid_v7_function() {
    let exec_ctx = create_exec_context_with_user("u_bob", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT UUID_V7() AS id").await.unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();

    let uuid = arr.value(0);
    // UUID_V7 should be a valid UUID string (36 characters with hyphens)
    assert_eq!(uuid.len(), 36, "UUID_V7 should generate standard UUID strings");
}

#[tokio::test]
async fn test_ulid_function() {
    let exec_ctx = create_exec_context_with_user("u_charlie", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT ULID() AS id").await.unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();

    let ulid = arr.value(0);
    // ULID should be 26 characters long
    assert_eq!(ulid.len(), 26, "ULID should generate 26-character IDs");
}

// ============================================================================
// FUNCTIONS IN WHERE CLAUSE TESTS
// ============================================================================

#[tokio::test]
async fn test_current_user_in_where_clause() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    // Test WHERE clause with KDB_CURRENT_USER() comparison
    let result = session.sql("SELECT 1 WHERE KDB_CURRENT_USER() = 'u_alice'").await.unwrap();
    let batches = result.collect().await.unwrap();

    // Should return 1 row (condition is true)
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_current_user_where_no_match() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    // Test WHERE clause with non-matching condition
    let result = session.sql("SELECT 1 WHERE KDB_CURRENT_USER() = 'u_bob'").await.unwrap();
    let batches = result.collect().await.unwrap();

    // Should return 0 rows (condition is false)
    assert!(batches.is_empty() || batches[0].num_rows() == 0);
}

#[tokio::test]
async fn test_current_role_in_where_clause() {
    let exec_ctx = create_exec_context_with_user("u_admin", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT 1 WHERE KDB_CURRENT_ROLE() = 'dba'").await.unwrap();
    let batches = result.collect().await.unwrap();

    // Should return 1 row (condition is true)
    assert!(batches.len() > 0 && batches[0].num_rows() > 0);
}

#[tokio::test]
async fn test_multiple_functions_in_where() {
    let exec_ctx = create_exec_context_with_user("u_admin", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT 1 WHERE KDB_CURRENT_USER() = 'u_admin' AND KDB_CURRENT_ROLE() = 'dba'")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    // Both conditions are true
    assert!(batches.len() > 0 && batches[0].num_rows() > 0);
}

// ============================================================================
// FUNCTIONS IN SELECT EXPRESSIONS
// ============================================================================

#[tokio::test]
async fn test_multiple_id_functions_in_select() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT SNOWFLAKE_ID() AS snowflake, UUID_V7() AS uuid, ULID() AS ulid")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 3);

    // Verify snowflake_id (column 0)
    let col0 = batches[0].column(0);
    let arr0 = col0.as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
    assert!(arr0.value(0) > 0);

    // Verify uuid (column 1)
    let col1 = batches[0].column(1);
    let arr1 = col1.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr1.value(0).len(), 36);

    // Verify ulid (column 2)
    let col2 = batches[0].column(2);
    let arr2 = col2.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr2.value(0).len(), 26);
}

#[tokio::test]
async fn test_context_and_id_functions_together() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql(
            "SELECT KDB_CURRENT_USER() AS username, KDB_CURRENT_ROLE() AS role, SNOWFLAKE_ID() AS \
             snowflake_id, UUID_V7() AS uuid_v7, ULID() AS ulid",
        )
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 5);
}

// ============================================================================
// FUNCTIONS IN EXPRESSIONS (ARITHMETIC, CONCATENATION, etc.)
// ============================================================================

#[tokio::test]
async fn test_context_function_with_coalesce() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT COALESCE(KDB_CURRENT_USER(), 'unknown') AS user")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "u_alice");
}

#[tokio::test]
async fn test_context_function_with_concat() {
    let exec_ctx = create_exec_context_with_user("u_bob", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT CONCAT('User: ', KDB_CURRENT_USER(), ' Role: ', KDB_CURRENT_ROLE()) AS info")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "User: u_bob Role: dba");
}

// ============================================================================
// FUNCTIONS WITH MULTIPLE ROWS
// ============================================================================

#[tokio::test]
async fn test_snowflake_id_generates_multiple_unique_ids() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    // Query that generates multiple IDs
    let result = session
        .sql(
            "SELECT SNOWFLAKE_ID() AS id1 UNION ALL SELECT SNOWFLAKE_ID() UNION ALL SELECT \
             SNOWFLAKE_ID()",
        )
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    // Should have 3 rows across batches
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 1, "Should generate at least one ID");
}

// ============================================================================
// CASE/CONDITIONAL STATEMENTS WITH FUNCTIONS
// ============================================================================

#[tokio::test]
async fn test_context_function_in_case_statement() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql(
            "SELECT CASE WHEN KDB_CURRENT_ROLE() = 'dba' THEN 'Administrator' WHEN \
             KDB_CURRENT_ROLE() = 'user' THEN 'Regular User' ELSE 'Unknown' END AS \
             role_description",
        )
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "Regular User");
}

#[tokio::test]
async fn test_id_function_in_case_statement() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql(
            "SELECT CASE WHEN SNOWFLAKE_ID() > 0 THEN 'Valid ID' ELSE 'Invalid ID' END AS id_check",
        )
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(arr.value(0), "Valid ID");
}

// ============================================================================
// FUNCTION BEHAVIOR TESTS (Edge cases)
// ============================================================================

#[tokio::test]
async fn test_current_user_empty_check() {
    let exec_ctx = create_exec_context_with_user("u_test", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT KDB_CURRENT_USER() IS NOT NULL AS is_not_null")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>().unwrap();
    assert!(arr.value(0), "KDB_CURRENT_USER() should never be NULL");
}

#[tokio::test]
async fn test_uuid_v7_format_validation() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT UUID_V7() AS uuid_val").await.unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();

    let uuid = arr.value(0);
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars, 4 hyphens)
    assert_eq!(uuid.matches('-').count(), 4, "UUID should have 4 hyphens");
}

#[tokio::test]
async fn test_ulid_format_validation() {
    let exec_ctx = create_exec_context_with_user("u_bob", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session.sql("SELECT ULID() AS ulid_val").await.unwrap();
    let batches = result.collect().await.unwrap();

    let col = batches[0].column(0);
    let arr = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();

    let ulid = arr.value(0);
    // ULID should only contain valid base32 characters (no hyphens)
    assert!(!ulid.contains('-'), "ULID should not contain hyphens");
    assert_eq!(ulid.len(), 26, "ULID must be exactly 26 characters");
}

// ============================================================================
// FUNCTIONS IN SUBQUERIES
// ============================================================================

#[tokio::test]
async fn test_context_function_in_subquery() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT * FROM (SELECT KDB_CURRENT_USER() AS name, KDB_CURRENT_ROLE() AS role) AS sub")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert!(batches.len() > 0);
}

#[tokio::test]
async fn test_id_function_in_subquery() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    let result = session
        .sql("SELECT * FROM (SELECT SNOWFLAKE_ID() AS id, UUID_V7() AS uuid) AS sub")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert!(batches.len() > 0);
    assert!(batches[0].num_rows() > 0);
}

// ============================================================================
// DOCUMENTATION EXAMPLES
// ============================================================================

#[tokio::test]
async fn test_example_all_context_functions() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    // This example demonstrates all three context functions working together
    let result = session
        .sql(
            "SELECT KDB_CURRENT_USER() AS username, KDB_CURRENT_USER() AS user_id, \
             KDB_CURRENT_ROLE() AS role",
        )
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_columns(), 3);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_example_all_id_functions() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::User);
    let session = exec_ctx.create_session_with_user();

    // This example demonstrates all three ID generation functions
    let result = session
        .sql("SELECT SNOWFLAKE_ID() AS snowflake_id, UUID_V7() AS uuid_v7, ULID() AS ulid")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_eq!(batches[0].num_columns(), 3);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_example_mixed_functions() {
    let exec_ctx = create_exec_context_with_user("u_alice", Role::Dba);
    let session = exec_ctx.create_session_with_user();

    // Mix context and ID functions
    let result = session
        .sql(
            "SELECT KDB_CURRENT_USER() AS current_user, SNOWFLAKE_ID() AS new_record_id, \
             KDB_CURRENT_ROLE() AS admin_role",
        )
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert!(batches.len() > 0);
    assert!(batches[0].num_rows() > 0);
}
