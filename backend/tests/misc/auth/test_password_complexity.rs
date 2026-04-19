//! Tests for password complexity enforcement.

use super::test_support::TestServer;
use kalamdb_commons::{AuthType, Role, StorageId, UserId};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::executor::handler_registry::HandlerRegistry;
use kalamdb_core::sql::{ExecutionContext, ExecutionResult, SqlExecutor};
use kalamdb_system::providers::storages::models::StorageMode;
use std::sync::Arc;

async fn setup_executor(
    enforce_complexity: bool,
) -> (SqlExecutor, Arc<AppContext>, Arc<datafusion::prelude::SessionContext>) {
    // Use shared TestServer harness to initialize AppContext and session
    let server = TestServer::new_shared().await;
    let app_context = server.app_context.clone();
    let session_context = server.session_context.clone();

    // Create SqlExecutor with desired password complexity enforcement
    let registry = Arc::new(HandlerRegistry::new());
    kalamdb_handlers::register_all_handlers(&registry, app_context.clone(), enforce_complexity);
    let executor = SqlExecutor::new(app_context.clone(), registry);

    (executor, app_context, session_context)
}

async fn create_admin_user(app_context: &Arc<AppContext>) -> UserId {
    use kalamdb_system::User;
    let user_id = UserId::new("complexity_admin");
    let now = chrono::Utc::now().timestamp_millis();

    // If user already exists (singleton AppContext across tests), return existing id
    if let Ok(Some(existing)) = app_context.system_tables().users().get_user_by_id(&user_id) {
        return existing.user_id;
    }

    let user = User {
        user_id: user_id.clone(),
        password_hash: "hashed".to_string(),
        role: Role::System,
        email: Some("complexity@kalamdb.local".to_string()),
        auth_type: AuthType::Internal,
        auth_data: None,
        storage_mode: StorageMode::Table,
        storage_id: Some(StorageId::local()),
        failed_login_attempts: 0,
        locked_until: None,
        last_login_at: None,
        created_at: now,
        updated_at: now,
        last_seen: None,
        deleted_at: None,
    };

    app_context
        .system_tables()
        .users()
        .create_user(user)
        .expect("Failed to insert admin user");
    user_id
}

fn assert_complexity_error(result: Result<ExecutionResult, KalamDbError>, expected_fragment: &str) {
    match result {
        Err(KalamDbError::InvalidOperation(msg)) => {
            assert!(
                msg.contains(expected_fragment),
                "Expected error to contain '{}', got '{}'",
                expected_fragment,
                msg
            );
        },
        other => panic!("Expected InvalidOperation error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_complexity_uppercase_required() {
    let (executor, app_context, session_ctx) = setup_executor(true).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let result = executor
        .execute(
            "CREATE USER 'no_upper' WITH PASSWORD 'lowercase1!' ROLE user",
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert_complexity_error(result, "uppercase");
}

#[tokio::test]
async fn test_complexity_lowercase_required() {
    let (executor, app_context, session_ctx) = setup_executor(true).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let result = executor
        .execute(
            "CREATE USER 'no_lower' WITH PASSWORD 'UPPERCASE1!' ROLE user",
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert_complexity_error(result, "lowercase");
}

#[tokio::test]
async fn test_complexity_digit_required() {
    let (executor, app_context, session_ctx) = setup_executor(true).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let result = executor
        .execute(
            "CREATE USER 'no_digit' WITH PASSWORD 'NoDigits!' ROLE user",
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert_complexity_error(result, "digit");
}

#[tokio::test]
async fn test_complexity_special_required() {
    let (executor, app_context, session_ctx) = setup_executor(true).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let result = executor
        .execute(
            "CREATE USER 'no_special' WITH PASSWORD 'NoSpecial1' ROLE user",
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert_complexity_error(result, "special character");
}

#[tokio::test]
async fn test_complexity_valid_password_succeeds() {
    let (executor, app_context, session_ctx) = setup_executor(true).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let result = executor
        .execute(
            "CREATE USER 'valid_complex' WITH PASSWORD 'ValidPass1!' ROLE user",
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert!(result.is_ok(), "Valid password should be accepted");
}

#[tokio::test]
async fn test_complexity_disabled_allows_simple_password() {
    let (executor, app_context, session_ctx) = setup_executor(false).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let result = executor
        .execute(
            "CREATE USER 'simple_allowed' WITH PASSWORD 'Simplepass1' ROLE user",
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert!(
        result.is_ok(),
        "Password complexity disabled should allow simple passwords: {:?}",
        result
    );
}

#[tokio::test]
async fn test_complexity_alter_user_requires_special_character() {
    let (executor, app_context, session_ctx) = setup_executor(true).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let username = format!("alter_target_{}", std::process::id());

    executor
        .execute(
            &format!("CREATE USER '{}' WITH PASSWORD 'ValidPass1!' ROLE user", username),
            &exec_ctx,
            Vec::new(),
        )
        .await
        .expect("Initial CREATE USER should succeed");

    let result = executor
        .execute(
            &format!("ALTER USER '{}' SET PASSWORD 'NoSpecial2'", username),
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert_complexity_error(result, "special character");
}

#[tokio::test]
async fn test_complexity_alter_user_valid_password_succeeds() {
    let (executor, app_context, session_ctx) = setup_executor(true).await;
    let admin_id = create_admin_user(&app_context).await;
    let exec_ctx = ExecutionContext::new(admin_id.clone(), Role::System, session_ctx.clone());

    let username = format!("alter_target_ok_{}", std::process::id());

    executor
        .execute(
            &format!("CREATE USER '{}' WITH PASSWORD 'ValidPass1!' ROLE user", username),
            &exec_ctx,
            Vec::new(),
        )
        .await
        .expect("Initial CREATE USER should succeed");

    let result = executor
        .execute(
            &format!("ALTER USER '{}' SET PASSWORD 'AnotherPass2@'", username),
            &exec_ctx,
            Vec::new(),
        )
        .await;

    assert!(result.is_ok(), "ALTER USER with complex password should succeed: {:?}", result);
}
