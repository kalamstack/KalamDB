//! HTTP-backed TestServer wrapper for integration tests.
//!
//! Provides a TestServer API similar to the legacy in-process test server,
//! but backed by the shared HttpTestServer instance.

use super::http_server::{self, HttpTestServer};
use datafusion::prelude::SessionContext;
use kalam_client::models::{ErrorDetail, QueryResponse, ResponseStatus};
use kalamdb_auth::{CoreUsersRepo, UserRepository};
use kalamdb_commons::constants::AuthConstants;
use kalamdb_commons::{AuthType, Role, StorageId, UserId, UserName};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::executor::SqlExecutor;
use kalamdb_system::providers::storages::models::StorageMode;
use std::sync::Arc;

/// Test server instance backed by the shared HTTP server.
#[derive(Clone)]
pub struct TestServer {
    http: &'static HttpTestServer,
    pub app_context: Arc<AppContext>,
    pub session_context: Arc<SessionContext>,
    pub sql_executor: Arc<SqlExecutor>,
}

impl TestServer {
    /// Create a new TestServer with an isolated HTTP server instance.
    /// Each test gets its own server, RocksDB instance, and temp directory.
    /// This ensures complete test isolation but is slower than using the global server.
    ///
    /// **DEPRECATED**: Use `new_shared()` instead. Isolated servers have issues with
    /// table catalog and query routing. All tests should use the shared global server.
    #[deprecated(note = "Use new_shared() instead - isolated servers have catalog issues")]
    pub async fn new() -> Self {
        // Create an isolated server instance for this test
        let http = Box::leak(Box::new(
            http_server::start_http_test_server()
                .await
                .expect("Failed to start isolated HTTP test server"),
        ));

        let app_context = http.app_context();
        let session_context = app_context.base_session_context();
        let sql_executor = app_context.sql_executor();

        // Ensure root/system user has a password hash for auth tests
        let system_user_id = UserId::new(AuthConstants::DEFAULT_ROOT_USER_ID);
        if let Ok(Some(mut user)) =
            app_context.system_tables().users().get_user_by_id(&system_user_id)
        {
            if user.password_hash.is_empty() {
                user.password_hash =
                    bcrypt::hash("admin", bcrypt::DEFAULT_COST).unwrap_or_default();
                user.updated_at = chrono::Utc::now().timestamp_millis();
                let _ = app_context.system_tables().users().update_user(user);
            }
        }

        Self {
            http,
            app_context,
            session_context,
            sql_executor,
        }
    }

    /// Create a TestServer using the shared global HTTP server.
    /// Tests using this share the same server, RocksDB, and temp directory.
    /// Much faster but requires tests to use unique namespaces/tables.
    /// Only use this for simple read-only tests or tests that coordinate their resources.
    pub async fn new_shared() -> Self {
        let http = http_server::get_global_server().await;
        let app_context = http.app_context();
        let session_context = app_context.base_session_context();
        let sql_executor = app_context.sql_executor();

        // Ensure root/system user has a password hash for auth tests
        let system_user_id = UserId::new(AuthConstants::DEFAULT_ROOT_USER_ID);
        if let Ok(Some(mut user)) =
            app_context.system_tables().users().get_user_by_id(&system_user_id)
        {
            if user.password_hash.is_empty() {
                user.password_hash =
                    bcrypt::hash("admin", bcrypt::DEFAULT_COST).unwrap_or_default();
                user.updated_at = chrono::Utc::now().timestamp_millis();
                let _ = app_context.system_tables().users().update_user(user);
            }
        }

        Self {
            http,
            app_context,
            session_context,
            sql_executor,
        }
    }

    /// Execute SQL as the root user (HTTP).
    pub async fn execute_sql(&self, sql: &str) -> QueryResponse {
        let root_id = UserId::new(AuthConstants::DEFAULT_ROOT_USER_ID);
        let root_name = UserName::new(AuthConstants::DEFAULT_SYSTEM_USERNAME);
        let token = self.http.create_jwt_token_with_id(&root_id, &root_name, &Role::System);
        let auth_header = format!("Bearer {}", token);

        match self.http.execute_sql_with_auth(sql, &auth_header).await {
            Ok(resp) => resp,
            Err(err) => QueryResponse {
                status: ResponseStatus::Error,
                results: vec![],
                took: None,
                error: Some(ErrorDetail {
                    code: String::new(),
                    message: format!("HTTP SQL failed: {}", err),
                    details: None,
                }),
            },
        }
    }

    /// Execute SQL as a specific user (HTTP).
    pub async fn execute_sql_as_user(&self, sql: &str, user_id: &str) -> QueryResponse {
        // Map admin aliases to root auth for convenience
        let user_id_lower = user_id.to_lowercase();
        if user_id_lower == "root" || user_id_lower == "system" || user_id_lower == "admin" {
            return self.execute_sql(sql).await;
        }

        let user_id_obj = UserId::new(user_id);

        // Ensure user exists with at least a USER role
        let users_provider = self.app_context.system_tables().users();
        let role = if let Ok(Some(user)) = users_provider.get_user_by_id(&user_id_obj) {
            // Check if user is soft-deleted
            if user.deleted_at.is_some() {
                return QueryResponse {
                    status: ResponseStatus::Error,
                    results: vec![],
                    took: None,
                    error: Some(ErrorDetail {
                        code: "INVALID_CREDENTIALS".to_string(),
                        message: "Invalid username or password".to_string(),
                        details: None,
                    }),
                };
            }
            user.role
        } else if let Ok(Some(user)) = users_provider.get_user_by_username(user_id) {
            // Check if user is soft-deleted
            if user.deleted_at.is_some() {
                return QueryResponse {
                    status: ResponseStatus::Error,
                    results: vec![],
                    took: None,
                    error: Some(ErrorDetail {
                        code: "INVALID_CREDENTIALS".to_string(),
                        message: "Invalid username or password".to_string(),
                        details: None,
                    }),
                };
            }
            user.role
        } else {
            let password_hash =
                bcrypt::hash("test123", bcrypt::DEFAULT_COST).unwrap_or_else(|_| String::new());
            let user = kalamdb_system::User {
                user_id: user_id_obj.clone(),
                username: user_id.into(),
                password_hash,
                role: Role::User,
                email: Some(format!("{}@test.com", user_id)),
                auth_type: AuthType::Password,
                auth_data: None,
                storage_mode: StorageMode::Table,
                storage_id: None,
                failed_login_attempts: 0,
                locked_until: None,
                last_login_at: None,
                created_at: chrono::Utc::now().timestamp_millis(),
                updated_at: chrono::Utc::now().timestamp_millis(),
                last_seen: None,
                deleted_at: None,
            };
            let _ = users_provider.create_user(user);
            Role::User
        };

        let username = UserName::new(user_id);
        let token = self.http.create_jwt_token_with_id(&user_id_obj, &username, &role);
        let auth_header = format!("Bearer {}", token);

        match self.http.execute_sql_with_auth(sql, &auth_header).await {
            Ok(resp) => resp,
            Err(err) => QueryResponse {
                status: ResponseStatus::Error,
                results: vec![],
                took: None,
                error: Some(ErrorDetail {
                    code: String::new(),
                    message: format!("HTTP SQL failed: {}", err),
                    details: None,
                }),
            },
        }
    }

    /// Helper to create a test user with explicit role and password.
    pub async fn create_user(&self, username: &str, password: &str, role: Role) -> UserId {
        let user_id = UserId::new(username);
        let password_hash =
            bcrypt::hash(password, bcrypt::DEFAULT_COST).expect("Failed to hash password");
        let users_provider = self.app_context.system_tables().users();

        if let Ok(Some(mut existing)) = users_provider.get_user_by_id(&user_id) {
            existing.password_hash = password_hash.clone();
            existing.role = role;
            existing.email = Some(format!("{}@test.com", username));
            existing.auth_type = AuthType::Password;
            existing.auth_data = None;
            existing.failed_login_attempts = 0;
            existing.locked_until = None;
            existing.deleted_at = None;
            existing.updated_at = chrono::Utc::now().timestamp_millis();
            let _ = users_provider.update_user(existing);
            return user_id;
        }

        let user = kalamdb_system::User {
            user_id: user_id.clone(),
            username: username.into(),
            password_hash,
            role,
            email: Some(format!("{}@test.com", username)),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: StorageMode::Table,
            storage_id: Some(StorageId::local()),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: chrono::Utc::now().timestamp_millis(),
            updated_at: chrono::Utc::now().timestamp_millis(),
            last_seen: None,
            deleted_at: None,
        };

        let _ = users_provider.create_user(user);

        user_id
    }

    /// Get a CoreUsersRepo instance connected to the server's user table.
    pub fn users_repo(&self) -> Arc<dyn UserRepository> {
        Arc::new(CoreUsersRepo::new(self.app_context.system_tables().users()))
    }

    /// Check if a namespace exists via system tables.
    pub async fn namespace_exists(&self, namespace: &str) -> bool {
        let query = format!(
            "SELECT namespace_id FROM system.namespaces WHERE namespace_id = '{}'",
            namespace
        );
        let resp = self.execute_sql(&query).await;
        resp.status == ResponseStatus::Success
            && resp
                .results
                .first()
                .and_then(|r| r.rows.as_ref())
                .map(|rows| !rows.is_empty())
                .unwrap_or(false)
    }

    /// Check if a table exists via system tables.
    pub async fn table_exists(&self, namespace: &str, table_name: &str) -> bool {
        let query = format!(
            "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
            namespace, table_name
        );
        let resp = self.execute_sql(&query).await;
        resp.status == ResponseStatus::Success
            && resp
                .results
                .first()
                .and_then(|r| r.rows.as_ref())
                .map(|rows| !rows.is_empty())
                .unwrap_or(false)
    }
}
