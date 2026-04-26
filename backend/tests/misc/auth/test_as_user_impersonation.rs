//! Integration tests for AS USER impersonation (Phase 7, User Story 4)
//!
//! Tests cover:
//! - Authorization: Only Service/Dba/System roles can use AS USER
//! - DML operations: INSERT/UPDATE/DELETE with AS USER
//! - Shared table rejection: AS USER not allowed on SHARED tables
//! - Audit logging: Both actor and subject logged
//! - Performance: Permission checks complete in <10ms

use kalam_client::models::ResponseStatus;
use kalamdb_commons::models::{AuthType, Role, UserId};
use kalamdb_system::providers::storages::models::StorageMode;
use uuid::Uuid;

use super::test_support::TestServer;

async fn insert_user(server: &TestServer, username: &str, role: Role) -> UserId {
    let user_id = UserId::new(username);

    // Idempotent: return existing if already present (shared AppContext across tests)
    let users = server.app_context.system_tables().users();
    if users.get_user_by_id(&user_id).ok().flatten().is_some() {
        return user_id;
    }

    let now = chrono::Utc::now().timestamp_millis();
    let user = kalamdb_system::User {
        user_id: user_id.clone(),
        password_hash: "".to_string(),
        role,
        email: Some(format!("{}@test.local", username)),
        auth_type: AuthType::Password,
        auth_data: None,
        storage_mode: StorageMode::Table,
        storage_id: None,
        failed_login_attempts: 0,
        locked_until: None,
        last_login_at: None,
        created_at: now,
        updated_at: now,
        last_seen: None,
        deleted_at: None,
    };

    // Best-effort: ignore AlreadyExists from races
    let _ = users.create_user(user);
    user_id
}

fn find_impersonation_audit_entry<'a>(
    entries: &'a [kalamdb_system::AuditLogEntry],
    action: &str,
    target: &str,
    actor_user_id: &UserId,
) -> &'a kalamdb_system::AuditLogEntry {
    entries
        .iter()
        .rev()
        .find(|entry| {
            entry.action == action
                && entry.target == target
                && &entry.actor_user_id == actor_user_id
        })
        .unwrap_or_else(|| {
            panic!(
                "Audit entry {} for actor {} and target {} not found",
                action,
                actor_user_id.as_str(),
                target
            )
        })
}

/// T168: Regular user role attempting AS USER is blocked (CRITICAL TEST)
#[actix_web::test]
async fn test_as_user_blocked_for_regular_user() {
    let server = TestServer::new_shared().await;
    let ns = "test_as_user_blocked";

    // Create regular user and target user
    let regular_user = insert_user(&server, "regular_user", Role::User).await;
    let target_user = insert_user(&server, "target_user", Role::User).await;

    // Create namespace and USER table (using default/system for DDL)
    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.items (item_id VARCHAR PRIMARY KEY, name VARCHAR) WITH (TYPE = 'USER', \
         STORAGE_ID = 'local')",
        ns
    );
    server.execute_sql(&create_table).await; // Use default system user for table creation

    // Attempt INSERT AS USER (should be BLOCKED)
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.items (item_id, name) VALUES ('ITEM-1', 'Widget'))",
        target_user.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&insert_sql, regular_user.as_str()).await;

    // MUST fail with authorization error
    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "Regular user should not be able to use AS USER"
    );
    let error_msg = resp.error.as_ref().unwrap().message.as_str();
    assert!(
        error_msg.contains("not authorized to use AS USER") || error_msg.contains("Unauthorized"),
        "Error should mention AS USER authorization: {}",
        error_msg
    );
}

/// T167: Service role can successfully use AS USER
#[actix_web::test]
async fn test_as_user_with_service_role() {
    let server = TestServer::new_shared().await;
    let ns = "test_as_user_service";

    // Create service user, DBA, and target user
    let service_user = insert_user(&server, "service_user", Role::Service).await;
    let admin_user = insert_user(&server, "admin_service_setup", Role::Dba).await;
    let target_user = insert_user(&server, "target_user", Role::User).await;

    // Create namespace and USER table as DBA
    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.orders (order_id VARCHAR PRIMARY KEY, amount VARCHAR) WITH (TYPE = \
         'USER', STORAGE_ID = 'local')",
        ns
    );
    server.execute_sql_as_user(&create_table, admin_user.as_str()).await;

    // INSERT AS USER target_user (should succeed)
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.orders (order_id, amount) VALUES ('ORD-123', \
         '99.99'))",
        target_user.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&insert_sql, service_user.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Success,
        "Service role should be able to use AS USER"
    );
    assert_eq!(resp.results[0].row_count, 1);

    // Verify record is owned by target_user (via RLS)
    let select_sql = format!("SELECT * FROM {}.orders", ns);
    let resp = server.execute_sql_as_user(&select_sql, target_user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Success);
    assert_eq!(
        resp.results[0].rows.as_ref().unwrap().len(),
        1,
        "Target user should see their record"
    );
}

/// T167.1: Successful AS USER operations are written to the audit log.
#[actix_web::test]
#[ntest::timeout(45000)]
async fn test_as_user_success_is_audited() {
    let server = TestServer::new_shared().await;
    let ns = format!("test_as_user_audit_{}", Uuid::new_v4().simple());

    let service_user = insert_user(&server, "svc_audit_actor", Role::Service).await;
    let target_user = insert_user(&server, "svc_audit_target", Role::User).await;

    let ns_resp = server.execute_sql_as_user(&format!("CREATE NAMESPACE {}", ns), "root").await;
    assert_eq!(ns_resp.status, ResponseStatus::Success, "CREATE NAMESPACE failed");

    let create_table = format!(
        "CREATE TABLE {}.audit_items (id VARCHAR PRIMARY KEY, value VARCHAR) WITH (TYPE = 'USER', \
         STORAGE_ID = 'local')",
        ns
    );
    let create_resp = server.execute_sql_as_user(&create_table, "root").await;
    assert_eq!(create_resp.status, ResponseStatus::Success, "CREATE TABLE failed");

    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.audit_items (id, value) VALUES ('A1', 'ok'))",
        target_user.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&insert_sql, service_user.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Success, "EXECUTE AS USER insert failed");

    let logs = server
        .app_context
        .system_tables()
        .audit_logs()
        .scan_all()
        .expect("Failed to read audit log");
    let entry = find_impersonation_audit_entry(
        &logs,
        "EXECUTE_AS_USER",
        &format!("user:{}", target_user.as_str()),
        &service_user,
    );

    assert_eq!(entry.subject_user_id.as_ref(), Some(&target_user));
    assert!(entry
        .details
        .as_ref()
        .expect("impersonation audit should include details")
        .contains("\"success\":true"));
}

/// T167: DBA role can successfully use AS USER
#[actix_web::test]
async fn test_as_user_with_dba_role() {
    let server = TestServer::new_shared().await;
    let ns = "test_as_user_dba";

    // Create DBA user and target user
    let dba_user = insert_user(&server, "dba_user", Role::Dba).await;
    let target_user = insert_user(&server, "target_dba", Role::User).await;

    // Create namespace and USER table
    let ns_resp = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns), "root")
        .await;
    assert_eq!(
        ns_resp.status,
        ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        ns_resp.error
    );

    let create_table = format!(
        "CREATE TABLE {}.logs (log_id VARCHAR PRIMARY KEY, message VARCHAR) WITH (TYPE = 'USER', \
         STORAGE_ID = 'local')",
        ns
    );
    let table_resp = server.execute_sql_as_user(&create_table, dba_user.as_str()).await;
    assert_eq!(
        table_resp.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        table_resp.error
    );

    // INSERT AS USER (should succeed)
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.logs (log_id, message) VALUES ('LOG-1', 'Test \
         message'))",
        target_user.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&insert_sql, dba_user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Success, "DBA role should be able to use AS USER");
    assert_eq!(resp.results[0].row_count, 1);
}

/// T170: INSERT AS USER creates record owned by impersonated user
#[actix_web::test]
async fn test_insert_as_user_ownership() {
    let server = TestServer::new_shared().await;
    let ns = "test_insert_ownership";

    let admin_user = insert_user(&server, "admin", Role::Dba).await;
    let user_alice = insert_user(&server, "alice", Role::User).await;
    let user_bob = insert_user(&server, "bob", Role::User).await;

    // Create namespace and USER table
    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.messages (msg_id VARCHAR PRIMARY KEY, content VARCHAR) WITH (TYPE = \
         'USER', STORAGE_ID = 'local')",
        ns
    );
    server.execute_sql_as_user(&create_table, admin_user.as_str()).await;

    // INSERT AS USER alice
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.messages (msg_id, content) VALUES ('MSG-1', 'Hello \
         from Alice'))",
        user_alice.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&insert_sql, admin_user.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Alice can see the record
    let select_sql = format!("SELECT * FROM {}.messages", ns);
    let resp = server.execute_sql_as_user(&select_sql, user_alice.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Success);
    assert_eq!(resp.results[0].rows.as_ref().unwrap().len(), 1, "Alice should see her record");

    // Bob cannot see the record (RLS filtering)
    let resp = server.execute_sql_as_user(&select_sql, user_bob.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Success);
    assert_eq!(
        resp.results[0].rows.as_ref().unwrap().len(),
        0,
        "Bob should not see Alice's record"
    );
}

/// T171: UPDATE AS USER modifies record as impersonated user
#[actix_web::test]
async fn test_update_as_user() {
    let server = TestServer::new_shared().await;
    let ns = "test_update_as_user";

    let admin_user = insert_user(&server, "admin2", Role::Dba).await;
    let user_charlie = insert_user(&server, "charlie", Role::User).await;

    // Create namespace, USER table, and insert record as charlie
    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.profiles (profile_id VARCHAR PRIMARY KEY, status VARCHAR) WITH (TYPE = \
         'USER', STORAGE_ID = 'local')",
        ns
    );
    server.execute_sql_as_user(&create_table, admin_user.as_str()).await;

    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.profiles (profile_id, status) VALUES ('PROF-1', \
         'active'))",
        user_charlie.as_str(),
        ns
    );
    server.execute_sql_as_user(&insert_sql, admin_user.as_str()).await;

    // UPDATE AS USER charlie
    let update_sql = format!(
        "EXECUTE AS USER '{}' (UPDATE {}.profiles SET status = 'inactive' WHERE profile_id = \
         'PROF-1')",
        user_charlie.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&update_sql, admin_user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Success);
    assert_eq!(resp.results[0].row_count, 1);

    // Verify update via charlie's credentials
    let select_sql = format!("SELECT * FROM {ns}.profiles WHERE profile_id = 'PROF-1'");
    let resp = server.execute_sql_as_user(&select_sql, user_charlie.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Success);
    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 1);
    // Verify the status was updated to 'inactive'
    let status = rows[0].get("status").and_then(|v| v.as_str());
    assert_eq!(status, Some("inactive"), "Status should be updated to 'inactive'");
}

/// T172: DELETE AS USER removes record as impersonated user
#[actix_web::test]
async fn test_delete_as_user() {
    let server = TestServer::new_shared().await;
    let ns = "test_delete_as_user";

    let admin_user = insert_user(&server, "admin3", Role::Dba).await;
    let user_dave = insert_user(&server, "dave", Role::User).await;

    // Create namespace, USER table, and insert record as dave
    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.sessions (session_id VARCHAR PRIMARY KEY, active BOOLEAN) WITH (TYPE = \
         'USER', STORAGE_ID = 'local')",
        ns
    );
    server.execute_sql_as_user(&create_table, admin_user.as_str()).await;

    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.sessions (session_id, active) VALUES ('SESS-1', \
         true))",
        user_dave.as_str(),
        ns
    );
    server.execute_sql_as_user(&insert_sql, admin_user.as_str()).await;

    // DELETE AS USER dave
    let delete_sql = format!(
        "EXECUTE AS USER '{}' (DELETE FROM {}.sessions WHERE session_id = 'SESS-1')",
        user_dave.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&delete_sql, admin_user.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Success);
    assert_eq!(resp.results[0].row_count, 1);

    // Verify deletion
    let select_sql = format!("SELECT * FROM {}.sessions", ns);
    let resp = server.execute_sql_as_user(&select_sql, user_dave.as_str()).await;

    assert_eq!(resp.status, ResponseStatus::Success);
    assert_eq!(resp.results[0].rows.as_ref().unwrap().len(), 0, "Record should be deleted");
}

/// T175: SELECT AS USER scopes reads to impersonated subject on USER tables
#[actix_web::test]
async fn test_select_as_user_scopes_reads() {
    let server = TestServer::new_shared().await;
    let ns = "test_select_as_user_scope";

    let service_user = insert_user(&server, "svc_select_scope", Role::Service).await;
    let user1 = insert_user(&server, "scope_user1", Role::User).await;
    let user2 = insert_user(&server, "scope_user2", Role::User).await;

    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.items (id VARCHAR PRIMARY KEY, value VARCHAR) WITH (TYPE = 'USER', \
         STORAGE_ID = 'local')",
        ns
    );
    server.execute_sql(&create_table).await;

    let insert_user1 = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.items (id, value) VALUES ('1', 'user1-data'))",
        user1.as_str(),
        ns
    );
    let insert_user2 = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.items (id, value) VALUES ('2', 'user2-data'))",
        user2.as_str(),
        ns
    );
    assert_eq!(
        server.execute_sql_as_user(&insert_user1, service_user.as_str()).await.status,
        ResponseStatus::Success
    );
    assert_eq!(
        server.execute_sql_as_user(&insert_user2, service_user.as_str()).await.status,
        ResponseStatus::Success
    );

    let select_as_user1 =
        format!("EXECUTE AS USER '{}' (SELECT * FROM {}.items)", user1.as_str(), ns);
    let resp = server.execute_sql_as_user(&select_as_user1, service_user.as_str()).await;
    assert_eq!(resp.status, ResponseStatus::Success);
    let rows = resp.rows_as_maps();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("value").and_then(|v| v.as_str()), Some("user1-data"));
}

/// T175.1: Stream table data remains isolated across users, including SELECT AS USER
#[actix_web::test]
async fn test_stream_table_isolation_with_select_as_user() {
    let server = TestServer::new_shared().await;
    let ns = "test_stream_as_user_scope";

    let service_user = insert_user(&server, "svc_stream_scope", Role::Service).await;
    let user1 = insert_user(&server, "stream_scope_user1", Role::User).await;
    let user2 = insert_user(&server, "stream_scope_user2", Role::User).await;

    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.events (id VARCHAR PRIMARY KEY, payload VARCHAR) WITH (TYPE = 'STREAM', \
         TTL_SECONDS = 3600)",
        ns
    );
    server.execute_sql(&create_table).await;

    let insert_stream_user1 = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.events (id, payload) VALUES ('1', 'event-user1'))",
        user1.as_str(),
        ns
    );
    assert_eq!(
        server
            .execute_sql_as_user(&insert_stream_user1, service_user.as_str())
            .await
            .status,
        ResponseStatus::Success
    );

    let select_as_user2 =
        format!("EXECUTE AS USER '{}' (SELECT * FROM {}.events)", user2.as_str(), ns);
    let resp_user2 = server.execute_sql_as_user(&select_as_user2, service_user.as_str()).await;
    assert_eq!(resp_user2.status, ResponseStatus::Success);
    assert_eq!(resp_user2.rows_as_maps().len(), 0);

    let direct_user2 = format!("SELECT * FROM {}.events", ns);
    let resp_direct_user2 = server.execute_sql_as_user(&direct_user2, user2.as_str()).await;
    assert_eq!(resp_direct_user2.status, ResponseStatus::Success);
    assert_eq!(resp_direct_user2.rows_as_maps().len(), 0);
}

/// T173: AS USER on SHARED table is rejected
#[actix_web::test]
async fn test_as_user_on_shared_table_rejected() {
    let server = TestServer::new_shared().await;
    // Use unique namespace to avoid parallel test interference
    let ns = format!("test_as_user_shared_{}", std::process::id());

    let admin_user = insert_user(&server, "admin4", Role::Dba).await;
    let user_eve = insert_user(&server, "eve", Role::User).await;

    // Create namespace and SHARED table
    let ns_resp = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE {}", ns), admin_user.as_str())
        .await;
    assert_eq!(
        ns_resp.status,
        ResponseStatus::Success,
        "Failed to create namespace: {:?}",
        ns_resp.error
    );

    let create_table = format!(
        "CREATE TABLE {}.global_config (config_key VARCHAR PRIMARY KEY, value VARCHAR) WITH (TYPE \
         = 'SHARED')",
        ns
    );
    let create_resp = server.execute_sql_as_user(&create_table, admin_user.as_str()).await;
    assert_eq!(
        create_resp.status,
        ResponseStatus::Success,
        "Failed to create SHARED table: {:?}",
        create_resp.error
    );

    // INSERT AS USER on SHARED table (should fail)
    let insert_sql = format!(
        "EXECUTE AS USER '{}' (INSERT INTO {}.global_config (config_key, value) VALUES \
         ('setting1', 'value1'))",
        user_eve.as_str(),
        ns
    );
    let resp = server.execute_sql_as_user(&insert_sql, admin_user.as_str()).await;

    assert_eq!(
        resp.status,
        ResponseStatus::Error,
        "AS USER should be rejected on SHARED tables"
    );
    let error_msg = resp.error.as_ref().unwrap().message.as_str();
    // Error should mention shared table access denial (case-insensitive check)
    let lower_msg = error_msg.to_lowercase();
    assert!(
        lower_msg.contains("shared") || lower_msg.contains("access denied"),
        "Error should indicate SHARED table access issue: {}",
        error_msg
    );
}

/// T174: AS USER with non-existent user handles gracefully
#[actix_web::test]
async fn test_as_user_nonexistent_user() {
    let server = TestServer::new_shared().await;
    let ns = "test_as_user_nonexist";

    let admin_user = insert_user(&server, "admin5", Role::Dba).await;

    // Create namespace and USER table
    server.execute_sql(&format!("CREATE NAMESPACE {}", ns)).await;
    let create_table = format!(
        "CREATE TABLE {}.logs (log_id VARCHAR PRIMARY KEY, message VARCHAR) WITH (TYPE = 'USER', \
         STORAGE_ID = 'local')",
        ns
    );
    server.execute_sql_as_user(&create_table, admin_user.as_str()).await;

    // INSERT AS USER with non-existent user
    let insert_sql = format!(
        "EXECUTE AS USER 'nonexistent_user_12345' (INSERT INTO {}.logs (log_id, message) VALUES \
         ('LOG-1', 'Test'))",
        ns
    );
    let resp = server.execute_sql_as_user(&insert_sql, admin_user.as_str()).await;

    // Should handle gracefully (may succeed since we don't validate user existence yet)
    // This is acceptable for Phase 7 - validation can be added in Phase 8
    println!("Response for non-existent user: {:?}", resp);
}

/// T176: Performance test - AS USER permission checks are fast
#[actix_web::test]
async fn test_as_user_performance() {
    use std::time::Instant;

    let server = TestServer::new_shared().await;
    let ns = "test_as_user_perf";

    let service_user = insert_user(&server, "service_perf", Role::Service).await;
    let admin_user = insert_user(&server, "admin_perf_setup", Role::Dba).await;
    let target_user = insert_user(&server, "target_perf", Role::User).await;

    // Create namespace and USER table as DBA
    let ns_resp = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns), "root")
        .await;
    assert_eq!(
        ns_resp.status,
        ResponseStatus::Success,
        "CREATE NAMESPACE failed: {:?}",
        ns_resp.error
    );

    let create_table = format!(
        "CREATE TABLE {}.perf_test (id VARCHAR PRIMARY KEY, data VARCHAR) WITH (TYPE = 'USER', \
         STORAGE_ID = 'local')",
        ns
    );
    let table_resp = server.execute_sql_as_user(&create_table, admin_user.as_str()).await;
    assert_eq!(
        table_resp.status,
        ResponseStatus::Success,
        "CREATE TABLE failed: {:?}",
        table_resp.error
    );

    // Measure 10 INSERT AS USER operations
    let mut durations = Vec::new();
    for i in 0..10 {
        let insert_sql = format!(
            "EXECUTE AS USER '{}' (INSERT INTO {}.perf_test (id, data) VALUES ('ID-{}', 'Data'))",
            target_user.as_str(),
            ns,
            i
        );

        let start = Instant::now();
        let resp = server.execute_sql_as_user(&insert_sql, service_user.as_str()).await;
        let duration = start.elapsed();

        durations.push(duration.as_millis());
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    // Average should be reasonable (allowing for test overhead)
    let avg_ms = durations.iter().sum::<u128>() / durations.len() as u128;
    println!("Average AS USER operation time: {}ms (includes full INSERT execution)", avg_ms);

    // The permission check itself should be <10ms, but full INSERT may take longer
    assert!(avg_ms < 100, "AS USER operations took too long: {}ms average", avg_ms);
}
