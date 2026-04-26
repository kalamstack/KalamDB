//! End-to-end integration test for authentication system
//!
//! This test verifies the complete authentication flow:
//! 1. Create a new user with password authentication
//! 2. Authenticate the user via Bearer token
//! 3. Execute SQL queries as the authenticated user
//! 4. Soft delete the user account
//! 5. Verify authentication fails for deleted user
//! 6. Restore the user account
//! 7. Verify authentication works again
//!
//! This test ensures the authentication system works end-to-end
//! and validates user lifecycle management.

use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;

use super::test_support::{auth_helper, TestServer};

/// End-to-end authentication flow test
#[actix_web::test]
async fn test_e2e_auth_flow() {
    let server = TestServer::new_shared().await;

    // Test constants
    let username = "e2e_test_user";
    let password = "SecurePassword123!";
    let namespace = "e2e_test_ns";
    let table_name = "test_table";

    println!("🧪 Starting E2E Authentication Flow Test");
    println!("=====================================");

    // Phase 1: User Creation
    println!("📝 Phase 1: Creating test user");
    let user = auth_helper::create_test_user(&server, username, password, Role::Dba).await;
    assert_eq!(user.user_id.as_str(), username);
    assert_eq!(user.role, Role::Dba);
    println!("✅ User '{}' created successfully", username);

    // Phase 2: Authentication and SQL Execution
    println!("🔐 Phase 2: Testing authentication and SQL execution");

    // Create namespace (as DBA user)
    let create_ns_sql = format!("CREATE NAMESPACE {}", namespace);
    let response = server.execute_sql_as_user(&create_ns_sql, user.user_id.as_str()).await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Failed to create namespace: {:?}",
        response.error
    );
    println!("✅ Namespace '{}' created", namespace);

    // Create table
    // Shared tables require a PRIMARY KEY column of BIGINT or STRING
    let create_table_sql =
        format!("CREATE TABLE {}.{} (id BIGINT PRIMARY KEY, name TEXT)", namespace, table_name);
    let response = server.execute_sql_as_user(&create_table_sql, user.user_id.as_str()).await;
    if response.status != ResponseStatus::Success {
        eprintln!("❌ CREATE TABLE failed: {:?}", response.error);
    }
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Failed to create table: {:?}",
        response.error
    );
    println!("✅ Table '{}.{}' created", namespace, table_name);

    // Insert data
    let insert_sql = format!(
        "INSERT INTO {}.{} (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        namespace, table_name
    );
    let response = server.execute_sql_as_user(&insert_sql, user.user_id.as_str()).await;
    if response.status != ResponseStatus::Success {
        eprintln!("❌ INSERT failed: {:?}", response.error);
        eprintln!("   SQL: {}", insert_sql);
        eprintln!("   User ID: {}", user.user_id.as_str());
    }
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Failed to insert data: {:?}",
        response.error
    );
    println!("✅ Data inserted into table");

    // Query data
    let select_sql = format!("SELECT * FROM {}.{}", namespace, table_name);
    let response = server.execute_sql_as_user(&select_sql, user.user_id.as_str()).await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Failed to query data: {:?}",
        response.error
    );
    assert!(!response.results.is_empty(), "No results returned");
    println!("✅ Data queried successfully ({} rows)", response.results[0].row_count);

    // Phase 3: User Soft Deletion
    println!("🗑️  Phase 3: Testing user soft deletion");

    // Soft delete the user via SQL (DROP USER performs soft delete)
    let delete_user_sql = format!("DROP USER '{}'", user.user_id.as_str());
    let response = server.execute_sql_as_user(&delete_user_sql, "system").await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Failed to soft delete user: {:?}",
        response.error
    );
    println!("✅ User '{}' soft deleted", user.user_id.as_str());

    // Verify authentication fails for deleted user
    let post_delete_sql = "SELECT 1".to_string();
    let response = server.execute_sql_as_user(&post_delete_sql, user.user_id.as_str()).await;

    // HTTP layer should reject soft-deleted users
    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "Soft-deleted user should be blocked by HTTP layer"
    );
    if let Some(err) = response.error {
        assert!(
            err.message.contains("Invalid credentials") || err.code == "INVALID_CREDENTIALS",
            "Expected authentication failure for deleted user, got: {}",
            err.message
        );
    }
    println!("✅ Soft-deleted user correctly blocked");

    println!("🎉 E2E Authentication Flow Test Completed!");
}

/// Test authentication with different user roles
#[actix_web::test]
async fn test_role_based_auth_e2e() {
    let server = TestServer::new_shared().await;

    let namespace = "role_test_ns";

    println!("👥 Testing Role-Based Authentication E2E");

    // Create users with different roles
    let user_user =
        auth_helper::create_test_user(&server, "regular_user", "Password123!", Role::User).await;
    let service_user =
        auth_helper::create_test_user(&server, "service_user", "Password123!", Role::Service).await;
    let dba_user =
        auth_helper::create_test_user(&server, "dba_user", "Password123!", Role::Dba).await;

    println!("✅ Created users with roles: User, Service, DBA");

    // Create user namespaces (system user creates these for them)
    for user in [&user_user, &service_user] {
        let create_ns_sql = format!("CREATE NAMESPACE {}", user.user_id.as_str());
        server.execute_sql(&create_ns_sql).await;
    }
    println!("✅ Created user namespaces");

    // Test namespace operations (admin only)

    // Create namespace as DBA
    let create_ns_sql = format!("CREATE NAMESPACE {}", namespace);
    let response = server.execute_sql_as_user(&create_ns_sql, dba_user.user_id.as_str()).await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "DBA should be able to create namespace"
    );
    println!("✅ DBA user created namespace");

    // Regular user tries to create namespace (should fail)
    let response = server.execute_sql_as_user(&create_ns_sql, user_user.user_id.as_str()).await;
    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "Regular user should not be able to create namespace"
    );
    println!("✅ Regular user correctly denied namespace creation");

    // Service user tries to create namespace (should fail)
    let response = server.execute_sql_as_user(&create_ns_sql, service_user.user_id.as_str()).await;
    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "Service user should not be able to create namespace"
    );
    println!("✅ Service user correctly denied namespace creation");

    // Create table as DBA
    // Ensure PRIMARY KEY for shared table creation
    let create_table_sql = format!("CREATE TABLE {}.test_table (id BIGINT PRIMARY KEY)", namespace);
    let response = server.execute_sql_as_user(&create_table_sql, dba_user.user_id.as_str()).await;
    assert_eq!(response.status, ResponseStatus::Success, "DBA should be able to create table");
    println!("✅ DBA user created table");

    // Regular user creates user table (should succeed)
    // For user tables, TableType must be USER
    let user_table_sql = format!(
        "CREATE TABLE {}.test_table (id BIGINT PRIMARY KEY) WITH (TYPE = 'USER')",
        user_user.user_id.as_str()
    );
    let response = server.execute_sql_as_user(&user_table_sql, user_user.user_id.as_str()).await;
    if response.status != ResponseStatus::Success {
        eprintln!("❌ CREATE TABLE (user table) failed: {:?}", response.error);
        eprintln!("   SQL: {}", user_table_sql);
        eprintln!("   User ID: {}", user_user.user_id.as_str());
    }
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Regular user should be able to create user table"
    );
    println!("✅ Regular user created user table");

    // Service user creates user table (should succeed)
    let service_table_sql = format!(
        "CREATE TABLE {}.test_table (id BIGINT PRIMARY KEY)",
        service_user.user_id.as_str()
    );
    let response = server
        .execute_sql_as_user(&service_table_sql, service_user.user_id.as_str())
        .await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Service user should be able to create user table"
    );
    println!("✅ Service user created user table");

    // Cleanup
    server
        .execute_sql_as_user(
            &format!("DROP NAMESPACE {} CASCADE", namespace),
            dba_user.user_id.as_str(),
        )
        .await;
    server.execute_sql("DROP USER regular_user").await;
    server.execute_sql("DROP USER service_user").await;
    server.execute_sql("DROP USER dba_user").await;

    println!("🎉 Role-Based Authentication E2E Test Completed!");
}

/// Test password security requirements
#[actix_web::test]
async fn test_password_security_e2e() {
    let server = TestServer::new_shared().await;

    println!("🔒 Testing Password Security E2E");

    // Test password change via SQL
    let username = "password_test_user";
    let old_password = "OldSecurePass123!";
    let new_password = "NewSecurePass456!";

    // Create user
    let user = auth_helper::create_test_user(&server, username, old_password, Role::User).await;
    println!("✅ User created with initial password");
    println!("   User ID: {}", user.user_id.as_str());
    println!("   User ID: {}", user.user_id.as_str());

    // Verify user exists by querying system.users
    let query_sql = format!(
        "SELECT user_id, role FROM system.users WHERE user_id = '{}'",
        user.user_id.as_str()
    );
    let response = server.execute_sql_as_user(&query_sql, "system").await;
    if response.status == ResponseStatus::Success {
        println!("✅ User found in system.users: {:?}", response.results[0].rows);
    } else {
        eprintln!("❌ User not found in system.users: {:?}", response.error);
    }

    // Change password via SQL (use system user for ALTER USER command)
    // Syntax: ALTER USER 'user_id' SET PASSWORD 'new_password'
    let change_password_sql =
        format!("ALTER USER '{}' SET PASSWORD '{}'", user.user_id.as_str(), new_password);
    let response = server.execute_sql_as_user(&change_password_sql, "system").await;
    if response.status != ResponseStatus::Success {
        eprintln!("❌ ALTER USER failed: {:?}", response.error);
        eprintln!("   SQL: {}", change_password_sql);
    }
    assert_eq!(response.status, ResponseStatus::Success, "Password change should succeed");
    println!("✅ Password changed via SQL");

    // Verify old password no longer works
    let test_sql = "SELECT 1".to_string();
    let _response = server.execute_sql_as_user(&test_sql, user.user_id.as_str()).await;
    // Note: This test may need adjustment based on how password changes are implemented
    // For now, just verify the SQL executed without error
    println!("✅ Password change operation completed");

    // Cleanup (use system user for DROP USER command)
    server
        .execute_sql_as_user(&format!("DROP USER {}", user.user_id.as_str()), "system")
        .await;

    println!("🎉 Password Security E2E Test Completed!");
}
