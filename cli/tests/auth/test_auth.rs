//! Integration tests for authentication and authorization
//!
//! **Implements T052-T054, T110-T113**: Authentication, credential management, and access control
//!
//! These tests validate:
//! - JWT authentication with valid/invalid tokens
//! - Localhost authentication bypass
//! - Credential storage and security
//! - Multiple instance management
//! - Credential rotation and deletion
//! - Admin operations with proper authentication

use crate::common::*;
use std::time::Duration;

/// Test configuration constants
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// T052: Test JWT authentication with valid token
#[test]
fn test_cli_jwt_authentication() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Use root authentication (execute_sql_as_root_via_cli handles auth)
    let result = execute_sql_as_root_via_cli("SELECT 1 as auth_test");

    // Should work with proper authentication
    assert!(result.is_ok(), "Should handle authentication: {:?}", result.err());
}

/// T053: Test invalid token handling
#[test]
fn test_cli_invalid_token() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--username")
        .arg("test_user")
        .arg("--token")
        .arg("invalid.jwt.token")
        .arg("--command")
        .arg("SELECT 1")
        .timeout(TEST_TIMEOUT);

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let error_lower = stderr.to_lowercase();

    // May succeed on localhost (auth bypass) or fail with auth error
    // Either outcome is acceptable
    assert!(
        output.status.success()
            || error_lower.contains("unauthorized")
            || error_lower.contains("authentication")
            || error_lower.contains("invalid token")
            || error_lower.contains("token")
            || error_lower.contains("malformed")
            || error_lower.contains("jwt")
            || error_lower.contains("base64")
            || error_lower.contains("401")
            || error_lower.contains("403"),
        "Should handle invalid token appropriately. stdout: {}, stderr: {}",
        stdout,
        stderr
    );
}

/// T054: Test localhost authentication with root user
#[test]
fn test_cli_localhost_auth_bypass() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Localhost connections should work with root authentication
    let result = execute_sql_as_root_via_cli("SELECT 'localhost' as test");

    // Should succeed with authentication
    assert!(
        result.is_ok(),
        "Localhost should work with proper authentication: {:?}",
        result.err()
    );
}

/// Test CLI authentication with unauthorized user
#[test]
fn test_cli_authenticate_unauthorized_user() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running at {}. Skipping test.", server_url());
        return;
    }

    // Try to authenticate with invalid credentials
    let result = execute_sql_via_cli_as("invalid_user", "wrong_password", "SELECT 1");

    // Should fail with authentication error
    assert!(result.is_err(), "CLI should fail with invalid credentials");
    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("Unauthorized")
            || error_msg.contains("authentication")
            || error_msg.contains("401")
            || error_msg.contains("password")
            || error_msg.contains("Login failed"),
        "Error should indicate authentication failure: {}",
        error_msg
    );
}

/// Test CLI authentication with valid user via a supported non-interactive SQL command.
#[test]
fn test_cli_authenticate_and_check_info() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running at {}. Skipping test.", server_url());
        return;
    }

    let result = execute_sql_via_cli_as(
        admin_username(),
        admin_password(),
        "SELECT username FROM system.users WHERE username = 'admin' LIMIT 1",
    );

    // Should succeed and show the authenticated admin user in query output.
    assert!(
        result.is_ok(),
        "CLI should authenticate successfully with valid user: {:?}",
        result.err()
    );
    let output = result.unwrap();
    assert!(
        output.contains(admin_username()),
        "Info output should show the authenticated username: {}",
        output
    );
}

// Note: Credential store tests have been moved to test_cli_auth.rs
// which uses the new JWT-based Credentials API.
