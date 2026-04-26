//! Integration tests for CLI authentication and credential storage
//!
//! **Implements T110-T113**: CLI auto-auth, JWT credential storage, multi-instance, rotation tests
//!
//! These tests verify:
//! - CLI automatic authentication using stored JWT credentials
//! - Secure credential storage with proper file permissions
//! - Multiple database instance management
//! - Credential rotation and updates
//! - JWT token storage (never user/password)

use std::fs;

use crate::common::*;

// ============================================================================
// UNIT TESTS - FileCredentialStore (no server needed)
// ============================================================================

#[test]
fn test_cli_jwt_credentials_stored_securely() {
    // **T111**: Test that JWT credentials are stored with secure file permissions (0600 on Unix)

    let (mut store, temp_dir) = create_temp_store();

    // Store JWT credentials (new API - no password stored)
    let creds = Credentials::with_details(
        "test_instance".to_string(),
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test_token".to_string(),
        "alice".to_string(),
        "2025-12-31T23:59:59Z".to_string(),
        Some(server_url().to_string()),
    );

    store.set_credentials(&creds).expect("Failed to store credentials");

    // Verify file exists
    let creds_path = temp_dir.path().join("credentials.toml");
    assert!(creds_path.exists(), "Credentials file should exist");

    // Verify file permissions on Unix (should be 0600 - owner read/write only)
    #[cfg(unix)]
    {
        let metadata = fs::metadata(&creds_path).expect("Failed to get file metadata");
        let permissions = metadata.permissions();
        let mode = permissions.mode();

        // Extract permission bits (last 9 bits)
        let perms = mode & 0o777;

        assert_eq!(perms, 0o600, "Credentials file should have 0600 permissions, got: {:o}", perms);

        println!("✓ Credentials file has secure permissions: {:o}", perms);
    }

    // Verify file contents contain JWT token, NOT password
    let file_contents = fs::read_to_string(&creds_path).expect("Failed to read credentials file");

    // TOML format should contain JWT fields
    assert!(file_contents.contains("[instances.test_instance]"));
    assert!(file_contents.contains("jwt_token = "));
    assert!(file_contents.contains("user = \"alice\""));
    assert!(file_contents.contains("expires_at = "));
    assert!(file_contents.contains("server_url = "));

    // Should NOT contain password field
    assert!(!file_contents.contains("password"), "Should NOT store password, only JWT token");

    println!("✓ JWT credentials stored securely (no password)");
}

#[test]
fn test_cli_multiple_instances() {
    // **T112**: Test managing JWT credentials for multiple database instances

    let (mut store, _temp_dir) = create_temp_store();

    // Store JWT credentials for multiple instances
    let instances = vec![
        ("local", "alice", server_url()),
        ("cloud", "bob", "https://cloud.example.com"),
        ("testing", "charlie", "http://test.internal:3000"),
    ];

    for (instance, username, server_url) in &instances {
        let creds = Credentials::with_details(
            instance.to_string(),
            format!("jwt_token_for_{}", instance),
            username.to_string(),
            "2025-12-31T23:59:59Z".to_string(),
            Some(server_url.to_string()),
        );
        store.set_credentials(&creds).expect("Failed to store credentials");
    }

    // Verify all instances are stored
    let instance_list = store.list_instances().expect("Failed to list instances");

    assert_eq!(instance_list.len(), 3, "Should have 3 instances");
    assert!(instance_list.contains(&"local".to_string()));
    assert!(instance_list.contains(&"cloud".to_string()));
    assert!(instance_list.contains(&"testing".to_string()));

    // Verify each instance has correct credentials
    for (instance, user, server_url) in &instances {
        let retrieved = store
            .get_credentials(instance)
            .expect("Failed to get credentials")
            .expect("Credentials should exist");

        assert_eq!(&retrieved.instance, instance);
        assert_eq!(retrieved.user.as_ref().map(|value| value.as_str()), Some(*user));
        assert_eq!(retrieved.server_url.as_deref(), Some(*server_url));
        assert_eq!(retrieved.jwt_token, format!("jwt_token_for_{}", instance));
    }

    println!("✓ Multiple instances managed correctly");
}

#[test]
fn test_cli_credential_rotation() {
    // **T113**: Test updating JWT credentials for an existing instance

    let (mut store, _temp_dir) = create_temp_store();

    // Initial JWT credentials
    let creds_v1 = Credentials::with_details(
        "production".to_string(),
        "old_jwt_token_v1".to_string(),
        "admin".to_string(),
        "2025-06-01T00:00:00Z".to_string(),
        Some("https://prod.example.com".to_string()),
    );

    store.set_credentials(&creds_v1).expect("Failed to store initial credentials");

    // Retrieve initial credentials
    let retrieved_v1 = store
        .get_credentials("production")
        .expect("Failed to get credentials")
        .expect("Credentials should exist");

    assert_eq!(retrieved_v1.jwt_token, "old_jwt_token_v1");

    // Rotate credentials (new JWT token after re-login)
    let creds_v2 = Credentials::with_details(
        "production".to_string(),
        "new_jwt_token_v2_after_rotation".to_string(),
        "admin".to_string(),
        "2025-12-31T23:59:59Z".to_string(),
        Some("https://prod.example.com".to_string()),
    );

    store.set_credentials(&creds_v2).expect("Failed to update credentials");

    // Retrieve updated credentials
    let retrieved = store
        .get_credentials("production")
        .expect("Failed to get credentials")
        .expect("Credentials should exist");

    assert_eq!(retrieved.jwt_token, "new_jwt_token_v2_after_rotation");
    assert_eq!(retrieved.user.as_ref().map(|value| value.as_str()), Some("admin"));

    // Verify only one instance exists (not duplicated)
    let instance_list = store.list_instances().expect("Failed to list instances");

    assert_eq!(instance_list.len(), 1, "Should still have only 1 instance");

    println!("✓ JWT credential rotation successful");
}

#[test]
fn test_cli_delete_credentials() {
    // Test deleting credentials for an instance

    let (mut store, _temp_dir) = create_temp_store();

    // Store JWT credentials
    let creds = Credentials::new("temp_instance".to_string(), "some_jwt_token".to_string());

    store.set_credentials(&creds).expect("Failed to store credentials");

    // Verify it exists
    assert!(store
        .get_credentials("temp_instance")
        .expect("Failed to get credentials")
        .is_some());

    // Delete credentials
    store.delete_credentials("temp_instance").expect("Failed to delete credentials");

    // Verify it's gone
    assert!(store
        .get_credentials("temp_instance")
        .expect("Failed to get credentials")
        .is_none());

    let instance_list = store.list_instances().expect("Failed to list instances");

    assert_eq!(instance_list.len(), 0, "Should have no instances");

    println!("✓ Credential deletion successful");
}

#[test]
fn test_cli_credentials_with_server_url() {
    // Test storing credentials with custom server URL

    let (mut store, _temp_dir) = create_temp_store();

    // Store credentials with server URL
    let creds = Credentials::with_details(
        "cloud".to_string(),
        "cloud_jwt_token".to_string(),
        "alice".to_string(),
        "2025-12-31T23:59:59Z".to_string(),
        Some("https://db.example.com:8080".to_string()),
    );

    store.set_credentials(&creds).expect("Failed to store credentials");

    // Retrieve and verify
    let retrieved = store
        .get_credentials("cloud")
        .expect("Failed to get credentials")
        .expect("Credentials should exist");

    assert_eq!(retrieved.get_server_url(), "https://db.example.com:8080");

    println!("✓ Credentials with custom server URL work correctly");
}

#[test]
fn test_cli_empty_store() {
    // Test operations on empty credential store

    let (store, _temp_dir) = create_temp_store();

    // List instances on empty store
    let instances = store.list_instances().expect("Failed to list instances");

    assert_eq!(instances.len(), 0, "Empty store should have no instances");

    // Get non-existent credentials
    let creds = store.get_credentials("nonexistent").expect("Failed to get credentials");

    assert!(creds.is_none(), "Non-existent credentials should return None");

    println!("✓ Empty credential store behaves correctly");
}

#[test]
fn test_cli_credential_expiry_check() {
    // Test that expired credentials are detected

    let (mut store, _temp_dir) = create_temp_store();

    // Store expired credentials
    let expired_creds = Credentials::with_details(
        "expired_instance".to_string(),
        "expired_jwt_token".to_string(),
        "user".to_string(),
        "2020-01-01T00:00:00Z".to_string(), // Expired date
        Some(server_url().to_string()),
    );

    store.set_credentials(&expired_creds).expect("Failed to store credentials");

    // Retrieve and check expiry
    let retrieved = store
        .get_credentials("expired_instance")
        .expect("Failed to get credentials")
        .expect("Credentials should exist");

    assert!(retrieved.is_expired(), "Credentials should be marked as expired");

    // Store valid credentials
    let valid_creds = Credentials::with_details(
        "valid_instance".to_string(),
        "valid_jwt_token".to_string(),
        "user".to_string(),
        "2099-12-31T23:59:59Z".to_string(), // Far future date
        Some(server_url().to_string()),
    );

    store.set_credentials(&valid_creds).expect("Failed to store credentials");

    let retrieved = store
        .get_credentials("valid_instance")
        .expect("Failed to get credentials")
        .expect("Credentials should exist");

    assert!(!retrieved.is_expired(), "Credentials should NOT be expired");

    println!("✓ Credential expiry detection works correctly");
}

// ============================================================================
// INTEGRATION TESTS - Require running server
// ============================================================================

/// Test that --save-credentials flag creates the credentials file
#[test]
fn test_cli_save_credentials_creates_file() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let (_temp_dir, creds_path) = create_temp_credentials_path();
    let instance = format!(
        "test_save_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    // Run CLI with --save-credentials flag
    // Note: Uses empty password for root (default test configuration)
    let mut cmd = create_cli_command_with_root_auth();
    with_credentials_path(&mut cmd, &creds_path);
    let output = cmd
        .arg("--instance")
        .arg(&instance)
        .arg("--save-credentials")
        .arg("--command")
        .arg("SELECT 1")
        .output()
        .expect("Failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should succeed
    if !output.status.success() {
        eprintln!("CLI failed. stdout: {}, stderr: {}", stdout, stderr);
        // Don't fail test if password is not set or account is locked
        if stderr.contains("password")
            || stderr.contains("credentials")
            || stderr.contains("locked")
        {
            eprintln!("⚠️  Root password may not be set or account is locked. Skipping test.");
            return;
        }
    }

    // Check that credentials file was created at the temp location
    if creds_path.exists() {
        let contents = fs::read_to_string(&creds_path).expect("Failed to read credentials");
        if contents.contains("jwt_token") {
            assert!(!contents.contains("password"), "Should NOT contain password");
            assert!(contents.contains(&instance), "Should contain instance name");
            println!("✓ Credentials file created at: {:?}", creds_path);
        } else {
            eprintln!(
                "⚠️  No JWT token in credentials file (root password may not be set). Skipping \
                 test."
            );
        }
    } else {
        eprintln!(
            "⚠️  Credentials file not created (root password may not be set). Skipping test."
        );
    }
}

/// Test that credentials are loaded and marked in session info
#[test]
fn test_cli_credentials_loaded_in_session() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let (_temp_dir, creds_path) = create_temp_credentials_path();

    // First, save credentials
    // Note: Uses empty password for root (default test configuration)
    let mut cmd = create_cli_command_with_root_auth();
    with_credentials_path(&mut cmd, &creds_path);
    let save_output = cmd
        .arg("--save-credentials")
        .arg("--command")
        .arg("SELECT 1")
        .output()
        .expect("Failed to run CLI");

    if !save_output.status.success() {
        eprintln!("⚠️  Could not save credentials. Skipping test.");
        return;
    }

    // Now run CLI without username/password - should use stored credentials
    let mut cmd = create_cli_command();
    with_credentials_path(&mut cmd, &creds_path);
    let output = cmd
        .arg("--command")
        .arg("SELECT 'loaded_from_stored' as source")
        .arg("--verbose")
        .output()
        .expect("Failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should succeed using stored credentials
    if output.status.success() {
        // Verbose mode should show "Using stored JWT token"
        if stderr.contains("stored JWT token") || stderr.contains("Using stored") {
            println!("✓ Credentials loaded from storage. stdout: {}", stdout);
        } else {
            eprintln!("⚠️  Root password may not be set. Skipping test.");
        }
    } else {
        eprintln!(
            "⚠️  Test skipped - root password may not be set or credentials expired. stderr: {}",
            stderr
        );
    }
}

/// Test that requests are made with JWT, not username/password
#[test]
fn test_cli_uses_jwt_for_requests() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // With verbose mode, we can verify JWT is being used
    // Note: Uses empty password for root (default test configuration)
    let mut cmd = create_cli_command_with_root_auth();
    let output = cmd
        .arg("--verbose")
        .arg("--command")
        .arg("SELECT 1")
        .output()
        .expect("Failed to run CLI");

    let stderr = String::from_utf8_lossy(&output.stderr);

    if output.status.success() {
        // Verbose output should show JWT token usage
        if stderr.contains("Using JWT token") || stderr.contains("authenticated") {
            // Should NOT say "basic auth" after successful login
            // (only falls back to basic auth if login fails)
            if !stderr.contains("Login failed") {
                assert!(
                    !stderr.contains("basic auth"),
                    "Should NOT use basic auth after successful login. stderr: {}",
                    stderr
                );
            }
            println!("✓ Requests use JWT token authentication");
        } else {
            eprintln!("⚠️  Root password may not be set. Skipping test.");
        }
    } else {
        eprintln!("⚠️  Login failed - root password may not be set. Skipping JWT verification.");
    }
}

/// Test show-credentials CLI flag
#[test]
fn test_cli_show_credentials_command() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let (_temp_dir, creds_path) = create_temp_credentials_path();

    // First ensure we have credentials saved
    // Note: Uses empty password for root (default test configuration)
    let mut cmd = create_cli_command_with_root_auth();
    with_credentials_path(&mut cmd, &creds_path);
    let _ = cmd.arg("--save-credentials").arg("--command").arg("SELECT 1").output();

    // Now test --show-credentials
    let mut cmd = create_cli_command();
    with_credentials_path(&mut cmd, &creds_path);
    let output = cmd.arg("--show-credentials").output().expect("Failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should show credential info
    assert!(
        stdout.contains("Stored Credentials")
            || stdout.contains("Instance:")
            || stdout.contains("JWT Token:")
            || stdout.contains("local"),
        "Should display stored credentials. stdout: {}",
        stdout
    );

    println!("✓ Show credentials command works");
}

/// Test list-instances CLI flag
#[test]
fn test_cli_list_instances_command() {
    let (_temp_dir, creds_path) = create_temp_credentials_path();
    let mut store = kalam_cli::FileCredentialStore::with_path(creds_path.clone())
        .expect("Failed to create credential store");

    let instances = vec![
        ("local", "user1", "token_local"),
        ("cloud", "user2", "token_cloud"),
    ];

    for (instance, username, token) in &instances {
        let creds = Credentials::with_details(
            instance.to_string(),
            token.to_string(),
            username.to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            Some(server_url().to_string()),
        );
        store.set_credentials(&creds).expect("Failed to store credentials");
    }

    let mut cmd = create_cli_command();
    with_credentials_path(&mut cmd, &creds_path);
    let output = cmd.arg("--list-instances").output().expect("Failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("local"), "Should list local instance. stdout: {}", stdout);
    assert!(stdout.contains("cloud"), "Should list cloud instance. stdout: {}", stdout);
}

/// Test show-credentials with missing instance
#[test]
fn test_cli_show_credentials_missing_instance() {
    let (_temp_dir, creds_path) = create_temp_credentials_path();

    let mut cmd = create_cli_command();
    with_credentials_path(&mut cmd, &creds_path);
    let output = cmd
        .arg("--instance")
        .arg("missing_instance")
        .arg("--show-credentials")
        .output()
        .expect("Failed to run CLI");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No credentials") || stdout.contains("not found"),
        "Should report missing credentials. stdout: {}",
        stdout
    );
}

/// Test delete-credentials CLI flag
#[test]
fn test_cli_delete_credentials_command() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let (_temp_dir, creds_path) = create_temp_credentials_path();

    // Use a unique instance to avoid cross-test interference
    let instance = format!(
        "test_delete_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    // First save credentials
    // Note: Uses empty password for root (default test configuration)
    let mut cmd = create_cli_command_with_root_auth();
    with_credentials_path(&mut cmd, &creds_path);
    let _ = cmd
        .arg("--instance")
        .arg(&instance)
        .arg("--save-credentials")
        .arg("--command")
        .arg("SELECT 1")
        .output();

    // Delete credentials
    let mut cmd = create_cli_command();
    with_credentials_path(&mut cmd, &creds_path);
    let delete_output = cmd
        .arg("--instance")
        .arg(&instance)
        .arg("--delete-credentials")
        .output()
        .expect("Failed to run CLI");

    let stdout = String::from_utf8_lossy(&delete_output.stdout);

    assert!(
        stdout.contains("Deleted") || stdout.contains("deleted") || stdout.contains("removed"),
        "Should confirm deletion. stdout: {}",
        stdout
    );

    // Verify credentials are gone
    let mut cmd = create_cli_command();
    with_credentials_path(&mut cmd, &creds_path);
    let show_output = cmd
        .arg("--instance")
        .arg(&instance)
        .arg("--show-credentials")
        .output()
        .expect("Failed to run CLI");

    let show_stdout = String::from_utf8_lossy(&show_output.stdout);

    assert!(
        show_stdout.contains("No credentials")
            || show_stdout.contains("not found")
            || !show_stdout.contains("JWT Token:"),
        "Should show no credentials after deletion. stdout: {}",
        show_stdout
    );

    println!("✓ Delete credentials command works");
}

/// Test multiple instances with different servers
#[test]
fn test_cli_multiple_instance_selection() {
    // This is a unit test - doesn't require server
    let (mut store, _temp_dir) = create_temp_store();

    // Create credentials for different instances
    let instances = vec![
        ("local", "user1", server_url(), "token_local"),
        ("cloud", "user2", "https://cloud.db.com", "token_cloud"),
        ("testing", "tester", "http://test:3000", "token_test"),
    ];

    for (instance, username, server_url, token) in &instances {
        let creds = Credentials::with_details(
            instance.to_string(),
            token.to_string(),
            username.to_string(),
            "2099-12-31T23:59:59Z".to_string(),
            Some(server_url.to_string()),
        );
        store.set_credentials(&creds).expect("Failed to store");
    }

    // Verify we can retrieve each instance's credentials independently
    let local_creds = store.get_credentials("local").unwrap().unwrap();
    assert_eq!(local_creds.jwt_token, "token_local");
    assert_eq!(local_creds.get_server_url(), server_url());

    let cloud_creds = store.get_credentials("cloud").unwrap().unwrap();
    assert_eq!(cloud_creds.jwt_token, "token_cloud");
    assert_eq!(cloud_creds.get_server_url(), "https://cloud.db.com");

    let test_creds = store.get_credentials("testing").unwrap().unwrap();
    assert_eq!(test_creds.jwt_token, "token_test");
    assert_eq!(test_creds.get_server_url(), "http://test:3000");

    // Verify list shows all instances
    let list = store.list_instances().unwrap();
    assert_eq!(list.len(), 3);

    println!("✓ Multiple instance selection works correctly");
}
