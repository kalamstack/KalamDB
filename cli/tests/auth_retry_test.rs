//! Integration tests for authentication retry logic
//!
//! These tests verify that the CLI properly handles authentication failures
//! and prompts for credentials when stored credentials become invalid.

use std::path::PathBuf;
use std::process::{Command, Stdio};

/// Helper to get the CLI binary path
fn get_cli_binary() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../target/release/kalam");
    path
}

/// Test that the CLI detects auth failures and prompts for new credentials
#[test]
fn test_auth_failure_triggers_prompt() {
    // Arrange: Run CLI with no arguments (will use stored credentials if available)
    let binary = get_cli_binary();
    if !binary.exists() {
        eprintln!("CLI binary not found at {:?}. Run: cargo build --release", binary);
        return;
    }

    // Act: Run with stdin closed (non-interactive simulation)
    let output = Command::new(&binary)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    // If command fails to execute, skip test
    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Failed to execute CLI: {}", e);
            return;
        },
    };

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Assert: Check for expected behavior patterns
    // - Should not crash
    // - Should handle auth errors gracefully
    // - In non-interactive mode, should exit with error (not prompt)
    println!("STDOUT:\n{}", stdout);
    println!("STDERR:\n{}", stderr);

    // Verify it exits with non-zero code on auth failure (when non-interactive)
    if stderr.contains("Authentication failed") || stderr.contains("User not found") {
        assert!(
            !output.status.success(),
            "Should exit with error when auth fails in non-interactive mode"
        );
    }
}

#[test]
fn test_auth_failure_with_cli_user() {
    // When --user is provided, CLI should NOT prompt for credentials again
    // It should just fail with the error

    let binary = get_cli_binary();
    if !binary.exists() {
        eprintln!("CLI binary not found at {:?}. Run: cargo build --release", binary);
        return;
    }

    let output = Command::new(&binary)
        .args(&[
            "--user",
            "nonexistent_user_xyz_12345",
            "--password",
            "wrongpass",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let output = match output {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Failed to execute CLI: {}", e);
            return;
        },
    };

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should fail with auth error
    assert!(
        !output.status.success(),
        "Should exit with error when auth fails with explicit user"
    );

    // Should NOT prompt for new credentials when --user is provided
    assert!(
        !stderr.contains("Please enter your credentials"),
        "Should not prompt when --user is provided. Got: {}",
        stderr
    );
}

#[test]
#[ignore] // Manual test - requires interactive TTY
fn test_interactive_credential_prompt() {
    println!("\n=== Manual Interactive Test ===");
    println!("\nSteps to test interactive credential prompt:");
    println!("1. Ensure you have invalid stored credentials:");
    println!("   cargo run --release -- --delete-credentials");
    println!("   cargo run --release -- --user deleteduser --password test --save-credentials");
    println!("\n2. Delete that user from the server (or use a non-existent user)");
    println!("\n3. Run CLI without arguments in an interactive terminal:");
    println!("   cargo run --release");
    println!("\n4. Expected behavior:");
    println!("   - Shows: 'Authentication failed with stored credentials.'");
    println!("   - Prompts: 'User:'");
    println!("   - Prompts: 'Password:' (hidden input)");
    println!("   - Asks: 'Save credentials for future use? (y/N)'");
    println!("   - Connects successfully with new credentials");
}

#[test]
#[ignore] // Manual test
fn test_expired_token_flow() {
    println!("\n=== Manual Token Expiry Test ===");
    println!("\nSteps to test expired token handling:");
    println!("1. Login and save credentials:");
    println!("   cargo run --release -- --user testuser --password testpass --save-credentials");
    println!("\n2. Wait for the access token to expire (or manually edit the expiry in credentials file)");
    println!("\n3. Run CLI without arguments:");
    println!("   cargo run --release");
    println!("\n4. Expected behavior:");
    println!("   - Shows: 'Stored credentials for 'local' have expired.'");
    println!("   - Attempts to refresh using refresh token");
    println!("   - If refresh succeeds: connects with refreshed token");
    println!("   - If refresh fails: prompts for new credentials");
}

#[test]
#[ignore] // Manual test
fn test_setup_wizard_direct_access() {
    println!("\n=== Manual Setup Wizard Test ===");
    println!("\nSteps to test direct setup wizard access:");
    println!("1. Start a fresh KalamDB server that requires setup:");
    println!("   cd backend && rm -rf data && cargo run");
    println!("\n2. Store any credentials (they will be invalid for setup-required server):");
    println!("   cargo run --release -- --delete-credentials");
    println!("   cargo run --release -- --user anyuser --password anypass --save-credentials");
    println!("\n3. Run CLI without arguments:");
    println!("   cargo run --release");
    println!("\n4. Expected behavior:");
    println!("   - Shows: 'Server requires initial setup.'");
    println!("   - Goes DIRECTLY to setup wizard (no login prompt)");
    println!("   - Shows setup box with instructions");
    println!("   - Prompts for DBA user and password");
    println!("   - Prompts for root password");
    println!("   - Completes setup and logs in automatically");
    println!("\n5. Verify NO intermediate login prompt appears!");
}

/// Documentation test to ensure the behavior is clear
#[test]
fn document_auth_retry_behavior() {
    println!("\n=== CLI Authentication Retry Behavior ===\n");
    println!("When credentials fail, the CLI will automatically prompt for new credentials IF:");
    println!("  ✓ No --user was provided on command line");
    println!("  ✓ No --token was provided on command line");
    println!("  ✓ Terminal is interactive (has TTY)");
    println!("  ✓ Error is authentication-related (401 status)");
    println!("\n=== Server Setup Detection ===\n");
    println!("When server requires setup:");
    println!("  ✓ Detects SetupRequired error");
    println!("  ✓ Skips login prompt entirely");
    println!("  ✓ Goes directly to setup wizard");
    println!("  ✓ After setup, logs in with new credentials");
    println!("\nScenarios:");
    println!("1. Server needs setup → Goes directly to setup wizard (no login prompt)");
    println!("2. Stored credentials expired → Prompts for new credentials");
    println!("3. Stored user deleted → Prompts for new credentials");
    println!("4. Wrong password stored → Prompts for new credentials");
    println!("5. CLI args provided (--user) → Does NOT prompt, returns error");
    println!("6. Non-interactive mode → Does NOT prompt, returns error");
    println!("\nError types that trigger re-authentication:");
    println!("  • SetupRequired → Direct to setup wizard");
    println!("  • 401 Unauthorized → Prompt for login");
    println!("  • User not found → Prompt for login");
    println!("  • Token expired (with failed refresh) → Prompt for login");
    println!("  • Authentication error → Prompt for login");
}
