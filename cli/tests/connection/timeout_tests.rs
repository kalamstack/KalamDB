//! Integration tests for WebSocket connection timeout behavior
//!
//! These tests verify timeout handling for WebSocket connections:
//! - Connection timeout when server is unreachable
//! - Timeouts are properly applied from KalamLinkTimeouts
//!
//! NOTE: These are integration tests that test actual network behavior.
//! They do NOT require a running server (they test timeout behavior).

use std::time::{Duration, Instant};

use crate::common::{client_for_url_no_auth, KalamLinkTimeouts};

/// Test that connection timeout is properly enforced when server is unreachable
///
/// This test connects to a non-existent server and verifies that:
/// 1. The connection fails (as expected)
/// 2. The failure happens within the configured timeout window
#[tokio::test]
async fn test_connection_timeout_unreachable_server() {
    // Use a non-routable IP address to ensure connection times out
    // 10.255.255.1 is a non-routable IP that will cause a timeout
    let unreachable_url = "http://10.255.255.1:9999";

    // Configure a short timeout for the test
    let timeouts = KalamLinkTimeouts::builder()
        .connection_timeout_secs(2) // 2 second connection timeout
        .build();

    let client =
        client_for_url_no_auth(unreachable_url, timeouts).expect("Client build should succeed");

    let start = Instant::now();

    // Attempt to execute a query (which requires connection)
    let result = client.execute_query("SELECT 1", None, None, None).await;

    let elapsed = start.elapsed();

    // Verify the connection failed
    assert!(result.is_err(), "Should fail to connect to unreachable server");

    // Verify it failed within a reasonable time window
    // Allow some buffer for OS-level timeout handling
    assert!(
        elapsed < Duration::from_secs(10),
        "Timeout should trigger within 10 seconds, but took {:?}",
        elapsed
    );
}

/// Test that fast timeout preset is properly applied
#[tokio::test]
async fn test_fast_timeout_preset() {
    let unreachable_url = "http://10.255.255.1:9999";

    // Use the fast preset (optimized for local development)
    let timeouts = KalamLinkTimeouts::fast();

    let client =
        client_for_url_no_auth(unreachable_url, timeouts).expect("Client build should succeed");

    let start = Instant::now();
    let result = client.execute_query("SELECT 1", None, None, None).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Should fail to connect");

    // Fast preset should timeout within reasonable time
    // Allow extra buffer for OS-level timeout variations
    assert!(
        elapsed < Duration::from_secs(15),
        "Fast timeout preset should trigger reasonably quickly, but took {:?}",
        elapsed
    );
}

/// Test that connection to localhost on wrong port fails gracefully
#[tokio::test]
async fn test_connection_refused() {
    // Port 59999 is unlikely to have a server running
    let wrong_port_url = "http://localhost:59999";

    let client = client_for_url_no_auth(wrong_port_url, KalamLinkTimeouts::fast())
        .expect("Client build should succeed");

    let start = Instant::now();
    let result = client.execute_query("SELECT 1", None, None, None).await;
    let elapsed = start.elapsed();

    // Should fail with connection refused (very fast)
    assert!(result.is_err(), "Should fail to connect");

    // Connection refused should be reasonably fast
    // Note: On Windows, connection refused can take longer due to retry behavior
    assert!(
        elapsed < Duration::from_secs(15),
        "Connection refused should be fast, but took {:?}",
        elapsed
    );
}

/// Test that timeouts are cloned correctly
#[test]
fn test_timeouts_clone() {
    let timeouts = KalamLinkTimeouts::builder()
        .connection_timeout_secs(5)
        .receive_timeout_secs(30)
        .auth_timeout_secs(10)
        .build();

    let cloned = timeouts.clone();

    assert_eq!(cloned.connection_timeout, timeouts.connection_timeout);
    assert_eq!(cloned.receive_timeout, timeouts.receive_timeout);
    assert_eq!(cloned.auth_timeout, timeouts.auth_timeout);
}
