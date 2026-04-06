//! Tests for ConnectionOptions - client-level connection settings
//!
//! These tests verify the behavior of ConnectionOptions including:
//! - Default values
//! - Builder pattern
//! - HTTP version selection
//! - Serialization/deserialization
//! - Exponential backoff calculations

use kalam_client::{ConnectionOptions, HttpVersion};

/// Test that default ConnectionOptions have sensible values
#[test]
fn test_connection_options_defaults() {
    let opts = ConnectionOptions::default();

    // HTTP/1.1 should be the default for maximum compatibility
    assert_eq!(opts.http_version, HttpVersion::Http1, "http_version should default to Http1");

    // Auto-reconnect should be enabled by default
    assert!(opts.auto_reconnect, "auto_reconnect should default to true");

    // Initial delay should be 1 second
    assert_eq!(opts.reconnect_delay_ms, 1000, "reconnect_delay_ms should be 1000ms");

    // Max delay should be 30 seconds
    assert_eq!(opts.max_reconnect_delay_ms, 30000, "max_reconnect_delay_ms should be 30000ms");

    // Infinite retries by default
    assert!(
        opts.max_reconnect_attempts.is_none(),
        "max_reconnect_attempts should be None (infinite)"
    );
}

/// Test the builder pattern for ConnectionOptions
#[test]
fn test_connection_options_builder() {
    let opts = ConnectionOptions::new()
        .with_http_version(HttpVersion::Http2)
        .with_auto_reconnect(false)
        .with_reconnect_delay_ms(2000)
        .with_max_reconnect_delay_ms(60000)
        .with_max_reconnect_attempts(Some(5));

    assert_eq!(opts.http_version, HttpVersion::Http2);
    assert!(!opts.auto_reconnect);
    assert_eq!(opts.reconnect_delay_ms, 2000);
    assert_eq!(opts.max_reconnect_delay_ms, 60000);
    assert_eq!(opts.max_reconnect_attempts, Some(5));
}

/// Test HTTP version selection
#[test]
fn test_connection_options_http_version() {
    // HTTP/1.1 (default)
    let opts1 = ConnectionOptions::new().with_http_version(HttpVersion::Http1);
    assert_eq!(opts1.http_version, HttpVersion::Http1);

    // HTTP/2
    let opts2 = ConnectionOptions::new().with_http_version(HttpVersion::Http2);
    assert_eq!(opts2.http_version, HttpVersion::Http2);

    // Auto (let client negotiate)
    let opts3 = ConnectionOptions::new().with_http_version(HttpVersion::Auto);
    assert_eq!(opts3.http_version, HttpVersion::Auto);
}

/// Test disabling reconnection via max_attempts = 0
#[test]
fn test_connection_options_disable_reconnect() {
    let opts = ConnectionOptions::new().with_max_reconnect_attempts(Some(0));

    // With max_attempts = 0, reconnection is effectively disabled
    assert_eq!(opts.max_reconnect_attempts, Some(0));
}

/// Test that JSON serialization preserves all fields including http_version
#[test]
fn test_connection_options_json_serialization() {
    let opts = ConnectionOptions::new()
        .with_http_version(HttpVersion::Http2)
        .with_auto_reconnect(true)
        .with_reconnect_delay_ms(1500)
        .with_max_reconnect_delay_ms(45000)
        .with_max_reconnect_attempts(Some(10));

    let json = serde_json::to_string(&opts).expect("serialization failed");
    println!("Serialized JSON: {}", json);

    let parsed: ConnectionOptions = serde_json::from_str(&json).expect("deserialization failed");

    assert_eq!(parsed.http_version, opts.http_version);
    assert_eq!(parsed.auto_reconnect, opts.auto_reconnect);
    assert_eq!(parsed.reconnect_delay_ms, opts.reconnect_delay_ms);
    assert_eq!(parsed.max_reconnect_delay_ms, opts.max_reconnect_delay_ms);
    assert_eq!(parsed.max_reconnect_attempts, opts.max_reconnect_attempts);
}

/// Test that missing fields get defaults during deserialization
#[test]
fn test_connection_options_partial_json() {
    // Only provide auto_reconnect, other fields should use defaults
    let json = r#"{"auto_reconnect": false}"#;
    let opts: ConnectionOptions = serde_json::from_str(json).expect("deserialization failed");

    assert_eq!(opts.http_version, HttpVersion::Http1); // default
    assert!(!opts.auto_reconnect);
    assert_eq!(opts.reconnect_delay_ms, 1000); // default
    assert_eq!(opts.max_reconnect_delay_ms, 30000); // default
    assert!(opts.max_reconnect_attempts.is_none()); // default
}

/// Test HTTP version JSON serialization formats
#[test]
fn test_http_version_json_serialization() {
    // Http1 serializes as "http1"
    let json1 = serde_json::to_string(&HttpVersion::Http1).unwrap();
    assert_eq!(json1, r#""http1""#);

    // Http2 serializes as "http2"
    let json2 = serde_json::to_string(&HttpVersion::Http2).unwrap();
    assert_eq!(json2, r#""http2""#);

    // Auto serializes as "auto"
    let json3 = serde_json::to_string(&HttpVersion::Auto).unwrap();
    assert_eq!(json3, r#""auto""#);
}

/// Test HTTP version JSON deserialization with aliases
#[test]
fn test_http_version_json_deserialization() {
    // Standard formats
    assert_eq!(serde_json::from_str::<HttpVersion>(r#""http1""#).unwrap(), HttpVersion::Http1);
    assert_eq!(serde_json::from_str::<HttpVersion>(r#""http2""#).unwrap(), HttpVersion::Http2);
    assert_eq!(serde_json::from_str::<HttpVersion>(r#""auto""#).unwrap(), HttpVersion::Auto);

    // Aliases
    assert_eq!(
        serde_json::from_str::<HttpVersion>(r#""http/1.1""#).unwrap(),
        HttpVersion::Http1
    );
    assert_eq!(serde_json::from_str::<HttpVersion>(r#""1.1""#).unwrap(), HttpVersion::Http1);
    assert_eq!(serde_json::from_str::<HttpVersion>(r#""http/2""#).unwrap(), HttpVersion::Http2);
    assert_eq!(serde_json::from_str::<HttpVersion>(r#""2""#).unwrap(), HttpVersion::Http2);
}

/// Test exponential backoff delay calculation
#[test]
fn test_exponential_backoff_calculation() {
    let opts = ConnectionOptions::new()
        .with_reconnect_delay_ms(1000)
        .with_max_reconnect_delay_ms(30000);

    // Simulate exponential backoff calculation (as done in WASM client)
    let base_delay = opts.reconnect_delay_ms;
    let max_delay = opts.max_reconnect_delay_ms;

    // Attempt 0: 1000ms
    let delay_0 = std::cmp::min(base_delay * 2u64.pow(0), max_delay);
    assert_eq!(delay_0, 1000, "First attempt should be 1000ms");

    // Attempt 1: 2000ms
    let delay_1 = std::cmp::min(base_delay * 2u64.pow(1), max_delay);
    assert_eq!(delay_1, 2000, "Second attempt should be 2000ms");

    // Attempt 2: 4000ms
    let delay_2 = std::cmp::min(base_delay * 2u64.pow(2), max_delay);
    assert_eq!(delay_2, 4000, "Third attempt should be 4000ms");

    // Attempt 3: 8000ms
    let delay_3 = std::cmp::min(base_delay * 2u64.pow(3), max_delay);
    assert_eq!(delay_3, 8000, "Fourth attempt should be 8000ms");

    // Attempt 4: 16000ms
    let delay_4 = std::cmp::min(base_delay * 2u64.pow(4), max_delay);
    assert_eq!(delay_4, 16000, "Fifth attempt should be 16000ms");

    // Attempt 5: 32000ms -> capped at 30000ms
    let delay_5 = std::cmp::min(base_delay * 2u64.pow(5), max_delay);
    assert_eq!(delay_5, 30000, "Sixth attempt should be capped at 30000ms");

    // Attempt 10: still capped at 30000ms
    let delay_10 = std::cmp::min(base_delay * 2u64.pow(10), max_delay);
    assert_eq!(delay_10, 30000, "Later attempts should stay at max delay");
}

/// Test that fast reconnect preset has reasonable values
#[test]
fn test_fast_reconnect_preset() {
    // For local development, you might want faster reconnection
    let opts = ConnectionOptions::new()
        .with_reconnect_delay_ms(100)    // Start at 100ms
        .with_max_reconnect_delay_ms(5000) // Cap at 5 seconds
        .with_max_reconnect_attempts(Some(5)); // Give up after 5 attempts

    assert_eq!(opts.reconnect_delay_ms, 100);
    assert_eq!(opts.max_reconnect_delay_ms, 5000);
    assert_eq!(opts.max_reconnect_attempts, Some(5));
}

/// Test that relaxed reconnect preset has longer delays
#[test]
fn test_relaxed_reconnect_preset() {
    // For high-latency networks or unreliable connections
    let opts = ConnectionOptions::new()
        .with_reconnect_delay_ms(5000)      // Start at 5 seconds
        .with_max_reconnect_delay_ms(120000) // Cap at 2 minutes
        .with_max_reconnect_attempts(None); // Never give up

    assert_eq!(opts.reconnect_delay_ms, 5000);
    assert_eq!(opts.max_reconnect_delay_ms, 120000);
    assert!(opts.max_reconnect_attempts.is_none());
}

/// Test HTTP/2 preset for high-performance scenarios
#[test]
fn test_http2_performance_preset() {
    // For high-performance scenarios with many concurrent requests
    let opts = ConnectionOptions::new()
        .with_http_version(HttpVersion::Http2)
        .with_reconnect_delay_ms(500)        // Fast reconnect
        .with_max_reconnect_delay_ms(10000); // Cap at 10 seconds

    assert_eq!(opts.http_version, HttpVersion::Http2);
    assert_eq!(opts.reconnect_delay_ms, 500);
    assert_eq!(opts.max_reconnect_delay_ms, 10000);
}
