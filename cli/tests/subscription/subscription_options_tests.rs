//! Tests for SubscriptionOptions - subscription-level settings
//!
//! These tests verify the behavior of SubscriptionOptions including:
//! - Default values
//! - Builder pattern for batch_size, last_rows, from_seq_id
//! - Resume from seq_id for reconnection scenarios
//! - Serialization aligned with backend

use kalam_link::{SeqId, SubscriptionOptions};

/// Test that default SubscriptionOptions have all fields as None
#[test]
fn test_subscription_options_defaults() {
    let opts = SubscriptionOptions::default();

    assert!(opts.batch_size.is_none(), "batch_size should default to None");
    assert!(opts.last_rows.is_none(), "last_rows should default to None");
    assert!(opts.from.is_none(), "from should default to None");
}

/// Test the builder pattern for SubscriptionOptions
#[test]
fn test_subscription_options_builder() {
    let seq_id = SeqId::from(12345i64);

    let opts = SubscriptionOptions::new()
        .with_batch_size(100)
        .with_last_rows(50)
        .with_from_seq_id(seq_id);

    assert_eq!(opts.batch_size, Some(100));
    assert_eq!(opts.last_rows, Some(50));
    assert_eq!(opts.from, Some(seq_id));
}

/// Test setting only batch_size
#[test]
fn test_subscription_options_batch_size_only() {
    let opts = SubscriptionOptions::new().with_batch_size(500);

    assert_eq!(opts.batch_size, Some(500));
    assert!(opts.last_rows.is_none());
    assert!(opts.from.is_none());
}

/// Test setting only last_rows
#[test]
fn test_subscription_options_last_rows_only() {
    let opts = SubscriptionOptions::new().with_last_rows(25);

    assert!(opts.batch_size.is_none());
    assert_eq!(opts.last_rows, Some(25));
    assert!(opts.from.is_none());
}

/// Test setting only from_seq_id (for resume after reconnection)
#[test]
fn test_subscription_options_from_seq_id_only() {
    let seq_id = SeqId::from(99999i64);

    let opts = SubscriptionOptions::new().with_from_seq_id(seq_id);

    assert!(opts.batch_size.is_none());
    assert!(opts.last_rows.is_none());
    assert_eq!(opts.from, Some(seq_id));
}

/// Test the has_resume_seq_id helper method
#[test]
fn test_subscription_options_has_resume_seq_id() {
    let opts_without = SubscriptionOptions::new();
    assert!(!opts_without.has_resume_seq_id(), "Should not have resume seq_id");

    let opts_with = SubscriptionOptions::new().with_from_seq_id(SeqId::from(123i64));
    assert!(opts_with.has_resume_seq_id(), "Should have resume seq_id");
}

/// Test that JSON serialization skips None fields
#[test]
fn test_subscription_options_json_serialization_sparse() {
    let opts = SubscriptionOptions::new().with_batch_size(200);

    let json = serde_json::to_string(&opts).expect("serialization failed");

    // Should contain batch_size
    assert!(json.contains("\"batch_size\":200"), "JSON should contain batch_size");

    // Should NOT contain last_rows or from (they're None)
    assert!(!json.contains("last_rows"), "JSON should not contain last_rows");
    assert!(!json.contains("\"from\""), "JSON should not contain from");
}

/// Test that JSON serialization includes from_seq_id when set
#[test]
fn test_subscription_options_json_with_seq_id() {
    let seq_id = SeqId::from(42i64);

    let opts = SubscriptionOptions::new().with_batch_size(50).with_from_seq_id(seq_id);

    let json = serde_json::to_string(&opts).expect("serialization failed");

    assert!(json.contains("batch_size"), "JSON should contain batch_size");
    assert!(json.contains("\"from\""), "JSON should contain from");
}

/// Test JSON deserialization with all fields
#[test]
fn test_subscription_options_json_deserialization_full() {
    let json = r#"{"batch_size": 100, "last_rows": 50, "from_seq_id": 12345}"#;

    let opts: SubscriptionOptions = serde_json::from_str(json).expect("deserialization failed");

    assert_eq!(opts.batch_size, Some(100));
    assert_eq!(opts.last_rows, Some(50));
    assert_eq!(opts.from, Some(SeqId::from(12345i64)));
}

/// Test JSON deserialization with minimal fields
#[test]
fn test_subscription_options_json_deserialization_empty() {
    let json = r#"{}"#;

    let opts: SubscriptionOptions = serde_json::from_str(json).expect("deserialization failed");

    assert!(opts.batch_size.is_none());
    assert!(opts.last_rows.is_none());
    assert!(opts.from.is_none());
}

/// Test reconnection workflow: initial subscription -> receive data -> reconnect with seq_id
#[test]
fn test_subscription_options_reconnection_workflow() {
    // Step 1: Initial subscription with just batch_size
    let initial_opts = SubscriptionOptions::new().with_batch_size(100);

    assert!(
        !initial_opts.has_resume_seq_id(),
        "Initial subscription should not have resume seq_id"
    );

    // Step 2: After receiving data, we track the last seq_id (simulated)
    let last_received_seq = SeqId::from(54321i64);

    // Step 3: On reconnection, create options with from_seq_id to resume
    let reconnect_opts = SubscriptionOptions::new()
        .with_batch_size(initial_opts.batch_size.unwrap_or(100))
        .with_from_seq_id(last_received_seq);

    assert!(
        reconnect_opts.has_resume_seq_id(),
        "Reconnect subscription should have resume seq_id"
    );
    assert_eq!(reconnect_opts.from, Some(last_received_seq));
    assert_eq!(reconnect_opts.batch_size, Some(100), "Should preserve batch_size from original");
}

/// Test that SubscriptionOptions is independent from ConnectionOptions
#[test]
fn test_subscription_options_no_connection_fields() {
    let opts = SubscriptionOptions::new().with_batch_size(100).with_last_rows(50);

    let json = serde_json::to_string(&opts).expect("serialization failed");

    // SubscriptionOptions should NOT have any connection-related fields
    assert!(!json.contains("auto_reconnect"), "Should not have auto_reconnect");
    assert!(!json.contains("reconnect_delay"), "Should not have reconnect_delay");
    assert!(!json.contains("max_reconnect"), "Should not have max_reconnect fields");
}

/// Test typical use case: fetch last 100 rows with batch size of 50
#[test]
fn test_subscription_options_typical_use_case() {
    let opts = SubscriptionOptions::new()
        .with_batch_size(50)    // Server will send 50 rows per batch
        .with_last_rows(100); // Only fetch the last 100 rows

    assert_eq!(opts.batch_size, Some(50));
    assert_eq!(opts.last_rows, Some(100));
    assert!(opts.from.is_none(), "New subscription should not have from");
}

/// Test SeqId comparison for resume scenarios
#[test]
fn test_seq_id_comparison() {
    let seq_a = SeqId::from(1000i64);
    let seq_b = SeqId::from(2000i64);
    let seq_c = SeqId::from(1000i64);

    // Equal comparison
    assert_eq!(seq_a, seq_c, "Same seq_id values should be equal");
    assert_ne!(seq_a, seq_b, "Different seq_id values should not be equal");

    // Ordering for MVCC
    assert!(seq_a < seq_b, "seq_a should be less than seq_b");
    assert!(seq_b > seq_a, "seq_b should be greater than seq_a");
}

/// Test SeqId string conversion
#[test]
fn test_seq_id_to_string() {
    let seq = SeqId::from(12345i64);
    let str_rep = seq.to_string();

    assert_eq!(str_rep, "12345", "SeqId should convert to string correctly");
}
