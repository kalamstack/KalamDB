//! Tests for seq_id resumption scenarios
//!
//! These tests verify that subscriptions can correctly resume
//! from a specific sequence ID after reconnection.
//!
//! NOTE: These are unit tests that don't require a running server.

use kalam_client::{SeqId, SubscriptionOptions};

/// Test creating subscription options with from_seq_id
#[test]
fn test_subscription_with_from_seq_id() {
    let seq = SeqId::from(12345i64);
    let opts = SubscriptionOptions::new().with_from_seq_id(seq);

    assert_eq!(opts.from, Some(SeqId::from(12345i64)));
    assert!(opts.has_resume_seq_id());
}

/// Test updating from_seq_id via builder pattern
#[test]
fn test_update_from_seq_id() {
    // Initial subscription with batch_size
    let opts = SubscriptionOptions::new().with_batch_size(50).with_last_rows(10);

    assert!(opts.from.is_none());

    // After receiving events, create updated options for reconnect
    let reconnect_opts = opts.clone().with_from_seq_id(SeqId::from(5000i64));

    // Original unchanged
    assert!(opts.from.is_none());

    // New options have from_seq_id
    assert_eq!(reconnect_opts.from, Some(SeqId::from(5000i64)));
    // Other fields preserved
    assert_eq!(reconnect_opts.batch_size, Some(50));
    assert_eq!(reconnect_opts.last_rows, Some(10));
}

/// Test seq_id value extraction
#[test]
fn test_seq_id_value_extraction() {
    let seq = SeqId::from(999999i64);
    let opts = SubscriptionOptions::new().with_from_seq_id(seq);

    // Verify we can get the value back
    if let Some(resume_seq) = opts.from {
        let raw_value: i64 = resume_seq.into();
        assert_eq!(raw_value, 999999i64);
    } else {
        panic!("Expected from_seq_id to be Some");
    }
}

/// Test edge case: seq_id of 0
#[test]
fn test_seq_id_zero() {
    let seq = SeqId::from(0i64);
    let opts = SubscriptionOptions::new().with_from_seq_id(seq);

    // Zero is a valid seq_id (means resume from the very beginning)
    assert!(opts.has_resume_seq_id());
    assert_eq!(opts.from, Some(SeqId::from(0i64)));
}

/// Test edge case: large seq_id values
#[test]
fn test_large_seq_id() {
    let large_seq = SeqId::from(i64::MAX);
    let opts = SubscriptionOptions::new().with_from_seq_id(large_seq);

    assert!(opts.has_resume_seq_id());

    if let Some(seq) = opts.from {
        let raw: i64 = seq.into();
        assert_eq!(raw, i64::MAX);
    }
}

/// Test negative seq_id (if supported)
#[test]
fn test_negative_seq_id() {
    // Negative seq_ids might be used for special cases (e.g., -1 = start from latest)
    let neg_seq = SeqId::from(-1i64);
    let opts = SubscriptionOptions::new().with_from_seq_id(neg_seq);

    assert!(opts.has_resume_seq_id());

    if let Some(seq) = opts.from {
        let raw: i64 = seq.into();
        assert_eq!(raw, -1i64);
    }
}

/// Test subscription options cloning preserves all fields including from_seq_id
#[test]
fn test_clone_preserves_from_seq_id() {
    let opts = SubscriptionOptions::new()
        .with_batch_size(100)
        .with_last_rows(50)
        .with_from_seq_id(SeqId::from(42i64));

    let cloned = opts.clone();

    assert_eq!(cloned.batch_size, Some(100));
    assert_eq!(cloned.last_rows, Some(50));
    assert_eq!(cloned.from, Some(SeqId::from(42i64)));
}

/// Test resumption scenario: simulating receiving multiple batches
#[test]
#[allow(unused_assignments)]
fn test_batch_resumption_scenario() {
    // Initial subscription
    let mut opts = SubscriptionOptions::new().with_batch_size(100);
    let mut last_received_seq: Option<SeqId> = None;

    // Simulate receiving batch 1 (seq 0-99)
    last_received_seq = Some(SeqId::from(99i64));

    // Simulate receiving batch 2 (seq 100-199)
    last_received_seq = Some(SeqId::from(199i64));

    // Simulate receiving batch 3 (seq 200-299)
    last_received_seq = Some(SeqId::from(299i64));

    // Simulate disconnect

    // On reconnect, we resume from 299
    if let Some(seq) = last_received_seq {
        opts = opts.with_from_seq_id(seq);
    }

    assert!(opts.has_resume_seq_id());
    assert_eq!(opts.from, Some(SeqId::from(299i64)));

    // Server should send events starting from seq > 299
}

/// Test combining from_seq_id with other options
#[test]
fn test_from_seq_id_with_other_options() {
    let opts = SubscriptionOptions::new()
        .with_batch_size(200)
        .with_last_rows(10)
        .with_from_seq_id(SeqId::from(500i64));

    // All options should be set
    assert_eq!(opts.batch_size, Some(200));
    assert_eq!(opts.last_rows, Some(10));
    assert_eq!(opts.from, Some(SeqId::from(500i64)));

    // Should be resuming
    assert!(opts.has_resume_seq_id());
}

/// Test that last_rows might be ignored when from_seq_id is set
/// (semantically, you either want "last N rows" OR "resume from seq")
#[test]
fn test_last_rows_vs_from_seq_id() {
    // When from_seq_id is set, last_rows is typically ignored by the server
    // This test documents that behavior expectation

    let opts_last_rows = SubscriptionOptions::new().with_last_rows(100);
    let opts_from_seq = SubscriptionOptions::new().with_from_seq_id(SeqId::from(50i64));
    let opts_both = SubscriptionOptions::new()
        .with_last_rows(100)
        .with_from_seq_id(SeqId::from(50i64));

    // Both can be set, but semantically from_seq_id takes precedence
    assert_eq!(opts_last_rows.last_rows, Some(100));
    assert!(!opts_last_rows.has_resume_seq_id());

    assert!(opts_from_seq.has_resume_seq_id());
    assert!(opts_from_seq.last_rows.is_none());

    // When both set, from_seq_id indicates resumption
    assert!(opts_both.has_resume_seq_id());
    // But last_rows is also set (server decides which to use)
    assert_eq!(opts_both.last_rows, Some(100));
}
