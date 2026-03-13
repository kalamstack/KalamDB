//! Watermark Integration Tests
//!
//! Tests for Phase 3 & 4 of spec 018:
//! - Watermark buffering on followers
//! - Pending buffer persistence via snapshot/restore
//! - Meta→Data ordering during rejoin
//!
//! NOTE: Tests that use the global MetadataCoordinator singleton are marked
//! with `#[serial]` to avoid race conditions.

use kalamdb_commons::models::rows::Row;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use kalamdb_commons::models::{NamespaceId, UserId};
use kalamdb_commons::TableId;
use kalamdb_raft::codec::command_codec::{encode_shared_data_command, encode_user_data_command};
use kalamdb_raft::commands::{SharedDataCommand, UserDataCommand};
use kalamdb_raft::state_machine::{
    get_coordinator, init_coordinator, MetadataCoordinator, PendingBuffer, PendingCommand,
    SharedDataStateMachine, UserDataStateMachine,
};
use kalamdb_raft::KalamStateMachine;
use serial_test::serial;

// =============================================================================
// PendingBuffer Unit Tests
// =============================================================================

#[test]
fn test_pending_buffer_add_and_drain() {
    let buffer = PendingBuffer::new();

    // Add commands with different required_meta_index values
    buffer.add(PendingCommand {
        log_index: 10,
        log_term: 1,
        required_meta_index: 5,
        command_bytes: vec![1, 2, 3],
    });

    buffer.add(PendingCommand {
        log_index: 11,
        log_term: 1,
        required_meta_index: 7,
        command_bytes: vec![4, 5, 6],
    });

    buffer.add(PendingCommand {
        log_index: 12,
        log_term: 1,
        required_meta_index: 5,
        command_bytes: vec![7, 8, 9],
    });

    assert_eq!(buffer.len(), 3);

    // Drain up to meta index 5 - should get 2 commands (both with required_meta_index=5)
    let drained = buffer.drain_satisfied(5);
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].log_index, 10); // Sorted by log_index
    assert_eq!(drained[1].log_index, 12);

    // Only one command left (required_meta_index=7)
    assert_eq!(buffer.len(), 1);

    // Drain up to meta index 6 - nothing new
    let drained = buffer.drain_satisfied(6);
    assert!(drained.is_empty());

    // Drain up to meta index 7 - should get the last command
    let drained = buffer.drain_satisfied(7);
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].log_index, 11);

    assert!(buffer.is_empty());
}

#[test]
fn test_pending_buffer_log_order_preserved() {
    let buffer = PendingBuffer::new();

    // Add commands out of log order, same required_meta_index
    buffer.add(PendingCommand {
        log_index: 15,
        log_term: 1,
        required_meta_index: 10,
        command_bytes: vec![1],
    });

    buffer.add(PendingCommand {
        log_index: 13,
        log_term: 1,
        required_meta_index: 10,
        command_bytes: vec![2],
    });

    buffer.add(PendingCommand {
        log_index: 14,
        log_term: 1,
        required_meta_index: 10,
        command_bytes: vec![3],
    });

    let drained = buffer.drain_satisfied(10);
    assert_eq!(drained.len(), 3);
    // Must be sorted by log_index for Raft ordering
    assert_eq!(drained[0].log_index, 13);
    assert_eq!(drained[1].log_index, 14);
    assert_eq!(drained[2].log_index, 15);
}

#[test]
fn test_pending_buffer_persistence_roundtrip() {
    let buffer = PendingBuffer::new();

    buffer.add(PendingCommand {
        log_index: 100,
        log_term: 5,
        required_meta_index: 50,
        command_bytes: vec![1, 2, 3, 4, 5],
    });

    buffer.add(PendingCommand {
        log_index: 101,
        log_term: 5,
        required_meta_index: 55,
        command_bytes: vec![6, 7, 8, 9, 10],
    });

    // Get all for persistence
    let commands = buffer.get_all();
    assert_eq!(commands.len(), 2);

    // Create a new buffer and load
    let buffer2 = PendingBuffer::new();
    buffer2.load_from(commands);

    assert_eq!(buffer2.len(), 2);

    // Verify we can drain correctly after restore
    let drained = buffer2.drain_satisfied(50);
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].log_index, 100);

    let drained = buffer2.drain_satisfied(55);
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].log_index, 101);
}

// =============================================================================
// MetadataCoordinator Tests
// =============================================================================

#[tokio::test]
async fn test_metadata_coordinator_isolated() {
    // Use an isolated coordinator (not the global one)
    let coordinator = MetadataCoordinator::new();

    assert_eq!(coordinator.current_index(), 0);

    // Advance to 5
    coordinator.advance(5);
    assert_eq!(coordinator.current_index(), 5);

    // is_satisfied should work correctly
    assert!(coordinator.is_satisfied(3));
    assert!(coordinator.is_satisfied(5));
    assert!(!coordinator.is_satisfied(6));

    // Advance should not go backwards
    coordinator.advance(3);
    assert_eq!(coordinator.current_index(), 5); // Still 5
}

#[tokio::test]
async fn test_metadata_coordinator_wait_for_notification() {
    // Use a fresh coordinator for this test
    let coordinator = MetadataCoordinator::new();
    let coordinator = Arc::new(coordinator);
    let coord_clone = coordinator.clone();

    // Spawn a task that waits for index 10
    let wait_handle = tokio::spawn(async move {
        coord_clone.wait_for(10).await;
        true
    });

    // Give the waiter time to start
    sleep(Duration::from_millis(10)).await;

    // Advance to 5 - waiter should still be waiting
    coordinator.advance(5);
    sleep(Duration::from_millis(10)).await;
    assert!(!wait_handle.is_finished());

    // Advance to 10 - waiter should complete
    coordinator.advance(10);

    // Wait for completion with timeout
    let result = tokio::time::timeout(Duration::from_millis(100), wait_handle).await;
    assert!(result.is_ok());
    assert!(result.unwrap().unwrap());
}

// =============================================================================
// UserDataStateMachine Watermark Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_user_data_buffering_when_meta_behind() {
    // Initialize coordinator at index 0
    let _ = init_coordinator(0);
    // Reset coordinator to 0 for this test
    // Note: global coordinator is shared across tests, so we work with current state

    let sm = UserDataStateMachine::new(0);

    // Get current meta index
    let current_meta = get_coordinator().current_index();

    // Create a command with required_meta_index higher than current
    let cmd = UserDataCommand::Insert {
        table_id: TableId::new(NamespaceId::default(), "users".into()),
        user_id: UserId::new("user1"),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: current_meta + 100, // Well above current, so must buffer
    };

    let cmd_bytes = encode_user_data_command(&cmd).unwrap();

    // Apply command - should be buffered since meta is behind
    let _result = sm.apply(1, 1, &cmd_bytes).await.unwrap();

    // Command was buffered
    assert_eq!(sm.pending_count(), 1);

    // Advance meta to required level
    get_coordinator().advance(current_meta + 100);

    // Apply another command (with lower requirement) which will trigger drain
    let cmd2 = UserDataCommand::Insert {
        table_id: TableId::new(NamespaceId::default(), "users".into()),
        user_id: UserId::new("user2"),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: 0, // Can apply immediately, also triggers drain
    };

    let cmd2_bytes = encode_user_data_command(&cmd2).unwrap();
    let _result = sm.apply(2, 1, &cmd2_bytes).await.unwrap();

    // After drain, pending should be 0
    assert_eq!(sm.pending_count(), 0);
}

#[tokio::test]
#[serial]
async fn test_user_data_immediate_apply_when_meta_caught_up() {
    // Initialize coordinator at a high index
    let _ = init_coordinator(1000);
    get_coordinator().advance(1000);

    let sm = UserDataStateMachine::new(1);

    // Create a command with required_meta_index lower than current
    let cmd = UserDataCommand::Insert {
        table_id: TableId::new(NamespaceId::default(), "orders".into()),
        user_id: UserId::new("user2"),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: 500, // Meta is at 1000, so this applies immediately
    };

    let cmd_bytes = encode_user_data_command(&cmd).unwrap();

    // Apply command - should apply immediately
    let _result = sm.apply(1, 1, &cmd_bytes).await.unwrap();

    // No commands should be pending
    assert_eq!(sm.pending_count(), 0);
}

// =============================================================================
// Snapshot Persistence Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_user_data_snapshot_includes_pending_commands() {
    // Initialize coordinator at index 0 to force buffering
    let _ = init_coordinator(0);

    let sm = UserDataStateMachine::new(2);

    let current_meta = get_coordinator().current_index();

    // Create and buffer a command (well above current meta)
    let cmd = UserDataCommand::Insert {
        table_id: TableId::new(NamespaceId::new("ns"), "table".into()),
        user_id: UserId::new("user"),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: current_meta + 1000, // Will be buffered
    };

    let cmd_bytes = encode_user_data_command(&cmd).unwrap();
    sm.apply(1, 1, &cmd_bytes).await.unwrap();

    assert_eq!(sm.pending_count(), 1);

    // Take snapshot
    let snapshot = sm.snapshot().await.unwrap();

    // Create new state machine and restore
    let sm2 = UserDataStateMachine::new(2);
    sm2.restore(snapshot).await.unwrap();

    // Pending commands should be restored
    assert_eq!(sm2.pending_count(), 1);
}

// =============================================================================
// SharedDataStateMachine Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_shared_data_buffering() {
    // Initialize coordinator at index 0
    let _ = init_coordinator(0);

    let sm = SharedDataStateMachine::default();

    let current_meta = get_coordinator().current_index();

    // Create a command with required_meta_index well above current
    let cmd = SharedDataCommand::Insert {
        table_id: TableId::new(NamespaceId::system(), "shared_table".into()),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: current_meta + 500,
    };

    let cmd_bytes = encode_shared_data_command(&cmd).unwrap();

    // Apply command - should be buffered
    sm.apply(1, 1, &cmd_bytes).await.unwrap();

    assert_eq!(sm.pending_count(), 1);

    // Advance meta
    get_coordinator().advance(current_meta + 500);

    // Apply another command to trigger drain
    let cmd2 = SharedDataCommand::Insert {
        table_id: TableId::new(NamespaceId::system(), "other".into()),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: 0,
    };
    let cmd2_bytes = encode_shared_data_command(&cmd2).unwrap();
    sm.apply(2, 1, &cmd2_bytes).await.unwrap();

    assert_eq!(sm.pending_count(), 0);
}

#[tokio::test]
#[serial]
async fn test_shared_data_snapshot_roundtrip() {
    // Initialize coordinator at index 0
    let _ = init_coordinator(0);

    let sm = SharedDataStateMachine::default();

    let current_meta = get_coordinator().current_index();

    let cmd = SharedDataCommand::Insert {
        table_id: TableId::new(NamespaceId::system(), "config".into()),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: current_meta + 2000, // Will be buffered
    };

    let cmd_bytes = encode_shared_data_command(&cmd).unwrap();
    sm.apply(1, 1, &cmd_bytes).await.unwrap();

    assert_eq!(sm.pending_count(), 1);

    // Snapshot and restore
    let snapshot = sm.snapshot().await.unwrap();
    let sm2 = SharedDataStateMachine::default();
    sm2.restore(snapshot).await.unwrap();

    assert_eq!(sm2.pending_count(), 1);
}

// =============================================================================
// Rejoin Ordering Scenario Test
// =============================================================================

/// This test simulates the rejoin scenario from spec 018:
/// 1. Node was partitioned, has Meta at some index N, Data behind
/// 2. Leader sends Data log entries with required_meta_index values
/// 3. Data shard buffers entries that need meta > N
/// 4. As Meta replays entries, data drains buffered commands
#[tokio::test]
#[serial]
async fn test_rejoin_ordering_scenario() {
    // Initialize coordinator and get current base
    let _ = init_coordinator(0);

    // Use very high base to avoid interference from other tests (100 million range)
    let current_meta = 100_000_000_u64;
    get_coordinator().advance(current_meta);

    let sm = UserDataStateMachine::new(25); // Use unique shard to avoid interference

    // Simulate receiving replicated data entries with various watermarks relative to current_meta
    // Entries that can apply immediately: required_meta <= current_meta
    // Entries that must buffer: required_meta > current_meta

    let entries: Vec<(u64, u64, u64)> = vec![
        (51, 1, current_meta - 5),  // can apply immediately
        (52, 1, current_meta - 2),  // can apply immediately
        (53, 1, current_meta + 5),  // MUST BUFFER
        (54, 1, current_meta + 10), // MUST BUFFER
        (55, 1, current_meta),      // can apply immediately (exact match)
    ];

    for (log_index, term, required_meta) in &entries {
        let cmd = UserDataCommand::Insert {
            table_id: TableId::new(NamespaceId::default(), "test".into()),
            user_id: UserId::new("user"),
            rows: vec![Row {
                values: BTreeMap::new(),
            }],
            required_meta_index: *required_meta,
        };

        let cmd_bytes = encode_user_data_command(&cmd).unwrap();
        sm.apply(*log_index, *term, &cmd_bytes).await.unwrap();
    }

    // Should have 2 pending commands (entries 53 and 54)
    assert_eq!(sm.pending_count(), 2);

    // Simulate Meta catching up to current_meta + 5
    get_coordinator().advance(current_meta + 5);

    // Apply another command to trigger drain
    let trigger_cmd = UserDataCommand::Insert {
        table_id: TableId::new(NamespaceId::default(), "trigger".into()),
        user_id: UserId::new("trigger"),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: 0, // Immediate
    };
    let trigger_bytes = encode_user_data_command(&trigger_cmd).unwrap();
    sm.apply(56, 1, &trigger_bytes).await.unwrap();

    // After drain at current_meta+5, only one should remain (entry 54 needs current_meta+10)
    assert_eq!(sm.pending_count(), 1);

    // Advance Meta to current_meta + 10
    get_coordinator().advance(current_meta + 10);

    // Trigger drain again
    let trigger_cmd2 = UserDataCommand::Insert {
        table_id: TableId::new(NamespaceId::default(), "trigger2".into()),
        user_id: UserId::new("trigger2"),
        rows: vec![Row {
            values: BTreeMap::new(),
        }],
        required_meta_index: 0,
    };
    let trigger2_bytes = encode_user_data_command(&trigger_cmd2).unwrap();
    sm.apply(57, 1, &trigger2_bytes).await.unwrap();

    // All pending cleared
    assert_eq!(sm.pending_count(), 0);
}
