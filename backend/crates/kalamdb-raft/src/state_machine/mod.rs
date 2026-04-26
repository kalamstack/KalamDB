//! State Machine implementations for Raft consensus
//!
//! Each Raft group has its own state machine that:
//! - Applies committed log entries to local state
//! - Creates snapshots for log compaction
//! - Restores state from snapshots
//!
//! ## State Machine Types
//!
//! - [`MetaStateMachine`]: Unified metadata (namespaces, tables, storages, users, jobs)
//! - [`UserDataStateMachine`]: Per-user data operations (DataUserShard groups)
//! - [`SharedDataStateMachine`]: Shared table operations (DataSharedShard group)
//!
//! ## Watermark Coordination
//!
//! - [`PendingBuffer`]: Buffers data commands until Meta catches up
//! - [`MetadataCoordinator`]: Broadcasts meta-advanced events to data shards

mod meta;
mod meta_coordinator;
mod pending_buffer;
pub mod serde_helpers;
mod shared_data;
mod trait_def;
mod user_data;

// Re-export serialization helpers for convenience
// Unified Meta state machine
pub use meta::MetaStateMachine;
// Watermark coordination
pub use meta_coordinator::{get_coordinator, init_coordinator, MetadataCoordinator};
pub use pending_buffer::{PendingBuffer, PendingCommand};
pub use serde_helpers::{decode, encode};
// Data state machines
pub use shared_data::SharedDataStateMachine;
pub use trait_def::{ApplyResult, KalamStateMachine, StateMachineSnapshot};
pub use user_data::UserDataStateMachine;
