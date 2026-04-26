//! Unified Command Applier (Phase 20 - Single Code Path)
//!
//! All commands flow through Raft consensus, even in single-node mode.
//! This ensures the same code path is tested in both deployment modes.
//!
//! ## Architecture
//!
//! ```text
//! SQL Handler
//!      │
//!      ▼
//! UnifiedApplier (trait)
//!      │
//!      ▼
//! RaftApplier (only implementation)
//!      │
//!      ├── Meta commands (DDL, users) → Meta Raft Group
//!      ├── User data → User Data Shards (by user_id)
//!      └── Shared data → Shared Data Shard
//!      │
//!      ▼
//! Raft Consensus (even single-node)
//!      │
//!      ▼
//! State Machine Apply
//!      │
//!      ▼
//! CommandExecutorImpl (actual mutations)
//! ```
//!
//! ## Key Invariants
//!
//! 1. **Single Mutation Point**: All mutations in `CommandExecutorImpl`
//! 2. **No Code Duplication**: Logic exists in exactly one place
//! 3. **No Mode Branching**: Same code for single-node and cluster
//! 4. **OpenRaft Quorum**: We trust OpenRaft for consensus

#[allow(clippy::module_inception)]
mod applier;
mod command;
mod error;
pub mod executor;

// Raft applier implementations (traits defined in kalamdb-raft)
pub mod raft;

// Re-exports
use std::sync::Arc;

pub use applier::{RaftApplier, UnifiedApplier};
pub use command::{CommandResult, CommandType, Validate};
pub use error::ApplierError;
pub use executor::CommandExecutorImpl;
// Re-export Raft appliers
pub use raft::{ProviderMetaApplier, ProviderSharedDataApplier, ProviderUserDataApplier};

use crate::app_context::AppContext;

/// Create the unified Raft applier
///
/// All commands flow through Raft (even in single-node mode).
pub fn create_applier(app_context: Arc<AppContext>) -> Arc<dyn UnifiedApplier> {
    log::debug!("Creating RaftApplier (unified Raft mode)");
    Arc::new(RaftApplier::new(app_context))
}
