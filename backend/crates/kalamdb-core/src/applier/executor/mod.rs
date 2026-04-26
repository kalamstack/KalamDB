//! Command Executor - The SINGLE place where all mutations happen
//!
//! This module contains the shared execution logic for all database commands.
//! Whether in standalone or cluster mode, all mutations flow through here.
//!
//! ## Modules
//!
//! - `ddl`: DDL operations (CREATE/ALTER/DROP TABLE) - Meta group
//! - `dml`: DML operations (INSERT/UPDATE/DELETE) - Data groups (sharded)
//! - `namespace`: Namespace management - Meta group
//! - `storage`: Storage backend management - Meta group
//! - `user`: User management - Meta group
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     CommandExecutorImpl                         │
//! │  ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌───────────────┐  │
//! │  │DdlExecutor│ │DmlExecutor│ │NamespaceExec│ │Storage/UserExec│ │
//! │  └─────┬─────┘ └─────┬────┘ └──────┬───────┘ └───────┬───────┘  │
//! └────────┼─────────────┼─────────────┼─────────────────┼──────────┘
//!          │             │             │                 │
//!   Meta Group     Data Groups    Meta Group        Meta Group
//!   (1 Raft)      (32 Shards)     (1 Raft)          (1 Raft)
//! ```
//!
//! ## Unified Applier Design
//!
//! Both standalone and cluster modes use the same executor logic:
//! - **Cluster**: Raft StateMachine → ProviderXxxApplier → CommandExecutorImpl
//! - **Standalone**: Direct calls to CommandExecutorImpl (DML via handlers)
//!
//! The DmlExecutor is stateless, allowing parallel execution across shards.

mod ddl;
mod dml;
mod namespace;
mod storage;
mod user;
mod utils;

use std::sync::Arc;

pub use ddl::DdlExecutor;
pub use dml::DmlExecutor;
pub use namespace::NamespaceExecutor;
pub use storage::StorageExecutor;
pub use user::UserExecutor;

use crate::app_context::AppContext;

/// The unified command executor
///
/// This is the SINGLE place where all database mutations happen.
/// It's used by:
/// - `StandaloneApplier` for direct execution
/// - `RaftStateMachine::apply()` for replicated execution
///
/// ## Key Invariant
///
/// All state changes (Meta and Data) happen HERE (via sub-executors),
/// not in handlers or other appliers.
///
/// ## Sharding Note
///
/// While this structure unifies the logic, `DmlExecutor` is designed to be
/// called by specific Data Raft Groups (User Shards). It is stateless and
/// allows parallel execution across different shards.
pub struct CommandExecutorImpl {
    app_context: Arc<AppContext>,
    ddl: DdlExecutor,
    dml: DmlExecutor,
    namespace: NamespaceExecutor,
    storage: StorageExecutor,
    user: UserExecutor,
}

impl CommandExecutorImpl {
    /// Create a new CommandExecutorImpl
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self {
            ddl: DdlExecutor::new(Arc::clone(&app_context)),
            dml: DmlExecutor::new(Arc::clone(&app_context)),
            namespace: NamespaceExecutor::new(Arc::clone(&app_context)),
            storage: StorageExecutor::new(Arc::clone(&app_context)),
            user: UserExecutor::new(Arc::clone(&app_context)),
            app_context,
        }
    }

    /// Get the DDL executor
    pub fn ddl(&self) -> &DdlExecutor {
        &self.ddl
    }

    /// Get the DML executor
    pub fn dml(&self) -> &DmlExecutor {
        &self.dml
    }

    /// Get the namespace executor
    pub fn namespace(&self) -> &NamespaceExecutor {
        &self.namespace
    }

    /// Get the storage executor
    pub fn storage(&self) -> &StorageExecutor {
        &self.storage
    }

    /// Get the user executor
    pub fn user(&self) -> &UserExecutor {
        &self.user
    }

    /// Get the app context
    pub fn app_context(&self) -> &Arc<AppContext> {
        &self.app_context
    }
}
