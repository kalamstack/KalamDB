//! Command and response types for Raft operations
//!
//! Each Raft group has its own command and response types that define
//! the operations it can perform.
//!
//! ## Group Structure
//!
//! - **Meta group**: Unified metadata (namespaces, tables, storages, users, jobs)
//! - **Data groups**: User table shards + shared table shards

use kalamdb_commons::models::TransactionId;
use kalamdb_transactions::StagedMutation;
use serde::{Deserialize, Serialize};

mod data_response;
mod meta;
mod shared_data;
mod user_data;

// Unified Meta commands
pub use meta::{MetaCommand, MetaResponse};

// Data commands (split into separate files for better organization)
pub use data_response::{DataResponse, TransactionApplyResult};
pub use shared_data::SharedDataCommand;
pub use user_data::UserDataCommand;

/// Unified command type for all Raft operations
///
/// This wraps specific command types (Meta, UserData, SharedData) into a single
/// enum for type-safe routing and centralized serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Metadata operation (namespaces, tables, users, jobs)
    Meta(MetaCommand),
    /// User table data operation
    UserData(UserDataCommand),
    /// Shared table data operation
    SharedData(SharedDataCommand),
    /// Atomically replay an explicit transaction inside a single data group.
    TransactionCommit {
        transaction_id: TransactionId,
        mutations: Vec<StagedMutation>,
    },
}

/// Unified response type for all Raft operations
///
/// This wraps specific response types into a single enum for type-safe
/// deserialization and centralized handling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    /// Metadata operation response
    Meta(MetaResponse),
    /// Data operation response (user or shared)
    Data(DataResponse),
}
