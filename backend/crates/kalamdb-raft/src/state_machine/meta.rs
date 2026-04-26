//! MetaStateMachine - Unified metadata state machine
//!
//! This state machine handles all metadata operations in a single Raft group:
//! - Namespace CRUD (create, delete)
//! - Table CRUD (create, alter, drop)
//! - Storage registration/unregistration
//! - User CRUD (create, update, delete, login, lock)
//! - Job lifecycle (create, claim, update, complete, fail, release, cancel)
//!
//! Runs in the unified Meta Raft group (replaces MetaSystem + MetaUsers + MetaJobs).

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{
    decode as bincode_decode, encode as bincode_encode, ApplyResult, KalamStateMachine,
    StateMachineSnapshot,
};
use crate::{
    applier::MetaApplier,
    commands::{MetaCommand, MetaResponse},
    GroupId, RaftError,
};

// =============================================================================
// Snapshot Structure
// =============================================================================

/// Minimal snapshot data for MetaStateMachine
///
/// The actual metadata (namespaces, tables, storages, users, jobs, live queries)
/// is persisted by the MetaApplier to RocksDB. This snapshot only tracks the
/// last applied Raft index/term for idempotency on restore.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MetaSnapshot {
    // Version for future compatibility
    version: u8,
}

// =============================================================================
// MetaStateMachine
// =============================================================================

/// Unified state machine for all metadata operations
///
/// This replaces SystemStateMachine, UsersStateMachine, and JobsStateMachine
/// with a single state machine that handles all metadata in one totally-ordered log.
///
/// ## Benefits
///
/// - Single `meta_index` for watermark-based data group ordering
/// - No cross-metadata race conditions
/// - Simpler catch-up coordination
pub struct MetaStateMachine {
    /// Last applied log index (for idempotency)
    last_applied_index: AtomicU64,
    /// Last applied log term
    last_applied_term: AtomicU64,
    /// Notifies waiters when the applied index advances.
    last_applied_tx: tokio::sync::watch::Sender<u64>,
    /// Approximate data size in bytes
    approximate_size: AtomicU64,

    /// Optional applier for persisting to providers
    applier: parking_lot::RwLock<Option<Arc<dyn MetaApplier>>>,
}

impl std::fmt::Debug for MetaStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStateMachine")
            .field("last_applied_index", &self.last_applied_index.load(Ordering::Relaxed))
            .field("last_applied_term", &self.last_applied_term.load(Ordering::Relaxed))
            .field("approximate_size", &self.approximate_size.load(Ordering::Relaxed))
            .finish()
    }
}

impl MetaStateMachine {
    /// Create a new MetaStateMachine without an applier
    ///
    /// Use `set_applier` to inject persistence after construction.
    pub fn new() -> Self {
        let (last_applied_tx, _) = tokio::sync::watch::channel(0);
        Self {
            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
            last_applied_tx,
            approximate_size: AtomicU64::new(0),
            applier: parking_lot::RwLock::new(None),
        }
    }

    /// Create a new MetaStateMachine with an applier
    pub fn with_applier(applier: Arc<dyn MetaApplier>) -> Self {
        let (last_applied_tx, _) = tokio::sync::watch::channel(0);
        Self {
            last_applied_index: AtomicU64::new(0),
            last_applied_term: AtomicU64::new(0),
            last_applied_tx,
            approximate_size: AtomicU64::new(0),
            applier: parking_lot::RwLock::new(Some(applier)),
        }
    }

    /// Set the applier for persisting to providers
    ///
    /// This is called after RaftManager creation once providers are available.
    pub fn set_applier(&self, applier: Arc<dyn MetaApplier>) {
        let mut guard = self.applier.write();
        *guard = Some(applier);
        log::debug!("MetaStateMachine: Applier registered for persistence");
    }

    /// Check if an applier is registered
    pub fn has_applier(&self) -> bool {
        self.applier.read().is_some()
    }

    /// Get the current last applied index
    ///
    /// This is used by data groups to capture the watermark at proposal time.
    pub fn last_applied_index(&self) -> u64 {
        self.last_applied_index.load(Ordering::Acquire)
    }

    fn publish_last_applied(&self, index: u64) {
        self.last_applied_tx.send_replace(index);
    }

    /// Apply a meta command
    async fn apply_command(&self, cmd: MetaCommand) -> Result<MetaResponse, RaftError> {
        let applier = {
            let guard = self.applier.read();
            guard.clone()
        };

        if applier.is_none() {
            // Without an applier, Raft metadata operations will NOT be persisted into providers
            // (system tables, schema registry, etc.). This can make DDL appear to succeed while
            // subsequent queries cannot see the change.
            log::warn!(
                "MetaStateMachine: No applier registered; applying MetaCommand without persistence"
            );
        }

        match cmd {
            // =================================================================
            // Namespace Operations
            // =================================================================
            MetaCommand::CreateNamespace {
                namespace_id,
                created_by,
            } => {
                log::debug!(
                    "MetaStateMachine: CreateNamespace {:?} by {:?}",
                    namespace_id,
                    created_by
                );

                let message = if let Some(ref a) = applier {
                    a.create_namespace(&namespace_id, created_by.as_ref()).await?
                } else {
                    String::new()
                };

                self.approximate_size.fetch_add(100, Ordering::Relaxed);
                Ok(MetaResponse::NamespaceCreated {
                    namespace_id,
                    message,
                })
            },

            MetaCommand::DeleteNamespace { namespace_id } => {
                log::debug!("MetaStateMachine: DeleteNamespace {:?}", namespace_id);

                if let Some(ref a) = applier {
                    a.delete_namespace(&namespace_id).await?;
                }

                Ok(MetaResponse::Ok)
            },

            // =================================================================
            // Table Operations
            // =================================================================
            MetaCommand::CreateTable {
                table_id,
                table_type,
                table_def,
            } => {
                log::debug!("MetaStateMachine: CreateTable {:?} (type: {})", table_id, table_type);

                let message = if let Some(ref a) = applier {
                    a.create_table(&table_id, table_type, &table_def).await?
                } else {
                    String::new()
                };

                self.approximate_size.fetch_add(500, Ordering::Relaxed);
                Ok(MetaResponse::TableCreated { table_id, message })
            },

            MetaCommand::AlterTable {
                table_id,
                table_def,
            } => {
                log::debug!("MetaStateMachine: AlterTable {:?}", table_id);

                if let Some(ref a) = applier {
                    a.alter_table(&table_id, &table_def).await?;
                }

                Ok(MetaResponse::Ok)
            },

            MetaCommand::DropTable { table_id } => {
                log::debug!("MetaStateMachine: DropTable {:?}", table_id);

                if let Some(ref a) = applier {
                    a.drop_table(&table_id).await?;
                }

                Ok(MetaResponse::Ok)
            },

            // =================================================================
            // Storage Operations
            // =================================================================
            MetaCommand::RegisterStorage {
                storage_id,
                storage,
            } => {
                log::debug!("MetaStateMachine: RegisterStorage {:?}", storage_id);

                if let Some(ref a) = applier {
                    a.register_storage(&storage_id, &storage).await?;
                }

                self.approximate_size.fetch_add(300, Ordering::Relaxed);
                Ok(MetaResponse::Ok)
            },

            MetaCommand::UnregisterStorage { storage_id } => {
                log::debug!("MetaStateMachine: UnregisterStorage {:?}", storage_id);

                if let Some(ref a) = applier {
                    a.unregister_storage(&storage_id).await?;
                }

                Ok(MetaResponse::Ok)
            },

            // =================================================================
            // User Operations
            // =================================================================
            MetaCommand::CreateUser { user } => {
                log::debug!("MetaStateMachine: CreateUser {:?}", user.user_id);

                let message = if let Some(ref a) = applier {
                    a.create_user(&user).await?
                } else {
                    String::new()
                };

                let user_id = user.user_id.clone();
                Ok(MetaResponse::UserCreated { user_id, message })
            },

            MetaCommand::UpdateUser { user } => {
                log::debug!("MetaStateMachine: UpdateUser {:?}", user.user_id);

                if let Some(ref a) = applier {
                    a.update_user(&user).await?;
                }
                Ok(MetaResponse::Ok)
            },

            MetaCommand::DeleteUser {
                user_id,
                deleted_at,
            } => {
                log::debug!("MetaStateMachine: DeleteUser {:?}", user_id);

                if let Some(ref a) = applier {
                    a.delete_user(&user_id, deleted_at.timestamp_millis()).await?;
                }
                Ok(MetaResponse::Ok)
            },

            MetaCommand::RecordLogin {
                user_id,
                logged_in_at,
            } => {
                log::trace!("MetaStateMachine: RecordLogin {:?}", user_id);

                if let Some(ref a) = applier {
                    a.record_login(&user_id, logged_in_at.timestamp_millis()).await?;
                }
                Ok(MetaResponse::Ok)
            },

            MetaCommand::SetUserLocked {
                user_id,
                locked_until,
                updated_at,
            } => {
                log::debug!("MetaStateMachine: SetUserLocked {:?} = {:?}", user_id, locked_until);

                if let Some(ref a) = applier {
                    a.set_user_locked(&user_id, locked_until, updated_at.timestamp_millis())
                        .await?;
                }
                Ok(MetaResponse::Ok)
            },

            // =================================================================
            // Job Operations
            // =================================================================
            MetaCommand::CreateJob { job } => {
                log::debug!("MetaStateMachine: CreateJob {} (type: {})", job.job_id, job.job_type);

                let message = if let Some(ref a) = applier {
                    a.create_job(&job).await?
                } else {
                    String::new()
                };

                self.approximate_size.fetch_add(200, Ordering::Relaxed);
                Ok(MetaResponse::JobCreated {
                    job_id: job.job_id,
                    message,
                })
            },

            MetaCommand::CreateJobNode {
                job_id,
                node_id,
                status,
                created_at,
            } => {
                log::debug!(
                    "MetaStateMachine: CreateJobNode job={} node={} status={:?}",
                    job_id,
                    node_id,
                    status
                );

                if let Some(ref a) = applier {
                    a.create_job_node(&job_id, node_id, status, created_at.timestamp_millis())
                        .await?;
                }

                Ok(MetaResponse::Ok)
            },

            MetaCommand::ClaimJobNode {
                job_id,
                node_id,
                claimed_at,
            } => {
                log::debug!("MetaStateMachine: ClaimJobNode job={} node={}", job_id, node_id);

                if let Some(ref a) = applier {
                    a.claim_job_node(&job_id, node_id, claimed_at.timestamp_millis()).await?;
                }

                Ok(MetaResponse::Ok)
            },

            MetaCommand::UpdateJobNodeStatus {
                job_id,
                node_id,
                status,
                error_message,
                updated_at,
            } => {
                log::debug!(
                    "MetaStateMachine: UpdateJobNodeStatus job={} node={} -> {:?}",
                    job_id,
                    node_id,
                    status
                );

                if let Some(ref a) = applier {
                    a.update_job_node_status(
                        &job_id,
                        node_id,
                        status,
                        error_message.as_deref(),
                        updated_at.timestamp_millis(),
                    )
                    .await?;
                }

                Ok(MetaResponse::Ok)
            },

            MetaCommand::ClaimJob {
                job_id,
                node_id,
                claimed_at,
            } => {
                log::debug!("MetaStateMachine: ClaimJob {} by node {}", job_id, node_id);

                // Already-claimed check moved to MetaApplier::claim_job() which reads from RocksDB
                let message = if let Some(ref a) = applier {
                    a.claim_job(&job_id, node_id, claimed_at.timestamp_millis()).await?
                } else {
                    String::new()
                };

                Ok(MetaResponse::JobClaimed {
                    job_id,
                    node_id,
                    message,
                })
            },

            MetaCommand::UpdateJobStatus {
                job_id,
                status,
                updated_at,
            } => {
                log::debug!("MetaStateMachine: UpdateJobStatus {} -> {:?}", job_id, status);

                if let Some(ref a) = applier {
                    a.update_job_status(&job_id, status, updated_at.timestamp_millis()).await?;
                }

                Ok(MetaResponse::Ok)
            },

            MetaCommand::CompleteJob {
                job_id,
                result,
                completed_at,
            } => {
                log::debug!("MetaStateMachine: CompleteJob {}", job_id);

                if let Some(ref a) = applier {
                    a.complete_job(&job_id, result.as_deref(), completed_at.timestamp_millis())
                        .await?;
                }

                Ok(MetaResponse::Ok)
            },
            MetaCommand::FailJob {
                job_id,
                error_message,
                failed_at,
            } => {
                log::warn!("MetaStateMachine: FailJob {}: {}", job_id, error_message);

                if let Some(ref a) = applier {
                    a.fail_job(&job_id, &error_message, failed_at.timestamp_millis()).await?;
                }

                Ok(MetaResponse::Ok)
            },

            MetaCommand::ReleaseJob {
                job_id,
                reason,
                released_at,
            } => {
                log::debug!("MetaStateMachine: ReleaseJob {}: {}", job_id, reason);

                if let Some(ref a) = applier {
                    a.release_job(&job_id, &reason, released_at.timestamp_millis()).await?;
                }

                Ok(MetaResponse::Ok)
            },

            MetaCommand::CancelJob {
                job_id,
                reason,
                cancelled_at,
            } => {
                log::debug!("MetaStateMachine: CancelJob {}: {}", job_id, reason);

                if let Some(ref a) = applier {
                    a.cancel_job(&job_id, &reason, cancelled_at.timestamp_millis()).await?;
                }

                Ok(MetaResponse::Ok)
            },
        }
    }
}

impl Default for MetaStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KalamStateMachine for MetaStateMachine {
    fn group_id(&self) -> GroupId {
        GroupId::Meta
    }

    async fn apply(&self, index: u64, term: u64, command: &[u8]) -> Result<ApplyResult, RaftError> {
        // Idempotency check
        let last_applied = self.last_applied_index.load(Ordering::Acquire);
        if index <= last_applied {
            log::debug!(
                "MetaStateMachine: Skipping already applied entry {} (last_applied={})",
                index,
                last_applied
            );
            return Ok(ApplyResult::NoOp);
        }

        // Deserialize command
        let cmd = crate::codec::command_codec::decode_meta_command(command)?;

        log::debug!(
            "MetaStateMachine: Applying entry {} (term={}, category={})",
            index,
            term,
            cmd.category()
        );

        // Apply command
        let response = self.apply_command(cmd).await?;

        // Update last applied
        self.last_applied_index.store(index, Ordering::Release);
        self.last_applied_term.store(term, Ordering::Release);
        self.publish_last_applied(index);

        // Notify data shards that meta has advanced (for watermark draining)
        super::get_coordinator().advance(index);

        // Serialize response
        let response_bytes = crate::codec::command_codec::encode_meta_response(&response)?;
        Ok(ApplyResult::ok_with_data(response_bytes))
    }

    fn last_applied_index(&self) -> u64 {
        self.last_applied_index.load(Ordering::Acquire)
    }

    fn subscribe_last_applied(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.last_applied_tx.subscribe())
    }

    fn mark_applied_index(&self, index: u64, term: u64) {
        let last_applied = self.last_applied_index.load(Ordering::Acquire);
        if index <= last_applied {
            return;
        }

        self.last_applied_index.store(index, Ordering::Release);
        self.last_applied_term.store(term, Ordering::Release);
        self.publish_last_applied(index);
    }

    fn last_applied_term(&self) -> u64 {
        self.last_applied_term.load(Ordering::Acquire)
    }

    async fn snapshot(&self) -> Result<StateMachineSnapshot, RaftError> {
        // Minimal snapshot - actual metadata is in RocksDB via MetaApplier
        let snapshot = MetaSnapshot { version: 1 };
        let snapshot_bytes = bincode_encode(&snapshot)?;

        Ok(StateMachineSnapshot::new(
            GroupId::Meta,
            self.last_applied_index.load(Ordering::Acquire),
            self.last_applied_term.load(Ordering::Acquire),
            snapshot_bytes,
        ))
    }

    async fn restore(&self, snapshot: StateMachineSnapshot) -> Result<(), RaftError> {
        // Validate snapshot format (version check for future compatibility)
        let _snapshot: MetaSnapshot = bincode_decode(&snapshot.data)?;

        self.last_applied_index.store(snapshot.last_applied_index, Ordering::Release);
        self.last_applied_term.store(snapshot.last_applied_term, Ordering::Release);
        self.publish_last_applied(snapshot.last_applied_index);

        log::info!(
            "MetaStateMachine: Restored snapshot at index {} term {}",
            snapshot.last_applied_index,
            snapshot.last_applied_term
        );

        Ok(())
    }

    fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_state_machine_new() {
        let sm = MetaStateMachine::new();
        assert_eq!(sm.last_applied_index(), 0);
        assert_eq!(sm.group_id(), GroupId::Meta);
    }
}
