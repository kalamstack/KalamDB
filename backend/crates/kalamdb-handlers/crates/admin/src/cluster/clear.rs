//! CLUSTER CLEAR handler
//!
//! Clears old snapshots from the cluster storage

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::executor::handlers::{
    ExecutionContext, ExecutionResult, ScalarValue, StatementHandler,
};
use kalamdb_sql::classifier::{SqlStatement, SqlStatementKind};
use std::sync::Arc;

pub struct ClusterClearHandler {
    app_context: Arc<AppContext>,
}

impl ClusterClearHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }
}

impl StatementHandler for ClusterClearHandler {
    async fn execute(
        &self,
        statement: SqlStatement,
        _params: Vec<ScalarValue>,
        ctx: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        if !matches!(statement.kind(), SqlStatementKind::ClusterClear) {
            return Err(KalamDbError::InvalidOperation(format!(
                "CLUSTER CLEAR handler received wrong statement type: {}",
                statement.name()
            )));
        }

        log::info!("CLUSTER CLEAR initiated by user: {}", ctx.user_id());

        // Get snapshot directory from config
        let config = self.app_context.config();
        let snapshots_dir = config.storage.resolved_snapshots_dir();

        if !snapshots_dir.exists() {
            return Ok(ExecutionResult::Success {
                message: format!(
                    "No snapshots directory found at: {}\nNothing to clear.",
                    snapshots_dir.display()
                ),
            });
        }

        // Count files before clearing
        let mut total_files = 0;
        let mut total_size: u64 = 0;
        let mut cleared_files = 0;
        let mut cleared_size: u64 = 0;
        let mut errors = Vec::new();

        // Keep the most recent snapshot per group (based on max_snapshots_to_keep config)
        // For now, we'll just report what would be cleared without actually deleting
        // since OpenRaft manages its own snapshot lifecycle

        // Walk the snapshots directory
        if let Ok(entries) = std::fs::read_dir(&snapshots_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    // Each subdirectory is a Raft group
                    if let Ok(group_entries) = std::fs::read_dir(&path) {
                        let mut snapshots: Vec<_> = group_entries
                            .flatten()
                            .filter(|e| e.path().extension().is_some_and(|ext| ext == "bin"))
                            .collect();

                        // Sort by modification time (newest first)
                        snapshots.sort_by_key(|e| {
                            std::cmp::Reverse(
                                e.metadata()
                                    .ok()
                                    .and_then(|m| m.modified().ok())
                                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                            )
                        });

                        let max_to_keep =
                            config.cluster.as_ref().map(|c| c.max_snapshots_to_keep).unwrap_or(3)
                                as usize;

                        for (i, snapshot) in snapshots.iter().enumerate() {
                            let size = snapshot.metadata().ok().map(|m| m.len()).unwrap_or(0);
                            total_files += 1;
                            total_size += size;

                            // Delete snapshots beyond the keep limit
                            if i >= max_to_keep {
                                match std::fs::remove_file(snapshot.path()) {
                                    Ok(()) => {
                                        cleared_files += 1;
                                        cleared_size += size;
                                        log::debug!("Cleared old snapshot: {:?}", snapshot.path());
                                    },
                                    Err(e) => {
                                        errors.push(format!(
                                            "{}: {}",
                                            snapshot.path().display(),
                                            e
                                        ));
                                    },
                                }
                            }
                        }
                    }
                }
            }
        }

        // Build response message
        let mut message = format!(
            "Cluster clear completed\n\
             Snapshots directory: {}\n\
             Total snapshots found: {} ({:.2} MB)\n\
             Snapshots cleared: {} ({:.2} MB freed)",
            snapshots_dir.display(),
            total_files,
            total_size as f64 / 1024.0 / 1024.0,
            cleared_files,
            cleared_size as f64 / 1024.0 / 1024.0
        );

        if !errors.is_empty() {
            message.push_str(&format!("\n\nErrors ({}):", errors.len()));
            for error in errors.iter().take(5) {
                message.push_str(&format!("\n  - {}", error));
            }
            if errors.len() > 5 {
                message.push_str(&format!("\n  ... and {} more errors", errors.len() - 5));
            }
        }

        log::info!(
            "CLUSTER CLEAR completed: {} files cleared, {:.2} MB freed",
            cleared_files,
            cleared_size as f64 / 1024.0 / 1024.0
        );

        Ok(ExecutionResult::Success { message })
    }
}
