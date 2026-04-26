//! # Job Management System
//!
//! Extracted from `kalamdb-core` to reduce compile times.
//! Provides `JobsManager` with concrete executors, flush/eviction schedulers,
//! health monitoring, and leader-only job execution for cluster mode.

// ============================================================================
// PHASE 9: UNIFIED JOB MANAGEMENT (PRODUCTION-READY)
// ============================================================================
pub mod executors;
pub mod flush_scheduler;
pub mod health_monitor;
pub mod jobs_manager;
pub mod stream_eviction;

// ============================================================================
// PHASE 16: LEADER-ONLY JOB EXECUTION (CLUSTER MODE)
// ============================================================================
pub mod leader_failover;
pub mod leader_guard;

// Phase 9 exports (production API)
pub use executors::{JobContext, JobDecision, JobExecutor as JobExecutorTrait, JobRegistry};
pub use flush_scheduler::FlushScheduler;
pub use health_monitor::HealthMonitor;
pub use jobs_manager::JobsManager;
// Phase 16 exports (cluster mode)
pub use leader_failover::{JobRecoveryAction, LeaderFailoverHandler, RecoveryReport};
pub use leader_guard::{LeaderOnlyJobGuard, LeadershipStatus};
pub use stream_eviction::StreamEvictionScheduler;

// ============================================================================
// JobWaker implementation (bridges kalamdb-core trait → JobsManager)
// ============================================================================
impl kalamdb_core::job_waker::JobWaker for JobsManager {
    fn awake_job(&self, job_id: kalamdb_commons::JobId) {
        // Delegate to the inherent method on JobsManager
        JobsManager::awake_job(self, job_id);
    }
}

// ============================================================================
// Extension trait: ergonomic `.job_manager()` on AppContext
// ============================================================================
use std::sync::Arc;

use kalamdb_core::app_context::AppContext;

/// Extension trait that provides typed access to the `JobsManager` stored
/// inside `AppContext` (which stores it as `Arc<dyn Any>`).
pub trait AppContextJobsExt {
    /// Downcast the type-erased job manager to `Arc<JobsManager>`.
    fn job_manager(&self) -> Arc<JobsManager>;
}

impl AppContextJobsExt for AppContext {
    fn job_manager(&self) -> Arc<JobsManager> {
        self.job_manager_raw()
            .clone()
            .downcast::<JobsManager>()
            .expect("job_manager is not a JobsManager — was set_job_manager called?")
    }
}

/// Convenience: create a fully-wired `JobsManager`, register all executors,
/// and install it into the given `AppContext`.
pub fn init_job_manager(app_ctx: &Arc<AppContext>) {
    use crate::executors::*;

    let job_registry = Arc::new(JobRegistry::new());
    job_registry.register(Arc::new(FlushExecutor::new()));
    job_registry.register(Arc::new(CleanupExecutor::new()));
    job_registry.register(Arc::new(RetentionExecutor::new()));
    job_registry.register(Arc::new(StreamEvictionExecutor::new()));
    job_registry.register(Arc::new(UserCleanupExecutor::new()));
    job_registry.register(Arc::new(CompactExecutor::new()));
    job_registry.register(Arc::new(BackupExecutor::new()));
    job_registry.register(Arc::new(RestoreExecutor::new()));
    job_registry.register(Arc::new(VectorIndexExecutor::new()));
    job_registry.register(Arc::new(TopicCleanupExecutor::new()));
    job_registry.register(Arc::new(TopicRetentionExecutor::new()));
    job_registry.register(Arc::new(UserExportExecutor::new()));

    let jobs_provider = app_ctx.system_tables().jobs();
    let job_nodes_provider = app_ctx.system_tables().job_nodes();
    let job_manager = Arc::new(JobsManager::new(
        jobs_provider,
        job_nodes_provider,
        job_registry,
        Arc::clone(app_ctx),
    ));
    // Pass as both the type-erased store and the JobWaker impl
    app_ctx.set_job_manager(job_manager.clone(), job_manager);
}
