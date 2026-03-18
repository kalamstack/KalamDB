//! # Job Management System
//!
//! **Phase 9 Status**: JobsManager with 8 concrete executors is production-ready.
//! Legacy components (JobExecutor, old schedulers) are DEPRECATED and pending migration.
//!
//! ## Examples
//!
//! ```rust,no_run
//! // Phase 9: Unified Job Management with typed JobIds
//! use kalamdb_core::jobs::{JobsManager, JobRegistry};
//! use kalamdb_core::app_context::AppContext;
//! use kalamdb_core::jobs::executors::*;
//! use kalamdb_core::tables::system::JobsTableProvider;
//! use kalamdb_system::JobNodesTableProvider;
//! use kalamdb_store::test_utils::InMemoryBackend;
//! use kalamdb_store::StorageBackend;
//! use std::sync::Arc;
//! use kalamdb_system::JobType;
//!
//! # fn example() {
//! let backend: Arc<dyn StorageBackend> = Arc::new(InMemoryBackend::new());
//! let jobs_provider = Arc::new(JobsTableProvider::new(backend));
//! # // app_ctx is injected by the caller
//! let job_registry = Arc::new(JobRegistry::new());
//! let job_nodes_provider = Arc::new(JobNodesTableProvider::new(backend));
//! let job_manager = Arc::new(JobsManager::new(
//!     jobs_provider,
//!     job_nodes_provider,
//!     job_registry,
//!     app_ctx,
//! ));
//!
//! // Register executors (8 concrete implementations)
//! job_registry.register("flush", Arc::new(FlushExecutor::new(/* ... */)));
//! job_registry.register("cleanup", Arc::new(CleanupExecutor::new(/* ... */)));
//! // ... register remaining 6 executors
//!
//! // Create job with typed JobId (idempotency enforced)
//! let job_id = job_manager.create_job(
//!     JobType::Flush,
//!     serde_json::json!({"namespace_id": "default", "table_name": "xyz"}),
//!     Some("flush-table-xyz".to_string()),  // Idempotency key
//!     None,  // JobOptions
//! ).await.unwrap();
//!
//! // Job processing loop (spawned in lifecycle.rs)
//! // job_manager.run_loop(max_concurrent).await?;
//! # }
//! ```

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
pub use health_monitor::HealthMonitor;
pub use jobs_manager::JobsManager;
pub use flush_scheduler::FlushScheduler;
pub use stream_eviction::StreamEvictionScheduler;

// Phase 16 exports (cluster mode)
pub use leader_failover::{JobRecoveryAction, LeaderFailoverHandler, RecoveryReport};
pub use leader_guard::{LeaderOnlyJobGuard, LeadershipStatus};
