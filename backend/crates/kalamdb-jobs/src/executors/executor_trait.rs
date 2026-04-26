//! Job Executor Trait
//!
//! **Type-Safe Job Execution Framework**
//!
//! This module provides a type-safe job execution system with:
//! - Generic JobExecutor trait with associated Params type
//! - JobParams trait for parameter validation and serialization
//! - Type-safe JobContext<T> with embedded parameters
//! - Zero-cost abstractions (compile-time type checking)
//!
//! # Example
//!
//! ```rust,ignore
//! // 1. Define parameter struct
//! #[derive(Serialize, Deserialize, Clone)]
//! struct FlushParams {
//!     table_id: TableId,
//!     threshold: u64,
//! }
//!
//! impl JobParams for FlushParams {
//!     fn validate(&self) -> Result<(), KalamDbError> {
//!         Ok(()) // Validation logic
//!     }
//! }
//!
//! // 2. Implement executor with associated type
//! #[async_trait]
//! impl JobExecutor for FlushExecutor {
//!     type Params = FlushParams;
//!     
//!     async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
//!         // Type-safe access to params
//!         let threshold = ctx.params().threshold;
//!         // ... execution logic
//!     }
//! }
//! ```

// Note: CancellationToken is not available in tokio::sync in older versions
// We'll use a simple atomic bool for now
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use kalamdb_core::{app_context::AppContext, error::KalamDbError};
use kalamdb_system::JobType;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};

/// Simple cancellation token wrapper
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Decision made by a job executor after execution
#[derive(Debug, Clone)]
pub enum JobDecision {
    /// Job completed successfully
    Completed {
        /// Success message
        message: Option<String>,
    },
    /// Job should be retried after a delay
    Retry {
        /// Error message
        message: String,
        /// Optional stack trace
        exception_trace: Option<String>,
        /// Backoff delay in milliseconds
        backoff_ms: u64,
    },
    /// Job failed permanently (no more retries)
    Failed {
        /// Error message
        message: String,
        /// Optional stack trace
        exception_trace: Option<String>,
    },
    /// Job was skipped (e.g., table doesn't exist, precondition not met)
    Skipped {
        /// Reason for skipping
        message: String,
    },
}

/// Trait for job parameters
///
/// All job parameter structs must implement this trait.
/// Provides validation and type-safe serialization.
pub trait JobParams: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static {
    /// Validate parameters
    ///
    /// Called before job execution to ensure parameters are valid.
    /// Return Err if validation fails.
    fn validate(&self) -> Result<(), KalamDbError> {
        Ok(()) // Default: no validation
    }
}

/// Type-safe context passed to job executors
///
/// Generic over parameter type T for compile-time type safety.
#[derive(Clone)]
pub struct JobContext<T: JobParams> {
    /// Application context for accessing stores, registries, etc.
    pub app_ctx: Arc<AppContext>,
    /// Cancellation token for graceful shutdown
    pub cancellation_token: CancellationToken,
    /// Job ID for logging (automatically prefixed to log messages)
    pub job_id: String,
    /// Typed job parameters (deserialized once at creation)
    params: T,
}

impl<T: JobParams> JobContext<T> {
    /// Create a new job context with typed parameters
    pub fn new(app_ctx: Arc<AppContext>, job_id: String, params: T) -> Self {
        Self {
            app_ctx,
            cancellation_token: CancellationToken::new(),
            job_id,
            params,
        }
    }

    /// Create a new job context with cancellation token
    pub fn with_cancellation(
        app_ctx: Arc<AppContext>,
        job_id: String,
        params: T,
        token: CancellationToken,
    ) -> Self {
        Self {
            app_ctx,
            cancellation_token: token,
            job_id,
            params,
        }
    }

    /// Get typed parameters (zero-cost access)
    pub fn params(&self) -> &T {
        &self.params
    }

    /// Consume context and return parameters
    pub fn into_params(self) -> T {
        self.params
    }

    /// Log debug message with [JobId] prefix
    pub fn log_debug(&self, message: &str) {
        debug!("[{}] {}", self.job_id, message);
    }

    /// Log trace message with [JobId] prefix
    pub fn log_trace(&self, message: &str) {
        trace!("[{}] {}", self.job_id, message);
    }

    /// Log info message with [JobId] prefix
    pub fn log_info(&self, message: &str) {
        info!("[{}] {}", self.job_id, message);
    }

    /// Log warning message with [JobId] prefix
    pub fn log_warn(&self, message: &str) {
        warn!("[{}] {}", self.job_id, message);
    }

    /// Log error message with [JobId] prefix
    pub fn log_error(&self, message: &str) {
        error!("[{}] {}", self.job_id, message);
    }

    /// Get current timestamp in milliseconds
    pub fn now_millis() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    /// Get current timestamp in seconds
    pub fn now_secs() -> i64 {
        chrono::Utc::now().timestamp()
    }
}

/// Trait for type-safe job executors
///
/// Uses associated types for compile-time parameter type checking.
/// Each executor defines its Params type, ensuring type safety throughout execution.
///
/// # Example
///
/// ```rust,ignore
/// use kalamdb_core::jobs::executors::{JobExecutor, JobParams, JobDecision, JobContext};
/// use kalamdb_core::error::KalamDbError;
/// use kalamdb_system::JobType;
/// use async_trait::async_trait;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, Clone)]
/// struct FlushParams {
///     pub table_id: TableId,
///     pub threshold: u64,
/// }
///
/// impl JobParams for FlushParams {}
///
/// struct FlushExecutor;
///
/// #[async_trait]
/// impl JobExecutor for FlushExecutor {
///     type Params = FlushParams;
///
///     fn job_type(&self) -> JobType {
///         JobType::Flush
///     }
///
///     fn name(&self) -> &'static str {
///         "FlushExecutor"
///     }
///
///     async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
///         ctx.log_info(&format!("Flushing table: {}", ctx.params().table_id));
///         // Type-safe parameter access - no JSON parsing!
///         Ok(JobDecision::Completed { message: None })
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait JobExecutor: Send + Sync {
    /// Associated parameter type
    ///
    /// Defines the parameter struct this executor expects.
    /// Must implement JobParams trait.
    type Params: JobParams;

    /// Returns the job type this executor handles
    fn job_type(&self) -> JobType;

    /// Returns the executor name for logging
    fn name(&self) -> &'static str;

    /// Optional pre-validation hook executed before a job is created
    ///
    /// Returns `Ok(true)` when the scheduler should enqueue the job,
    /// `Ok(false)` to skip job creation (e.g., nothing to process), and
    /// `Err` when validation fails.
    async fn pre_validate(
        &self,
        _app_ctx: &Arc<AppContext>,
        _params: &Self::Params,
    ) -> Result<bool, KalamDbError> {
        Ok(true)
    }

    /// Executes the job with type-safe parameters (legacy single-phase execution)
    ///
    /// Parameters are already deserialized and validated in JobContext.
    /// Returns a JobDecision indicating whether the job completed, should retry, or failed.
    ///
    /// **Deprecated**: For new executors, implement `execute_local()` and optionally
    /// `execute_leader()` for the two-phase distributed execution model.
    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError>;

    /// Execute local work phase (runs on ALL nodes in cluster)
    ///
    /// This phase handles node-local operations that don't require cluster coordination:
    /// - RocksDB flushes (each node flushes its own buffered data)
    /// - Local cache eviction
    /// - Local file cleanup
    /// - RocksDB compaction
    ///
    /// By default, delegates to `execute()` for backwards compatibility with single-phase
    /// executors.
    ///
    /// # Returns
    /// - `Ok(JobDecision::Completed { .. })` - Local work completed successfully
    /// - `Ok(JobDecision::Failed { .. })` - Local work failed, skip leader phase
    /// - `Err(..)` - Critical error
    async fn execute_local(
        &self,
        ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        // Default: delegate to execute() for backward compatibility
        self.execute(ctx).await
    }

    /// Execute leader-only phase (runs ONLY on leader node)
    ///
    /// This phase handles operations that require cluster-wide coordination:
    /// - Uploading Parquet files to external storage (S3, GCS)
    /// - Registering manifest entries (shared metadata)
    /// - Updating cluster-wide state in system tables
    /// - Coordinating with external services
    ///
    /// Called only for job types where `job_type().has_leader_actions()` returns true.
    /// Only executed if `execute_local()` returned `JobDecision::Completed`.
    ///
    /// By default, returns Completed (no leader actions needed).
    ///
    /// # Returns
    /// - `Ok(JobDecision::Completed { .. })` - Leader actions completed
    /// - `Ok(JobDecision::Failed { .. })` - Leader actions failed
    /// - `Err(..)` - Critical error
    async fn execute_leader(
        &self,
        _ctx: &JobContext<Self::Params>,
    ) -> Result<JobDecision, KalamDbError> {
        // Default: no leader actions needed
        Ok(JobDecision::Completed { message: None })
    }

    /// Cancels a running job
    ///
    /// Called when a job cancellation is requested. The executor should
    /// perform any necessary cleanup and stop execution gracefully.
    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Cancel not implemented for this executor");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_core::test_helpers::test_app_context_simple;

    use super::*;

    #[test]
    fn test_job_decision_completed() {
        let decision = JobDecision::Completed {
            message: Some("Success".to_string()),
        };
        match decision {
            JobDecision::Completed { message } => {
                assert_eq!(message, Some("Success".to_string()));
            },
            _ => panic!("Expected Completed decision"),
        }
    }

    #[test]
    fn test_job_decision_retry() {
        let decision = JobDecision::Retry {
            message: "Temporary failure".to_string(),
            exception_trace: Some("Stack trace".to_string()),
            backoff_ms: 1000,
        };
        match decision {
            JobDecision::Retry {
                message,
                exception_trace,
                backoff_ms,
            } => {
                assert_eq!(message, "Temporary failure");
                assert_eq!(exception_trace, Some("Stack trace".to_string()));
                assert_eq!(backoff_ms, 1000);
            },
            _ => panic!("Expected Retry decision"),
        }
    }

    #[test]
    fn test_job_decision_failed() {
        let decision = JobDecision::Failed {
            message: "Permanent failure".to_string(),
            exception_trace: None,
        };
        match decision {
            JobDecision::Failed {
                message,
                exception_trace,
            } => {
                assert_eq!(message, "Permanent failure");
                assert_eq!(exception_trace, None);
            },
            _ => panic!("Expected Failed decision"),
        }
    }

    #[tokio::test]
    async fn test_job_context_logging() {
        let app_ctx = test_app_context_simple();

        // Use dummy params for testing
        #[derive(Clone, Serialize, Deserialize)]
        struct DummyParams;
        impl JobParams for DummyParams {}

        let ctx = JobContext::new(app_ctx, "FL-abc123".to_string(), DummyParams);

        // These should not panic
        ctx.log_debug("Debug message");
        ctx.log_info("Info message");
        ctx.log_warn("Warning message");
        ctx.log_error("Error message");
    }

    #[test]
    fn test_job_context_timestamp() {
        let now_millis = JobContext::<DummyParams>::now_millis();
        let now_secs = JobContext::<DummyParams>::now_secs();

        assert!(now_millis > 0);
        assert!(now_secs > 0);
        // now_millis and now_secs use separate Utc::now() calls, so
        // now_millis can equal now_secs*1000 on an exact-second boundary
        // or even be slightly behind if the second ticks between calls.
        assert!(now_millis >= now_secs * 1000);
    }

    // Helper type for tests
    #[derive(Clone, Serialize, Deserialize)]
    struct DummyParams;
    impl JobParams for DummyParams {}
}
