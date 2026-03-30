//! Job Registry
//!
//! Central registry for all job executors with type erasure for heterogeneous storage.
//!
//! ## Type Erasure Pattern
//!
//! The registry uses a two-layer approach:
//! 1. `JobExecutor<Params = T>` - Type-safe trait with associated types
//! 2. `DynJobExecutor` - Type-erased trait for storage in DashMap
//!
//! This allows storing executors with different parameter types in a single collection
//! while preserving type safety at execution time through runtime deserialization.

use super::executor_trait::{JobContext, JobDecision, JobExecutor, JobParams};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::error_extensions::SerdeJsonResultExt;
use async_trait::async_trait;
use kalamdb_system::Job;
use kalamdb_system::JobType;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Type-erased job executor trait for heterogeneous storage
///
/// Internal trait used by JobRegistry to store executors with different
/// parameter types in a single DashMap. Handles deserialization and
/// validation at runtime.
#[async_trait]
pub(crate) trait DynJobExecutor: Send + Sync {
    /// Returns the executor name for logging
    fn name(&self) -> &'static str;

    /// Pre-validates job parameters before job creation
    ///
    /// Returns Ok(true) to proceed with job creation, Ok(false) to skip,
    /// or Err if validation fails.
    async fn pre_validate_dyn(
        &self,
        app_ctx: &Arc<AppContext>,
        params_json: &str,
    ) -> Result<bool, KalamDbError>;

    /// Executes the job with dynamic parameter deserialization (legacy single-phase)
    ///
    /// Deserializes parameters from JSON, validates them, creates typed
    /// JobContext, and delegates to type-safe execute method.
    async fn execute_dyn(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError>;

    /// Execute local work phase (runs on ALL nodes)
    ///
    /// Deserializes parameters and delegates to executor's execute_local method.
    async fn execute_local_dyn(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError>;

    /// Execute leader-only phase (runs ONLY on leader)
    ///
    /// Deserializes parameters and delegates to executor's execute_leader method.
    async fn execute_leader_dyn(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError>;

    /// Cancels a running job with dynamic parameter deserialization
    async fn cancel_dyn(&self, app_ctx: Arc<AppContext>, job: &Job) -> Result<(), KalamDbError>;
}

/// Bridge from type-safe JobExecutor<T> to type-erased DynJobExecutor
///
/// Implements DynJobExecutor for any type that implements JobExecutor,
/// handling the deserialization and type conversion automatically.
#[async_trait]
impl<T, E> DynJobExecutor for E
where
    T: JobParams,
    E: JobExecutor<Params = T>,
{
    fn name(&self) -> &'static str {
        JobExecutor::name(self)
    }

    async fn pre_validate_dyn(
        &self,
        app_ctx: &Arc<AppContext>,
        params_json: &str,
    ) -> Result<bool, KalamDbError> {
        // Deserialize parameters from JSON string
        let params: T = serde_json::from_str(params_json)
            .into_serde_error("Failed to deserialize job parameters for pre-validation")?;

        // Validate parameters
        params.validate()?;

        // Call type-safe pre_validate method
        self.pre_validate(app_ctx, &params).await
    }

    async fn execute_dyn(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError> {
        // Deserialize parameters from JSON string
        let params_json = job
            .parameters
            .as_ref()
            .ok_or_else(|| KalamDbError::InvalidOperation("Missing job parameters".to_string()))?;

        let params: T = serde_json::from_str(params_json)
            .into_serde_error("Failed to deserialize job parameters")?;

        // Validate parameters
        params.validate()?;

        // Create typed JobContext with validated parameters
        let ctx = JobContext::new(app_ctx, job.job_id.as_str().to_string(), params);

        // Call type-safe execute method
        self.execute(&ctx).await
    }

    async fn execute_local_dyn(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError> {
        // Deserialize parameters from JSON string
        let params_json = job
            .parameters
            .as_ref()
            .ok_or_else(|| KalamDbError::InvalidOperation("Missing job parameters".to_string()))?;

        let params: T = serde_json::from_str(params_json)
            .into_serde_error("Failed to deserialize job parameters")?;

        // Validate parameters
        params.validate()?;

        // Create typed JobContext with validated parameters
        let ctx = JobContext::new(app_ctx, job.job_id.as_str().to_string(), params);

        // Call type-safe execute_local method
        self.execute_local(&ctx).await
    }

    async fn execute_leader_dyn(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError> {
        // Deserialize parameters from JSON string
        let params_json = job
            .parameters
            .as_ref()
            .ok_or_else(|| KalamDbError::InvalidOperation("Missing job parameters".to_string()))?;

        let params: T = serde_json::from_str(params_json)
            .into_serde_error("Failed to deserialize job parameters")?;

        // Validate parameters
        params.validate()?;

        // Create typed JobContext with validated parameters
        let ctx = JobContext::new(app_ctx, job.job_id.as_str().to_string(), params);

        // Call type-safe execute_leader method
        self.execute_leader(&ctx).await
    }

    async fn cancel_dyn(&self, app_ctx: Arc<AppContext>, job: &Job) -> Result<(), KalamDbError> {
        // For cancellation, we still need to deserialize params to create context
        let params_json = job
            .parameters
            .as_ref()
            .ok_or_else(|| KalamDbError::InvalidOperation("Missing job parameters".to_string()))?;

        let params: T = serde_json::from_str(params_json)
            .into_serde_error("Failed to deserialize job parameters")?;

        // No validation needed for cancellation
        let ctx = JobContext::new(app_ctx, job.job_id.as_str().to_string(), params);

        // Call type-safe cancel method
        self.cancel(&ctx).await
    }
}

/// Registry of job executors
///
/// Thread-safe registry that maps job types to their executor implementations.
/// Uses an RwLock<HashMap> since registration happens at startup.
///
/// Stores type-erased `Arc<dyn DynJobExecutor>` internally but provides
/// type-safe registration through `register<E: JobExecutor>()`.
///
/// # Example
///
/// ```no_run
/// use kalamdb_core::jobs::executors::{JobRegistry, flush::FlushExecutor};
/// use std::sync::Arc;
///
/// let registry = JobRegistry::new();
///
/// // Register executors (type-safe at registration)
/// registry.register(Arc::new(FlushExecutor::new()));
///
/// // Execution is type-safe through DynJobExecutor bridge
/// ```
pub struct JobRegistry {
    executors: RwLock<HashMap<JobType, Arc<dyn DynJobExecutor>>>,
}

impl JobRegistry {
    /// Create a new empty job registry
    pub fn new() -> Self {
        Self {
            executors: RwLock::new(HashMap::new()),
        }
    }

    /// Register a job executor
    ///
    /// # Arguments
    /// * `executor` - Job executor implementation
    ///
    /// # Panics
    /// Panics if an executor for this job type is already registered
    pub fn register<E>(&self, executor: Arc<E>)
    where
        E: JobExecutor + 'static,
    {
        let job_type = executor.job_type();
        let mut executors = self.executors.write();
        if executors.contains_key(&job_type) {
            panic!("Executor for job type {:?} is already registered", job_type);
        }
        // Coerce to Arc<dyn DynJobExecutor> through the blanket impl
        executors.insert(job_type, executor);
    }

    /// Register a job executor, replacing any existing one
    ///
    /// # Arguments
    /// * `executor` - Job executor implementation
    ///
    /// Returns the previous executor if one was registered
    #[cfg(test)]
    pub(crate) fn register_or_replace<E>(&self, executor: Arc<E>) -> Option<Arc<dyn DynJobExecutor>>
    where
        E: JobExecutor + 'static,
    {
        let job_type = executor.job_type();
        let mut executors = self.executors.write();
        executors.insert(job_type, executor)
    }

    /// Pre-validate job parameters before job creation
    ///
    /// Calls the executor's pre_validate method to determine if the job
    /// should be created. Returns Ok(true) to proceed, Ok(false) to skip.
    ///
    /// # Arguments
    /// * `app_ctx` - Application context
    /// * `job_type` - Type of job to validate
    /// * `params_json` - JSON-serialized job parameters
    ///
    /// # Errors
    /// Returns error if:
    /// - No executor registered for job type
    /// - Parameter deserialization fails
    /// - Validation fails
    pub async fn pre_validate(
        &self,
        app_ctx: &Arc<AppContext>,
        job_type: &JobType,
        params_json: &str,
    ) -> Result<bool, KalamDbError> {
        let executor = {
            let executors = self.executors.read();
            executors.get(job_type).cloned()
        }
        .ok_or_else(|| {
            KalamDbError::NotFound(format!("No executor registered for job type: {:?}", job_type))
        })?;

        executor.pre_validate_dyn(app_ctx, params_json).await
    }

    /// Execute a job using the registered executor
    ///
    /// Looks up the executor by job type, deserializes parameters,
    /// and delegates to the type-safe execute method.
    ///
    /// # Arguments
    /// * `app_ctx` - Application context
    /// * `job` - Job to execute
    ///
    /// # Errors
    /// Returns error if:
    /// - No executor registered for job type
    /// - Parameter deserialization fails
    /// - Parameter validation fails
    /// - Execution fails
    pub async fn execute(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError> {
        let executor = {
            let executors = self.executors.read();
            executors.get(&job.job_type).cloned()
        }
        .ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "No executor registered for job type: {:?}",
                job.job_type
            ))
        })?;

        executor.execute_dyn(app_ctx, job).await
    }

    /// Execute local work phase of a job (runs on ALL nodes)
    ///
    /// Looks up the executor by job type and executes the local work phase.
    /// This is called on every node in the cluster, not just the leader.
    ///
    /// # Arguments
    /// * `app_ctx` - Application context
    /// * `job` - Job to execute local phase for
    ///
    /// # Errors
    /// Returns error if:
    /// - No executor registered for job type
    /// - Parameter deserialization fails
    /// - Execution fails
    pub async fn execute_local(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError> {
        let executor = {
            let executors = self.executors.read();
            executors.get(&job.job_type).cloned()
        }
        .ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "No executor registered for job type: {:?}",
                job.job_type
            ))
        })?;

        executor.execute_local_dyn(app_ctx, job).await
    }

    /// Execute leader-only phase of a job (runs ONLY on leader)
    ///
    /// Looks up the executor by job type and executes the leader-only phase.
    /// This should only be called on the leader node for jobs that have
    /// leader actions (`job.job_type.has_leader_actions() == true`).
    ///
    /// # Arguments
    /// * `app_ctx` - Application context
    /// * `job` - Job to execute leader phase for
    ///
    /// # Errors
    /// Returns error if:
    /// - No executor registered for job type
    /// - Parameter deserialization fails
    /// - Execution fails
    pub async fn execute_leader(
        &self,
        app_ctx: Arc<AppContext>,
        job: &Job,
    ) -> Result<JobDecision, KalamDbError> {
        let executor = {
            let executors = self.executors.read();
            executors.get(&job.job_type).cloned()
        }
        .ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "No executor registered for job type: {:?}",
                job.job_type
            ))
        })?;

        executor.execute_leader_dyn(app_ctx, job).await
    }

    /// Cancel a running job using the registered executor
    ///
    /// # Arguments
    /// * `app_ctx` - Application context
    /// * `job` - Job to cancel
    ///
    /// # Errors
    /// Returns error if:
    /// - No executor registered for job type
    /// - Cancellation fails
    pub async fn cancel(&self, app_ctx: Arc<AppContext>, job: &Job) -> Result<(), KalamDbError> {
        let executor = {
            let executors = self.executors.read();
            executors.get(&job.job_type).cloned()
        }
        .ok_or_else(|| {
            KalamDbError::NotFound(format!(
                "No executor registered for job type: {:?}",
                job.job_type
            ))
        })?;

        executor.cancel_dyn(app_ctx, job).await
    }

    /// Check if an executor is registered for a job type
    pub fn contains(&self, job_type: &JobType) -> bool {
        let executors = self.executors.read();
        executors.contains_key(job_type)
    }

    /// Get the number of registered executors
    pub fn len(&self) -> usize {
        let executors = self.executors.read();
        executors.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        let executors = self.executors.read();
        executors.is_empty()
    }

    /// List all registered job types
    pub fn job_types(&self) -> Vec<JobType> {
        let executors = self.executors.read();
        executors.keys().cloned().collect()
    }

    /// Get executor name for a job type
    pub fn executor_name(&self, job_type: &JobType) -> Option<&'static str> {
        let executors = self.executors.read();
        executors.get(job_type).map(|e| e.name())
    }

    /// Clear all registered executors
    pub fn clear(&self) {
        let mut executors = self.executors.write();
        executors.clear();
    }
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::JobParams;
    use kalamdb_core::test_helpers::test_app_context_simple;
    use kalamdb_commons::models::{JobId, NodeId};
    use kalamdb_system::JobStatus;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    struct MockParams {
        value: i32,
    }

    impl JobParams for MockParams {}

    struct MockExecutor {
        job_type: JobType,
    }

    #[async_trait]
    impl JobExecutor for MockExecutor {
        type Params = MockParams;

        fn job_type(&self) -> JobType {
            self.job_type
        }

        fn name(&self) -> &'static str {
            "MockExecutor"
        }

        async fn execute(
            &self,
            _ctx: &JobContext<Self::Params>,
        ) -> Result<JobDecision, KalamDbError> {
            Ok(JobDecision::Completed { message: None })
        }
    }

    fn make_test_job(job_type: JobType, params: &str) -> Job {
        let now = chrono::Utc::now().timestamp_millis();
        Job {
            job_id: JobId::new("TEST-001"),
            job_type,
            status: JobStatus::Running,
            leader_status: None,
            parameters: Some(params.to_string()),
            message: None,
            exception_trace: None,
            idempotency_key: None,
            retry_count: 0,
            max_retries: 3,
            memory_used: None,
            cpu_used: None,
            created_at: now,
            updated_at: now,
            started_at: Some(now),
            finished_at: None,
            node_id: NodeId::default_node(),
            leader_node_id: None,
            queue: None,
            priority: None,
        }
    }

    #[test]
    fn test_registry_register_and_contains() {
        let registry = JobRegistry::new();
        let executor = Arc::new(MockExecutor {
            job_type: JobType::Flush,
        });

        registry.register(executor);

        assert!(registry.contains(&JobType::Flush));
        assert!(!registry.contains(&JobType::Cleanup));
    }

    #[test]
    #[should_panic(expected = "already registered")]
    fn test_registry_duplicate_registration() {
        let registry = JobRegistry::new();
        let executor1 = Arc::new(MockExecutor {
            job_type: JobType::Flush,
        });
        let executor2 = Arc::new(MockExecutor {
            job_type: JobType::Flush,
        });

        registry.register(executor1);
        registry.register(executor2); // Should panic
    }

    #[test]
    fn test_registry_replace() {
        let registry = JobRegistry::new();
        let executor1 = Arc::new(MockExecutor {
            job_type: JobType::Flush,
        });
        let executor2 = Arc::new(MockExecutor {
            job_type: JobType::Flush,
        });

        let old = registry.register_or_replace(executor1);
        assert!(old.is_none());

        let old = registry.register_or_replace(executor2);
        assert!(old.is_some());
    }

    #[tokio::test]
    async fn test_registry_execute() {
        let app_ctx = test_app_context_simple();

        let registry = JobRegistry::new();
        let executor = Arc::new(MockExecutor {
            job_type: JobType::Flush,
        });
        registry.register(executor);

        let job = make_test_job(JobType::Flush, r#"{"value": 42}"#);
        let result = registry.execute(app_ctx, &job).await;

        assert!(result.is_ok());
        match result.unwrap() {
            JobDecision::Completed { .. } => {},
            _ => panic!("Expected Completed decision"),
        }
    }

    #[tokio::test]
    async fn test_registry_execute_not_found() {
        let app_ctx = test_app_context_simple();

        let registry = JobRegistry::new();
        let job = make_test_job(JobType::Flush, r#"{"value": 42}"#);

        let result = registry.execute(app_ctx, &job).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No executor registered"));
    }

    #[test]
    fn test_registry_len_and_empty() {
        let registry = JobRegistry::new();
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());

        let executor = Arc::new(MockExecutor {
            job_type: JobType::Flush,
        });
        registry.register(executor);

        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
    }

    #[test]
    fn test_registry_job_types() {
        let registry = JobRegistry::new();

        let types = registry.job_types();
        assert_eq!(types.len(), 0);

        registry.register(Arc::new(MockExecutor {
            job_type: JobType::Flush,
        }));
        registry.register(Arc::new(MockExecutor {
            job_type: JobType::Cleanup,
        }));

        let types = registry.job_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&JobType::Flush));
        assert!(types.contains(&JobType::Cleanup));
    }

    #[test]
    fn test_registry_executor_name() {
        let registry = JobRegistry::new();

        assert!(registry.executor_name(&JobType::Flush).is_none());

        registry.register(Arc::new(MockExecutor {
            job_type: JobType::Flush,
        }));

        let name = registry.executor_name(&JobType::Flush);
        assert_eq!(name, Some("MockExecutor"));
    }

    #[test]
    fn test_registry_clear() {
        let registry = JobRegistry::new();

        registry.register(Arc::new(MockExecutor {
            job_type: JobType::Flush,
        }));
        assert_eq!(registry.len(), 1);

        registry.clear();
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
    }
}
