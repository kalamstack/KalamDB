//! Manifest Eviction Job Executor
//!
//! JobExecutor implementation for manifest cache eviction based on last_accessed timestamp.
//!
//! ## Responsibilities
//! - Evict stale manifest entries from both hot cache (RAM) and RocksDB
//! - Check last_accessed timestamp against configurable TTL
//! - Track eviction metrics (entries evicted)
//!
//! ## Parameters Format
//! ```json
//! {
//!   "ttl_days": 7
//! }
//! ```
//!
//! ## Configuration
//! Configure in server.toml under [manifest_cache] section:
//! - eviction_interval_seconds: How often the job runs (default: 600s = 10 minutes)
//! - eviction_ttl_days: How many days before an unaccessed manifest is evicted (default: 7 days)

use crate::executors::{JobContext, JobDecision, JobExecutor, JobParams};
use async_trait::async_trait;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_system::JobType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

fn default_ttl_days() -> u64 {
    7
}

/// Typed parameters for manifest eviction operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEvictionParams {
    /// TTL in days - manifests not accessed for this many days will be evicted
    /// Default: 7 days
    #[serde(default = "default_ttl_days")]
    pub ttl_days: u64,
}

impl Default for ManifestEvictionParams {
    fn default() -> Self {
        Self {
            ttl_days: default_ttl_days(),
        }
    }
}

impl JobParams for ManifestEvictionParams {
    fn validate(&self) -> Result<(), KalamDbError> {
        // ttl_days = 0 means no eviction, which is valid
        Ok(())
    }
}

/// Manifest Eviction Job Executor
///
/// Evicts stale manifest entries based on last_accessed timestamp.
pub struct ManifestEvictionExecutor;

impl ManifestEvictionExecutor {
    /// Create a new ManifestEvictionExecutor
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl JobExecutor for ManifestEvictionExecutor {
    type Params = ManifestEvictionParams;

    fn job_type(&self) -> JobType {
        JobType::ManifestEviction
    }

    fn name(&self) -> &'static str {
        "ManifestEvictionExecutor"
    }

    async fn pre_validate(
        &self,
        _app_ctx: &Arc<AppContext>,
        params: &Self::Params,
    ) -> Result<bool, KalamDbError> {
        params.validate()?;

        // Skip if TTL is 0 (disabled)
        if params.ttl_days == 0 {
            return Ok(false);
        }

        Ok(true)
    }

    async fn execute(&self, ctx: &JobContext<Self::Params>) -> Result<JobDecision, KalamDbError> {
        ctx.log_info("Starting manifest eviction operation");

        let params = ctx.params();
        let ttl_days = params.ttl_days;

        // Convert days to seconds
        let ttl_seconds = (ttl_days * 24 * 60 * 60) as i64;

        ctx.log_info(&format!(
            "Evicting manifests not accessed for {} days ({} seconds)",
            ttl_days, ttl_seconds
        ));

        // Get manifest service from app context
        let manifest_service = ctx.app_ctx.manifest_service();

        // Evict stale entries
        let evicted_count = manifest_service.evict_stale_entries(ttl_seconds).map_err(|e| {
            KalamDbError::InvalidOperation(format!("Failed to evict manifests: {}", e))
        })?;

        ctx.log_info(&format!("Manifest eviction completed - {} entries evicted", evicted_count));

        Ok(JobDecision::Completed {
            message: Some(format!(
                "Evicted {} stale manifest entries (ttl_days={})",
                evicted_count, ttl_days
            )),
        })
    }

    async fn cancel(&self, ctx: &JobContext<Self::Params>) -> Result<(), KalamDbError> {
        ctx.log_warn("Manifest eviction job cancellation requested");
        // Allow cancellation since partial eviction is acceptable
        Ok(())
    }
}

impl Default for ManifestEvictionExecutor {
    fn default() -> Self {
        Self::new()
    }
}
