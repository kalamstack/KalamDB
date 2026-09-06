//! Admission and memory budgets shared by the runtime adapters.

use std::time::Duration;

use crate::{FunctionsError, Result};

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub workers:            usize,
    pub max_active:         usize,
    pub nested_reserve:     usize,
    pub max_queued:         usize,
    pub max_depth:          usize,
    pub max_heap_bytes:     usize,
    pub max_artifact_bytes: usize,
    pub max_value_bytes:    usize,
    pub cache_bytes:        u64,
    pub timeout:            Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            workers:            std::thread::available_parallelism()
                .map_or(1, |n| (n.get() / 2).max(1)),
            max_active:         32,
            nested_reserve:     8,
            max_queued:         256,
            max_depth:          16,
            max_heap_bytes:     64 * 1024 * 1024,
            max_artifact_bytes: 16 * 1024 * 1024,
            max_value_bytes:    8 * 1024 * 1024,
            cache_bytes:        128 * 1024 * 1024,
            timeout:            Duration::from_secs(5),
        }
    }
}

impl EngineConfig {
    pub fn validate(&self) -> Result<()> {
        if self.workers == 0
            || self.max_active <= self.nested_reserve
            || self.max_depth == 0
            || self.max_heap_bytes == 0
            || self.max_artifact_bytes == 0
            || self.max_value_bytes == 0
            || self.timeout.is_zero()
        {
            return Err(FunctionsError::Invalid("invalid function engine budgets".into()));
        }
        Ok(())
    }
}
