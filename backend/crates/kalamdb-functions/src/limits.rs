//! Isolate limits for a function invocation.

use std::time::Duration;

/// Memory, deadline, and ABI settings for one loaded revision.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeLimits {
    pub timeout:        Duration,
    pub max_heap_bytes: usize,
    pub abi_version:    u32,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            timeout:        Duration::from_millis(5_000),
            max_heap_bytes: 64 * 1024 * 1024,
            abi_version:    ABI_VERSION,
        }
    }
}

/// Host ABI version accepted by this runtime.
pub const ABI_VERSION: u32 = 1;
