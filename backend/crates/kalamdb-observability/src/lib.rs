//! Health monitoring and observability utilities for KalamDB.
//!
//! This crate provides lightweight health monitoring capabilities:
//! - System resource metrics (CPU, memory, open files)
//! - CPU detection and monitoring
//! - WebSocket session tracking
//! - Health metrics collection and reporting
//! - Runtime metrics collection (uptime, memory, CPU, threads)
//!
//! This crate is intentionally minimal to reduce compilation dependencies
//! for the core kalamdb-core crate.

pub mod activity;
pub mod allocator_metrics;
pub mod cpu;
pub mod health_monitor;
pub mod runtime_metrics;

pub use activity::{idle_duration, last_activity_ms, record_activity_now};
pub use allocator_metrics::{
    collect_allocator_metrics, force_allocator_collection, AllocatorMetrics,
};
pub use cpu::{get_cpu_count, get_physical_cpu_count};
pub use health_monitor::{
    decrement_websocket_sessions, get_websocket_session_count, get_websocket_session_peak_count,
    increment_websocket_sessions, HealthCounts, HealthMetrics, HealthMonitor,
};
pub use runtime_metrics::{
    collect_runtime_metrics, RuntimeMetrics, BUILD_DATE, GIT_BRANCH, GIT_COMMIT_HASH,
    SERVER_VERSION,
};
