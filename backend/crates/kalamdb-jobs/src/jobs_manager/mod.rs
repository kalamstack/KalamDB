//! Unified Job Management System
//!
//! **Phase 9 (US6)**: Single JobsManager with typed JobIds, richer statuses, idempotency, retry/backoff, dedicated logging
//!
//! This module provides a centralized job management system with:
//! - Typed JobIds with prefixes (FL, CL, RT, SE, UC, CO, BK, RS)
//! - Rich job status tracking (New, Queued, Running, Completed, Failed, Retrying, Cancelled)
//! - Idempotency enforcement (prevent duplicate jobs)
//! - Automatic retry with exponential backoff
//! - Dedicated jobs.log file for job-specific logging
//! - Crash recovery (mark incomplete jobs as failed on restart)
//! - Type-safe job creation with JobParams trait
//!
//! ## Architecture
//!
//! ```text
//! JobsManager
//! ├── JobsTableProvider    (persistence via system.jobs table)
//! ├── JobRegistry         (dispatches to JobExecutor implementations)
//! └── jobs.log            (dedicated log file with [JobId] prefix)
//! ```

mod actions;
mod queries;
mod runner;
mod types;
mod utils;

pub use types::JobsManager;
