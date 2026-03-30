//! Lightweight trait for awakening jobs from within kalamdb-core (e.g., the Raft applier).
//!
//! The concrete `JobsManager` implementation lives in `kalamdb-jobs`; this trait
//! allows kalamdb-core to notify it without a direct dependency.

use kalamdb_commons::JobId;

/// Minimal interface so kalamdb-core can wake up a job without depending on
/// kalamdb-jobs.
pub trait JobWaker: Send + Sync {
    fn awake_job(&self, job_id: JobId);
}
