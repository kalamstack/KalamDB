use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Enum representing job execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    New,
    Queued,
    Running,
    Retrying,
    Completed,
    Failed,
    Cancelled,
    Skipped,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::New => "new",
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Retrying => "retrying",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
            JobStatus::Skipped => "skipped",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "new" => Some(JobStatus::New),
            "queued" => Some(JobStatus::Queued),
            "running" => Some(JobStatus::Running),
            "retrying" => Some(JobStatus::Retrying),
            "completed" => Some(JobStatus::Completed),
            "failed" => Some(JobStatus::Failed),
            "cancelled" => Some(JobStatus::Cancelled),
            "skipped" => Some(JobStatus::Skipped),
            _ => None,
        }
    }
}

impl FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JobStatus::from_str_opt(s).ok_or_else(|| format!("Invalid JobStatus: {}", s))
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for JobStatus {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "new" => JobStatus::New,
            "queued" => JobStatus::Queued,
            "running" => JobStatus::Running,
            "retrying" => JobStatus::Retrying,
            "completed" => JobStatus::Completed,
            "failed" => JobStatus::Failed,
            "cancelled" => JobStatus::Cancelled,
            "skipped" => JobStatus::Skipped,
            _ => JobStatus::Failed,
        }
    }
}

impl From<String> for JobStatus {
    fn from(s: String) -> Self {
        JobStatus::from(s.as_str())
    }
}
