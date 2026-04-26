//! Job management SQL commands
//!
//! This module provides SQL command parsing and execution for job management
//! operations like KILL JOB.
//!
//! ## Supported Commands
//!
//! ### KILL JOB
//!
//! Cancels a running job by job_id:
//!
//! ```sql
//! KILL JOB 'flush-messages-123';
//! ```
//!
//! Returns error if:
//! - Job doesn't exist
//! - Job is already completed/failed/cancelled
//!
//! ## Examples
//!
//! ```rust,no_run
//! use kalamdb_dialect::ddl::job_commands::{parse_job_command, JobCommand};
//!
//! // Parse KILL JOB command
//! let cmd = parse_job_command("KILL JOB 'flush-001'").unwrap();
//! match cmd {
//!     JobCommand::Kill { job_id } => {
//!         println!("Cancelling job: {}", job_id);
//!     },
//! }
//! ```

use anyhow::{anyhow, Result};

/// Job management commands
#[derive(Debug, Clone, PartialEq)]
pub enum JobCommand {
    /// Kill (cancel) a running job
    Kill {
        /// Job ID to cancel
        job_id: String, // TODO: use JobId type?
    },
}

/// Parse a job management command
///
/// # Arguments
///
/// * `sql` - SQL command string (e.g., "KILL JOB 'job-123'")
///
/// # Returns
///
/// Parsed JobCommand or error if invalid syntax
///
/// # Examples
///
/// ```rust,no_run
/// use kalamdb_dialect::ddl::job_commands::parse_job_command;
///
/// let cmd = parse_job_command("KILL JOB 'flush-001'").unwrap();
/// ```
pub fn parse_job_command(sql: &str) -> Result<JobCommand> {
    use crate::parser::utils::normalize_sql;

    let normalized = normalize_sql(sql);
    let sql_upper = normalized.to_uppercase();

    // Check for KILL JOB command
    if sql_upper.starts_with("KILL JOB") {
        return parse_kill_job(&normalized);
    }

    Err(anyhow!("Unknown job command: {}", sql))
}

/// Parse KILL JOB command
///
/// Syntax: KILL JOB 'job_id' | KILL JOB "job_id" | KILL JOB job_id
fn parse_kill_job(sql: &str) -> Result<JobCommand> {
    // Remove "KILL JOB" prefix (handle variable whitespace)
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 3 {
        return Err(anyhow!("KILL JOB requires a job_id"));
    }

    let job_id_part = parts[2..].join(" ");

    if job_id_part.is_empty() {
        return Err(anyhow!("KILL JOB requires a job_id"));
    }

    // Extract job_id (handle quoted and unquoted)
    let job_id = if job_id_part.starts_with('\'') || job_id_part.starts_with('"') {
        // Quoted job_id
        let quote_char = job_id_part.chars().next().unwrap();
        let end_quote = job_id_part[1..]
            .find(quote_char)
            .ok_or_else(|| anyhow!("Unterminated string in KILL JOB command"))?;
        job_id_part[1..=end_quote].to_string()
    } else {
        // Unquoted job_id (take until whitespace or semicolon)
        job_id_part
            .split(|c: char| c.is_whitespace() || c == ';')
            .next()
            .unwrap_or("")
            .to_string()
    };

    if job_id.is_empty() {
        return Err(anyhow!("KILL JOB requires a non-empty job_id"));
    }

    Ok(JobCommand::Kill { job_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kill_job_single_quotes() {
        let cmd = parse_job_command("KILL JOB 'flush-001'").unwrap();
        assert_eq!(
            cmd,
            JobCommand::Kill {
                job_id: "flush-001".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_kill_job_double_quotes() {
        let cmd = parse_job_command("KILL JOB \"flush-001\"").unwrap();
        assert_eq!(
            cmd,
            JobCommand::Kill {
                job_id: "flush-001".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_kill_job_unquoted() {
        let cmd = parse_job_command("KILL JOB flush-001").unwrap();
        assert_eq!(
            cmd,
            JobCommand::Kill {
                job_id: "flush-001".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_kill_job_case_insensitive() {
        let cmd = parse_job_command("kill job 'flush-001'").unwrap();
        assert_eq!(
            cmd,
            JobCommand::Kill {
                job_id: "flush-001".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_kill_job_with_whitespace() {
        let cmd = parse_job_command("  KILL   JOB   'flush-001'  ").unwrap();
        assert_eq!(
            cmd,
            JobCommand::Kill {
                job_id: "flush-001".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_kill_job_with_semicolon() {
        let cmd = parse_job_command("KILL JOB 'flush-001';").unwrap();
        assert_eq!(
            cmd,
            JobCommand::Kill {
                job_id: "flush-001".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_kill_job_empty_id() {
        let result = parse_job_command("KILL JOB");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires a job_id"));
    }

    #[test]
    fn test_parse_kill_job_unterminated_quote() {
        let result = parse_job_command("KILL JOB 'flush-001");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unterminated string"));
    }

    #[test]
    fn test_parse_unknown_command() {
        let result = parse_job_command("CANCEL JOB 'flush-001'");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown job command"));
    }
}
