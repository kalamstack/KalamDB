//! KILL LIVE QUERY statement parser
//!
//! Parses SQL statements like:
//! - KILL LIVE QUERY 'user123-conn_abc-messages-updatedMessages'

use kalamdb_commons::models::{ConnectionId, LiveQueryId, UserId};

use crate::ddl::DdlResult;

/// KILL LIVE QUERY statement
#[derive(Debug, Clone, PartialEq)]
pub struct KillLiveQueryStatement {
    /// Live query ID to kill
    pub live_id: LiveQueryId,
}

impl KillLiveQueryStatement {
    /// Parse a KILL LIVE QUERY statement from SQL
    ///
    /// Supports syntax:
    /// - KILL LIVE QUERY 'live_id'
    /// - KILL LIVE QUERY "live_id"
    /// - KILL LIVE QUERY live_id
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let sql_upper = sql.trim().to_uppercase();

        if !sql_upper.starts_with("KILL LIVE QUERY") {
            return Err("Expected KILL LIVE QUERY statement".to_string());
        }

        // Extract live_id (everything after "KILL LIVE QUERY")
        let live_id_str = sql
            .trim()
            .strip_prefix("KILL LIVE QUERY")
            .or_else(|| sql.trim().strip_prefix("kill live query"))
            .map(|s| s.trim())
            .ok_or_else(|| "Live query ID is required".to_string())?;

        // Remove quotes if present
        let live_id_str = live_id_str.trim_matches(|c| c == '\'' || c == '"' || c == '`').trim();

        if live_id_str.is_empty() {
            return Err("Live query ID cannot be empty".to_string());
        }

        // Parse live_id string to LiveId struct
        let live_id = LiveQueryId::from_string(live_id_str).map_err(|e| e.to_string())?;

        Ok(Self { live_id })
    }

    /// Get the user_id from the live_id
    pub fn user_id(&self) -> &UserId {
        self.live_id.user_id()
    }

    /// Get the connection_id from the live_id
    pub fn connection_id(&self) -> &ConnectionId {
        self.live_id.connection_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kill_live_query() {
        let stmt =
            KillLiveQueryStatement::parse("KILL LIVE QUERY 'user123-conn_abc-sub1'").unwrap();
        assert_eq!(stmt.live_id.user_id().as_str(), "user123");
        assert_eq!(stmt.live_id.connection_id().as_str(), "conn_abc");
        assert_eq!(stmt.live_id.subscription_id(), "sub1");
    }

    #[test]
    fn test_parse_kill_live_query_double_quotes() {
        let stmt =
            KillLiveQueryStatement::parse("KILL LIVE QUERY \"user456-conn_xyz-notifications\"")
                .unwrap();
        assert_eq!(stmt.live_id.user_id().as_str(), "user456");
        assert_eq!(stmt.live_id.subscription_id(), "notifications");
    }

    #[test]
    fn test_parse_kill_live_query_no_quotes() {
        let stmt = KillLiveQueryStatement::parse("KILL LIVE QUERY user789-conn_123-sub3").unwrap();
        assert_eq!(stmt.live_id.user_id().as_str(), "user789");
    }

    #[test]
    fn test_parse_kill_live_query_lowercase() {
        let stmt =
            KillLiveQueryStatement::parse("kill live query 'user123-conn_abc-sub1'").unwrap();
        assert_eq!(stmt.live_id.user_id().as_str(), "user123");
    }

    #[test]
    fn test_parse_kill_live_query_invalid_format() {
        let result = KillLiveQueryStatement::parse("KILL LIVE QUERY 'invalid-format'");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_kill_live_query_empty() {
        let result = KillLiveQueryStatement::parse("KILL LIVE QUERY ''");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_kill_live_query_missing_id() {
        let result = KillLiveQueryStatement::parse("KILL LIVE QUERY");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_statement() {
        let result = KillLiveQueryStatement::parse("DROP TABLE users");
        assert!(result.is_err());
    }

    #[test]
    fn test_user_id_helper() {
        let stmt =
            KillLiveQueryStatement::parse("KILL LIVE QUERY 'user123-conn_abc-sub1'").unwrap();
        assert_eq!(stmt.user_id().as_str(), "user123");
    }

    #[test]
    fn test_connection_id_helper() {
        let stmt =
            KillLiveQueryStatement::parse("KILL LIVE QUERY 'user123-conn_abc-sub1'").unwrap();
        assert_eq!(stmt.connection_id().as_str(), "conn_abc");
    }
}
