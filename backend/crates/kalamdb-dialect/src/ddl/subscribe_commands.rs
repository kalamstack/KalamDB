//! SUBSCRIBE TO command parser for live query subscriptions.
//!
//! **Purpose**: Enable SQL-based syntax for creating live query subscriptions via WebSocket.
//!
//! **Syntax**:
//! ```sql
//! SUBSCRIBE TO [namespace.]table_name [WHERE condition] [OPTIONS (...)];
//! ```
//!
//! **Supported Options**:
//! - `last_rows=N` - Number of recent rows to fetch initially (default: fetch all)
//! - `batch_size=N` - Hint for server-side batch sizing during initial data load
//! - `from=N` - Resume subscription from a specific sequence ID
//!
//! **Examples**:
//! ```sql
//! -- Basic subscription
//! SUBSCRIBE TO app.messages;
//!
//! -- With WHERE clause filter
//! SUBSCRIBE TO app.messages WHERE user_id = CURRENT_USER();
//!
//! -- With initial data fetch (last 10 rows)
//! SUBSCRIBE TO app.messages WHERE user_id = CURRENT_USER() OPTIONS (last_rows=10);
//!
//! -- With multiple options
//! SUBSCRIBE TO app.messages OPTIONS (last_rows=50, batch_size=50);
//!
//! -- Resume from specific sequence ID
//! SUBSCRIBE TO app.messages OPTIONS (from=12345);
//!
//! -- Shared table subscription
//! SUBSCRIBE TO shared.announcements WHERE priority > 5;
//! ```
//!
//! **Integration**:
//! When executed via /api/sql endpoint, this command returns metadata instructing
//! the client to establish a WebSocket connection with the appropriate subscription message.
//!
//! **Response Format**:
//! ```json
//! {
//!   "status": "subscription_required",
//!   "ws_url": "ws://localhost:2900/ws",
//!   "subscription": {
//!     "id": "auto-generated-id",
//!     "sql": "SELECT * FROM app.messages WHERE user_id = CURRENT_USER()",
//!     "options": {"last_rows": 10, "batch_size": 50}
//!   }
//! }
//! ```

use kalamdb_commons::{websocket::SubscriptionOptions, NamespaceId, TableName};
use sqlparser::{
    ast::{ObjectName, ObjectNamePart, SetExpr, Statement, TableFactor},
    dialect::{GenericDialect, PostgreSqlDialect},
};

use super::DdlResult;
use crate::parser::{
    query_parser::QueryParser,
    utils::{normalize_context_keyword_calls_for_sqlparser, parse_sql_statements},
};

/// SUBSCRIBE TO statement for live query subscriptions.
///
/// This command initiates a live query subscription via WebSocket.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribeStatement {
    /// Full SELECT query (e.g., "SELECT event_type FROM app.messages WHERE user_id = 'alice'")
    pub select_query: String,
    /// Namespace name (e.g., "app") - extracted from query
    pub namespace:    NamespaceId,
    /// Table name (e.g., "messages") - extracted from query
    pub table_name:   TableName,
    /// Optional subscription options (e.g., last_rows=10, batch_size=100, from=123)
    pub options:      SubscriptionOptions,
}

impl SubscribeStatement {
    /// Parse SUBSCRIBE TO command from SQL string using sqlparser-rs.
    ///
    /// # Supported Formats
    ///
    /// 1. `SUBSCRIBE TO namespace.table [WHERE ...] [OPTIONS (...)]`
    ///    - Expands to: `SELECT * FROM namespace.table [WHERE ...]`
    ///
    /// 2. `SUBSCRIBE TO SELECT columns FROM namespace.table [WHERE ...] [OPTIONS (...)]`
    ///    - Uses custom SELECT query as-is
    ///
    /// # Examples
    ///
    /// ```
    /// use kalamdb_dialect::ddl::subscribe_commands::SubscribeStatement;
    ///
    /// // Basic subscription (SELECT * FROM)
    /// let stmt = SubscribeStatement::parse("SUBSCRIBE TO app.messages").unwrap();
    /// assert_eq!(stmt.select_query, "SELECT * FROM app.messages");
    ///
    /// // Custom column selection
    /// let stmt =
    ///     SubscribeStatement::parse("SUBSCRIBE TO SELECT event_type FROM app.messages").unwrap();
    /// assert_eq!(stmt.select_query, "SELECT event_type FROM app.messages");
    /// ```
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let sql = sql.trim().trim_end_matches(';').trim();

        let Some(subscribe_body) = Self::strip_subscribe_to_prefix(sql) else {
            return Err("Expected 'SUBSCRIBE TO' command".to_string());
        };

        // Extract OPTIONS clause first
        let (sql_without_options, options) = Self::extract_options_clause(subscribe_body)?;

        // Check if user provided custom SELECT query
        let select_sql = if Self::starts_with_keyword(&sql_without_options, "SELECT") {
            // Format: SUBSCRIBE TO SELECT columns FROM table [WHERE ...]
            sql_without_options
        } else {
            // Format: SUBSCRIBE TO table [WHERE ...]
            // Convert to SELECT * FROM table [WHERE ...]
            let mut select_sql =
                String::with_capacity("SELECT * FROM ".len() + sql_without_options.len());
            select_sql.push_str("SELECT * FROM ");
            select_sql.push_str(&sql_without_options);
            select_sql
        };

        let select_sql = match normalize_context_keyword_calls_for_sqlparser(&select_sql) {
            std::borrow::Cow::Borrowed(_) => select_sql,
            std::borrow::Cow::Owned(normalized) => normalized,
        };

        // Parse the SELECT statement using sqlparser
        let dialect = PostgreSqlDialect {};
        let mut ast = match parse_sql_statements(&select_sql, &dialect) {
            Ok(ast) => ast,
            Err(e) => return Err(format!("Failed to parse SUBSCRIBE TO as SELECT: {}", e)),
        };

        if ast.len() != 1 {
            return Err("Expected exactly one SUBSCRIBE TO statement".to_string());
        }

        let statement = ast.remove(0);
        let Statement::Query(query) = statement else {
            return Err("SUBSCRIBE TO must parse as SELECT query".to_string());
        };

        QueryParser::analyze_subscription_query_ast(&query)
            .map_err(|e| format!("Invalid subscription query: {}", e))?;

        // Extract table name from FROM clause
        let SetExpr::Select(select_box) = *query.body else {
            return Err("SUBSCRIBE TO requires simple SELECT structure".to_string());
        };

        if select_box.from.len() != 1 {
            return Err("SUBSCRIBE TO requires exactly one table".to_string());
        }

        let table_factor = &select_box.from[0].relation;
        let TableFactor::Table { name, .. } = table_factor else {
            return Err("SUBSCRIBE TO requires direct table reference".to_string());
        };

        let (namespace, table_name) =
            Self::extract_namespace_table(name, &NamespaceId::default_ns())?;

        Ok(SubscribeStatement {
            select_query: select_sql,
            namespace,
            table_name,
            options,
        })
    }

    /// Extract OPTIONS clause from SUBSCRIBE TO SQL, return modified SQL and parsed options.
    ///
    /// Uses sqlparser tokenizer to find OPTIONS keyword, avoiding false matches in strings.
    fn extract_options_clause(sql: &str) -> DdlResult<(String, SubscriptionOptions)> {
        use sqlparser::tokenizer::{Token, Tokenizer};

        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);

        // Tokenize to get list of tokens
        let tokens = tokenizer
            .tokenize()
            .map_err(|e| format!("Failed to tokenize SUBSCRIBE TO: {}", e))?;

        let mut saw_options = false;
        let mut has_options_clause = false;
        for token in tokens.iter().filter(|token| !matches!(token, Token::Whitespace(_))) {
            if saw_options {
                has_options_clause = matches!(token, Token::LParen);
                break;
            }
            saw_options =
                matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case("OPTIONS"));
        }

        if saw_options && !has_options_clause {
            return Err("OPTIONS clause must be wrapped in parentheses, e.g., OPTIONS \
                        (last_rows=10)"
                .to_string());
        }

        if !has_options_clause {
            return Ok((sql.to_string(), SubscriptionOptions::default()));
        }

        // Find actual OPTIONS keyword position in SQL (case-insensitive)
        let sql_upper = sql.to_uppercase();
        let options_idx = sql_upper
            .rfind(" OPTIONS ")
            .or_else(|| sql_upper.rfind(" OPTIONS("))
            .ok_or_else(|| "OPTIONS keyword not found in SQL".to_string())?;

        // Split SQL at OPTIONS
        let before_options = sql[..options_idx].trim();
        let after_options_start = options_idx + " OPTIONS".len(); // " OPTIONS".len() == 8
        let after_options = sql[after_options_start..].trim();

        // Parse OPTIONS (don't modify SQL here, will be processed later)
        let options = parse_subscribe_options(after_options)?;

        Ok((before_options.to_string(), options))
    }

    fn strip_subscribe_to_prefix(sql: &str) -> Option<&str> {
        let trimmed = sql.trim_start();
        let prefix = "SUBSCRIBE TO";
        let head = trimmed.get(..prefix.len())?;
        if !head.eq_ignore_ascii_case(prefix) {
            return None;
        }

        let rest = trimmed[prefix.len()..].trim_start();
        if rest.is_empty() {
            None
        } else {
            Some(rest)
        }
    }

    fn starts_with_keyword(sql: &str, keyword: &str) -> bool {
        let trimmed = sql.trim_start();
        let Some(head) = trimmed.get(..keyword.len()) else {
            return false;
        };
        if !head.eq_ignore_ascii_case(keyword) {
            return false;
        }

        trimmed[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
    }

    fn extract_namespace_table(
        name: &ObjectName,
        default_namespace: &NamespaceId,
    ) -> DdlResult<(NamespaceId, TableName)> {
        let parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|part| match part {
                ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                _ => None,
            })
            .collect();

        if parts.len() == 2 {
            Ok((NamespaceId::new(parts[0].as_str()), TableName::from(parts[1].clone())))
        } else if parts.len() == 1 {
            Ok((default_namespace.clone(), TableName::from(parts[0].clone())))
        } else {
            Err(format!("Invalid table reference: expected [namespace.]table, got {}", name))
        }
    }

    /// Get the SELECT query for execution.
    ///
    /// This returns the full SELECT statement that should be executed for the subscription.
    ///
    /// # Examples
    ///
    /// ```
    /// use kalamdb_dialect::ddl::subscribe_commands::SubscribeStatement;
    ///
    /// let stmt =
    ///     SubscribeStatement::parse("SUBSCRIBE TO app.messages WHERE user_id = 'alice'").unwrap();
    /// assert_eq!(stmt.to_select_sql(), "SELECT * FROM app.messages WHERE user_id = 'alice'");
    ///
    /// let stmt =
    ///     SubscribeStatement::parse("SUBSCRIBE TO SELECT event_type FROM app.messages").unwrap();
    /// assert_eq!(stmt.to_select_sql(), "SELECT event_type FROM app.messages");
    /// ```
    pub fn to_select_sql(&self) -> String {
        self.select_query.clone()
    }
}

/// Parse OPTIONS clause for SUBSCRIBE TO command.
///
/// Expected format: `(key=value, key=value, ...)`
///
/// Supported options:
/// - `last_rows=N` - Number of recent rows to fetch initially
/// - `batch_size=N` - Hint for server-side batch sizing during initial data load
/// - `from=N` - Resume subscription from a specific sequence ID
///
/// Unknown options are rejected with an error to catch typos early.
fn parse_subscribe_options(options_str: &str) -> DdlResult<SubscriptionOptions> {
    use kalamdb_commons::ids::SeqId;

    let options_str = options_str.trim();

    // Expect options wrapped in parentheses
    if !options_str.starts_with('(') || !options_str.ends_with(')') {
        return Err("OPTIONS clause must be wrapped in parentheses, e.g., OPTIONS (last_rows=10)"
            .to_string());
    }

    let inner = &options_str[1..options_str.len() - 1].trim();

    // Parse key=value pairs
    let mut batch_size = None;
    let mut last_rows = None;
    let mut from = None;

    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((key, value)) = part.split_once('=') {
            let key = key.trim().to_lowercase();
            let value = value.trim();

            match key.as_str() {
                "last_rows" => {
                    last_rows = Some(
                        value
                            .parse::<u32>()
                            .map_err(|_| format!("Invalid last_rows value: {}", value))?,
                    );
                },
                "batch_size" => {
                    batch_size = Some(
                        value
                            .parse::<usize>()
                            .map_err(|_| format!("Invalid batch_size value: {}", value))?,
                    );
                },
                "from" | "from_seq_id" => {
                    let seq_val = value
                        .parse::<i64>()
                        .map_err(|_| format!("Invalid from value: {}", value))?;
                    from = Some(SeqId::new(seq_val));
                },
                _ => {
                    return Err(format!(
                        "Unknown subscription option: '{}'. Valid options are: last_rows, \
                         batch_size, from",
                        key
                    ));
                },
            }
        } else {
            return Err(format!("Invalid option format: '{}'. Expected key=value", part));
        }
    }

    Ok(SubscriptionOptions {
        batch_size,
        last_rows,
        from,
        auto_fetch_batches: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_subscribe() {
        let stmt = SubscribeStatement::parse("SUBSCRIBE TO app.messages").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT * FROM app.messages");
        assert!(stmt.options.last_rows.is_none());
    }

    #[test]
    fn test_parse_subscribe_with_semicolon() {
        let stmt = SubscribeStatement::parse("SUBSCRIBE TO app.messages;").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
    }

    #[test]
    fn test_parse_subscribe_case_insensitive() {
        let stmt = SubscribeStatement::parse("subscribe to app.messages").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
    }

    #[test]
    fn test_parse_subscribe_with_where_clause() {
        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages WHERE user_id = 'alice'").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT * FROM app.messages WHERE user_id = 'alice'");
    }

    #[test]
    fn test_parse_subscribe_with_options() {
        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (last_rows=10)").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT * FROM app.messages");
        assert_eq!(stmt.options.last_rows, Some(10));
    }

    #[test]
    fn test_parse_subscribe_with_where_and_options() {
        let stmt = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE user_id = 'alice' OPTIONS (last_rows=20)",
        )
        .unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT * FROM app.messages WHERE user_id = 'alice'");
        assert_eq!(stmt.options.last_rows, Some(20));
    }

    #[test]
    fn test_parse_subscribe_unqualified_table() {
        // Unqualified table names should use "default" namespace
        let stmt = SubscribeStatement::parse("SUBSCRIBE TO messages").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("default"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT * FROM messages");
    }

    #[test]
    fn test_parse_subscribe_custom_columns() {
        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO SELECT event_type FROM app.messages").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT event_type FROM app.messages");
        assert!(stmt.options.last_rows.is_none());
    }

    #[test]
    fn test_parse_subscribe_custom_columns_with_where() {
        let stmt = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT event_type, user_id FROM app.messages WHERE conversation_id = 1",
        )
        .unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(
            stmt.select_query,
            "SELECT event_type, user_id FROM app.messages WHERE conversation_id = 1"
        );
    }

    #[test]
    fn test_parse_subscribe_custom_columns_with_options() {
        let stmt = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT event_type FROM chat.typing_events OPTIONS (last_rows=20)",
        )
        .unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("chat"));
        assert_eq!(stmt.table_name, TableName::from("typing_events"));
        assert_eq!(stmt.select_query, "SELECT event_type FROM chat.typing_events");
        assert_eq!(stmt.options.last_rows, Some(20));
    }

    #[test]
    fn test_parse_subscribe_invalid_syntax() {
        let result = SubscribeStatement::parse("SUBSCRIBE messages");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected 'SUBSCRIBE TO'"));
    }

    #[test]
    fn test_parse_subscribe_invalid_options() {
        let result = SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS last_rows=10");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("parentheses"));
    }

    #[test]
    fn test_parse_subscribe_invalid_option_value() {
        let result = SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (last_rows=abc)");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid last_rows value"));
    }

    #[test]
    fn test_to_select_sql_basic() {
        let stmt = SubscribeStatement::parse("SUBSCRIBE TO app.messages").unwrap();
        assert_eq!(stmt.to_select_sql(), "SELECT * FROM app.messages");
    }

    #[test]
    fn test_to_select_sql_with_where() {
        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages WHERE user_id = 'alice'").unwrap();
        assert_eq!(stmt.to_select_sql(), "SELECT * FROM app.messages WHERE user_id = 'alice'");
    }

    #[test]
    fn test_to_select_sql_custom_columns() {
        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO SELECT event_type FROM app.messages").unwrap();
        assert_eq!(stmt.to_select_sql(), "SELECT event_type FROM app.messages");
    }

    #[test]
    fn test_parse_subscribe_malformed_table_reference() {
        // This now works because we support custom SELECT queries!
        // "SUBSCRIBE TO select * from admin_ops_test.users" is interpreted as:
        // "SUBSCRIBE TO SELECT * FROM admin_ops_test.users"
        let result = SubscribeStatement::parse("SUBSCRIBE TO select * from admin_ops_test.users");
        assert!(result.is_ok());
        let stmt = result.unwrap();
        assert_eq!(stmt.select_query, "select * from admin_ops_test.users");
    }

    #[test]
    fn test_parse_subscribe_table_reference_with_spaces() {
        let result = SubscribeStatement::parse("SUBSCRIBE TO my table");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("table aliases"));
    }

    #[test]
    fn test_parse_subscribe_ignores_options_keyword_in_string_literal() {
        let stmt = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE body = 'OPTIONS (last_rows=10)'",
        )
        .unwrap();

        assert_eq!(
            stmt.select_query,
            "SELECT * FROM app.messages WHERE body = 'OPTIONS (last_rows=10)'"
        );
        assert_eq!(stmt.options, SubscriptionOptions::default());
    }

    #[test]
    fn test_parse_subscribe_rejects_table_alias() {
        let result = SubscribeStatement::parse("SUBSCRIBE TO app.messages AS m WHERE m.id = 1");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("table aliases"));
    }

    #[test]
    fn test_parse_subscribe_rejects_computed_projection() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT CONCAT(user_id, '-x') FROM app.messages",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("direct column references"));
    }

    #[test]
    fn test_parse_subscribe_rejects_projection_alias() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO SELECT user_id AS actor FROM app.messages");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("direct column references"));
    }

    #[test]
    fn test_parse_subscribe_invalid_option_format() {
        let result = SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS invalid");
        assert!(result.is_err());
    }

    // ========================================================================
    // Tests for new subscription options: batch_size, from
    // ========================================================================

    #[test]
    fn test_parse_subscribe_with_batch_size() {
        let stmt = SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (batch_size=500)")
            .unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT * FROM app.messages");
        assert_eq!(stmt.options.batch_size, Some(500));
        assert!(stmt.options.last_rows.is_none());
        assert!(stmt.options.from.is_none());
    }

    #[test]
    fn test_parse_subscribe_with_from() {
        use kalamdb_commons::ids::SeqId;

        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (from=12345)").unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.options.from, Some(SeqId::new(12345)));
        assert!(stmt.options.batch_size.is_none());
        assert!(stmt.options.last_rows.is_none());
    }

    #[test]
    fn test_parse_subscribe_with_from_seq_id_alias() {
        use kalamdb_commons::ids::SeqId;

        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (from_seq_id=12345)")
                .unwrap();
        assert_eq!(stmt.options.from, Some(SeqId::new(12345)));
    }

    #[test]
    fn test_parse_subscribe_with_multiple_options() {
        use kalamdb_commons::ids::SeqId;

        let stmt = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages OPTIONS (last_rows=50, batch_size=50, from=999)",
        )
        .unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.options.last_rows, Some(50));
        assert_eq!(stmt.options.batch_size, Some(50));
        assert_eq!(stmt.options.from, Some(SeqId::new(999)));
    }

    #[test]
    fn test_parse_subscribe_with_where_and_multiple_options() {
        let stmt = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE user_id = 'alice' OPTIONS (last_rows=50, \
             batch_size=25)",
        )
        .unwrap();
        assert_eq!(stmt.namespace, NamespaceId::from("app"));
        assert_eq!(stmt.table_name, TableName::from("messages"));
        assert_eq!(stmt.select_query, "SELECT * FROM app.messages WHERE user_id = 'alice'");
        assert_eq!(stmt.options.last_rows, Some(50));
        assert_eq!(stmt.options.batch_size, Some(25));
    }

    #[test]
    fn test_parse_subscribe_invalid_batch_size() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (batch_size=abc)");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid batch_size value"));
    }

    #[test]
    fn test_parse_subscribe_invalid_from() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (from=not_a_number)");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid from value"));
    }

    #[test]
    fn test_parse_subscribe_unknown_option() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (unknown_option=123)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unknown subscription option"));
        assert!(err.contains("unknown_option"));
        assert!(err.contains("Valid options are"));
    }

    #[test]
    fn test_parse_subscribe_negative_from() {
        use kalamdb_commons::ids::SeqId;

        // Negative seq_id should be valid (might be used for special cases)
        let stmt =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages OPTIONS (from=-1)").unwrap();
        assert_eq!(stmt.options.from, Some(SeqId::new(-1)));
    }

    #[test]
    fn test_parse_subscribe_options_with_spaces() {
        // Test that options parsing handles spaces correctly
        let stmt = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages OPTIONS ( last_rows = 10 , batch_size = 20 )",
        )
        .unwrap();
        assert_eq!(stmt.options.last_rows, Some(10));
        assert_eq!(stmt.options.batch_size, Some(20));
    }

    #[test]
    fn test_parse_subscribe_rejects_order_by() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO SELECT id FROM app.messages ORDER BY id");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("ORDER BY"));
    }

    #[test]
    fn test_parse_subscribe_rejects_group_by() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT user_id FROM app.messages GROUP BY user_id",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("GROUP BY"));
    }

    #[test]
    fn test_parse_subscribe_rejects_system_projection() {
        let result = SubscribeStatement::parse("SUBSCRIBE TO SELECT _seq FROM app.messages");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("_seq"));
        assert!(err.contains("_deleted"));
    }

    // ========================================================================
    // Security Tests: SQL Injection & User Impersonation Prevention
    //
    // Each test verifies that a specific attack class is BLOCKED.
    // A test asserting `is_err()` is a tripwire: if the guard is removed the
    // test turns red. Do not relax these assertions without a security review.
    // ========================================================================

    // ── WHERE-clause subquery injection (data exfiltration) ─────────────────

    #[test]
    fn test_security_where_rejects_in_subquery_system_table() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE user_id IN (SELECT user_id FROM system.users WHERE \
             role = 'admin')",
        );
        assert!(result.is_err(), "IN (SELECT …) must be blocked");
        assert!(result.unwrap_err().contains("subqueries"));
    }

    #[test]
    fn test_security_where_rejects_scalar_subquery_password_leak() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE secret = (SELECT password FROM system.users LIMIT 1)",
        );
        assert!(result.is_err(), "scalar subquery in WHERE must be blocked");
        assert!(result.unwrap_err().contains("subqueries"));
    }

    #[test]
    fn test_security_where_rejects_exists_admin_check() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE EXISTS (SELECT 1 FROM system.users WHERE role = \
             'admin' AND user_id = 'attacker')",
        );
        assert!(result.is_err(), "EXISTS (…) must be blocked");
        assert!(result.unwrap_err().contains("subqueries"));
    }

    #[test]
    fn test_security_where_rejects_not_in_subquery_blocklist_bypass() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE user_id NOT IN (SELECT blocked_user FROM \
             security.blocklist)",
        );
        assert!(result.is_err(), "NOT IN (SELECT …) must be blocked");
        assert!(result.unwrap_err().contains("subqueries"));
    }

    #[test]
    fn test_security_where_rejects_in_subquery_api_key_scope() {
        // Attempts to check if the user holds a secret API key by correlating
        // with a privileged table.
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE token IN (SELECT api_key FROM system.api_keys WHERE \
             scope = 'superadmin')",
        );
        assert!(result.is_err(), "IN (SELECT …) on privileged table must be blocked");
        assert!(result.unwrap_err().contains("subqueries"));
    }

    // ── Multi-statement / stacked-query injection ────────────────────────────

    #[test]
    fn test_security_rejects_stacked_drop_table() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO app.messages; DROP TABLE app.messages");
        assert!(result.is_err(), "Stacked DROP TABLE must be rejected");
    }

    #[test]
    fn test_security_rejects_stacked_update_escalation() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages; UPDATE system.users SET role = 'admin' WHERE user_id = \
             'attacker'",
        );
        assert!(result.is_err(), "Stacked UPDATE for privilege escalation must be rejected");
    }

    #[test]
    fn test_security_rejects_stacked_delete_after_where() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages WHERE id = 1; DELETE FROM app.messages WHERE 1=1",
        );
        assert!(result.is_err(), "Stacked DELETE after WHERE must be rejected");
    }

    #[test]
    fn test_security_rejects_stacked_create_user() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO app.messages; CREATE USER attacker PASSWORD 'secret'",
        );
        assert!(result.is_err(), "Stacked CREATE USER must be rejected");
    }

    // ── UNION-based exfiltration via SELECT path ─────────────────────────────

    #[test]
    fn test_security_rejects_union_select_system_users() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT body FROM app.messages UNION SELECT password FROM system.users",
        );
        assert!(result.is_err(), "UNION SELECT must be blocked");
    }

    #[test]
    fn test_security_rejects_union_all_api_keys() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT body FROM app.messages UNION ALL SELECT token FROM \
             system.api_keys",
        );
        assert!(result.is_err(), "UNION ALL must be blocked");
    }

    // ── JOIN-based cross-table data exfiltration ─────────────────────────────

    #[test]
    fn test_security_rejects_inner_join_system_tables() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT msg.body FROM app.messages JOIN system.users ON msg.user_id = \
             system.users.id",
        );
        assert!(result.is_err(), "INNER JOIN must be blocked");
        let err = result.unwrap_err();
        assert!(err.contains("JOIN") || err.contains("alias"), "JOIN or alias guard must fire");
    }

    #[test]
    fn test_security_rejects_left_join_secrets_table() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT m.body FROM app.messages m LEFT JOIN app.secrets s ON m.user_id \
             = s.user_id",
        );
        // Either the JOIN guard or the table-alias guard fires first.
        assert!(result.is_err(), "LEFT JOIN must be blocked");
    }

    // ── Projection injection (expression / function / subquery) ─────────────

    #[test]
    fn test_security_projection_rejects_scalar_subquery_leak() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT (SELECT password FROM system.users WHERE username = 'admin'), \
             body FROM app.messages",
        );
        assert!(result.is_err(), "Scalar subquery in SELECT list must be blocked");
        assert!(result.unwrap_err().contains("direct column references"));
    }

    #[test]
    fn test_security_projection_rejects_concat_with_sensitive_column() {
        // An attacker could expose a secret column by concatenating it with a
        // known column so the combined value is delivered over the subscription
        // channel.
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT user_id || secret_token FROM app.messages",
        );
        assert!(result.is_err(), "String concat in projection must be blocked");
        assert!(result.unwrap_err().contains("direct column references"));
    }

    #[test]
    fn test_security_projection_rejects_case_when_leaking_data() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT CASE WHEN role = 'admin' THEN api_key ELSE 'hidden' END FROM \
             app.users",
        );
        assert!(result.is_err(), "CASE expression in projection must be blocked");
        assert!(result.unwrap_err().contains("direct column references"));
    }

    #[test]
    fn test_security_projection_rejects_coalesce_secret_fallback() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT COALESCE(api_key, 'fallback') FROM app.users",
        );
        assert!(result.is_err(), "COALESCE in projection must be blocked");
        assert!(result.unwrap_err().contains("direct column references"));
    }

    #[test]
    fn test_security_projection_rejects_cast_type_bypass() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT CAST(internal_flags AS TEXT) FROM app.messages",
        );
        assert!(result.is_err(), "CAST in projection must be blocked");
        assert!(result.unwrap_err().contains("direct column references"));
    }

    #[test]
    fn test_security_projection_rejects_arithmetic_expression() {
        // Even purely arithmetic expressions must be rejected to prevent
        // side-channel reads of numeric sensitive columns.
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT price * discount_factor FROM app.products",
        );
        assert!(result.is_err(), "Arithmetic in projection must be blocked");
        assert!(result.unwrap_err().contains("direct column references"));
    }

    // ── Query-structure injection (LIMIT, LOCK, DISTINCT, HAVING, CTE) ───────

    #[test]
    fn test_security_rejects_limit_clause() {
        // LIMIT inside a subscription is a structural mismatch and must be
        // rejected — it is also a potential DoS vector (fetch-all bypass).
        let result = SubscribeStatement::parse("SUBSCRIBE TO app.messages LIMIT 1000");
        assert!(result.is_err(), "LIMIT clause must be blocked");
    }

    #[test]
    fn test_security_rejects_for_update_locking() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO SELECT body FROM app.messages FOR UPDATE");
        assert!(result.is_err(), "FOR UPDATE lock must be blocked");
    }

    #[test]
    fn test_security_rejects_distinct_aggregation_bypass() {
        let result =
            SubscribeStatement::parse("SUBSCRIBE TO SELECT DISTINCT user_id FROM app.messages");
        assert!(result.is_err(), "DISTINCT must be blocked");
    }

    #[test]
    fn test_security_rejects_having_aggregation_bypass() {
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT user_id FROM app.messages HAVING COUNT(*) > 5",
        );
        assert!(result.is_err(), "HAVING must be blocked");
    }

    #[test]
    fn test_security_rejects_cte_via_stacked_statement() {
        // WITH … SELECT is a second statement stacked after the subscription.
        let result = SubscribeStatement::parse(
            "SUBSCRIBE TO SELECT body FROM app.messages; WITH leaked AS (SELECT * FROM \
             system.users) SELECT * FROM leaked",
        );
        assert!(result.is_err(), "CTE via stacked statement must be blocked");
    }

    #[test]
    fn test_security_rejects_three_part_table_name() {
        // A three-part name like catalog.namespace.table could be used to
        // escape namespace enforcement.
        let result = SubscribeStatement::parse("SUBSCRIBE TO catalog.system.users");
        assert!(result.is_err(), "Three-part table names must be rejected");
    }

    // ── CURRENT_USER impersonation / placeholder hijacking ───────────────────

    #[test]
    fn test_security_current_user_in_string_literal_not_replaced() {
        // The string value 'CURRENT_USER' must stay as a literal; only AST
        // identifier / function nodes are substituted.
        let resolved = crate::parser::query_parser::QueryParser::resolve_where_clause_placeholders(
            "category = 'CURRENT_USER' AND owner_id = CURRENT_USER",
            &kalamdb_commons::models::UserId::from("alice"),
        )
        .expect("placeholder resolution");
        assert!(
            resolved.contains("'CURRENT_USER'"),
            "String literal CURRENT_USER must NOT be replaced; got: {resolved}"
        );
        assert!(
            resolved.contains("'alice'"),
            "AST-level CURRENT_USER must be replaced with user id; got: {resolved}"
        );
        // 'alice' must appear exactly once — not substituted into the literal too.
        assert_eq!(
            resolved.matches("'alice'").count(),
            1,
            "User id must appear exactly once; got: {resolved}"
        );
    }

    #[test]
    fn test_security_multiple_current_user_occurrences_all_replaced() {
        // Every CURRENT_USER node in the AST must be replaced, not just the
        // first one. An attacker might craft a filter like
        // `owner = CURRENT_USER AND viewer = CURRENT_USER` hoping the second
        // instance escapes substitution and remains runnable as a keyword.
        let resolved = crate::parser::query_parser::QueryParser::resolve_where_clause_placeholders(
            "owner_id = CURRENT_USER AND delegate_id = CURRENT_USER",
            &kalamdb_commons::models::UserId::from("carol"),
        )
        .expect("placeholder resolution");
        assert_eq!(
            resolved.matches("'carol'").count(),
            2,
            "Both CURRENT_USER nodes must be replaced; got: {resolved}"
        );
        assert!(
            !resolved.contains("CURRENT_USER"),
            "No unresolved CURRENT_USER must remain; got: {resolved}"
        );
    }

    #[test]
    fn test_security_current_user_function_form_replaced_in_nested_and_or() {
        // CURRENT_USER() buried inside parenthesised AND/OR must still be
        // replaced — the AST visitor traverses the full expression tree.
        let resolved = crate::parser::query_parser::QueryParser::resolve_where_clause_placeholders(
            "(owner_id = CURRENT_USER() OR shared = true) AND active = true",
            &kalamdb_commons::models::UserId::from("dave"),
        )
        .expect("placeholder resolution");
        assert!(
            resolved.contains("'dave'"),
            "CURRENT_USER() inside AND/OR must be replaced; got: {resolved}"
        );
        assert!(
            !resolved.to_uppercase().contains("CURRENT_USER"),
            "No unresolved CURRENT_USER must remain; got: {resolved}"
        );
    }

    #[test]
    fn test_security_current_user_id_alias_replaced() {
        // CURRENT_USER_ID() is a KalamDB alias; it must resolve identically.
        let resolved = crate::parser::query_parser::QueryParser::resolve_where_clause_placeholders(
            "owner_id = CURRENT_USER_ID()",
            &kalamdb_commons::models::UserId::from("frank"),
        )
        .expect("placeholder resolution");
        assert!(
            resolved.contains("'frank'"),
            "CURRENT_USER_ID() must be replaced; got: {resolved}"
        );
        assert!(
            !resolved.to_uppercase().contains("CURRENT_USER"),
            "No unresolved placeholder must remain; got: {resolved}"
        );
    }
}
