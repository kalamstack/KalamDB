//! Common DDL parsing utilities
//!
//! This module provides reusable parsing functions to eliminate code duplication
//! across DDL statement parsers.

use crate::ddl::DdlResult;

/// Normalize SQL and convert to uppercase for pattern matching (optimized)
///
/// Removes extra whitespace, trailing semicolons, and converts to uppercase.
#[inline]
pub fn normalize_and_upper(sql: &str) -> String {
    let trimmed = sql.trim().trim_end_matches(';');
    let mut result = String::with_capacity(trimmed.len());
    let mut prev_was_space = false;

    for c in trimmed.chars() {
        if c.is_whitespace() {
            if !prev_was_space && !result.is_empty() {
                result.push(' ');
                prev_was_space = true;
            }
        } else {
            result.push(c.to_ascii_uppercase());
            prev_was_space = false;
        }
    }

    // Remove trailing space if any
    if result.ends_with(' ') {
        result.pop();
    }

    result
}

/// Parse a simple statement with no arguments (e.g., "SHOW NAMESPACES")
///
/// # Examples
///
/// ```ignore
/// parse_simple_statement(sql, "SHOW NAMESPACES")?;
/// ```
pub fn parse_simple_statement(sql: &str, expected: &str) -> DdlResult<()> {
    let normalized = normalize_and_upper(sql);
    if normalized == expected.to_uppercase() {
        Ok(())
    } else {
        Err(format!("Expected {} statement", expected))
    }
}

/// Parse a statement with an optional IN clause (e.g., "SHOW TABLES IN namespace")
///
/// # Returns
///
/// - `Ok(None)` if no IN clause is present
/// - `Ok(Some(namespace))` if IN clause is found with a namespace
/// - `Err(...)` if IN clause is malformed
///
/// # Examples
///
/// ```ignore
/// let namespace = parse_optional_in_clause(sql, "SHOW TABLES")?;
/// ```
pub fn parse_optional_in_clause(sql: &str, command: &str) -> DdlResult<Option<String>> {
    let sql_trimmed = sql.trim().trim_end_matches(';');
    let sql_upper = sql_trimmed.to_uppercase();
    let command_upper = command.to_uppercase();

    if !sql_upper.starts_with(&command_upper) {
        return Err(format!("Expected {} statement", command));
    }

    // Check for IN clause
    if sql_upper.contains(" IN ") {
        let in_pos = sql_upper.find(" IN ").unwrap();
        let after_in_orig = sql_trimmed[in_pos + 4..].trim();
        let after_in_upper = sql_upper[in_pos + 4..].trim();

        if after_in_upper.is_empty() {
            return Err("Namespace name required after IN".to_string());
        }

        // Support optional "NAMESPACE" keyword: "IN NAMESPACE <ns>"
        // We operate on the original-cased slice to preserve identifier case
        let mut parts_orig = after_in_orig.split_whitespace();
        let first_token_orig = parts_orig
            .next()
            .ok_or_else(|| "Namespace name required after IN".to_string())?;

        if first_token_orig.eq_ignore_ascii_case("NAMESPACE") {
            // Expect a namespace name after the NAMESPACE keyword
            let ns = parts_orig
                .next()
                .ok_or_else(|| "Namespace name required after IN NAMESPACE".to_string())?;
            Ok(Some(ns.to_string()))
        } else {
            // Classic form: IN <namespace>
            Ok(Some(first_token_orig.to_string()))
        }
    } else if sql_upper.ends_with(" IN") {
        Err("Namespace name required after IN".to_string())
    } else {
        Ok(None)
    }
}

/// Parse CREATE/DROP statement with optional IF [NOT] EXISTS clause (optimized)
///
/// # Returns
///
/// Tuple of (entity_name, has_if_clause)
///
/// # Examples
///
/// ```ignore
/// let (name, if_not_exists) = parse_create_drop_statement(sql, "CREATE NAMESPACE", "IF NOT EXISTS")?;
/// let (name, if_exists) = parse_create_drop_statement(sql, "DROP NAMESPACE", "IF EXISTS")?;
/// ```
pub fn parse_create_drop_statement(
    sql: &str,
    command: &str,
    if_clause: &str,
) -> DdlResult<(String, bool)> {
    let trimmed = sql.trim().trim_end_matches(';');
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();

    // Pre-compute counts to avoid multiple iterations
    let command_token_count = command.split_whitespace().count();
    let if_clause_tokens: Vec<&str> = if_clause.split_whitespace().collect();
    let if_clause_token_count = if_clause_tokens.len();

    // Check if command matches (case-insensitive)
    if tokens.len() < command_token_count {
        return Err(format!("Expected {} statement", command));
    }

    let command_parts: Vec<&str> = command.split_whitespace().collect();
    for (i, expected) in command_parts.iter().enumerate() {
        if !tokens[i].eq_ignore_ascii_case(expected) {
            return Err(format!("Expected {} statement", command));
        }
    }

    // Check for IF clause
    let has_if_clause = if tokens.len() > command_token_count + if_clause_token_count {
        tokens[command_token_count..command_token_count + if_clause_token_count]
            .iter()
            .zip(if_clause_tokens.iter())
            .all(|(actual, expected)| actual.eq_ignore_ascii_case(expected))
    } else {
        false
    };

    // Extract entity name
    let skip_tokens = command_token_count
        + if has_if_clause {
            if_clause_token_count
        } else {
            0
        };
    let name = tokens
        .get(skip_tokens)
        .ok_or_else(|| format!("Entity name is required after {}", command))?
        .to_string();

    Ok((name, has_if_clause))
}

/// Extract an identifier from a specific token position
///
/// # Examples
///
/// ```ignore
/// // "STORAGE FLUSH TABLE prod.events" - extract "prod.events" (token index 3)
/// let table_ref = extract_token(sql, 2)?;
/// ```
pub fn extract_token(sql: &str, token_index: usize) -> DdlResult<String> {
    let tokens: Vec<&str> = sql.trim().trim_end_matches(';').split_whitespace().collect();

    tokens
        .get(token_index)
        .map(|s| s.to_string())
        .ok_or_else(|| format!("Expected token at position {}", token_index))
}

/// Validate no extra tokens after expected position
///
/// # Examples
///
/// ```ignore
/// // Ensure "STORAGE FLUSH TABLE prod.events" has no tokens after position 3
/// validate_no_extra_tokens(sql, 4, "STORAGE FLUSH TABLE")?;
/// ```
pub fn validate_no_extra_tokens(
    sql: &str,
    expected_token_count: usize,
    command: &str,
) -> DdlResult<()> {
    let tokens: Vec<&str> = sql.trim().trim_end_matches(';').split_whitespace().collect();

    if tokens.len() > expected_token_count {
        return Err(format!(
            "Unexpected tokens after {}. Found: {}",
            command,
            tokens[expected_token_count..].join(" ")
        ));
    }

    Ok(())
}

/// Parse a potentially qualified table reference (namespace.table or just table)
///
/// # Returns
///
/// Tuple of (Option<namespace>, table_name)
///
/// # Examples
///
/// ```ignore
/// let (ns, table) = parse_table_reference("prod.events")?;
/// assert_eq!(ns, Some("prod"));
/// assert_eq!(table, "events");
///
/// let (ns, table) = parse_table_reference("users")?;
/// assert_eq!(ns, None);
/// assert_eq!(table, "users");
/// ```
pub fn parse_table_reference(table_ref: &str) -> DdlResult<(Option<String>, String)> {
    // Basic validation: table reference should not contain SQL keywords or complex expressions
    let trimmed = table_ref.trim();
    if trimmed.is_empty() {
        return Err("Table reference cannot be empty".to_string());
    }

    // Check for SQL keywords that indicate this is not a simple table reference
    let sql_keywords = [
        "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
    ];
    let upper_ref = trimmed.to_uppercase();
    for keyword in &sql_keywords {
        if upper_ref.contains(&format!(" {}", keyword)) || upper_ref.starts_with(keyword) {
            return Err(format!(
                "Invalid table reference '{}'. Table references should be simple identifiers like \
                 'table' or 'namespace.table', not SQL statements",
                table_ref
            ));
        }
    }

    // Check for spaces (indicates complex expression)
    if trimmed.contains(' ') {
        return Err(format!(
            "Invalid table reference '{}'. Table references should not contain spaces",
            table_ref
        ));
    }

    let mut parts = trimmed.split('.');
    let first = parts.next();
    let second = parts.next();
    let third = parts.next();
    match (first, second, third) {
        (Some(table), None, None) => {
            if table.is_empty() {
                return Err("Table name cannot be empty".to_string());
            }
            Ok((None, table.to_string()))
        },
        (Some(namespace), Some(table), None) => {
            if namespace.is_empty() {
                return Err("Namespace name cannot be empty".to_string());
            }
            if table.is_empty() {
                return Err("Table name cannot be empty".to_string());
            }
            Ok((Some(namespace.to_string()), table.to_string()))
        },
        _ => Err(format!(
            "Invalid table reference '{}'. Expected 'table' or 'namespace.table'",
            table_ref
        )),
    }
}

/// Extract table reference after a command prefix
///
/// # Examples
///
/// ```ignore
/// let table_ref = extract_after_prefix(sql, "DESCRIBE TABLE")?;
/// let table_ref = extract_after_prefix(sql, "SHOW STATS FOR TABLE")?;
/// ```
pub fn extract_after_prefix(sql: &str, prefix: &str) -> DdlResult<String> {
    let sql_trimmed = sql.trim().trim_end_matches(';');
    let sql_upper = sql_trimmed.to_uppercase();
    let prefix_upper = prefix.to_uppercase();

    if !sql_upper.starts_with(&prefix_upper) {
        return Err(format!("Expected {} statement", prefix));
    }

    // Find where the prefix ends in the original (case-preserved) SQL
    let prefix_len = prefix.len();
    let remainder = sql_trimmed[prefix_len..].trim();

    if remainder.is_empty() {
        return Err(format!("Missing argument after {}", prefix));
    }

    // Extract first token (may contain dots for qualified names)
    let first_token = remainder
        .split_whitespace()
        .next()
        .ok_or_else(|| format!("Missing argument after {}", prefix))?;

    Ok(first_token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_and_upper() {
        assert_eq!(normalize_and_upper("  show  tables  ;"), "SHOW TABLES");
        assert_eq!(normalize_and_upper("create\nnamespace\napp"), "CREATE NAMESPACE APP");
    }

    #[test]
    fn test_parse_simple_statement() {
        assert!(parse_simple_statement("SHOW NAMESPACES", "SHOW NAMESPACES").is_ok());
        assert!(parse_simple_statement("show namespaces", "SHOW NAMESPACES").is_ok());
        assert!(parse_simple_statement("SHOW TABLES", "SHOW NAMESPACES").is_err());
    }

    #[test]
    fn test_parse_optional_in_clause() {
        // No IN clause
        let result = parse_optional_in_clause("SHOW TABLES", "SHOW TABLES").unwrap();
        assert!(result.is_none());

        // With IN clause
        let result = parse_optional_in_clause("SHOW TABLES IN myapp", "SHOW TABLES").unwrap();
        assert_eq!(result.unwrap(), "myapp");

        // With IN NAMESPACE clause
        let result =
            parse_optional_in_clause("SHOW TABLES IN NAMESPACE myapp", "SHOW TABLES").unwrap();
        assert_eq!(result.unwrap(), "myapp");

        // Missing namespace after IN
        assert!(parse_optional_in_clause("SHOW TABLES IN", "SHOW TABLES").is_err());
    }

    #[test]
    fn test_parse_create_drop_statement() {
        // CREATE with IF NOT EXISTS
        let (name, has_if) = parse_create_drop_statement(
            "CREATE NAMESPACE IF NOT EXISTS app",
            "CREATE NAMESPACE",
            "IF NOT EXISTS",
        )
        .unwrap();
        assert_eq!(name, "app");
        assert!(has_if);

        // CREATE without IF NOT EXISTS
        let (name, has_if) = parse_create_drop_statement(
            "CREATE NAMESPACE app",
            "CREATE NAMESPACE",
            "IF NOT EXISTS",
        )
        .unwrap();
        assert_eq!(name, "app");
        assert!(!has_if);

        // DROP with IF EXISTS
        let (name, has_if) = parse_create_drop_statement(
            "DROP NAMESPACE IF EXISTS app",
            "DROP NAMESPACE",
            "IF EXISTS",
        )
        .unwrap();
        assert_eq!(name, "app");
        assert!(has_if);

        // Preserve case
        let (name, _) = parse_create_drop_statement(
            "CREATE NAMESPACE MyApp",
            "CREATE NAMESPACE",
            "IF NOT EXISTS",
        )
        .unwrap();
        assert_eq!(name, "MyApp");
    }

    #[test]
    fn test_extract_token() {
        let sql = "STORAGE FLUSH TABLE prod.events";
        assert_eq!(extract_token(sql, 0).unwrap(), "STORAGE");
        assert_eq!(extract_token(sql, 1).unwrap(), "FLUSH");
        assert_eq!(extract_token(sql, 2).unwrap(), "TABLE");
        assert_eq!(extract_token(sql, 3).unwrap(), "prod.events");
        assert!(extract_token(sql, 5).is_err());
    }

    #[test]
    fn test_validate_no_extra_tokens() {
        assert!(validate_no_extra_tokens(
            "STORAGE FLUSH TABLE prod.events",
            4,
            "STORAGE FLUSH TABLE"
        )
        .is_ok());
        assert!(validate_no_extra_tokens(
            "STORAGE FLUSH TABLE prod.events extra",
            4,
            "STORAGE FLUSH TABLE"
        )
        .is_err());
    }

    #[test]
    fn test_parse_table_reference_valid() {
        // Simple table name
        let (ns, table) = parse_table_reference("users").unwrap();
        assert_eq!(ns, None);
        assert_eq!(table, "users");

        // Namespace.table
        let (ns, table) = parse_table_reference("prod.events").unwrap();
        assert_eq!(ns, Some("prod".to_string()));
        assert_eq!(table, "events");

        // Quoted identifiers
        let (ns, table) = parse_table_reference("\"my-table\"").unwrap();
        assert_eq!(ns, None);
        assert_eq!(table, "\"my-table\"");
    }

    #[test]
    fn test_parse_table_reference_invalid() {
        // SQL statements should be rejected
        assert!(parse_table_reference("select * from users").is_err());
        assert!(parse_table_reference("SELECT * FROM users").is_err());
        assert!(parse_table_reference("users where id > 1").is_err());

        // Spaces should be rejected
        assert!(parse_table_reference("my table").is_err());
        assert!(parse_table_reference("prod.my table").is_err());

        // Empty parts
        assert!(parse_table_reference("").is_err());
        assert!(parse_table_reference(".").is_err());
        assert!(parse_table_reference("prod.").is_err());
        assert!(parse_table_reference(".table").is_err());

        // Too many dots
        assert!(parse_table_reference("a.b.c").is_err());
    }
}
