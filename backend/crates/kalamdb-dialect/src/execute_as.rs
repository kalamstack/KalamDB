//! EXECUTE AS USER parsing utilities
//!
//! Provides a single shared implementation for parsing `EXECUTE AS USER` SQL
//! wrappers.  Both the API execution handler and the cluster forwarding logic
//! delegate to these functions so there is **one** canonical parser.
//!
//! Grammar:
//! ```text
//! EXECUTE AS USER <username> ( <inner_sql> )
//! ```
//! `<username>` may be single-quoted (`'alice'`) or bare (`alice`).
//! The inner SQL must be exactly one statement.

use crate::split_statements;

/// Prefix used for case-insensitive matching.
const EXECUTE_AS_PREFIX: &str = "EXECUTE AS USER";
/// Byte length of the prefix — used for fast slice comparisons.
const EXECUTE_AS_PREFIX_LEN: usize = EXECUTE_AS_PREFIX.len(); // 15

/// Result of parsing an `EXECUTE AS USER` envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteAsEnvelope {
    /// The user identifier that follows `EXECUTE AS USER`.
    pub username: String,
    /// The inner SQL statement (parentheses stripped).
    pub inner_sql: String,
}

/// Check whether `sql` starts with `EXECUTE AS USER` (case-insensitive).
///
/// This is a very cheap check that avoids any allocation.
#[inline]
pub fn is_execute_as(sql: &str) -> bool {
    let trimmed = sql.trim();
    trimmed.len() >= EXECUTE_AS_PREFIX_LEN
        && trimmed.as_bytes()[..EXECUTE_AS_PREFIX_LEN]
            .eq_ignore_ascii_case(EXECUTE_AS_PREFIX.as_bytes())
}

/// Extract just the inner SQL from an `EXECUTE AS USER '...' (...)` wrapper.
///
/// Returns `Some(inner_sql)` if the SQL matches the EXECUTE AS USER pattern,
/// `None` otherwise.  This is intentionally lightweight — it only needs to
/// strip the wrapper so callers (e.g. statement classifier) can inspect the
/// actual SQL statement without allocating a full parsed result.
pub fn extract_inner_sql(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    if trimmed.len() < EXECUTE_AS_PREFIX_LEN
        || !trimmed.as_bytes()[..EXECUTE_AS_PREFIX_LEN]
            .eq_ignore_ascii_case(EXECUTE_AS_PREFIX.as_bytes())
    {
        return None;
    }

    let open_paren = trimmed.find('(')?;
    let close_paren = find_matching_close_paren(trimmed, open_paren)?;
    if close_paren <= open_paren + 1 {
        return None;
    }

    let inner = trimmed[open_paren + 1..close_paren].trim();
    if inner.is_empty() {
        return None;
    }

    Some(inner.to_string())
}

fn parse_single_quoted(input: &str) -> Result<(String, &str), String> {
    let mut value = String::new();
    let mut chars = input.char_indices().peekable();

    if !input.starts_with('\'') {
        return Err("Expected single-quoted string".to_string());
    }

    chars.next();

    while let Some((idx, ch)) = chars.next() {
        if ch != '\'' {
            value.push(ch);
            continue;
        }

        if matches!(chars.peek(), Some((_, '\''))) {
            chars.next();
            value.push('\'');
            continue;
        }

        return Ok((value, &input[idx + 1..]));
    }

    Err("EXECUTE AS USER username quote was not closed".to_string())
}

fn find_matching_close_paren(input: &str, open_idx: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = input.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if idx < open_idx {
            continue;
        }

        if in_single_quote {
            if ch == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    chars.next();
                } else {
                    in_single_quote = false;
                }
            }
            continue;
        }

        if in_double_quote {
            if ch == '"' {
                if matches!(chars.peek(), Some((_, '"'))) {
                    chars.next();
                } else {
                    in_double_quote = false;
                }
            }
            continue;
        }

        match ch {
            '\'' => in_single_quote = true,
            '"' => in_double_quote = true,
            '(' => depth += 1,
            ')' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(idx);
                }
            },
            _ => {},
        }
    }

    None
}

/// Fully parse an `EXECUTE AS USER` envelope, extracting the username and the
/// parenthesised inner SQL.
///
/// The username may be quoted (`'alice'`) or bare (`alice`).
/// Bare usernames extend up to the first whitespace or `(` character.
///
/// Returns:
/// - `Ok(Some(envelope))` when the statement is an EXECUTE AS USER wrapper
/// - `Ok(None)` when the statement is a normal SQL statement (no wrapper)
/// - `Err(msg)` on syntax errors inside the wrapper
pub fn parse_execute_as(statement: &str) -> Result<Option<ExecuteAsEnvelope>, String> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    // Fast prefix check without allocating an uppercased copy.
    if trimmed.len() < EXECUTE_AS_PREFIX_LEN
        || !trimmed.as_bytes()[..EXECUTE_AS_PREFIX_LEN]
            .eq_ignore_ascii_case(EXECUTE_AS_PREFIX.as_bytes())
    {
        return Ok(None);
    }

    let after_prefix = trimmed[EXECUTE_AS_PREFIX_LEN..].trim_start();

    // --- Username extraction (quoted or bare) ---
    let (username, rest): (String, &str) = if after_prefix.starts_with('\'') {
        let (parsed_username, rest) = parse_single_quoted(after_prefix)?;
        let uname = parsed_username.trim();
        if uname.is_empty() {
            return Err("EXECUTE AS USER username cannot be empty".to_string());
        }
        (uname.to_string(), rest.trim_start())
    } else {
        // Bare: EXECUTE AS USER alice (...)
        // Username extends until whitespace or '('.
        let end = after_prefix
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(after_prefix.len());
        let uname = after_prefix[..end].trim();
        if uname.is_empty() {
            return Err("EXECUTE AS USER username cannot be empty".to_string());
        }
        (uname.to_string(), after_prefix[end..].trim_start())
    };

    // --- Parenthesised SQL body ---
    if !rest.starts_with('(') {
        return Err("EXECUTE AS USER must wrap SQL in parentheses".to_string());
    }

    let close_idx = find_matching_close_paren(rest, 0)
        .ok_or_else(|| "EXECUTE AS USER missing closing ')'".to_string())?;
    let inner_sql = rest[1..close_idx].trim();
    if inner_sql.is_empty() {
        return Err("EXECUTE AS USER requires a non-empty inner SQL statement".to_string());
    }

    let trailing = rest[close_idx + 1..].trim();
    if !trailing.is_empty() {
        return Err("EXECUTE AS USER must contain exactly one wrapped SQL statement".to_string());
    }

    let inner_statements = split_statements(inner_sql)
        .map_err(|e| format!("Failed to parse inner SQL for EXECUTE AS USER: {}", e))?;
    if inner_statements.len() != 1 {
        return Err("EXECUTE AS USER can only wrap a single SQL statement".to_string());
    }

    Ok(Some(ExecuteAsEnvelope {
        username,
        inner_sql: inner_statements[0].trim().to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // is_execute_as
    // -----------------------------------------------------------------------

    #[test]
    fn is_execute_as_positive() {
        assert!(is_execute_as("EXECUTE AS USER 'alice' (SELECT 1)"));
        assert!(is_execute_as("execute as user bob (SELECT 1)"));
        assert!(is_execute_as("  EXECUTE AS USER 'x' (SELECT 1)  "));
    }

    #[test]
    fn is_execute_as_negative() {
        assert!(!is_execute_as("SELECT 1"));
        assert!(!is_execute_as("INSERT INTO t VALUES (1)"));
        assert!(!is_execute_as("EXECUTE SOMETHING ELSE"));
    }

    // -----------------------------------------------------------------------
    // extract_inner_sql
    // -----------------------------------------------------------------------

    #[test]
    fn extract_inner_sql_basic() {
        let inner = extract_inner_sql("EXECUTE AS USER 'alice' (SELECT * FROM t)");
        assert_eq!(inner.as_deref(), Some("SELECT * FROM t"));
    }

    #[test]
    fn extract_inner_sql_bare_username() {
        let inner = extract_inner_sql("EXECUTE AS USER bob (INSERT INTO t VALUES (1, 'x'))");
        assert_eq!(inner.as_deref(), Some("INSERT INTO t VALUES (1, 'x')"));
    }

    #[test]
    fn extract_inner_sql_non_wrapper() {
        assert!(extract_inner_sql("SELECT 1").is_none());
    }

    #[test]
    fn extract_inner_sql_case_insensitive() {
        let inner = extract_inner_sql("execute as user 'alice' (DELETE FROM t WHERE id = 1)");
        assert_eq!(inner.as_deref(), Some("DELETE FROM t WHERE id = 1"));
    }

    // -----------------------------------------------------------------------
    // parse_execute_as — quoted username
    // -----------------------------------------------------------------------

    #[test]
    fn parse_quoted_username() {
        let result =
            parse_execute_as("EXECUTE AS USER 'alice' (SELECT * FROM default.todos WHERE id = 1);")
                .expect("should parse")
                .expect("should be an envelope");

        assert_eq!(result.username, "alice");
        assert_eq!(result.inner_sql, "SELECT * FROM default.todos WHERE id = 1");
    }

    #[test]
    fn reject_multi_statement() {
        let err = parse_execute_as("EXECUTE AS USER 'alice' (SELECT 1; SELECT 2)")
            .expect_err("multiple statements should be rejected");
        assert!(err.contains("single SQL statement"));
    }

    // -----------------------------------------------------------------------
    // parse_execute_as — bare (unquoted) username
    // -----------------------------------------------------------------------

    #[test]
    fn parse_bare_username() {
        let result =
            parse_execute_as("EXECUTE AS USER alice (SELECT * FROM default.todos WHERE id = 1);")
                .expect("should parse")
                .expect("should be an envelope");

        assert_eq!(result.username, "alice");
        assert_eq!(result.inner_sql, "SELECT * FROM default.todos WHERE id = 1");
    }

    #[test]
    fn parse_bare_case_insensitive() {
        let result = parse_execute_as("execute as user bob (INSERT INTO default.t VALUES (1))")
            .expect("should parse")
            .expect("should be an envelope");

        assert_eq!(result.username, "bob");
        assert_eq!(result.inner_sql, "INSERT INTO default.t VALUES (1)");
    }

    #[test]
    fn parse_escaped_quote_in_username() {
        let result =
            parse_execute_as("EXECUTE AS USER 'alice''o' (INSERT INTO default.t VALUES (1))")
                .expect("should parse")
                .expect("should be an envelope");

        assert_eq!(result.username, "alice'o");
        assert_eq!(result.inner_sql, "INSERT INTO default.t VALUES (1)");
    }

    #[test]
    fn parse_inner_sql_with_parenthesis_in_string_literal() {
        let result = parse_execute_as(
            "EXECUTE AS USER 'alice' (INSERT INTO default.t VALUES ('hello (', 'done'))",
        )
        .expect("should parse")
        .expect("should be an envelope");

        assert_eq!(result.username, "alice");
        assert_eq!(result.inner_sql, "INSERT INTO default.t VALUES ('hello (', 'done')");
    }

    #[test]
    fn extract_inner_sql_ignores_parenthesis_in_string_literal() {
        let inner = extract_inner_sql(
            "EXECUTE AS USER 'alice' (INSERT INTO default.t VALUES ('hello (', 'done'))",
        );
        assert_eq!(inner.as_deref(), Some("INSERT INTO default.t VALUES ('hello (', 'done')"));
    }

    #[test]
    fn parse_bare_no_space_before_paren() {
        let result = parse_execute_as("EXECUTE AS USER alice(SELECT 1)")
            .expect("should parse")
            .expect("should be an envelope");

        assert_eq!(result.username, "alice");
        assert_eq!(result.inner_sql, "SELECT 1");
    }

    // -----------------------------------------------------------------------
    // Passthrough (normal SQL)
    // -----------------------------------------------------------------------

    #[test]
    fn passthrough_normal_sql() {
        let result =
            parse_execute_as("SELECT * FROM default.todos WHERE id = 10").expect("should parse");
        assert!(result.is_none());
    }
}
