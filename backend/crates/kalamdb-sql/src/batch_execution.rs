//! Batch SQL execution utilities.
//!
//! Provides robust statement splitting for multi-statement SQL payloads.
//! Handles quoted strings, comments, and whitespace to avoid breaking on
//! semicolons that appear inside literals or comment blocks.

/// Error produced when parsing a batch SQL string fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchParseError {
    message: String,
}

impl BatchParseError {
    /// Create a new parse error with the provided message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for BatchParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for BatchParseError {}

/// Split a SQL batch payload into individual statements.
///
/// Preserves statement order and whitespace while ignoring semicolons that
/// appear within quoted strings or comments.
///
/// # Examples
///
/// ```
/// use kalamdb_sql::batch_execution::split_statements;
///
/// let statements = split_statements("CREATE TABLE t(id INT); INSERT INTO t VALUES (1);").unwrap();
/// assert_eq!(statements.len(), 2);
/// assert_eq!(statements[0], "CREATE TABLE t(id INT)");
/// assert_eq!(statements[1], "INSERT INTO t VALUES (1)");
/// ```
pub fn split_statements(sql: &str) -> Result<Vec<String>, BatchParseError> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                if !current.ends_with(char::is_whitespace) {
                    current.push('\n');
                }
            }
            continue;
        }

        if in_block_comment {
            if ch == '*' && chars.peek() == Some(&'/') {
                if !current.ends_with(char::is_whitespace) {
                    current.push(' ');
                }
                chars.next();
                in_block_comment = false;
                continue;
            }
            if ch == '\n' && !current.ends_with(char::is_whitespace) {
                current.push('\n');
            }
            continue;
        }

        if !in_single_quote && !in_double_quote && !in_backtick {
            if ch == '-' && chars.peek() == Some(&'-') {
                chars.next();
                in_line_comment = true;
                continue;
            }

            if ch == '/' && chars.peek() == Some(&'*') {
                chars.next();
                in_block_comment = true;
                continue;
            }
        }

        match ch {
            '\'' if !in_double_quote && !in_backtick => {
                if in_single_quote && chars.peek() == Some(&'\'') {
                    // Escaped quote inside single-quoted string
                    current.push(ch);
                    current.push(chars.next().unwrap());
                    continue;
                }
                in_single_quote = !in_single_quote;
                current.push(ch);
                continue;
            },
            '"' if !in_single_quote && !in_backtick => {
                if in_double_quote && chars.peek() == Some(&'"') {
                    current.push(ch);
                    current.push(chars.next().unwrap());
                    continue;
                }
                in_double_quote = !in_double_quote;
                current.push(ch);
                continue;
            },
            '`' if !in_single_quote && !in_double_quote => {
                in_backtick = !in_backtick;
                current.push(ch);
                continue;
            },
            ';' if !(in_single_quote || in_double_quote || in_backtick) => {
                let stmt = current.trim();
                if !stmt.is_empty() {
                    statements.push(stmt.to_string());
                }
                current.clear();
                continue;
            },
            _ => {
                current.push(ch);
            },
        }
    }

    if in_single_quote || in_double_quote || in_backtick {
        return Err(BatchParseError::new("Unterminated quoted string in SQL batch"));
    }

    if in_block_comment {
        return Err(BatchParseError::new("Unterminated block comment in SQL batch"));
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::split_statements;

    #[test]
    fn splits_simple_statements() {
        let sql = "CREATE TABLE t(id INT); INSERT INTO t VALUES (1);";
        let statements = split_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "CREATE TABLE t(id INT)");
        assert_eq!(statements[1], "INSERT INTO t VALUES (1)");
    }

    #[test]
    fn ignores_semicolons_in_strings() {
        let sql = "INSERT INTO logs(message) VALUES('value;still part of string'); SELECT 1;";
        let statements = split_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("value;still part of string"));
    }

    #[test]
    fn ignores_semicolons_in_comments() {
        let sql = "SELECT 1; -- second statement;\nSELECT 2; /* comment; */ SELECT 3;";
        let statements = split_statements(sql).unwrap();
        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "SELECT 2");
        assert_eq!(statements[2], "SELECT 3");
    }

    #[test]
    fn strips_commented_lines_and_keeps_executable_statement() {
        let sql = "-- create namespace rag;\nselect * from system.schemas;\n-- test\n-- dfdvd\n";
        let statements = split_statements(sql).unwrap();
        assert_eq!(statements, vec!["select * from system.schemas"]);
    }

    #[test]
    fn returns_empty_for_comment_only_input() {
        let sql = "-- first\n/* second */\n-- third";
        let statements = split_statements(sql).unwrap();
        assert!(statements.is_empty());
    }

    #[test]
    fn preserves_comment_markers_inside_strings() {
        let sql = "SELECT '-- keep', '/* keep */';";
        let statements = split_statements(sql).unwrap();
        assert_eq!(statements, vec!["SELECT '-- keep', '/* keep */'"]);
    }

    #[test]
    fn handles_escaped_quotes() {
        let sql = "INSERT INTO t(text) VALUES('It''s fine; really'); SELECT 1;";
        let statements = split_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("It''s fine; really"));
    }

    #[test]
    fn error_on_unterminated_string() {
        let sql = "INSERT INTO t(text) VALUES('missing end);";
        let err = split_statements(sql).unwrap_err();
        assert!(err.to_string().contains("Unterminated quoted string"));
    }
}
