//! Batch SQL execution utilities.
//!
//! Provides robust statement splitting for multi-statement SQL payloads.
//! Handles quoted strings, comments, and whitespace to avoid breaking on
//! semicolons that appear inside literals or comment blocks.

use crate::dialect::KalamDbDialect;
use crate::execute_as::parse_execute_as;
use crate::parser::utils::parse_single_statement;
use crate::parser::utils::parse_sql_statements;
use kalamdb_commons::models::Username;
use sqlparser::ast::Spanned;
use sqlparser::ast::Statement;
use sqlparser::tokenizer::{Location, Span};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedExecutionStatement {
    pub sql: String,
    pub execute_as_username: Option<Username>,
    pub parsed_statement: Option<Statement>,
}

#[derive(Debug, Clone)]
pub struct PreparedExecutionBatchStatement<T> {
    pub execute_as_username: Option<Username>,
    pub parsed_statement: Option<Statement>,
    pub prepared_statement: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionBatchParseError {
    Batch(BatchParseError),
    Statement {
        statement_index: usize,
        message: String,
    },
}

impl std::fmt::Display for ExecutionBatchParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch(err) => write!(f, "{}", err),
            Self::Statement {
                statement_index,
                message,
            } => write!(f, "Statement {} failed: {}", statement_index, message),
        }
    }
}

impl std::error::Error for ExecutionBatchParseError {}

#[derive(Debug)]
pub enum ExecutionBatchPrepareError<E> {
    Parse(ExecutionBatchParseError),
    Prepare { statement_index: usize, error: E },
}

impl<E: std::fmt::Display> std::fmt::Display for ExecutionBatchPrepareError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(err) => write!(f, "{}", err),
            Self::Prepare {
                statement_index,
                error,
            } => write!(f, "Statement {} failed: {}", statement_index, error),
        }
    }
}

impl<E> From<ExecutionBatchParseError> for ExecutionBatchPrepareError<E> {
    fn from(value: ExecutionBatchParseError) -> Self {
        Self::Parse(value)
    }
}

pub fn parse_execution_statement(statement: &str) -> Result<ParsedExecutionStatement, String> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    match parse_execute_as(statement)? {
        Some(envelope) => Ok(ParsedExecutionStatement {
            sql: envelope.inner_sql.clone(),
            execute_as_username: Some(
                Username::try_new(&envelope.username)
                    .map_err(|e| format!("Invalid execute-as username: {}", e))?,
            ),
            parsed_statement: parse_single_statement(&envelope.inner_sql).ok().flatten(),
        }),
        None => Ok(ParsedExecutionStatement {
            sql: trimmed.to_string(),
            execute_as_username: None,
            parsed_statement: parse_single_statement(trimmed).ok().flatten(),
        }),
    }
}

pub fn parse_execution_batch(
    sql: &str,
) -> Result<Vec<ParsedExecutionStatement>, ExecutionBatchParseError> {
    let raw_statements = split_statements(sql).map_err(ExecutionBatchParseError::Batch)?;
    let mut parsed = Vec::with_capacity(raw_statements.len());

    for (idx, statement) in raw_statements.iter().enumerate() {
        let parsed_statement = parse_execution_statement(statement).map_err(|message| {
            ExecutionBatchParseError::Statement {
                statement_index: idx + 1,
                message,
            }
        })?;
        parsed.push(parsed_statement);
    }
    Ok(parsed)
}

pub fn prepare_execution_batch<T, E, F>(
    sql: &str,
    mut prepare_statement: F,
) -> Result<Vec<PreparedExecutionBatchStatement<T>>, ExecutionBatchPrepareError<E>>
where
    F: FnMut(&ParsedExecutionStatement) -> Result<T, E>,
{
    let parsed_statements = parse_execution_batch(sql)?;
    let mut prepared = Vec::with_capacity(parsed_statements.len());

    for (idx, statement) in parsed_statements.into_iter().enumerate() {
        let prepared_statement =
            prepare_statement(&statement).map_err(|error| ExecutionBatchPrepareError::Prepare {
                statement_index: idx + 1,
                error,
            })?;

        prepared.push(PreparedExecutionBatchStatement {
            execute_as_username: statement.execute_as_username,
            parsed_statement: statement.parsed_statement,
            prepared_statement,
        });
    }

    Ok(prepared)
}

fn line_start_offsets(sql: &str) -> Vec<usize> {
    let mut starts = vec![0];
    for (idx, byte) in sql.bytes().enumerate() {
        if byte == b'\n' {
            starts.push(idx + 1);
        }
    }
    starts
}

fn byte_index_for_location(sql: &str, line_starts: &[usize], location: Location) -> Option<usize> {
    if location.line == 0 || location.column == 0 {
        return None;
    }

    let line_idx = location.line.checked_sub(1)? as usize;
    let line_start = *line_starts.get(line_idx)?;
    let line_end = line_starts.get(line_idx + 1).copied().unwrap_or(sql.len());
    let line = &sql[line_start..line_end];

    let mut column = 1u64;
    for (offset, ch) in line.char_indices() {
        if column == location.column {
            return Some(line_start + offset);
        }
        if ch == '\n' {
            break;
        }
        column += 1;
    }

    if column == location.column {
        return Some(line_end);
    }

    None
}

fn byte_range_for_span(sql: &str, line_starts: &[usize], span: Span) -> Option<(usize, usize)> {
    if span == Span::empty() {
        return None;
    }

    let start = byte_index_for_location(sql, line_starts, span.start)?;
    let end_inclusive = byte_index_for_location(sql, line_starts, span.end)?;
    let end = if end_inclusive >= sql.len() {
        sql.len()
    } else {
        let ch = sql[end_inclusive..].chars().next()?;
        end_inclusive + ch.len_utf8()
    };

    Some((start, end))
}

fn tail_has_separator_or_eof(mut tail: &str) -> bool {
    loop {
        tail = tail.trim_start_matches(char::is_whitespace);
        if tail.is_empty() || tail.starts_with(';') {
            return true;
        }

        if let Some(rest) = tail.strip_prefix("--") {
            match rest.find('\n') {
                Some(idx) => {
                    tail = &rest[idx + 1..];
                    continue;
                },
                None => return true,
            }
        }

        if let Some(rest) = tail.strip_prefix("/*") {
            match rest.find("*/") {
                Some(idx) => {
                    tail = &rest[idx + 2..];
                    continue;
                },
                None => return false,
            }
        }

        return false;
    }
}

fn parse_batch_with_sqlparser(sql: &str) -> Result<Vec<ParsedExecutionStatement>, BatchParseError> {
    let dialect = KalamDbDialect::default();
    let statements =
        parse_sql_statements(sql, &dialect).map_err(|err| BatchParseError::new(err.to_string()))?;

    if statements.is_empty() {
        return Ok(Vec::new());
    }

    let line_starts = line_start_offsets(sql);
    let mut parsed = Vec::with_capacity(statements.len());

    for statement in statements {
        let (start, end) = byte_range_for_span(sql, &line_starts, statement.span())
            .ok_or_else(|| BatchParseError::new("Failed to resolve sqlparser statement span"))?;
        let raw_slice = sql[start..end].trim();
        if !raw_slice.ends_with(';') && !tail_has_separator_or_eof(&sql[end..]) {
            return Err(BatchParseError::new(
                "sqlparser statement span did not reach a valid statement boundary",
            ));
        }

        let slice = raw_slice.trim_end_matches(';').trim_end();
        if !slice.is_empty() {
            let reparsed = parse_sql_statements(slice, &dialect).map_err(|_| {
                BatchParseError::new(
                    "sqlparser statement span did not contain a valid standalone statement",
                )
            })?;
            if reparsed.len() != 1 {
                return Err(BatchParseError::new(
                    "sqlparser statement span resolved to multiple statements",
                ));
            }
            parsed.push(ParsedExecutionStatement {
                sql: slice.to_string(),
                execute_as_username: None,
                parsed_statement: reparsed.into_iter().next(),
            });
        }
    }

    Ok(parsed)
}

/// Parse a full SQL payload in one sqlparser pass and return the original
/// statement slices when possible.
///
/// Falls back to the legacy splitter for KalamDB-specific top-level syntax that
/// sqlparser does not yet recognize, such as `EXECUTE AS USER` envelopes.
pub fn parse_batch_statements(sql: &str) -> Result<Vec<String>, BatchParseError> {
    match parse_batch_with_sqlparser(sql) {
        Ok(statements) => Ok(statements.into_iter().map(|statement| statement.sql).collect()),
        Err(_) => split_statements(sql),
    }
}

/// Split a SQL batch payload into individual statements.
///
/// Preserves statement order and whitespace while ignoring semicolons that
/// appear within quoted strings or comments.
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::split_statements;
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
    use super::{
        parse_batch_statements, parse_execution_batch, parse_execution_statement,
        prepare_execution_batch, split_statements, ExecutionBatchParseError,
    };
    use kalamdb_commons::models::Username;

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

    #[test]
    fn parse_batch_statements_uses_sqlparser_spans() {
        let sql = "SELECT 1;\nINSERT INTO demo VALUES (1, 'x');\nSELECT 3";
        let statements = parse_batch_statements(sql).unwrap();
        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0], "SELECT 1");
        assert_eq!(statements[1], "INSERT INTO demo VALUES (1, 'x')");
        assert_eq!(statements[2], "SELECT 3");
    }

    #[test]
    fn parse_batch_statements_falls_back_for_execute_as() {
        let sql = "EXECUTE AS USER alice (SELECT 1); SELECT 2;";
        let statements = parse_batch_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "EXECUTE AS USER alice (SELECT 1)");
        assert_eq!(statements[1], "SELECT 2");
    }

    #[test]
    fn parse_batch_statements_preserves_file_placeholder_statement() {
        let sql = "INSERT INTO uploads SELECT * FROM FILE('orders.csv');";
        let statements = parse_batch_statements(sql).unwrap();
        assert_eq!(statements, vec!["INSERT INTO uploads SELECT * FROM FILE('orders.csv')"]);
    }

    #[test]
    fn parse_batch_statements_preserves_create_table_prefix_when_spans_are_partial() {
        let sql = "CREATE TABLE demo.notes (id BIGINT PRIMARY KEY, content TEXT) WITH (TYPE='USER', STORAGE_ID='local'); SELECT 1;";
        let statements = parse_batch_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(
            statements[0],
            "CREATE TABLE demo.notes (id BIGINT PRIMARY KEY, content TEXT) WITH (TYPE='USER', STORAGE_ID='local')"
        );
        assert_eq!(statements[1], "SELECT 1");
    }

    #[test]
    fn parse_execution_statement_extracts_execute_as() {
        let parsed = parse_execution_statement(
            "EXECUTE AS USER 'alice' (SELECT * FROM default.todos WHERE id = 1);",
        )
        .unwrap();

        assert_eq!(parsed.execute_as_username, Some(Username::from("alice")));
        assert_eq!(parsed.sql, "SELECT * FROM default.todos WHERE id = 1");
        assert!(parsed.parsed_statement.is_some());
    }

    #[test]
    fn parse_execution_batch_reports_statement_index() {
        let err = parse_execution_batch("SELECT 1; EXECUTE AS USER '' (SELECT 2)").unwrap_err();
        assert_eq!(
            err,
            ExecutionBatchParseError::Statement {
                statement_index: 2,
                message: "EXECUTE AS USER username cannot be empty".to_string(),
            }
        );
    }

    #[test]
    fn prepare_execution_batch_preserves_execute_as_username() {
        let prepared =
            prepare_execution_batch("SELECT 1; EXECUTE AS USER alice (SELECT 2)", |statement| {
                Ok::<_, String>(statement.sql.clone())
            })
            .unwrap();

        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].prepared_statement, "SELECT 1".to_string());
        assert_eq!(prepared[0].execute_as_username, None);
        assert!(prepared[0].parsed_statement.is_some());
        assert_eq!(prepared[1].prepared_statement, "SELECT 2".to_string());
        assert_eq!(prepared[1].execute_as_username, Some(Username::from("alice")));
        assert!(prepared[1].parsed_statement.is_some());
    }
}
