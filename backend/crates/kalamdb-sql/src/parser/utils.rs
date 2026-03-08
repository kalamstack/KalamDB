//! Common parsing utilities
//!
//! This module provides shared parsing helpers to avoid code duplication across
//! custom parsers (CREATE STORAGE, STORAGE FLUSH, KILL JOB, etc.).

use kalamdb_commons::TableId;
use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast::{ObjectNamePart, Statement, TableFactor, TableObject};
use sqlparser::dialect::Dialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError, ParserOptions};
use sqlparser::tokenizer::{Span, Token};

const DEFAULT_SQL_RECURSION_LIMIT: usize = 512;

static CURRENT_USER_CALL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bCURRENT_USER\s*\(\s*\)").expect("valid regex"));
static CURRENT_ROLE_CALL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bCURRENT_ROLE\s*\(\s*\)").expect("valid regex"));
static CURRENT_USER_ID_CALL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bCURRENT_USER_ID\s*\(\s*\)").expect("valid regex"));

/// Default sqlparser options used across KalamDB
pub fn parser_options() -> ParserOptions {
    ParserOptions::new().with_trailing_commas(true)
}

/// Normalize SQL-standard context keyword calls for `sqlparser-rs`.
///
/// `sqlparser-rs` treats `CURRENT_USER` and `CURRENT_ROLE` as reserved keywords,
/// not regular zero-argument function calls. Queries like `SELECT CURRENT_USER()`
/// therefore fail parsing unless the empty parentheses are stripped first.
pub fn normalize_context_keyword_calls_for_sqlparser(sql: &str) -> String {
    let sql = CURRENT_USER_CALL_RE.replace_all(sql, "CURRENT_USER");
    CURRENT_ROLE_CALL_RE.replace_all(&sql, "CURRENT_ROLE").into_owned()
}

/// Rewrite public context function spellings to KalamDB's internal DataFusion UDFs.
///
/// These aliases let user-facing SQL use `CURRENT_USER()`, `CURRENT_USER_ID()`, and
/// `CURRENT_ROLE()` while execution resolves to the registered `KDB_*` functions.
pub fn rewrite_context_functions_for_datafusion(sql: &str) -> String {
    let sql = CURRENT_USER_ID_CALL_RE.replace_all(sql, "KDB_CURRENT_USER_ID()");
    let sql = CURRENT_USER_CALL_RE.replace_all(&sql, "KDB_CURRENT_USER()");
    CURRENT_ROLE_CALL_RE
        .replace_all(&sql, "KDB_CURRENT_ROLE()")
        .into_owned()
}

/// Parse SQL into statements using KalamDB defaults (options + recursion limit)
pub fn parse_sql_statements(
    sql: &str,
    dialect: &dyn Dialect,
) -> Result<Vec<Statement>, ParserError> {
    let sql = normalize_context_keyword_calls_for_sqlparser(sql);
    Parser::new(dialect)
        .with_options(parser_options())
        .with_recursion_limit(DEFAULT_SQL_RECURSION_LIMIT)
        .try_with_sql(&sql)?
        .parse_statements()
}

/// Parse a single SQL statement using KalamDB defaults and GenericDialect.
pub fn parse_single_statement(sql: &str) -> Result<Option<Statement>, ParserError> {
    let dialect = GenericDialect {};
    let mut statements = parse_sql_statements(sql, &dialect)?;
    if statements.len() != 1 {
        return Ok(None);
    }
    Ok(statements.pop())
}

/// Extract the target table for INSERT/UPDATE/DELETE DML statements.
///
/// Returns None if parsing fails or the statement is not INSERT/UPDATE/DELETE.
pub fn extract_dml_table_id(sql: &str, default_namespace: &str) -> Option<TableId> {
    let dialect = GenericDialect {};
    let statements = parse_sql_statements(sql, &dialect).ok()?;
    statements
        .first()
        .and_then(|statement| extract_dml_table_id_from_statement(statement, default_namespace))
}

/// Extract the target table for INSERT/UPDATE/DELETE from a parsed SQL statement.
pub fn extract_dml_table_id_from_statement(
    statement: &Statement,
    default_namespace: &str,
) -> Option<TableId> {
    match statement {
        Statement::Insert(insert) => {
            let table_parts: Vec<String> = match &insert.table {
                TableObject::TableName(obj_name) => obj_name
                    .0
                    .iter()
                    .filter_map(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })
                    .collect(),
                _ => return None,
            };
            table_id_from_parts(&table_parts, default_namespace)
        },
        Statement::Update(sqlparser::ast::Update { table, .. }) => match &table.relation {
            TableFactor::Table { name, .. } => {
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .filter_map(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })
                    .collect();
                table_id_from_parts(&parts, default_namespace)
            },
            _ => None,
        },
        Statement::Delete(delete) => {
            let table_name = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => match tables.first() {
                    Some(table_with_joins) => match &table_with_joins.relation {
                        TableFactor::Table { name, .. } => name,
                        _ => return None,
                    },
                    None => return None,
                },
            };

            let parts: Vec<String> = table_name
                .0
                .iter()
                .filter_map(|part| match part {
                    ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                    _ => None,
                })
                .collect();
            table_id_from_parts(&parts, default_namespace)
        },
        _ => None,
    }
}

fn table_id_from_parts(parts: &[String], default_namespace: &str) -> Option<TableId> {
    match parts.len() {
        1 => Some(TableId::from_strings(default_namespace, &parts[0])),
        2 => Some(TableId::from_strings(&parts[0], &parts[1])),
        _ => None,
    }
}

/// Collect non-whitespace tokens using sqlparser's parser lookahead.
///
/// This avoids ad-hoc string splitting and provides consistent tokenization
/// across the codebase.
pub fn collect_non_whitespace_tokens(
    sql: &str,
    dialect: &dyn Dialect,
) -> Result<Vec<Token>, ParserError> {
    let parser = Parser::new(dialect)
        .with_options(parser_options())
        .with_recursion_limit(DEFAULT_SQL_RECURSION_LIMIT)
        .try_with_sql(sql)?;

    let mut tokens = Vec::new();
    let mut idx = 0;
    loop {
        let token_with_span = parser.peek_nth_token_ref(idx);
        let token = token_with_span.token.clone();
        if matches!(token, Token::EOF) {
            break;
        }
        tokens.push(token);
        idx += 1;
    }

    Ok(tokens)
}

/// Extract uppercased keyword-ish tokens (WORD + NUMBER) for command classification.
pub fn tokens_to_words(tokens: &[Token]) -> Vec<String> {
    tokens
        .iter()
        .filter_map(|tok| match tok {
            Token::Word(word) => Some(word.value.to_ascii_uppercase()),
            Token::Number(value, _) => Some(value.to_ascii_uppercase()),
            _ => None,
        })
        .collect()
}

/// Format a source span for user-facing error messages.
pub fn format_span(span: Span) -> String {
    format!("line {}, col {}", span.start.line, span.start.column)
}

/// Normalize SQL by removing extra whitespace and semicolons
///
/// Converts multiple spaces, tabs, newlines into single spaces and removes trailing semicolons.
///
/// # Examples
///
/// ```
/// use kalamdb_sql::parser::utils::normalize_sql;
///
/// let sql = "CREATE   STORAGE\n  s3_prod  ;";
/// assert_eq!(normalize_sql(sql), "CREATE STORAGE s3_prod");
/// ```
pub fn normalize_sql(sql: &str) -> String {
    let mut normalized = String::new();
    for part in sql.trim().trim_end_matches(';').split_whitespace() {
        if !normalized.is_empty() {
            normalized.push(' ');
        }
        normalized.push_str(part);
    }
    normalized
}

/// Extract a quoted string value after a keyword
///
/// Finds keyword (case-insensitive whole-word match) and extracts the quoted string after it.
///
/// # Examples
///
/// ```
/// use kalamdb_sql::parser::utils::extract_quoted_keyword_value;
///
/// let sql = "CREATE STORAGE s3 NAME 'Production Storage'";
/// let name = extract_quoted_keyword_value(sql, "NAME").unwrap();
/// assert_eq!(name, "Production Storage");
/// ```
///
/// # Errors
///
/// Returns error if:
/// - Keyword not found
/// - No quote after keyword
/// - Unclosed quote
pub fn extract_quoted_keyword_value(sql: &str, keyword: &str) -> Result<String, String> {
    let keyword_upper = keyword.to_ascii_uppercase();
    let sql_upper = sql.to_ascii_uppercase();

    // Find keyword as a whole word (surrounded by whitespace or start/end)
    let keyword_pos = find_whole_word(&sql_upper, &keyword_upper)
        .ok_or_else(|| format!("{} keyword not found", keyword))?;

    let after_keyword = &sql[keyword_pos + keyword.len()..];

    // Find the quoted value
    let quote_start = after_keyword
        .find('\'')
        .ok_or_else(|| format!("Missing quoted value after {}", keyword))?;

    let after_quote = &after_keyword[quote_start + 1..];
    let quote_end = after_quote
        .find('\'')
        .ok_or_else(|| format!("Unclosed quote after {}", keyword))?;

    Ok(after_quote[..quote_end].to_string())
}

/// Extract an unquoted keyword value (allows both quoted and unquoted)
///
/// Similar to `extract_quoted_keyword_value` but also accepts unquoted values.
///
/// # Examples
///
/// ```
/// use kalamdb_sql::parser::utils::extract_keyword_value;
///
/// // Quoted value
/// let sql = "CREATE STORAGE s3 TYPE 's3' NAME 'Prod'";
/// assert_eq!(extract_keyword_value(sql, "TYPE").unwrap(), "s3");
///
/// // Unquoted value
/// let sql2 = "CREATE STORAGE s3 TYPE filesystem";
/// assert_eq!(extract_keyword_value(sql2, "TYPE").unwrap(), "filesystem");
/// ```
pub fn extract_keyword_value(sql: &str, keyword: &str) -> Result<String, String> {
    let keyword_upper = keyword.to_ascii_uppercase();
    let sql_upper = sql.to_ascii_uppercase();

    let keyword_pos = find_whole_word(&sql_upper, &keyword_upper)
        .ok_or_else(|| format!("{} keyword not found", keyword))?;

    let after_keyword = &sql[keyword_pos + keyword.len()..];
    let trimmed = after_keyword.trim();

    if let Some(after_quote) = trimmed.strip_prefix('\'') {
        // Quoted value
        let quote_end = after_quote
            .find('\'')
            .ok_or_else(|| format!("Unclosed quote after {}", keyword))?;
        Ok(after_quote[..quote_end].to_string())
    } else {
        // Unquoted value - extract next whitespace-separated token
        let value = trimmed
            .split_whitespace()
            .next()
            .ok_or_else(|| format!("Missing value after {}", keyword))?;
        Ok(value.to_string())
    }
}

/// Extract a storage/table/namespace identifier after N tokens
///
/// Skips N tokens and extracts the next identifier.
///
/// # Examples
///
/// ```
/// use kalamdb_sql::parser::utils::extract_identifier;
///
/// let sql = "CREATE STORAGE s3_prod";
/// let id = extract_identifier(sql, 2).unwrap(); // Skip "CREATE" and "STORAGE"
/// assert_eq!(id, "s3_prod");
/// ```
pub fn extract_identifier(sql: &str, skip_tokens: usize) -> Result<String, String> {
    sql.split_whitespace()
        .nth(skip_tokens)
        .map(|s| s.to_string())
        .ok_or_else(|| format!("Expected identifier after {} tokens", skip_tokens))
}

/// Extract a qualified table name (namespace.table_name)
///
/// Parses a fully qualified table reference and returns (namespace, table_name).
///
/// # Examples
///
/// ```
/// use kalamdb_sql::parser::utils::extract_qualified_table;
///
/// let (ns, table) = extract_qualified_table("prod.events").unwrap();
/// assert_eq!(ns, "prod");
/// assert_eq!(table, "events");
/// ```
///
/// # Errors
///
/// Returns error if:
/// - Table reference is not qualified (missing namespace)
/// - Invalid format
pub fn extract_qualified_table(table_ref: &str) -> Result<(String, String), String> {
    let (namespace, table) = table_ref.split_once('.').ok_or_else(|| {
        "Table name must be qualified as exactly namespace.table_name".to_string()
    })?;
    if table.contains('.') {
        return Err("Table name must be qualified as exactly namespace.table_name".to_string());
    }
    Ok((namespace.to_string(), table.to_string()))
}

/// Find whole-word match in a string (case-insensitive)
///
/// Returns the starting position of the keyword if found as a complete word.
/// A complete word is surrounded by whitespace or start/end of string.
fn find_whole_word(haystack: &str, needle: &str) -> Option<usize> {
    let mut search_pos = 0;
    loop {
        match haystack[search_pos..].find(needle) {
            Some(pos) => {
                let absolute_pos = search_pos + pos;
                let is_word_start = absolute_pos == 0
                    || haystack.chars().nth(absolute_pos - 1).unwrap().is_whitespace();
                let is_word_end = absolute_pos + needle.len() >= haystack.len()
                    || haystack.chars().nth(absolute_pos + needle.len()).unwrap().is_whitespace();

                if is_word_start && is_word_end {
                    return Some(absolute_pos);
                } else {
                    search_pos = absolute_pos + 1;
                }
            },
            None => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;

    #[test]
    fn test_normalize_sql() {
        assert_eq!(normalize_sql("  SELECT  * ;"), "SELECT *");
        assert_eq!(normalize_sql("CREATE\n  STORAGE\n  s3  "), "CREATE STORAGE s3");
    }

    #[test]
    fn test_extract_quoted_keyword_value() {
        let sql = "CREATE STORAGE s3 NAME 'Production Storage'";
        assert_eq!(extract_quoted_keyword_value(sql, "NAME").unwrap(), "Production Storage");

        // Keyword not found
        assert!(extract_quoted_keyword_value(sql, "MISSING").is_err());

        // Missing quote
        let bad_sql = "NAME value";
        assert!(extract_quoted_keyword_value(bad_sql, "NAME").is_err());
    }

    #[test]
    fn test_parse_sql_statements_with_trailing_commas() {
        let dialect = GenericDialect {};
        let sql = "SELECT a, b, FROM table_1";
        let statements = parse_sql_statements(sql, &dialect).unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn test_collect_tokens_and_words() {
        let dialect = GenericDialect {};
        let tokens = collect_non_whitespace_tokens("USE NAMESPACE demo", &dialect).unwrap();
        let words = tokens_to_words(&tokens);
        assert!(words.starts_with(&["USE".to_string(), "NAMESPACE".to_string()]));
    }

    #[test]
    fn test_extract_keyword_value() {
        // Quoted
        let sql = "TYPE 's3'";
        assert_eq!(extract_keyword_value(sql, "TYPE").unwrap(), "s3");

        // Unquoted
        let sql2 = "TYPE filesystem";
        assert_eq!(extract_keyword_value(sql2, "TYPE").unwrap(), "filesystem");
    }

    #[test]
    fn test_extract_identifier() {
        let sql = "CREATE STORAGE s3_prod";
        assert_eq!(extract_identifier(sql, 2).unwrap(), "s3_prod");

        // Not enough tokens
        assert!(extract_identifier(sql, 10).is_err());
    }

    #[test]
    fn test_extract_qualified_table() {
        let (ns, table) = extract_qualified_table("prod.events").unwrap();
        assert_eq!(ns, "prod");
        assert_eq!(table, "events");

        // Not qualified
        assert!(extract_qualified_table("events").is_err());
        assert!(extract_qualified_table("a.b.c").is_err());
    }

    #[test]
    fn test_find_whole_word() {
        let text = "CREATE TABLE tables IN namespace";
        assert_eq!(find_whole_word(&text.to_uppercase(), "TABLE"), Some(7));
        assert_eq!(find_whole_word(&text.to_uppercase(), "TABLES"), Some(13));
        assert_eq!(find_whole_word(&text.to_uppercase(), "IN"), Some(20));

        // Not a whole word
        assert!(find_whole_word("TABLES", "TABLE").is_none());
    }

    #[test]
    fn test_extract_dml_table_id_insert_update_delete() {
        let insert = extract_dml_table_id("INSERT INTO chat.messages (id) VALUES (1)", "default")
            .expect("insert table id");
        assert_eq!(insert.full_name(), "chat.messages");

        let update = extract_dml_table_id("UPDATE messages SET id = 2 WHERE id = 1", "chat")
            .expect("update table id");
        assert_eq!(update.full_name(), "chat.messages");

        let delete = extract_dml_table_id("DELETE FROM chat.messages WHERE id = 1", "default")
            .expect("delete table id");
        assert_eq!(delete.full_name(), "chat.messages");
    }

    #[test]
    fn test_normalize_context_keyword_calls_for_sqlparser() {
        let normalized = normalize_context_keyword_calls_for_sqlparser(
            "SELECT CURRENT_USER(), CURRENT_ROLE()",
        );
        assert_eq!(normalized, "SELECT CURRENT_USER, CURRENT_ROLE");
    }

    #[test]
    fn test_rewrite_context_functions_for_datafusion() {
        let rewritten = rewrite_context_functions_for_datafusion(
            "SELECT CURRENT_USER(), CURRENT_USER_ID(), CURRENT_ROLE()",
        );
        assert_eq!(
            rewritten,
            "SELECT KDB_CURRENT_USER(), KDB_CURRENT_USER_ID(), KDB_CURRENT_ROLE()"
        );
    }
}
