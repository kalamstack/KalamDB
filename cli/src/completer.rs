//! TAB completion for SQL keywords and table names
//!
//! **Implements T088**: AutoCompleter for rustyline TAB completion
//! **Implements T114b**: Enhanced autocomplete with table and column names
//! **Enhanced**: Styled completions with Warp-like design
//!
//! Provides intelligent autocompletion for SQL keywords, table names, column names,
//! and backslash commands with context-aware suggestions and beautiful styling.

use std::collections::HashMap;

use colored::*;
use rustyline::completion::{Completer, Pair};

pub(crate) const SQL_KEYWORDS: &[&str] = &[
    // DML
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "FROM",
    "WHERE",
    "JOIN",
    "ON",
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "BETWEEN",
    "IS",
    "NULL",
    "AS",
    "ORDER",
    "BY",
    "GROUP",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "DISTINCT",
    "VALUES",
    "SET",
    "DESCRIBE",
    "SHOW",
    "EXPLAIN",
    // DDL
    "CREATE",
    "DROP",
    "ALTER",
    "TABLE",
    "INDEX",
    "VIEW",
    "DATABASE",
    "SCHEMA",
    // Constraints
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "UNIQUE",
    "DEFAULT",
    "CHECK",
    "AUTO_INCREMENT",
    // Functions
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "COALESCE",
    "CAST",
    "CONCAT",
    "LENGTH",
    "UPPER",
    "LOWER",
    "TRIM",
    "NOW",
    "CURRENT_TIMESTAMP",
    // Types
    "INTEGER",
    "BIGINT",
    "TEXT",
    "VARCHAR",
    "BOOLEAN",
    "TIMESTAMP",
    "FLOAT",
    "DOUBLE",
    "JSON",
];

pub(crate) const SQL_TYPES: &[&str] = &[
    "INTEGER",
    "BIGINT",
    "TEXT",
    "VARCHAR",
    "BOOLEAN",
    "TIMESTAMP",
    "FLOAT",
    "DOUBLE",
    "JSON",
];

/// Styled completion candidate
#[derive(Debug, Clone)]
pub struct StyledPair {
    /// Display text (with styling)
    display: String,
    /// Replacement text (plain)
    replacement: String,
}

impl StyledPair {
    fn new(text: String, category: CompletionCategory) -> Self {
        let display = match category {
            CompletionCategory::Keyword => {
                format!("{}  {}", text.blue().bold(), "keyword".dimmed())
            },
            CompletionCategory::Table => format!("{}  {}", text.green(), "table".dimmed()),
            CompletionCategory::Column => format!("{}  {}", text.yellow(), "column".dimmed()),
            CompletionCategory::MetaCommand => {
                format!("{}  {}", text.cyan().bold(), "command".dimmed())
            },
            CompletionCategory::Type => format!("{}  {}", text.magenta(), "type".dimmed()),
            CompletionCategory::Namespace => {
                format!("{}  {}", text.cyan(), "namespace".dimmed())
            },
        };

        Self {
            display,
            replacement: text,
        }
    }
}

/// Category of completion for styling
#[derive(Debug, Clone, Copy)]
enum CompletionCategory {
    Keyword,
    Table,
    Column,
    MetaCommand,
    Type,
    Namespace,
}

/// Auto-completer for SQL and meta-commands
pub struct AutoCompleter {
    /// SQL keywords for completion
    keywords: Vec<String>,

    /// Meta-commands for completion
    meta_commands: Vec<String>,

    /// Cached table names
    tables: Vec<String>,

    /// Cached column names per table (table_name -> Vec<column_name>)
    columns: HashMap<String, Vec<String>>,

    /// Cached namespaces
    namespaces: Vec<String>,

    /// Cached tables per namespace (namespace -> Vec<table_name>)
    ns_tables: HashMap<String, Vec<String>>,
}

impl AutoCompleter {
    /// Create a new auto-completer
    pub fn new() -> Self {
        let keywords = SQL_KEYWORDS.iter().map(|s| s.to_string()).collect::<Vec<String>>();

        let meta_commands = vec![
            "\\quit",
            "\\q",
            "\\help",
            "\\?",
            "\\history",
            "\\h",
            "\\sessions",
            "\\stats",
            "\\metrics",
            "\\flush",
            "\\health",
            "\\pause",
            "\\continue",
            "\\dt",
            "\\tables",
            "\\d",
            "\\describe",
            "\\format",
            "\\refresh-tables",
            "\\show-credentials",
            "\\credentials",
            "\\update-credentials",
            "\\delete-credentials",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        Self {
            keywords,
            meta_commands,
            tables: Vec::new(),
            columns: HashMap::new(),
            namespaces: Vec::new(),
            ns_tables: HashMap::new(),
        }
    }

    /// Update cached table names
    pub fn set_tables(&mut self, tables: Vec<String>) {
        self.tables = tables;
    }

    /// Update cached namespaces
    pub fn set_namespaces(&mut self, namespaces: Vec<String>) {
        self.namespaces = namespaces;
    }

    /// Update cached tables for a given namespace
    pub fn set_namespace_tables(&mut self, namespace: String, tables: Vec<String>) {
        self.ns_tables.insert(namespace, tables);
    }

    /// Provide a completion hint for inline suggestions
    pub fn completion_hint(&self, line: &str, pos: usize) -> Option<String> {
        let start = line[..pos]
            .rfind(|c: char| c.is_whitespace() || c == '(' || c == '.')
            .map(|i| i + 1)
            .unwrap_or(0);

        let word = &line[start..pos];
        if word.is_empty() {
            return None;
        }

        let candidates = self.get_styled_completions(word, line, pos);
        for candidate in candidates {
            if candidate.replacement.len() <= word.len() {
                continue;
            }

            let suffix = &candidate.replacement[word.len()..];
            if word.chars().all(|c| c.is_ascii_lowercase()) {
                return Some(suffix.to_ascii_lowercase());
            } else {
                return Some(suffix.to_string());
            }
        }

        None
    }

    /// Update cached column names for a table
    pub fn set_columns(&mut self, table: String, cols: Vec<String>) {
        self.columns.insert(table, cols);
    }

    /// Clear all cached column information
    pub fn clear_columns(&mut self) {
        self.columns.clear();
    }

    /// Detect completion context from the line
    fn detect_context(&self, line: &str, pos: usize) -> CompletionContext {
        let line_upper = line.to_uppercase();

        // Check if we're after FROM or JOIN (table name context)
        if line_upper.contains(" FROM ") || line_upper.contains(" JOIN ") {
            // Consider the token currently being edited (up to pos)
            let upto = &line[..pos];
            if let Some(dot_pos) = upto.rfind('.') {
                // Extract table name before the dot
                let before_dot = &upto[..dot_pos];
                if let Some(word_start) =
                    before_dot.rfind(|c: char| c.is_whitespace() || c == '(' || c == ',')
                {
                    let table_name = before_dot[word_start + 1..].trim().to_string();
                    // If the token before dot matches a known namespace, assume namespace.table
                    // completion
                    let ns_upper = table_name.to_ascii_uppercase();
                    if self.namespaces.iter().any(|ns| ns.to_ascii_uppercase() == ns_upper) {
                        return CompletionContext::NamespaceTable(table_name);
                    }
                    return CompletionContext::Column(table_name);
                }
            }
            return CompletionContext::Table;
        }

        // Default to keyword/table mixed context
        CompletionContext::Mixed
    }

    /// Get completions with styling for display
    fn get_styled_completions(&self, input: &str, line: &str, pos: usize) -> Vec<StyledPair> {
        let input_upper = input.to_uppercase();
        let mut results = Vec::new();

        // Check for meta-commands
        if input.starts_with('\\') {
            for cmd in &self.meta_commands {
                if cmd.to_uppercase().starts_with(&input_upper) {
                    results.push(StyledPair::new(cmd.clone(), CompletionCategory::MetaCommand));
                }
            }
            return results;
        }

        let context = self.detect_context(line, pos);

        match context {
            CompletionContext::Table => {
                // Only suggest table names
                // If input contains a dot, try namespace.table suggestions
                if let Some(dot_idx) = input.find('.') {
                    let (ns_part, tbl_part) = input.split_at(dot_idx);
                    let ns_part_upper = ns_part.to_ascii_uppercase();
                    let tbl_part = tbl_part.trim_start_matches('.');
                    let tbl_part_upper = tbl_part.to_ascii_uppercase();

                    // Exact namespace match
                    if let Some((ns_name, tables)) =
                        self.ns_tables.iter().find(|(k, _)| k.to_ascii_uppercase() == ns_part_upper)
                    {
                        for t in tables {
                            let candidate = if t.contains('.') {
                                t.clone() // already fully qualified
                            } else {
                                format!("{}.{}", ns_name, t)
                            };

                            if candidate.to_ascii_uppercase().starts_with(&tbl_part_upper) {
                                results.push(StyledPair::new(candidate, CompletionCategory::Table));
                            }
                        }
                    }

                    // Also suggest namespaces (ns.) if partial matches
                    for ns in &self.namespaces {
                        if ns.to_ascii_uppercase().starts_with(&ns_part_upper) {
                            results.push(StyledPair::new(
                                format!("{}.", ns),
                                CompletionCategory::Namespace,
                            ));
                        }
                    }
                } else {
                    // No dot → offer namespaces (as ns.) and bare tables
                    for ns in &self.namespaces {
                        if ns.to_ascii_uppercase().starts_with(&input_upper) {
                            results.push(StyledPair::new(
                                format!("{}.", ns),
                                CompletionCategory::Namespace,
                            ));
                        }
                    }
                    for table in &self.tables {
                        if table.to_uppercase().starts_with(&input_upper) {
                            results.push(StyledPair::new(table.clone(), CompletionCategory::Table));
                        }
                    }
                }
            },
            CompletionContext::Column(ref table_name) => {
                // Only suggest column names for the specific table
                if let Some(cols) = self.columns.get(table_name) {
                    for col in cols {
                        if col.to_uppercase().starts_with(&input_upper) {
                            results.push(StyledPair::new(col.clone(), CompletionCategory::Column));
                        }
                    }
                }
            },
            CompletionContext::NamespaceTable(ref namespace) => {
                if let Some(cols) = self.ns_tables.get(namespace) {
                    // Here `cols` are tables within the namespace
                    for t in cols {
                        let composite = if t.contains('.') {
                            t.clone() // already qualified, avoid double namespace
                        } else {
                            format!("{}.{}", namespace, t)
                        };
                        if composite.to_ascii_uppercase().starts_with(&input_upper) {
                            results.push(StyledPair::new(composite, CompletionCategory::Table));
                        }
                    }
                }
            },
            CompletionContext::Mixed => {
                // Suggest keywords
                for kw in &self.keywords {
                    if kw.starts_with(&input_upper) {
                        let category = if Self::is_type(kw) {
                            CompletionCategory::Type
                        } else {
                            CompletionCategory::Keyword
                        };
                        results.push(StyledPair::new(kw.clone(), category));
                    }
                }

                // Suggest table names
                for table in &self.tables {
                    if table.to_uppercase().starts_with(&input_upper) {
                        results.push(StyledPair::new(table.clone(), CompletionCategory::Table));
                    }
                }
            },
        }

        results.sort_by(|a, b| a.replacement.cmp(&b.replacement));
        results.dedup_by(|a, b| a.replacement == b.replacement);
        results
    }

    /// Check if a keyword is a type
    fn is_type(word: &str) -> bool {
        SQL_TYPES.contains(&word)
    }
}

/// Completion context for context-aware suggestions
#[derive(Debug)]
enum CompletionContext {
    /// Mixed keywords and tables
    Mixed,
    /// Table name context (after FROM/JOIN)
    Table,
    /// Column name context (after table.)
    Column(String),
    /// Namespace-qualified table context (after FROM/JOIN with ns.)
    NamespaceTable(String),
}

impl Default for AutoCompleter {
    fn default() -> Self {
        Self::new()
    }
}

impl Completer for AutoCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Find the start of the current word
        // Compute token start. In Table context keep namespace prefix (do not split on '.')
        let ctx = self.detect_context(line, pos);
        let start = match ctx {
            CompletionContext::Table | CompletionContext::NamespaceTable(_) => line[..pos]
                .rfind(|c: char| c.is_whitespace() || c == '(' || c == ',')
                .map(|i| i + 1)
                .unwrap_or(0),
            _ => line[..pos]
                .rfind(|c: char| c.is_whitespace() || c == '(' || c == '.')
                .map(|i| i + 1)
                .unwrap_or(0),
        };

        let word = &line[start..pos];
        let styled_completions = self.get_styled_completions(word, line, pos);

        let pairs: Vec<Pair> = styled_completions
            .into_iter()
            .map(|s| Pair {
                display: s.display,
                replacement: s.replacement,
            })
            .collect();

        Ok((start, pairs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_completion() {
        let completer = AutoCompleter::new();
        let line = "SEL";
        let completions = completer.get_styled_completions("SEL", line, line.len());
        assert!(completions.iter().any(|c| c.replacement == "SELECT"));
    }

    #[test]
    fn test_meta_command_completion() {
        let completer = AutoCompleter::new();
        let line = "\\q";
        let completions = completer.get_styled_completions("\\q", line, line.len());
        assert!(completions.iter().any(|c| c.replacement == "\\quit"));
        assert!(completions.iter().any(|c| c.replacement == "\\q"));
    }

    #[test]
    fn test_table_completion() {
        let mut completer = AutoCompleter::new();
        completer.set_tables(vec!["users".to_string(), "user_sessions".to_string()]);

        let line = "user";
        let completions = completer.get_styled_completions("user", line, line.len());
        assert!(completions.iter().any(|c| c.replacement == "users"));
        assert!(completions.iter().any(|c| c.replacement == "user_sessions"));
    }

    #[test]
    fn test_context_aware_table_completion() {
        let mut completer = AutoCompleter::new();
        completer.set_tables(vec!["users".to_string(), "messages".to_string()]);

        // After FROM, should only suggest tables
        let line = "SELECT * FROM me";
        let completions = completer.get_styled_completions("me", line, line.len());
        assert!(completions.iter().any(|c| c.replacement == "messages"));
    }

    #[test]
    fn test_column_completion() {
        let mut completer = AutoCompleter::new();
        completer.set_tables(vec!["users".to_string()]);
        completer.set_columns(
            "users".to_string(),
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );

        // After table., should suggest columns (with FROM keyword present)
        let line = "SELECT users.na FROM users";
        let completions = completer.get_styled_completions("na", line, line.len());
        assert!(completions.iter().any(|c| c.replacement == "name"));
    }
}
