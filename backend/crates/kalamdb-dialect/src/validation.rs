//! Naming validation for namespaces, tables, and columns.

use kalamdb_commons::constants::{SystemColumnNames, RESERVED_NAMESPACE_NAMES};
use once_cell::sync::Lazy;
use sqlparser::keywords::{
    Keyword, ALL_KEYWORDS, ALL_KEYWORDS_INDEX, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_IDENTIFIER,
    RESERVED_FOR_TABLE_ALIAS, RESERVED_FOR_TABLE_FACTOR,
};
use std::collections::HashSet;

pub static RESERVED_NAMESPACES: Lazy<HashSet<&'static str>> =
    Lazy::new(|| RESERVED_NAMESPACE_NAMES.iter().copied().collect());

pub static RESERVED_COLUMN_NAMES: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    let mut set = HashSet::new();
    set.insert(SystemColumnNames::SEQ);
    set.insert(SystemColumnNames::DELETED);
    set.insert("_id");
    set.insert("_row_id");
    set.insert("_rowid");
    set.insert("_key");
    set.insert("_updated");
    set.insert("_created");
    set.insert("_timestamp");
    set.insert("_version");
    set.insert("_hash");
    set
});

const CRITICAL_RESERVED_KEYWORDS: &[&str] = &[
    "TABLE",
    "TABLES",
    "TABLESPACE",
    "INDEX",
    "VIEW",
    "SCHEMA",
    "DATABASE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
];

pub static RESERVED_SQL_KEYWORDS: Lazy<HashSet<String>> = Lazy::new(|| {
    let mut keywords: HashSet<String> = RESERVED_FOR_TABLE_ALIAS
        .iter()
        .chain(RESERVED_FOR_COLUMN_ALIAS.iter())
        .chain(RESERVED_FOR_TABLE_FACTOR.iter())
        .chain(RESERVED_FOR_IDENTIFIER.iter())
        .map(keyword_to_str)
        .map(|keyword| keyword.to_ascii_uppercase())
        .collect();

    for keyword in CRITICAL_RESERVED_KEYWORDS {
        keywords.insert(keyword.to_ascii_uppercase());
    }

    keywords
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    Empty,
    TooLong(usize),
    StartsWithUnderscore,
    InvalidCharacters(String),
    ReservedNamespace(String),
    ReservedColumnName(String),
    ReservedSqlKeyword(String),
    StartsWithNumber,
    ContainsSpaces,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::Empty => write!(f, "Name cannot be empty"),
            ValidationError::TooLong(len) => {
                write!(f, "Name is too long ({} characters, max 64)", len)
            },
            ValidationError::StartsWithUnderscore => {
                write!(f, "Name cannot start with underscore (reserved for system columns)")
            },
            ValidationError::InvalidCharacters(name) => write!(
                f,
                "Name '{}' contains invalid characters (only alphanumeric and underscore allowed)",
                name
            ),
            ValidationError::ReservedNamespace(name) => {
                write!(f, "Namespace '{}' is reserved and cannot be used", name)
            },
            ValidationError::ReservedColumnName(name) => {
                write!(f, "Column name '{}' is reserved and cannot be used", name)
            },
            ValidationError::ReservedSqlKeyword(name) => {
                write!(f, "Name '{}' is a reserved SQL keyword and cannot be used", name)
            },
            ValidationError::StartsWithNumber => write!(f, "Name cannot start with a number"),
            ValidationError::ContainsSpaces => write!(f, "Name cannot contain spaces"),
        }
    }
}

impl std::error::Error for ValidationError {}

pub const MAX_NAME_LENGTH: usize = 64;

pub fn validate_namespace_name(name: &str) -> Result<(), ValidationError> {
    if is_reserved_case_insensitive(&RESERVED_NAMESPACES, name) {
        return Err(ValidationError::ReservedNamespace(name.to_string()));
    }

    validate_identifier_base(name)?;
    ensure_not_reserved_sql_keyword(name)?;
    Ok(())
}

pub fn validate_table_name(name: &str) -> Result<(), ValidationError> {
    validate_identifier_base(name)?;
    ensure_not_reserved_sql_keyword(name)?;
    Ok(())
}

pub fn validate_column_name(name: &str) -> Result<(), ValidationError> {
    if is_reserved_case_insensitive(&RESERVED_COLUMN_NAMES, name) {
        return Err(ValidationError::ReservedColumnName(name.to_string()));
    }

    validate_identifier_base(name)?;
    ensure_not_reserved_sql_keyword(name)?;
    Ok(())
}

fn validate_identifier_base(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError::Empty);
    }

    if name.len() > MAX_NAME_LENGTH {
        return Err(ValidationError::TooLong(name.len()));
    }

    if name.contains(' ') {
        return Err(ValidationError::ContainsSpaces);
    }

    if name.starts_with('_') {
        return Err(ValidationError::StartsWithUnderscore);
    }

    if name.chars().next().expect("non-empty checked above").is_ascii_digit() {
        return Err(ValidationError::StartsWithNumber);
    }

    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(ValidationError::InvalidCharacters(name.to_string()));
    }

    Ok(())
}

fn ensure_not_reserved_sql_keyword(name: &str) -> Result<(), ValidationError> {
    if RESERVED_SQL_KEYWORDS.contains(name) {
        return Err(ValidationError::ReservedSqlKeyword(name.to_string()));
    }

    if name.bytes().any(|b| b.is_ascii_lowercase()) {
        let uppercase = name.to_ascii_uppercase();
        if RESERVED_SQL_KEYWORDS.contains(uppercase.as_str()) {
            return Err(ValidationError::ReservedSqlKeyword(name.to_string()));
        }
    }

    Ok(())
}

fn is_reserved_case_insensitive(reserved: &HashSet<&'static str>, name: &str) -> bool {
    if reserved.contains(name) {
        return true;
    }

    if name.bytes().any(|b| b.is_ascii_uppercase()) {
        let lowercase = name.to_ascii_lowercase();
        return reserved.contains(lowercase.as_str());
    }

    false
}

fn keyword_to_str(keyword: &Keyword) -> &'static str {
    let index = ALL_KEYWORDS_INDEX
        .iter()
        .position(|entry| entry == keyword)
        .expect("keyword missing from sqlparser ALL_KEYWORDS_INDEX");
    ALL_KEYWORDS[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_namespace_names() {
        assert!(validate_namespace_name("app").is_ok());
        assert!(validate_namespace_name("my_namespace").is_ok());
        assert!(validate_namespace_name("namespace123").is_ok());
        assert!(validate_namespace_name("MyNamespace").is_ok());
    }

    #[test]
    fn test_reserved_namespaces() {
        assert_eq!(
            validate_namespace_name("system"),
            Err(ValidationError::ReservedNamespace("system".to_string()))
        );
        assert_eq!(
            validate_namespace_name("SYSTEM"),
            Err(ValidationError::ReservedNamespace("SYSTEM".to_string()))
        );
    }

    #[test]
    fn test_valid_table_names() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("user_messages").is_ok());
        assert!(validate_table_name("table123").is_ok());
    }

    #[test]
    fn test_valid_column_names() {
        assert!(validate_column_name("user_id").is_ok());
        assert!(validate_column_name("firstName").is_ok());
        assert!(validate_column_name("age").is_ok());
        assert!(validate_column_name("field123").is_ok());
    }

    #[test]
    fn test_invalid_names() {
        assert_eq!(validate_column_name("_custom"), Err(ValidationError::StartsWithUnderscore));
        assert_eq!(validate_table_name("1table"), Err(ValidationError::StartsWithNumber));
        assert_eq!(validate_column_name("user name"), Err(ValidationError::ContainsSpaces));
    }

    #[test]
    fn test_reserved_column_names() {
        assert_eq!(
            validate_column_name(SystemColumnNames::SEQ),
            Err(ValidationError::ReservedColumnName(SystemColumnNames::SEQ.to_string()))
        );
        assert_eq!(
            validate_column_name("_ID"),
            Err(ValidationError::ReservedColumnName("_ID".to_string()))
        );
    }

    #[test]
    fn test_reserved_sql_keywords() {
        assert_eq!(
            validate_table_name("select"),
            Err(ValidationError::ReservedSqlKeyword("select".to_string()))
        );
        assert_eq!(
            validate_namespace_name("WHERE"),
            Err(ValidationError::ReservedSqlKeyword("WHERE".to_string()))
        );
    }
}
