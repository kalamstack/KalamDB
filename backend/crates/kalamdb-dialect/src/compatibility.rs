//! SQL dialect compatibility helpers.
//!
//! This module provides utilities for mapping PostgreSQL/MySQL specific
//! data types into Arrow data types that KalamDB understands.  Centralising
//! these conversions keeps the CREATE TABLE parsers in sync across crates.

use std::string::String;

use arrow::datatypes::{DataType, IntervalUnit};
use kalamdb_commons::models::datatypes::{KalamDataType, ToArrowType};
use sqlparser::ast::{DataType as SQLDataType, DataType::*, ObjectName};

fn map_decimal_kalam_type(info: &sqlparser::ast::ExactNumberInfo) -> Result<KalamDataType, String> {
    let (precision, scale) = match info {
        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            (*precision as u8, *scale as u8)
        },
        sqlparser::ast::ExactNumberInfo::Precision(precision) => (*precision as u8, 0),
        sqlparser::ast::ExactNumberInfo::None => (38, 10),
    };

    KalamDataType::validate_decimal_params(precision, scale).map_err(|error| error.to_string())?;
    Ok(KalamDataType::Decimal { precision, scale })
}

fn custom_type_identifier(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|id| id.to_string().to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn parse_embedding_dimension(modifiers: &[String]) -> Result<u16, String> {
    if modifiers.len() != 1 {
        return Err("EMBEDDING type requires exactly one dimension parameter, e.g., \
                    EMBEDDING(384)"
            .to_string());
    }

    let dim_str = &modifiers[0];
    let dim = dim_str
        .parse::<u16>()
        .map_err(|_| format!("EMBEDDING dimension must be a positive integer, got '{dim_str}'"))?;
    KalamDataType::validate_embedding_dimension(dim).map_err(|error| error.to_string())?;
    Ok(dim)
}

/// Map a parsed `sqlparser` data type into a [`KalamDataType`].
pub fn map_sql_type_to_kalam(sql_type: &SQLDataType) -> Result<KalamDataType, String> {
    match sql_type {
        SmallInt(_) | Int2(_) | TinyInt(_) => Ok(KalamDataType::SmallInt),
        Int(_) | Integer(_) | Int4(_) | MediumInt(_) => Ok(KalamDataType::Int),
        BigInt(_) | Int8(_) | Int64 => Ok(KalamDataType::BigInt),
        Float(_) | Real | Float4 => Ok(KalamDataType::Float),
        SQLDataType::Double(_) | DoublePrecision | Float8 | Float64 => Ok(KalamDataType::Double),
        Boolean | Bool => Ok(KalamDataType::Boolean),
        SQLDataType::JSON | SQLDataType::JSONB => Ok(KalamDataType::Json),
        Character(_)
        | Char(_)
        | CharacterVarying(_)
        | CharVarying(_)
        | Varchar(_)
        | Nvarchar(_)
        | CharacterLargeObject(_)
        | CharLargeObject(_)
        | Clob(_)
        | Text
        | String(_) => Ok(KalamDataType::Text),
        Binary(_) | Varbinary(_) | Blob(_) | Bytes(_) | Bytea => Ok(KalamDataType::Bytes),
        Date => Ok(KalamDataType::Date),
        Timestamp(_, _) => Ok(KalamDataType::Timestamp),
        Datetime(_) => Ok(KalamDataType::DateTime),
        Time(_, _) => Ok(KalamDataType::Time),
        SQLDataType::Uuid => Ok(KalamDataType::Uuid),
        Decimal(info) => map_decimal_kalam_type(info),
        Custom(name, modifiers) => map_custom_kalam_type(name, modifiers),
        Array(_) | Enum(_, _) | Set(_) | Struct(_, _) => Ok(KalamDataType::Text),
        other => Err(format!("Unsupported data type: {other:?}")),
    }
}

/// Map a parsed `sqlparser` data type into Arrow via [`KalamDataType`].
pub fn map_sql_type_to_arrow(sql_type: &SQLDataType) -> Result<DataType, String> {
    match sql_type {
        UnsignedInteger => Ok(DataType::UInt32),
        SQLDataType::Interval { .. } => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        Custom(name, _) if custom_type_identifier(name) == "unsigned" => Ok(DataType::UInt32),
        _ => map_sql_type_to_kalam(sql_type)?
            .to_arrow_type()
            .map_err(|error| error.to_string()),
    }
}

fn map_custom_kalam_type(name: &ObjectName, modifiers: &[String]) -> Result<KalamDataType, String> {
    let ident = custom_type_identifier(name);
    match ident.as_str() {
        "file" => Ok(KalamDataType::File),
        "embedding" => {
            let dim = parse_embedding_dimension(modifiers)?;
            Ok(KalamDataType::Embedding(dim))
        },
        "serial" | "serial4" | "signed" => Ok(KalamDataType::Int),
        "bigserial" | "serial8" => Ok(KalamDataType::BigInt),
        "smallserial" | "serial2" | "int1" | "int2" => Ok(KalamDataType::SmallInt),
        "int4" => Ok(KalamDataType::Int),
        "int8" => Ok(KalamDataType::BigInt),
        other if other.ends_with("text") || other.ends_with("string") => Ok(KalamDataType::Text),
        other => KalamDataType::from_sql_name(other)
            .ok_or_else(|| format!("Unsupported custom data type '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use super::*;

    fn custom(name: &str) -> SQLDataType {
        SQLDataType::Custom(
            ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(Ident::new(name))]),
            vec![],
        )
    }

    fn custom_with_size(name: &str, size: i32) -> SQLDataType {
        SQLDataType::Custom(
            ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(Ident::new(name))]),
            vec![size.to_string()],
        )
    }

    #[test]
    fn maps_postgres_serial_types() {
        assert_eq!(map_sql_type_to_arrow(&custom("serial")).unwrap(), DataType::Int32);
        assert_eq!(map_sql_type_to_arrow(&custom("serial8")).unwrap(), DataType::Int64);
        assert_eq!(map_sql_type_to_arrow(&custom("smallserial")).unwrap(), DataType::Int16);
    }

    #[test]
    fn maps_unsigned_variants() {
        assert_eq!(map_sql_type_to_arrow(&SQLDataType::UnsignedInteger).unwrap(), DataType::UInt32);
    }

    #[test]
    fn rejects_unknown_custom_types() {
        let err = map_sql_type_to_arrow(&custom("geography")).unwrap_err();
        assert!(err.to_string().contains("Unsupported custom data type"));
    }

    #[test]
    fn maps_embedding_type() {
        // Test valid embedding dimensions
        for dim in [384, 768, 1536, 3072] {
            let result = map_sql_type_to_arrow(&custom_with_size("EMBEDDING", dim)).unwrap();
            match result {
                DataType::FixedSizeList(field, size) => {
                    assert_eq!(size, dim);
                    assert_eq!(field.data_type(), &DataType::Float32);
                    assert_eq!(field.name(), "item");
                    assert!(!field.is_nullable());
                },
                _ => panic!("Expected FixedSizeList, got {:?}", result),
            }
        }
    }

    #[test]
    fn maps_sql_type_to_kalam() {
        let dtype = map_sql_type_to_kalam(&SQLDataType::Text).unwrap();
        assert_eq!(dtype, KalamDataType::Text);
    }

    #[test]
    fn maps_file_custom_type_to_kalam() {
        let dtype = map_sql_type_to_kalam(&custom("file")).unwrap();
        assert_eq!(dtype, KalamDataType::File);
    }

    #[test]
    fn rejects_embedding_without_dimension() {
        let err = map_sql_type_to_arrow(&custom("EMBEDDING")).unwrap_err();
        assert!(err.contains("requires exactly one dimension parameter"));
    }

    #[test]
    fn rejects_embedding_dimension_zero() {
        let err = map_sql_type_to_arrow(&custom_with_size("EMBEDDING", 0)).unwrap_err();
        assert!(err.contains("between 1 and 8192"));
    }

    #[test]
    fn rejects_embedding_dimension_too_large() {
        let err = map_sql_type_to_arrow(&custom_with_size("EMBEDDING", 9000)).unwrap_err();
        assert!(err.contains("between 1 and 8192"));
    }
}

/// Database error message style configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorStyle {
    /// PostgreSQL-style errors (default)
    /// Examples:
    /// - "ERROR: relation \"users\" does not exist"
    /// - "ERROR: column \"age\" does not exist"
    /// - "ERROR: syntax error at or near \"FROM\""
    #[default]
    PostgreSQL,

    /// MySQL-style errors
    /// Examples:
    /// - "ERROR 1146 (42S02): Table 'db.users' doesn't exist"
    /// - "ERROR 1054 (42S22): Unknown column 'age' in 'field list'"
    MySQL,
}

/// Format an error message in PostgreSQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_postgres_error;
///
/// let msg = format_postgres_error("relation \"users\" does not exist");
/// assert_eq!(msg, "ERROR: relation \"users\" does not exist");
/// ```
pub fn format_postgres_error(message: &str) -> String {
    format!("ERROR: {}", message)
}

/// Format a table not found error in PostgreSQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_postgres_table_not_found;
///
/// let msg = format_postgres_table_not_found("users");
/// assert_eq!(msg, "ERROR: relation \"users\" does not exist");
/// ```
pub fn format_postgres_table_not_found(table_name: &str) -> String {
    format!("ERROR: relation \"{}\" does not exist", table_name)
}

/// Format a column not found error in PostgreSQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_postgres_column_not_found;
///
/// let msg = format_postgres_column_not_found("age");
/// assert_eq!(msg, "ERROR: column \"age\" does not exist");
/// ```
pub fn format_postgres_column_not_found(column_name: &str) -> String {
    format!("ERROR: column \"{}\" does not exist", column_name)
}

/// Format a syntax error in PostgreSQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_postgres_syntax_error;
///
/// let msg = format_postgres_syntax_error("FROM");
/// assert_eq!(msg, "ERROR: syntax error at or near \"FROM\"");
/// ```
pub fn format_postgres_syntax_error(token: &str) -> String {
    format!("ERROR: syntax error at or near \"{}\"", token)
}

/// Format an error message in MySQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_mysql_error;
///
/// let msg = format_mysql_error(1146, "42S02", "Table 'db.users' doesn't exist");
/// assert_eq!(msg, "ERROR 1146 (42S02): Table 'db.users' doesn't exist");
/// ```
pub fn format_mysql_error(error_code: u16, sqlstate: &str, message: &str) -> String {
    format!("ERROR {} ({}): {}", error_code, sqlstate, message)
}

/// Format a table not found error in MySQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_mysql_table_not_found;
///
/// let msg = format_mysql_table_not_found("db", "users");
/// assert_eq!(msg, "ERROR 1146 (42S02): Table 'db.users' doesn't exist");
/// ```
pub fn format_mysql_table_not_found(database: &str, table_name: &str) -> String {
    format!("ERROR 1146 (42S02): Table '{}.{}' doesn't exist", database, table_name)
}

/// Format a column not found error in MySQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_mysql_column_not_found;
///
/// let msg = format_mysql_column_not_found("age");
/// assert_eq!(msg, "ERROR 1054 (42S22): Unknown column 'age' in 'field list'");
/// ```
pub fn format_mysql_column_not_found(column_name: &str) -> String {
    format!("ERROR 1054 (42S22): Unknown column '{}' in 'field list'", column_name)
}

/// Format a syntax error in MySQL style
///
/// # Examples
///
/// ```
/// use kalamdb_dialect::compatibility::format_mysql_syntax_error;
///
/// let msg = format_mysql_syntax_error("FROM", 1);
/// assert_eq!(
///     msg,
///     "ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that \
///      corresponds to your MySQL server version for the right syntax to use near 'FROM' at line \
///      1"
/// );
/// ```
pub fn format_mysql_syntax_error(token: &str, line: usize) -> String {
    format!(
        "ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that \
         corresponds to your MySQL server version for the right syntax to use near '{}' at line {}",
        token, line
    )
}

#[cfg(test)]
mod error_formatting_tests {
    use super::*;

    #[test]
    fn test_postgres_table_not_found() {
        assert_eq!(
            format_postgres_table_not_found("users"),
            "ERROR: relation \"users\" does not exist"
        );
    }

    #[test]
    fn test_postgres_column_not_found() {
        assert_eq!(format_postgres_column_not_found("age"), "ERROR: column \"age\" does not exist");
    }

    #[test]
    fn test_postgres_syntax_error() {
        assert_eq!(format_postgres_syntax_error("FROM"), "ERROR: syntax error at or near \"FROM\"");
    }

    #[test]
    fn test_mysql_table_not_found() {
        assert_eq!(
            format_mysql_table_not_found("mydb", "users"),
            "ERROR 1146 (42S02): Table 'mydb.users' doesn't exist"
        );
    }

    #[test]
    fn test_mysql_column_not_found() {
        assert_eq!(
            format_mysql_column_not_found("age"),
            "ERROR 1054 (42S22): Unknown column 'age' in 'field list'"
        );
    }

    #[test]
    fn test_mysql_syntax_error() {
        assert_eq!(
            format_mysql_syntax_error("FROM", 1),
            "ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that \
             corresponds to your MySQL server version for the right syntax to use near 'FROM' at \
             line 1"
        );
    }
}
