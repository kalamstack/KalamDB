//! SQL dialect compatibility helpers.
//!
//! This module provides utilities for mapping PostgreSQL/MySQL specific
//! data types into Arrow data types that KalamDB understands.  Centralising
//! these conversions keeps the CREATE TABLE parsers in sync across crates.

use std::string::String;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use kalamdb_commons::models::datatypes::{FromArrowType, KalamDataType};
use sqlparser::ast::{DataType as SQLDataType, DataType::*, ObjectName};

/// Map a parsed `sqlparser` data type into an Arrow data type while accounting
/// for PostgreSQL/MySQL aliases (e.g. `SERIAL`, `INT4`, `AUTO_INCREMENT`).
pub fn map_sql_type_to_arrow(sql_type: &SQLDataType) -> Result<DataType, String> {
    let dtype = match sql_type {
        // Signed integers ----------------------------------------------------
        SmallInt(_) | Int2(_) => DataType::Int16,
        Int(_) | Integer(_) | Int4(_) => DataType::Int32,
        MediumInt(_) => DataType::Int32,
        BigInt(_) | Int8(_) | Int64 => DataType::Int64,
        TinyInt(_) => DataType::Int8,

        // Unsigned integers --------------------------------------------------
        UnsignedInteger => DataType::UInt32,

        // Floating point -----------------------------------------------------
        Float(_) | Real | Float4 => DataType::Float32,
        SQLDataType::Double(_) | DoublePrecision | Float8 | Float64 => DataType::Float64,

        // Boolean ------------------------------------------------------------
        Boolean | Bool => DataType::Boolean,

        // Character / string -------------------------------------------------
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
        | String(_)
        | JSON
        | JSONB => DataType::Utf8,

        // Binary -------------------------------------------------------------
        Binary(_) | Varbinary(_) | Blob(_) | Bytes(_) | Bytea => DataType::Binary,

        // Temporal -----------------------------------------------------------
        Date => DataType::Date32,
        Timestamp(precision, _) => {
            let unit = match precision {
                Some(p) if *p <= 3 => TimeUnit::Millisecond,
                Some(p) if *p <= 6 => TimeUnit::Microsecond,
                Some(_) => TimeUnit::Nanosecond,
                None => TimeUnit::Microsecond,
            };
            DataType::Timestamp(unit, None)
        },
        Datetime(_) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Time(_, _) => DataType::Time64(TimeUnit::Microsecond),
        SQLDataType::Interval { .. } => DataType::Interval(IntervalUnit::MonthDayNano),

        // UUID ---------------------------------------------------------------
        SQLDataType::Uuid => DataType::FixedSizeBinary(16),

        // Decimal ------------------------------------------------------------
        Decimal(info) => match info {
            sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                DataType::Decimal128(*p as u8, *s as i8)
            },
            sqlparser::ast::ExactNumberInfo::Precision(p) => DataType::Decimal128(*p as u8, 0),
            sqlparser::ast::ExactNumberInfo::None => DataType::Decimal128(38, 10),
        },

        // Custom or dialect specific identifiers ----------------------------
        Custom(name, modifiers) => map_custom_type(name, modifiers)?,

        // Struct / collection types -----------------------------------------
        Array(_) | Enum(_, _) | Set(_) | Struct(_, _) => DataType::Utf8,

        // Otherwise, leave unsupported so callers can surface a friendly error
        _ => {
            return Err(format!("Unsupported data type: {:?}", sql_type));
        },
    };

    Ok(dtype)
}

/// Map a parsed `sqlparser` data type into a KalamDataType via Arrow.
pub fn map_sql_type_to_kalam(sql_type: &SQLDataType) -> Result<KalamDataType, String> {
    match sql_type {
        SQLDataType::JSON | SQLDataType::JSONB => Ok(KalamDataType::Json),
        SQLDataType::Uuid => Ok(KalamDataType::Uuid),
        // Handle FILE type directly to avoid going through Arrow Utf8 -> Text
        SQLDataType::Custom(name, _) => {
            let ident = name
                .0
                .iter()
                .map(|id| id.to_string().to_lowercase())
                .collect::<Vec<_>>()
                .join(".");
            if ident == "file" {
                return Ok(KalamDataType::File);
            }
            // Fall through to standard Arrow conversion for other custom types
            let arrow_type = map_sql_type_to_arrow(sql_type)?;
            KalamDataType::from_arrow_type(&arrow_type).map_err(|e| e.to_string())
        },
        SQLDataType::Decimal(info) => {
            let (precision, scale) = match info {
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
                sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sqlparser::ast::ExactNumberInfo::None => (38, 10),
            };
            KalamDataType::validate_decimal_params(precision, scale).map_err(|e| e.to_string())?;
            Ok(KalamDataType::Decimal { precision, scale })
        },
        SQLDataType::Date => Ok(KalamDataType::Date),
        SQLDataType::Time(_, _) => Ok(KalamDataType::Time),
        SQLDataType::Timestamp(_, _) => Ok(KalamDataType::Timestamp),
        SQLDataType::Datetime(_) => Ok(KalamDataType::DateTime),
        _ => {
            let arrow_type = map_sql_type_to_arrow(sql_type)?;
            KalamDataType::from_arrow_type(&arrow_type).map_err(|e| e.to_string())
        },
    }
}

fn map_custom_type(name: &ObjectName, modifiers: &[String]) -> Result<DataType, String> {
    let ident = name
        .0
        .iter()
        .map(|id| id.to_string().to_lowercase())
        .collect::<Vec<_>>()
        .join(".");

    let dtype = match ident.as_str() {
        // KalamDB-specific: FILE -> Utf8 (stores FileRef as JSON string)
        "file" => DataType::Utf8,
        // KalamDB-specific: EMBEDDING(dimension) -> FixedSizeList<Float32>
        "embedding" => {
            // Extract dimension from modifiers
            if modifiers.len() != 1 {
                return Err("EMBEDDING type requires exactly one dimension parameter, e.g., \
                            EMBEDDING(384)"
                    .to_string());
            }

            let dim_str = &modifiers[0];
            let dim = dim_str.parse::<usize>().map_err(|_| {
                format!("EMBEDDING dimension must be a positive integer, got '{}'", dim_str)
            })?;

            // Validate dimension is within allowed range (1-8192)
            if dim < 1 {
                return Err("EMBEDDING dimension must be at least 1".to_string());
            }
            if dim > 8192 {
                return Err(format!("EMBEDDING dimension must be at most 8192 (found {})", dim));
            }

            // Return FixedSizeList<Float32> to match KalamDataType::Embedding Arrow conversion
            DataType::FixedSizeList(
                std::sync::Arc::new(arrow::datatypes::Field::new("item", DataType::Float32, false)),
                dim as i32,
            )
        },

        // PostgreSQL serial aliases
        "serial" | "serial4" => DataType::Int32,
        "bigserial" | "serial8" => DataType::Int64,
        "smallserial" | "serial2" => DataType::Int16,
        // Postgres integer aliases
        "int1" => DataType::Int8,
        "int2" => DataType::Int16,
        "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        // MySQL aliases
        "signed" => DataType::Int32,
        "unsigned" => DataType::UInt32,
        // Fallback to treating unknown custom types as UTF8 strings
        other if other.ends_with("text") || other.ends_with("string") => DataType::Utf8,
        other => {
            return Err(format!("Unsupported custom data type '{}'", other));
        },
    };

    Ok(dtype)
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
    fn rejects_embedding_without_dimension() {
        let err = map_sql_type_to_arrow(&custom("EMBEDDING")).unwrap_err();
        assert!(err.contains("requires exactly one dimension parameter"));
    }

    #[test]
    fn rejects_embedding_dimension_zero() {
        let err = map_sql_type_to_arrow(&custom_with_size("EMBEDDING", 0)).unwrap_err();
        assert!(err.contains("must be at least 1"));
    }

    #[test]
    fn rejects_embedding_dimension_too_large() {
        let err = map_sql_type_to_arrow(&custom_with_size("EMBEDDING", 9000)).unwrap_err();
        assert!(err.contains("must be at most 8192"));
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
