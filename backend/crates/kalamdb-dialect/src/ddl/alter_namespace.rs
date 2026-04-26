//! ALTER NAMESPACE statement parser
//!
//! Parses SQL statements like:
//! - ALTER NAMESPACE app SET OPTIONS (key1 = 'value1', key2 = 'value2')

use std::collections::HashMap;

use kalamdb_commons::models::NamespaceId;
use serde_json::Value as JsonValue;

use crate::ddl::DdlResult;

/// ALTER NAMESPACE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterNamespaceStatement {
    /// Namespace name to alter
    pub name: NamespaceId,

    /// Options to set
    pub options: HashMap<String, JsonValue>,
}

impl AlterNamespaceStatement {
    /// Parse an ALTER NAMESPACE statement from SQL
    ///
    /// Supports syntax:
    /// - ALTER NAMESPACE name SET OPTIONS (key1 = 'value1', key2 = 42, key3 = true)
    pub fn parse(sql: &str) -> DdlResult<Self> {
        let sql_upper = sql.trim().to_uppercase();

        if !sql_upper.starts_with("ALTER NAMESPACE") {
            return Err("Expected ALTER NAMESPACE statement".to_string());
        }

        if !sql_upper.contains("SET OPTIONS") {
            return Err("Expected SET OPTIONS clause".to_string());
        }

        // Extract namespace name (between ALTER NAMESPACE and SET OPTIONS)
        let name_part = sql
            .trim()
            .strip_prefix("ALTER NAMESPACE")
            .or_else(|| sql.trim().strip_prefix("alter namespace"))
            .ok_or_else(|| "Invalid ALTER NAMESPACE syntax".to_string())?
            .trim();

        let set_options_pos = name_part
            .to_uppercase()
            .find("SET OPTIONS")
            .ok_or_else(|| "SET OPTIONS clause not found".to_string())?;

        let name = name_part[..set_options_pos].trim();

        if name.is_empty() {
            return Err("Namespace name is required".to_string());
        }

        // Extract options from parentheses
        let options_part = name_part[set_options_pos..]
            .strip_prefix("SET OPTIONS")
            .or_else(|| name_part[set_options_pos..].strip_prefix("set options"))
            .ok_or_else(|| "Invalid SET OPTIONS syntax".to_string())?
            .trim();

        let options = Self::parse_options(options_part)?;

        Ok(Self {
            name: NamespaceId::new(name),
            options,
        })
    }

    /// Parse options from the (key1 = value1, key2 = value2) format
    fn parse_options(options_str: &str) -> DdlResult<HashMap<String, JsonValue>> {
        let options_str = options_str.trim();

        if !options_str.starts_with('(') || !options_str.ends_with(')') {
            return Err("Options must be enclosed in parentheses".to_string());
        }

        let inner = &options_str[1..options_str.len() - 1].trim();

        if inner.is_empty() {
            return Ok(HashMap::new());
        }

        let mut options = HashMap::new();

        // Simple parsing: split by comma (doesn't handle commas in strings, but good enough for
        // now)
        for pair in inner.split(',') {
            let mut parts = pair.splitn(2, '=').map(|s| s.trim());
            let key = parts.next();
            let value_str = parts.next();

            if key.is_none() || value_str.is_none() {
                return Err(format!("Invalid option format: {}", pair));
            }

            let key = key.unwrap().to_string();
            let value_str = value_str.unwrap();

            // Parse value as JSON
            let value = if value_str.starts_with('\'') && value_str.ends_with('\'') {
                // String value
                JsonValue::String(value_str[1..value_str.len() - 1].to_string())
            } else if value_str == "true" || value_str == "false" {
                // Boolean value
                JsonValue::Bool(value_str == "true")
            } else if let Ok(num) = value_str.parse::<i64>() {
                // Integer value
                JsonValue::Number(num.into())
            } else if let Ok(num) = value_str.parse::<f64>() {
                // Float value
                JsonValue::Number(
                    serde_json::Number::from_f64(num)
                        .ok_or_else(|| "Invalid number".to_string())?,
                )
            } else {
                return Err(format!("Invalid value format: {}", value_str));
            };

            options.insert(key, value);
        }

        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_alter_namespace() {
        let stmt = AlterNamespaceStatement::parse(
            "ALTER NAMESPACE app SET OPTIONS (max_tables = 100, region = 'us-west')",
        )
        .unwrap();

        assert_eq!(stmt.name.as_str(), "app");
        assert_eq!(stmt.options.len(), 2);
        assert_eq!(stmt.options.get("max_tables"), Some(&JsonValue::Number(100.into())));
        assert_eq!(stmt.options.get("region"), Some(&JsonValue::String("us-west".to_string())));
    }

    #[test]
    fn test_parse_alter_namespace_empty_options() {
        let stmt = AlterNamespaceStatement::parse("ALTER NAMESPACE app SET OPTIONS ()").unwrap();

        assert_eq!(stmt.name.as_str(), "app");
        assert!(stmt.options.is_empty());
    }

    #[test]
    fn test_parse_alter_namespace_boolean() {
        let stmt =
            AlterNamespaceStatement::parse("ALTER NAMESPACE app SET OPTIONS (enabled = true)")
                .unwrap();

        assert_eq!(stmt.options.get("enabled"), Some(&JsonValue::Bool(true)));
    }

    #[test]
    fn test_parse_alter_namespace_missing_name() {
        let result = AlterNamespaceStatement::parse("ALTER NAMESPACE SET OPTIONS ()");
        assert!(result.is_err());
    }
}
