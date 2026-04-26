//! Column default value specification

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;

/// Represents the default value for a column
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ColumnDefault {
    /// No default value - column must be specified in INSERT
    #[default]
    None,

    /// Literal value as JSON (supports all KalamDataTypes)
    /// Examples: null, true, 42, "hello", [1.0, 2.0, 3.0]
    Literal(JsonValue),

    /// Function call with arguments
    /// Examples: NOW(), UUID(), CURRENT_USER()
    FunctionCall {
        /// Function name (case-insensitive)
        name: String,
        /// Function arguments (empty for no-arg functions)
        args: Vec<JsonValue>,
    },
}

impl Serialize for ColumnDefault {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let repr = ColumnDefaultRepr::from(self);
            repr.serialize(serializer)
        } else {
            let stored = StoredColumnDefault::from(self);
            stored.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ColumnDefault {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let repr = ColumnDefaultRepr::deserialize(deserializer)?;
            Ok(repr.into())
        } else {
            let stored = StoredColumnDefault::deserialize(deserializer)?;
            ColumnDefault::try_from(stored).map_err(DeError::custom)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ColumnDefaultRepr {
    None,
    Literal(JsonValue),
    FunctionCall { name: String, args: Vec<JsonValue> },
}

impl From<&ColumnDefault> for ColumnDefaultRepr {
    fn from(value: &ColumnDefault) -> Self {
        match value {
            ColumnDefault::None => ColumnDefaultRepr::None,
            ColumnDefault::Literal(json) => ColumnDefaultRepr::Literal(json.clone()),
            ColumnDefault::FunctionCall { name, args } => ColumnDefaultRepr::FunctionCall {
                name: name.clone(),
                args: args.clone(),
            },
        }
    }
}

impl From<ColumnDefaultRepr> for ColumnDefault {
    fn from(value: ColumnDefaultRepr) -> Self {
        match value {
            ColumnDefaultRepr::None => ColumnDefault::None,
            ColumnDefaultRepr::Literal(json) => ColumnDefault::Literal(json),
            ColumnDefaultRepr::FunctionCall { name, args } => {
                ColumnDefault::FunctionCall { name, args }
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StoredColumnDefault {
    None,
    Literal(String),
    FunctionCall { name: String, args: Vec<String> },
}

impl From<&ColumnDefault> for StoredColumnDefault {
    fn from(value: &ColumnDefault) -> Self {
        match value {
            ColumnDefault::None => StoredColumnDefault::None,
            ColumnDefault::Literal(json) => StoredColumnDefault::Literal(json.to_string()),
            ColumnDefault::FunctionCall { name, args } => {
                let serialized_args = args.iter().map(|arg| arg.to_string()).collect();
                StoredColumnDefault::FunctionCall {
                    name: name.clone(),
                    args: serialized_args,
                }
            },
        }
    }
}

impl TryFrom<StoredColumnDefault> for ColumnDefault {
    type Error = serde_json::Error;

    fn try_from(value: StoredColumnDefault) -> Result<Self, Self::Error> {
        match value {
            StoredColumnDefault::None => Ok(ColumnDefault::None),
            StoredColumnDefault::Literal(json) => {
                let parsed = serde_json::from_str(&json)?;
                Ok(ColumnDefault::Literal(parsed))
            },
            StoredColumnDefault::FunctionCall { name, args } => {
                let mut parsed_args = Vec::with_capacity(args.len());
                for arg in args {
                    parsed_args.push(serde_json::from_str(&arg)?);
                }
                Ok(ColumnDefault::FunctionCall {
                    name,
                    args: parsed_args,
                })
            },
        }
    }
}

impl ColumnDefault {
    /// Create a None default
    pub fn none() -> Self {
        ColumnDefault::None
    }

    /// Create a literal default from a JSON value
    pub fn literal(value: JsonValue) -> Self {
        ColumnDefault::Literal(value)
    }

    /// Create a function call default
    pub fn function(name: impl Into<String>, args: Vec<JsonValue>) -> Self {
        ColumnDefault::FunctionCall {
            name: name.into(),
            args,
        }
    }

    /// Check if this is a None default
    pub fn is_none(&self) -> bool {
        matches!(self, ColumnDefault::None)
    }

    /// Get SQL representation for display
    pub fn to_sql(&self) -> String {
        match self {
            ColumnDefault::None => "".to_string(),
            ColumnDefault::Literal(value) => {
                // Format JSON value as SQL literal
                match value {
                    JsonValue::Null => "NULL".to_string(),
                    JsonValue::Bool(b) => b.to_string().to_uppercase(),
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
                    JsonValue::Array(_) | JsonValue::Object(_) => {
                        format!("'{}'", value.to_string().replace('\'', "''"))
                    },
                }
            },
            ColumnDefault::FunctionCall { name, args } => {
                if args.is_empty() {
                    format!("{}()", name.to_uppercase())
                } else {
                    let args_str = args
                        .iter()
                        .map(|arg| match arg {
                            JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
                            other => other.to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("{}({})", name.to_uppercase(), args_str)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_none_default() {
        let default = ColumnDefault::none();
        assert!(default.is_none());
        assert_eq!(default.to_sql(), "");
    }

    #[test]
    fn test_literal_defaults() {
        let cases = vec![
            (json!(null), "NULL"),
            (json!(true), "TRUE"),
            (json!(false), "FALSE"),
            (json!(42), "42"),
            (json!(3.15), "3.15"),
            (json!("hello"), "'hello'"),
            (json!([1.0, 2.0, 3.0]), "'[1.0,2.0,3.0]'"),
        ];

        for (value, expected_sql) in cases {
            let default = ColumnDefault::literal(value);
            assert_eq!(default.to_sql(), expected_sql);
        }
    }

    #[test]
    fn test_function_call_defaults() {
        // No-arg function
        let default = ColumnDefault::function("NOW", vec![]);
        assert_eq!(default.to_sql(), "NOW()");

        // Function with args
        let default = ColumnDefault::function("UUID_GENERATE", vec![json!("v4")]);
        assert_eq!(default.to_sql(), "UUID_GENERATE('v4')");

        // Function with multiple args
        let default = ColumnDefault::function("CONCAT", vec![json!("prefix_"), json!("value")]);
        assert_eq!(default.to_sql(), "CONCAT('prefix_', 'value')");
    }

    #[test]
    fn test_serialization() {
        let defaults = vec![
            ColumnDefault::none(),
            ColumnDefault::literal(json!(42)),
            ColumnDefault::function("NOW", vec![]),
        ];

        for original in defaults {
            let json = serde_json::to_string(&original).unwrap();
            let decoded: ColumnDefault = serde_json::from_str(&json).unwrap();
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_flexbuffers_roundtrip() {
        let defaults = vec![
            ColumnDefault::none(),
            ColumnDefault::literal(json!({"key": "value", "nested": [1, 2, 3]})),
            ColumnDefault::function("CONCAT", vec![json!("prefix"), json!({"expr": "value"})]),
        ];

        for original in defaults {
            let bytes = flexbuffers::to_vec(&original).expect("encode to flexbuffers");
            let decoded: ColumnDefault =
                flexbuffers::from_slice(&bytes).expect("decode from flexbuffers");
            assert_eq!(original, decoded);
        }
    }
}
