//! Column default value specification.

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;

use crate::models::{CallArgument, RoutineCall, RoutineId};

/// Represents the default value for a column.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ColumnDefault {
    /// No default value — column must be specified in INSERT.
    #[default]
    None,

    /// Literal value as JSON (supports all KalamDataTypes).
    Literal(JsonValue),

    /// Built-in ScalarUDF or user procedure. Same `RoutineCall` shape as `CALL`.
    FunctionCall(RoutineCall),
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
    FunctionCall {
        routine_id: RoutineId,
        arguments:  Vec<CallArgument>,
    },
}

impl From<&ColumnDefault> for ColumnDefaultRepr {
    fn from(value: &ColumnDefault) -> Self {
        match value {
            ColumnDefault::None => ColumnDefaultRepr::None,
            ColumnDefault::Literal(json) => ColumnDefaultRepr::Literal(json.clone()),
            ColumnDefault::FunctionCall(call) => ColumnDefaultRepr::FunctionCall {
                routine_id: call.routine_id.clone(),
                arguments:  call.arguments.clone(),
            },
        }
    }
}

impl From<ColumnDefaultRepr> for ColumnDefault {
    fn from(value: ColumnDefaultRepr) -> Self {
        match value {
            ColumnDefaultRepr::None => ColumnDefault::None,
            ColumnDefaultRepr::Literal(json) => ColumnDefault::Literal(json),
            ColumnDefaultRepr::FunctionCall {
                routine_id,
                arguments,
            } => ColumnDefault::FunctionCall(RoutineCall::new(routine_id, arguments)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StoredColumnDefault {
    None,
    Literal(String),
    FunctionCall {
        routine_id: RoutineId,
        arguments:  Vec<CallArgument>,
    },
}

impl From<&ColumnDefault> for StoredColumnDefault {
    fn from(value: &ColumnDefault) -> Self {
        match value {
            ColumnDefault::None => StoredColumnDefault::None,
            ColumnDefault::Literal(json) => StoredColumnDefault::Literal(json.to_string()),
            ColumnDefault::FunctionCall(call) => StoredColumnDefault::FunctionCall {
                routine_id: call.routine_id.clone(),
                arguments:  call.arguments.clone(),
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
            StoredColumnDefault::FunctionCall {
                routine_id,
                arguments,
            } => Ok(ColumnDefault::FunctionCall(RoutineCall::new(routine_id, arguments))),
        }
    }
}

impl ColumnDefault {
    pub fn none() -> Self {
        ColumnDefault::None
    }

    pub fn literal(value: JsonValue) -> Self {
        ColumnDefault::Literal(value)
    }

    /// Built-in or unqualified routine name with CALL-shaped arguments.
    pub fn function(name: impl Into<String>, args: Vec<CallArgument>) -> Self {
        let name = name.into();
        let routine_id = if crate::models::is_builtin_default_name(&name) {
            RoutineId::new(crate::models::normalize_builtin_name(&name))
        } else {
            RoutineId::new(name.to_ascii_lowercase())
        };
        ColumnDefault::FunctionCall(RoutineCall::new(routine_id, args))
    }

    pub fn procedure(routine_id: RoutineId, arguments: Vec<CallArgument>) -> Self {
        ColumnDefault::FunctionCall(RoutineCall::new(routine_id, arguments))
    }

    pub fn is_none(&self) -> bool {
        matches!(self, ColumnDefault::None)
    }

    pub fn as_routine_call(&self) -> Option<&RoutineCall> {
        match self {
            ColumnDefault::FunctionCall(call) => Some(call),
            _ => None,
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            ColumnDefault::None => "".to_string(),
            ColumnDefault::Literal(value) => match value {
                JsonValue::Null => "NULL".to_string(),
                JsonValue::Bool(b) => b.to_string().to_uppercase(),
                JsonValue::Number(n) => n.to_string(),
                JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
                JsonValue::Array(_) | JsonValue::Object(_) => {
                    format!("'{}'", value.to_string().replace('\'', "''"))
                },
            },
            ColumnDefault::FunctionCall(call) => call.to_sql(),
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
        let default = ColumnDefault::function("NOW", vec![]);
        assert_eq!(default.to_sql(), "NOW()");
        assert!(default.as_routine_call().unwrap().is_builtin_default());

        let default = ColumnDefault::function("chat.next_id", vec![CallArgument::text("v4")]);
        assert_eq!(default.to_sql(), "chat.next_id('v4')");
        assert!(!default.as_routine_call().unwrap().is_builtin_default());

        let default = ColumnDefault::function(
            "concat",
            vec![CallArgument::text("prefix_"), CallArgument::text("value")],
        );
        assert_eq!(default.to_sql(), "concat('prefix_', 'value')");
    }

    #[test]
    fn test_serialization() {
        let defaults = vec![
            ColumnDefault::none(),
            ColumnDefault::literal(json!(42)),
            ColumnDefault::function("NOW", vec![]),
            ColumnDefault::procedure(
                RoutineId::from_parts(Some(&crate::models::NamespaceId::new("app")), "gen_id"),
                vec![CallArgument::bigint(1)],
            ),
        ];

        for original in defaults {
            let json = serde_json::to_string(&original).unwrap();
            let decoded: ColumnDefault = serde_json::from_str(&json).unwrap();
            assert_eq!(original, decoded);
        }
    }
}
