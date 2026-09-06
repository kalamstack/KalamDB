//! Positional argument to `CALL` and column-default procedure invocations.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::datatypes::KalamDataType;

/// Literal or placeholder argument shared by `CALL` and `DEFAULT fn(...)`.
///
/// Literals carry a [`KalamDataType`] so every catalog type can appear as an
/// argument (`UUID`, `JSON`, `BYTES`, `TIMESTAMP`, …), not only text/bool/number.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CallArgument {
    /// Untyped SQL `NULL`.
    Null,
    /// Catalog-typed literal, including typed `NULL` (`CAST(NULL AS UUID)`).
    Typed {
        data_type: KalamDataType,
        value:     JsonValue,
    },
    Placeholder(usize),
}

impl CallArgument {
    pub fn typed(data_type: KalamDataType, value: JsonValue) -> Self {
        Self::Typed { data_type, value }
    }

    pub fn boolean(value: bool) -> Self {
        Self::typed(KalamDataType::Boolean, JsonValue::Bool(value))
    }

    pub fn text(value: impl Into<String>) -> Self {
        Self::typed(KalamDataType::Text, JsonValue::String(value.into()))
    }

    pub fn smallint(value: i16) -> Self {
        Self::typed(KalamDataType::SmallInt, JsonValue::Number(value.into()))
    }

    pub fn int(value: i32) -> Self {
        Self::typed(KalamDataType::Int, JsonValue::Number(value.into()))
    }

    pub fn bigint(value: i64) -> Self {
        Self::typed(KalamDataType::BigInt, JsonValue::Number(value.into()))
    }

    pub fn float(value: f32) -> Result<Self, String> {
        json_number(f64::from(value))
            .map(|number| Self::typed(KalamDataType::Float, JsonValue::Number(number)))
    }

    pub fn double(value: f64) -> Result<Self, String> {
        json_number(value)
            .map(|number| Self::typed(KalamDataType::Double, JsonValue::Number(number)))
    }

    pub fn uuid(value: impl Into<String>) -> Self {
        Self::typed(KalamDataType::Uuid, JsonValue::String(value.into()))
    }

    pub fn json(value: JsonValue) -> Self {
        Self::typed(KalamDataType::Json, value)
    }

    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder(_))
    }

    pub fn to_sql(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Placeholder(index) => format!("${index}"),
            Self::Typed { data_type, value } => typed_to_sql(data_type, value),
        }
    }
}

fn json_number(value: f64) -> Result<serde_json::Number, String> {
    serde_json::Number::from_f64(value).ok_or_else(|| "float argument must be finite".to_string())
}

fn quote_sql_string(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn typed_to_sql(data_type: &KalamDataType, value: &JsonValue) -> String {
    if value.is_null() {
        return format!("CAST(NULL AS {})", data_type.sql_name());
    }
    match data_type {
        KalamDataType::Boolean => {
            if value.as_bool().unwrap_or(false) {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        },
        KalamDataType::SmallInt | KalamDataType::Int | KalamDataType::BigInt => value.to_string(),
        KalamDataType::Float | KalamDataType::Double => value.to_string(),
        KalamDataType::Text => quote_sql_string(value.as_str().unwrap_or(&value.to_string())),
        KalamDataType::Uuid
        | KalamDataType::Date
        | KalamDataType::Time
        | KalamDataType::Timestamp
        | KalamDataType::DateTime => {
            let text = value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string());
            format!("{} {}", data_type.sql_name(), quote_sql_string(&text))
        },
        KalamDataType::Bytes => match value {
            JsonValue::Array(items) => {
                let hex = items
                    .iter()
                    .filter_map(|item| item.as_u64().and_then(|byte| u8::try_from(byte).ok()))
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                format!("X'{hex}'")
            },
            JsonValue::String(text) => format!("X'{text}'"),
            other => format!("CAST({} AS BYTES)", quote_sql_string(&other.to_string())),
        },
        KalamDataType::Json
        | KalamDataType::File
        | KalamDataType::Decimal { .. }
        | KalamDataType::Embedding(_) => {
            let literal = match value {
                JsonValue::String(text) => quote_sql_string(text),
                other => quote_sql_string(&other.to_string()),
            };
            format!("CAST({literal} AS {})", data_type.sql_name())
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_literals_render_sql() {
        assert_eq!(CallArgument::text("hi").to_sql(), "'hi'");
        assert_eq!(CallArgument::bigint(7).to_sql(), "7");
        assert_eq!(CallArgument::boolean(true).to_sql(), "TRUE");
        assert_eq!(CallArgument::Null.to_sql(), "NULL");
        assert_eq!(CallArgument::Placeholder(1).to_sql(), "$1");
        assert_eq!(
            CallArgument::uuid("550e8400-e29b-41d4-a716-446655440000").to_sql(),
            "UUID '550e8400-e29b-41d4-a716-446655440000'"
        );
        assert_eq!(
            CallArgument::json(serde_json::json!({"ok": true})).to_sql(),
            "CAST('{\"ok\":true}' AS JSON)"
        );
    }
}
