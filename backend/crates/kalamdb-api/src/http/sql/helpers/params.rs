//! Parameter parsing helpers

use kalamdb_core::providers::arrow_json_conversion::json_value_to_scalar_strict;
use kalamdb_core::sql::executor::ScalarValue;
use kalamdb_raft::ForwardSqlParam;
use serde_json::Value as JsonValue;

/// Parse scalar parameters from JSON values
pub fn parse_scalar_params(
    params_json: &Option<Vec<JsonValue>>,
) -> Result<Vec<ScalarValue>, String> {
    match params_json {
        Some(json_params) => {
            let mut scalar_params = Vec::new();
            for (idx, json_val) in json_params.iter().enumerate() {
                let scalar = json_value_to_scalar_strict(json_val)
                    .map_err(|err| format!("Parameter ${} invalid: {}", idx + 1, err))?;
                scalar_params.push(scalar);
            }
            Ok(scalar_params)
        },
        None => Ok(Vec::new()),
    }
}

/// Convert external JSON request params into typed forwarded SQL params.
pub fn parse_forward_params(
    params_json: &Option<Vec<JsonValue>>,
) -> Result<Vec<ForwardSqlParam>, String> {
    match params_json {
        Some(json_params) => {
            let mut params = Vec::with_capacity(json_params.len());
            for (idx, json_val) in json_params.iter().enumerate() {
                let param = match json_val {
                    JsonValue::Null => ForwardSqlParam::null(),
                    JsonValue::Bool(value) => ForwardSqlParam::boolean(*value),
                    JsonValue::Number(value) => {
                        if let Some(integer) = value.as_i64() {
                            ForwardSqlParam::int64(integer)
                        } else if let Some(float) = value.as_f64() {
                            ForwardSqlParam::float64(float)
                        } else {
                            return Err(format!(
                                "Parameter ${} invalid: Unsupported number format: {}",
                                idx + 1,
                                value
                            ));
                        }
                    },
                    JsonValue::String(value) => ForwardSqlParam::text(value.clone()),
                    JsonValue::Array(_) => {
                        return Err(format!(
                            "Parameter ${} invalid: Array parameters not yet supported",
                            idx + 1
                        ));
                    },
                    JsonValue::Object(_) => {
                        return Err(format!(
                            "Parameter ${} invalid: Object parameters not yet supported",
                            idx + 1
                        ));
                    },
                };
                params.push(param);
            }
            Ok(params)
        },
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_raft::forward_sql_param;
    use serde_json::json;

    #[test]
    fn parse_forward_params_preserves_scalar_types() {
        let params = Some(vec![
            json!(null),
            json!(true),
            json!(42),
            json!(3.5),
            json!("abc"),
        ]);

        let parsed = parse_forward_params(&params).expect("convert forwarded params");

        assert!(matches!(parsed[0].value, Some(forward_sql_param::Value::NullValue(_))));
        assert!(matches!(parsed[1].value, Some(forward_sql_param::Value::BoolValue(true))));
        assert!(matches!(parsed[2].value, Some(forward_sql_param::Value::Int64Value(42))));
        assert!(matches!(
            parsed[3].value,
            Some(forward_sql_param::Value::Float64Value(value)) if value == 3.5
        ));
        assert!(matches!(
            parsed[4].value,
            Some(forward_sql_param::Value::StringValue(ref value)) if value == "abc"
        ));
    }

    #[test]
    fn parse_forward_params_rejects_nested_values() {
        let params = Some(vec![json!([1, 2, 3])]);

        let err = parse_forward_params(&params).expect_err("arrays should be rejected");
        assert!(err.contains("Array parameters not yet supported"));
    }
}
