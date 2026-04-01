//! Parameter parsing helpers

use kalamdb_core::providers::arrow_json_conversion::json_value_to_scalar_strict;
use kalamdb_core::sql::executor::ScalarValue;
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
