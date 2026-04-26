use std::collections::BTreeMap;

use datafusion_common::ScalarValue;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

use crate::{
    conversions::{json_value_to_scalar_for_column, scalar_to_json_for_column},
    models::rows::Row,
    schemas::TableDefinition,
};

/// Serde-based fallback for metadata models that still bridge through JSON values.
///
/// This is intentionally kept at a crate boundary helper so callers can wrap the
/// string error in their local error type without duplicating the mapping logic.
pub fn serde_model_to_row<T: Serialize>(
    model: &T,
    table_def: &TableDefinition,
) -> Result<Row, String> {
    let value =
        serde_json::to_value(model).map_err(|error| format!("model serialize failed: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "model serialize failed: expected JSON object".to_string())?;

    let mut fields = BTreeMap::new();
    for column in &table_def.columns {
        let json_value = object.get(&column.column_name).unwrap_or(&Value::Null);
        let scalar = json_value_to_scalar_for_column(json_value, &column.data_type)
            .map_err(|error| format!("json->scalar conversion failed: {error}"))?;
        fields.insert(column.column_name.clone(), scalar);
    }

    Ok(Row::new(fields))
}

/// Inverse of [`serde_model_to_row`].
pub fn row_to_serde_model<T: DeserializeOwned>(
    row: &Row,
    table_def: &TableDefinition,
) -> Result<T, String> {
    let mut object = Map::new();

    for column in &table_def.columns {
        let scalar = row.values.get(&column.column_name).cloned().unwrap_or(ScalarValue::Null);
        let json_value = scalar_to_json_for_column(&scalar, &column.data_type)
            .map_err(|error| format!("scalar->json conversion failed: {error}"))?;
        object.insert(column.column_name.clone(), json_value);
    }

    serde_json::from_value(Value::Object(object))
        .map_err(|error| format!("model deserialize failed: {error}"))
}
