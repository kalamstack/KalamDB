use crate::error::{DbaError, Result};
use datafusion::scalar::ScalarValue;
use kalamdb_commons::conversions::{json_value_to_scalar_for_column, scalar_to_json_for_column};
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::schemas::TableDefinition;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub fn model_to_row<T: Serialize>(model: &T, table_def: &TableDefinition) -> Result<Row> {
    let value = serde_json::to_value(model)
        .map_err(|error| DbaError::Serialization(format!("model serialize failed: {error}")))?;
    let object = value.as_object().ok_or_else(|| {
        DbaError::Serialization("model serialize failed: expected JSON object".to_string())
    })?;

    let mut fields = BTreeMap::new();
    for column in &table_def.columns {
        let json_value = object.get(&column.column_name).unwrap_or(&Value::Null);
        let scalar =
            json_value_to_scalar_for_column(json_value, &column.data_type).map_err(|error| {
                DbaError::Serialization(format!("json->scalar conversion failed: {error}"))
            })?;
        fields.insert(column.column_name.clone(), scalar);
    }

    Ok(Row::new(fields))
}

pub fn row_to_model<T: DeserializeOwned>(row: &Row, table_def: &TableDefinition) -> Result<T> {
    let mut object = Map::new();

    for column in &table_def.columns {
        let scalar = row.values.get(&column.column_name).cloned().unwrap_or(ScalarValue::Null);
        let json_value =
            scalar_to_json_for_column(&scalar, &column.data_type).map_err(|error| {
                DbaError::Serialization(format!("scalar->json conversion failed: {error}"))
            })?;
        object.insert(column.column_name.clone(), json_value);
    }

    serde_json::from_value(Value::Object(object))
        .map_err(|error| DbaError::Serialization(format!("model deserialize failed: {error}")))
}
