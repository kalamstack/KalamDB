use datafusion::scalar::ScalarValue;
use kalamdb_commons::conversions::{
    json_value_to_scalar_for_column as commons_json_value_to_scalar_for_column,
    scalar_to_json_for_column as commons_scalar_to_json_for_column,
};
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::rows::{Row, SystemTableRow};
use kalamdb_commons::schemas::TableDefinition;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

use crate::error::SystemError;

pub fn model_to_system_row<T: Serialize>(
    model: &T,
    table_def: &TableDefinition,
) -> Result<SystemTableRow, SystemError> {
    let value = serde_json::to_value(model)
        .map_err(|e| SystemError::SerializationError(format!("model serialize failed: {e}")))?;
    let object = value.as_object().ok_or_else(|| {
        SystemError::SerializationError("model serialize failed: expected JSON object".to_string())
    })?;

    let mut fields = BTreeMap::new();
    for column in &table_def.columns {
        let json_value = object.get(&column.column_name).unwrap_or(&Value::Null);
        let scalar = json_value_to_scalar_for_column(json_value, &column.data_type)?;
        fields.insert(column.column_name.clone(), scalar);
    }

    Ok(SystemTableRow {
        fields: Row::new(fields),
    })
}

pub fn system_row_to_model<T: DeserializeOwned>(
    row: &SystemTableRow,
    table_def: &TableDefinition,
) -> Result<T, SystemError> {
    let mut object = Map::new();

    for column in &table_def.columns {
        let scalar =
            row.fields.values.get(&column.column_name).cloned().unwrap_or(ScalarValue::Null);
        let json_value = scalar_to_json_for_column(&scalar, &column.data_type)?;
        object.insert(column.column_name.clone(), json_value);
    }

    serde_json::from_value(Value::Object(object))
        .map_err(|e| SystemError::SerializationError(format!("model deserialize failed: {e}")))
}

fn json_value_to_scalar_for_column(
    value: &Value,
    data_type: &KalamDataType,
) -> Result<ScalarValue, SystemError> {
    commons_json_value_to_scalar_for_column(value, data_type).map_err(|e| {
        SystemError::SerializationError(format!("json->scalar conversion failed: {e}"))
    })
}

fn scalar_to_json_for_column(
    scalar: &ScalarValue,
    data_type: &KalamDataType,
) -> Result<Value, SystemError> {
    commons_scalar_to_json_for_column(scalar, data_type).map_err(|e| {
        SystemError::SerializationError(format!("scalar->json conversion failed: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::rows::SystemTableRow;
    use kalamdb_commons::schemas::TableDefinition;
    use kalamdb_commons::{NamespaceId, TableName};
    use serde::{Deserialize, Serialize};

    use super::{model_to_system_row, system_row_to_model};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestModel {
        id: String,
        name: String,
        created_at: i64,
        routes: Vec<String>,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct UuidDecimalModel {
        id: String,
        user_uuid: String,
        amount: String,
    }

    #[test]
    fn test_system_row_model_roundtrip() {
        let model = TestModel {
            id: "a1".to_string(),
            name: "hello".to_string(),
            created_at: 12345,
            routes: vec!["x".to_string(), "y".to_string()],
        };

        let table_def = TableDefinition::new(
            NamespaceId::system(),
            TableName::new("test_models"),
            kalamdb_commons::schemas::TableType::System,
            vec![
                kalamdb_commons::schemas::ColumnDefinition::primary_key(
                    1,
                    "id",
                    1,
                    kalamdb_commons::datatypes::KalamDataType::Text,
                ),
                kalamdb_commons::schemas::ColumnDefinition::simple(
                    2,
                    "name",
                    2,
                    kalamdb_commons::datatypes::KalamDataType::Text,
                ),
                kalamdb_commons::schemas::ColumnDefinition::simple(
                    3,
                    "created_at",
                    3,
                    kalamdb_commons::datatypes::KalamDataType::Timestamp,
                ),
                kalamdb_commons::schemas::ColumnDefinition::simple(
                    4,
                    "routes",
                    4,
                    kalamdb_commons::datatypes::KalamDataType::Json,
                ),
            ],
            kalamdb_commons::schemas::TableOptions::system(),
            None,
        )
        .expect("table definition");

        let row: SystemTableRow = model_to_system_row(&model, &table_def).expect("to row");
        let decoded: TestModel = system_row_to_model(&row, &table_def).expect("from row");
        assert_eq!(decoded, model);
    }

    #[test]
    fn test_system_row_uuid_and_decimal_roundtrip() {
        let model = UuidDecimalModel {
            id: "r1".to_string(),
            user_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            amount: "200.75".to_string(),
        };

        let table_def = TableDefinition::new(
            NamespaceId::system(),
            TableName::new("uuid_decimal_models"),
            kalamdb_commons::schemas::TableType::System,
            vec![
                kalamdb_commons::schemas::ColumnDefinition::primary_key(
                    1,
                    "id",
                    1,
                    kalamdb_commons::datatypes::KalamDataType::Text,
                ),
                kalamdb_commons::schemas::ColumnDefinition::simple(
                    2,
                    "user_uuid",
                    2,
                    kalamdb_commons::datatypes::KalamDataType::Uuid,
                ),
                kalamdb_commons::schemas::ColumnDefinition::simple(
                    3,
                    "amount",
                    3,
                    kalamdb_commons::datatypes::KalamDataType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                ),
            ],
            kalamdb_commons::schemas::TableOptions::system(),
            None,
        )
        .expect("table definition");

        let row: SystemTableRow = model_to_system_row(&model, &table_def).expect("to row");
        let decoded: UuidDecimalModel = system_row_to_model(&row, &table_def).expect("from row");

        assert_eq!(decoded.id, model.id);
        assert_eq!(decoded.user_uuid, model.user_uuid);
        assert_eq!(decoded.amount, model.amount);
    }
}
