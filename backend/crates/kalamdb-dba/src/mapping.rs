use kalamdb_commons::{models::rows::Row, schemas::TableDefinition};
use serde::{de::DeserializeOwned, Serialize};

use crate::error::{DbaError, Result};

pub fn model_to_row<T: Serialize>(model: &T, table_def: &TableDefinition) -> Result<Row> {
    let mut row = kalamdb_serialization::model_to_row(model, table_def)
        .map_err(|error| DbaError::Serialization(error.to_string()))?;
    kalamdb_serialization::model_ms_to_storage_micros(&mut row, table_def)
        .map_err(|error| DbaError::Serialization(error.to_string()))?;
    Ok(row)
}

pub fn row_to_model<T: DeserializeOwned>(row: &Row, table_def: &TableDefinition) -> Result<T> {
    let mut row = row.clone();
    kalamdb_serialization::storage_micros_to_model_ms(&mut row, table_def)
        .map_err(|error| DbaError::Serialization(error.to_string()))?;
    kalamdb_serialization::row_to_model(&row, table_def)
        .map_err(|error| DbaError::Serialization(error.to_string()))
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::{model_to_row, row_to_model};
    use crate::models::NotificationRow;

    #[test]
    fn dba_timestamp_models_store_microseconds_and_decode_milliseconds() {
        let row = NotificationRow {
            id:         "notif-1".to_string(),
            user_id:    kalamdb_commons::models::UserId::new("user-1"),
            title:      "Maintenance complete".to_string(),
            body:       None,
            is_read:    false,
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_000_500,
        };

        let encoded = model_to_row(&row, &NotificationRow::definition()).expect("encode row");
        assert!(matches!(
            encoded.values.get("created_at"),
            Some(ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), None))
        ));

        let decoded: NotificationRow =
            row_to_model(&encoded, &NotificationRow::definition()).expect("decode row");
        assert_eq!(decoded.created_at, row.created_at);
        assert_eq!(decoded.updated_at, row.updated_at);
    }
}
