use kalamdb_commons::{
    conversions::{row_to_serde_model, serde_model_to_row},
    models::rows::Row,
    schemas::TableDefinition,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::error::{DbaError, Result};

pub fn model_to_row<T: Serialize>(model: &T, table_def: &TableDefinition) -> Result<Row> {
    serde_model_to_row(model, table_def).map_err(DbaError::Serialization)
}

pub fn row_to_model<T: DeserializeOwned>(row: &Row, table_def: &TableDefinition) -> Result<T> {
    row_to_serde_model(row, table_def).map_err(DbaError::Serialization)
}
