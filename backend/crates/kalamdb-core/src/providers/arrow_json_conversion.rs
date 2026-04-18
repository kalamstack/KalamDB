use crate::error::KalamDbError;
use datafusion::arrow::record_batch::RecordBatch;
use kalamdb_commons::conversions::arrow_json_conversion as commons;
use kalamdb_commons::errors::CommonError;
use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::KalamCellValue;
use std::collections::HashMap;

fn map_error(err: CommonError) -> KalamDbError {
    KalamDbError::InvalidOperation(err.to_string())
}

pub fn record_batch_to_json_arrays(
    batch: &RecordBatch,
) -> Result<Vec<Vec<KalamCellValue>>, KalamDbError> {
    commons::record_batch_to_json_arrays(batch).map_err(map_error)
}

pub fn row_to_json_map(row: &Row) -> Result<HashMap<String, KalamCellValue>, KalamDbError> {
    commons::row_to_json_map(row).map_err(map_error)
}

pub fn row_into_json_map(row: Row) -> Result<HashMap<String, KalamCellValue>, KalamDbError> {
    commons::row_into_json_map(row).map_err(map_error)
}

pub use commons::{
    arrow_value_to_scalar, coerce_rows, coerce_updates, json_rows_to_arrow_batch, json_to_row,
    json_value_to_scalar, json_value_to_scalar_strict,
};
