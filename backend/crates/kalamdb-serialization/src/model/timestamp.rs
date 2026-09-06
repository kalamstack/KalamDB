use datafusion_common::ScalarValue;
use kalamdb_commons::{
    models::{datatypes::KalamDataType, rows::Row},
    schemas::TableDefinition,
};

use crate::error::{Result, SerializationError};

pub fn model_ms_to_storage_micros(row: &mut Row, table_def: &TableDefinition) -> Result<()> {
    for column in &table_def.columns {
        if !matches!(column.data_type, KalamDataType::Timestamp | KalamDataType::DateTime) {
            continue;
        }
        let Some(value) = row.values.get_mut(&column.column_name) else {
            continue;
        };
        let current = std::mem::replace(value, ScalarValue::Null);
        *value = match current {
            ScalarValue::Int64(Some(ms)) => {
                ScalarValue::TimestampMicrosecond(Some(ms_to_micros(ms)?), None)
            },
            ScalarValue::TimestampMicrosecond(Some(ms), timezone) => {
                ScalarValue::TimestampMicrosecond(Some(ms_to_micros(ms)?), timezone)
            },
            ScalarValue::TimestampMillisecond(Some(ms), timezone) => {
                ScalarValue::TimestampMicrosecond(Some(ms_to_micros(ms)?), timezone)
            },
            other => other,
        };
    }
    Ok(())
}

pub fn storage_micros_to_model_ms(row: &mut Row, table_def: &TableDefinition) -> Result<()> {
    for column in &table_def.columns {
        if !matches!(column.data_type, KalamDataType::Timestamp | KalamDataType::DateTime) {
            continue;
        }
        let Some(value) = row.values.get_mut(&column.column_name) else {
            continue;
        };
        let current = std::mem::replace(value, ScalarValue::Null);
        *value = match current {
            ScalarValue::TimestampSecond(Some(seconds), _) => {
                ScalarValue::Int64(Some(seconds_to_millis(seconds)?))
            },
            ScalarValue::TimestampMillisecond(Some(ms), _) => ScalarValue::Int64(Some(ms)),
            ScalarValue::TimestampMicrosecond(Some(micros), _) => {
                ScalarValue::Int64(Some(micros / 1_000))
            },
            ScalarValue::TimestampNanosecond(Some(nanos), _) => {
                ScalarValue::Int64(Some(nanos / 1_000_000))
            },
            other => other,
        };
    }
    Ok(())
}

fn ms_to_micros(value: i64) -> Result<i64> {
    value.checked_mul(1_000).ok_or_else(|| {
        SerializationError::Encode(format!(
            "timestamp value {value} overflows when converted to microseconds"
        ))
    })
}

fn seconds_to_millis(value: i64) -> Result<i64> {
    value.checked_mul(1_000).ok_or_else(|| {
        SerializationError::Decode(format!(
            "timestamp value {value} overflows when converted to milliseconds"
        ))
    })
}
