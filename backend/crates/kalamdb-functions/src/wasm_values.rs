//! Arrow IPC boundary for WASM; one row holds the invocation's typed values.

use std::{io::Cursor, sync::Arc};

use arrow::{
    array::RecordBatch,
    datatypes::{Field, Schema},
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use datafusion_common::ScalarValue;

use crate::{FunctionsError, Result, RoutineValue};

pub fn encode_values(values: &[RoutineValue]) -> Result<Vec<u8>> {
    let fields: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("kalam.json".into(), value.json_sql.to_string());
            Field::new(i.to_string(), value.value.data_type(), true).with_metadata(metadata)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays = values
        .iter()
        .map(|v| v.value.to_array())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(invalid)?;
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        arrays,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(invalid)?;
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema).map_err(invalid)?;
    writer.write(&batch).map_err(invalid)?;
    writer.finish().map_err(invalid)?;
    drop(writer);
    Ok(bytes)
}

pub fn decode_values(bytes: &[u8], max_bytes: usize) -> Result<Vec<RoutineValue>> {
    if bytes.len() > max_bytes {
        return Err(FunctionsError::ResourceLimit("WASM value bytes".into()));
    }
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(invalid)?;
    let batch = reader
        .next()
        .ok_or_else(|| FunctionsError::Invalid("WASM values require one Arrow batch".into()))?
        .map_err(invalid)?;
    if batch.num_rows() != 1 || reader.next().is_some() || batch.num_columns() > 1024 {
        return Err(FunctionsError::Invalid(
            "WASM values require one row and at most 1024 columns".into(),
        ));
    }
    batch
        .columns()
        .iter()
        .enumerate()
        .map(|(index, array)| {
            let value = ScalarValue::try_from_array(array, 0).map_err(invalid)?;
            Ok(
                if batch
                    .schema()
                    .field(index)
                    .metadata()
                    .get("kalam.json")
                    .is_some_and(|v| v == "true")
                {
                    RoutineValue::json(value)
                } else {
                    RoutineValue::new(value)
                },
            )
        })
        .collect()
}

fn invalid(error: impl std::fmt::Display) -> FunctionsError {
    FunctionsError::Invalid(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ipc_preserves_values_and_empty_arguments() {
        let values = vec![
            RoutineValue::new(ScalarValue::Int64(Some(i64::MIN))),
            RoutineValue::json(ScalarValue::Utf8(Some("{\"a\":1}".into()))),
        ];
        assert_eq!(decode_values(&encode_values(&values).unwrap(), 8192).unwrap(), values);
        assert!(decode_values(&encode_values(&[]).unwrap(), 8192).unwrap().is_empty());
    }
}
