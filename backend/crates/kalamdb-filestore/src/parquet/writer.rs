//! Parquet writer utilities for StorageCached.
//!
//! Provides serialization helpers for Parquet writes managed by StorageCached.

use crate::error::{FilestoreError, Result};
use arrow::array::{Array, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::arrow::compute::{self, SortOptions};
use kalamdb_commons::constants::SystemColumnNames;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Result of a Parquet write operation.
#[derive(Debug, Clone)]
pub struct ParquetWriteResult {
    /// Size of the written data in bytes.
    pub size_bytes: u64,
}

/// Serialize Arrow RecordBatches to Parquet format in memory.
pub(crate) fn serialize_to_parquet(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    bloom_filter_columns: Option<Vec<String>>,
) -> Result<Bytes> {
    let row_count: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    let bloom_filter_count = bloom_filter_columns.as_ref().map_or(0, Vec::len);
    let span = tracing::debug_span!(
        "parquet.serialize",
        row_count = row_count,
        bloom_filter_count = bloom_filter_count
    );
    let _span_guard = span.entered();

    const MIN_ROWS_FOR_BLOOM_FILTERS: u64 = 1024;

    let batches = sort_batches_by_seq(batches)?;
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let bloom_ndv_estimate = total_rows.max(1);
    let bloom_filter_columns = if total_rows < MIN_ROWS_FOR_BLOOM_FILTERS {
        None
    } else {
        bloom_filter_columns
    };

    // Build writer properties
    let mut props_builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level()))
        .set_max_row_group_size(128 * 1024); // 128K rows per group

    // Add bloom filters for specified columns
    if let Some(cols) = bloom_filter_columns {
        for col in cols {
            let col_path: parquet::schema::types::ColumnPath = col.into();
            props_builder = props_builder.set_column_bloom_filter_enabled(col_path.clone(), true);
            props_builder = props_builder.set_column_bloom_filter_fpp(col_path.clone(), 0.01);
            props_builder = props_builder.set_column_bloom_filter_ndv(col_path, bloom_ndv_estimate);
        }
    }

    let props = props_builder.build();

    // Write to in-memory buffer
    let mut buffer = Vec::with_capacity(1024 * 1024); // 1MB initial capacity
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
            .map_err(|e| FilestoreError::Parquet(e.to_string()))?;

        for batch in batches {
            writer.write(&batch).map_err(|e| FilestoreError::Parquet(e.to_string()))?;
        }

        writer.close().map_err(|e| FilestoreError::Parquet(e.to_string()))?;
    }

    tracing::debug!(size_bytes = buffer.len(), "Parquet serialization completed");
    Ok(Bytes::from(buffer))
}

fn zstd_level() -> ZstdLevel {
    // Prefer a reasonable default level; keep this small to avoid heavy CPU on flush.
    // If the Parquet crate changes accepted ranges, fall back to default.
    ZstdLevel::try_new(1).unwrap_or_default()
}

fn sort_batches_by_seq(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    batches.into_iter().map(sort_record_batch_by_seq).collect()
}

fn sort_record_batch_by_seq(batch: RecordBatch) -> Result<RecordBatch> {
    let Some(seq_idx) =
        batch.schema().fields().iter().position(|f| f.name() == SystemColumnNames::SEQ)
    else {
        return Ok(batch);
    };

    if is_sorted_by_seq(&batch, seq_idx)? {
        return Ok(batch);
    }

    let seq_col = batch.column(seq_idx);
    let indices = compute::sort_to_indices(
        seq_col.as_ref(),
        Some(SortOptions {
            descending: false,
            nulls_first: false,
        }),
        None,
    )
    .map_err(|e| FilestoreError::Other(format!("Failed to sort by _seq: {e}")))?;

    let new_columns: Result<Vec<_>> = batch
        .columns()
        .iter()
        .map(|col| {
            compute::take(col.as_ref(), &indices, None)
                .map_err(|e| FilestoreError::Other(format!("Failed to apply sort indices: {e}")))
        })
        .collect();

    RecordBatch::try_new(batch.schema(), new_columns?)
        .map_err(|e| FilestoreError::Other(format!("Failed to build sorted RecordBatch: {e}")))
}

fn is_sorted_by_seq(batch: &RecordBatch, seq_idx: usize) -> Result<bool> {
    if batch.num_rows() <= 1 {
        return Ok(true);
    }

    let seq_col = batch.column(seq_idx);
    let Some(seq_array) = seq_col.as_any().downcast_ref::<Int64Array>() else {
        // If _seq isn't Int64 in this batch, fall back to sorting.
        return Ok(false);
    };

    if seq_array.null_count() > 0 {
        // System column should be non-null; treat nulls as unsorted.
        return Ok(false);
    }

    let mut prev = seq_array.value(0);
    for i in 1..seq_array.len() {
        let cur = seq_array.value(i);
        if cur < prev {
            return Ok(false);
        }
        prev = cur;
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use kalamdb_commons::arrow_utils::{field_utf8, schema};
    use std::sync::Arc;

    fn make_test_batch() -> (SchemaRef, Vec<RecordBatch>) {
        let schema = schema(vec![field_utf8("name", false)]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["alice", "bob", "charlie"]))],
        )
        .unwrap();
        (schema, vec![batch])
    }

    #[test]
    fn test_serialize_to_parquet() {
        let (schema, batches) = make_test_batch();
        let bytes = serialize_to_parquet(schema, batches, None).unwrap();
        assert!(!bytes.is_empty());
        // Parquet magic number at start
        assert_eq!(&bytes[0..4], b"PAR1");
    }
}
