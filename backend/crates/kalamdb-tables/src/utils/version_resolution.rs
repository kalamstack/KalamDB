//! Table-layer MVCC helpers that remain after moving shared winner-selection
//! and Parquet decoding logic into `kalamdb-datafusion-sources`.

use datafusion::{arrow::array::RecordBatch, error::DataFusionError, scalar::ScalarValue};
use kalamdb_commons::{ids::SeqId, serialization::row_codec::RowMetadata};
use kalamdb_datafusion_sources::exec::{
    parquet_batch_to_metadata as shared_parquet_batch_to_metadata,
    parquet_batch_to_rows as shared_parquet_batch_to_rows, VersionedRow,
};

use crate::{error::KalamDbError, SharedTableRow};

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use datafusion::arrow::{
        array::{BooleanArray, Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    };
    use kalamdb_commons::constants::SystemColumnNames;

    use super::*;

    #[derive(Debug, Clone)]
    struct TestVersionedRow {
        seq: SeqId,
        deleted: bool,
        pk: String,
        value: String,
    }

    impl VersionedRow for TestVersionedRow {
        fn seq_id(&self) -> SeqId {
            self.seq
        }

        fn commit_seq(&self) -> u64 {
            0
        }

        fn deleted(&self) -> bool {
            self.deleted
        }

        fn pk_value(&self, _pk_name: &str) -> Option<String> {
            Some(self.pk.clone())
        }
    }

    #[test]
    fn resolve_latest_kvs_from_cold_batch_only_builds_winning_cold_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new(SystemColumnNames::DELETED, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let cold_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![1, 3, 2])),
                Arc::new(BooleanArray::from(vec![false, false, false])),
                Arc::new(StringArray::from(vec!["cold-a", "cold-b", "cold-c"])),
            ],
        )
        .unwrap();

        let hot_rows = vec![(
            "hot-a".to_string(),
            TestVersionedRow {
                seq: SeqId::from_i64(2),
                deleted: false,
                pk: "a".to_string(),
                value: "hot-a".to_string(),
            },
        )];
        let build_calls = AtomicUsize::new(0);

        let resolved = resolve_latest_kvs_from_cold_batch(
            "id",
            hot_rows,
            &cold_batch,
            false,
            None,
            |row_data| {
                build_calls.fetch_add(1, Ordering::SeqCst);
                let pk = match row_data.fields.get("id").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                let name = match row_data.fields.get("name").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                Ok((
                    pk.clone(),
                    TestVersionedRow {
                        seq: row_data.seq_id,
                        deleted: false,
                        pk,
                        value: name,
                    },
                ))
            },
        )
        .unwrap();

        assert_eq!(build_calls.load(Ordering::SeqCst), 2);
        assert_eq!(resolved.len(), 3);
        assert!(resolved.iter().any(|(_, row)| row.pk == "a" && row.value == "hot-a"));
        assert!(resolved.iter().any(|(_, row)| row.pk == "b" && row.value == "cold-b"));
        assert!(resolved.iter().any(|(_, row)| row.pk == "c" && row.value == "cold-c"));
    }

    #[test]
    fn resolve_latest_kvs_from_cold_batch_honors_snapshot_commit_seq() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(SystemColumnNames::SEQ, DataType::Int64, false),
            Field::new(SystemColumnNames::COMMIT_SEQ, DataType::UInt64, false),
            Field::new(SystemColumnNames::DELETED, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let cold_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![5])),
                Arc::new(UInt64Array::from(vec![2_u64])),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(StringArray::from(vec!["cold-new"])),
            ],
        )
        .unwrap();

        let hot_rows = vec![(
            "hot-a".to_string(),
            TestVersionedRow {
                seq: SeqId::from_i64(4),
                deleted: false,
                pk: "a".to_string(),
                value: "hot-visible".to_string(),
            },
        )];

        let resolved = resolve_latest_kvs_from_cold_batch(
            "id",
            hot_rows,
            &cold_batch,
            false,
            Some(1),
            |row_data| {
                let pk = match row_data.fields.get("id").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                let name = match row_data.fields.get("name").unwrap() {
                    ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                        value.clone()
                    },
                    value => value.to_string(),
                };
                Ok((
                    pk.clone(),
                    TestVersionedRow {
                        seq: row_data.seq_id,
                        deleted: row_data.deleted,
                        pk,
                        value: name,
                    },
                ))
            },
        )
        .unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].1.value, "hot-visible");
    }
}

pub use kalamdb_datafusion_sources::exec::{
    count_merged_rows, count_resolved_from_metadata, merge_versioned_rows,
    resolve_latest_kvs_from_cold_batch, ParquetRowData,
};

fn shared_decoder_error(error: DataFusionError) -> KalamDbError {
    KalamDbError::Other(error.to_string())
}

/// Convert Parquet RecordBatch rows into SeqId + JSON field maps
pub fn parquet_batch_to_rows(batch: &RecordBatch) -> Result<Vec<ParquetRowData>, KalamDbError> {
    shared_parquet_batch_to_rows(batch).map_err(shared_decoder_error)
}

impl VersionedRow for SharedTableRow {
    fn seq_id(&self) -> SeqId {
        self._seq
    }

    fn commit_seq(&self) -> u64 {
        self._commit_seq
    }

    fn deleted(&self) -> bool {
        self._deleted
    }

    fn pk_value(&self, pk_name: &str) -> Option<String> {
        self.fields.get(pk_name).and_then(|val| {
            if val.is_null() {
                None
            } else {
                match val {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
                    _ => Some(val.to_string()),
                }
            }
        })
    }
}

/// Extract lightweight metadata (seq, deleted, pk_value) from a Parquet RecordBatch
/// without materializing full Row objects. Used for count-only scan paths.
pub fn parquet_batch_to_metadata(
    batch: &RecordBatch,
    pk_name: &str,
) -> Result<Vec<(SeqId, RowMetadata)>, KalamDbError> {
    shared_parquet_batch_to_metadata(batch, pk_name)
        .map(|rows| rows.into_iter().map(|metadata| (metadata.seq, metadata)).collect())
        .map_err(shared_decoder_error)
}
