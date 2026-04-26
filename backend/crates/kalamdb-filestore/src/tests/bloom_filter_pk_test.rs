//! Bloom Filter for PRIMARY KEY columns test (FR-054, FR-055)
//!
//! Tests that Bloom filters are generated for PRIMARY KEY columns + _seq system column.
//! This enables efficient point query filtering (WHERE id = X) by skipping batch files
//! where the Bloom filter indicates "definitely not present".

use std::{env, fs, sync::Arc};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use kalamdb_commons::{
    models::{ids::StorageId, TableId},
    schemas::TableType,
};
use kalamdb_system::{providers::storages::models::StorageType, Storage};

use crate::registry::StorageCached;

fn create_test_storage(temp_dir: &std::path::Path) -> Storage {
    let now = chrono::Utc::now().timestamp_millis();
    Storage {
        storage_id: StorageId::from("test"),
        storage_name: "test".to_string(),
        description: None,
        storage_type: StorageType::Filesystem,
        base_directory: temp_dir.to_string_lossy().to_string(),
        credentials: None,
        config_json: None,
        shared_tables_template: "{namespace}/{tableName}".to_string(),
        user_tables_template: "{namespace}/{tableName}/{userId}".to_string(),
        created_at: now,
        updated_at: now,
    }
}

/// Creates a batch with 1024 rows (minimum required for bloom filters).
fn create_test_batch_with_bloom_size(schema: Arc<Schema>, num_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i)).collect();
    let seqs: Vec<i64> = (0..num_rows as i64).map(|i| i * 1000000).collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(seqs)),
        ],
    )
    .unwrap()
}

/// Test that Bloom filters are generated for PRIMARY KEY column + _seq
#[test]
fn test_bloom_filter_for_primary_key_column() {
    let temp_dir = env::temp_dir().join("kalamdb_bloom_filter_pk_test");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = create_test_storage(&temp_dir);
    let storage_cached = StorageCached::with_default_timeouts(storage);
    let table_id = TableId::from_strings("test", "bloom_pk");

    // Schema with PRIMARY KEY column "id" + _seq system column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),   // PRIMARY KEY
        Field::new("name", DataType::Utf8, true),   // Regular column
        Field::new("_seq", DataType::Int64, false), // System column
    ]));

    // Create a batch with 1024 rows (minimum for bloom filters)
    let batch = create_test_batch_with_bloom_size(schema.clone(), 1024);

    // Write Parquet file with Bloom filters on PRIMARY KEY ("id") and _seq
    let file_path = "with_pk_bloom.parquet";
    let bloom_filter_columns = vec!["id".to_string(), "_seq".to_string()];
    let result = storage_cached.write_parquet_sync(
        TableType::Shared,
        &table_id,
        None,
        file_path,
        schema,
        vec![batch],
        Some(bloom_filter_columns),
    );
    assert!(result.is_ok());

    // Verify Bloom filters exist in Parquet metadata
    let full_path = storage_cached
        .get_file_path(TableType::Shared, &table_id, None, file_path)
        .full_path;
    let file = fs::File::open(&full_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();

    // Check that file has row groups
    assert!(metadata.num_row_groups() > 0);

    // Get first row group metadata
    let row_group = metadata.row_group(0);

    // Find column indexes for "id" and "_seq"
    let id_col_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "id")
        .expect("PRIMARY KEY column 'id' should exist");

    let seq_col_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "_seq")
        .expect("System column '_seq' should exist");

    // Verify "id" column has Bloom filter (FR-055: indexed columns)
    let id_col_metadata = row_group.column(id_col_idx);
    assert!(
        id_col_metadata.bloom_filter_offset().is_some(),
        "PRIMARY KEY column 'id' should have Bloom filter (FR-055)"
    );

    // Verify "_seq" column has Bloom filter (FR-054: default columns)
    let seq_col_metadata = row_group.column(seq_col_idx);
    assert!(
        seq_col_metadata.bloom_filter_offset().is_some(),
        "System column '_seq' should have Bloom filter (FR-054)"
    );

    // Verify "name" column does NOT have Bloom filter (not PRIMARY KEY)
    let name_col_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "name")
        .expect("Column 'name' should exist");

    let name_col_metadata = row_group.column(name_col_idx);
    assert!(
        name_col_metadata.bloom_filter_offset().is_none(),
        "Regular column 'name' should NOT have Bloom filter (not indexed)"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

/// Test that Bloom filters work with composite PRIMARY KEY (multiple columns)
#[test]
fn test_bloom_filter_for_composite_primary_key() {
    let temp_dir = env::temp_dir().join("kalamdb_bloom_filter_composite_pk_test");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = create_test_storage(&temp_dir);
    let storage_cached = StorageCached::with_default_timeouts(storage);
    let table_id = TableId::from_strings("test", "bloom_composite");

    // Schema with composite PRIMARY KEY (user_id, order_id) + _seq
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false), // PRIMARY KEY part 1
        Field::new("order_id", DataType::Int64, false), // PRIMARY KEY part 2
        Field::new("amount", DataType::Int64, true),   // Regular column
        Field::new("_seq", DataType::Int64, false),    // System column
    ]));

    // Create batch with 1024 rows (minimum for bloom filters)
    let num_rows = 1024;
    let user_ids: Vec<i64> = (0..num_rows as i64).map(|i| i % 100).collect();
    let order_ids: Vec<i64> = (0..num_rows as i64).collect();
    let amounts: Vec<i64> = (0..num_rows as i64).map(|i| (i + 1) * 50).collect();
    let seqs: Vec<i64> = (0..num_rows as i64).map(|i| i * 1000000).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Int64Array::from(amounts)),
            Arc::new(Int64Array::from(seqs)),
        ],
    )
    .unwrap();

    // Write Parquet file with Bloom filters on both PK columns + _seq
    let file_path = "with_composite_pk_bloom.parquet";
    let bloom_filter_columns = vec![
        "user_id".to_string(),
        "order_id".to_string(),
        "_seq".to_string(),
    ];
    let result = storage_cached.write_parquet_sync(
        TableType::Shared,
        &table_id,
        None,
        file_path,
        schema,
        vec![batch],
        Some(bloom_filter_columns),
    );
    assert!(result.is_ok());

    // Verify Bloom filters exist for both PRIMARY KEY columns
    let full_path = storage_cached
        .get_file_path(TableType::Shared, &table_id, None, file_path)
        .full_path;
    let file = fs::File::open(&full_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);

    // Verify user_id has Bloom filter
    let user_id_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "user_id")
        .expect("user_id column should exist");
    assert!(
        row_group.column(user_id_idx).bloom_filter_offset().is_some(),
        "user_id (PRIMARY KEY part 1) should have Bloom filter"
    );

    // Verify order_id has Bloom filter
    let order_id_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "order_id")
        .expect("order_id column should exist");
    assert!(
        row_group.column(order_id_idx).bloom_filter_offset().is_some(),
        "order_id (PRIMARY KEY part 2) should have Bloom filter"
    );

    // Verify _seq has Bloom filter
    let seq_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "_seq")
        .expect("_seq column should exist");
    assert!(
        row_group.column(seq_idx).bloom_filter_offset().is_some(),
        "_seq (system column) should have Bloom filter"
    );

    // Verify amount does NOT have Bloom filter (not PRIMARY KEY)
    let amount_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "amount")
        .expect("amount column should exist");
    assert!(
        row_group.column(amount_idx).bloom_filter_offset().is_none(),
        "amount (regular column) should NOT have Bloom filter"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

/// Test that default behavior (None) creates no Bloom filters
#[test]
fn test_bloom_filter_default_behavior() {
    let temp_dir = env::temp_dir().join("kalamdb_bloom_filter_default_test");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).unwrap();

    let storage = create_test_storage(&temp_dir);
    let storage_cached = StorageCached::with_default_timeouts(storage);
    let table_id = TableId::from_strings("test", "bloom_default");

    // Schema with PRIMARY KEY + _seq
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_seq", DataType::Int64, false),
    ]));

    // Create batch with 1024 rows
    let num_rows = 1024;
    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let seqs: Vec<i64> = (0..num_rows as i64).map(|i| i * 1000000).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(seqs)),
        ],
    )
    .unwrap();

    // Write with None (no bloom filters requested)
    let file_path = "default_bloom.parquet";
    let result = storage_cached.write_parquet_sync(
        TableType::Shared,
        &table_id,
        None,
        file_path,
        schema,
        vec![batch],
        None, // No bloom filters specified
    );
    assert!(result.is_ok());

    // Verify no column has Bloom filter when None is passed
    let full_path = storage_cached
        .get_file_path(TableType::Shared, &table_id, None, file_path)
        .full_path;
    let file = fs::File::open(&full_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);

    // Neither column should have Bloom filter when None is passed
    let seq_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "_seq")
        .unwrap();
    assert!(
        row_group.column(seq_idx).bloom_filter_offset().is_none(),
        "_seq should NOT have Bloom filter when None is passed"
    );

    let id_idx = row_group
        .columns()
        .iter()
        .position(|col| col.column_path().string() == "id")
        .unwrap();
    assert!(
        row_group.column(id_idx).bloom_filter_offset().is_none(),
        "id should NOT have Bloom filter when None is passed"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}
