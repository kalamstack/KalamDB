//! Tests for flush operations
//!
//! These tests cover utility functions and error scenarios in flush operations.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use kalamdb_core::{
    error::KalamDbError,
    manifest::{flush::helpers, FlushJobResult, FlushMetadata, TableFlush},
};

#[test]
fn test_extract_pk_field_name_with_non_system_column() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_seq", DataType::Int64, false),
        Field::new("_version", DataType::Int64, false),
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let pk_field = helpers::extract_pk_field_name(&schema);
    assert_eq!(pk_field, "id");
}

#[test]
fn test_extract_pk_field_name_only_system_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_seq", DataType::Int64, false),
        Field::new("_version", DataType::Int64, false),
        Field::new("_deleted", DataType::Boolean, true),
    ]));

    let pk_field = helpers::extract_pk_field_name(&schema);
    assert_eq!(pk_field, "id"); // Fallback to "id"
}

#[test]
fn test_extract_pk_field_name_empty_schema() {
    let schema = Arc::new(Schema::new(Vec::<Field>::new()));

    let pk_field = helpers::extract_pk_field_name(&schema);
    assert_eq!(pk_field, "id"); // Fallback to "id"
}

#[test]
fn test_advance_cursor() {
    let key = b"user123";
    let next = helpers::advance_cursor(key);

    assert_eq!(next.len(), key.len() + 1);
    assert_eq!(&next[..key.len()], key);
    assert_eq!(next[key.len()], 0);
}

#[test]
fn test_advance_cursor_empty() {
    let key = b"";
    let next = helpers::advance_cursor(key);

    assert_eq!(next.len(), 1);
    assert_eq!(next[0], 0);
}

#[test]
fn test_calculate_dedup_ratio_with_duplicates() {
    let before = 100;
    let after = 80;
    let ratio = helpers::calculate_dedup_ratio(before, after);

    assert_eq!(ratio, 20.0); // 20% deduplication
}

#[test]
fn test_calculate_dedup_ratio_no_duplicates() {
    let before = 100;
    let after = 100;
    let ratio = helpers::calculate_dedup_ratio(before, after);

    assert_eq!(ratio, 0.0); // No deduplication
}

#[test]
fn test_calculate_dedup_ratio_all_duplicates() {
    let before = 100;
    let after = 0;
    let ratio = helpers::calculate_dedup_ratio(before, after);

    assert_eq!(ratio, 100.0); // 100% deduplication
}

#[test]
fn test_calculate_dedup_ratio_zero_before() {
    let before = 0;
    let after = 0;
    let ratio = helpers::calculate_dedup_ratio(before, after);

    assert_eq!(ratio, 0.0);
}

#[test]
fn test_flush_job_result_shared_table() {
    let result = FlushJobResult {
        rows_flushed: 100,
        parquet_files: vec!["batch-0.parquet".to_string()],
        metadata: FlushMetadata::shared_table(),
    };

    assert_eq!(result.rows_flushed, 100);
    assert_eq!(result.parquet_files.len(), 1);
}

#[test]
fn test_flush_job_result_user_table() {
    let result = FlushJobResult {
        rows_flushed: 250,
        parquet_files: vec![
            "user_123/batch-0.parquet".to_string(),
            "user_456/batch-0.parquet".to_string(),
        ],
        metadata: FlushMetadata::user_table(2, vec![]),
    };

    assert_eq!(result.rows_flushed, 250);
    assert_eq!(result.parquet_files.len(), 2);
}

#[test]
fn test_flush_job_result_empty() {
    let result = FlushJobResult {
        rows_flushed: 0,
        parquet_files: vec![],
        metadata: FlushMetadata::shared_table(),
    };

    assert_eq!(result.rows_flushed, 0);
    assert!(result.parquet_files.is_empty());
}

// Mock flush job for testing trait implementation
struct SuccessfulFlushJob {
    rows: usize,
}

impl TableFlush for SuccessfulFlushJob {
    fn execute(&self) -> Result<FlushJobResult, KalamDbError> {
        Ok(FlushJobResult {
            rows_flushed: self.rows,
            parquet_files: vec!["batch-0.parquet".to_string()],
            metadata: FlushMetadata::shared_table(),
        })
    }

    fn table_identifier(&self) -> String {
        "test_ns.test_table".to_string()
    }
}

struct FailingFlushJob {
    error_msg: String,
}

impl TableFlush for FailingFlushJob {
    fn execute(&self) -> Result<FlushJobResult, KalamDbError> {
        Err(KalamDbError::Other(self.error_msg.clone()))
    }

    fn table_identifier(&self) -> String {
        "test_ns.failing_table".to_string()
    }
}

#[test]
fn test_successful_flush_job() {
    let job = SuccessfulFlushJob { rows: 100 };
    let result = job.execute();

    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.rows_flushed, 100);
    assert_eq!(result.parquet_files.len(), 1);
}

#[test]
fn test_failing_flush_job() {
    let job = FailingFlushJob {
        error_msg: "Disk full".to_string(),
    };
    let result = job.execute();

    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        KalamDbError::Other(msg) => assert_eq!(msg, "Disk full"),
        _ => panic!("Expected Other error variant"),
    }
}

#[test]
fn test_table_identifier() {
    let job = SuccessfulFlushJob { rows: 0 };
    assert_eq!(job.table_identifier(), "test_ns.test_table");

    let failing_job = FailingFlushJob {
        error_msg: "test".to_string(),
    };
    assert_eq!(failing_job.table_identifier(), "test_ns.failing_table");
}

#[test]
fn test_flush_metadata_shared() {
    let metadata = FlushMetadata::shared_table();

    // Verify it serializes correctly and contains expected content
    let json = serde_json::to_string(&metadata).expect("Failed to serialize");
    assert!(!json.is_empty(), "JSON should not be empty");
    assert!(json.contains("shared_table"), "JSON should contain 'shared_table': {}", json);
}

#[test]
fn test_flush_metadata_user_no_errors() {
    let metadata = FlushMetadata::user_table(5, vec![]);

    let json = serde_json::to_string(&metadata).expect("Failed to serialize");
    assert!(!json.is_empty());
}

#[test]
fn test_flush_metadata_user_with_errors() {
    let errors = vec![
        "Failed to flush user_123".to_string(),
        "Failed to flush user_456".to_string(),
    ];
    let metadata = FlushMetadata::user_table(3, errors);

    let json = serde_json::to_string(&metadata).expect("Failed to serialize");
    assert!(!json.is_empty(), "JSON should not be empty");
    assert!(json.contains("user_123"), "JSON should contain error 'user_123': {}", json);
}

#[test]
fn test_dedup_ratio_precision() {
    // Test with various ratios
    assert_eq!(helpers::calculate_dedup_ratio(1000, 900), 10.0);
    assert_eq!(helpers::calculate_dedup_ratio(1000, 500), 50.0);
    assert_eq!(helpers::calculate_dedup_ratio(1000, 1), 99.9);
    assert_eq!(helpers::calculate_dedup_ratio(1000, 0), 100.0);
}

#[test]
fn test_cursor_advancement_with_special_bytes() {
    // Test cursor with special byte values
    let key = b"\x00\xff\x80";
    let next = helpers::advance_cursor(key);

    assert_eq!(next.len(), 4);
    assert_eq!(&next[..3], key);
    assert_eq!(next[3], 0);
}

#[test]
fn test_pk_field_extraction_from_complex_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_seq", DataType::Int64, false),
        Field::new("_version", DataType::Int64, false),
        Field::new("_user_id", DataType::Utf8, true),
        Field::new("_deleted", DataType::Boolean, true),
        Field::new("id", DataType::Int32, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("created_at", DataType::Int64, false),
    ]));

    let pk_field = helpers::extract_pk_field_name(&schema);
    assert_eq!(pk_field, "id"); // First non-system column
}
