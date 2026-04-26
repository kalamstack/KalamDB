//! Integration tests for manifest operations
//!
//! These tests cover error scenarios, edge cases, and failure modes
//! that could occur during manifest read/write operations.

use std::collections::HashMap;

use kalamdb_commons::{
    ids::SeqId, models::rows::StoredScalarValue, NamespaceId, TableId, TableName, UserId,
};
use kalamdb_system::{Manifest, SegmentMetadata};

#[test]
fn test_manifest_serialization_deserialization() {
    let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
    let user_id = Some(UserId::from("u_123"));

    let mut manifest = Manifest::new(table_id.clone(), user_id.clone());

    // Add a segment
    let segment = SegmentMetadata::new(
        "seg-1".to_string(),
        "batch-0.parquet".to_string(),
        HashMap::new(),
        SeqId::from(100i64),
        SeqId::from(200i64),
        50,
        1024,
    );
    manifest.add_segment(segment);

    // Serialize to JSON
    let json = serde_json::to_string(&manifest).expect("Failed to serialize manifest");

    // Deserialize back
    let deserialized: Manifest =
        serde_json::from_str(&json).expect("Failed to deserialize manifest");

    assert_eq!(deserialized.table_id, table_id);
    assert_eq!(deserialized.user_id, user_id);
    assert_eq!(deserialized.segments.len(), 1);
    assert_eq!(deserialized.segments[0].id, "seg-1");
    assert_eq!(deserialized.segments[0].min_seq, SeqId::from(100i64));
    assert_eq!(deserialized.segments[0].max_seq, SeqId::from(200i64));
}

#[test]
fn test_manifest_empty_segments() {
    let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
    let manifest = Manifest::new(table_id.clone(), None);

    assert_eq!(manifest.segments.len(), 0);
    assert_eq!(manifest.last_sequence_number, 0);

    // Serialize empty manifest
    let json = serde_json::to_string(&manifest).expect("Failed to serialize empty manifest");

    // Deserialize
    let deserialized: Manifest =
        serde_json::from_str(&json).expect("Failed to deserialize empty manifest");

    assert_eq!(deserialized.segments.len(), 0);
    assert_eq!(deserialized.table_id, table_id);
}

#[test]
fn test_manifest_multiple_segments() {
    let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
    let mut manifest = Manifest::new(table_id, None);

    // Add multiple segments
    for i in 0..10 {
        let segment = SegmentMetadata::new(
            format!("seg-{}", i),
            format!("batch-{}.parquet", i),
            HashMap::new(),
            SeqId::from((i * 100) as i64),
            SeqId::from(((i + 1) * 100 - 1) as i64),
            100,
            (1024 * (i + 1)) as u64,
        );
        manifest.add_segment(segment);
    }

    assert_eq!(manifest.segments.len(), 10);

    // Serialize
    let json = serde_json::to_string(&manifest).expect("Failed to serialize");

    // Deserialize
    let deserialized: Manifest = serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(deserialized.segments.len(), 10);
    for i in 0..10 {
        assert_eq!(deserialized.segments[i].id, format!("seg-{}", i));
        assert_eq!(deserialized.segments[i].min_seq, SeqId::from((i * 100) as i64));
        assert_eq!(deserialized.segments[i].max_seq, SeqId::from(((i + 1) * 100 - 1) as i64));
    }
}

#[test]
fn test_manifest_with_column_stats() {
    let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
    let mut manifest = Manifest::new(table_id, None);

    // Create column stats using StoredScalarValue
    let mut column_stats = HashMap::new();
    column_stats.insert(
        1u64,
        kalamdb_system::ColumnStats {
            min: Some(StoredScalarValue::Int64(Some("1".to_string()))),
            max: Some(StoredScalarValue::Int64(Some("100".to_string()))),
            null_count: Some(0),
        },
    );
    column_stats.insert(
        2u64,
        kalamdb_system::ColumnStats {
            min: Some(StoredScalarValue::Utf8(Some("alice".to_string()))),
            max: Some(StoredScalarValue::Utf8(Some("zoe".to_string()))),
            null_count: Some(5),
        },
    );

    let segment = SegmentMetadata::new(
        "seg-1".to_string(),
        "batch-0.parquet".to_string(),
        column_stats.clone(),
        SeqId::from(0i64),
        SeqId::from(99i64),
        100,
        2048,
    );
    manifest.add_segment(segment);

    // Serialize (JSON for manifest.json file output)
    let json = serde_json::to_string(&manifest).expect("Failed to serialize");

    // Deserialize
    let deserialized: Manifest = serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(deserialized.segments.len(), 1);
    let segment = &deserialized.segments[0];
    assert_eq!(segment.column_stats.len(), 2);

    let id_stats = segment.column_stats.get(&1u64).unwrap();
    assert_eq!(id_stats.min_as_i64(), Some(1));
    assert_eq!(id_stats.max_as_i64(), Some(100));
    assert_eq!(id_stats.null_count, Some(0));

    let name_stats = segment.column_stats.get(&2u64).unwrap();
    assert_eq!(name_stats.min_as_str(), Some("alice".to_string()));
    assert_eq!(name_stats.max_as_str(), Some("zoe".to_string()));
    assert_eq!(name_stats.null_count, Some(5));
}

#[test]
fn test_manifest_corrupted_json() {
    let corrupted_json = r#"{"table_id": "invalid", "segments": [}"#;

    let result: Result<Manifest, _> = serde_json::from_str(corrupted_json);
    assert!(result.is_err(), "Should fail to deserialize corrupted JSON");
}

#[test]
fn test_manifest_missing_required_fields() {
    // Missing table_id field
    let incomplete_json = r#"{"segments": [], "last_sequence_number": 0}"#;

    let result: Result<Manifest, _> = serde_json::from_str(incomplete_json);
    assert!(result.is_err(), "Should fail when required fields are missing");
}

#[test]
fn test_manifest_version_tracking() {
    let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
    let mut manifest = Manifest::new(table_id, None);

    let initial_version = manifest.version;

    // Add segment
    let segment = SegmentMetadata::new(
        "seg-1".to_string(),
        "batch-0.parquet".to_string(),
        HashMap::new(),
        SeqId::from(0i64),
        SeqId::from(99i64),
        100,
        1024,
    );
    manifest.add_segment(segment);

    // Version should increment
    assert_eq!(manifest.version, initial_version + 1);

    // Add another segment
    let segment2 = SegmentMetadata::new(
        "seg-2".to_string(),
        "batch-1.parquet".to_string(),
        HashMap::new(),
        SeqId::from(100i64),
        SeqId::from(199i64),
        100,
        1024,
    );
    manifest.add_segment(segment2);

    // Version should increment again
    assert_eq!(manifest.version, initial_version + 2);
}

#[test]
fn test_manifest_sequence_number_updates() {
    let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
    let mut manifest = Manifest::new(table_id, None);

    assert_eq!(manifest.last_sequence_number, 0);

    // Add segment with specific sequence range
    let segment = SegmentMetadata::new(
        "seg-1".to_string(),
        "batch-0.parquet".to_string(),
        HashMap::new(),
        SeqId::from(0i64),
        SeqId::from(99i64),
        100,
        1024,
    );
    manifest.add_segment(segment);

    // After adding first batch (0), next batch should be 1
    // last_sequence_number tracks the last batch index
    assert_eq!(manifest.last_sequence_number, 0);

    // Add second segment
    let segment2 = SegmentMetadata::new(
        "seg-2".to_string(),
        "batch-1.parquet".to_string(),
        HashMap::new(),
        SeqId::from(100i64),
        SeqId::from(199i64),
        100,
        1024,
    );
    manifest.add_segment(segment2);

    // Now last_sequence_number should be 1
    assert_eq!(manifest.last_sequence_number, 1);
}

#[test]
fn test_segment_metadata_schema_version() {
    let segment = SegmentMetadata::with_schema_version(
        "seg-1".to_string(),
        "batch-0.parquet".to_string(),
        HashMap::new(),
        SeqId::from(0i64),
        SeqId::from(99i64),
        100,
        1024,
        5, // schema version
    );

    assert_eq!(segment.schema_version, 5);

    // Serialize
    let json = serde_json::to_string(&segment).expect("Failed to serialize");

    // Deserialize
    let deserialized: SegmentMetadata = serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(deserialized.schema_version, 5);
}

#[test]
fn test_segment_metadata_without_schema_version() {
    let segment = SegmentMetadata::new(
        "seg-1".to_string(),
        "batch-0.parquet".to_string(),
        HashMap::new(),
        SeqId::from(0i64),
        SeqId::from(99i64),
        100,
        1024,
    );

    assert_eq!(segment.schema_version, 1); // Default version
}

#[test]
fn test_manifest_large_sequence_numbers() {
    let table_id = TableId::new(NamespaceId::new("test_ns"), TableName::new("test_table"));
    let mut manifest = Manifest::new(table_id, None);

    // Add segment with large sequence numbers
    let segment = SegmentMetadata::new(
        "seg-1".to_string(),
        "batch-0.parquet".to_string(),
        HashMap::new(),
        SeqId::from(1_000_000_000i64),
        SeqId::from(2_000_000_000i64),
        1_000_000_000,
        1024 * 1024 * 100, // 100 MB
    );
    manifest.add_segment(segment);

    // Serialize
    let json = serde_json::to_string(&manifest).expect("Failed to serialize");

    // Deserialize
    let deserialized: Manifest = serde_json::from_str(&json).expect("Failed to deserialize");

    assert_eq!(deserialized.segments[0].min_seq, SeqId::from(1_000_000_000i64));
    assert_eq!(deserialized.segments[0].max_seq, SeqId::from(2_000_000_000i64));
    assert_eq!(deserialized.segments[0].row_count, 1_000_000_000);
}
