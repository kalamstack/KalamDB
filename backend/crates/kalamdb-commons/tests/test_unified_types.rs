//! Integration tests for unified type system (Phase 4 - 008-schema-consolidation)
//!
//! Tests that all 13 KalamDataTypes convert to Arrow and back losslessly

use arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit};
use kalamdb_commons::models::datatypes::{FromArrowType, KalamDataType, ToArrowType};
use std::sync::Arc;

#[test]
fn test_all_kalambdata_types_convert_to_arrow_losslessly() {
    let test_cases = vec![
        (KalamDataType::Boolean, ArrowDataType::Boolean),
        (KalamDataType::Int, ArrowDataType::Int32),
        (KalamDataType::BigInt, ArrowDataType::Int64),
        (KalamDataType::Float, ArrowDataType::Float32),
        (KalamDataType::Double, ArrowDataType::Float64),
        (KalamDataType::Text, ArrowDataType::Utf8),
        (KalamDataType::Timestamp, ArrowDataType::Timestamp(TimeUnit::Microsecond, None)),
        (KalamDataType::Date, ArrowDataType::Date32),
        (
            KalamDataType::DateTime,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        (KalamDataType::Time, ArrowDataType::Time64(TimeUnit::Microsecond)),
        // Note: Json maps to Utf8 (same as Text), so roundtrip will return Text
        // This is expected - we can't distinguish them in Arrow
        (KalamDataType::Json, ArrowDataType::Utf8),
        (KalamDataType::Bytes, ArrowDataType::Binary),
        // EMBEDDING(384) example
        (
            KalamDataType::Embedding(384),
            ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                384,
            ),
        ),
    ];

    for (kalam_type, expected_arrow_type) in test_cases {
        // Convert KalamDataType → Arrow
        let arrow_type = kalam_type.to_arrow_type().expect("Should convert to Arrow");
        assert_eq!(
            arrow_type, expected_arrow_type,
            "KalamDataType::{:?} should convert to {:?}",
            kalam_type, expected_arrow_type
        );

        // Convert Arrow → KalamDataType (roundtrip)
        // Note: Json → Utf8 → Text (expected loss of type information)
        let roundtrip = KalamDataType::from_arrow_type(&arrow_type).expect("Should roundtrip");

        // Special case: Json and Text both map to Utf8, so roundtrip returns Text
        if kalam_type == KalamDataType::Json {
            assert_eq!(
                roundtrip,
                KalamDataType::Text,
                "Json should roundtrip to Text (both use Utf8)"
            );
        } else {
            assert_eq!(
                roundtrip, kalam_type,
                "Arrow type {:?} should roundtrip to {:?}",
                arrow_type, kalam_type
            );
        }
    }

    println!("✅ All 13 KalamDataTypes convert to Arrow correctly (Json→Utf8→Text is expected)");
}

#[test]
fn test_embedding_dimensions_work_correctly() {
    let dimensions = vec![384, 768, 1536, 3072];

    for dim in dimensions {
        let kalam_type = KalamDataType::Embedding(dim);
        let arrow_type = kalam_type.to_arrow_type().expect("Should convert to Arrow");

        // Verify it's a FixedSizeList
        match &arrow_type {
            ArrowDataType::FixedSizeList(field, size) => {
                assert_eq!(*size, dim as i32, "Dimension should match");
                assert_eq!(
                    field.data_type(),
                    &ArrowDataType::Float32,
                    "Element type should be Float32"
                );
                assert!(!field.is_nullable(), "Elements should not be nullable");
            },
            _ => panic!("EMBEDDING should convert to FixedSizeList, got {:?}", arrow_type),
        }

        // Roundtrip test
        let roundtrip = KalamDataType::from_arrow_type(&arrow_type).expect("Should roundtrip");
        assert_eq!(roundtrip, kalam_type, "EMBEDDING({}) should roundtrip correctly", dim);
    }

    println!(
        "✅ EMBEDDING(384), EMBEDDING(768), EMBEDDING(1536), EMBEDDING(3072) all work correctly"
    );
}

#[test]
fn test_type_conversion_performance() {
    // Test that repeated conversions are fast (even without caching)
    let test_types = vec![
        KalamDataType::Int,
        KalamDataType::Text,
        KalamDataType::BigInt,
        KalamDataType::Float,
        KalamDataType::Timestamp,
        KalamDataType::Embedding(768),
    ];

    let iterations = 10_000;
    let start = std::time::Instant::now();

    for _ in 0..iterations {
        for kalam_type in &test_types {
            let arrow_type = kalam_type.to_arrow_type().expect("Should convert");
            let _ = KalamDataType::from_arrow_type(&arrow_type);
        }
    }

    let elapsed = start.elapsed();
    let total_ops = (iterations as usize * test_types.len() * 2) as u64;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "✅ Type conversion performance: {:.0} ops/sec ({} iterations × {} types × 2 directions = {} ops in {:?})",
        ops_per_sec,
        iterations,
        test_types.len(),
        total_ops,
        elapsed
    );

    // Verify conversions complete in reasonable time (< 1 second for 120,000 ops)
    assert!(
        elapsed.as_secs() < 1,
        "Type conversions should be fast (completed {} ops in {:?})",
        total_ops,
        elapsed
    );
}
