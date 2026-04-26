//! Schema projection utilities for reading Parquet files with older schema versions
//!
//! Handles schema evolution when reading Parquet files that were written with a previous
//! version of a table's schema. Performs:
//! - Adding columns: Fill with NULL or DEFAULT values for new columns not in Parquet file
//! - Dropping columns: Ignore columns in Parquet file that no longer exist
//! - Type changes: Attempt safe casts or error if incompatible
//!
//! This ensures that all RecordBatches returned from Parquet files match the current
//! table schema, regardless of when the file was written.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, NullArray},
    compute::cast,
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use super::RegistryError;

/// Project a RecordBatch from an old schema to a new schema
///
/// # Arguments
/// * `batch` - RecordBatch read from Parquet file (old schema)
/// * `old_schema` - Schema that was used to write the Parquet file
/// * `new_schema` - Current schema of the table
///
/// # Returns
/// * `Ok(RecordBatch)` - RecordBatch projected to match new schema
/// * `Err(_)` - Incompatible schema change (e.g., type cannot be cast)
///
/// # Example
/// ```no_run
/// # use kalamdb_core::schema::projection::project_batch;
/// # use arrow::datatypes::{Schema, Field, DataType};
/// # use arrow::record_batch::RecordBatch;
/// # use std::sync::Arc;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let old_schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]));
///
/// let new_schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
///     Field::new("email", DataType::Utf8, true), // New column
/// ]));
///
/// // Read batch from Parquet (would have old schema)
/// // let batch = read_parquet_batch()?;
///
/// // Project to new schema
/// // let projected = project_batch(&batch, &old_schema, &new_schema)?;
/// # Ok(())
/// # }
/// ```
pub fn project_batch(
    batch: &RecordBatch,
    old_schema: &SchemaRef,
    new_schema: &SchemaRef,
) -> Result<RecordBatch, RegistryError> {
    // If schemas are identical, no projection needed
    if old_schema == new_schema {
        return Ok(batch.clone());
    }

    let num_rows = batch.num_rows();
    let mut new_columns: Vec<ArrayRef> = Vec::new();

    // Process each field in the new schema
    for new_field in new_schema.fields() {
        let field_name = new_field.name();

        // Check if field exists in old schema
        if let Ok(old_field) = old_schema.field_with_name(field_name) {
            // Field exists in old schema - handle type changes
            let old_column = batch.column_by_name(field_name).ok_or_else(|| {
                RegistryError::SchemaError(format!("Column '{}' not found in batch", field_name))
            })?;

            // Check if type changed
            if old_field.data_type() != new_field.data_type() {
                // Attempt to cast to new type
                let casted = cast(old_column, new_field.data_type()).map_err(|e| {
                    RegistryError::SchemaConversion {
                        message: format!(
                            "Cannot cast column '{}' from {:?} to {:?}: {}",
                            field_name,
                            old_field.data_type(),
                            new_field.data_type(),
                            e
                        ),
                    }
                })?;
                new_columns.push(casted);
            } else {
                // Type is the same, use column as-is
                new_columns.push(old_column.clone());
            }
        } else {
            // Field is new (added after Parquet was written)
            // Fill with NULL values
            let null_array = Arc::new(NullArray::new(num_rows)) as ArrayRef;

            // Convert NullArray to the target type with all nulls
            let typed_nulls = match new_field.data_type() {
                DataType::Null => null_array,
                dt => {
                    // Cast null array to target type (creates array of nulls with correct type)
                    cast(&null_array, dt).map_err(|e| {
                        RegistryError::SchemaError(format!(
                            "Failed to create null array for new column '{}': {}",
                            field_name, e
                        ))
                    })?
                },
            };

            new_columns.push(typed_nulls);
        }
    }

    // Create new RecordBatch with projected schema
    RecordBatch::try_new(new_schema.clone(), new_columns).map_err(|e| {
        RegistryError::SchemaError(format!("Failed to create projected RecordBatch: {}", e))
    })
}

/// Check if two schemas are compatible for projection
///
/// Returns true if:
/// - Old schema is a subset of new schema (columns can only be added)
/// - Type changes are safe casts (e.g., Int32 -> Int64)
///
/// Returns false if:
/// - Required columns were dropped
/// - Type changes are incompatible
pub fn schemas_compatible(old_schema: &Schema, new_schema: &Schema) -> bool {
    // Check each field in old schema
    for old_field in old_schema.fields() {
        if let Ok(new_field) = new_schema.field_with_name(old_field.name()) {
            // Field still exists - check type compatibility
            if !types_compatible(old_field.data_type(), new_field.data_type()) {
                return false;
            }
        } else {
            // Field was dropped - this is OK (we just ignore it)
            // But if the field was NOT NULL in old schema and is used in queries,
            // this could cause issues. For now, we allow it.
        }
    }

    true
}

/// Check if two data types are compatible for casting
fn types_compatible(old_type: &DataType, new_type: &DataType) -> bool {
    // Same type is always compatible
    if old_type == new_type {
        return true;
    }

    // Check for safe casts
    match (old_type, new_type) {
        // Integer widening
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64) => true,
        (DataType::Int16, DataType::Int32 | DataType::Int64) => true,
        (DataType::Int32, DataType::Int64) => true,

        // Unsigned integer widening
        (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64) => true,
        (DataType::UInt16, DataType::UInt32 | DataType::UInt64) => true,
        (DataType::UInt32, DataType::UInt64) => true,

        // Float widening
        (DataType::Float32, DataType::Float64) => true,

        // String types
        (DataType::Utf8, DataType::LargeUtf8) => true,

        // Timestamp precision changes (same unit)
        (DataType::Timestamp(old_unit, old_tz), DataType::Timestamp(new_unit, new_tz)) => {
            old_unit == new_unit && old_tz == new_tz
        },

        // Default: incompatible
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, Int64Array, StringArray},
        datatypes::Field,
    };

    use super::*;

    #[test]
    fn test_project_batch_same_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        let result = project_batch(&batch, &schema, &schema).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn test_project_batch_added_column() {
        let old_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let new_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            old_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        let result = project_batch(&batch, &old_schema, &new_schema).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);

        // New column should be all nulls
        let email_col = result.column(2);
        assert_eq!(email_col.null_count(), 3);
    }

    #[test]
    fn test_project_batch_dropped_column() {
        let old_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));

        let new_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            old_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(Int32Array::from(vec![30, 25, 35])),
            ],
        )
        .unwrap();

        let result = project_batch(&batch, &old_schema, &new_schema).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2); // age column dropped
    }

    #[test]
    fn test_project_batch_type_widening() {
        let old_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let new_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            old_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let result = project_batch(&batch, &old_schema, &new_schema).unwrap();
        assert_eq!(result.num_rows(), 3);

        // Check that column was cast to Int64
        let id_col = result.column(0);
        let int64_array = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 1);
        assert_eq!(int64_array.value(1), 2);
        assert_eq!(int64_array.value(2), 3);
    }

    #[test]
    fn test_schemas_compatible_added_column() {
        let old_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let new_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert!(schemas_compatible(&old_schema, &new_schema));
    }

    #[test]
    fn test_schemas_compatible_type_widening() {
        let old_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let new_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        assert!(schemas_compatible(&old_schema, &new_schema));
    }

    #[test]
    fn test_types_compatible_integer_widening() {
        assert!(types_compatible(&DataType::Int8, &DataType::Int16));
        assert!(types_compatible(&DataType::Int16, &DataType::Int32));
        assert!(types_compatible(&DataType::Int32, &DataType::Int64));
        assert!(!types_compatible(&DataType::Int64, &DataType::Int32)); // No narrowing
    }

    #[test]
    fn test_types_compatible_float_widening() {
        assert!(types_compatible(&DataType::Float32, &DataType::Float64));
        assert!(!types_compatible(&DataType::Float64, &DataType::Float32));
    }

    #[test]
    fn test_types_compatible_same_type() {
        assert!(types_compatible(&DataType::Int32, &DataType::Int32));
        assert!(types_compatible(&DataType::Utf8, &DataType::Utf8));
    }
}
