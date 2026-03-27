//! Arrow to JSON conversion helpers

use arrow::record_batch::RecordBatch;
use kalamdb_commons::conversions::{read_kalam_data_type_metadata, KALAM_DATA_TYPE_METADATA_KEY};
use kalamdb_commons::models::datatypes::{FromArrowType, KalamDataType};
use kalamdb_commons::models::KalamCellValue;
use kalamdb_commons::models::Role;
use kalamdb_commons::models::Username;
use kalamdb_commons::schemas::SchemaField;
use kalamdb_core::providers::arrow_json_conversion::record_batch_to_json_arrays;
use std::collections::HashMap;

use super::super::models::QueryResult;

/// Convert Arrow RecordBatches to QueryResult
pub fn record_batch_to_query_result(
    batches: Vec<RecordBatch>,
    schema: Option<arrow::datatypes::SchemaRef>,
    user_role: Option<Role>,
) -> Result<QueryResult, Box<dyn std::error::Error>> {
    // Get schema from first batch, or from explicitly provided schema for empty results
    let arrow_schema = match resolve_arrow_schema(&batches, schema) {
        Some(schema) => schema,
        None => return Ok(QueryResult::with_message("Query executed successfully".to_string())),
    };

    let schema_fields = schema_fields_from_arrow_schema(&arrow_schema);

    // Build column name to index mapping for sensitive column masking
    let column_indices = column_indices_from_arrow_schema(&arrow_schema);

    let mut rows = Vec::new();
    for batch in &batches {
        let batch_rows = record_batch_to_json_arrays(batch)
            .map_err(|e| format!("Failed to convert batch to JSON: {}", e))?;
        rows.extend(batch_rows);
    }

    // Mask sensitive columns for non-admin users
    if !is_admin_role(user_role) {
        mask_sensitive_column_array(&mut rows, &column_indices, "credentials");
        mask_sensitive_column_array(&mut rows, &column_indices, "password_hash");
    }

    let result = QueryResult::with_rows_and_schema(rows, schema_fields);
    Ok(result)
}

/// Mask a sensitive column with "***" (for array-based rows)
fn mask_sensitive_column_array(
    rows: &mut [Vec<KalamCellValue>],
    column_indices: &HashMap<String, usize>,
    target_column: &str,
) {
    if let Some(&col_idx) = column_indices.get(&target_column.to_lowercase()) {
        for row in rows.iter_mut() {
            if let Some(value) = row.get_mut(col_idx) {
                if !value.is_null() {
                    *value = KalamCellValue::text("***");
                }
            }
        }
    }
}

/// Check if user has admin privileges for viewing sensitive data.
fn is_admin_role(role: Option<Role>) -> bool {
    matches!(role, Some(Role::Dba) | Some(Role::System))
}

pub fn resolve_arrow_schema(
    batches: &[RecordBatch],
    schema: Option<arrow::datatypes::SchemaRef>,
) -> Option<arrow::datatypes::SchemaRef> {
    if !batches.is_empty() {
        Some(batches[0].schema())
    } else {
        schema
    }
}

pub fn schema_fields_from_arrow_schema(
    arrow_schema: &arrow::datatypes::SchemaRef,
) -> Vec<SchemaField> {
    arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let kalam_type = if let Some(metadata_type) = read_kalam_data_type_metadata(field) {
                metadata_type
            } else {
                if field.metadata().contains_key(KALAM_DATA_TYPE_METADATA_KEY) {
                    log::warn!(
                        "Invalid '{}' metadata for column '{}'; falling back to Arrow type inference ({:?})",
                        KALAM_DATA_TYPE_METADATA_KEY,
                        field.name(),
                        field.data_type()
                    );
                }

                match KalamDataType::from_arrow_type(field.data_type()) {
                    Ok(inferred_type) => inferred_type,
                    Err(err) => {
                        log::warn!(
                            "Unsupported Arrow type {:?} for column '{}': {}. Defaulting schema type to Text",
                            field.data_type(),
                            field.name(),
                            err
                        );
                        KalamDataType::Text
                    },
                }
            };

            SchemaField::from_arrow_field(field, kalam_type, index)
        })
        .collect()
}

pub fn column_indices_from_arrow_schema(
    arrow_schema: &arrow::datatypes::SchemaRef,
) -> HashMap<String, usize> {
    arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().to_lowercase(), i))
        .collect()
}

pub fn mask_sensitive_rows_for_role(
    rows: &mut [Vec<KalamCellValue>],
    column_indices: &HashMap<String, usize>,
    user_role: Option<Role>,
) {
    if !is_admin_role(user_role) {
        mask_sensitive_column_array(rows, column_indices, "credentials");
        mask_sensitive_column_array(rows, column_indices, "password_hash");
    }
}

pub fn row_result_prefix(schema_fields: &[SchemaField]) -> Result<String, serde_json::Error> {
    Ok(format!(
        "{{\"status\":\"success\",\"results\":[{{\"schema\":{},\"rows\":[",
        serde_json::to_string(schema_fields)?
    ))
}

pub fn success_response_suffix(row_count: usize, as_user: &Username, took: f64) -> String {
    let rounded = (took * 1000.0).round() / 1000.0;
    format!(
        "],\"row_count\":{},\"as_user\":{}}}],\"took\":{},\"error\":null}}",
        row_count,
        serde_json::to_string(as_user).unwrap_or_else(|_| "\"unknown\"".to_string()),
        rounded
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema};
    use kalamdb_commons::conversions::{
        with_kalam_column_flags_metadata, with_kalam_data_type_metadata,
    };
    use kalamdb_commons::schemas::{FieldFlag, FieldFlags};
    use std::sync::Arc;

    #[test]
    fn test_record_batch_to_query_result_includes_flags_and_omits_empty() {
        let id_field = with_kalam_column_flags_metadata(
            with_kalam_data_type_metadata(
                Field::new("id", DataType::FixedSizeBinary(16), false),
                &KalamDataType::Uuid,
            ),
            &FieldFlags::from([FieldFlag::PrimaryKey, FieldFlag::NonNull, FieldFlag::Unique]),
        );
        let tenant_field = with_kalam_column_flags_metadata(
            with_kalam_data_type_metadata(
                Field::new("tenant_id", DataType::Utf8, false),
                &KalamDataType::Text,
            ),
            &FieldFlags::from([FieldFlag::NonNull]),
        );
        let payload_field = with_kalam_data_type_metadata(
            Field::new("payload", DataType::Utf8, true),
            &KalamDataType::Text,
        );

        let schema = Arc::new(Schema::new(vec![id_field, tenant_field, payload_field]));

        let result = record_batch_to_query_result(vec![], Some(schema), None).unwrap();

        assert_eq!(result.schema.len(), 3);
        assert_eq!(result.schema[0].name, "id");
        assert!(matches!(
            result.schema[0].flags,
            Some(ref flags)
                if flags.contains(&FieldFlag::PrimaryKey)
                    && flags.contains(&FieldFlag::NonNull)
                    && flags.contains(&FieldFlag::Unique)
        ));
        assert_eq!(result.schema[1].name, "tenant_id");
        assert!(matches!(
            result.schema[1].flags,
            Some(ref flags) if flags.contains(&FieldFlag::NonNull)
        ));
        assert_eq!(result.schema[2].name, "payload");
        assert!(result.schema[2].flags.is_none());
    }

    #[test]
    fn test_record_batch_to_query_result_without_column_flags_metadata() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let empty_batch = RecordBatch::new_empty(schema);

        let result = record_batch_to_query_result(vec![empty_batch], None, None).unwrap();
        assert_eq!(result.schema.len(), 1);
        assert_eq!(result.schema[0].name, "name");
        assert!(result.schema[0].flags.is_none());
    }
}
