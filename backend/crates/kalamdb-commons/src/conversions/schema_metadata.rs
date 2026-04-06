//! Arrow schema metadata helpers
//!
//! Centralized helpers for reading/writing Arrow Field metadata keys used by KalamDB.
//! This ensures consistent key names and serialization across the codebase.

use crate::models::datatypes::KalamDataType;
use crate::schemas::FieldFlags;
use arrow_schema::Field;

/// Metadata key for serialized KalamDataType.
pub const KALAM_DATA_TYPE_METADATA_KEY: &str = "kalam_data_type";
/// Metadata key for serialized `FieldFlags`.
pub const KALAM_COLUMN_FLAGS_METADATA_KEY: &str = "kalam_column_flags";

/// Read `KalamDataType` from Arrow field metadata.
pub fn read_kalam_data_type_metadata(field: &Field) -> Option<KalamDataType> {
    field
        .metadata()
        .get(KALAM_DATA_TYPE_METADATA_KEY)
        .and_then(|s| serde_json::from_str::<KalamDataType>(s).ok())
}

/// Attach `KalamDataType` metadata to an Arrow field.
///
/// Preserves any existing metadata on the field.
pub fn with_kalam_data_type_metadata(mut field: Field, kalam_type: &KalamDataType) -> Field {
    let kalam_type_json =
        serde_json::to_string(kalam_type).unwrap_or_else(|_| "\"Text\"".to_string());
    let mut metadata = field.metadata().clone();
    metadata.insert(KALAM_DATA_TYPE_METADATA_KEY.to_string(), kalam_type_json);
    field = field.with_metadata(metadata);
    field
}

/// Read typed `FieldFlags` from Arrow field metadata.
pub fn read_kalam_column_flags_metadata(field: &Field) -> Option<FieldFlags> {
    field
        .metadata()
        .get(KALAM_COLUMN_FLAGS_METADATA_KEY)
        .and_then(|s| serde_json::from_str::<FieldFlags>(s).ok())
}

/// Attach typed `FieldFlags` to an Arrow field.
pub fn with_kalam_column_flags_metadata(mut field: Field, flags: &FieldFlags) -> Field {
    let mut metadata = field.metadata().clone();
    let flags_json = serde_json::to_string(flags).unwrap_or_else(|_| "[]".to_string());
    metadata.insert(KALAM_COLUMN_FLAGS_METADATA_KEY.to_string(), flags_json);
    field = field.with_metadata(metadata);
    field
}

/// Convert an Arrow schema into KalamDB `SchemaField` descriptors.
///
/// Reads `KalamDataType` from field metadata when present, otherwise infers
/// from the Arrow `DataType`. Falls back to `Text` for unsupported types.
#[cfg(feature = "arrow-conversion")]
pub fn schema_fields_from_arrow_schema(
    arrow_schema: &arrow_schema::SchemaRef,
) -> Vec<crate::schemas::SchemaField> {
    use crate::conversions::arrow_conversion::FromArrowType;
    use crate::models::datatypes::KalamDataType;

    arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let kalam_type = read_kalam_data_type_metadata(field).unwrap_or_else(|| {
                KalamDataType::from_arrow_type(field.data_type()).unwrap_or(KalamDataType::Text)
            });
            crate::schemas::SchemaField::from_arrow_field(field, kalam_type, index)
        })
        .collect()
}

/// Mask sensitive columns (`credentials`, `password_hash`) for non-admin roles.
///
/// Admin roles (`Dba`, `System`) see unmasked values. All other roles see `"***"`.
#[cfg(feature = "arrow-conversion")]
pub fn mask_sensitive_rows_for_role(
    rows: &mut [Vec<crate::models::KalamCellValue>],
    schema_fields: &[crate::schemas::SchemaField],
    user_role: crate::models::Role,
) {
    use crate::models::KalamCellValue;

    if matches!(user_role, crate::models::Role::Dba | crate::models::Role::System) {
        return;
    }
    for target in &["credentials", "password_hash"] {
        if let Some(col_idx) =
            schema_fields.iter().position(|f| f.name.eq_ignore_ascii_case(target))
        {
            for row in rows.iter_mut() {
                if let Some(v) = row.get_mut(col_idx) {
                    if !v.is_null() {
                        *v = KalamCellValue::text("***");
                    }
                }
            }
        }
    }
}
