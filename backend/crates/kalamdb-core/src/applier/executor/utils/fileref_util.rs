use datafusion::scalar::ScalarValue;
use kalamdb_commons::{
    models::{datatypes::KalamDataType, rows::Row, UserId},
    schemas::TableType,
    TableId,
};
use kalamdb_system::FileRef;

use crate::app_context::AppContext;

pub fn collect_file_refs_from_row(
    app_context: &AppContext,
    table_id: &TableId,
    row: &Row,
) -> Vec<FileRef> {
    let schema_registry = app_context.schema_registry();
    let table_def = match schema_registry.get_table_if_exists(table_id) {
        Ok(Some(def)) => def,
        Ok(None) => return Vec::new(),
        Err(err) => {
            log::warn!(
                "Failed to load table definition for {} while collecting file refs: {}",
                table_id,
                err
            );
            return Vec::new();
        },
    };

    let mut refs = Vec::new();
    for column in table_def.columns.iter().filter(|c| c.data_type == KalamDataType::File) {
        if let Some(value) = row.values.get(&column.column_name) {
            let json_opt = match value {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s),
                _ => None,
            };

            if let Some(json) = json_opt {
                match FileRef::from_json(json) {
                    Ok(file_ref) => refs.push(file_ref),
                    Err(err) => log::warn!(
                        "Failed to parse FileRef JSON for {}.{}: {}",
                        table_id,
                        column.column_name,
                        err
                    ),
                }
            }
        }
    }

    refs
}

pub fn collect_replaced_file_refs_for_update(
    app_context: &AppContext,
    table_id: &TableId,
    prior_row: &Row,
    updates: &Row,
) -> Vec<FileRef> {
    let schema_registry = app_context.schema_registry();
    let table_def = match schema_registry.get_table_if_exists(table_id) {
        Ok(Some(def)) => def,
        Ok(None) => return Vec::new(),
        Err(err) => {
            log::warn!(
                "Failed to load table definition for {} while collecting file refs: {}",
                table_id,
                err
            );
            return Vec::new();
        },
    };

    let mut refs = Vec::new();
    for column in table_def.columns.iter().filter(|c| c.data_type == KalamDataType::File) {
        let update_value = match updates.values.get(&column.column_name) {
            Some(value) => value,
            None => continue,
        };

        let update_json = match update_value {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s),
            ScalarValue::Null => None,
            _ => None,
        };

        let prior_json = match prior_row.values.get(&column.column_name) {
            Some(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => Some(s),
            _ => None,
        };

        let prior_ref = match prior_json {
            Some(json) => match FileRef::from_json(json) {
                Ok(file_ref) => Some(file_ref),
                Err(err) => {
                    log::warn!(
                        "Failed to parse prior FileRef JSON for {}.{}: {}",
                        table_id,
                        column.column_name,
                        err
                    );
                    None
                },
            },
            None => None,
        };

        let update_ref = match update_json {
            Some(json) => match FileRef::from_json(json) {
                Ok(file_ref) => Some(file_ref),
                Err(err) => {
                    log::warn!(
                        "Failed to parse update FileRef JSON for {}.{}: {}",
                        table_id,
                        column.column_name,
                        err
                    );
                    None
                },
            },
            None => None,
        };

        if let Some(prior_file_ref) = prior_ref {
            let should_delete = match update_ref {
                Some(new_ref) => new_ref.id != prior_file_ref.id,
                None => true,
            };

            if should_delete {
                refs.push(prior_file_ref);
            }
        }
    }

    refs
}

pub async fn delete_file_refs_best_effort(
    app_context: &AppContext,
    table_id: &TableId,
    table_type: TableType,
    user_id: Option<&UserId>,
    file_refs: &[FileRef],
) {
    if file_refs.is_empty() {
        return;
    }

    let schema_registry = app_context.schema_registry();
    let storage_id = match schema_registry.get_storage_id(table_id) {
        Ok(id) => id,
        Err(err) => {
            log::warn!(
                "Failed to resolve storage_id for {} while deleting file refs: {}",
                table_id,
                err
            );
            return;
        },
    };

    let file_service = app_context.file_storage_service();
    let results = file_service
        .delete_files(file_refs, &storage_id, table_type, table_id, user_id)
        .await;

    for (file_ref, result) in file_refs.iter().zip(results.into_iter()) {
        if let Err(err) = result {
            log::warn!("Failed to delete file {} for {}: {}", file_ref.id, table_id, err);
        }
    }
}
