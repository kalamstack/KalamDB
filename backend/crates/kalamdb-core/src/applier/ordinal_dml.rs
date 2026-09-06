//! Encode Raft DML rows with the ordinal RocksDB field codec.
//!
//! Protocol commands still use a KOBJ + FlexBuffers envelope. INSERT used to
//! FlexBuffer `Row` (`BTreeMap<String, ScalarValue>`), which is larger and
//! slower than the ordinal payload RocksDB already stores. `commit_seq` is
//! assigned at apply from the log index, so the Raft blob is fields-only.

use std::sync::Arc;

use kalamdb_commons::{models::rows::Row, TableId};
use kalamdb_serialization::{decode_row_fields, encode_row_fields, StorageSchema};
use kalamdb_tables::storage_schema_for_table;

use crate::app_context::AppContext;

/// Encode insert rows for a Raft proposal.
///
/// On success, `rows` is emptied and `encoded_fields` holds ordinal payloads.
/// If the table schema is missing or encode fails, logical `rows` are kept so
/// followers can still apply (same FlexBuffers path as before).
pub fn encode_insert_rows(
    app_context: &AppContext,
    table_id: &TableId,
    rows: Vec<Row>,
) -> (Vec<Row>, Vec<Vec<u8>>) {
    if rows.is_empty() {
        return (rows, Vec::new());
    }
    let Some(schema) = storage_schema(app_context, table_id) else {
        return (rows, Vec::new());
    };

    let mut encoded_fields = Vec::with_capacity(rows.len());
    for row in &rows {
        match encode_row_fields(row, schema.as_ref()) {
            Ok(bytes) => encoded_fields.push(bytes),
            Err(error) => {
                log::warn!(
                    "ordinal DML encode failed for {table_id}: {error}; proposing logical rows"
                );
                return (rows, Vec::new());
            },
        }
    }
    (Vec::new(), encoded_fields)
}

/// Decode insert rows at apply time.
///
/// Prefer `encoded_fields` when present so new logs skip FlexBuffers maps.
/// Empty `encoded_fields` is the pre-change log shape (`rows` populated).
pub fn decode_insert_rows(
    app_context: &AppContext,
    table_id: &TableId,
    rows: &[Row],
    encoded_fields: &[Vec<u8>],
) -> Result<Vec<Row>, String> {
    if encoded_fields.is_empty() {
        return Ok(rows.to_vec());
    }
    let schema = storage_schema(app_context, table_id)
        .ok_or_else(|| format!("missing storage schema for ordinal insert into {table_id}"))?;
    let mut decoded = Vec::with_capacity(encoded_fields.len());
    for payload in encoded_fields {
        decoded
            .push(decode_row_fields(payload, schema.as_ref()).map_err(|error| error.to_string())?);
    }
    Ok(decoded)
}

fn storage_schema(app_context: &AppContext, table_id: &TableId) -> Option<Arc<StorageSchema>> {
    let cached = app_context.schema_registry().get(table_id)?;
    match storage_schema_for_table(&cached.table) {
        Ok(schema) => Some(schema),
        Err(error) => {
            log::warn!("storage schema for {table_id} is invalid: {error}");
            None
        },
    }
}
