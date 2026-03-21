use std::collections::HashMap;

use actix_web::{error::ErrorInternalServerError, HttpResponse};
use bytes::Bytes;
use futures_util::stream;
use kalamdb_commons::models::{KalamCellValue, Role, Username};
use kalamdb_commons::schemas::SchemaField;
use kalamdb_core::providers::arrow_json_conversion::record_batch_to_json_arrays;

use super::converter::{
    column_indices_from_arrow_schema, mask_sensitive_rows_for_role, resolve_arrow_schema,
    row_result_prefix, schema_fields_from_arrow_schema, success_response_suffix,
};

struct StreamingRowsState {
    prefix: Option<Bytes>,
    batches: std::vec::IntoIter<arrow::record_batch::RecordBatch>,
    suffix: Option<Bytes>,
    column_indices: HashMap<String, usize>,
    user_role: Option<Role>,
    row_separator_needed: bool,
}

fn serialize_rows_chunk(
    rows: &[Vec<KalamCellValue>],
    row_separator_needed: &mut bool,
) -> Result<Option<Bytes>, serde_json::Error> {
    if rows.is_empty() {
        return Ok(None);
    }

    let mut chunk = String::new();
    for row in rows {
        if *row_separator_needed {
            chunk.push(',');
        } else {
            *row_separator_needed = true;
        }
        chunk.push_str(&serde_json::to_string(row)?);
    }

    Ok(Some(Bytes::from(chunk)))
}

pub fn stream_sql_rows_response(
    batches: Vec<arrow::record_batch::RecordBatch>,
    schema: Option<arrow::datatypes::SchemaRef>,
    user_role: Option<Role>,
    as_user: Username,
    row_count: usize,
    took: f64,
) -> Result<HttpResponse, actix_web::Error> {
    let arrow_schema = resolve_arrow_schema(&batches, schema)
        .ok_or_else(|| ErrorInternalServerError("Missing schema for row response"))?;
    let schema_fields: Vec<SchemaField> = schema_fields_from_arrow_schema(&arrow_schema);
    let column_indices = column_indices_from_arrow_schema(&arrow_schema);

    let prefix = row_result_prefix(&schema_fields)
        .map(Bytes::from)
        .map_err(ErrorInternalServerError)?;
    let suffix = Bytes::from(success_response_suffix(row_count, &as_user, took));

    let response_stream = stream::unfold(
        StreamingRowsState {
            prefix: Some(prefix),
            batches: batches.into_iter(),
            suffix: Some(suffix),
            column_indices,
            user_role,
            row_separator_needed: false,
        },
        |mut state| async move {
            if let Some(prefix) = state.prefix.take() {
                return Some((Ok(prefix), state));
            }

            while let Some(batch) = state.batches.next() {
                let mut rows = match record_batch_to_json_arrays(&batch) {
                    Ok(rows) => rows,
                    Err(err) => return Some((Err(ErrorInternalServerError(err)), state)),
                };
                mask_sensitive_rows_for_role(&mut rows, &state.column_indices, state.user_role);

                match serialize_rows_chunk(&rows, &mut state.row_separator_needed) {
                    Ok(Some(chunk)) => return Some((Ok(chunk), state)),
                    Ok(None) => continue,
                    Err(err) => return Some((Err(ErrorInternalServerError(err)), state)),
                }
            }

            state.suffix.take().map(|suffix| (Ok(suffix), state))
        },
    );

    Ok(HttpResponse::Ok().content_type("application/json").streaming(response_stream))
}
