use std::str::FromStr;

use arrow::record_batch::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use async_trait::async_trait;
use tonic::Status;

use kalamdb_commons::models::rows::Row;
use kalamdb_commons::models::UserId;
use kalamdb_commons::{TableId, TableType};

use crate::service::{ScanRpcRequest, ScanRpcResponse};
use crate::{InsertRpcRequest, UpdateRpcRequest, DeleteRpcRequest};

// Re-export domain types from kalamdb-commons (canonical location).
pub use kalamdb_commons::models::pg_operations::{
    DeleteRequest, InsertRequest, MutationResult, ScanRequest, ScanResult, UpdateRequest,
};

/// Domain-typed query executor.
///
/// `KalamPgService` translates gRPC wire types into domain types, calls this
/// trait, then encodes domain results back to gRPC responses.
#[async_trait]
pub trait OperationExecutor: Send + Sync + 'static {
    async fn execute_scan(&self, request: ScanRequest) -> Result<ScanResult, Status>;
    async fn execute_insert(&self, request: InsertRequest) -> Result<MutationResult, Status>;
    async fn execute_update(&self, request: UpdateRequest) -> Result<MutationResult, Status>;
    async fn execute_delete(&self, request: DeleteRequest) -> Result<MutationResult, Status>;
    async fn execute_sql(&self, sql: &str) -> Result<String, Status>;
}

// ── Helpers for gRPC ↔ domain translation ──────────────────────────────────

pub fn parse_table_type(s: &str) -> Result<TableType, Status> {
    TableType::from_str(s)
        .map_err(|e| Status::invalid_argument(format!("invalid table_type: {}", e)))
}

pub fn parse_table_id(namespace: &str, table_name: &str) -> Result<TableId, Status> {
    use kalamdb_commons::models::{NamespaceId, TableName};
    if namespace.trim().is_empty() {
        return Err(Status::invalid_argument("namespace must not be empty"));
    }
    if table_name.trim().is_empty() {
        return Err(Status::invalid_argument("table_name must not be empty"));
    }
    Ok(TableId::new(
        NamespaceId::new(namespace.trim()),
        TableName::new(table_name.trim()),
    ))
}

pub fn parse_user_id(raw: Option<&str>) -> Option<UserId> {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(UserId::new)
}

pub fn parse_row(json: &str) -> Result<Row, Status> {
    serde_json::from_str::<Row>(json)
        .map_err(|e| Status::invalid_argument(format!("invalid row JSON: {}", e)))
}

/// Encode Arrow RecordBatches into IPC bytes for gRPC transport.
pub fn encode_batches(
    batches: &[RecordBatch],
) -> Result<(Vec<bytes::Bytes>, Option<bytes::Bytes>), Status> {
    if batches.is_empty() {
        return Ok((Vec::new(), None));
    }

    let schema = batches[0].schema();
    let mut ipc_batches = Vec::with_capacity(batches.len());

    for batch in batches {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(|e| {
                Status::internal(format!("failed to create IPC writer: {}", e))
            })?;
            writer.write(batch).map_err(|e| {
                Status::internal(format!("failed to write IPC batch: {}", e))
            })?;
            writer.finish().map_err(|e| {
                Status::internal(format!("failed to finish IPC writer: {}", e))
            })?;
        }
        ipc_batches.push(bytes::Bytes::from(buf));
    }

    Ok((ipc_batches, None))
}

/// Convert a `ScanRpcRequest` into a domain `ScanRequest`.
pub fn scan_request_from_rpc(rpc: &ScanRpcRequest) -> Result<ScanRequest, Status> {
    let table_id = parse_table_id(&rpc.namespace, &rpc.table_name)?;
    let table_type = parse_table_type(&rpc.table_type)?;
    Ok(ScanRequest {
        table_id,
        table_type,
        columns: rpc.columns.clone(),
        limit: rpc.limit.map(|l| l as usize),
        user_id: parse_user_id(rpc.user_id.as_deref()),
    })
}

/// Convert a domain `ScanResult` into a `ScanRpcResponse`.
pub fn scan_result_to_rpc(result: ScanResult) -> Result<ScanRpcResponse, Status> {
    let (ipc_batches, schema_ipc) = encode_batches(&result.batches)?;
    Ok(ScanRpcResponse {
        ipc_batches,
        schema_ipc,
    })
}

/// Convert an `InsertRpcRequest` into a domain `InsertRequest`.
pub fn insert_request_from_rpc(rpc: &InsertRpcRequest) -> Result<InsertRequest, Status> {
    let table_id = parse_table_id(&rpc.namespace, &rpc.table_name)?;
    let table_type = parse_table_type(&rpc.table_type)?;
    let mut rows = Vec::with_capacity(rpc.rows_json.len());
    for json in &rpc.rows_json {
        rows.push(parse_row(json)?);
    }
    Ok(InsertRequest {
        table_id,
        table_type,
        user_id: parse_user_id(rpc.user_id.as_deref()),
        rows,
    })
}

/// Convert an `UpdateRpcRequest` into a domain `UpdateRequest`.
pub fn update_request_from_rpc(rpc: &UpdateRpcRequest) -> Result<UpdateRequest, Status> {
    let table_id = parse_table_id(&rpc.namespace, &rpc.table_name)?;
    let table_type = parse_table_type(&rpc.table_type)?;
    let updates = vec![parse_row(&rpc.updates_json)?];
    Ok(UpdateRequest {
        table_id,
        table_type,
        user_id: parse_user_id(rpc.user_id.as_deref()),
        updates,
        pk_value: rpc.pk_value.clone(),
    })
}

/// Convert a `DeleteRpcRequest` into a domain `DeleteRequest`.
pub fn delete_request_from_rpc(rpc: &DeleteRpcRequest) -> Result<DeleteRequest, Status> {
    let table_id = parse_table_id(&rpc.namespace, &rpc.table_name)?;
    let table_type = parse_table_type(&rpc.table_type)?;
    Ok(DeleteRequest {
        table_id,
        table_type,
        user_id: parse_user_id(rpc.user_id.as_deref()),
        pk_value: rpc.pk_value.clone(),
    })
}
