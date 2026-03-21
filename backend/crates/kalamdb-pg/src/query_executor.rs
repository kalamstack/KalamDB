use async_trait::async_trait;
use tonic::Status;

use crate::service::{
    DeleteRpcRequest, DeleteRpcResponse, InsertRpcRequest, InsertRpcResponse, ScanRpcRequest,
    ScanRpcResponse, UpdateRpcRequest, UpdateRpcResponse,
};

/// Backend query executor used by `KalamPgService` to handle remote scan/mutation RPCs.
///
/// Implemented in `kalamdb-core` where `AppContext` is available.
#[async_trait]
pub trait PgQueryExecutor: Send + Sync + 'static {
    async fn execute_scan(&self, request: ScanRpcRequest) -> Result<ScanRpcResponse, Status>;
    async fn execute_insert(&self, request: InsertRpcRequest) -> Result<InsertRpcResponse, Status>;
    async fn execute_update(&self, request: UpdateRpcRequest) -> Result<UpdateRpcResponse, Status>;
    async fn execute_delete(&self, request: DeleteRpcRequest) -> Result<DeleteRpcResponse, Status>;
}
