//! PostgreSQL remote service bridge for KalamDB.

#[cfg(feature = "server")]
pub mod operation_executor;
mod service;
#[cfg(feature = "server")]
mod session_registry;

#[cfg(feature = "server")]
pub use operation_executor::{
    delete_request_from_rpc, encode_batches, insert_request_from_rpc, parse_row, parse_table_id,
    parse_table_type, parse_user_id, scan_request_from_rpc, scan_result_to_rpc,
    update_request_from_rpc,
};
#[cfg(feature = "server")]
pub use operation_executor::{
    DeleteRequest, InsertRequest, MutationResult, OperationExecutor, ScanRequest, ScanResult,
    UpdateRequest,
};
#[cfg(feature = "server")]
pub use service::{open_session_response_to_session, KalamPgService, PgService, PgServiceServer};
pub use service::{
    BeginTransactionRequest, BeginTransactionResponse, CloseSessionRequest, CloseSessionResponse,
    CommitTransactionRequest, CommitTransactionResponse, DeleteRpcRequest, DeleteRpcResponse,
    ExecuteQueryRpcRequest, ExecuteQueryRpcResponse, ExecuteSqlRpcRequest, ExecuteSqlRpcResponse,
    InsertRpcRequest, InsertRpcResponse, OpenSessionRequest, OpenSessionResponse, PgServiceClient,
    PingRequest, PingResponse, RollbackTransactionRequest, RollbackTransactionResponse,
    ScanRpcRequest, ScanRpcResponse, UpdateRpcRequest, UpdateRpcResponse,
};
#[cfg(feature = "server")]
pub use kalamdb_commons::models::TransactionState;
#[cfg(feature = "server")]
pub use session_registry::{RemotePgSession, SessionRegistry};
