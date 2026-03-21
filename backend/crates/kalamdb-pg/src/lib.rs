//! PostgreSQL remote service bridge for KalamDB.

pub mod query_executor;
mod service;
mod session_registry;

pub use query_executor::{
    DeleteRequest, InsertRequest, MutationResult, OperationExecutor, ScanRequest, ScanResult,
    UpdateRequest,
};
pub use query_executor::{
    delete_request_from_rpc, encode_batches, insert_request_from_rpc, parse_row, parse_table_id,
    parse_table_type, parse_user_id, scan_request_from_rpc, scan_result_to_rpc,
    update_request_from_rpc,
};
pub use service::{
    open_session_response_to_session, DeleteRpcRequest, DeleteRpcResponse, InsertRpcRequest,
    InsertRpcResponse, KalamPgService, OpenSessionRequest, OpenSessionResponse, PgServiceClient,
    PgServiceServer, PingRequest, PingResponse, ScanRpcRequest, ScanRpcResponse, UpdateRpcRequest,
    UpdateRpcResponse,
};
pub use session_registry::{RemotePgSession, SessionRegistry};
