//! PostgreSQL remote service bridge for KalamDB.

pub mod query_executor;
mod service;
mod session_registry;

pub use query_executor::PgQueryExecutor;
pub use service::{
    open_session_response_to_session, DeleteRpcRequest, DeleteRpcResponse, InsertRpcRequest,
    InsertRpcResponse, KalamPgService, OpenSessionRequest, OpenSessionResponse, PgServiceClient,
    PgServiceServer, PingRequest, PingResponse, ScanRpcRequest, ScanRpcResponse, UpdateRpcRequest,
    UpdateRpcResponse,
};
pub use session_registry::{RemotePgSession, SessionRegistry};
