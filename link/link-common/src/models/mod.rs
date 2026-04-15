//! Data models shared by the KalamDB SDK.
//!
//! Core shared types (cell values, schema, file references) live here.
//! Domain-specific models are organized in their respective modules:
//! - Auth:         [`crate::auth::models`]
//! - Connection:   [`crate::connection::models`]
//! - Consumer:     [`crate::consumer::models`]
//! - Query:        [`crate::query::models`]
//! - Subscription: [`crate::subscription::models`]

// ── Core shared types (defined here) ────────────────────────────────────────
pub mod file_ref;
pub mod kalam_cell_value;
pub mod kalam_data_type;
pub mod schema_field;
pub mod utils;

#[cfg(test)]
mod tests;

pub use file_ref::FileRef;
pub use kalam_cell_value::{KalamCellValue, RowData};
pub use kalam_data_type::KalamDataType;
pub use schema_field::{FieldFlag, FieldFlags, SchemaField};
pub use utils::parse_i64;

// ── Auth models ──────────────────────────────────────────────────────────────
pub use crate::auth::models::{
    LoginRequest, LoginResponse, LoginUserInfo, ServerSetupRequest, ServerSetupResponse,
    SetupStatusResponse, SetupUserInfo, Username, WsAuthCredentials,
};

// ── Connection models ────────────────────────────────────────────────────────
pub use crate::connection::models::{
    ClientMessage, ClusterHealthResponse, ClusterNodeHealth, CompressionType, ConnectionOptions,
    HealthCheckResponse, HttpVersion, ProtocolOptions, SerializationType, ServerMessage,
};

// ── Consumer models ──────────────────────────────────────────────────────────
#[cfg(feature = "consumer")]
pub use crate::consumer::models::{AckResponse, ConsumeMessage, ConsumeRequest, ConsumeResponse};

// ── Query models ─────────────────────────────────────────────────────────────
pub use crate::query::models::{
    ErrorDetail, QueryRequest, QueryResponse, QueryResult, ResponseStatus, UploadProgress,
};

// ── Subscription models ──────────────────────────────────────────────────────
pub use crate::subscription::models::{
    BatchControl, BatchStatus, ChangeEvent, ChangeTypeRaw, SubscriptionConfig, SubscriptionInfo,
    SubscriptionOptions, SubscriptionRequest,
};
