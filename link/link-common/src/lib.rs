//! Shared Rust implementation used by `kalam-client`, `kalam-consumer`, and
//! `kalam-link-dart`.

pub mod auth;
#[cfg(feature = "tokio-runtime")]
pub mod client;
pub mod compression;
pub(crate) mod connection;
#[cfg(feature = "consumer")]
pub mod consumer;
pub mod credentials;
pub mod error;
pub mod event_handlers;
#[path = "models/mod.rs"]
pub mod models;
pub mod query;
pub mod seq_id;
pub mod seq_tracking;
pub mod subscription;
pub mod timeouts;
pub mod timestamp;
#[cfg(feature = "wasm")]
#[path = "wasm/mod.rs"]
pub mod wasm;

#[cfg(feature = "tokio-runtime")]
pub use auth::{ArcDynAuthProvider, AuthProvider, DynamicAuthProvider, ResolvedAuth};
#[cfg(feature = "tokio-runtime")]
pub use client::KalamLinkClient;
#[cfg(feature = "consumer")]
pub use consumer::{
    AutoOffsetReset, CommitMode, CommitResult, ConsumerConfig, ConsumerOffsets, ConsumerRecord,
    PayloadMode, TopicOp,
};
#[cfg(all(feature = "tokio-runtime", feature = "consumer"))]
pub use consumer::{ConsumerBuilder, TopicConsumer};

pub use credentials::{CredentialStore, Credentials, MemoryCredentialStore};
pub use error::{KalamLinkError, Result};
pub use event_handlers::{ConnectionError, DisconnectReason, EventHandlers, MessageDirection};
pub use kalamdb_commons::{Role, UserId};
pub use models::{
    parse_i64, ChangeEvent, ClusterHealthResponse, ClusterNodeHealth, ConnectionOptions,
    ErrorDetail, FieldFlag, FieldFlags, FileRef, HealthCheckResponse, HttpVersion, KalamCellValue,
    KalamDataType, LoginRequest, LoginResponse, LoginUserInfo, QueryRequest, QueryResponse,
    QueryResult, RowData, SchemaField, ServerSetupRequest, ServerSetupResponse,
    SetupStatusResponse, SetupUserInfo, SubscriptionConfig, SubscriptionInfo, SubscriptionOptions,
    UploadProgress,
};
#[cfg(feature = "consumer")]
pub use models::{AckResponse, ConsumeMessage, ConsumeRequest, ConsumeResponse};
pub use seq_id::SeqId;
pub use timeouts::{KalamLinkTimeouts, KalamLinkTimeoutsBuilder};
pub use timestamp::{now, parse_iso8601, TimestampFormat, TimestampFormatter};

#[cfg(feature = "tokio-runtime")]
pub use query::AuthRefreshCallback;
#[cfg(feature = "tokio-runtime")]
pub use query::QueryExecutor;
#[cfg(feature = "tokio-runtime")]
pub use query::UploadProgressCallback;
#[cfg(feature = "tokio-runtime")]
pub use subscription::LiveRowsSubscription;
#[cfg(feature = "tokio-runtime")]
pub use subscription::SubscriptionManager;
pub use subscription::{LiveRowsConfig, LiveRowsEvent, LiveRowsMaterializer};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
