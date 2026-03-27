//! # kalam-link: KalamDB Client Library
//!
//! A WebAssembly-compatible client library for connecting to KalamDB servers.
//! Provides both HTTP query execution and WebSocket subscription capabilities.
//!
//! ## Features
//!
//! - **Query Execution**: Execute SQL queries via HTTP with automatic retry
//! - **WebSocket Subscriptions**: Real-time change notifications via WebSocket streams
//! - **Authentication**: HTTP Basic Auth, JWT token, and API key support
//! - **Credential Storage**: Platform-agnostic credential management trait
//! - **WASM Compatible**: Can be compiled to WebAssembly for browser usage
//! - **Connection Pooling**: Automatic HTTP connection reuse
//! - **Configurable Timeouts**: Per-request timeout and retry configuration
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use kalam_link::{KalamLinkClient, QueryRequest};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Build a client with custom configuration
//!     let client = KalamLinkClient::builder()
//!         .base_url("http://localhost:3000")
//!         .timeout(std::time::Duration::from_secs(30))
//!         .build()?;
//!
//!     // Execute a query
//!     let response = client
//!         .execute_query("SELECT * FROM users LIMIT 10", None, None, None)
//!         .await?;
//!     println!("Results: {:?}", response.results);
//!
//!     // Subscribe to real-time changes
//!     let mut subscription = client.subscribe("SELECT * FROM messages").await?;
//!     while let Some(event) = subscription.next().await {
//!         println!("Change detected: {:?}", event);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Authentication
//!
//! ```rust,no_run
//! use kalam_link::{KalamLinkClient, AuthProvider};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Using HTTP Basic Auth (recommended)
//! let client = KalamLinkClient::builder()
//!     .base_url("http://localhost:3000")
//!     .auth(AuthProvider::basic_auth("username".to_string(), "password".to_string()))
//!     .build()?;
//!
//! // Using system user (default "root" user created during DB initialization)
//! let client = KalamLinkClient::builder()
//!     .base_url("http://localhost:3000")
//!     .auth(AuthProvider::system_user_auth("admin_password".to_string()))
//!     .build()?;
//!
//! // Using JWT token
//! let client = KalamLinkClient::builder()
//!     .base_url("http://localhost:3000")
//!     .jwt_token("your-jwt-token")
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Credential Storage
//!
//! ```rust,no_run
//! use kalam_link::credentials::{CredentialStore, Credentials, MemoryCredentialStore};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut store = MemoryCredentialStore::new();
//!
//! // Store JWT token credentials (obtained from login)
//! let creds = Credentials::with_details(
//!     "production".to_string(),
//!     "eyJhbGciOiJIUzI1NiJ9.jwt_token_here".to_string(),
//!     "alice".to_string(),
//!     "2025-12-31T23:59:59Z".to_string(),
//!     Some("https://db.example.com".to_string()),
//! );
//! store.set_credentials(&creds)?;
//!
//! // Retrieve credentials
//! if let Some(stored) = store.get_credentials("production")? {
//!     if !stored.is_expired() {
//!         println!("Found valid token for user: {:?}", stored.username);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

pub mod compression;
pub mod error;
pub mod event_handlers;
#[path = "models/mod.rs"]
pub mod models;
pub mod seq_id;
pub mod seq_tracking;
pub mod timeouts;
pub mod timestamp;

// Credential storage (available in both native and WASM)
pub mod credentials;

// Always-available modules (contain data models usable in WASM too)
pub mod auth;
pub(crate) mod connection;
pub mod consumer;

// Native-only modules (require tokio, reqwest, tokio-tungstenite)
#[cfg(feature = "tokio-runtime")]
pub mod client;
pub mod query;
pub mod subscription;

// WASM bindings module (T041)
#[cfg(feature = "wasm")]
#[path = "wasm/mod.rs"]
pub mod wasm;

// Re-export main types for convenience
#[cfg(feature = "tokio-runtime")]
pub use auth::{ArcDynAuthProvider, AuthProvider, DynamicAuthProvider, ResolvedAuth};
#[cfg(feature = "tokio-runtime")]
pub use client::KalamLinkClient;
#[cfg(feature = "tokio-runtime")]
pub use consumer::{
    AutoOffsetReset, CommitMode, CommitResult, ConsumerConfig, ConsumerOffsets, ConsumerRecord,
    PayloadMode, TopicConsumer, TopicOp,
};

pub use credentials::{CredentialStore, Credentials, MemoryCredentialStore};
pub use error::{KalamLinkError, Result};
pub use event_handlers::{ConnectionError, DisconnectReason, EventHandlers, MessageDirection};
pub use models::{
    parse_i64, AckResponse, ChangeEvent, ConnectionOptions, ConsumeMessage, ConsumeRequest,
    ConsumeResponse, ErrorDetail, FieldFlag, FieldFlags, FileRef, HealthCheckResponse, HttpVersion,
    KalamCellValue, KalamDataType, LoginRequest, LoginResponse, LoginUserInfo, QueryRequest,
    QueryResponse, QueryResult, RowData, SchemaField, ServerSetupRequest, ServerSetupResponse,
    SetupStatusResponse, SetupUserInfo, SubscriptionConfig, SubscriptionInfo, SubscriptionOptions,
    UploadProgress,
};
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

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
