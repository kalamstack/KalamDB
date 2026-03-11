//! # kalamdb-commons
//!
//! Shared types, constants, and utilities for KalamDB.
//!
//! This crate provides foundational types and constants used across all KalamDB crates
//! (kalamdb-core, kalamdb-sql, kalamdb-store, kalamdb-api, kalamdb-live). It has zero
//! external dependencies to prevent circular dependency issues.
//!
//! ## Type-Safe Wrappers
//!
//! The crate provides type-safe wrappers for common identifiers:
//! - `UserId`: User identifier wrapper
//! - `NamespaceId`: Namespace identifier wrapper
//! - `TableName`: Table name wrapper
//! - `TableType`: Enum for USER/SHARED/STREAM tables
//!
//! ## System Table Models
//!
//! System table models live in `kalamdb-system`:
//! - `User`: System users (authentication, authorization)
//! - `Job`: Background jobs (flush, retention, cleanup)
//! - `Namespace`: Database namespaces
//! - `SystemTable`: Table metadata registry
//! - `LiveQuery`: Active WebSocket subscriptions
//! - `InformationSchemaTable`: SQL standard table metadata
//! - `UserTableCounter`: Per-user table flush tracking
//!
//! **CRITICAL**: DO NOT create duplicate model definitions elsewhere in the codebase.
//! Always import from `kalamdb_system::*` for system table models.
//!
//! ## Example Usage
//!
//! ```rust
//! use kalamdb_commons::models::{UserId, NamespaceId, TableName};
//! use kalamdb_system::{User, Job, LiveQuery};
//!
//! let user_id = UserId::new("user_123");
//! let namespace_id = NamespaceId::default();
//! let table_name = TableName::new("conversations");
//!
//! // Convert to string
//! let id_str: &str = user_id.as_str();
//! ```

pub mod constants;
#[cfg(feature = "full")]
pub mod conversions; // Centralized datatype and value conversion utilities (see conversions/mod.rs)
pub mod errors;
pub mod helpers;
pub mod ids;
pub mod models;
#[cfg(feature = "full")]
pub mod serialization; // KSerializable trait for entity storage
pub mod storage; // Storage backend abstraction (Partition, StorageError, etc.)
pub mod storage_key; // StorageKey trait for type-safe key serialization
pub mod system_tables; // System table enumeration (SystemTable, StoragePartition)
#[cfg(feature = "full")]
pub mod websocket;
#[cfg(feature = "websocket-auth")]
pub mod websocket_auth;

// Allow procedural macros to refer to this crate by name.
extern crate self as kalamdb_commons;

// Re-export commonly used types at crate root
pub use constants::{MAX_SQL_QUERY_LENGTH, RESERVED_NAMESPACE_NAMES};
#[cfg(feature = "full")]
pub use conversions::{
    as_f64, encode_pk_value, estimate_scalar_value_size, json_value_to_scalar_for_column,
    scalar_to_f64, scalar_to_i64, scalar_to_json_for_column, scalar_to_pk_string,
    scalar_value_to_bytes,
};
pub use errors::{CommonError, NotLeaderError, Result};
#[cfg(feature = "full")]
pub use helpers::arrow_utils;
#[cfg(feature = "full")]
pub use helpers::arrow_utils::{empty_batch, RecordBatchBuilder};
pub use helpers::file_helpers;
pub use helpers::security;
pub use helpers::string_interner;
pub use models::{
    // Phase 15 (008-schema-consolidation): Re-export schema types
    datatypes,
    schemas,
    AuditLogId,
    AuthType,
    JobId,
    LiveQueryId,
    ManifestId,
    NamespaceId,
    NodeId,
    OAuthProvider,
    Role,
    StorageId,
    TableId,
    UserId,
    UserName,
    Username,
};
pub use schemas::{TableAccess, TableName, TableType};
#[cfg(feature = "full")]
pub use serialization::KSerializable;
pub use storage_key::{decode_key, encode_key, encode_prefix, next_storage_key_bytes, StorageKey};
pub use string_interner::{intern, stats as interner_stats, SystemColumns, SYSTEM_COLUMNS};
pub use system_tables::{StoragePartition, SystemTable};
#[cfg(feature = "full")]
pub use websocket::{
    ChangeNotification, ChangeType as WsChangeType, Notification, WebSocketMessage,
};
#[cfg(feature = "websocket-auth")]
pub use websocket_auth::WsAuthCredentials;
