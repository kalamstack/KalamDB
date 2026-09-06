//! # kalamdb-commons
//!
//! Shared types, constants, and utilities for KalamDB.
//!
//! This crate provides foundational types and constants used across KalamDB crates
//! (kalamdb-core, kalamdb-dialect, kalamdb-system, kalamdb-store, kalamdb-api). It has zero
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
//! use kalamdb_commons::models::{NamespaceId, TableName, UserId};
//! use kalamdb_system::{Job, LiveQuery, User};
//!
//! let user_id = UserId::new("user_123");
//! let namespace_id = NamespaceId::default();
//! let table_name = TableName::new("conversations");
//!
//! // Convert to string
//! let id_str: &str = user_id.as_str();
//! ```

pub mod api_models;
pub mod constants;
#[cfg(any(
    feature = "conversions",
    feature = "schema-metadata",
    feature = "arrow-conversion"
))]
pub mod conversions; // Centralized datatype and value conversion utilities (see conversions/mod.rs)
pub mod errors;
pub mod helpers;
pub mod ids;
pub mod models;
#[cfg(feature = "serialization")]
pub mod serialization; // KSerializable trait for entity storage
pub mod storage; // Storage backend abstraction (Partition, StorageError, etc.)
#[cfg(feature = "storage")]
pub mod storage_key; // StorageKey trait for type-safe key serialization
pub mod system_tables; // System table enumeration (SystemTable, StoragePartition)
#[cfg(feature = "full")]
pub mod websocket;
#[cfg(feature = "websocket-auth")]
pub mod websocket_auth;
pub mod websocket_messages;
pub mod websocket_protocol;

// Allow procedural macros to refer to this crate by name.
extern crate self as kalamdb_commons;

// Re-export commonly used types at crate root
pub use api_models::{
    ClusterHealthResponse, ClusterNodeHealth, HealthCheckResponse, ResponseStatus,
    SqlSubscriptionDescriptor, SqlSubscriptionRow, SqlSubscriptionStatus,
};
pub use constants::{
    CRITICAL_RESERVED_SQL_KEYWORDS, MAX_SQL_QUERY_LENGTH, RESERVED_NAMESPACE_NAMES,
};
#[cfg(feature = "conversions")]
pub use conversions::{
    as_f64, estimate_scalar_value_size, json_value_to_scalar_for_column, pk_bucket_key_from_array,
    pk_bucket_key_from_row, pk_bucket_key_from_scalar, pk_bucket_key_from_typed_string,
    scalar_to_f64, scalar_to_i64, scalar_to_json_for_column, scalar_to_pk_string,
    scalar_value_to_bytes, try_pk_bucket_key, try_pk_bucket_key_from_array,
    try_pk_bucket_key_from_typed_string, PkBucketKey,
};
pub use errors::{CommonError, NotLeaderError, Result};
#[cfg(feature = "arrow-utils")]
pub use helpers::arrow_utils;
#[cfg(feature = "arrow-utils")]
pub use helpers::arrow_utils::{empty_batch, RecordBatchBuilder};
#[cfg(feature = "storage")]
pub use helpers::string_interner;
pub use helpers::{
    file_helpers, naming,
    naming::{
        normalize_sql_identifier, validate_namespace_reference, validate_sql_identifier,
        validate_user_namespace_name, SqlIdentifierError, MAX_SQL_IDENTIFIER_LENGTH,
    },
    security,
};
pub use models::{
    // Phase 15 (008-schema-consolidation): Re-export schema types
    datatypes,
    schemas,
    ArtifactId,
    AuditLogId,
    AuthType,
    AuthorizationRelation,
    BoundExprShape,
    CallArgument,
    CatalogTypeKind,
    ColumnId,
    FieldFlag,
    FieldFlags,
    FileRef,
    FunctionModuleId,
    FunctionRevisionId,
    FunctionRuntime,
    InvalidationStrategy,
    JobId,
    KalamDataType,
    LiveQueryId,
    ManifestId,
    NamespaceId,
    NodeId,
    OperationKind,
    PolicyCommand,
    PolicyId,
    PolicyProgram,
    PolicyScalar,
    PolicyTarget,
    PredicateOperator,
    PrincipalExpr,
    Role,
    RoutineCall,
    RoutineGrantId,
    RoutineGrantee,
    RoutineId,
    RoutineParameterId,
    RoutineSecurityMode,
    ScalarPredicate,
    SchemaField,
    StorageId,
    TableId,
    TablePolicy,
    TransactionId,
    TransactionOrigin,
    TransactionState,
    TriggerAttemptId,
    TriggerId,
    TypeFieldId,
    TypeId,
    UserId,
};
pub use schemas::{TableAccess, TableName, TableType};
#[cfg(feature = "serialization")]
pub use serialization::KSerializable;
#[cfg(feature = "storage")]
pub use storage_key::{decode_key, encode_key, encode_prefix, next_storage_key_bytes, StorageKey};
#[cfg(feature = "storage")]
pub use string_interner::{intern, stats as interner_stats, SystemColumns, SYSTEM_COLUMNS};
pub use system_tables::{StoragePartition, SystemTable};
#[cfg(feature = "full")]
pub use websocket::{
    ChangeNotification, ChangeType as WsChangeType, Notification, SharedChangePayload,
    WebSocketMessage, WireNotification,
};
#[cfg(feature = "websocket-auth")]
pub use websocket_auth::WsAuthCredentials;
#[cfg(feature = "websocket-auth")]
pub use websocket_messages::ClientMessage;
pub use websocket_messages::{
    BatchControl, BatchStatus, ChangeTypeRaw, ServerMessage, SubscriptionOptions,
    SubscriptionRequest,
};
pub use websocket_protocol::{
    jwt_from_websocket_subprotocol, jwt_websocket_subprotocol, CompressionType, ProtocolOptions,
    SerializationType, WS_JWT_SUBPROTOCOL_PREFIX,
};
