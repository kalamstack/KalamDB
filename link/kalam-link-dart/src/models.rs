//! FRB-friendly model types that mirror kalam-client models.
//!
//! All types here are simple structs/enums with only primitive fields
//! and `Vec`/`Option` wrappers — fully compatible with flutter_rust_bridge codegen.

use kalam_client::{
    models::{
        BatchStatus, ChangeEvent, ErrorDetail, LoginResponse, LoginUserInfo, QueryResponse,
        QueryResult, ResponseStatus, SchemaField,
    },
    LiveRowsConfig, LiveRowsEvent, Role,
};

// ---------------------------------------------------------------------------
// Connection lifecycle events (mirrors kalam_client::event_handlers)
// ---------------------------------------------------------------------------

/// Reason why a WebSocket connection was closed.
///
/// Mirrors `kalam_client::DisconnectReason`.
pub struct DartDisconnectReason {
    /// Human-readable description of why the connection closed.
    pub message: String,
    /// WebSocket close code, if available (e.g. 1000 = normal, 1006 = abnormal).
    pub code: Option<i32>,
}

/// Error information from a connection or protocol error.
///
/// Mirrors `kalam_client::ConnectionError`.
pub struct DartConnectionError {
    /// Human-readable error message.
    pub message: String,
    /// Whether this error is recoverable (auto-reconnect may succeed).
    pub recoverable: bool,
}

/// A connection lifecycle event pulled via
/// [`dart_next_connection_event`](crate::api::dart_next_connection_event).
///
/// Follows the same async-pull model used for subscription events.
/// On the Dart side, poll in a loop (or wrap in a `Stream`):
///
/// ```dart
/// while (true) {
///   final event = await dartNextConnectionEvent(client: client);
///   if (event == null) break; // client destroyed / events disabled
///   switch (event) {
///     case DartConnectionEvent_Connect(): print('connected');
///     case DartConnectionEvent_Disconnect(:final reason): ...
///     case DartConnectionEvent_Error(:final error): ...
///     case DartConnectionEvent_Receive(:final message): ...
///     case DartConnectionEvent_Send(:final message): ...
///   }
/// }
/// ```
pub enum DartConnectionEvent {
    /// WebSocket connection established and authenticated.
    Connect,
    /// WebSocket connection closed.
    Disconnect { reason: DartDisconnectReason },
    /// Connection or protocol error.
    Error { error: DartConnectionError },
    /// Raw message received from the server (debug).
    Receive { message: String },
    /// Raw message sent to the server (debug).
    Send { message: String },
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Authentication method for connecting to KalamDB.
pub enum DartAuthProvider {
    /// HTTP Basic Auth with user and password.
    BasicAuth { user: String, password: String },
    /// JWT bearer token.
    JwtToken { token: String },
    /// No authentication (localhost bypass).
    None,
}

impl DartAuthProvider {
    pub(crate) fn into_native(self) -> kalam_client::AuthProvider {
        match self {
            Self::BasicAuth { user, password } => {
                kalam_client::AuthProvider::basic_auth(user, password)
            },
            Self::JwtToken { token } => kalam_client::AuthProvider::jwt_token(token),
            Self::None => kalam_client::AuthProvider::none(),
        }
    }
}

// ---------------------------------------------------------------------------
// Query response models
// ---------------------------------------------------------------------------

pub struct DartQueryResponse {
    pub success: bool,
    pub results: Vec<DartQueryResult>,
    /// Execution time in milliseconds.
    pub took_ms: Option<f64>,
    pub error: Option<DartErrorDetail>,
}

impl From<QueryResponse> for DartQueryResponse {
    fn from(r: QueryResponse) -> Self {
        Self {
            success: r.status == ResponseStatus::Success,
            results: r.results.into_iter().map(DartQueryResult::from).collect(),
            took_ms: r.took,
            error: r.error.map(DartErrorDetail::from),
        }
    }
}

pub struct DartQueryResult {
    pub columns: Vec<DartSchemaField>,
    /// Each row is a JSON-encoded string (array of values).
    /// Dart side parses this into typed values.
    pub rows_json: Vec<String>,
    /// Each row as a JSON-encoded object (`{"col": value, ...}`).
    /// Pre-computed from `schema` + `rows` so the Dart SDK doesn't need to
    /// perform the schema → map transformation itself.
    pub named_rows_json: Vec<String>,
    pub row_count: i64,
    pub message: Option<String>,
}

impl From<QueryResult> for DartQueryResult {
    fn from(r: QueryResult) -> Self {
        // Build named_rows: schema + rows → Vec<HashMap<String, KalamCellValue>>
        let named_rows = r.rows_as_maps();
        let named_rows_json = named_rows
            .iter()
            .map(|row| serde_json::to_string(row).unwrap_or_default())
            .collect();

        let rows_json = r
            .rows
            .unwrap_or_default()
            .into_iter()
            .map(|row| serde_json::to_string(&row).unwrap_or_default())
            .collect();
        Self {
            columns: r.schema.into_iter().map(DartSchemaField::from).collect(),
            rows_json,
            named_rows_json,
            row_count: r.row_count as i64,
            message: r.message,
        }
    }
}

pub struct DartSchemaField {
    pub name: String,
    pub data_type: String,
    pub index: i32,
    /// Comma-separated flag short names, e.g. `"pk,nn,uq"`.
    /// `None` when no flags are present.
    pub flags: Option<String>,
}

impl From<SchemaField> for DartSchemaField {
    fn from(f: SchemaField) -> Self {
        Self {
            name: f.name,
            data_type: format!("{:?}", f.data_type),
            index: f.index as i32,
            flags: f.flags.map(|fl| {
                fl.iter()
                    .map(|flag| match flag {
                        kalam_client::FieldFlag::PrimaryKey => "pk",
                        kalam_client::FieldFlag::NonNull => "nn",
                        kalam_client::FieldFlag::Unique => "uq",
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            }),
        }
    }
}

pub struct DartErrorDetail {
    pub code: String,
    pub message: String,
    pub details: Option<String>,
}

impl From<ErrorDetail> for DartErrorDetail {
    fn from(e: ErrorDetail) -> Self {
        Self {
            code: e.code,
            message: e.message,
            details: e.details,
        }
    }
}

// ---------------------------------------------------------------------------
// Login / Auth responses
// ---------------------------------------------------------------------------

pub struct DartLoginResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: String,
    pub refresh_expires_at: Option<String>,
    pub admin_ui_access: bool,
    pub user: DartLoginUserInfo,
}

impl From<LoginResponse> for DartLoginResponse {
    fn from(l: LoginResponse) -> Self {
        Self {
            access_token: l.access_token,
            refresh_token: l.refresh_token,
            expires_at: l.expires_at,
            refresh_expires_at: l.refresh_expires_at,
            admin_ui_access: l.admin_ui_access,
            user: DartLoginUserInfo::from(l.user),
        }
    }
}

pub enum DartRole {
    Anonymous,
    User,
    Service,
    Dba,
    System,
}

impl From<Role> for DartRole {
    fn from(role: Role) -> Self {
        match role {
            Role::Anonymous => Self::Anonymous,
            Role::User => Self::User,
            Role::Service => Self::Service,
            Role::Dba => Self::Dba,
            Role::System => Self::System,
        }
    }
}

pub struct DartLoginUserInfo {
    pub id: String,
    pub role: DartRole,
    pub email: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<LoginUserInfo> for DartLoginUserInfo {
    fn from(u: LoginUserInfo) -> Self {
        Self {
            id: u.id.to_string(),
            role: DartRole::from(u.role),
            email: u.email,
            created_at: u.created_at,
            updated_at: u.updated_at,
        }
    }
}

// ---------------------------------------------------------------------------
// Subscription / Change events
// ---------------------------------------------------------------------------

/// A single change event from a live subscription.
pub enum DartChangeEvent {
    /// Subscription acknowledged — contains schema info.
    Ack {
        subscription_id: String,
        total_rows: i32,
        schema: Vec<DartSchemaField>,
        batch_num: i32,
        has_more: bool,
        status: String,
    },
    /// Batch of initial data rows.
    InitialDataBatch {
        subscription_id: String,
        /// Each entry is a JSON-encoded row object (`{"col": value, ...}`).
        rows_json: Vec<String>,
        batch_num: i32,
        has_more: bool,
        status: String,
    },
    /// One or more rows were inserted.
    Insert {
        subscription_id: String,
        /// Each entry is a JSON-encoded row object.
        rows_json: Vec<String>,
    },
    /// One or more rows were updated.
    Update {
        subscription_id: String,
        /// Delta rows — only changed columns + PK + `_seq`.
        /// Changed user columns are the non-system keys: filter by `!key.starts_with('_')`.
        rows_json: Vec<String>,
        old_rows_json: Vec<String>,
    },
    /// One or more rows were deleted.
    Delete {
        subscription_id: String,
        old_rows_json: Vec<String>,
    },
    /// Server-side error on this subscription.
    Error {
        subscription_id: String,
        code: String,
        message: String,
    },
}

/// Configuration for Rust-side live row materialization.
pub struct DartLiveRowsConfig {
    pub limit: Option<i32>,
    pub key_columns: Option<Vec<String>>,
}

impl DartLiveRowsConfig {
    pub(crate) fn into_native(self) -> LiveRowsConfig {
        LiveRowsConfig {
            limit: self.limit.map(|value| value.max(0) as usize),
            key_columns: self.key_columns.map(|columns| {
                columns
                    .into_iter()
                    .map(|column| column.trim().to_string())
                    .filter(|column| !column.is_empty())
                    .collect()
            }),
        }
    }
}

/// High-level event emitted by a Rust live-row subscription.
pub enum DartLiveRowsEvent {
    Rows {
        subscription_id: String,
        rows_json: Vec<String>,
    },
    Error {
        subscription_id: String,
        code: String,
        message: String,
    },
}

fn batch_status_str(bs: &BatchStatus) -> String {
    match bs {
        BatchStatus::Loading => "loading".to_owned(),
        BatchStatus::LoadingBatch => "loading_batch".to_owned(),
        BatchStatus::Ready => "ready".to_owned(),
    }
}

fn json_vec(
    rows: Vec<std::collections::HashMap<String, kalam_client::KalamCellValue>>,
) -> Vec<String> {
    rows.into_iter()
        .map(|row| serde_json::to_string(&row).unwrap_or_default())
        .collect()
}

impl From<ChangeEvent> for DartChangeEvent {
    fn from(e: ChangeEvent) -> Self {
        match e {
            ChangeEvent::Ack {
                subscription_id,
                total_rows,
                batch_control,
                schema,
            } => Self::Ack {
                subscription_id,
                total_rows: total_rows as i32,
                schema: schema.into_iter().map(DartSchemaField::from).collect(),
                batch_num: batch_control.batch_num as i32,
                has_more: batch_control.has_more,
                status: batch_status_str(&batch_control.status),
            },
            ChangeEvent::InitialDataBatch {
                subscription_id,
                rows,
                batch_control,
            } => Self::InitialDataBatch {
                subscription_id,
                rows_json: rows
                    .into_iter()
                    .map(|row| serde_json::to_string(&row).unwrap_or_default())
                    .collect(),
                batch_num: batch_control.batch_num as i32,
                has_more: batch_control.has_more,
                status: batch_status_str(&batch_control.status),
            },
            ChangeEvent::Insert {
                subscription_id,
                rows,
            } => Self::Insert {
                subscription_id,
                rows_json: json_vec(rows),
            },
            ChangeEvent::Update {
                subscription_id,
                rows,
                old_rows,
            } => Self::Update {
                subscription_id,
                rows_json: json_vec(rows),
                old_rows_json: json_vec(old_rows),
            },
            ChangeEvent::Delete {
                subscription_id,
                old_rows,
            } => Self::Delete {
                subscription_id,
                old_rows_json: json_vec(old_rows),
            },
            ChangeEvent::Error {
                subscription_id,
                code,
                message,
            } => Self::Error {
                subscription_id,
                code,
                message,
            },
            ChangeEvent::Unknown { raw } => Self::Error {
                subscription_id: String::new(),
                code: "unknown".to_owned(),
                message: serde_json::to_string(&raw).unwrap_or_default(),
            },
        }
    }
}

impl From<LiveRowsEvent> for DartLiveRowsEvent {
    fn from(event: LiveRowsEvent) -> Self {
        match event {
            LiveRowsEvent::Rows {
                subscription_id,
                rows,
            } => Self::Rows {
                subscription_id,
                rows_json: json_vec(rows),
            },
            LiveRowsEvent::Error {
                subscription_id,
                code,
                message,
            } => Self::Error {
                subscription_id,
                code,
                message,
            },
        }
    }
}

/// Subscription configuration.
pub struct DartSubscriptionConfig {
    pub sql: String,
    /// Optional subscription ID (auto-generated if omitted).
    pub id: Option<String>,
    pub batch_size: Option<i32>,
    pub last_rows: Option<i32>,
    /// Resume from a specific sequence ID.
    /// When set, the server only sends changes after this seq_id.
    pub from: Option<i64>,
}

impl DartSubscriptionConfig {
    pub(crate) fn into_native(self) -> kalam_client::SubscriptionConfig {
        kalam_client::SubscriptionConfig {
            id: self.id.unwrap_or_else(|| uuid_v4()),
            sql: self.sql,
            options: Some(kalam_client::SubscriptionOptions {
                batch_size: self.batch_size.map(|v| v as usize),
                last_rows: self.last_rows.map(|v| v as u32),
                from: self.from.map(kalam_client::SeqId::new),
            }),
            ws_url: None,
        }
    }
}

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("dart-sub-{}", ts)
}

/// Read-only snapshot of an active subscription's metadata.
///
/// Returned by [`dart_list_subscriptions`].
pub struct DartSubscriptionInfo {
    /// Subscription ID assigned when subscribing.
    pub id: String,
    /// The SQL query this subscription is tracking.
    pub query: String,
    /// Last received sequence ID (for resume on reconnect), as i64.
    pub last_seq_id: Option<i64>,
    /// Timestamp (millis since epoch) of the last received event.
    pub last_event_time_ms: Option<i64>,
    /// Timestamp (millis since epoch) when the subscription was created.
    pub created_at_ms: i64,
    /// Whether the subscription has been closed.
    pub closed: bool,
}

impl From<kalam_client::models::SubscriptionInfo> for DartSubscriptionInfo {
    fn from(info: kalam_client::models::SubscriptionInfo) -> Self {
        Self {
            id: info.id,
            query: info.query,
            last_seq_id: info.last_seq_id.map(|s| s.as_i64()),
            last_event_time_ms: info.last_event_time_ms.map(|v| v as i64),
            created_at_ms: info.created_at_ms as i64,
            closed: info.closed,
        }
    }
}
