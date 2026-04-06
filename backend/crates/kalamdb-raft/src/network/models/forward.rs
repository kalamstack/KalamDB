//! Forward-SQL request/response message types.
//!
//! Used to forward SQL write requests from follower nodes to the current
//! meta-group leader via the `ClusterService/ForwardSql` gRPC method.

/// Typed SQL parameter value forwarded between cluster nodes.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ForwardSqlParam {
    #[prost(oneof = "forward_sql_param::Value", tags = "1, 2, 3, 4, 5")]
    pub value: Option<forward_sql_param::Value>,
}

/// Oneof payload for [`ForwardSqlParam`].
pub mod forward_sql_param {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(bool, tag = "1")]
        NullValue(bool),
        #[prost(bool, tag = "2")]
        BoolValue(bool),
        #[prost(int64, tag = "3")]
        Int64Value(i64),
        #[prost(double, tag = "4")]
        Float64Value(f64),
        #[prost(string, tag = "5")]
        StringValue(String),
    }
}

impl ForwardSqlParam {
    pub fn null() -> Self {
        Self {
            value: Some(forward_sql_param::Value::NullValue(true)),
        }
    }

    pub fn boolean(value: bool) -> Self {
        Self {
            value: Some(forward_sql_param::Value::BoolValue(value)),
        }
    }

    pub fn int64(value: i64) -> Self {
        Self {
            value: Some(forward_sql_param::Value::Int64Value(value)),
        }
    }

    pub fn float64(value: f64) -> Self {
        Self {
            value: Some(forward_sql_param::Value::Float64Value(value)),
        }
    }

    pub fn text(value: impl Into<String>) -> Self {
        Self {
            value: Some(forward_sql_param::Value::StringValue(value.into())),
        }
    }
}

/// Request to forward a SQL write request from a follower to the leader.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ForwardSqlRequest {
    /// SQL text
    #[prost(string, tag = "1")]
    pub sql: String,

    /// Optional namespace for unqualified table names
    #[prost(string, optional, tag = "2")]
    pub namespace_id: Option<String>,

    /// Optional `Authorization: Bearer <token>` header value – required for
    /// authenticating the forwarded request on the leader.
    #[prost(string, optional, tag = "4")]
    pub authorization_header: Option<String>,

    /// Optional `X-Request-ID` header value for end-to-end request tracing.
    #[prost(string, optional, tag = "5")]
    pub request_id: Option<String>,

    /// Typed forwarded SQL parameters.
    #[prost(message, repeated, tag = "6")]
    pub params: Vec<ForwardSqlParam>,
}

/// Response for a forwarded SQL execution.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ForwardSqlResponse {
    /// HTTP status code produced by the leader's SQL handler.
    #[prost(uint32, tag = "1")]
    pub status_code: u32,

    /// JSON response body bytes (same format as a direct `/v1/api/sql` response).
    #[prost(bytes = "vec", tag = "2")]
    pub body: Vec<u8>,

    /// Transport-level error message when forwarding fails before execution.
    /// Empty on success.
    #[prost(string, tag = "3")]
    pub error: String,
}

/// Application-level payload returned by [`ClusterMessageHandler::handle_forward_sql`].
///
/// Separates the handler result from the wire format so the gRPC layer can
/// wrap it into a [`ForwardSqlResponse`] consistently.
#[derive(Debug, Clone)]
pub struct ForwardSqlResponsePayload {
    /// HTTP status code to surface back to the original client.
    pub status_code: u16,
    /// JSON body bytes.
    pub body: Vec<u8>,
}
