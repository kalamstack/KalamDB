//! Parsed SQL payload models

use bytes::Bytes;
use kalamdb_commons::models::NamespaceId;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use super::ErrorCode;

/// Result of parsing a multipart SQL request
#[derive(Debug)]
pub struct ParsedMultipartRequest {
    /// The SQL statement
    pub sql: String,
    /// Optional JSON params
    pub params_json: Option<String>,
    /// Optional namespace_id
    pub namespace_id: Option<NamespaceId>,
    /// Files: key -> (original_name, data, mime_type)
    pub files: HashMap<String, (String, Bytes, Option<String>)>,
}

/// Result of parsing a SQL payload (JSON or multipart)
#[derive(Debug)]
pub struct ParsedSqlPayload {
    /// SQL statement
    pub sql: String,
    /// Optional params JSON array
    pub params: Option<Vec<JsonValue>>,
    /// Optional namespace_id
    pub namespace_id: Option<NamespaceId>,
    /// Optional files map (present for multipart)
    pub files: Option<HashMap<String, (String, Bytes, Option<String>)>>,
    /// Whether payload was multipart
    pub is_multipart: bool,
}

/// Error type for file operations
#[derive(Debug)]
pub struct FileError {
    pub code: ErrorCode,
    pub message: String,
}

impl FileError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
