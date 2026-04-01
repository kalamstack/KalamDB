//! SQL request and response models
//!
//! This module contains type-safe models for SQL API endpoints.

mod parsed_payload;
mod sql_request;
mod sql_response;

pub use parsed_payload::{FileError, ParsedMultipartRequest, ParsedSqlPayload};
pub use sql_request::QueryRequest;
pub use sql_response::{ErrorCode, ErrorDetail, QueryResult, ResponseStatus, SqlResponse};
