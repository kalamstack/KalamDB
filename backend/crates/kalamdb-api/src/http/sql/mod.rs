//! SQL execution handlers for the `/v1/api/sql` REST API endpoint
//!
//! This module provides HTTP handlers for executing SQL statements via the REST API.
//! Authentication is handled automatically by the `AuthSession` extractor.
//!
//! ## Endpoints
//! - POST /v1/api/sql - Execute SQL statements (requires Authorization header)
//!
//! **Security Note**: This endpoint only accepts Bearer token authentication.
//! Basic auth (username/password) is not supported for SQL execution to encourage
//! secure token-based authentication patterns.

pub mod models;

mod execute;
mod execution_paths;
mod file_utils;
mod forward;
mod helpers;
mod request;
mod statements;

pub(crate) use execute::execute_sql_v1;
