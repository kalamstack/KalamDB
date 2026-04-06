//! SQL execution helper functions
//!
//! This module provides helper utilities for SQL execution:
//! - Parameter parsing and validation
//! - File cleanup after errors
//! - Result conversion from Arrow to JSON
//! - Sensitive data masking

mod converter;
mod executor;
mod files;
mod params;
mod streaming;

pub use executor::{
    execute_single_statement, execute_single_statement_raw, execution_result_to_query_result,
};
pub use files::cleanup_files;
pub use params::{parse_forward_params, parse_scalar_params};
pub use streaming::stream_sql_rows_response;
