//! SQL parser module organization.
//!
//! This module provides a clean separation between standard SQL parsing
//! (using sqlparser-rs) and KalamDB-specific extensions.
//!
//! ## Architecture
//!
//! - **standard.rs**: Wraps sqlparser-rs for ANSI SQL, PostgreSQL, and MySQL syntax
//! - **extensions.rs**: Custom parsers for KalamDB-specific commands (CREATE STORAGE, STORAGE FLUSH, STORAGE COMPACT, etc.)
//! - **system.rs**: Parsers for system table queries
//! - **utils.rs**: Common parsing utilities (keyword extraction, normalization, etc.)
//! - **query_parser.rs**: Safe SQL query parsing for live queries and subscriptions
//!
//! ## Design Principles
//!
//! 1. **Use sqlparser-rs for standard SQL**: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, etc.
//! 2. **Custom parsers only for KalamDB extensions**: Commands that don't fit SQL standards
//! 3. **PostgreSQL/MySQL compatibility**: Accept common syntax variants
//! 4. **Type-safe AST**: Strongly typed statement representations

pub mod extensions;
pub mod query_parser;
pub mod system;
pub mod utils;

pub use extensions::*;
pub use query_parser::*;
pub use system::*;
pub use utils::{
    extract_dml_table_id, extract_dml_table_id_from_statement,
    normalize_context_keyword_calls_for_sqlparser, parse_single_statement,
    rewrite_context_functions_for_datafusion,
};
