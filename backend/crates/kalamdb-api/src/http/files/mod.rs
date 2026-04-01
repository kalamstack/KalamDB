//! File download handlers for FILE datatype
//!
//! ## Endpoints
//! - GET /v1/files/{namespace}/{table_name}/{subfolder}/{file_id} - Download a file
//! - GET /v1/exports/{user_id}/{export_id} - Download a user data export ZIP
//!
//! ## Download Flow
//! 1. Parse path parameters (namespace, table_name, subfolder, file_id)
//! 2. Check user permissions for the table
//! 3. Build FileRef path from parameters
//! 4. Stream file from storage with proper Content-Type

pub mod models;

mod download;
mod export_download;

pub(crate) use download::download_file;
pub(crate) use export_download::download_export;
