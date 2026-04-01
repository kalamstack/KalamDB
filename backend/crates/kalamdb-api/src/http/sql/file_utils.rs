//! File upload utilities for FILE datatype.
//!
//! Provides functions for:
//! - Parsing multipart form data with files
//! - Extracting FILE("name") placeholders from SQL
//! - Substituting placeholders with FileRef JSON
//! - Staging and finalizing files

use actix_multipart::Multipart;
use actix_web::{web, Either};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use kalamdb_commons::models::ids::StorageId;
use kalamdb_commons::models::{NamespaceId, TableId, UserId};
use kalamdb_commons::schemas::TableType;
use kalamdb_configs::FileUploadSettings;
use kalamdb_filestore::FileStorageService;
use kalamdb_system::{FileRef, FileSubfolderState};
use std::collections::HashMap;

use super::models::{ErrorCode, FileError, ParsedMultipartRequest, ParsedSqlPayload, QueryRequest};

/// Parse a multipart form request containing SQL and files
pub async fn parse_multipart_request(
    mut payload: Multipart,
    config: &FileUploadSettings,
) -> Result<ParsedMultipartRequest, FileError> {
    let max_file_size = config.max_size_bytes;
    let max_files = config.max_files_per_request;
    const MAX_FILE_PART_SIZE: usize = 100 * 1024 * 1024; // 100MB absolute max

    let mut sql: Option<String> = None;
    let mut params_json: Option<String> = None;
    let mut namespace_id: Option<NamespaceId> = None;
    let mut files: HashMap<String, (String, Bytes, Option<String>)> = HashMap::new();
    let mut file_count = 0;

    while let Some(field_result) = payload.next().await {
        let mut field = match field_result {
            Ok(f) => f,
            Err(e) => {
                return Err(FileError::new(
                    ErrorCode::InvalidInput,
                    format!("Multipart parse error: {}", e),
                ));
            },
        };

        let content_disposition = match field.content_disposition() {
            Some(cd) => cd,
            None => continue,
        };
        let field_name = content_disposition.get_name().unwrap_or("").to_string();

        if field_name == "sql" {
            let mut data = BytesMut::new();
            while let Some(chunk) = field.next().await {
                match chunk {
                    Ok(bytes) => data.extend_from_slice(&bytes),
                    Err(e) => {
                        return Err(FileError::new(
                            ErrorCode::InvalidInput,
                            format!("Failed to read SQL field: {}", e),
                        ));
                    },
                }
            }
            sql = Some(String::from_utf8_lossy(&data).to_string());
        } else if field_name == "params" {
            let mut data = BytesMut::new();
            while let Some(chunk) = field.next().await {
                if let Ok(bytes) = chunk {
                    data.extend_from_slice(&bytes);
                }
            }
            params_json = Some(String::from_utf8_lossy(&data).to_string());
        } else if field_name == "namespace_id" {
            let mut data = BytesMut::new();
            while let Some(chunk) = field.next().await {
                if let Ok(bytes) = chunk {
                    data.extend_from_slice(&bytes);
                }
            }
            namespace_id = Some(NamespaceId::new(String::from_utf8_lossy(&data).as_ref()));
        } else if field_name.starts_with("file:") {
            if file_count >= max_files {
                return Err(FileError::new(
                    ErrorCode::TooManyFiles,
                    format!("Too many files: maximum {} allowed", max_files),
                ));
            }

            let file_key = field_name.strip_prefix("file:").unwrap().to_string();
            let original_name = content_disposition
                .get_filename()
                .map(|s| s.to_string())
                .unwrap_or_else(|| file_key.clone());
            let mime_type = field.content_type().map(|m| m.to_string());

            let mut data = BytesMut::with_capacity(1024 * 1024);
            while let Some(chunk) = field.next().await {
                match chunk {
                    Ok(bytes) => {
                        if data.len() + bytes.len() > max_file_size {
                            return Err(FileError::new(
                                ErrorCode::FileTooLarge,
                                format!(
                                    "File '{}' exceeds maximum size of {} bytes",
                                    original_name, max_file_size
                                ),
                            ));
                        }
                        if data.len() + bytes.len() > MAX_FILE_PART_SIZE {
                            return Err(FileError::new(
                                ErrorCode::FileTooLarge,
                                "File exceeds absolute maximum size",
                            ));
                        }
                        data.extend_from_slice(&bytes);
                    },
                    Err(e) => {
                        return Err(FileError::new(
                            ErrorCode::InvalidInput,
                            format!("Failed to read file '{}': {}", file_key, e),
                        ));
                    },
                }
            }

            files.insert(file_key, (original_name, data.freeze(), mime_type));
            file_count += 1;
        }
    }

    let sql =
        sql.ok_or_else(|| FileError::new(ErrorCode::EmptySql, "SQL statement is required"))?;

    if sql.trim().is_empty() {
        return Err(FileError::new(ErrorCode::EmptySql, "SQL statement is required"));
    }

    Ok(ParsedMultipartRequest {
        sql,
        params_json,
        namespace_id,
        files,
    })
}

/// Parse JSON or multipart SQL payload
pub async fn parse_sql_payload(
    payload: Either<web::Json<QueryRequest>, Multipart>,
    config: &FileUploadSettings,
) -> Result<ParsedSqlPayload, FileError> {
    match payload {
        Either::Left(req) => Ok(ParsedSqlPayload {
            sql: req.sql.clone(),
            params: req.params.clone(),
            namespace_id: req.namespace_id.clone(),
            files: None,
            is_multipart: false,
        }),
        Either::Right(multipart) => {
            let parsed = parse_multipart_request(multipart, config).await?;
            let params = match parsed.params_json {
                Some(json_str) => {
                    let json_val: serde_json::Value =
                        serde_json::from_str(&json_str).map_err(|e| {
                            FileError::new(
                                ErrorCode::InvalidParameter,
                                format!("Invalid params JSON: {}", e),
                            )
                        })?;
                    let arr = json_val.as_array().ok_or_else(|| {
                        FileError::new(ErrorCode::InvalidParameter, "Params must be a JSON array")
                    })?;
                    Some(arr.to_vec())
                },
                None => None,
            };

            Ok(ParsedSqlPayload {
                sql: parsed.sql,
                params,
                namespace_id: parsed.namespace_id,
                files: Some(parsed.files),
                is_multipart: true,
            })
        },
    }
}

/// Extract FILE("name") placeholders from SQL string.
///
/// Matches patterns like:
/// - FILE("avatar")
/// - FILE('invoice_pdf')
/// - FILE("my-file")
pub fn extract_file_placeholders(sql: &str) -> Vec<String> {
    let mut placeholders = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == 'F' || c == 'f' {
            let mut matched = String::from(c);
            for expected in ['I', 'L', 'E', '('] {
                if let Some(&next) = chars.peek() {
                    if next.to_ascii_uppercase() == expected {
                        matched.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
            }

            if matched.to_uppercase() == "FILE(" {
                // Skip whitespace
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        chars.next();
                    } else {
                        break;
                    }
                }

                // Read quote character
                if let Some(&quote) = chars.peek() {
                    if quote == '"' || quote == '\'' {
                        chars.next();
                        let mut name = String::new();
                        while let Some(c) = chars.next() {
                            if c == quote {
                                break;
                            }
                            name.push(c);
                        }
                        if !name.is_empty() {
                            placeholders.push(name);
                        }
                    }
                }
            }
        }
    }

    placeholders
}

/// Replace FILE("name") placeholders in SQL with FileRef JSON strings
pub fn substitute_file_placeholders(sql: &str, file_refs: &HashMap<String, FileRef>) -> String {
    let mut result = sql.to_string();

    for (name, file_ref) in file_refs {
        // Build patterns to match: FILE("name"), FILE('name'), file("name"), etc.
        let patterns = [
            format!(r#"FILE("{}")"#, name),
            format!(r#"FILE('{}')"#, name),
            format!(r#"file("{}")"#, name),
            format!(r#"file('{}')"#, name),
            format!(r#"File("{}")"#, name),
            format!(r#"File('{}')"#, name),
        ];

        // Serialize FileRef to JSON and escape for SQL string
        let json = file_ref.to_json();
        let escaped = json.replace('\'', "''"); // Escape single quotes for SQL
        let replacement = format!("'{}'", escaped);

        for pattern in patterns {
            result = result.replace(&pattern, &replacement);
        }
    }

    result
}

/// Stage files and create FileRef values
pub async fn stage_and_finalize_files(
    file_service: &FileStorageService,
    files: &HashMap<String, (String, Bytes, Option<String>)>,
    storage_id: &StorageId,
    table_type: TableType,
    table_id: &TableId,
    user_id: Option<&UserId>,
    subfolder_state: &mut FileSubfolderState,
    shard_id: Option<u32>,
) -> Result<HashMap<String, FileRef>, FileError> {
    let mut file_refs = HashMap::new();

    // Create a temporary staging directory
    let request_id = uuid::Uuid::new_v4().to_string();
    let system_id = kalamdb_commons::UserId::system();
    let uid = user_id.unwrap_or(&system_id);

    let staging_dir = file_service.create_staging_dir(&request_id, uid).map_err(|e| {
        FileError::new(ErrorCode::InternalError, format!("Failed to create staging dir: {}", e))
    })?;

    for (placeholder_name, (original_name, data, mime_type)) in files {
        // Stage the file
        let staged = file_service
            .stage_file(
                &staging_dir,
                placeholder_name,
                original_name,
                data.clone(),
                mime_type.as_deref(),
            )
            .map_err(|e| {
                FileError::new(ErrorCode::InternalError, format!("Failed to stage file: {}", e))
            })?;

        // Finalize to permanent storage
        let file_ref = file_service
            .finalize_file(
                &staged,
                storage_id,
                table_type,
                table_id,
                user_id,
                subfolder_state,
                shard_id,
            )
            .await
            .map_err(|e| {
                FileError::new(ErrorCode::InternalError, format!("Failed to finalize file: {}", e))
            })?;

        file_refs.insert(placeholder_name.clone(), file_ref);
    }

    // Cleanup staging directory
    let _ = std::fs::remove_dir_all(&staging_dir);

    Ok(file_refs)
}

/// Parse table name from INSERT/UPDATE SQL to determine target table
/// Returns TableId or None if cannot parse
#[cfg(test)]
pub fn extract_table_from_sql(sql: &str, default_namespace: &str) -> Option<TableId> {
    kalamdb_sql::extract_dml_table_id(sql, default_namespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_file_placeholders() {
        let sql = r#"INSERT INTO messages (avatar, doc) VALUES (FILE("avatar"), FILE('document'))"#;
        let placeholders = extract_file_placeholders(sql);
        assert_eq!(placeholders, vec!["avatar", "document"]);
    }

    #[test]
    fn test_extract_file_placeholders_case_insensitive() {
        let sql = r#"INSERT INTO t (f) VALUES (file("myfile"))"#;
        let placeholders = extract_file_placeholders(sql);
        assert_eq!(placeholders, vec!["myfile"]);
    }

    #[test]
    fn test_extract_file_placeholders_with_dashes() {
        let sql = r#"INSERT INTO t (f) VALUES (FILE("my-file-name"))"#;
        let placeholders = extract_file_placeholders(sql);
        assert_eq!(placeholders, vec!["my-file-name"]);
    }

    #[test]
    fn test_extract_file_placeholders_no_files() {
        let sql = "INSERT INTO messages (content) VALUES ('hello')";
        let placeholders = extract_file_placeholders(sql);
        assert!(placeholders.is_empty());
    }

    #[test]
    fn test_substitute_file_placeholders() {
        let sql = r#"INSERT INTO t (f) VALUES (FILE("avatar"))"#;
        let file_ref = FileRef::new(
            "123".to_string(),
            "f0001".to_string(),
            "avatar.png".to_string(),
            1024,
            "image/png".to_string(),
            "abc123".to_string(),
        );
        let mut refs = HashMap::new();
        refs.insert("avatar".to_string(), file_ref);

        let result = substitute_file_placeholders(sql, &refs);
        assert!(result.contains("'{")); // Contains JSON string
        assert!(!result.contains("FILE"));
    }

    #[test]
    fn test_extract_table_from_insert() {
        let sql = "INSERT INTO myapp.users (name) VALUES ('Alice')";
        let table_id = extract_table_from_sql(sql, "default");
        assert!(table_id.is_some());
        let tid = table_id.unwrap();
        assert_eq!(tid.namespace_id().as_str(), "myapp");
        assert_eq!(tid.table_name().as_str(), "users");
    }

    #[test]
    fn test_extract_table_from_insert_no_namespace() {
        let sql = "INSERT INTO users (name) VALUES ('Alice')";
        let table_id = extract_table_from_sql(sql, "default");
        assert!(table_id.is_some());
        let tid = table_id.unwrap();
        assert_eq!(tid.namespace_id().as_str(), "default");
        assert_eq!(tid.table_name().as_str(), "users");
    }
}
