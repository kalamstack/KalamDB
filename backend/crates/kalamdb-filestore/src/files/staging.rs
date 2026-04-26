//! Staging manager for temporary file uploads.
//!
//! Files are staged in a temp directory before being finalized to their
//! permanent location. This enables atomic operations and cleanup on failure.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::error::{FilestoreError, Result};

/// A staged file with computed metadata.
#[derive(Debug, Clone)]
pub struct StagedFile {
    /// Path to the staged file
    pub path: PathBuf,

    /// Original filename
    pub original_name: String,

    /// File size in bytes
    pub size: u64,

    /// MIME type (detected or provided)
    pub mime_type: String,

    /// SHA-256 hash of file content (hex-encoded)
    pub sha256: String,
}

impl StagedFile {
    /// Delete the staged file
    pub fn cleanup(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path).map_err(|e| {
                FilestoreError::Other(format!("Failed to cleanup staged file: {}", e))
            })?;
        }
        Ok(())
    }
}

/// Manages staging directory for file uploads.
pub struct StagingManager {
    /// Base staging directory
    staging_dir: PathBuf,
}

impl StagingManager {
    /// Create a new staging manager with the given base directory.
    pub fn new(staging_dir: impl AsRef<Path>) -> Self {
        Self {
            staging_dir: staging_dir.as_ref().to_path_buf(),
        }
    }

    /// Create a request-specific staging directory.
    ///
    /// Format: `{staging_dir}/{request_id}-{user_id}/`
    pub fn create_request_dir(
        &self,
        request_id: &str,
        user_id: &kalamdb_commons::UserId,
    ) -> Result<PathBuf> {
        let dir_name = format!(
            "{}-{}",
            sanitize_path_component(request_id),
            sanitize_path_component(user_id.as_str())
        );
        let request_dir = self.staging_dir.join(dir_name);

        fs::create_dir_all(&request_dir).map_err(|e| {
            FilestoreError::Other(format!("Failed to create staging directory: {}", e))
        })?;

        Ok(request_dir)
    }

    /// Stage a file with streaming upload.
    ///
    /// Computes size, SHA-256, and MIME type during upload.
    pub fn stage_file(
        &self,
        request_dir: &Path,
        file_name: &str,
        original_name: &str,
        data: Bytes,
        provided_mime: Option<&str>,
    ) -> Result<StagedFile> {
        let staged_path = request_dir.join(sanitize_path_component(file_name));

        // Write file and compute hash
        let mut file = fs::File::create(&staged_path)
            .map_err(|e| FilestoreError::Other(format!("Failed to create staged file: {}", e)))?;

        let mut hasher = Sha256::new();
        hasher.update(&data);

        file.write_all(&data)
            .map_err(|e| FilestoreError::Other(format!("Failed to write staged file: {}", e)))?;
        file.sync_all()
            .map_err(|e| FilestoreError::Other(format!("Failed to sync staged file: {}", e)))?;

        let size = data.len() as u64;
        let sha256 = hex::encode(hasher.finalize());

        // Detect MIME type
        let mime_type = provided_mime
            .map(|s| s.to_string())
            .unwrap_or_else(|| detect_mime_type(original_name, &data));

        Ok(StagedFile {
            path: staged_path,
            original_name: original_name.to_string(),
            size,
            mime_type,
            sha256,
        })
    }

    /// Cleanup a request's staging directory.
    pub fn cleanup_request_dir(&self, request_dir: &Path) -> Result<()> {
        if request_dir.exists() && request_dir.starts_with(&self.staging_dir) {
            fs::remove_dir_all(request_dir).map_err(|e| {
                FilestoreError::Other(format!("Failed to cleanup staging directory: {}", e))
            })?;
        }
        Ok(())
    }

    /// Cleanup all stale staging directories older than max_age_secs.
    pub fn cleanup_stale_directories(&self, max_age_secs: u64) -> Result<usize> {
        let mut cleaned = 0;

        if !self.staging_dir.exists() {
            return Ok(0);
        }

        let now = std::time::SystemTime::now();

        for entry in fs::read_dir(&self.staging_dir).map_err(|e| {
            FilestoreError::Other(format!("Failed to read staging directory: {}", e))
        })? {
            let entry = entry.map_err(|e| {
                FilestoreError::Other(format!("Failed to read directory entry: {}", e))
            })?;

            if let Ok(metadata) = entry.metadata() {
                if let Ok(created) = metadata.created().or_else(|_| metadata.modified()) {
                    if let Ok(age) = now.duration_since(created) {
                        if age.as_secs() > max_age_secs {
                            if let Err(e) = fs::remove_dir_all(entry.path()) {
                                log::warn!(
                                    "Failed to cleanup stale staging dir {:?}: {}",
                                    entry.path(),
                                    e
                                );
                            } else {
                                cleaned += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(cleaned)
    }
}

/// Sanitize a path component to prevent directory traversal attacks.
fn sanitize_path_component(s: &str) -> String {
    let mut output = String::with_capacity(s.len().min(255));
    let mut dot_run = 0;

    for c in s.chars() {
        if !(c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.') {
            continue;
        }

        if c == '.' {
            if dot_run >= 3 {
                continue;
            }
            dot_run += 1;
        } else {
            dot_run = 0;
        }

        output.push(c);

        if output.len() >= 255 {
            break;
        }
    }

    output
}

/// Detect MIME type from filename extension and magic bytes.
fn detect_mime_type(filename: &str, data: &[u8]) -> String {
    // Try magic bytes first
    if data.len() >= 8 {
        // PNG
        if data.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) {
            return "image/png".to_string();
        }
        // JPEG
        if data.starts_with(&[0xFF, 0xD8, 0xFF]) {
            return "image/jpeg".to_string();
        }
        // GIF
        if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
            return "image/gif".to_string();
        }
        // PDF
        if data.starts_with(b"%PDF") {
            return "application/pdf".to_string();
        }
        // ZIP (also docx, xlsx, etc.)
        if data.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
            return "application/zip".to_string();
        }
        // WebP
        if data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP" {
            return "image/webp".to_string();
        }
    }

    // Fall back to extension
    if let Some(ext) = filename.rsplit('.').next() {
        match ext.to_lowercase().as_str() {
            "txt" => return "text/plain".to_string(),
            "html" | "htm" => return "text/html".to_string(),
            "css" => return "text/css".to_string(),
            "js" => return "application/javascript".to_string(),
            "json" => return "application/json".to_string(),
            "xml" => return "application/xml".to_string(),
            "csv" => return "text/csv".to_string(),
            "md" => return "text/markdown".to_string(),
            "svg" => return "image/svg+xml".to_string(),
            "mp3" => return "audio/mpeg".to_string(),
            "mp4" => return "video/mp4".to_string(),
            "webm" => return "video/webm".to_string(),
            "ogg" => return "audio/ogg".to_string(),
            "wav" => return "audio/wav".to_string(),
            "doc" => return "application/msword".to_string(),
            "docx" => {
                return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    .to_string()
            },
            "xls" => return "application/vnd.ms-excel".to_string(),
            "xlsx" => {
                return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    .to_string()
            },
            "ppt" => return "application/vnd.ms-powerpoint".to_string(),
            "pptx" => {
                return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
                    .to_string()
            },
            "tar" => return "application/x-tar".to_string(),
            "gz" | "gzip" => return "application/gzip".to_string(),
            "rar" => return "application/vnd.rar".to_string(),
            "7z" => return "application/x-7z-compressed".to_string(),
            _ => {},
        }
    }

    "application/octet-stream".to_string()
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[test]
    fn test_sanitize_path_component() {
        assert_eq!(sanitize_path_component("hello-world_123.txt"), "hello-world_123.txt");
        assert_eq!(sanitize_path_component("../../../etc/passwd"), "...etcpasswd");
        assert_eq!(sanitize_path_component("file with spaces"), "filewithspaces");
    }

    #[test]
    fn test_detect_mime_type() {
        // PNG magic bytes
        let png_data = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00];
        assert_eq!(detect_mime_type("test.png", &png_data), "image/png");

        // JPEG magic bytes
        let jpeg_data = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46];
        assert_eq!(detect_mime_type("test.jpg", &jpeg_data), "image/jpeg");

        // PDF magic bytes
        let pdf_data = b"%PDF-1.4 test";
        assert_eq!(detect_mime_type("test.pdf", pdf_data), "application/pdf");

        // Extension fallback
        assert_eq!(detect_mime_type("test.txt", b"hello"), "text/plain");
        assert_eq!(detect_mime_type("test.json", b"{}"), "application/json");

        // Unknown
        assert_eq!(detect_mime_type("test.unknown", b"data"), "application/octet-stream");
    }

    #[test]
    fn test_staging_manager() {
        let temp_dir = env::temp_dir().join("kalamdb_staging_test");
        let _ = fs::remove_dir_all(&temp_dir);

        let manager = StagingManager::new(&temp_dir);

        // Create request dir
        let request_dir = manager
            .create_request_dir("req123", &kalamdb_commons::UserId::new("user456"))
            .unwrap();
        assert!(request_dir.exists());

        // Stage a file
        let data = Bytes::from("Hello, World!");
        let staged = manager
            .stage_file(&request_dir, "test-file", "hello.txt", data, Some("text/plain"))
            .unwrap();

        assert!(staged.path.exists());
        assert_eq!(staged.size, 13);
        assert_eq!(staged.mime_type, "text/plain");
        assert!(!staged.sha256.is_empty());

        // Cleanup
        manager.cleanup_request_dir(&request_dir).unwrap();
        assert!(!request_dir.exists());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
