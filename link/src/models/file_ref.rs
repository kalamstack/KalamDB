//! FileRef model for FILE datatype in kalam-link client.
//!
//! This is the **canonical client-side** definition of a file reference.
//! Every SDK (TypeScript via tsify / WASM, Dart via flutter_rust_bridge)
//! should derive its `FileRef` type from this struct rather than
//! re-implementing parsing, URL generation, and utility methods.

use serde::{Deserialize, Serialize};

/// File reference stored as JSON in FILE columns.
///
/// Contains all metadata needed to locate and serve the file.
/// The server stores this as a JSON string inside FILE-typed columns.
///
/// # JSON example
///
/// ```json
/// {
///   "id": "1234567890123456789",
///   "sub": "f0001",
///   "name": "document.pdf",
///   "size": 1048576,
///   "mime": "application/pdf",
///   "sha256": "abc123..."
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct FileRef {
    /// Unique file identifier (Snowflake ID).
    pub id: String,

    /// Subfolder name (e.g., `"f0001"`, `"f0002"`).
    pub sub: String,

    /// Original filename (preserved for display/download).
    pub name: String,

    /// File size in bytes.
    pub size: u64,

    /// MIME type (e.g., `"image/png"`, `"application/pdf"`).
    pub mime: String,

    /// SHA-256 hash of file content (hex-encoded).
    pub sha256: String,

    /// Optional shard ID for shared tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard: Option<u32>,
}

impl FileRef {
    // ------------------------------------------------------------------
    // Constructors / Parsing
    // ------------------------------------------------------------------

    /// Parse a `FileRef` from a raw JSON string (as stored in FILE columns).
    pub fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json).ok()
    }

    /// Try to extract a `FileRef` from a [`serde_json::Value`].
    ///
    /// Handles both:
    /// - A JSON **string** (the value is parsed as JSON)
    /// - A JSON **object** (deserialized directly)
    pub fn from_json_value(value: &serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::String(s) => Self::from_json(s),
            serde_json::Value::Object(_) => serde_json::from_value(value.clone()).ok(),
            _ => None,
        }
    }

    /// Serialize back to a JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    // ------------------------------------------------------------------
    // URL generation
    // ------------------------------------------------------------------

    /// Full download URL for this file.
    ///
    /// ```text
    /// {base_url}/api/v1/files/{namespace}/{table}/{sub}/{id}
    /// ```
    pub fn download_url(&self, base_url: &str, namespace: &str, table: &str) -> String {
        let base = base_url.trim_end_matches('/');
        format!(
            "{}/api/v1/files/{}/{}/{}/{}",
            base, namespace, table, self.sub, self.id
        )
    }

    /// Relative HTTP path (no host) for this file.
    ///
    /// ```text
    /// /api/v1/files/{namespace}/{table}/{sub}/{id}
    /// ```
    pub fn relative_url(&self, namespace: &str, table: &str) -> String {
        format!(
            "/api/v1/files/{}/{}/{}/{}",
            namespace, table, self.sub, self.id
        )
    }

    // ------------------------------------------------------------------
    // Storage helpers (match backend `kalamdb-system` logic)
    // ------------------------------------------------------------------

    /// Stored filename on disk.
    ///
    /// Format: `{id}-{sanitized_name}.{ext}` or `{id}.{ext}` when the
    /// original name contains only non-ASCII characters.
    pub fn stored_name(&self) -> String {
        let sanitized = Self::sanitize_filename(&self.name);
        let ext = Self::extract_extension(&self.name);

        if sanitized.is_empty() {
            format!("{}.{}", self.id, ext)
        } else {
            format!("{}-{}.{}", self.id, sanitized, ext)
        }
    }

    /// Relative path within the table folder.
    ///
    /// - User tables: `{sub}/{stored_name}`
    /// - Shared tables with shard: `shard-{n}/{sub}/{stored_name}`
    pub fn relative_path(&self) -> String {
        let stored_name = self.stored_name();
        match self.shard {
            Some(shard_id) => format!("shard-{}/{}/{}", shard_id, self.sub, stored_name),
            None => format!("{}/{}", self.sub, stored_name),
        }
    }

    // ------------------------------------------------------------------
    // MIME helpers
    // ------------------------------------------------------------------

    /// Returns `true` if the MIME type indicates an image.
    pub fn is_image(&self) -> bool {
        self.mime.starts_with("image/")
    }

    /// Returns `true` if the MIME type indicates a video.
    pub fn is_video(&self) -> bool {
        self.mime.starts_with("video/")
    }

    /// Returns `true` if the MIME type indicates audio.
    pub fn is_audio(&self) -> bool {
        self.mime.starts_with("audio/")
    }

    /// Returns `true` if the MIME type indicates a PDF.
    pub fn is_pdf(&self) -> bool {
        self.mime == "application/pdf"
    }

    /// Human-readable file type description.
    ///
    /// Examples: `"Image"`, `"Video"`, `"PDF Document"`, `"PNG File"`.
    pub fn type_description(&self) -> String {
        if self.is_image() {
            return "Image".to_string();
        }
        if self.is_video() {
            return "Video".to_string();
        }
        if self.is_audio() {
            return "Audio".to_string();
        }
        if self.is_pdf() {
            return "PDF Document".to_string();
        }
        // Extract subtype from MIME
        if let Some((_type_part, subtype)) = self.mime.split_once('/') {
            format!("{} File", subtype.to_uppercase())
        } else {
            "File".to_string()
        }
    }

    // ------------------------------------------------------------------
    // Size formatting
    // ------------------------------------------------------------------

    /// Format file size in human-readable units.
    ///
    /// Examples: `"0 B"`, `"256 KB"`, `"1.5 MB"`, `"3.2 GB"`.
    pub fn format_size(&self) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut size = self.size as f64;
        let mut idx = 0;

        while size >= 1024.0 && idx < UNITS.len() - 1 {
            size /= 1024.0;
            idx += 1;
        }

        if idx == 0 {
            format!("{} {}", size as u64, UNITS[idx])
        } else {
            format!("{:.1} {}", size, UNITS[idx])
        }
    }

    // ------------------------------------------------------------------
    // Filename sanitization (mirrors backend logic)
    // ------------------------------------------------------------------

    /// Sanitize filename for storage (lowercase, ASCII-only, dashes).
    fn sanitize_filename(name: &str) -> String {
        let name_without_ext = name.rsplit_once('.').map(|(n, _)| n).unwrap_or(name);

        let sanitized: String = name_without_ext
            .chars()
            .filter_map(|c| {
                if c.is_ascii_alphanumeric() {
                    Some(c.to_ascii_lowercase())
                } else if c == ' ' || c == '_' || c == '-' {
                    Some('-')
                } else {
                    None
                }
            })
            .take(50)
            .collect();

        // Collapse multiple dashes, strip leading/trailing dashes.
        let mut result = String::with_capacity(sanitized.len());
        let mut last_was_dash = true;
        for c in sanitized.chars() {
            if c == '-' {
                if !last_was_dash {
                    result.push(c);
                }
                last_was_dash = true;
            } else {
                result.push(c);
                last_was_dash = false;
            }
        }
        result.trim_end_matches('-').to_string()
    }

    /// Extract file extension, defaulting to `"bin"`.
    fn extract_extension(name: &str) -> String {
        name.rsplit_once('.')
            .map(|(_, ext)| {
                let ext_lower = ext.to_ascii_lowercase();
                if ext_lower.len() <= 10 && ext_lower.chars().all(|c| c.is_ascii_alphanumeric()) {
                    ext_lower
                } else {
                    "bin".to_string()
                }
            })
            .unwrap_or_else(|| "bin".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_from_json_string() {
        let json = r#"{"id":"123","sub":"f0001","name":"test.png","size":1024,"mime":"image/png","sha256":"abc"}"#;
        let fr = FileRef::from_json(json).unwrap();
        assert_eq!(fr.id, "123");
        assert_eq!(fr.sub, "f0001");
        assert_eq!(fr.name, "test.png");
        assert_eq!(fr.size, 1024);
        assert!(fr.is_image());
    }

    #[test]
    fn parse_from_json_value_object() {
        let val = serde_json::json!({
            "id": "456", "sub": "f0002", "name": "doc.pdf",
            "size": 2048, "mime": "application/pdf", "sha256": "def"
        });
        let fr = FileRef::from_json_value(&val).unwrap();
        assert!(fr.is_pdf());
        assert_eq!(fr.type_description(), "PDF Document");
    }

    #[test]
    fn parse_from_json_value_string() {
        let inner = r#"{"id":"789","sub":"f0001","name":"a.txt","size":10,"mime":"text/plain","sha256":"x"}"#;
        let val = serde_json::Value::String(inner.to_string());
        let fr = FileRef::from_json_value(&val).unwrap();
        assert_eq!(fr.id, "789");
    }

    #[test]
    fn download_url_generation() {
        let fr = FileRef {
            id: "123".into(),
            sub: "f0001".into(),
            name: "t.png".into(),
            size: 0,
            mime: "image/png".into(),
            sha256: String::new(),
            shard: None,
        };
        assert_eq!(
            fr.download_url("http://localhost:8080", "default", "users"),
            "http://localhost:8080/api/v1/files/default/users/f0001/123"
        );
        assert_eq!(
            fr.relative_url("default", "users"),
            "/api/v1/files/default/users/f0001/123"
        );
    }

    #[test]
    fn format_size_units() {
        let mk = |size: u64| FileRef {
            id: String::new(),
            sub: String::new(),
            name: String::new(),
            size,
            mime: String::new(),
            sha256: String::new(),
            shard: None,
        };
        assert_eq!(mk(0).format_size(), "0 B");
        assert_eq!(mk(512).format_size(), "512 B");
        assert_eq!(mk(1024).format_size(), "1.0 KB");
        assert_eq!(mk(1_048_576).format_size(), "1.0 MB");
    }

    #[test]
    fn stored_name_and_path() {
        let fr = FileRef {
            id: "42".into(),
            sub: "f0001".into(),
            name: "My Document.pdf".into(),
            size: 100,
            mime: "application/pdf".into(),
            sha256: String::new(),
            shard: None,
        };
        assert_eq!(fr.stored_name(), "42-my-document.pdf");
        assert_eq!(fr.relative_path(), "f0001/42-my-document.pdf");
    }

    #[test]
    fn stored_name_with_shard() {
        let fr = FileRef {
            id: "42".into(),
            sub: "f0001".into(),
            name: "test.png".into(),
            size: 100,
            mime: "image/png".into(),
            sha256: String::new(),
            shard: Some(3),
        };
        assert_eq!(fr.relative_path(), "shard-3/f0001/42-test.png");
    }

    #[test]
    fn cell_as_file() {
        use super::super::kalam_cell_value::KalamCellValue;

        // JSON object → FileRef
        let cell = KalamCellValue::from(serde_json::json!({
            "id": "1", "sub": "f0001", "name": "a.png",
            "size": 10, "mime": "image/png", "sha256": "x"
        }));
        let fr = cell.as_file().unwrap();
        assert_eq!(fr.id, "1");
        assert!(fr.is_image());

        // Non-file value → None
        assert!(KalamCellValue::text("Alice").as_file().is_none());

        // Null → None
        assert!(KalamCellValue::null().as_file().is_none());
    }
}
