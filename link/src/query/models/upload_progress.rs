use serde::{Deserialize, Serialize};

/// Upload progress information for a single file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct UploadProgress {
    /// 1-based index of the current file being uploaded.
    pub file_index: usize,
    /// Total number of files in the upload.
    pub total_files: usize,
    /// Filename being uploaded.
    pub file_name: String,
    /// Bytes sent so far for this file.
    pub bytes_sent: u64,
    /// Total bytes for this file.
    pub total_bytes: u64,
    /// Percent complete for this file (0-100).
    pub percent: f64,
}
