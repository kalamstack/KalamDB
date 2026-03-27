//! Storage operation result types.
//!
//! Defines the return types for unified storage operations via `StorageCached`.
//! Each operation returns a structured result containing relevant information.

use bytes::Bytes;

/// Result of a list operation
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Absolute paths of all files found
    pub paths: Vec<String>,
    /// Number of files found
    pub count: usize,
    /// The prefix that was searched
    pub prefix: String,
}

impl ListResult {
    /// Create a new ListResult
    pub fn new(paths: Vec<String>, prefix: String) -> Self {
        let count = paths.len();
        Self {
            paths,
            count,
            prefix,
        }
    }

    /// Check if any files were found
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Get file paths as iterator
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.paths.iter()
    }
}

/// Result of a get (read) operation
#[derive(Debug)]
pub struct GetResult {
    /// File contents as bytes
    pub data: Bytes,
    /// Size in bytes
    pub size: usize,
    /// Path that was read
    pub path: String,
}

impl GetResult {
    /// Create a new GetResult
    pub fn new(data: Bytes, path: String) -> Self {
        let size = data.len();
        Self { data, size, path }
    }

    /// Check if the file is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Result of a put (write) operation
#[derive(Debug, Clone)]
pub struct PutResult {
    /// Path where data was written
    pub path: String,
    /// Size in bytes written
    pub size: usize,
}

impl PutResult {
    /// Create a new PutResult
    pub fn new(path: String, size: usize) -> Self {
        Self { path, size }
    }
}

/// Result of a delete operation
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Path that was deleted
    pub path: String,
    /// Whether the file existed before deletion
    pub existed: bool,
}

impl DeleteResult {
    /// Create a new DeleteResult
    pub fn new(path: String, existed: bool) -> Self {
        Self { path, existed }
    }
}

/// Result of a prefix delete operation (e.g., DROP TABLE cleanup)
#[derive(Debug, Clone)]
pub struct DeletePrefixResult {
    /// The prefix that was deleted
    pub prefix: String,
    /// Number of files deleted
    pub files_deleted: usize,
    /// Paths of deleted files
    pub deleted_paths: Vec<String>,
}

impl DeletePrefixResult {
    /// Create a new DeletePrefixResult
    pub fn new(prefix: String, deleted_paths: Vec<String>) -> Self {
        let files_deleted = deleted_paths.len();
        Self {
            prefix,
            files_deleted,
            deleted_paths,
        }
    }

    /// Check if any files were deleted
    pub fn is_empty(&self) -> bool {
        self.deleted_paths.is_empty()
    }
}

/// Result of a path resolution operation
#[derive(Debug, Clone)]
pub struct PathResult {
    /// Fully resolved path (including base directory)
    pub full_path: String,
    /// Relative path within storage (without base directory)
    pub relative_path: String,
    /// Base directory of the storage
    pub base_directory: String,
}

impl PathResult {
    /// Create a new PathResult
    pub fn new(full_path: String, relative_path: String, base_directory: String) -> Self {
        Self {
            full_path,
            relative_path,
            base_directory,
        }
    }
}

/// Result of checking if a path exists
#[derive(Debug, Clone)]
pub struct ExistsResult {
    /// Path that was checked
    pub path: String,
    /// Whether the path exists
    pub exists: bool,
    /// Size if file exists, None otherwise
    pub size: Option<usize>,
}

impl ExistsResult {
    /// Create a new ExistsResult
    pub fn new(path: String, exists: bool, size: Option<usize>) -> Self {
        Self { path, exists, size }
    }
}

/// Metadata about a file
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// Full path to the file
    pub path: String,
    /// Size in bytes
    pub size: usize,
    /// Last modified timestamp (milliseconds since epoch)
    pub last_modified_ms: Option<i64>,
}

impl FileInfo {
    /// Create new FileInfo
    pub fn new(path: String, size: usize, last_modified_ms: Option<i64>) -> Self {
        Self {
            path,
            size,
            last_modified_ms,
        }
    }
}

/// Result of a rename/move operation
#[derive(Debug, Clone)]
pub struct RenameResult {
    /// Original path
    pub from: String,
    /// New path
    pub to: String,
    /// Whether the operation succeeded
    pub success: bool,
}

impl RenameResult {
    /// Create a new RenameResult
    pub fn new(from: String, to: String, success: bool) -> Self {
        Self { from, to, success }
    }
}
