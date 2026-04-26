//! system.server_logs virtual view
//!
//! **Type**: Virtual View (not backed by persistent storage)
//!
//! Provides read access to server log files in JSON Lines format.
//! Requires `format = "json"` in logging configuration.
//!
//! **DataFusion Pattern**: Implements VirtualView trait for consistent view behavior
//! - Reads log files dynamically on each query (no cached state)
//! - Only used in development environments
//! - No memory consumption when idle
//!
//! **Schema Caching**: Memoized via `OnceLock`
//! **Schema**: TableDefinition provides consistent metadata for views

use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use datafusion::arrow::{
    array::{ArrayRef, Int64Builder, StringBuilder},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use kalamdb_commons::{
    datatypes::KalamDataType,
    schemas::{ColumnDefault, ColumnDefinition, TableDefinition, TableOptions, TableType},
    NamespaceId, TableName,
};
use kalamdb_system::SystemTable;

use crate::{
    error::RegistryError,
    view_base::{ViewTableProvider, VirtualView},
};

/// Get or initialize the server_logs schema (memoized)
fn server_logs_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            ServerLogsView::definition()
                .to_arrow_schema()
                .expect("Failed to convert server_logs TableDefinition to Arrow schema")
        })
        .clone()
}

/// Parsed server log entry exposed through `system.server_logs`.
#[derive(Debug)]
struct JsonLogEntry {
    timestamp: String,
    level: String,
    thread: Option<String>,
    target: Option<String>,
    line: Option<i64>,
    message: String,
}

/// Raw JSON log entry structure.
///
/// `tracing-subscriber` JSON output nests event fields under `fields`, while
/// older/manual log lines may already expose `message` at the root.
#[derive(Debug, serde::Deserialize)]
struct RawJsonLogEntry {
    timestamp: Option<String>,
    level: Option<String>,
    #[serde(alias = "threadName")]
    thread: Option<String>,
    target: Option<String>,
    line: Option<i64>,
    message: Option<String>,
    fields: Option<RawJsonLogFields>,
}

#[derive(Debug, serde::Deserialize)]
struct RawJsonLogFields {
    message: Option<String>,
}

impl RawJsonLogEntry {
    fn into_entry(self) -> Option<JsonLogEntry> {
        Some(JsonLogEntry {
            timestamp: self.timestamp?,
            level: self.level?,
            thread: self.thread,
            target: self.target,
            line: self.line,
            message: self.message.or_else(|| self.fields.and_then(|fields| fields.message))?,
        })
    }
}

/// ServerLogsView - Reads server log files dynamically
#[derive(Debug)]
pub struct ServerLogsView {
    logs_path: PathBuf,
}

impl ServerLogsView {
    /// Get the TableDefinition for system.server_logs view
    ///
    /// Schema:
    /// - timestamp TEXT NOT NULL (log timestamp)
    /// - level TEXT NOT NULL (log level: DEBUG, INFO, WARN, ERROR)
    /// - thread TEXT (nullable - thread name)
    /// - target TEXT (nullable - module/target name)
    /// - line BIGINT (nullable - source line number)
    /// - message TEXT NOT NULL (log message content)
    pub fn definition() -> TableDefinition {
        let columns = vec![
            ColumnDefinition::new(
                1,
                "timestamp",
                1,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Log entry timestamp (ISO 8601 format)".to_string()),
            ),
            ColumnDefinition::new(
                2,
                "level",
                2,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Log level (DEBUG, INFO, WARN, ERROR)".to_string()),
            ),
            ColumnDefinition::new(
                3,
                "thread",
                3,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Thread name that generated the log".to_string()),
            ),
            ColumnDefinition::new(
                4,
                "target",
                4,
                KalamDataType::Text,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Module or target that generated the log".to_string()),
            ),
            ColumnDefinition::new(
                5,
                "line",
                5,
                KalamDataType::BigInt,
                true,
                false,
                false,
                ColumnDefault::None,
                Some("Source code line number".to_string()),
            ),
            ColumnDefinition::new(
                6,
                "message",
                6,
                KalamDataType::Text,
                false,
                false,
                false,
                ColumnDefault::None,
                Some("Log message content".to_string()),
            ),
        ];

        TableDefinition::new(
            NamespaceId::system(),
            TableName::new(SystemTable::ServerLogs.table_name()),
            TableType::System,
            columns,
            TableOptions::system(),
            Some("Server log entries from JSON log files (read-only view)".to_string()),
        )
        .expect("Failed to create system.server_logs view definition")
    }

    /// Create a new server logs view
    pub fn new(logs_path: impl Into<PathBuf>) -> Self {
        Self {
            logs_path: logs_path.into(),
        }
    }

    /// Read and parse log entries from the server.jsonl file
    fn read_log_entries(&self) -> Result<Vec<JsonLogEntry>, RegistryError> {
        // Try .jsonl first (JSON format), fallback to .log (compact format)
        let jsonl_path = self.logs_path.join("server.jsonl");
        let log_file_path = if jsonl_path.exists() {
            jsonl_path
        } else {
            self.logs_path.join("server.log")
        };

        if !log_file_path.exists() {
            // No log file yet, return empty
            return Ok(vec![]);
        }

        let content = std::fs::read_to_string(&log_file_path).map_err(|e| {
            RegistryError::Other(format!(
                "Failed to read log file {}: {}",
                log_file_path.display(),
                e
            ))
        })?;

        let mut entries = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Try to parse as JSON
            match serde_json::from_str::<RawJsonLogEntry>(line) {
                Ok(entry) => {
                    if let Some(entry) = entry.into_entry() {
                        entries.push(entry);
                    }
                },
                Err(_) => {
                    // Skip non-JSON lines (e.g., if format is "compact")
                    // This allows graceful degradation
                    continue;
                },
            }
        }

        Ok(entries)
    }
}

impl VirtualView for ServerLogsView {
    fn system_table(&self) -> SystemTable {
        SystemTable::ServerLogs
    }

    fn schema(&self) -> SchemaRef {
        server_logs_schema()
    }

    fn compute_batch(&self) -> Result<RecordBatch, RegistryError> {
        let entries = self.read_log_entries()?;

        let mut timestamps = StringBuilder::new();
        let mut levels = StringBuilder::new();
        let mut threads = StringBuilder::new();
        let mut targets = StringBuilder::new();
        let mut lines = Int64Builder::new();
        let mut messages = StringBuilder::new();

        for entry in entries {
            timestamps.append_value(&entry.timestamp);
            levels.append_value(&entry.level);

            if let Some(thread) = &entry.thread {
                threads.append_value(thread);
            } else {
                threads.append_null();
            }

            if let Some(target) = &entry.target {
                targets.append_value(target);
            } else {
                targets.append_null();
            }

            if let Some(line) = entry.line {
                lines.append_value(line);
            } else {
                lines.append_null();
            }

            messages.append_value(&entry.message);
        }

        RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(timestamps.finish()) as ArrayRef,
                Arc::new(levels.finish()) as ArrayRef,
                Arc::new(threads.finish()) as ArrayRef,
                Arc::new(targets.finish()) as ArrayRef,
                Arc::new(lines.finish()) as ArrayRef,
                Arc::new(messages.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| RegistryError::Other(format!("Failed to build server_logs batch: {}", e)))
    }
}

/// Type alias for the server logs table provider
pub type ServerLogsTableProvider = ViewTableProvider<ServerLogsView>;

/// Helper function to create a server logs table provider
pub fn create_server_logs_provider(logs_path: impl Into<PathBuf>) -> ServerLogsTableProvider {
    ViewTableProvider::new(Arc::new(ServerLogsView::new(logs_path)))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_schema() {
        let schema = server_logs_schema();
        assert_eq!(schema.fields().len(), 6);
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "level");
        assert_eq!(schema.field(2).name(), "thread");
        assert_eq!(schema.field(3).name(), "target");
        assert_eq!(schema.field(4).name(), "line");
        assert_eq!(schema.field(5).name(), "message");
    }

    #[test]
    fn test_parse_json_logs() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("server.log");

        // Write some JSON log entries
        let mut file = std::fs::File::create(&log_file).unwrap();
        writeln!(
            file,
            r#"{{"timestamp":"2024-01-15T10:30:00.123+00:00","level":"INFO","thread":"main","target":"kalamdb_server","line":42,"message":"Server started"}}"#
        )
        .unwrap();
        writeln!(
            file,
            r#"{{"timestamp":"2024-01-15T10:30:01.456+00:00","level":"DEBUG","thread":"worker-1","target":"kalamdb_core","line":100,"message":"Processing query"}}"#
        )
        .unwrap();

        let view = ServerLogsView::new(dir.path());
        let entries = view.read_log_entries().unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].level, "INFO");
        assert_eq!(entries[0].message, "Server started");
        assert_eq!(entries[1].level, "DEBUG");
    }

    #[test]
    fn test_parse_tracing_json_logs() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("server.jsonl");

        let mut file = std::fs::File::create(&log_file).unwrap();
        writeln!(
            file,
            r#"{{"timestamp":"2024-01-15T10:30:00.123Z","level":"INFO","fields":{{"message":"Server started"}},"target":"kalamdb_server","threadName":"main"}}"#
        )
        .unwrap();

        let view = ServerLogsView::new(dir.path());
        let entries = view.read_log_entries().unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].level, "INFO");
        assert_eq!(entries[0].message, "Server started");
        assert_eq!(entries[0].thread.as_deref(), Some("main"));
    }

    #[test]
    fn test_empty_log_file() {
        let dir = tempdir().unwrap();
        // Don't create the log file

        let view = ServerLogsView::new(dir.path());
        let entries = view.read_log_entries().unwrap();

        assert!(entries.is_empty());
    }

    #[test]
    fn test_compute_batch() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("server.jsonl");

        let mut file = std::fs::File::create(&log_file).unwrap();
        writeln!(
            file,
            r#"{{"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"Test"}}"#
        )
        .unwrap();

        let view = ServerLogsView::new(dir.path());
        let batch = view.compute_batch().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6);
    }
}
