//! Topic message schema definition
//!
//! Provides cached Arrow schema for CONSUME query results.
//! The schema is derived from `TopicMessage::definition()` so the model and
//! Arrow projection stay in sync.

use datafusion::arrow::datatypes::SchemaRef;
#[cfg(test)]
use datafusion::arrow::datatypes::DataType;
use std::sync::OnceLock;

use super::topic_message_models::TopicMessage;

/// Topic message schema singleton
///
/// Returns cached Arrow schema for CONSUME FROM query results with 8 fields:
/// - topic_id: Utf8 (NOT NULL) - Topic identifier
/// - partition_id: Int32 (NOT NULL) - Partition ID
/// - offset: Int64 (NOT NULL) - Message offset
/// - key: Utf8 (NULLABLE) - Optional message key
/// - payload: Binary (NOT NULL) - Message payload bytes
/// - timestamp_ms: Int64 (NOT NULL) - Message timestamp in milliseconds
/// - user_id: Utf8 (NULLABLE) - User that triggered the message
/// - op: Utf8 (NOT NULL) - Operation type (insert, update, delete)
///
/// # Performance
/// Schema is constructed once and cached globally using `OnceLock`.
/// All subsequent calls return a cheap Arc clone (~8 bytes vs ~300 bytes for inline construction).
///
/// # Example
/// ```rust
/// use kalamdb_tables::topics::topic_message_schema;
///
/// let schema = topic_message_schema(); // First call: constructs schema
/// let schema2 = topic_message_schema(); // Subsequent calls: cheap Arc clone
/// ```
pub fn topic_message_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            TopicMessage::definition()
                .to_arrow_schema()
                .expect("Failed to create system.topic_messages Arrow schema")
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_message_schema() {
        let schema = topic_message_schema();
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).name(), "topic_id");
        assert_eq!(schema.field(0).is_nullable(), false);
        assert_eq!(schema.field(1).name(), "partition_id");
        assert_eq!(schema.field(2).name(), "offset");
        assert_eq!(schema.field(3).name(), "key");
        assert_eq!(schema.field(3).is_nullable(), true);
        assert_eq!(schema.field(4).name(), "payload");
        assert_eq!(schema.field(5).name(), "timestamp_ms");
        assert_eq!(schema.field(5).data_type(), &DataType::Int64);
        assert_eq!(schema.field(6).name(), "user_id");
        assert_eq!(schema.field(6).is_nullable(), true);
        assert_eq!(schema.field(7).name(), "op");
        assert_eq!(schema.field(7).is_nullable(), false);
    }

    #[test]
    fn test_schema_singleton() {
        let schema1 = topic_message_schema();
        let schema2 = topic_message_schema();

        // Verify both calls return Arc pointers to same underlying schema
        assert!(std::sync::Arc::ptr_eq(&schema1, &schema2));
    }
}
