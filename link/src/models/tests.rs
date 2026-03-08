use crate::seq_id::SeqId;
use serde_json::json;
use std::collections::BTreeSet;

use super::*;

// ==================== ConnectionOptions Tests ====================

#[test]
fn test_connection_options_default() {
    let opts = ConnectionOptions::default();

    assert!(opts.auto_reconnect, "auto_reconnect should default to true");
    assert_eq!(opts.reconnect_delay_ms, 1000, "reconnect_delay_ms should default to 1000");
    assert_eq!(
        opts.max_reconnect_delay_ms, 30000,
        "max_reconnect_delay_ms should default to 30000"
    );
    assert!(
        opts.max_reconnect_attempts.is_none(),
        "max_reconnect_attempts should default to None (infinite)"
    );
    assert!(
        opts.ws_local_bind_addresses.is_empty(),
        "ws_local_bind_addresses should default to empty"
    );
}

#[test]
fn test_connection_options_new() {
    let opts = ConnectionOptions::new();

    // new() should be equivalent to default()
    assert!(opts.auto_reconnect);
    assert_eq!(opts.reconnect_delay_ms, 1000);
    assert_eq!(opts.max_reconnect_delay_ms, 30000);
    assert!(opts.max_reconnect_attempts.is_none());
    assert!(opts.ws_local_bind_addresses.is_empty());
}

#[test]
fn test_connection_options_builder_pattern() {
    let opts = ConnectionOptions::new()
        .with_auto_reconnect(false)
        .with_reconnect_delay_ms(2000)
        .with_max_reconnect_delay_ms(60000)
        .with_max_reconnect_attempts(Some(5))
        .with_ws_local_bind_addresses(vec!["127.0.0.1".to_string(), "127.0.0.2".to_string()]);

    assert!(!opts.auto_reconnect);
    assert_eq!(opts.reconnect_delay_ms, 2000);
    assert_eq!(opts.max_reconnect_delay_ms, 60000);
    assert_eq!(opts.max_reconnect_attempts, Some(5));
    assert_eq!(
        opts.ws_local_bind_addresses,
        vec!["127.0.0.1".to_string(), "127.0.0.2".to_string()]
    );
}

#[test]
fn test_connection_options_disable_reconnect() {
    // Setting max_reconnect_attempts to Some(0) should disable reconnection
    let opts = ConnectionOptions::new().with_max_reconnect_attempts(Some(0));

    assert_eq!(opts.max_reconnect_attempts, Some(0));
}

#[test]
fn test_connection_options_infinite_retries() {
    // None means infinite retries
    let opts = ConnectionOptions::new().with_max_reconnect_attempts(None);

    assert!(opts.max_reconnect_attempts.is_none());
}

#[test]
fn test_connection_options_serialization() {
    let opts = ConnectionOptions::new()
        .with_auto_reconnect(true)
        .with_reconnect_delay_ms(500)
        .with_max_reconnect_delay_ms(10000)
        .with_max_reconnect_attempts(Some(3));

    let json = serde_json::to_string(&opts).unwrap();
    let parsed: ConnectionOptions = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.auto_reconnect, opts.auto_reconnect);
    assert_eq!(parsed.reconnect_delay_ms, opts.reconnect_delay_ms);
    assert_eq!(parsed.max_reconnect_delay_ms, opts.max_reconnect_delay_ms);
    assert_eq!(parsed.max_reconnect_attempts, opts.max_reconnect_attempts);
    assert_eq!(parsed.ws_local_bind_addresses, opts.ws_local_bind_addresses);
}

#[test]
fn test_connection_options_deserialization_with_defaults() {
    // Test that missing fields get proper defaults
    let json = r#"{"auto_reconnect": false}"#;
    let opts: ConnectionOptions = serde_json::from_str(json).unwrap();

    assert!(!opts.auto_reconnect);
    assert_eq!(opts.reconnect_delay_ms, 1000); // default
    assert_eq!(opts.max_reconnect_delay_ms, 30000); // default
    assert!(opts.max_reconnect_attempts.is_none()); // default
    assert!(opts.ws_local_bind_addresses.is_empty()); // default
}

// ==================== SubscriptionOptions Tests ====================

#[test]
fn test_subscription_options_default() {
    let opts = SubscriptionOptions::default();

    assert!(opts.batch_size.is_none(), "batch_size should default to None");
    assert!(opts.last_rows.is_none(), "last_rows should default to None");
    assert!(opts.from.is_none(), "from should default to None");
}

#[test]
fn test_subscription_options_new() {
    let opts = SubscriptionOptions::new();

    // new() should be equivalent to default()
    assert!(opts.batch_size.is_none());
    assert!(opts.last_rows.is_none());
    assert!(opts.from.is_none());
}

#[test]
fn test_subscription_options_builder_pattern() {
    let seq_id = SeqId::from(12345i64);
    let opts = SubscriptionOptions::new()
        .with_batch_size(100)
        .with_last_rows(50)
        .with_from(seq_id);

    assert_eq!(opts.batch_size, Some(100));
    assert_eq!(opts.last_rows, Some(50));
    assert!(opts.from.is_some());
    assert_eq!(opts.from.unwrap(), seq_id);
}

#[test]
fn test_subscription_options_with_batch_size_only() {
    let opts = SubscriptionOptions::new().with_batch_size(500);

    assert_eq!(opts.batch_size, Some(500));
    assert!(opts.last_rows.is_none());
    assert!(opts.from.is_none());
}

#[test]
fn test_subscription_options_with_last_rows_only() {
    let opts = SubscriptionOptions::new().with_last_rows(25);

    assert!(opts.batch_size.is_none());
    assert_eq!(opts.last_rows, Some(25));
    assert!(opts.from.is_none());
}

#[test]
fn test_subscription_options_with_from_only() {
    let seq_id = SeqId::from(99999i64);
    let opts = SubscriptionOptions::new().with_from(seq_id);

    assert!(opts.batch_size.is_none());
    assert!(opts.last_rows.is_none());
    assert_eq!(opts.from, Some(seq_id));
}

#[test]
fn test_subscription_options_has_resume_seq_id() {
    let opts_without = SubscriptionOptions::new();
    assert!(!opts_without.has_resume_seq_id());

    let opts_with = SubscriptionOptions::new().with_from(SeqId::from(123i64));
    assert!(opts_with.has_resume_seq_id());
}

#[test]
fn test_subscription_options_serialization() {
    let opts = SubscriptionOptions::new().with_batch_size(200).with_last_rows(100);

    let json = serde_json::to_string(&opts).unwrap();

    // Verify JSON structure
    assert!(json.contains("\"batch_size\":200"));
    assert!(json.contains("\"last_rows\":100"));
    assert!(!json.contains("\"from\":"));
}

#[test]
fn test_subscription_options_serialization_with_seq_id() {
    let seq_id = SeqId::from(42i64);
    let opts = SubscriptionOptions::new().with_batch_size(50).with_from(seq_id);

    let json = serde_json::to_string(&opts).unwrap();

    assert!(json.contains("\"batch_size\":50"));
    assert!(json.contains("\"from\":42"));
}

#[test]
fn test_subscription_options_deserialization() {
    let json = r#"{"batch_size": 100, "last_rows": 50}"#;
    let opts: SubscriptionOptions = serde_json::from_str(json).unwrap();

    assert_eq!(opts.batch_size, Some(100));
    assert_eq!(opts.last_rows, Some(50));
    assert!(opts.from.is_none());
}

#[test]
fn test_subscription_options_deserialization_alias() {
    let json = r#"{"from_seq_id": 75}"#;
    let opts: SubscriptionOptions = serde_json::from_str(json).unwrap();

    assert_eq!(opts.from, Some(SeqId::from(75i64)));
}

#[test]
fn test_subscription_options_deserialization_empty() {
    let json = r#"{}"#;
    let opts: SubscriptionOptions = serde_json::from_str(json).unwrap();

    assert!(opts.batch_size.is_none());
    assert!(opts.last_rows.is_none());
    assert!(opts.from.is_none());
}

// ==================== Options Separation Tests ====================

#[test]
fn test_connection_and_subscription_options_are_independent() {
    // Ensure the two option types don't overlap in their fields
    let conn_opts =
        ConnectionOptions::new().with_auto_reconnect(true).with_reconnect_delay_ms(1000);

    let sub_opts = SubscriptionOptions::new().with_batch_size(100).with_last_rows(50);

    // Connection options should NOT have subscription fields
    let conn_json = serde_json::to_string(&conn_opts).unwrap();
    assert!(!conn_json.contains("batch_size"));
    assert!(!conn_json.contains("last_rows"));
    assert!(!conn_json.contains("\"from\":"));

    // Subscription options should NOT have connection fields
    let sub_json = serde_json::to_string(&sub_opts).unwrap();
    assert!(!sub_json.contains("auto_reconnect"));
    assert!(!sub_json.contains("reconnect_delay"));
    assert!(!sub_json.contains("max_reconnect"));
}

// ==================== SubscriptionRequest Tests ====================

#[test]
fn test_subscription_request_with_options() {
    let opts = SubscriptionOptions::new().with_batch_size(100).with_last_rows(25);

    let request = SubscriptionRequest {
        id: "sub-123".to_string(),
        sql: "SELECT * FROM messages".to_string(),
        options: opts,
    };

    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("\"id\":\"sub-123\""));
    assert!(json.contains("SELECT * FROM messages"));
    assert!(json.contains("\"batch_size\":100"));
    assert!(json.contains("\"last_rows\":25"));
}

#[test]
fn test_subscription_request_with_default_options() {
    let request = SubscriptionRequest {
        id: "sub-456".to_string(),
        sql: "SELECT * FROM users".to_string(),
        options: SubscriptionOptions::default(),
    };

    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("\"id\":\"sub-456\""));
    // Default options with all None should serialize as empty object or with no optional fields
}

// ==================== ClientMessage Tests ====================

#[test]
fn test_client_message_authenticate_jwt_serialization() {
    let msg = ClientMessage::Authenticate {
        credentials: WsAuthCredentials::Jwt {
            token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test".to_string(),
        },
    };

    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"type\":\"authenticate\""));
    assert!(json.contains("\"method\":\"jwt\""));
    assert!(json.contains("\"token\":\"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test\""));
}

#[test]
fn test_client_message_subscribe_serialization() {
    let msg = ClientMessage::Subscribe {
        subscription: SubscriptionRequest {
            id: "test-sub".to_string(),
            sql: "SELECT * FROM chat.messages".to_string(),
            options: SubscriptionOptions::new().with_batch_size(50),
        },
    };

    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"type\":\"subscribe\""));
    assert!(json.contains("\"id\":\"test-sub\""));
    assert!(json.contains("SELECT * FROM chat.messages"));
    assert!(json.contains("\"batch_size\":50"));
}

#[test]
fn test_client_message_subscribe_with_resume() {
    let seq_id = SeqId::from(12345i64);
    let msg = ClientMessage::Subscribe {
        subscription: SubscriptionRequest {
            id: "resume-sub".to_string(),
            sql: "SELECT * FROM events".to_string(),
            options: SubscriptionOptions::new().with_from(seq_id),
        },
    };

    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"type\":\"subscribe\""));
    assert!(json.contains("\"from\":12345"));
}

// ==================== BatchControl Tests ====================

#[test]
fn test_batch_control_with_seq_id() {
    let seq_id = SeqId::from(999i64);
    let batch_control = BatchControl {
        batch_num: 0,
        has_more: true,
        status: BatchStatus::Loading,
        last_seq_id: Some(seq_id),
        snapshot_end_seq: Some(SeqId::from(1000i64)),
    };

    let json = serde_json::to_string(&batch_control).unwrap();
    assert!(json.contains("\"batch_num\":0"));
    assert!(json.contains("\"has_more\":true"));
    assert!(json.contains("\"status\":\"loading\""));
    assert!(json.contains("last_seq_id"));
    assert!(json.contains("snapshot_end_seq"));
}

#[test]
fn test_batch_control_ready_status() {
    let batch_control = BatchControl {
        batch_num: 5,
        has_more: false,
        status: BatchStatus::Ready,
        last_seq_id: Some(SeqId::from(1000i64)),
        snapshot_end_seq: Some(SeqId::from(1000i64)),
    };

    let json = serde_json::to_string(&batch_control).unwrap();
    assert!(json.contains("\"status\":\"ready\""));
    assert!(json.contains("\"has_more\":false"));
}

#[test]
fn test_batch_status_serialization() {
    // Test all BatchStatus variants
    let loading = BatchStatus::Loading;
    let loading_batch = BatchStatus::LoadingBatch;
    let ready = BatchStatus::Ready;

    assert_eq!(serde_json::to_string(&loading).unwrap(), "\"loading\"");
    assert_eq!(serde_json::to_string(&loading_batch).unwrap(), "\"loading_batch\"");
    assert_eq!(serde_json::to_string(&ready).unwrap(), "\"ready\"");
}

// ==================== ServerMessage Parsing Tests ====================

#[test]
fn test_server_message_initial_data_batch_parsing() {
    let json = r#"{
        "type": "initial_data_batch",
        "subscription_id": "sub-123",
        "rows": [{"id": 1, "name": "test"}],
        "batch_control": {
            "batch_num": 0,
            "has_more": true,
            "status": "loading",
            "last_seq_id": 12345
        }
    }"#;

    let msg: ServerMessage = serde_json::from_str(json).unwrap();
    match msg {
        ServerMessage::InitialDataBatch {
            subscription_id,
            rows,
            batch_control,
        } => {
            assert_eq!(subscription_id, "sub-123");
            assert_eq!(rows.len(), 1);
            assert!(batch_control.has_more);
            assert_eq!(batch_control.last_seq_id, Some(SeqId::from(12345i64)));
        },
        _ => panic!("Expected InitialDataBatch"),
    }
}

#[test]
fn test_server_message_change_parsing() {
    let json = r#"{
        "type": "change",
        "subscription_id": "sub-456",
        "change_type": "insert",
        "rows": [{"id": 2, "content": "new message"}]
    }"#;

    let msg: ServerMessage = serde_json::from_str(json).unwrap();
    match msg {
        ServerMessage::Change {
            subscription_id,
            change_type,
            rows,
            old_values,
        } => {
            assert_eq!(subscription_id, "sub-456");
            assert_eq!(change_type, ChangeTypeRaw::Insert);
            assert!(rows.is_some());
            assert!(old_values.is_none());
        },
        _ => panic!("Expected Change"),
    }
}

// ==================== Reconnection Scenario Tests ====================

#[test]
fn test_subscription_options_for_reconnection() {
    // Simulate what happens during reconnection:
    // 1. Start with default options
    let initial_opts = SubscriptionOptions::new().with_batch_size(100);
    assert!(!initial_opts.has_resume_seq_id());

    // 2. After receiving data, we have a last_seq_id
    let last_received_seq = SeqId::from(54321i64);

    // 3. On reconnect, we create new options with the resume seq_id
    let reconnect_opts = SubscriptionOptions::new()
        .with_batch_size(initial_opts.batch_size.unwrap_or(100))
        .with_from(last_received_seq);

    assert!(reconnect_opts.has_resume_seq_id());
    assert_eq!(reconnect_opts.from, Some(last_received_seq));
    assert_eq!(reconnect_opts.batch_size, Some(100));
}

#[test]
fn test_connection_options_exponential_backoff_calculation() {
    let opts = ConnectionOptions::new()
        .with_reconnect_delay_ms(1000)
        .with_max_reconnect_delay_ms(30000);

    // Simulate exponential backoff calculation
    let base_delay = opts.reconnect_delay_ms;
    let max_delay = opts.max_reconnect_delay_ms;

    // Attempt 0: 1000ms
    let delay_0 = std::cmp::min(base_delay * 2u64.pow(0), max_delay);
    assert_eq!(delay_0, 1000);

    // Attempt 1: 2000ms
    let delay_1 = std::cmp::min(base_delay * 2u64.pow(1), max_delay);
    assert_eq!(delay_1, 2000);

    // Attempt 2: 4000ms
    let delay_2 = std::cmp::min(base_delay * 2u64.pow(2), max_delay);
    assert_eq!(delay_2, 4000);

    // Attempt 5: 32000ms -> capped at 30000ms
    let delay_5 = std::cmp::min(base_delay * 2u64.pow(5), max_delay);
    assert_eq!(delay_5, 30000);
}

// ==================== Original Tests ====================

#[test]
fn test_query_request_serialization() {
    let request = QueryRequest {
        sql: "SELECT * FROM users WHERE id = $1".to_string(),
        params: Some(vec![json!(42)]),
        namespace_id: None,
    };

    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("SELECT * FROM users"));
    assert!(json.contains("params"));
}

#[test]
fn test_change_event_helpers() {
    let mut insert_row = std::collections::HashMap::new();
    insert_row.insert("id".to_string(), KalamCellValue::int(1));
    let insert = ChangeEvent::Insert {
        subscription_id: "sub-1".to_string(),
        rows: vec![insert_row],
    };
    assert_eq!(insert.subscription_id(), Some("sub-1"));
    assert!(!insert.is_error());

    let error = ChangeEvent::Error {
        subscription_id: "sub-2".to_string(),
        code: "ERR".to_string(),
        message: "test error".to_string(),
    };
    assert!(error.is_error());
    assert_eq!(error.subscription_id(), Some("sub-2"));

    let ack = ChangeEvent::Ack {
        subscription_id: "sub-1".to_string(),
        total_rows: 0,
        batch_control: BatchControl {
            batch_num: 0,
            has_more: false,
            status: BatchStatus::Ready,
            last_seq_id: None,
            snapshot_end_seq: None,
        },
        schema: vec![SchemaField {
            name: "id".to_string(),
            data_type: KalamDataType::BigInt,
            index: 0,
            flags: None,
        }],
    };
    assert_eq!(ack.subscription_id(), Some("sub-1"));
    assert!(!ack.is_error());
}

#[test]
fn test_schema_field_flags_deserializes_array() {
    let json = r#"{"name":"id","data_type":"BigInt","index":0,"flags":["pk","nn","uq"]}"#;
    let field: SchemaField = serde_json::from_str(json).unwrap();

    let expected = BTreeSet::from([FieldFlag::PrimaryKey, FieldFlag::NonNull, FieldFlag::Unique]);

    assert_eq!(field.flags, Some(expected));
}

#[test]
fn test_schema_field_flags_serializes_as_array() {
    let field = SchemaField {
        name: "id".to_string(),
        data_type: KalamDataType::BigInt,
        index: 0,
        flags: Some(BTreeSet::from([FieldFlag::PrimaryKey, FieldFlag::NonNull, FieldFlag::Unique])),
    };

    let json = serde_json::to_string(&field).unwrap();
    assert!(json.contains("\"flags\":["));
    assert!(json.contains("\"pk\""));
    assert!(json.contains("\"nn\""));
    assert!(json.contains("\"uq\""));
}
