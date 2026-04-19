#[cfg(test)]
mod tests {
    use crate::models::*;
    use kalam_client::models::{
        BatchControl, BatchStatus, ChangeEvent, ErrorDetail, LoginResponse, LoginUserInfo,
        QueryResponse, QueryResult, ResponseStatus, SchemaField,
    };
    use kalam_client::{FieldFlag, KalamDataType};
    use std::collections::BTreeSet;

    // -----------------------------------------------------------------------
    // Auth provider conversion
    // -----------------------------------------------------------------------

    #[test]
    fn auth_basic_converts_to_native() {
        let auth = DartAuthProvider::BasicAuth {
            user: "alice".into(),
            password: "secret".into(),
        };
        // into_native() should not panic
        let _native = auth.into_native();
    }

    #[test]
    fn auth_jwt_converts_to_native() {
        let auth = DartAuthProvider::JwtToken {
            token: "eyJ...".into(),
        };
        let _native = auth.into_native();
    }

    #[test]
    fn auth_none_converts_to_native() {
        let auth = DartAuthProvider::None;
        let _native = auth.into_native();
    }

    // -----------------------------------------------------------------------
    // Schema field conversion
    // -----------------------------------------------------------------------

    #[test]
    fn schema_field_with_no_flags() {
        let sf = SchemaField {
            name: "id".into(),
            data_type: KalamDataType::Int,
            index: 0,
            flags: None,
        };
        let dart: DartSchemaField = sf.into();
        assert_eq!(dart.name, "id");
        assert_eq!(dart.index, 0);
        assert!(dart.flags.is_none());
    }

    #[test]
    fn schema_field_with_flags() {
        let mut flags = BTreeSet::new();
        flags.insert(FieldFlag::PrimaryKey);
        flags.insert(FieldFlag::NonNull);
        let sf = SchemaField {
            name: "user_id".into(),
            data_type: KalamDataType::Text,
            index: 1,
            flags: Some(flags),
        };
        let dart: DartSchemaField = sf.into();
        assert_eq!(dart.name, "user_id");
        assert_eq!(dart.index, 1);
        let fl = dart.flags.unwrap();
        assert!(fl.contains("pk"));
        assert!(fl.contains("nn"));
        assert!(!fl.contains("uq"));
    }

    #[test]
    fn schema_field_all_flags() {
        let mut flags = BTreeSet::new();
        flags.insert(FieldFlag::PrimaryKey);
        flags.insert(FieldFlag::NonNull);
        flags.insert(FieldFlag::Unique);
        let sf = SchemaField {
            name: "email".into(),
            data_type: KalamDataType::Text,
            index: 2,
            flags: Some(flags),
        };
        let dart: DartSchemaField = sf.into();
        let fl = dart.flags.unwrap();
        // BTreeSet is sorted by enum variant order: PrimaryKey < NonNull < Unique
        assert_eq!(fl, "pk,nn,uq");
    }

    // -----------------------------------------------------------------------
    // Error detail conversion
    // -----------------------------------------------------------------------

    #[test]
    fn error_detail_converts() {
        let e = ErrorDetail {
            code: "table_not_found".into(),
            message: "Table 'x' does not exist".into(),
            details: Some("namespace: default".into()),
        };
        let dart: DartErrorDetail = e.into();
        assert_eq!(dart.code, "table_not_found");
        assert_eq!(dart.message, "Table 'x' does not exist");
        assert_eq!(dart.details.unwrap(), "namespace: default");
    }

    #[test]
    fn error_detail_without_details() {
        let e = ErrorDetail {
            code: "syntax".into(),
            message: "Bad SQL".into(),
            details: None,
        };
        let dart: DartErrorDetail = e.into();
        assert!(dart.details.is_none());
    }

    // -----------------------------------------------------------------------
    // QueryResponse / QueryResult conversion
    // -----------------------------------------------------------------------

    #[test]
    fn query_response_success() {
        let qr = QueryResult {
            schema: vec![SchemaField {
                name: "name".into(),
                data_type: KalamDataType::Text,
                index: 0,
                flags: None,
            }],
            rows: Some(vec![vec![kalam_client::KalamCellValue::text("Alice")]]),
            named_rows: None,
            row_count: 1,
            message: None,
        };
        let resp = QueryResponse {
            status: ResponseStatus::Success,
            results: vec![qr],
            took: Some(1.5),
            error: None,
        };
        let dart: DartQueryResponse = resp.into();
        assert!(dart.success);
        assert_eq!(dart.results.len(), 1);
        assert_eq!(dart.results[0].row_count, 1);
        assert_eq!(dart.results[0].columns.len(), 1);
        assert_eq!(dart.results[0].columns[0].name, "name");
        assert_eq!(dart.results[0].rows_json.len(), 1);
        assert!(dart.results[0].rows_json[0].contains("Alice"));
        assert!((dart.took_ms.unwrap() - 1.5).abs() < f64::EPSILON);
        assert!(dart.error.is_none());
    }

    #[test]
    fn query_response_error() {
        let resp = QueryResponse {
            status: ResponseStatus::Error,
            results: vec![],
            took: None,
            error: Some(ErrorDetail {
                code: "scan_fail".into(),
                message: "IO error".into(),
                details: None,
            }),
        };
        let dart: DartQueryResponse = resp.into();
        assert!(!dart.success);
        assert!(dart.results.is_empty());
        assert!(dart.error.is_some());
        assert_eq!(dart.error.unwrap().code, "scan_fail");
    }

    #[test]
    fn query_result_empty_rows() {
        let qr = QueryResult {
            schema: vec![],
            rows: None,
            named_rows: None,
            row_count: 0,
            message: Some("Table created".into()),
        };
        let dart: DartQueryResult = qr.into();
        assert!(dart.columns.is_empty());
        assert!(dart.rows_json.is_empty());
        assert_eq!(dart.row_count, 0);
        assert_eq!(dart.message.unwrap(), "Table created");
    }

    // -----------------------------------------------------------------------
    // Login response conversion
    // -----------------------------------------------------------------------

    #[test]
    fn login_response_converts() {
        let l = LoginResponse {
            access_token: "tok123".into(),
            refresh_token: Some("ref456".into()),
            expires_at: "2026-02-25T12:00:00Z".into(),
            refresh_expires_at: Some("2026-03-25T12:00:00Z".into()),
            admin_ui_access: true,
            user: LoginUserInfo {
                id: "user-1".into(),
                role: kalam_client::Role::Dba,
                email: Some("alice@example.com".into()),
                created_at: "2026-01-01T00:00:00Z".into(),
                updated_at: "2026-02-01T00:00:00Z".into(),
            },
        };
        let dart: DartLoginResponse = l.into();
        assert_eq!(dart.access_token, "tok123");
        assert_eq!(dart.refresh_token.unwrap(), "ref456");
        assert!(dart.admin_ui_access);
        assert_eq!(dart.user.id, "user-1");
        assert!(matches!(dart.user.role, DartRole::Dba));
        assert_eq!(dart.user.email.unwrap(), "alice@example.com");
    }

    // -----------------------------------------------------------------------
    // Change event conversion
    // -----------------------------------------------------------------------

    fn make_batch_control(batch_num: u32, has_more: bool, status: BatchStatus) -> BatchControl {
        BatchControl {
            batch_num,
            has_more,
            status,
            last_seq_id: None,
            snapshot_end_seq: None,
        }
    }

    #[test]
    fn change_event_ack_converts() {
        let e = ChangeEvent::Ack {
            subscription_id: "sub-1".into(),
            total_rows: 100,
            batch_control: make_batch_control(0, true, BatchStatus::Loading),
            schema: vec![SchemaField {
                name: "id".into(),
                data_type: KalamDataType::Int,
                index: 0,
                flags: None,
            }],
        };
        let dart: DartChangeEvent = e.into();
        match dart {
            DartChangeEvent::Ack {
                subscription_id,
                total_rows,
                schema,
                batch_num,
                has_more,
                status,
            } => {
                assert_eq!(subscription_id, "sub-1");
                assert_eq!(total_rows, 100);
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0].name, "id");
                assert_eq!(batch_num, 0);
                assert!(has_more);
                assert_eq!(status, "loading");
            },
            _ => panic!("Expected Ack variant"),
        }
    }

    #[test]
    fn change_event_initial_data_batch_converts() {
        let mut row = std::collections::HashMap::new();
        row.insert("name".to_string(), kalam_client::KalamCellValue::text("Alice"));
        let e = ChangeEvent::InitialDataBatch {
            subscription_id: "sub-2".into(),
            rows: vec![row],
            batch_control: make_batch_control(1, false, BatchStatus::Ready),
        };
        let dart: DartChangeEvent = e.into();
        match dart {
            DartChangeEvent::InitialDataBatch {
                subscription_id,
                rows_json,
                batch_num,
                has_more,
                status,
            } => {
                assert_eq!(subscription_id, "sub-2");
                assert_eq!(rows_json.len(), 1);
                assert!(rows_json[0].contains("Alice"));
                assert_eq!(batch_num, 1);
                assert!(!has_more);
                assert_eq!(status, "ready");
            },
            _ => panic!("Expected InitialDataBatch variant"),
        }
    }

    #[test]
    fn change_event_insert_converts() {
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), kalam_client::KalamCellValue::int(1));
        row.insert("name".to_string(), kalam_client::KalamCellValue::text("Bob"));
        let e = ChangeEvent::Insert {
            subscription_id: "sub-3".into(),
            rows: vec![row],
        };
        let dart: DartChangeEvent = e.into();
        match dart {
            DartChangeEvent::Insert {
                subscription_id,
                rows_json,
            } => {
                assert_eq!(subscription_id, "sub-3");
                assert_eq!(rows_json.len(), 1);
                let parsed: serde_json::Value = serde_json::from_str(&rows_json[0]).unwrap();
                assert_eq!(parsed["name"], "Bob");
            },
            _ => panic!("Expected Insert variant"),
        }
    }

    #[test]
    fn change_event_update_converts() {
        let mut new_row = std::collections::HashMap::new();
        new_row.insert("id".to_string(), kalam_client::KalamCellValue::int(1));
        new_row.insert("name".to_string(), kalam_client::KalamCellValue::text("Bob2"));
        let mut old_row = std::collections::HashMap::new();
        old_row.insert("id".to_string(), kalam_client::KalamCellValue::int(1));
        old_row.insert("name".to_string(), kalam_client::KalamCellValue::text("Bob"));
        let e = ChangeEvent::Update {
            subscription_id: "sub-4".into(),
            rows: vec![new_row],
            old_rows: vec![old_row],
        };
        let dart: DartChangeEvent = e.into();
        match dart {
            DartChangeEvent::Update {
                subscription_id,
                rows_json,
                old_rows_json,
            } => {
                assert_eq!(subscription_id, "sub-4");
                assert_eq!(rows_json.len(), 1);
                assert_eq!(old_rows_json.len(), 1);
                let new_val: serde_json::Value = serde_json::from_str(&rows_json[0]).unwrap();
                let old_val: serde_json::Value = serde_json::from_str(&old_rows_json[0]).unwrap();
                assert_eq!(new_val["name"], "Bob2");
                assert_eq!(old_val["name"], "Bob");
            },
            _ => panic!("Expected Update variant"),
        }
    }

    #[test]
    fn change_event_delete_converts() {
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), kalam_client::KalamCellValue::int(99));
        let e = ChangeEvent::Delete {
            subscription_id: "sub-5".into(),
            old_rows: vec![row],
        };
        let dart: DartChangeEvent = e.into();
        match dart {
            DartChangeEvent::Delete {
                subscription_id,
                old_rows_json,
            } => {
                assert_eq!(subscription_id, "sub-5");
                assert_eq!(old_rows_json.len(), 1);
            },
            _ => panic!("Expected Delete variant"),
        }
    }

    #[test]
    fn change_event_error_converts() {
        let e = ChangeEvent::Error {
            subscription_id: "sub-6".into(),
            code: "auth_fail".into(),
            message: "Token expired".into(),
        };
        let dart: DartChangeEvent = e.into();
        match dart {
            DartChangeEvent::Error {
                subscription_id,
                code,
                message,
            } => {
                assert_eq!(subscription_id, "sub-6");
                assert_eq!(code, "auth_fail");
                assert_eq!(message, "Token expired");
            },
            _ => panic!("Expected Error variant"),
        }
    }

    #[test]
    fn change_event_unknown_converts_to_error() {
        let e = ChangeEvent::Unknown {
            raw: serde_json::json!({"type": "weird"}),
        };
        let dart: DartChangeEvent = e.into();
        match dart {
            DartChangeEvent::Error { code, .. } => {
                assert_eq!(code, "unknown");
            },
            _ => panic!("Expected Error variant for Unknown"),
        }
    }

    // -----------------------------------------------------------------------
    // Subscription config conversion
    // -----------------------------------------------------------------------

    #[test]
    fn subscription_config_minimal() {
        let cfg = DartSubscriptionConfig {
            sql: "SELECT * FROM t".into(),
            id: None,
            batch_size: None,
            last_rows: None,
            from: None,
        };
        let native = cfg.into_native();
        assert_eq!(native.sql, "SELECT * FROM t");
        assert!(native.id.starts_with("dart-sub-"));
        assert!(native.options.as_ref().unwrap().from.is_none());
    }

    #[test]
    fn subscription_config_with_all_options() {
        let cfg = DartSubscriptionConfig {
            sql: "SELECT * FROM t".into(),
            id: Some("my-sub-1".into()),
            batch_size: Some(500),
            last_rows: Some(10),
            from: Some(42),
        };
        let native = cfg.into_native();
        assert_eq!(native.id, "my-sub-1");
        assert_eq!(native.sql, "SELECT * FROM t");
        let opts = native.options.unwrap();
        assert_eq!(opts.batch_size.unwrap(), 500);
        assert_eq!(opts.last_rows.unwrap(), 10);
        assert_eq!(opts.from.unwrap().as_i64(), 42);
    }

    #[test]
    fn subscription_config_with_from_only() {
        let cfg = DartSubscriptionConfig {
            sql: "SELECT * FROM events".into(),
            id: None,
            batch_size: None,
            last_rows: None,
            from: Some(12345),
        };
        let native = cfg.into_native();
        assert_eq!(native.sql, "SELECT * FROM events");
        let opts = native.options.unwrap();
        assert!(opts.batch_size.is_none());
        assert!(opts.last_rows.is_none());
        assert_eq!(opts.from.unwrap().as_i64(), 12345);
    }

    // -----------------------------------------------------------------------
    // DartSubscriptionInfo conversion
    // -----------------------------------------------------------------------

    #[test]
    fn subscription_info_from_native() {
        use kalam_client::models::SubscriptionInfo;
        use kalam_client::SeqId;

        let native = SubscriptionInfo {
            id: "sub-42".to_string(),
            query: "SELECT * FROM users".to_string(),
            last_seq_id: Some(SeqId::new(999)),
            last_event_time_ms: Some(1700000000000),
            created_at_ms: 1700000000000,
            closed: false,
        };
        let dart: DartSubscriptionInfo = native.into();
        assert_eq!(dart.id, "sub-42");
        assert_eq!(dart.query, "SELECT * FROM users");
        assert_eq!(dart.last_seq_id, Some(999));
        assert_eq!(dart.last_event_time_ms, Some(1700000000000));
        assert_eq!(dart.created_at_ms, 1700000000000);
        assert!(!dart.closed);
    }

    #[test]
    fn subscription_info_from_native_none_fields() {
        use kalam_client::models::SubscriptionInfo;

        let native = SubscriptionInfo {
            id: "sub-0".to_string(),
            query: "SELECT 1".to_string(),
            last_seq_id: None,
            last_event_time_ms: None,
            created_at_ms: 1700000000000,
            closed: true,
        };
        let dart: DartSubscriptionInfo = native.into();
        assert_eq!(dart.id, "sub-0");
        assert!(dart.last_seq_id.is_none());
        assert!(dart.last_event_time_ms.is_none());
        assert!(dart.closed);
    }

    // -----------------------------------------------------------------------
    // Data type formatting
    // -----------------------------------------------------------------------

    #[test]
    fn data_types_format_correctly() {
        let cases = vec![
            (KalamDataType::Int, "Int"),
            (KalamDataType::Text, "Text"),
            (KalamDataType::Boolean, "Boolean"),
            (KalamDataType::BigInt, "BigInt"),
            (KalamDataType::Double, "Double"),
            (KalamDataType::Timestamp, "Timestamp"),
            (KalamDataType::Uuid, "Uuid"),
            (KalamDataType::Json, "Json"),
        ];
        for (dt, expected) in cases {
            let sf = SchemaField {
                name: "col".into(),
                data_type: dt,
                index: 0,
                flags: None,
            };
            let dart: DartSchemaField = sf.into();
            assert_eq!(dart.data_type, expected);
        }
    }
}
