use crate::models::{ChangeEvent, KalamCellValue, RowData};

use super::{LiveRowsConfig, LiveRowsEvent};

/// Stateful reducer that materializes the current row set from change events.
#[derive(Debug, Clone, Default)]
pub struct LiveRowsMaterializer {
    rows: Vec<RowData>,
    limit: Option<usize>,
    key_columns: Vec<String>,
}

impl LiveRowsMaterializer {
    /// Create a new materializer.
    pub fn new(config: LiveRowsConfig) -> Self {
        let key_columns = config.normalized_key_columns();
        Self {
            rows: Vec::new(),
            limit: config.limit,
            key_columns,
        }
    }

    /// Borrow the current materialized rows.
    pub fn rows(&self) -> &[RowData] {
        &self.rows
    }

    /// Apply a low-level change event and emit a high-level snapshot when relevant.
    pub fn apply(&mut self, event: ChangeEvent) -> Option<LiveRowsEvent> {
        match event {
            ChangeEvent::Ack {
                subscription_id,
                total_rows,
                batch_control,
                ..
            } => {
                if total_rows == 0
                    || matches!(batch_control.status, crate::models::BatchStatus::Ready)
                {
                    return Some(LiveRowsEvent::Rows {
                        subscription_id,
                        rows: self.rows.clone(),
                    });
                }
                None
            },
            ChangeEvent::InitialDataBatch {
                subscription_id,
                rows,
                ..
            } => {
                self.rows.extend(rows);
                self.apply_limit();
                Some(LiveRowsEvent::Rows {
                    subscription_id,
                    rows: self.rows.clone(),
                })
            },
            ChangeEvent::Insert {
                subscription_id,
                rows,
            } => {
                self.upsert_rows(rows);
                Some(LiveRowsEvent::Rows {
                    subscription_id,
                    rows: self.rows.clone(),
                })
            },
            ChangeEvent::Update {
                subscription_id,
                rows,
                old_rows,
            } => {
                self.remove_rows(&old_rows);
                self.upsert_rows(rows);
                Some(LiveRowsEvent::Rows {
                    subscription_id,
                    rows: self.rows.clone(),
                })
            },
            ChangeEvent::Delete {
                subscription_id,
                old_rows,
            } => {
                self.remove_rows(&old_rows);
                Some(LiveRowsEvent::Rows {
                    subscription_id,
                    rows: self.rows.clone(),
                })
            },
            ChangeEvent::Error {
                subscription_id,
                code,
                message,
            } => Some(LiveRowsEvent::Error {
                subscription_id,
                code,
                message,
            }),
            ChangeEvent::Unknown { .. } => None,
        }
    }

    fn upsert_rows(&mut self, incoming: Vec<RowData>) {
        for row in incoming {
            if let Some(existing_index) = self
                .rows
                .iter()
                .position(|entry| rows_match_on_key_columns(entry, &row, &self.key_columns))
            {
                self.rows[existing_index] = row;
                continue;
            }

            self.rows.push(row);
        }

        self.apply_limit();
    }

    fn remove_rows(&mut self, incoming: &[RowData]) {
        for row in incoming {
            self.rows
                .retain(|entry| !rows_match_on_key_columns(entry, row, &self.key_columns));
        }
    }

    fn apply_limit(&mut self) {
        let Some(limit) = self.limit else {
            return;
        };

        let excess = self.rows.len().saturating_sub(limit);
        if excess > 0 {
            self.rows.drain(..excess);
        }
    }
}

fn rows_match_on_key_columns(left: &RowData, right: &RowData, key_columns: &[String]) -> bool {
    for column in key_columns {
        match (left.get(column), right.get(column)) {
            (Some(KalamCellValue(left_value)), Some(KalamCellValue(right_value)))
                if left_value == right_value => {},
            _ => return false,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{BatchControl, BatchStatus, ChangeEvent, KalamDataType, SchemaField};

    fn row(id: &str, value: &str) -> RowData {
        let mut row = RowData::new();
        row.insert("id".to_string(), KalamCellValue::text(id));
        row.insert("value".to_string(), KalamCellValue::text(value));
        row
    }

    fn batch_control(status: BatchStatus) -> BatchControl {
        BatchControl {
            batch_num: 1,
            has_more: false,
            status,
            last_seq_id: None,
            snapshot_end_seq: None,
        }
    }

    #[test]
    fn accumulates_initial_batches() {
        let mut materializer = LiveRowsMaterializer::new(LiveRowsConfig::default());

        let _ = materializer.apply(ChangeEvent::Ack {
            subscription_id: "sub-1".to_string(),
            total_rows: 2,
            batch_control: batch_control(BatchStatus::Loading),
            schema: vec![SchemaField {
                name: "id".to_string(),
                data_type: KalamDataType::Text,
                index: 0,
                flags: None,
            }],
        });

        let first = materializer
            .apply(ChangeEvent::InitialDataBatch {
                subscription_id: "sub-1".to_string(),
                rows: vec![row("1", "one")],
                batch_control: batch_control(BatchStatus::LoadingBatch),
            })
            .expect("first batch should emit");
        let second = materializer
            .apply(ChangeEvent::InitialDataBatch {
                subscription_id: "sub-1".to_string(),
                rows: vec![row("2", "two")],
                batch_control: batch_control(BatchStatus::Ready),
            })
            .expect("second batch should emit");

        match first {
            LiveRowsEvent::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            other => panic!("unexpected event: {:?}", other),
        }
        match second {
            LiveRowsEvent::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("id").and_then(KalamCellValue::as_text), Some("1"));
                assert_eq!(rows[1].get("id").and_then(KalamCellValue::as_text), Some("2"));
            },
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn applies_update_delete_and_limit() {
        let mut materializer = LiveRowsMaterializer::new(LiveRowsConfig {
            limit: Some(2),
            key_columns: None,
        });

        let _ = materializer.apply(ChangeEvent::InitialDataBatch {
            subscription_id: "sub-2".to_string(),
            rows: vec![row("1", "one")],
            batch_control: batch_control(BatchStatus::Ready),
        });
        let _ = materializer.apply(ChangeEvent::Insert {
            subscription_id: "sub-2".to_string(),
            rows: vec![row("2", "two")],
        });
        let updated = materializer
            .apply(ChangeEvent::Update {
                subscription_id: "sub-2".to_string(),
                rows: vec![row("2", "two-updated")],
                old_rows: vec![row("2", "two")],
            })
            .expect("update should emit");
        let limited = materializer
            .apply(ChangeEvent::Insert {
                subscription_id: "sub-2".to_string(),
                rows: vec![row("3", "three")],
            })
            .expect("insert should emit");
        let deleted = materializer
            .apply(ChangeEvent::Delete {
                subscription_id: "sub-2".to_string(),
                old_rows: vec![row("2", "two-updated")],
            })
            .expect("delete should emit");

        match updated {
            LiveRowsEvent::Rows { rows, .. } => {
                assert_eq!(
                    rows[1].get("value").and_then(KalamCellValue::as_text),
                    Some("two-updated")
                );
            },
            other => panic!("unexpected event: {:?}", other),
        }
        match limited {
            LiveRowsEvent::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("id").and_then(KalamCellValue::as_text), Some("2"));
                assert_eq!(rows[1].get("id").and_then(KalamCellValue::as_text), Some("3"));
            },
            other => panic!("unexpected event: {:?}", other),
        }
        match deleted {
            LiveRowsEvent::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("id").and_then(KalamCellValue::as_text), Some("3"));
            },
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn supports_custom_key_columns() {
        fn keyed_row(room_id: &str, message_id: &str, value: &str) -> RowData {
            let mut row = RowData::new();
            row.insert("room_id".to_string(), KalamCellValue::text(room_id));
            row.insert("message_id".to_string(), KalamCellValue::text(message_id));
            row.insert("value".to_string(), KalamCellValue::text(value));
            row
        }

        let mut materializer = LiveRowsMaterializer::new(LiveRowsConfig {
            limit: None,
            key_columns: Some(vec!["room_id".to_string(), "message_id".to_string()]),
        });

        let _ = materializer.apply(ChangeEvent::InitialDataBatch {
            subscription_id: "sub-3".to_string(),
            rows: vec![keyed_row("room-1", "msg-1", "hello")],
            batch_control: batch_control(BatchStatus::Ready),
        });

        let updated = materializer
            .apply(ChangeEvent::Update {
                subscription_id: "sub-3".to_string(),
                rows: vec![keyed_row("room-1", "msg-1", "updated")],
                old_rows: vec![keyed_row("room-1", "msg-1", "hello")],
            })
            .expect("update should emit");

        let deleted = materializer
            .apply(ChangeEvent::Delete {
                subscription_id: "sub-3".to_string(),
                old_rows: vec![keyed_row("room-1", "msg-1", "updated")],
            })
            .expect("delete should emit");

        match updated {
            LiveRowsEvent::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("value").and_then(KalamCellValue::as_text), Some("updated"));
            },
            other => panic!("unexpected event: {:?}", other),
        }

        match deleted {
            LiveRowsEvent::Rows { rows, .. } => assert!(rows.is_empty()),
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn emits_empty_snapshot_for_empty_ready_ack() {
        let mut materializer = LiveRowsMaterializer::new(LiveRowsConfig::default());

        let event = materializer
            .apply(ChangeEvent::Ack {
                subscription_id: "sub-3".to_string(),
                total_rows: 0,
                batch_control: batch_control(BatchStatus::Ready),
                schema: Vec::new(),
            })
            .expect("empty ack should emit snapshot");

        match event {
            LiveRowsEvent::Rows { rows, .. } => assert!(rows.is_empty()),
            other => panic!("unexpected event: {:?}", other),
        }
    }
}
