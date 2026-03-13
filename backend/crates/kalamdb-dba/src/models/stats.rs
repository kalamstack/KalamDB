use crate::models::DBA_NAMESPACE;
use kalamdb_commons::datatypes::KalamDataType;
use kalamdb_commons::models::{NamespaceId, TableId, TableName};
use kalamdb_commons::schemas::{TableDefinition, TableOptions};
use kalamdb_commons::TableAccess;
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

#[table(
    name = "stats",
    namespace = "dba",
    table_type = TableType::Shared,
    access_level = TableAccess::Dba,
    comment = "Bootstrap-managed DBA time-series metrics sampled from system.stats"
)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StatsRow {
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "Metric sample identifier"
    )]
    pub id: String,
    #[column(
        id = 2,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Source node identifier"
    )]
    pub node_id: String,
    #[column(
        id = 3,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Metric key captured from system.stats"
    )]
    pub metric_name: String,
    #[column(
        id = 4,
        ordinal = 4,
        data_type(KalamDataType::Double),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Numeric metric value for graphing"
    )]
    pub metric_value: f64,
    #[column(
        id = 5,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Optional metric unit such as MB, percent, or count"
    )]
    pub metric_unit: Option<String>,
    #[column(
        id = 6,
        ordinal = 6,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "When the metric sample was captured"
    )]
    pub sampled_at: i64,
}

impl StatsRow {
    pub fn sample_id(node_id: &str, metric_name: &str, sampled_at: i64) -> String {
        format!("{:013}:{}:{}", sampled_at, node_id, metric_name)
    }

    pub fn cutoff_id(cutoff_ms: i64) -> String {
        format!("{:013}:", cutoff_ms)
    }

    pub fn table_id() -> TableId {
        TableId::new(NamespaceId::new(DBA_NAMESPACE), TableName::new("stats"))
    }

    pub fn configured_definition() -> TableDefinition {
        let mut table_def = Self::definition();

        if let TableOptions::Shared(options) = &mut table_def.table_options {
            options.access_level = Some(TableAccess::Dba);
        }

        table_def
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_sample_id_sorts_by_timestamp() {
        let older = StatsRow::sample_id("node-a", "memory_usage_mb", 1_700_000_000_000);
        let newer = StatsRow::sample_id("node-a", "memory_usage_mb", 1_700_000_000_100);

        assert!(older < newer, "sample ids should keep lexicographic timestamp order");
        assert!(older.starts_with("1700000000000:"));
    }

    #[test]
    fn configured_definition_keeps_stats_hot_only() {
        let definition = StatsRow::configured_definition();
        let TableOptions::Shared(options) = definition.table_options else {
            panic!("dba.stats must be a shared table");
        };

        assert_eq!(options.access_level, Some(TableAccess::Dba));
        assert!(
            options.flush_policy.is_none(),
            "dba.stats should remain hot-only and avoid cold-storage flush scheduling"
        );
    }
}
