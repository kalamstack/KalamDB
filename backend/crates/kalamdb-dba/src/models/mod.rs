mod notification;
mod stats;

use crate::error::Result;
use kalamdb_commons::schemas::TableDefinition;

pub use notification::NotificationRow;
pub use stats::StatsRow;

pub const DBA_NAMESPACE: &str = "dba";

pub fn bootstrap_table_definitions() -> Result<Vec<TableDefinition>> {
    Ok(vec![
        NotificationRow::definition(),
        StatsRow::configured_definition(),
    ])
}
