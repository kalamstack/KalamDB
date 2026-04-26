use kalamdb_commons::{ids::StreamTableRowId, models::StreamTableRow};
use serde::{Deserialize, Serialize};

/// Log record stored in the commit log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StreamLogRecord {
    Put {
        row_id: StreamTableRowId,
        row: StreamTableRow,
    },
    Delete {
        row_id: StreamTableRowId,
    },
}
