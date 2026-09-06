use kalamdb_commons::{ids::StreamTableRowId, models::StreamTableRow};
use serde::{Deserialize, Serialize};

/// Log record stored in memory and used by the stream table store API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StreamLogRecord {
    Put {
        row_id: StreamTableRowId,
        row:    StreamTableRow,
    },
    Delete {
        row_id: StreamTableRowId,
    },
}

/// On-disk stream log record. Put payloads are ordinal KOBJ row bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PersistedStreamLogRecord {
    Put {
        row_id:  StreamTableRowId,
        payload: Vec<u8>,
    },
    Delete {
        row_id: StreamTableRowId,
    },
}
