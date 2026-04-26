use std::path::PathBuf;

use kalamdb_commons::models::TableId;
use kalamdb_sharding::ShardRouter;

use crate::time_bucket::StreamTimeBucket;

/// Stream log configuration.
#[derive(Debug, Clone)]
pub struct StreamLogConfig {
    pub base_dir: PathBuf,
    pub shard_router: ShardRouter,
    pub bucket: StreamTimeBucket,
    pub table_id: TableId,
}
