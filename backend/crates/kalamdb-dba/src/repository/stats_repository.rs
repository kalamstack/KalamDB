use crate::models::StatsRow;
use crate::repository::{RepositoryModel, SharedTableRepository};

pub type StatsRepository = SharedTableRepository<StatsRow>;

impl RepositoryModel for StatsRow {
    fn repository_table_id() -> kalamdb_commons::models::TableId {
        Self::table_id()
    }

    fn primary_key(&self) -> &str {
        &self.id
    }
}
