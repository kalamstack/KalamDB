use crate::models::NotificationRow;
use crate::repository::{RepositoryModel, SharedTableRepository};

pub type NotificationsRepository = SharedTableRepository<NotificationRow>;

impl RepositoryModel for NotificationRow {
    fn repository_table_id() -> kalamdb_commons::models::TableId {
        Self::table_id()
    }

    fn primary_key(&self) -> &str {
        &self.id
    }
}
