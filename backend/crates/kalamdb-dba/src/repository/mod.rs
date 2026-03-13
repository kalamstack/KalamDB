mod notifications_repository;
mod shared_repository;
mod stats_repository;

use kalamdb_commons::models::TableId;
use kalamdb_commons::schemas::TableDefinition;
use kalamdb_core::app_context::AppContext;
use serde::Serialize;
use std::sync::Arc;

pub use notifications_repository::NotificationsRepository;
pub use shared_repository::SharedTableRepository;
pub use stats_repository::StatsRepository;

use crate::error::{DbaError, Result};

pub trait RepositoryModel: Clone + Serialize + Send + Sync + 'static {
    fn repository_table_id() -> TableId;
    fn primary_key(&self) -> &str;
}

fn current_definition<M: RepositoryModel>(
    app_context: &AppContext,
) -> Result<Arc<TableDefinition>> {
    let table_id = M::repository_table_id();
    app_context
        .schema_registry()
        .get_table_if_exists(&table_id)?
        .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))
}

#[derive(Clone)]
pub struct DbaRegistry {
    notifications: NotificationsRepository,
    stats: StatsRepository,
}

impl DbaRegistry {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self {
            notifications: NotificationsRepository::new(app_context.clone()),
            stats: StatsRepository::new(app_context),
        }
    }

    pub fn notifications(&self) -> &NotificationsRepository {
        &self.notifications
    }

    pub fn stats(&self) -> &StatsRepository {
        &self.stats
    }
}
