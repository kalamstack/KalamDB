use crate::error::{DbaError, Result};
use crate::mapping::model_to_row;
use crate::repository::{current_definition, RepositoryModel};
use kalamdb_core::app_context::AppContext;
use kalamdb_core::providers::base::find_row_by_pk;
use kalamdb_core::providers::SharedTableProvider;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone)]
pub struct SharedTableRepository<M> {
    app_context: Arc<AppContext>,
    _marker: PhantomData<M>,
}

impl<M: RepositoryModel> SharedTableRepository<M> {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self {
            app_context,
            _marker: PhantomData,
        }
    }

    pub async fn insert(&self, model: M) -> Result<()> {
        self.insert_many(vec![model]).await
    }

    pub async fn insert_many(&self, models: Vec<M>) -> Result<()> {
        if models.is_empty() {
            return Ok(());
        }

        let table_def = current_definition::<M>(self.app_context.as_ref())?;
        let rows = models
            .iter()
            .map(|model| model_to_row(model, table_def.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        self.app_context
            .applier()
            .insert_shared_data(M::repository_table_id(), rows)
            .await?;
        Ok(())
    }

    pub async fn update(&self, model: M) -> Result<()> {
        let primary_key = model.primary_key().to_string();
        let table_def = current_definition::<M>(self.app_context.as_ref())?;
        let row = model_to_row(&model, table_def.as_ref())?;
        self.app_context
            .applier()
            .update_shared_data(M::repository_table_id(), vec![row], Some(primary_key))
            .await?;
        Ok(())
    }

    pub async fn upsert(&self, model: M) -> Result<()> {
        if self.exists(model.primary_key()).await? {
            self.update(model).await
        } else {
            self.insert(model).await
        }
    }

    pub async fn delete(&self, primary_key: &str) -> Result<()> {
        self.app_context
            .applier()
            .delete_shared_data(M::repository_table_id(), Some(vec![primary_key.to_string()]))
            .await?;
        Ok(())
    }

    pub async fn exists(&self, primary_key: &str) -> Result<bool> {
        let table_id = M::repository_table_id();
        let provider = self
            .app_context
            .schema_registry()
            .get_provider(&table_id)
            .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))?;
        let shared_provider = provider
            .as_any()
            .downcast_ref::<SharedTableProvider>()
            .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))?;
        Ok(find_row_by_pk(shared_provider, None, primary_key).await?.is_some())
    }
}
