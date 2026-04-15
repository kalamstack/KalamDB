use std::any::Any;

use datafusion::catalog::Session;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::execution::context::SessionState;

use crate::query_context::TransactionQueryContext;

/// Dedicated DataFusion config extension carrying transaction-local query state.
#[derive(Debug, Clone)]
pub struct TransactionQueryExtension {
    pub context: TransactionQueryContext,
}

impl TransactionQueryExtension {
    #[inline]
    pub fn new(context: TransactionQueryContext) -> Self {
        Self { context }
    }
}

impl ExtensionOptions for TransactionQueryExtension {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, _key: &str, _value: &str) -> datafusion::common::Result<()> {
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

impl ConfigExtension for TransactionQueryExtension {
    const PREFIX: &'static str = "kalamdb_tx";
}

#[inline]
pub fn extract_transaction_query_context(
    session: &dyn Session,
) -> Option<&TransactionQueryContext> {
    session
        .as_any()
        .downcast_ref::<SessionState>()?
        .config()
        .options()
        .extensions
        .get::<TransactionQueryExtension>()
        .map(|extension| &extension.context)
}
