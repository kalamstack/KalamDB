use std::sync::Arc;

use kalamdb_commons::models::{NamespaceId, TableId};
use kalamdb_core::app_context::AppContext;
use kalamdb_system::Namespace;

use crate::{
    error::Result,
    models::{bootstrap_table_definitions, DBA_NAMESPACE},
};

pub fn initialize_dba_namespace(app_context: Arc<AppContext>) -> Result<()> {
    ensure_namespace_exists(app_context.as_ref())?;

    for mut table_def in bootstrap_table_definitions()? {
        app_context.system_columns_service().add_system_columns(&mut table_def)?;
        ensure_table_exists(app_context.as_ref(), table_def)?;
    }

    Ok(())
}

fn ensure_namespace_exists(app_context: &AppContext) -> Result<()> {
    let namespace_id = NamespaceId::new(DBA_NAMESPACE);
    if app_context.system_tables().namespaces().get_namespace(&namespace_id)?.is_none() {
        app_context
            .system_tables()
            .namespaces()
            .create_namespace(Namespace::new(DBA_NAMESPACE))?;
    }

    Ok(())
}

fn ensure_table_exists(
    app_context: &AppContext,
    table_def: kalamdb_commons::schemas::TableDefinition,
) -> Result<()> {
    let table_id = TableId::new(table_def.namespace_id.clone(), table_def.table_name.clone());
    let tables_provider = app_context.system_tables().tables();
    let schema_registry = app_context.schema_registry();

    match tables_provider.get_table_by_id(&table_id)? {
        Some(existing) => {
            if existing != table_def {
                tables_provider.update_table(&table_id, &table_def)?;
                schema_registry.put(table_def)?;
            } else if schema_registry.get_provider(&table_id).is_none() {
                schema_registry.put(existing)?;
            }
        },
        None => {
            tables_provider.create_table(&table_id, &table_def)?;
            schema_registry.put(table_def)?;
        },
    }

    Ok(())
}
