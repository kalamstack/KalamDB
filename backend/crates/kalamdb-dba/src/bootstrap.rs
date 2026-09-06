use std::sync::Arc;

use kalamdb_commons::{
    models::{NamespaceId, TableId, TableName},
    schemas::TableDefinition,
    PolicyCommand, PolicyId, PolicyTarget, TablePolicy,
};
use kalamdb_core::{
    app_context::AppContext,
    error::KalamDbError,
    rls::{PolicyCompiler, SchemaPolicyTableResolver},
};
use kalamdb_system::Namespace;

use crate::{
    error::Result,
    models::{bootstrap_table_definitions, DBA_NAMESPACE},
};

pub fn initialize_dba_namespace(app_context: Arc<AppContext>) -> Result<()> {
    ensure_namespace_exists(app_context.as_ref())?;
    remove_retired_stats_table(app_context.as_ref())?;

    for mut table_def in bootstrap_table_definitions()? {
        app_context.system_columns_service().add_system_columns(&mut table_def)?;
        ensure_table_exists(app_context.as_ref(), table_def)?;
    }

    Ok(())
}

fn remove_retired_stats_table(app_context: &AppContext) -> Result<()> {
    let table_id = TableId::new(NamespaceId::new(DBA_NAMESPACE), TableName::new("stats"));
    if app_context.system_tables().tables().get_table_by_id(&table_id)?.is_some() {
        app_context.system_tables().tables().delete_table(&table_id)?;
        app_context.schema_registry().invalidate_all_versions(&table_id);
        log::info!("Removed retired DBA stats table metadata");
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

fn ensure_table_exists(app_context: &AppContext, table_def: TableDefinition) -> Result<()> {
    let table_id = TableId::new(table_def.namespace_id.clone(), table_def.table_name.clone());
    let tables_provider = app_context.system_tables().tables();
    let schema_registry = app_context.schema_registry();

    match tables_provider.get_table_by_id(&table_id)? {
        Some(existing) => {
            if !existing.semantically_equal(&table_def) {
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

pub async fn ensure_dba_notification_policies(app_context: Arc<AppContext>) -> Result<()> {
    let table_id = TableId::new(NamespaceId::new(DBA_NAMESPACE), TableName::new("notifications"));
    let policy_id = PolicyId::new(table_id.clone(), "dba_notifications_select")
        .map_err(KalamDbError::InvalidOperation)?;
    if app_context
        .system_tables()
        .table_policies()
        .get_policy(&policy_id)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let table = app_context.schema_registry().get_table_if_exists(&table_id)?.ok_or_else(|| {
        KalamDbError::InvalidOperation("dba.notifications was not bootstrapped".to_string())
    })?;
    let program =
        PolicyCompiler::new(SchemaPolicyTableResolver::new(app_context.schema_registry()))
            .compile(&table, "true")
            .map_err(KalamDbError::InvalidOperation)?;
    let policy = TablePolicy::new(
        policy_id,
        table_id,
        "dba_notifications_select",
        PolicyCommand::Select,
        vec![PolicyTarget::Public],
        Some("true".to_string()),
        None,
        Some(program),
        None,
        0,
        u64::from(table.schema_version),
    );
    app_context.system_tables().table_policies().ensure_policy(policy).await?;
    Ok(())
}
