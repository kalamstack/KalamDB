use kalam_pg_common::KalamPgError;
use kalam_pg_fdw::create_foreign_table_sql;
use kalamdb_commons::models::schemas::TableDefinition;
use kalamdb_commons::models::NamespaceId;
use kalamdb_commons::{TableId, TableType};
use kalamdb_core::app_context::AppContext;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Request for generating `IMPORT FOREIGN SCHEMA` SQL from an embedded Kalam runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportForeignSchemaRequest {
    pub server_name: String,
    pub foreign_schema: String,
    pub source_namespace: Option<NamespaceId>,
    pub included_table_types: Option<Vec<TableType>>,
    pub excluded_tables: HashSet<TableId>,
}

impl ImportForeignSchemaRequest {
    /// Create a request targeting a PostgreSQL foreign schema and server name.
    pub fn new(server_name: impl Into<String>, foreign_schema: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            foreign_schema: foreign_schema.into(),
            source_namespace: None,
            included_table_types: None,
            excluded_tables: HashSet::new(),
        }
    }

    fn includes_table_type(&self, table_type: TableType) -> bool {
        self.included_table_types
            .as_ref()
            .map(|types| types.contains(&table_type))
            .unwrap_or_else(|| {
                matches!(table_type, TableType::User | TableType::Shared | TableType::Stream)
            })
    }

    fn includes_table(&self, table_definition: &TableDefinition) -> bool {
        if table_definition.table_type == TableType::System {
            return false;
        }

        if let Some(namespace) = &self.source_namespace {
            if &table_definition.namespace_id != namespace {
                return false;
            }
        }

        if !self.includes_table_type(table_definition.table_type) {
            return false;
        }

        let table_id = TableId::new(
            table_definition.namespace_id.clone(),
            table_definition.table_name.clone(),
        );
        !self.excluded_tables.contains(&table_id)
    }
}

/// Embedded-mode import-schema service backed by the Kalam schema registry.
pub struct ImportForeignSchemaService {
    app_context: Arc<AppContext>,
}

impl ImportForeignSchemaService {
    /// Create the service from an existing embedded AppContext.
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    /// Generate foreign-table SQL statements for the requested namespace/table filters.
    pub fn generate_sql(
        &self,
        request: &ImportForeignSchemaRequest,
    ) -> Result<Vec<String>, KalamPgError> {
        let persisted_table_definitions = self
            .app_context
            .schema_registry()
            .scan_all_table_definitions()
            .map_err(|err| KalamPgError::Execution(err.to_string()))?;
        let cached_table_definitions =
            self.app_context.schema_registry().cached_table_definitions();

        let mut table_definitions_by_id = HashMap::new();
        for table_definition in
            persisted_table_definitions.into_iter().chain(cached_table_definitions)
        {
            let table_id = TableId::new(
                table_definition.namespace_id.clone(),
                table_definition.table_name.clone(),
            );
            table_definitions_by_id.insert(table_id, table_definition);
        }

        let mut table_definitions: Vec<TableDefinition> =
            table_definitions_by_id.into_values().collect();

        table_definitions.sort_by(|left, right| {
            (
                left.namespace_id.as_str(),
                left.table_name.as_str(),
                table_type_rank(left.table_type),
            )
                .cmp(&(
                    right.namespace_id.as_str(),
                    right.table_name.as_str(),
                    table_type_rank(right.table_type),
                ))
        });

        table_definitions
            .into_iter()
            .filter(|table_definition| request.includes_table(table_definition))
            .map(|table_definition| {
                create_foreign_table_sql(
                    &request.server_name,
                    &request.foreign_schema,
                    &table_definition,
                )
            })
            .collect()
    }
}

fn table_type_rank(table_type: TableType) -> u8 {
    match table_type {
        TableType::User => 0,
        TableType::Shared => 1,
        TableType::Stream => 2,
        TableType::System => 3,
    }
}
