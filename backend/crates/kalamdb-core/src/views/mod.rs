// System schema provider wiring stays in core (depends on SchemaRegistry)
pub mod system_schema_provider;
pub use system_schema_provider::SystemSchemaProvider;

// Re-export only the remaining view module used from kalamdb-core.
pub use kalamdb_views::describe;
