//! FDW core implementation shared across execution modes.

mod delete_input;
mod delete_plan;
mod insert_input;
mod insert_plan;
mod request_planner;
mod scan_input;
mod scan_plan;
mod server_options;
mod table_options;
mod update_input;
mod update_plan;
mod virtual_column;

#[cfg(feature = "import-foreign-schema")]
mod import_foreign_schema;

pub use delete_input::DeleteInput;
pub use delete_plan::DeletePlan;
pub use insert_input::InsertInput;
pub use insert_plan::InsertPlan;
pub use request_planner::RequestPlanner;
pub use scan_input::ScanInput;
pub use scan_plan::ScanPlan;
pub use server_options::ServerOptions;
pub use table_options::TableOptions;
pub use update_input::UpdateInput;
pub use update_plan::UpdatePlan;
pub use virtual_column::VirtualColumn;

#[cfg(feature = "import-foreign-schema")]
pub use import_foreign_schema::create_foreign_table_sql;
