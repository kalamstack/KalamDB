//! Table handlers module

pub mod alter;
pub mod create;
pub mod describe;
pub mod drop;
pub mod show;
pub mod show_stats;

pub use alter::AlterTableHandler;
pub use create::CreateTableHandler;
pub use describe::DescribeTableHandler;
pub use drop::DropTableHandler;
pub use show::ShowTablesHandler;
pub use show_stats::ShowStatsHandler;
