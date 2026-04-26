pub use crate::{
    e2e_common::{ensure_schema_exists, postgres_error_text, unique_name},
    e2e_ddl_common::DdlTestEnv,
};

/// Early-return from a DDL test when pgrx prerequisites are not met.
/// Usage: `let env = require_ddl_env!();`
macro_rules! require_ddl_env {
    () => {
        match crate::e2e_ddl_common::DdlTestEnv::global().await {
            Some(env) => env,
            None => return, // prerequisites not met — test is skipped
        }
    };
}
pub(crate) use require_ddl_env;

pub async fn pg_kalam_exec(pg: &tokio_postgres::Client, sql: &str) -> String {
    let row = pg.query_one("SELECT kalam_exec($1)", &[&sql]).await.expect("SELECT kalam_exec");
    row.get(0)
}
