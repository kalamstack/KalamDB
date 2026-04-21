// pg/tests/e2e_ddl.rs
//
// End-to-end tests for DDL propagation: CREATE TABLE ... USING kalamdb,
// ALTER FOREIGN TABLE, and DROP FOREIGN TABLE should automatically propagate
// to KalamDB via the ProcessUtility hook.
//
// Prerequisites:
//   1. KalamDB server running:          cd backend && cargo run
//   2. pgrx PG16 set up:               pg/scripts/pgrx-test-setup.sh
//
// Run:
//   cargo nextest run --features e2e -p kalam-pg-extension -E 'test(e2e_ddl)'

#![cfg(feature = "e2e")]

mod e2e_common;
mod e2e_ddl_common;

#[path = "e2e_ddl/mod.rs"]
mod ddl_cases;
