// Aggregator for CLI tests to ensure Cargo picks them up
//
// Run these tests with:
//   cargo test --test cli
//
// Run individual test files:
//   cargo test --test cli test_cli
//   cargo test --test cli test_cli_auth
//   cargo test --test cli test_cli_auth_admin

mod common;

#[path = "cli/test_cli.rs"]
mod test_cli;

#[path = "cli/test_cli_auth.rs"]
mod test_cli_auth;

#[path = "cli/test_cli_auth_admin.rs"]
mod test_cli_auth_admin;

#[path = "cli/test_cli_doc_matrix.rs"]
mod test_cli_doc_matrix;
