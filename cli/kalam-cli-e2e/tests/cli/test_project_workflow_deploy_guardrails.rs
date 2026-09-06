use std::fs;

use tempfile::TempDir;

use crate::common::*;

#[test]
fn test_project_workflow_deploy_blocks_prod_without_committed_migration() {
    let temp = TempDir::new().expect("temp dir");
    let project_dir = temp.path().join("guardrails-app");
    fs::create_dir_all(&project_dir).expect("create project dir");

    let mut init_cmd = create_cli_command();
    init_cmd.current_dir(&project_dir).args([
        "init",
        "--yes",
        "--name",
        "guardrails-app",
        "--schema-mode",
        "sql",
        "--languages",
        "typescript",
    ]);
    assert!(init_cmd.output().expect("init").status.success());

    let mut link_cmd = create_cli_command();
    link_cmd.current_dir(&project_dir).args([
        "link",
        "--env",
        "prod",
        "--url",
        "https://db.example.com",
        "--namespace",
        "prod_app",
    ]);
    assert!(link_cmd.output().expect("link").status.success());

    // Establish baseline then modify schema without creating a migration.
    fs::copy(project_dir.join("schema.sql"), project_dir.join("kalam/.schema-baseline.sql"))
        .expect("copy baseline");
    fs::write(
        project_dir.join("schema.sql"),
        "CREATE TABLE users (id INT);\nCREATE TABLE audit_log (id INT);\n",
    )
    .expect("modify schema");

    let mut deploy_cmd = create_cli_command();
    deploy_cmd.current_dir(&project_dir).args(["deploy", "--env", "prod"]);
    let deploy_output = deploy_cmd.output().expect("deploy");
    let stderr = String::from_utf8_lossy(&deploy_output.stderr);
    assert!(
        !deploy_output.status.success(),
        "prod deploy should fail when schema changes are not committed as a migration\nstderr: \
         {stderr}"
    );
    assert!(
        stderr.contains("deploy blocked") && stderr.contains("committed migration"),
        "expected production migration guardrail\nstderr: {stderr}"
    );
}
