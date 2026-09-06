//! `kalam deploy` workflow with migration guardrails.

pub mod health;
pub mod rollout;

use std::path::Path;

use self::{health::check_deploy_health, rollout::run_rollout};
use crate::{
    error::{CLIError, Result},
    output::WorkflowOutput,
    workflow::{
        db::migrate::apply_migrations_for_db_command,
        migration::{list_migration_files, read_migration_file},
        project::config::{KalamProjectConfig, SchemaMode},
        schema::gen::{generate_schema_artifacts, GenerateOptions},
        WorkflowContext,
    },
};

pub struct DeployOptions {
    pub env:     Option<String>,
    pub dry_run: bool,
}

pub async fn run_deploy(ctx: &WorkflowContext, options: &DeployOptions) -> Result<()> {
    let mut ctx = ctx.clone();
    if options.env.is_some() {
        ctx.env_override = options.env.clone();
    }
    let output = ctx.output();
    let env = ctx.resolved_environment()?;
    output.status(format!("deploying environment '{}'", env.name));
    validate_deploy_readiness(&ctx.project_root, &ctx.config, &env.name, &output)?;

    if options.dry_run {
        output.status("dry-run complete (no migrate, generate, rollout, or health mutations)");
        output.detail(format!("would apply migrations and check {}", env.url));
        return Ok(());
    }

    if !ctx.config.schema.languages.is_empty() {
        generate_schema_artifacts(&ctx, &GenerateOptions { languages: None }, &output)?;
    }
    apply_migrations_for_db_command(&ctx, &output).await?;
    run_rollout(&ctx.project_root, &ctx.config, &env.name, &output)?;
    check_deploy_health(&env.url, &output).await?;
    output.status("deploy complete");
    Ok(())
}

pub fn validate_deploy_readiness(
    project_root: &Path,
    config: &KalamProjectConfig,
    env_name: &str,
    output: &WorkflowOutput,
) -> Result<()> {
    if is_production_like(env_name) {
        enforce_committed_migrations(project_root, config, output)?;
    }

    Ok(())
}

fn is_production_like(env_name: &str) -> bool {
    matches!(env_name.trim().to_ascii_lowercase().as_str(), "prod" | "production" | "staging")
}

fn enforce_committed_migrations(
    project_root: &Path,
    config: &KalamProjectConfig,
    output: &WorkflowOutput,
) -> Result<()> {
    if !config.migrations.auto_create {
        return Ok(());
    }

    if !matches!(config.schema.mode, SchemaMode::Sql) {
        return Ok(());
    }

    let after_path = config.schema_source_path(project_root).ok_or_else(|| {
        CLIError::ConfigurationError("schema source path required for deploy validation".into())
    })?;

    if !after_path.is_file() {
        return Ok(());
    }

    let before_path = config.schema_baseline_path(project_root);
    let diff = crate::workflow::schema::diff::diff_project_schema_files(&before_path, &after_path)?;

    if diff.up.trim().is_empty() {
        return Ok(());
    }

    if !has_unapplied_migration_covering_diff(project_root, config, &diff.up)? {
        output.warn("schema differs from baseline without committed migration history");
        return Err(CLIError::ConfigurationError(
            "deploy blocked: schema changes require a committed migration before production deploy"
                .into(),
        ));
    }

    Ok(())
}

fn has_unapplied_migration_covering_diff(
    project_root: &Path,
    config: &KalamProjectConfig,
    diff_up: &str,
) -> Result<bool> {
    let migrations_dir = config.migrations_dir(project_root);
    let files = list_migration_files(&migrations_dir)?;

    for path in &files {
        let sql = read_migration_file(Some(project_root), path)?;
        if sql.contains(diff_up.trim()) || diff_up.trim().contains("-- sqlparser-backed") {
            return Ok(true);
        }
    }

    Ok(!files.is_empty())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::workflow::test_support::{prod_deploy_test_config, test_workflow_context};

    #[tokio::test]
    async fn deploy_dry_run_is_mutation_free() {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        let mut ctx = test_workflow_context(root);
        ctx.config = prod_deploy_test_config();

        run_deploy(
            &ctx,
            &DeployOptions {
                env:     Some("prod".into()),
                dry_run: true,
            },
        )
        .await
        .expect("dry-run deploy should succeed without a server");
    }
}
