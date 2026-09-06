//! Shared workflow module surface for project lifecycle commands.

pub(crate) mod agent;
pub(crate) mod auth;
pub mod db;
pub mod deploy;
pub mod dev;
pub(crate) mod display;
pub mod functions;
pub(crate) mod io;
pub mod migration;
pub mod project;
pub mod prompts;
pub mod schema;
pub(crate) mod sql;

#[cfg(test)]
pub(crate) mod test_support;

use std::path::PathBuf;

pub use db::reset::DbResetOptions;
pub(crate) use io::display_project_path;

use crate::{
    config::{CLIConfiguration, WorkflowLoggingPolicy},
    error::{CLIError, Result},
    output::{WorkflowDisplayMode, WorkflowOutput},
    workflow::{
        db::{
            migrate::apply_migrations_for_db_command,
            reset::{reset_local_dev_server_data, reset_remote_namespace_if_ready},
        },
        migration::{
            apply::{
                load_server_migration_state, save_server_migration_record,
                save_server_migration_records,
            },
            create::{create_migration, CreateMigrationOptions},
            seal_draft_migration,
            status::migration_status,
            MigrationStatus,
        },
        project::{
            config::KalamProjectConfig,
            init::{run_init, InitOptions},
            link::{link_environment, LinkOptions},
            resolve::{resolve_environment, EnvironmentOverrides, ResolvedEnvironment},
            status::show_status,
        },
        schema::{
            gen::{generate_schema_artifacts, validate_language_filter, GenerateOptions},
            load::pull_remote_schema,
        },
        sql::build_workflow_client,
    },
};

/// Shared workflow context for command handlers.
#[derive(Debug, Clone)]
pub struct WorkflowContext {
    pub project_root:       PathBuf,
    pub config:             KalamProjectConfig,
    pub cli_config:         CLIConfiguration,
    pub use_color:          bool,
    pub animations:         bool,
    pub agent:              bool,
    pub json:               bool,
    pub project_dir:        Option<PathBuf>,
    pub env_override:       Option<String>,
    pub namespace_override: Option<String>,
    pub url_override:       Option<String>,
}

impl WorkflowContext {
    pub fn discover(
        start: &std::path::Path,
        project_dir: Option<&std::path::Path>,
        cli_config: &CLIConfiguration,
        use_color: bool,
        env_override: Option<String>,
        namespace_override: Option<String>,
        url_override: Option<String>,
    ) -> Result<Self> {
        let (project_root, config) = KalamProjectConfig::discover(start, project_dir)?;
        Ok(Self {
            project_root,
            config,
            cli_config: cli_config.clone(),
            use_color,
            animations: true,
            agent: false,
            json: false,
            project_dir: project_dir.map(PathBuf::from),
            env_override,
            namespace_override,
            url_override,
        })
    }

    pub fn output(&self) -> WorkflowOutput {
        let logging = WorkflowLoggingPolicy::merge_global(
            &self.project_root,
            self.config.workflow_log_path(&self.project_root),
            &self.config.logging,
            self.cli_config.workflow_logging.as_ref(),
        );
        let display_mode = if self.agent {
            WorkflowDisplayMode::Agent
        } else {
            WorkflowDisplayMode::Normal
        };
        WorkflowOutput::new(self.use_color && !self.agent, logging)
            .with_animations(self.animations && !self.agent)
            .with_display_mode(display_mode)
            .with_json(self.json)
    }

    pub fn resolved_environment(&self) -> Result<ResolvedEnvironment> {
        resolve_environment(
            &self.config,
            &EnvironmentOverrides {
                env:       self.env_override.as_deref(),
                url:       self.url_override.as_deref(),
                namespace: self.namespace_override.as_deref(),
            },
        )
    }
}

pub async fn init_project(
    options: InitOptions,
    use_color: bool,
    animations: bool,
    json: bool,
) -> Result<()> {
    let output = WorkflowOutput::new(use_color, WorkflowLoggingPolicy::disabled())
        .with_animations(animations)
        .with_json(json);
    run_init(options, &output).await
}

pub fn generate_schema(ctx: &WorkflowContext, languages: Option<Vec<String>>) -> Result<()> {
    let output = ctx.output();
    if let Some(requested) = languages.as_ref() {
        validate_language_filter(requested, &ctx.config.schema.languages)?;
    }
    generate_schema_artifacts(ctx, &GenerateOptions { languages }, &output)
}

pub fn pull_schema(ctx: &WorkflowContext) -> Result<()> {
    let output = ctx.output();
    let env = ctx.resolved_environment()?;
    let snapshot = {
        let _spinner = output.status_spinner(format!(
            "pulling schema from {} namespace {}",
            env.url,
            env.namespace.as_str()
        ));
        pull_remote_schema(&ctx.project_root, &ctx.config, &env.url, &env.namespace)?
    };

    if let Some(path) = ctx.config.schema_source_path(&ctx.project_root) {
        let sql = render_snapshot_as_sql(&snapshot);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, sql)?;
        output.status(format!(
            "pulled schema into {}",
            display_project_path(&ctx.project_root, &path)
        ));
    }

    Ok(())
}

fn render_snapshot_as_sql(snapshot: &crate::workflow::schema::SchemaSnapshot) -> String {
    let mut out = String::from("-- Pulled from remote schema\n");
    for table in snapshot.tables.values() {
        out.push_str(&format!("\nCREATE TABLE {} (\n", table.name));
        for (idx, column) in table.columns.iter().enumerate() {
            let comma = if idx + 1 < table.columns.len() {
                ","
            } else {
                ""
            };
            let nullability = if column.nullable { "" } else { " NOT NULL" };
            let pk = if column.primary_key {
                " PRIMARY KEY"
            } else {
                ""
            };
            out.push_str(&format!(
                "  {} {}{}{}{}\n",
                column.name, column.sql_type, nullability, pk, comma
            ));
        }
        out.push_str(");\n");
    }
    out
}

pub fn create_project_migration(ctx: &WorkflowContext, name: String) -> Result<()> {
    let output = ctx.output();
    create_migration(&ctx.project_root, &ctx.config, &CreateMigrationOptions { name }, &output)
}

pub async fn show_migration_status(ctx: &WorkflowContext) -> Result<()> {
    let output = ctx.output();
    migration_status(ctx, &output).await
}

pub fn seal_project_migration(ctx: &WorkflowContext) -> Result<()> {
    let output = ctx.output();
    match seal_draft_migration(&ctx.project_root, &ctx.config, &output)? {
        Some(_) => Ok(()),
        None => {
            output.status("no draft migration to seal");
            Ok(())
        },
    }
}

pub async fn retry_project_migration(ctx: &WorkflowContext, migration_id: String) -> Result<()> {
    let output = ctx.output();
    let environment = ctx.resolved_environment()?;
    let client = build_workflow_client(ctx, &environment)?;
    let mut state = load_server_migration_state(&client, &environment.namespace).await?;
    let record = state.record(&migration_id).cloned().ok_or_else(|| {
        CLIError::ConfigurationError(format!("migration record not found: {migration_id}"))
    })?;
    if record.status != MigrationStatus::Failed {
        return Err(CLIError::ConfigurationError(format!(
            "migration {migration_id} is not failed"
        )));
    }
    state.upsert_applying(
        &record.migration_id,
        &record.namespace,
        record.sql.as_deref().unwrap_or_default(),
        record.source.as_deref().unwrap_or(&record.migration_id),
    );
    save_server_migration_record(&client, state.record(&migration_id).unwrap(), true).await?;
    output.status(format!("queued migration {migration_id} for retry"));
    Ok(())
}

pub async fn repair_project_migration_mark_applied(
    ctx: &WorkflowContext,
    migration_id: String,
) -> Result<()> {
    let output = ctx.output();
    let environment = ctx.resolved_environment()?;
    let client = build_workflow_client(ctx, &environment)?;
    let mut state = load_server_migration_state(&client, &environment.namespace).await?;
    if state.records_for_migration_id(&migration_id).is_empty() {
        return Err(CLIError::ConfigurationError(format!(
            "migration record not found: {migration_id}"
        )));
    }
    state.mark_applied(&migration_id);
    let records: Vec<_> = state.records_for_migration_id(&migration_id);
    save_server_migration_records(&client, &records).await?;
    state = load_server_migration_state(&client, &environment.namespace).await?;
    if state.has_failed_migration_id(&migration_id) || !state.is_applied(&migration_id) {
        return Err(CLIError::ConfigurationError(format!(
            "migration {migration_id} is still not applied on the server"
        )));
    }
    output.status(format!("marked migration {migration_id} as applied"));
    Ok(())
}

pub async fn reset_database(ctx: &WorkflowContext, options: DbResetOptions) -> Result<()> {
    let output = ctx.output();
    let had_local_server_data = ctx.config.local_server_dir(&ctx.project_root).exists();
    reset_local_dev_server_data(ctx, &output)?;
    reset_remote_namespace_if_ready(ctx, &output, had_local_server_data, options.assume_yes)
        .await?;
    Ok(())
}

pub async fn migrate_database(ctx: &WorkflowContext) -> Result<()> {
    let output = ctx.output();
    apply_migrations_for_db_command(ctx, &output).await
}

pub async fn start_dev(ctx: &WorkflowContext, force: bool) -> Result<()> {
    dev::session::start_background_session(ctx, force).await
}

pub async fn dev_session_status(ctx: &WorkflowContext) -> Result<()> {
    dev::session::show_background_status(ctx).await
}

pub async fn dev_session_logs(ctx: &WorkflowContext, follow: bool, lines: usize) -> Result<()> {
    dev::session::print_background_logs(ctx, follow, lines).await
}

pub async fn stop_dev(ctx: &WorkflowContext) -> Result<()> {
    dev::session::stop_background_session(ctx).await
}

pub async fn run_dev(
    ctx: &WorkflowContext,
    force: bool,
    display_mode: WorkflowDisplayMode,
) -> Result<()> {
    let display_mode = if ctx.agent {
        WorkflowDisplayMode::Agent
    } else {
        display_mode
    };
    dev::run_dev_session(
        ctx,
        dev::DevSessionOptions {
            force,
            display_mode,
            agent: ctx.agent,
        },
    )
    .await
}

pub fn link_project(ctx: &WorkflowContext, options: LinkOptions) -> Result<()> {
    let output = ctx.output();
    link_environment(ctx, &options, &output)
}

pub async fn project_status(ctx: &WorkflowContext) -> Result<()> {
    let output = ctx.output();
    show_status(ctx, &output).await
}

pub async fn deploy_project(
    ctx: &WorkflowContext,
    env: Option<String>,
    dry_run: bool,
) -> Result<()> {
    deploy::run_deploy(ctx, &deploy::DeployOptions { env, dry_run }).await
}

pub fn not_implemented(command: &str) -> Result<()> {
    Err(CLIError::ConfigurationError(format!(
        "{command} is not implemented yet; see `kalam {command} --help`"
    )))
}
