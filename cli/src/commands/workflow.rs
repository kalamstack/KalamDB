use kalam_cli::{
    output::WorkflowDisplayMode,
    workflow::{
        self,
        project::{init::InitOptions, link::LinkOptions},
        WorkflowContext,
    },
    CLIConfiguration, CLIError, Result, CLI_VERSION,
};

use crate::args::{
    Cli, CliCommand, DbArgs, DbCommand, DeployArgs, DevArgs, DevCommand, FunctionsArgs,
    FunctionsCommand, InitArgs, LinkArgs, MigrationArgs, MigrationCommand, SchemaArgs,
    SchemaCommand, StatusArgs,
};

pub async fn handle_workflow_command(cli: &Cli) -> Result<bool> {
    let Some(subcommand) = &cli.subcommand else {
        return Ok(false);
    };

    match subcommand {
        CliCommand::Init(args) => {
            handle_init(cli, args).await?;
            Ok(true)
        },
        CliCommand::Link(args) => {
            handle_link(cli, args)?;
            Ok(true)
        },
        CliCommand::Schema(args) => {
            handle_schema(cli, args)?;
            Ok(true)
        },
        CliCommand::Migration(args) => {
            handle_migration(cli, args).await?;
            Ok(true)
        },
        CliCommand::Db(args) => {
            handle_db(cli, args).await?;
            Ok(true)
        },
        CliCommand::Dev(args) => {
            handle_dev(cli, args).await?;
            Ok(true)
        },
        CliCommand::Status(args) => {
            handle_status(cli, args).await?;
            Ok(true)
        },
        CliCommand::Deploy(args) => {
            handle_deploy(cli, args).await?;
            Ok(true)
        },
        CliCommand::Functions(args) => {
            handle_functions(cli, args).await?;
            Ok(true)
        },
        _ => Ok(false),
    }
}

async fn handle_init(cli: &Cli, args: &InitArgs) -> Result<()> {
    if args.list_templates {
        return list_init_templates(cli.json);
    }

    let cwd = args
        .project_dir
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));

    workflow::init_project(
        InitOptions {
            name: args.name.clone(),
            schema_mode: args.schema_mode.map(Into::into),
            languages: args.languages.clone(),
            template: args.template.clone(),
            package_manager: args.package_manager.map(Into::into),
            server_mode: args.server_mode.map(Into::into),
            server_url: args.server_url.clone(),
            yes: args.yes,
            cwd,
        },
        !cli.no_color,
        !cli.no_spinner,
        cli.json,
    )
    .await
}

fn list_init_templates(json: bool) -> Result<()> {
    let templates = kalam_cli::workflow::project::init::list_init_templates();
    let payload = serde_json::json!({
        "ok": true,
        "cli_version": CLI_VERSION,
        "default_template": "simple-live",
        "next": "kalam init --yes --template <id> --languages typescript --package-manager npm && kalam dev start --agent",
        "templates": templates,
    });
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&payload)
                .map_err(|error| CLIError::FormatError(error.to_string()))?
        );
        return Ok(());
    }

    println!("id\tkind\tlanguage\tdescription");
    for template in &templates {
        println!(
            "{}\t{}\t{}\t{}",
            template.id, template.kind, template.language, template.description
        );
    }
    println!();
    println!("Next: kalam init --yes --template <id> --languages typescript --package-manager npm");
    println!("Then: kalam dev start --agent");
    Ok(())
}

fn workflow_context(
    cli: &Cli,
    project_dir: Option<&std::path::Path>,
    env: Option<&str>,
    namespace: Option<&str>,
) -> Result<WorkflowContext> {
    let start = project_dir
        .map(std::path::Path::to_path_buf)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| ".".into());

    let cli_config = CLIConfiguration::load(&cli.config)?;
    let mut ctx = WorkflowContext::discover(
        &start,
        project_dir,
        &cli_config,
        !cli.no_color,
        env.map(str::to_string),
        namespace.map(str::to_string),
        cli.url.clone(),
    )?;
    ctx.animations = !cli.no_spinner && !cli.json;
    ctx.json = cli.json;
    Ok(ctx)
}

fn apply_agent_mode(ctx: &mut WorkflowContext, agent: bool) {
    if !agent {
        return;
    }
    ctx.agent = true;
    ctx.use_color = false;
    ctx.animations = false;
}

fn handle_schema(cli: &Cli, args: &SchemaArgs) -> Result<()> {
    let ctx = workflow_context(cli, args.project_dir.as_deref(), args.env.as_deref(), None)?;

    match &args.command {
        SchemaCommand::Gen(gen_args) => workflow::generate_schema(&ctx, gen_args.languages.clone()),
        SchemaCommand::Pull(_) => workflow::pull_schema(&ctx),
    }
}

async fn handle_migration(cli: &Cli, args: &MigrationArgs) -> Result<()> {
    let ctx = workflow_context(cli, args.project_dir.as_deref(), args.env.as_deref(), None)?;

    match &args.command {
        MigrationCommand::Create(create_args) => {
            workflow::create_project_migration(&ctx, create_args.name.clone())
        },
        MigrationCommand::Status(_) => workflow::show_migration_status(&ctx).await,
        MigrationCommand::Seal(_) => workflow::seal_project_migration(&ctx),
        MigrationCommand::Retry(retry_args) => {
            workflow::retry_project_migration(&ctx, retry_args.migration_id.clone()).await
        },
        MigrationCommand::Repair(repair_args) => {
            if repair_args.mark_applied {
                workflow::repair_project_migration_mark_applied(
                    &ctx,
                    repair_args.migration_id.clone(),
                )
                .await
            } else {
                Err(kalam_cli::CLIError::ConfigurationError(
                    "migration repair requires --mark-applied".into(),
                ))
            }
        },
    }
}

async fn handle_db(cli: &Cli, args: &DbArgs) -> Result<()> {
    let ctx = workflow_context(cli, args.project_dir.as_deref(), args.env.as_deref(), None)?;

    match &args.command {
        DbCommand::Migrate(_) => workflow::migrate_database(&ctx).await,
        DbCommand::Reset(args) => {
            workflow::reset_database(
                &ctx,
                workflow::DbResetOptions {
                    assume_yes: args.yes,
                },
            )
            .await
        },
    }
}

fn handle_link(cli: &Cli, args: &LinkArgs) -> Result<()> {
    let ctx = workflow_context(cli, args.project_dir.as_deref(), args.env.as_deref(), None)?;
    workflow::link_project(
        &ctx,
        LinkOptions {
            env:       args.env.clone(),
            url:       args.url.clone(),
            namespace: args.namespace.clone(),
        },
    )
}

async fn handle_dev(cli: &Cli, args: &DevArgs) -> Result<()> {
    let mut ctx = workflow_context(
        cli,
        args.project_dir.as_deref(),
        args.env.as_deref(),
        args.namespace.as_deref(),
    )?;
    apply_agent_mode(&mut ctx, args.agent);
    let display_mode = if ctx.agent {
        WorkflowDisplayMode::Agent
    } else if cli.verbose {
        WorkflowDisplayMode::Verbose
    } else {
        WorkflowDisplayMode::Normal
    };
    match &args.command {
        None => workflow::run_dev(&ctx, args.force, display_mode).await,
        Some(DevCommand::Start) => workflow::start_dev(&ctx, args.force).await,
        Some(DevCommand::Status) => workflow::dev_session_status(&ctx).await,
        Some(DevCommand::Logs(logs)) => {
            workflow::dev_session_logs(&ctx, logs.follow, logs.lines).await
        },
        Some(DevCommand::Stop) => workflow::stop_dev(&ctx).await,
    }
}

async fn handle_status(cli: &Cli, args: &StatusArgs) -> Result<()> {
    let ctx = workflow_context(
        cli,
        args.project_dir.as_deref(),
        args.env.as_deref(),
        args.namespace.as_deref(),
    )?;
    workflow::project_status(&ctx).await
}

async fn handle_deploy(cli: &Cli, args: &DeployArgs) -> Result<()> {
    let ctx = workflow_context(cli, args.project_dir.as_deref(), args.env.as_deref(), None)?;
    workflow::deploy_project(&ctx, args.env.clone(), args.dry_run).await
}

async fn handle_functions(cli: &Cli, args: &FunctionsArgs) -> Result<()> {
    let ctx = workflow_context(cli, args.project_dir.as_deref(), args.env.as_deref(), None)?;
    match &args.command {
        FunctionsCommand::Build => workflow::functions::build_functions(&ctx).await,
        FunctionsCommand::Status => workflow::functions::show_function_status(&ctx).await,
        FunctionsCommand::Rollback(rollback) => {
            workflow::functions::rollback_function(&ctx, &rollback.revision)
        },
        FunctionsCommand::Logs(logs) => {
            workflow::functions::show_function_logs(&ctx, logs.procedure.as_deref()).await
        },
    }
}
