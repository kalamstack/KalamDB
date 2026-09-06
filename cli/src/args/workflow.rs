use std::path::PathBuf;

use clap::{Args, Subcommand, ValueEnum};
use kalam_cli::workflow::project::config::SchemaMode;

#[derive(Args, Debug, Clone, Default)]
pub struct InitArgs {
    /// Project name
    #[arg(long = "name")]
    pub name: Option<String>,

    /// Active schema source mode
    #[arg(long = "schema-mode", value_enum)]
    pub schema_mode: Option<SchemaModeArg>,

    /// Comma-separated generated language targets (typescript, dart/flutter)
    #[arg(long = "languages", value_delimiter = ',')]
    pub languages: Option<Vec<String>>,

    /// Project template or repository example id (for example simple-live or chat-with-ai)
    #[arg(long = "template")]
    pub template: Option<String>,

    /// List embedded templates and repository examples, then exit
    #[arg(long = "list-templates")]
    pub list_templates: bool,

    /// JavaScript package manager for TypeScript projects (npm, pnpm, yarn, bun)
    #[arg(long = "package-manager", value_enum)]
    pub package_manager: Option<PackageManagerArg>,

    /// Non-interactive mode (use defaults for unspecified values)
    #[arg(long = "yes")]
    pub yes: bool,

    /// Local KalamDB server management during kalam dev (local starts server, remote uses existing
    /// URL)
    #[arg(long = "server-mode", value_enum)]
    pub server_mode: Option<ServerModeArg>,

    /// KalamDB server URL for remote server mode
    #[arg(long = "server-url")]
    pub server_url: Option<String>,

    /// Project directory to initialize (defaults to current directory)
    #[arg(long = "project-dir")]
    pub project_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PackageManagerArg {
    Npm,
    Pnpm,
    Yarn,
    Bun,
}

impl From<PackageManagerArg> for kalam_cli::workflow::project::ts::PackageManager {
    fn from(value: PackageManagerArg) -> Self {
        match value {
            PackageManagerArg::Npm => Self::Npm,
            PackageManagerArg::Pnpm => Self::Pnpm,
            PackageManagerArg::Yarn => Self::Yarn,
            PackageManagerArg::Bun => Self::Bun,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ServerModeArg {
    Local,
    Remote,
}

impl From<ServerModeArg> for kalam_cli::workflow::project::init::ServerMode {
    fn from(value: ServerModeArg) -> Self {
        match value {
            ServerModeArg::Local => kalam_cli::workflow::project::init::ServerMode::Local,
            ServerModeArg::Remote => kalam_cli::workflow::project::init::ServerMode::Remote,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SchemaModeArg {
    Sql,
    Remote,
}

impl From<SchemaModeArg> for SchemaMode {
    fn from(value: SchemaModeArg) -> Self {
        match value {
            SchemaModeArg::Sql => SchemaMode::Sql,
            SchemaModeArg::Remote => SchemaMode::Remote,
        }
    }
}

#[derive(Args, Debug, Clone, Default)]
pub struct LinkArgs {
    /// Environment name to link (e.g. dev, prod)
    #[arg(long = "env")]
    pub env: Option<String>,

    /// Namespace to associate with the linked environment
    #[arg(long = "namespace")]
    pub namespace: Option<String>,

    /// KalamDB server URL for the environment
    #[arg(long = "url")]
    pub url: Option<String>,

    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,
}

#[derive(Args, Debug, Clone, Default)]
pub struct DevArgs {
    #[command(subcommand)]
    pub command: Option<DevCommand>,

    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,

    /// Target environment name
    #[arg(long = "env", global = true)]
    pub env: Option<String>,

    /// Namespace override for the resolved environment
    #[arg(long = "namespace", global = true)]
    pub namespace: Option<String>,

    /// Retry a paused schema pipeline on startup
    #[arg(long = "force", global = true)]
    pub force: bool,

    /// Runs the local KalamDB development environment in deterministic, non-interactive mode
    /// optimized for AI coding agents and automation
    #[arg(long = "agent", global = true)]
    pub agent: bool,

    /// Deprecated; kalam dev now streams append-only logs and uses modal prompts
    #[arg(long = "progress", conflicts_with = "verbose")]
    pub progress: bool,
}

#[derive(Subcommand, Debug, Clone)]
pub enum DevCommand {
    /// Start the development environment in the background
    Start,
    /// Show whether a background `kalam dev` session is running
    Status,
    /// Print or follow logs from a background `kalam dev` session
    Logs(DevLogsArgs),
    /// Stop a background `kalam dev` session
    Stop,
}

#[derive(Args, Debug, Clone, Default)]
pub struct DevLogsArgs {
    /// Follow the log file until interrupted
    #[arg(short = 'F', long = "follow")]
    pub follow: bool,

    /// Number of trailing lines to print (0 prints the full file)
    #[arg(short = 'n', long = "lines", default_value_t = 200)]
    pub lines: usize,
}

#[derive(Args, Debug, Clone, Default)]
pub struct StatusArgs {
    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,

    /// Target environment name
    #[arg(long = "env", global = true)]
    pub env: Option<String>,

    /// Namespace override for the resolved environment
    #[arg(long = "namespace", global = true)]
    pub namespace: Option<String>,
}

#[derive(Args, Debug, Clone, Default)]
pub struct DeployArgs {
    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,

    /// Target environment name
    #[arg(long = "env", global = true)]
    pub env: Option<String>,

    /// Validate readiness without applying migrations or health checks
    #[arg(long = "dry-run")]
    pub dry_run: bool,
}

#[derive(Args, Debug, Clone)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub command: SchemaCommand,

    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,

    /// Target environment name
    #[arg(long = "env", global = true)]
    pub env: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum SchemaCommand {
    /// Generate SDK and schema artifacts from the project schema source
    Gen(SchemaGenerateArgs),
    /// Pull the active schema source from a linked environment
    Pull(SchemaPullArgs),
}

#[derive(Args, Debug, Clone, Default)]
pub struct SchemaGenerateArgs {
    /// Limit generation to specific language targets (typescript, dart/flutter)
    #[arg(long = "languages", value_delimiter = ',')]
    pub languages: Option<Vec<String>>,
}

#[derive(Args, Debug, Clone, Default)]
pub struct SchemaPullArgs {}

#[derive(Args, Debug, Clone)]
pub struct MigrationArgs {
    #[command(subcommand)]
    pub command: MigrationCommand,

    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,

    /// Target environment name
    #[arg(long = "env", global = true)]
    pub env: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum MigrationCommand {
    /// Create a new ordered migration from the current schema changes
    Create(MigrationCreateArgs),
    /// Show migration state for the current project
    Status(MigrationStatusArgs),
    /// Create a numbered migration from kalam/migrations/_draft.sql
    Seal(MigrationSealArgs),
    /// Retry a failed migration
    Retry(MigrationRetryArgs),
    /// Repair migration state manually
    Repair(MigrationRepairArgs),
}

#[derive(Args, Debug, Clone)]
pub struct MigrationCreateArgs {
    /// Migration name
    pub name: String,
}

#[derive(Args, Debug, Clone, Default)]
pub struct MigrationStatusArgs {}

#[derive(Args, Debug, Clone, Default)]
pub struct MigrationSealArgs {}

#[derive(Args, Debug, Clone)]
pub struct MigrationRetryArgs {
    /// Migration id or filename to retry
    pub migration_id: String,
}

#[derive(Args, Debug, Clone)]
pub struct MigrationRepairArgs {
    /// Migration id or filename to repair
    pub migration_id: String,

    /// Mark this migration as applied
    #[arg(long = "mark-applied")]
    pub mark_applied: bool,
}

#[derive(Args, Debug, Clone)]
pub struct DbArgs {
    #[command(subcommand)]
    pub command: DbCommand,

    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,

    /// Target environment name
    #[arg(long = "env", global = true)]
    pub env: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum DbCommand {
    /// Apply the committed migration history to the linked database
    Migrate(DbMigrateArgs),

    /// Remove local dev server data so the next `kalam dev` starts with an empty database
    Reset(DbResetArgs),
}

#[derive(Args, Debug, Clone, Default)]
pub struct DbResetArgs {
    /// Confirm dropping namespace data on a remote or non-project server without prompting
    #[arg(long)]
    pub yes: bool,
}

#[derive(Args, Debug, Clone, Default)]
pub struct DbMigrateArgs {}

#[derive(Args, Debug, Clone)]
pub struct FunctionsArgs {
    #[command(subcommand)]
    pub command: FunctionsCommand,

    /// Project directory containing kalam.toml
    #[arg(long = "project-dir", global = true)]
    pub project_dir: Option<PathBuf>,

    /// Target environment name
    #[arg(long = "env", global = true)]
    pub env: Option<String>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum FunctionsCommand {
    /// Generate function contracts and SDK artifacts
    Build,
    /// Show catalogued procedures and revisions
    Status,
    /// Point the active revision at a previously activated hash
    Rollback(FunctionsRollbackArgs),
    /// Print recent trigger delivery attempts
    Logs(FunctionsLogsArgs),
}

#[derive(Args, Debug, Clone)]
pub struct FunctionsRollbackArgs {
    pub revision: String,
}

#[derive(Args, Debug, Clone, Default)]
pub struct FunctionsLogsArgs {
    pub procedure: Option<String>,
}
