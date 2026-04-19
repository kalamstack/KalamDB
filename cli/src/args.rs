use clap::Parser;
use kalam_cli::OutputFormat;
use std::path::PathBuf;

// Build information - Create a static version string at compile time

// Macro to create the version string at compile time
macro_rules! version_string {
    () => {
        concat!(
            env!("CARGO_PKG_VERSION"),
            "\nCommit: ",
            env!("GIT_COMMIT_HASH"),
            " (",
            env!("GIT_BRANCH"),
            ")\nBuilt: ",
            env!("BUILD_DATE")
        )
    };
}

/// Kalam CLI - Terminal client for KalamDB
#[derive(Parser, Debug)]
#[command(name = "kalam")]
#[command(author = "KalamDB Team")]
#[command(version = version_string!())]
#[command(about = "Interactive SQL terminal for KalamDB", long_about = None)]
pub struct Cli {
    /// Server URL (e.g., http://localhost:3000)
    #[arg(short = 'u', long = "url")]
    pub url: Option<String>,

    /// Host address (alternative to URL)
    #[arg(short = 'H', long = "host")]
    pub host: Option<String>,

    /// Port number (default: 3000)
    #[arg(short = 'p', long = "port", default_value = "3000")]
    pub port: u16,

    /// JWT authentication token (avoid in shared shells; may appear in process list/history)
    #[arg(long = "token")]
    pub token: Option<String>,

    /// HTTP Basic Auth user identifier
    #[arg(long = "user")]
    pub user: Option<String>,

    /// HTTP Basic Auth password (if flag is present without value, prompts interactively;
    /// avoid passing inline secrets in shared shells)
    #[arg(long = "password", num_args = 0..=1, default_missing_value = "")]
    pub password: Option<String>,

    /// Database instance name (for credential storage)
    #[arg(long = "instance", default_value = "local")]
    pub instance: String,

    /// Execute SQL from file and exit
    #[arg(short = 'f', long = "file")]
    pub file: Option<PathBuf>,

    /// Execute SQL command and exit
    #[arg(short = 'c', long = "command")]
    pub command: Option<String>,

    /// Output format
    #[arg(long = "format", default_value = "table")]
    pub format: OutputFormat,

    /// Enable JSON output (shorthand for --format=json)
    #[arg(long = "json", conflicts_with = "format")]
    pub json: bool,

    /// Enable CSV output (shorthand for --format=csv)
    #[arg(long = "csv", conflicts_with = "format")]
    pub csv: bool,

    /// Disable colored output
    #[arg(long = "no-color")]
    pub no_color: bool,

    /// Disable spinners/animations
    #[arg(long = "no-spinner")]
    pub no_spinner: bool,

    /// Loading indicator threshold in ms (0 to always show)
    #[arg(long = "loading-threshold-ms")]
    pub loading_threshold_ms: Option<u64>,

    /// Configuration file path
    #[arg(long = "config", default_value = "~/.kalam/config.toml")]
    pub config: PathBuf,

    /// Enable verbose logging
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,

    /// HTTP request timeout in seconds (default: 30)
    #[arg(long = "timeout", value_name = "SECONDS", default_value_t = 30)]
    pub timeout: u64,

    /// Connection timeout in seconds (TCP + TLS handshake, default: 10)
    #[arg(
        long = "connection-timeout",
        value_name = "SECONDS",
        default_value_t = 10
    )]
    pub connection_timeout: u64,

    /// Receive timeout in seconds (default: 30)
    #[arg(long = "receive-timeout", value_name = "SECONDS", default_value_t = 30)]
    pub receive_timeout: u64,

    /// WebSocket authentication timeout in seconds (default: 5)
    #[arg(long = "auth-timeout", value_name = "SECONDS", default_value_t = 5)]
    pub auth_timeout: u64,

    // Credential management commands
    /// Show stored credentials for instance
    #[arg(long = "show-credentials")]
    pub show_credentials: bool,

    /// Update stored credentials for instance
    #[arg(long = "update-credentials")]
    pub update_credentials: bool,

    /// Delete stored credentials for instance
    #[arg(long = "delete-credentials")]
    pub delete_credentials: bool,

    /// Save credentials (JWT token) after successful login
    /// When used with --user/--password, stores the JWT token for future sessions
    #[arg(long = "save-credentials")]
    pub save_credentials: bool,

    /// List all stored credential instances
    #[arg(long = "list-instances")]
    pub list_instances: bool,

    // Subscription management commands
    /// Subscribe to a table or live query
    #[arg(long = "subscribe")]
    pub subscribe: Option<String>,

    /// Subscription timeout in seconds (0 = no timeout, default: 0)
    /// After receiving initial data, subscription will exit after this duration
    #[arg(
        long = "subscription-timeout",
        value_name = "SECONDS",
        default_value_t = 0
    )]
    pub subscription_timeout: u64,

    /// Initial data timeout in seconds (0 = no timeout, default: 30)
    /// Maximum time to wait for initial data batch after subscribing
    #[arg(
        long = "initial-data-timeout",
        value_name = "SECONDS",
        default_value_t = 30
    )]
    pub initial_data_timeout: u64,

    /// Use fast timeout preset (optimized for local development)
    #[arg(long = "fast-timeouts")]
    pub fast_timeouts: bool,

    /// Use relaxed timeout preset (optimized for high-latency networks)
    #[arg(long = "relaxed-timeouts")]
    pub relaxed_timeouts: bool,

    // Agent project scaffolding
    /// Generate a new TypeScript agent project scaffold
    #[arg(long = "init-agent")]
    pub init_agent: bool,

    /// Disable interactive prompts for --init-agent and use defaults/flags
    #[arg(long = "init-agent-non-interactive", requires = "init_agent")]
    pub init_agent_non_interactive: bool,

    /// Project name for --init-agent (directory name)
    #[arg(long = "agent-name", requires = "init_agent")]
    pub agent_name: Option<String>,

    /// Output directory for --init-agent (default: current directory)
    #[arg(long = "agent-output", requires = "init_agent")]
    pub agent_output: Option<PathBuf>,

    /// Table id (namespace.table) for --init-agent
    #[arg(long = "agent-table", requires = "init_agent")]
    pub agent_table: Option<String>,

    /// Topic id for --init-agent
    #[arg(long = "agent-topic", requires = "init_agent")]
    pub agent_topic: Option<String>,

    /// Consumer group id for --init-agent
    #[arg(long = "agent-group", requires = "init_agent")]
    pub agent_group: Option<String>,

    /// Primary key column name in the target table
    #[arg(long = "agent-id-column", requires = "init_agent")]
    pub agent_id_column: Option<String>,

    /// Input text column name to summarize
    #[arg(long = "agent-input-column", requires = "init_agent")]
    pub agent_input_column: Option<String>,

    /// Output text column name for generated summary
    #[arg(long = "agent-output-column", requires = "init_agent")]
    pub agent_output_column: Option<String>,

    /// Default system prompt for LLM summarization
    #[arg(long = "agent-system-prompt", requires = "init_agent")]
    pub agent_system_prompt: Option<String>,

    /// Unsubscribe from a subscription
    #[arg(long = "unsubscribe")]
    pub unsubscribe: Option<String>,

    /// List active subscriptions
    #[arg(long = "list-subscriptions")]
    pub list_subscriptions: bool,

    // Topic consumption commands
    /// Start consumer mode (consume messages from a topic)
    #[arg(long = "consume")]
    pub consume: bool,

    /// Topic name for consume mode
    #[arg(long = "topic", requires = "consume")]
    pub topic: Option<String>,

    /// Consumer group ID for consume mode
    #[arg(long = "group")]
    pub group: Option<String>,

    /// Starting offset position: earliest, latest, or numeric offset
    #[arg(long = "from")]
    pub from: Option<String>,

    /// Maximum number of messages to consume before exiting
    #[arg(long = "consume-limit")]
    pub consume_limit: Option<usize>,

    /// Timeout in seconds for consume mode (exit if idle)
    #[arg(long = "consume-timeout")]
    pub consume_timeout: Option<u64>,
}
