//! Kalam CLI - Terminal client for KalamDB
//!
//! **Implements T083**: CLI entry point with argument parsing using clap 4.4
//!
//! # Usage
//!
//! ```bash
//! # Interactive mode
//! kalam-cli -u http://localhost:3000 --token <JWT>
//!
//! # Execute SQL file
//! kalam-cli -u http://localhost:3000 --file queries.sql
//!
//! # JSON output
//! kalam-cli -u http://localhost:3000 --json -c "SELECT * FROM users"
//! ```

use std::io::IsTerminal;

use clap::Parser;
use kalam_cli::{CLIConfiguration, CLIError, FileCredentialStore, Result};

mod args;
mod commands;
mod connect;

use args::Cli;
use commands::{
    credentials::{handle_credentials, login_and_store_credentials},
    init::handle_init_agent,
    subscriptions::handle_subscriptions,
};
use connect::create_session;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        // Use Display formatting instead of Debug to show nice error messages
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    // Parse command-line arguments
    let mut cli = Cli::parse();

    // Handle project scaffolding command before any network/session setup.
    if handle_init_agent(&cli)? {
        return Ok(());
    }

    // If the password is explicitly set to an empty string, only prompt in interactive mode.
    // In non-interactive modes (--command/--file), an empty password may be valid (e.g. default
    // root).
    let is_interactive_mode = cli.command.is_none() && cli.file.is_none();
    if cli.password.as_deref() == Some("") && is_interactive_mode && std::io::stdin().is_terminal()
    {
        let password = rpassword::prompt_password("Password: ")
            .map_err(|e| CLIError::FileError(format!("Failed to read password: {}", e)))?;
        cli.password = Some(password);
    }

    // Initialize logging (basic)
    if cli.verbose {
        eprintln!("Verbose mode enabled");
    }

    // Load configuration early to ensure config file exists
    // This will create a default config file if it doesn't exist
    let _config = CLIConfiguration::load(&cli.config)?;

    // Load credential store
    let mut credential_store = FileCredentialStore::new()?;

    // Handle credential management commands (sync operations like list, show, delete)
    if handle_credentials(&cli, &mut credential_store)? {
        return Ok(());
    }

    // Handle credential login/update (async - requires network)
    if login_and_store_credentials(&cli, &mut credential_store).await? {
        return Ok(());
    }

    // Handle subscription management commands
    if handle_subscriptions(&cli, &mut credential_store).await? {
        return Ok(());
    }

    // Load configuration
    let config = CLIConfiguration::load(&cli.config)?;
    let config_path = kalam_cli::config::expand_config_path(&cli.config);

    let mut session = create_session(&cli, &mut credential_store, &config, config_path).await?;

    // Execute based on mode
    match (cli.file, cli.command, cli.consume) {
        // Consume mode takes precedence
        (_, _, true) => {
            let topic = cli.topic.ok_or_else(|| {
                CLIError::ConfigurationError("--topic is required for consume mode".into())
            })?;
            session
                .cmd_consume(
                    &topic,
                    cli.group.as_deref(),
                    cli.from.as_deref(),
                    cli.consume_limit,
                    cli.consume_timeout,
                )
                .await?;
        },

        // Execute SQL file
        (Some(file), None, false) => {
            let sql = std::fs::read_to_string(&file).map_err(|e| {
                CLIError::FileError(format!("Failed to read {}: {}", file.display(), e))
            })?;
            session.execute_batch(&sql).await?;
        },

        // Execute single command
        (None, Some(command), false) => {
            session.execute(&command).await?;
        },

        // Interactive mode
        (None, None, false) => {
            session.run_interactive().await?;
        },

        // Invalid combination
        (Some(_), Some(_), false) => {
            return Err(CLIError::ConfigurationError(
                "Cannot specify both --file and --command".into(),
            ));
        },
    }

    Ok(())
}
