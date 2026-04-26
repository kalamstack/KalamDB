use std::time::Duration;

use kalam_cli::{CLIConfiguration, FileCredentialStore, Result};

use crate::{args::Cli, connect::create_session};

fn print_list_subscriptions() {
    println!("Subscription management:");
    println!("  • Subscriptions run in blocking mode per CLI session");
    println!("  • Use Ctrl+C to cancel active subscriptions");
    println!("  • Each CLI instance can have at most one active subscription");
    println!("  • No persistent subscription registry is currently implemented");
}

fn print_unsubscribe_message() {
    println!("To unsubscribe from an active subscription, use Ctrl+C in the terminal");
    println!("where the subscription is running, or kill the process.");
}

pub async fn handle_subscriptions(
    cli: &Cli,
    credential_store: &mut FileCredentialStore,
) -> Result<bool> {
    if !(cli.list_subscriptions || cli.subscribe.is_some() || cli.unsubscribe.is_some()) {
        return Ok(false);
    }

    if cli.list_subscriptions {
        print_list_subscriptions();
        return Ok(true);
    }

    if cli.unsubscribe.is_some() {
        print_unsubscribe_message();
        return Ok(true);
    }

    // Only subscriptions require a server session.
    // Load configuration
    let config = CLIConfiguration::load(&cli.config)?;
    let config_path = kalam_cli::config::expand_config_path(&cli.config);
    let mut session = create_session(cli, credential_store, &config, config_path).await?;

    if let Some(query) = &cli.subscribe {
        // Convert timeout from seconds to Duration (0 = no timeout)
        let timeout = if cli.subscription_timeout > 0 {
            Some(Duration::from_secs(cli.subscription_timeout))
        } else {
            None
        };
        session.subscribe_with_timeout(query, timeout).await?;
    }

    Ok(true)
}
