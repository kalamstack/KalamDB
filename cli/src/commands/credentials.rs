use std::{
    io::{self, Write},
    time::Duration,
};

use kalam_cli::{CLIError, FileCredentialStore, Result};
use kalam_client::{
    credentials::{CredentialStore, Credentials},
    KalamLinkClient,
};

use crate::args::Cli;

pub fn handle_credentials(cli: &Cli, credential_store: &mut FileCredentialStore) -> Result<bool> {
    if cli.list_instances {
        let instances = credential_store.list_instances().map_err(|e| {
            CLIError::ConfigurationError(format!("Failed to list instances: {}", e))
        })?;
        if instances.is_empty() {
            println!("No stored credentials");
        } else {
            println!("Stored credential instances:");
            for instance in instances {
                // Show additional info if available
                if let Ok(Some(creds)) = credential_store.get_credentials(&instance) {
                    let user_info =
                        creds.user.as_ref().map(|user| user.as_str()).unwrap_or("unknown");
                    let expired = if creds.is_expired() { " (expired)" } else { "" };
                    println!("  • {} (user: {}){}", instance, user_info, expired);
                } else {
                    println!("  • {}", instance);
                }
            }
        }
        return Ok(true);
    }

    if cli.show_credentials {
        match credential_store.get_credentials(&cli.instance).map_err(|e| {
            CLIError::ConfigurationError(format!("Failed to get credentials: {}", e))
        })? {
            Some(creds) => {
                println!("Instance: {}", creds.instance);
                if let Some(ref user) = creds.user {
                    println!("User: {}", user);
                }
                println!("JWT Token: [redacted]");
                if let Some(ref expires) = creds.expires_at {
                    let expired = if creds.is_expired() { " (EXPIRED)" } else { "" };
                    println!("Expires: {}{}", expires, expired);
                }
                if creds.refresh_token.is_some() {
                    println!("Refresh Token: [redacted]");
                    if let Some(ref refresh_expires) = creds.refresh_expires_at {
                        let expired = if creds.is_refresh_expired() {
                            " (EXPIRED)"
                        } else {
                            ""
                        };
                        println!("Refresh Expires: {}{}", refresh_expires, expired);
                    }
                }
                if let Some(ref url) = creds.server_url {
                    println!("Server URL: {}", url);
                }
            },
            None => {
                println!("No credentials stored for instance '{}'", cli.instance);
            },
        }
        return Ok(true);
    }

    if cli.delete_credentials {
        credential_store.delete_credentials(&cli.instance).map_err(|e| {
            CLIError::ConfigurationError(format!("Failed to delete credentials: {}", e))
        })?;
        println!("Deleted credentials for instance '{}'", cli.instance);
        return Ok(true);
    }

    Ok(false)
}

/// Login with user/password and store the JWT token
/// This is called from the async context in main.rs
pub async fn login_and_store_credentials(
    cli: &Cli,
    credential_store: &mut FileCredentialStore,
) -> Result<bool> {
    if !cli.update_credentials {
        return Ok(false);
    }

    // Get server URL
    let server_url = cli.url.clone().unwrap_or_else(|| {
        cli.host
            .as_ref()
            .map(|h| format!("http://{}:{}", h, cli.port))
            .unwrap_or_else(|| "http://localhost:8080".to_string())
    });

    // Prompt for credentials
    let user = if let Some(user) = &cli.user {
        user.clone()
    } else {
        print!("User: ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| CLIError::FileError(format!("Failed to read user: {}", e)))?;
        input.trim().to_string()
    };

    let password = if let Some(pass) = &cli.password {
        pass.clone()
    } else {
        rpassword::prompt_password("Password: ")
            .map_err(|e| CLIError::FileError(format!("Failed to read password: {}", e)))?
    };

    // Login to get JWT token
    let client = KalamLinkClient::builder()
        .base_url(&server_url)
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| CLIError::ConfigurationError(format!("Failed to create client: {}", e)))?;

    let login_response = client
        .login(&user, &password)
        .await
        .map_err(|e| CLIError::ConfigurationError(format!("Login failed: {}", e)))?;

    // Store the JWT token and refresh token
    let creds = Credentials::with_refresh_token(
        cli.instance.clone(),
        login_response.access_token,
        login_response.user.id.to_string(),
        login_response.expires_at.clone(),
        Some(server_url),
        login_response.refresh_token.clone(),
        login_response.refresh_expires_at.clone(),
    );

    credential_store
        .set_credentials(&creds)
        .map_err(|e| CLIError::ConfigurationError(format!("Failed to save credentials: {}", e)))?;

    println!("Successfully logged in and saved credentials for instance '{}'", cli.instance);
    println!("Token expires: {}", login_response.expires_at);
    if let Some(ref refresh_expires) = login_response.refresh_expires_at {
        println!("Refresh token expires: {}", refresh_expires);
    }

    Ok(true)
}
