use kalam_cli::{FileCredentialStore, Result};

use crate::args::{Cli, CliCommand};

pub mod auth;
pub mod credentials;
pub mod doctor;
pub mod subscriptions;
pub mod update;
pub mod watch_schema;
pub mod workflow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreSessionCommand {
    Login,
    Logout,
    Whoami,
    Invite,
    Token,
    CredentialManagement,
    CredentialLogin,
    WatchSchema,
    Subscriptions,
}

pub struct CommandContext<'a> {
    pub cli:              &'a Cli,
    pub credential_store: &'a mut FileCredentialStore,
}

pub enum PreSessionResult {
    NotHandled,
    Exit,
    ContinueToSession(auth::LoginShellContinuation),
}

fn pre_session_command(cli: &Cli) -> Option<PreSessionCommand> {
    if let Some(command) = &cli.subcommand {
        return match command {
            CliCommand::Login(_) => Some(PreSessionCommand::Login),
            CliCommand::Logout(_) => Some(PreSessionCommand::Logout),
            CliCommand::Whoami => Some(PreSessionCommand::Whoami),
            CliCommand::Invite(_) => Some(PreSessionCommand::Invite),
            CliCommand::Token(_) => Some(PreSessionCommand::Token),
            CliCommand::Version
            | CliCommand::Update(_)
            | CliCommand::Doctor(_)
            | CliCommand::Init(_)
            | CliCommand::Link(_)
            | CliCommand::Schema(_)
            | CliCommand::Migration(_)
            | CliCommand::Db(_)
            | CliCommand::Dev(_)
            | CliCommand::Status(_)
            | CliCommand::Deploy(_)
            | CliCommand::Functions(_) => None,
        };
    }

    if cli.list_instances || cli.show_credentials || cli.delete_credentials {
        Some(PreSessionCommand::CredentialManagement)
    } else if cli.update_credentials {
        Some(PreSessionCommand::CredentialLogin)
    } else if cli.watch_schema {
        Some(PreSessionCommand::WatchSchema)
    } else if cli.list_subscriptions || cli.subscribe.is_some() {
        Some(PreSessionCommand::Subscriptions)
    } else {
        None
    }
}

async fn run_pre_session_command(
    command: PreSessionCommand,
    context: CommandContext<'_>,
) -> Result<PreSessionResult> {
    match command {
        PreSessionCommand::Login => {
            let Some(CliCommand::Login(args)) = &context.cli.subcommand else {
                return Ok(PreSessionResult::NotHandled);
            };
            Ok(match auth::handle_login(context.cli, args, context.credential_store).await? {
                auth::LoginCommandResult::Exit => PreSessionResult::Exit,
                auth::LoginCommandResult::ContinueToSession(login_continuation) => {
                    PreSessionResult::ContinueToSession(login_continuation)
                },
            })
        },
        PreSessionCommand::Logout => {
            let Some(CliCommand::Logout(args)) = &context.cli.subcommand else {
                return Ok(PreSessionResult::NotHandled);
            };
            Ok(if auth::handle_logout(context.cli, args, context.credential_store).await? {
                PreSessionResult::Exit
            } else {
                PreSessionResult::NotHandled
            })
        },
        PreSessionCommand::Whoami => {
            Ok(if auth::handle_whoami(context.cli, context.credential_store).await? {
                PreSessionResult::Exit
            } else {
                PreSessionResult::NotHandled
            })
        },
        PreSessionCommand::Invite => {
            let Some(CliCommand::Invite(args)) = &context.cli.subcommand else {
                return Ok(PreSessionResult::NotHandled);
            };
            Ok(if auth::handle_invite(context.cli, args, context.credential_store).await? {
                PreSessionResult::Exit
            } else {
                PreSessionResult::NotHandled
            })
        },
        PreSessionCommand::Token => {
            let Some(CliCommand::Token(args)) = &context.cli.subcommand else {
                return Ok(PreSessionResult::NotHandled);
            };
            Ok(
                if auth::handle_token_command(context.cli, &args.command, context.credential_store)
                    .await?
                {
                    PreSessionResult::Exit
                } else {
                    PreSessionResult::NotHandled
                },
            )
        },
        PreSessionCommand::CredentialManagement => {
            Ok(if credentials::handle_credentials(context.cli, context.credential_store)? {
                PreSessionResult::Exit
            } else {
                PreSessionResult::NotHandled
            })
        },
        PreSessionCommand::CredentialLogin => Ok(
            if credentials::login_and_store_credentials(context.cli, context.credential_store)
                .await?
            {
                PreSessionResult::Exit
            } else {
                PreSessionResult::NotHandled
            },
        ),
        PreSessionCommand::WatchSchema => Ok(
            if watch_schema::handle_watch_schema(context.cli, context.credential_store).await? {
                PreSessionResult::Exit
            } else {
                PreSessionResult::NotHandled
            },
        ),
        PreSessionCommand::Subscriptions => Ok(
            if subscriptions::handle_subscriptions(context.cli, context.credential_store).await? {
                PreSessionResult::Exit
            } else {
                PreSessionResult::NotHandled
            },
        ),
    }
}

pub async fn handle_early_commands(cli: &Cli) -> Result<bool> {
    match &cli.subcommand {
        Some(CliCommand::Version) => {
            println!("{}", crate::args::version_report());
            Ok(true)
        },
        Some(CliCommand::Update(args)) => update::handle_update(cli, args).await,
        Some(CliCommand::Doctor(args)) => {
            let credential_store_result =
                FileCredentialStore::new().map_err(|error| error.to_string());
            doctor::handle_doctor(cli, credential_store_result, args.strict).await
        },
        _ => Ok(false),
    }
}

pub async fn handle_pre_session_commands(
    cli: &Cli,
    credential_store: &mut FileCredentialStore,
) -> Result<PreSessionResult> {
    let Some(command) = pre_session_command(cli) else {
        return Ok(PreSessionResult::NotHandled);
    };

    run_pre_session_command(
        command,
        CommandContext {
            cli,
            credential_store,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn parse_cli(args: &[&str]) -> Cli {
        let argv = std::iter::once("kalam").chain(args.iter().copied());
        Cli::try_parse_from(argv).expect("test cli args should parse")
    }

    #[test]
    fn credential_management_handler_matches_local_credential_modes() {
        for flag in [
            "--list-instances",
            "--show-credentials",
            "--delete-credentials",
        ] {
            let cli = parse_cli(&[flag]);

            assert_eq!(pre_session_command(&cli), Some(PreSessionCommand::CredentialManagement));
        }
    }

    #[test]
    fn top_level_auth_handlers_match_commands() {
        for (args, expected) in [
            (
                &["login", "--user", "root", "--password", "secret"][..],
                PreSessionCommand::Login,
            ),
            (&["logout"][..], PreSessionCommand::Logout),
            (&["whoami"][..], PreSessionCommand::Whoami),
            (&["invite", "--email", "alice@example.com"][..], PreSessionCommand::Invite),
            (&["token", "create", "--name", "ci-prod"][..], PreSessionCommand::Token),
        ] {
            let cli = parse_cli(args);

            assert_eq!(pre_session_command(&cli), Some(expected));
        }
    }

    #[test]
    fn early_commands_do_not_match_regular_pre_session_dispatch() {
        for args in [
            &["version"][..],
            &["doctor"][..],
            &["update", "--dry-run"][..],
        ] {
            let cli = parse_cli(args);

            assert_eq!(pre_session_command(&cli), None);
        }
    }

    #[tokio::test]
    async fn early_command_handlers_cover_version_and_update_dry_run() {
        let version_cli = parse_cli(&["version"]);
        assert!(handle_early_commands(&version_cli).await.expect("version command"));

        let update_cli = parse_cli(&[
            "update",
            "--version",
            env!("CARGO_PKG_VERSION"),
            "--dry-run",
            "--no-spinner",
        ]);
        assert!(handle_early_commands(&update_cli).await.expect("update dry run"));
    }

    #[test]
    fn credential_login_handler_matches_update_mode() {
        let cli = parse_cli(&[
            "--update-credentials",
            "--user",
            "root",
            "--password",
            "secret",
        ]);

        assert_eq!(pre_session_command(&cli), Some(PreSessionCommand::CredentialLogin));
    }

    #[test]
    fn watch_schema_handler_matches_watch_mode() {
        let cli = parse_cli(&["--watch-schema", "--run", "echo changed"]);

        assert_eq!(pre_session_command(&cli), Some(PreSessionCommand::WatchSchema));
    }

    #[test]
    fn subscription_handler_matches_subscription_modes() {
        for args in [
            &["--list-subscriptions"][..],
            &["--subscribe", "SELECT 1"][..],
        ] {
            let cli = parse_cli(args);

            assert_eq!(pre_session_command(&cli), Some(PreSessionCommand::Subscriptions));
        }
    }

    #[test]
    fn no_pre_session_handler_matches_regular_sql_modes() {
        for args in [
            &["--command", "SELECT 1"][..],
            &["--file", "queries.sql"][..],
            &[][..],
        ] {
            let cli = parse_cli(args);

            assert_eq!(pre_session_command(&cli), None);
        }
    }
}
