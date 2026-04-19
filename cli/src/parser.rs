//! Command parser for SQL and backslash commands
//!
//! **Implements T087**: CommandParser for SQL + backslash commands
//!
//! Parses user input to distinguish between SQL statements and CLI meta-commands.

use crate::error::{CLIError, Result};

/// Parsed command
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// SQL statement
    Sql(String),

    /// Meta-commands (backslash commands)
    Quit,
    Help,
    Flush,
    ClusterSnapshot,
    ClusterPurge {
        upto: u64,
    },
    ClusterTriggerElection,
    ClusterTransferLeader {
        node_id: u64,
    },
    ClusterStepdown,
    ClusterClear,
    ClusterList,
    ClusterListGroups,
    ClusterStatus,
    ClusterJoin(String), // node address to join
    ClusterLeave,
    Health,
    Pause,
    Continue,
    ListTables,
    Describe(String),
    SetFormat(String),
    Subscribe(String),
    Unsubscribe,
    RefreshTables,
    ShowCredentials,
    UpdateCredentials {
        username: String,
        password: String,
    },
    DeleteCredentials,
    Info,
    Sessions,
    /// Show system statistics (from system.stats)
    Stats,
    /// Open interactive history menu
    History,
    /// Consume messages from a topic
    Consume {
        topic: String,
        group: Option<String>,
        from: Option<String>,
        limit: Option<usize>,
        timeout: Option<u64>,
    },
    Unknown(String),
}

/// Command parser
pub struct CommandParser;

impl CommandParser {
    /// Create a new parser
    pub fn new() -> Self {
        Self
    }

    /// Parse a command line
    pub fn parse(&self, line: &str) -> Result<Command> {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            return Err(CLIError::ParseError("Empty command".into()));
        }

        // Check for backslash commands
        if trimmed.starts_with('\\') {
            return self.parse_meta_command(trimmed);
        }

        // Otherwise, treat as SQL
        Ok(Command::Sql(trimmed.to_string()))
    }

    /// Parse meta-commands (backslash commands)
    fn parse_meta_command(&self, line: &str) -> Result<Command> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Err(CLIError::ParseError("Invalid command".into()));
        }

        let command = parts[0];
        let args = parts.get(1..).unwrap_or(&[]);

        match command {
            "\\quit" | "\\q" => Ok(Command::Quit),
            "\\help" | "\\?" => Ok(Command::Help),
            "\\sessions" => Ok(Command::Sessions),
            "\\stats" | "\\metrics" => Ok(Command::Stats),
            "\\flush" => Ok(Command::Flush),
            "\\cluster" => {
                if args.is_empty() {
                    Err(CLIError::ParseError(
                        "\\cluster requires: snapshot, purge, trigger-election, transfer-leader, stepdown, clear, list, status, join, or leave".into(),
                    ))
                } else {
                    let sub = args[0].to_ascii_lowercase();
                    match sub.as_str() {
                        "snapshot" => Ok(Command::ClusterSnapshot),
                        "purge" => {
                            let upto = args
                                .iter()
                                .skip(1)
                                .position(|arg| {
                                    arg.trim_start_matches('-').eq_ignore_ascii_case("upto")
                                })
                                .and_then(|pos| args.get(pos + 2))
                                .and_then(|v| v.parse::<u64>().ok())
                                .or_else(|| args.get(1).and_then(|v| v.parse::<u64>().ok()));

                            if let Some(upto) = upto {
                                Ok(Command::ClusterPurge { upto })
                            } else {
                                Err(CLIError::ParseError(
                                    "\\cluster purge requires --upto <index> or a numeric index"
                                        .into(),
                                ))
                            }
                        },
                        "trigger-election" => Ok(Command::ClusterTriggerElection),
                        "trigger" => {
                            if args.get(1).map(|v| v.eq_ignore_ascii_case("election")) == Some(true)
                            {
                                Ok(Command::ClusterTriggerElection)
                            } else {
                                Err(CLIError::ParseError(
                                    "\\cluster trigger requires: election".into(),
                                ))
                            }
                        },
                        "transfer-leader" => {
                            let node_id = args.get(1).and_then(|v| v.parse::<u64>().ok());
                            if let Some(node_id) = node_id {
                                Ok(Command::ClusterTransferLeader { node_id })
                            } else {
                                Err(CLIError::ParseError(
                                    "\\cluster transfer-leader requires a numeric node id".into(),
                                ))
                            }
                        },
                        "transfer" => {
                            if args.get(1).map(|v| v.eq_ignore_ascii_case("leader")) == Some(true) {
                                let node_id = args.get(2).and_then(|v| v.parse::<u64>().ok());
                                if let Some(node_id) = node_id {
                                    Ok(Command::ClusterTransferLeader { node_id })
                                } else {
                                    Err(CLIError::ParseError(
                                        "\\cluster transfer leader requires a numeric node id"
                                            .into(),
                                    ))
                                }
                            } else {
                                Err(CLIError::ParseError(
                                    "\\cluster transfer requires: leader <node_id>".into(),
                                ))
                            }
                        },
                        "stepdown" | "step-down" => Ok(Command::ClusterStepdown),
                        "clear" => Ok(Command::ClusterClear),
                        "list" | "ls" => {
                            if args.get(1).map(|v| v.eq_ignore_ascii_case("groups")) == Some(true) {
                                Ok(Command::ClusterListGroups)
                            } else {
                                Ok(Command::ClusterList)
                            }
                        },
                        "status" => Ok(Command::ClusterStatus),
                        "join" => {
                            if args.len() < 2 {
                                Err(CLIError::ParseError(
                                    "\\cluster join requires a node address".into(),
                                ))
                            } else {
                                Ok(Command::ClusterJoin(args[1].to_string()))
                            }
                        },
                        "leave" => Ok(Command::ClusterLeave),
                        _ => Err(CLIError::ParseError(format!(
                            "Unknown cluster subcommand: {}",
                            args[0]
                        ))),
                    }
                }
            },
            "\\health" => Ok(Command::Health),
            "\\pause" => Ok(Command::Pause),
            "\\continue" => Ok(Command::Continue),
            "\\dt" | "\\tables" => Ok(Command::ListTables),
            "\\d" | "\\describe" => {
                if args.is_empty() {
                    Err(CLIError::ParseError("\\describe requires a table name".into()))
                } else {
                    Ok(Command::Describe(args.join(" ")))
                }
            },
            "\\format" => {
                if args.is_empty() {
                    Err(CLIError::ParseError("\\format requires: table, json, or csv".into()))
                } else {
                    Ok(Command::SetFormat(args[0].to_string()))
                }
            },
            "\\subscribe" | "\\watch" => {
                if args.is_empty() {
                    Err(CLIError::ParseError("\\subscribe requires a SQL query".into()))
                } else {
                    Ok(Command::Subscribe(args.join(" ")))
                }
            },
            "\\unsubscribe" | "\\unwatch" => Ok(Command::Unsubscribe),
            "\\refresh-tables" | "\\refresh" => Ok(Command::RefreshTables),
            "\\show-credentials" | "\\credentials" => Ok(Command::ShowCredentials),
            "\\update-credentials" => {
                if args.len() < 2 {
                    Err(CLIError::ParseError(
                        "\\update-credentials requires user and password".into(),
                    ))
                } else {
                    Ok(Command::UpdateCredentials {
                        username: args[0].to_string(),
                        password: args[1].to_string(),
                    })
                }
            },
            "\\delete-credentials" => Ok(Command::DeleteCredentials),
            "\\info" | "\\session" => Ok(Command::Info),
            "\\history" | "\\h" => Ok(Command::History),
            "\\consume" => {
                if args.is_empty() {
                    return Err(CLIError::ParseError(
                        "\\consume requires a topic name. Usage: \\consume <topic> [--group NAME] [--from earliest|latest|OFFSET] [--limit N] [--timeout SECONDS]".into(),
                    ));
                }
                let topic = args[0].to_string();
                let mut group = None;
                let mut from = None;
                let mut limit = None;
                let mut timeout = None;

                let mut i = 1;
                while i < args.len() {
                    match args[i] {
                        "--group" => {
                            if i + 1 < args.len() {
                                group = Some(args[i + 1].to_string());
                                i += 2;
                            } else {
                                return Err(CLIError::ParseError(
                                    "--group requires a value".into(),
                                ));
                            }
                        },
                        "--from" => {
                            if i + 1 < args.len() {
                                from = Some(args[i + 1].to_string());
                                i += 2;
                            } else {
                                return Err(CLIError::ParseError("--from requires a value".into()));
                            }
                        },
                        "--limit" => {
                            if i + 1 < args.len() {
                                limit = args[i + 1].parse::<usize>().ok();
                                if limit.is_none() {
                                    return Err(CLIError::ParseError(
                                        "--limit requires a numeric value".into(),
                                    ));
                                }
                                i += 2;
                            } else {
                                return Err(CLIError::ParseError(
                                    "--limit requires a value".into(),
                                ));
                            }
                        },
                        "--timeout" => {
                            if i + 1 < args.len() {
                                timeout = args[i + 1].parse::<u64>().ok();
                                if timeout.is_none() {
                                    return Err(CLIError::ParseError(
                                        "--timeout requires a numeric value (seconds)".into(),
                                    ));
                                }
                                i += 2;
                            } else {
                                return Err(CLIError::ParseError(
                                    "--timeout requires a value".into(),
                                ));
                            }
                        },
                        _ => {
                            return Err(CLIError::ParseError(format!(
                                "Unknown option for \\consume: {}",
                                args[i]
                            )));
                        },
                    }
                }

                Ok(Command::Consume {
                    topic,
                    group,
                    from,
                    limit,
                    timeout,
                })
            },
            _ => Ok(Command::Unknown(command.to_string())),
        }
    }
}

impl Default for CommandParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql() {
        let parser = CommandParser::new();
        let cmd = parser.parse("SELECT * FROM users").unwrap();
        assert_eq!(cmd, Command::Sql("SELECT * FROM users".to_string()));
    }

    #[test]
    fn test_parse_quit() {
        let parser = CommandParser::new();
        assert_eq!(parser.parse("\\quit").unwrap(), Command::Quit);
        assert_eq!(parser.parse("\\q").unwrap(), Command::Quit);
    }

    #[test]
    fn test_parse_help() {
        let parser = CommandParser::new();
        assert_eq!(parser.parse("\\help").unwrap(), Command::Help);
        assert_eq!(parser.parse("\\?").unwrap(), Command::Help);
    }

    #[test]
    fn test_parse_describe() {
        let parser = CommandParser::new();
        let cmd = parser.parse("\\describe users").unwrap();
        assert_eq!(cmd, Command::Describe("users".to_string()));
    }

    #[test]
    fn test_parse_unknown() {
        let parser = CommandParser::new();
        let cmd = parser.parse("\\unknown").unwrap();
        assert_eq!(cmd, Command::Unknown("\\unknown".to_string()));
    }

    #[test]
    fn test_parse_stats() {
        let parser = CommandParser::new();
        assert_eq!(parser.parse("\\stats").unwrap(), Command::Stats);
        assert_eq!(parser.parse("\\metrics").unwrap(), Command::Stats);
    }

    #[test]
    fn test_parse_sessions() {
        let parser = CommandParser::new();
        assert_eq!(parser.parse("\\sessions").unwrap(), Command::Sessions);
    }

    #[test]
    fn test_parse_history() {
        let parser = CommandParser::new();
        assert_eq!(parser.parse("\\history").unwrap(), Command::History);
        assert_eq!(parser.parse("\\h").unwrap(), Command::History);
    }

    #[test]
    fn test_empty_command() {
        let parser = CommandParser::new();
        assert!(parser.parse("").is_err());
        assert!(parser.parse("   ").is_err());
    }
}
