use super::{CLISession, OutputFormat};
use crate::error::{CLIError, Result};
use crate::parser::Command;
use colored::Colorize;
use kalam_client::SubscriptionConfig;
use std::time::Instant;

impl CLISession {
    /// Execute a parsed command
    ///
    /// **Implements T094**: Backslash command handling
    pub(super) async fn execute_command(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Sql(sql) => {
                self.execute(&sql).await?;
            },
            Command::Quit => {
                println!("Goodbye!");
                std::process::exit(0);
            },
            Command::Help => {
                self.show_help();
            },
            Command::Flush => {
                println!("Storage flushing all tables in current namespace...");
                match self.execute("STORAGE FLUSH ALL").await {
                    Ok(_) => println!("Storage flush completed successfully"),
                    Err(e) => eprintln!("Storage flush failed: {}", e),
                }
            },
            Command::ClusterSnapshot => {
                println!("Triggering cluster snapshots...");
                match self.execute("CLUSTER SNAPSHOT").await {
                    Ok(_) => println!("Snapshot triggered"),
                    Err(e) => eprintln!("Snapshot failed: {}", e),
                }
            },
            Command::ClusterPurge { upto } => {
                println!("Purging cluster logs up to {}...", upto);
                match self.execute(&format!("CLUSTER PURGE --UPTO {}", upto)).await {
                    Ok(_) => {},
                    Err(e) => eprintln!("Cluster purge failed: {}", e),
                }
            },
            Command::ClusterTriggerElection => {
                println!("Triggering cluster election...");
                match self.execute("CLUSTER TRIGGER ELECTION").await {
                    Ok(_) => {},
                    Err(e) => eprintln!("Cluster trigger-election failed: {}", e),
                }
            },
            Command::ClusterTransferLeader { node_id } => {
                println!("Transferring cluster leadership to node {}...", node_id);
                match self.execute(&format!("CLUSTER TRANSFER-LEADER {}", node_id)).await {
                    Ok(_) => {},
                    Err(e) => eprintln!("Cluster transfer-leader failed: {}", e),
                }
            },
            Command::ClusterStepdown => {
                println!("Requesting cluster leader stepdown...");
                match self.execute("CLUSTER STEPDOWN").await {
                    Ok(_) => {},
                    Err(e) => eprintln!("Cluster stepdown failed: {}", e),
                }
            },
            Command::ClusterClear => {
                println!("Clearing old cluster snapshots...");
                match self.execute("CLUSTER CLEAR").await {
                    Ok(_) => {},
                    Err(e) => eprintln!("Cluster clear failed: {}", e),
                }
            },
            Command::ClusterList => match self.execute("SELECT * FROM system.cluster").await {
                Ok(_) => {},
                Err(e) => eprintln!("Cluster list failed: {}", e),
            },
            Command::ClusterListGroups => {
                match self.execute("SELECT * FROM system.cluster_groups").await {
                    Ok(_) => {},
                    Err(e) => eprintln!("Cluster list groups failed: {}", e),
                }
            },
            Command::ClusterStatus => match self.execute("SELECT * FROM system.cluster").await {
                Ok(_) => {},
                Err(e) => eprintln!("Cluster status failed: {}", e),
            },
            Command::ClusterJoin(addr) => {
                println!("{}  CLUSTER JOIN is not implemented yet", "⚠️".yellow());
                println!("Would join node at: {}", addr);
                println!(
                    "\nTo add a node to the cluster, configure it in server.toml and restart."
                );
            },
            Command::ClusterLeave => {
                println!("{}  CLUSTER LEAVE is not implemented yet", "⚠️".yellow());
                println!(
                    "\nTo remove this node from the cluster, gracefully shut down the server."
                );
            },
            Command::Health => match self.health_check().await {
                Ok(_) => {},
                Err(e) => eprintln!("Health check failed: {}", e),
            },
            Command::Pause => {
                println!("Pausing ingestion...");
                match self.execute("PAUSE").await {
                    Ok(_) => println!("Ingestion paused"),
                    Err(e) => eprintln!("Pause failed: {}", e),
                }
            },
            Command::Continue => {
                println!("Resuming ingestion...");
                match self.execute("CONTINUE").await {
                    Ok(_) => println!("Ingestion resumed"),
                    Err(e) => eprintln!("Resume failed: {}", e),
                }
            },
            Command::ListTables => {
                self.execute(
                    "SELECT namespace_id AS namespace, table_name, table_type FROM system.tables ORDER BY namespace_id, table_name",
                )
                .await?;
            },
            Command::Describe(table) => {
                let query = format!(
                    "SELECT * FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
                    table
                );
                self.execute(&query).await?;
            },
            Command::SetFormat(format) => match format.to_lowercase().as_str() {
                "table" => {
                    self.set_format(OutputFormat::Table);
                    println!("Output format set to: table");
                },
                "json" => {
                    self.set_format(OutputFormat::Json);
                    println!("Output format set to: json");
                },
                "csv" => {
                    self.set_format(OutputFormat::Csv);
                    println!("Output format set to: csv");
                },
                _ => {
                    eprintln!("Unknown format: {}. Use: table, json, or csv", format);
                },
            },
            Command::Subscribe(query) => {
                let (clean_sql, options) = Self::extract_subscribe_options(&query);
                let sub_id = format!(
                    "sub_{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                );
                let mut config = SubscriptionConfig::new(sub_id, clean_sql);
                config.options = options;
                self.run_subscription(config).await?;
            },
            Command::Unsubscribe => {
                println!("No active subscription to cancel");
            },
            Command::RefreshTables => {
                // This is handled in run_interactive, shouldn't reach here
                println!("Table names refreshed");
            },
            Command::ShowCredentials => {
                self.show_credentials();
            },
            Command::UpdateCredentials { username, password } => {
                self.update_credentials(username, password).await?;
            },
            Command::DeleteCredentials => {
                self.delete_credentials()?;
            },
            Command::Info => {
                self.show_session_info().await;
            },
            Command::Sessions => {
                self.execute(
                    "SELECT * FROM system.sessions ORDER BY last_seen_at DESC, session_id",
                )
                .await?;
            },
            Command::Stats => {
                self.execute(
                    "SELECT metric_name, metric_value FROM system.stats ORDER BY metric_name LIMIT 5000",
                )
                .await?;
            },
            Command::History => {
                // Handled in run_interactive() where we have access to history
                // This should not be reached
                eprintln!("History command should be handled in interactive mode");
            },
            Command::Consume {
                topic,
                group,
                from,
                limit,
                timeout,
            } => {
                self.cmd_consume(&topic, group.as_deref(), from.as_deref(), limit, timeout)
                    .await?;
            },
            Command::Unknown(cmd) => {
                eprintln!("Unknown command: {}. Type \\help for help.", cmd);
            },
        }
        Ok(())
    }

    /// Show help message (styled, sectioned)
    fn show_help(&self) {
        println!();
        println!(
            "{}",
            "╔═══════════════════════════════════════════════════════════╗"
                .bright_blue()
                .bold()
        );
        println!(
            "{}{}{}",
            "║ ".bright_blue().bold(),
            "Commands & Shortcuts".white().bold(),
            "                                                 ║"
                .to_string()
                .bright_blue()
                .bold()
        );
        println!(
            "{}",
            "╠═══════════════════════════════════════════════════════════╣"
                .bright_blue()
                .bold()
        );

        // Basics
        println!("{}", "║  Basics".bright_blue().bold());
        println!("║    • Write SQL; end with ';' to run");
        println!(
            "║    • Autocomplete: keywords, namespaces, tables, columns  {}",
            "(Tab)".dimmed()
        );
        println!("║    • Inline hints and SQL highlighting enabled");

        // Meta-commands (two columns)
        println!(
            "{}",
            "╠───────────────────────────────────────────────────────────╣"
                .bright_blue()
                .bold()
        );
        println!("{}", "║  Meta-Commands".bright_blue().bold());
        let left = [
            ("\\help, \\?", "Show this help"),
            ("\\quit, \\q", "Exit CLI"),
            ("\\info, \\session", "Session info"),
            ("\\sessions", "PG gRPC sessions"),
            ("\\stats, \\metrics", "System stats"),
            ("\\health", "Health check"),
            ("\\format <type>", "table|json|csv"),
            ("\\history, \\h", "Browse history"),
            ("\\refresh-tables, \\refresh", "Refresh autocomplete"),
        ];
        let right = [
            ("\\dt, \\tables", "List tables"),
            ("\\d, \\describe <table>", "Describe table"),
            ("\\flush", "Run STORAGE FLUSH ALL"),
            ("\\pause", "Pause ingestion"),
            ("\\continue", "Resume ingestion"),
            ("\\subscribe, \\watch <SQL>", "Start live query"),
            ("\\unsubscribe, \\unwatch", "Stop live query"),
            ("\\consume <topic>", "Consume topic messages"),
            ("\\cluster <subcommand>", "Cluster operations"),
        ];
        for i in 0..left.len().max(right.len()) {
            let l = left
                .get(i)
                .map(|(a, b)| format!("{:<28} {:<18}", a.cyan(), b))
                .unwrap_or_default();
            let r = right
                .get(i)
                .map(|(a, b)| format!("{:<28} {:<18}", a.cyan(), b))
                .unwrap_or_default();
            println!("║  {:<47}{:<47} ║", l, r);
        }

        // Cluster Commands
        println!(
            "{}",
            "╠───────────────────────────────────────────────────────────╣"
                .bright_blue()
                .bold()
        );
        println!("{}", "║  Cluster Commands".bright_blue().bold());
        println!("║    {:<48} Trigger snapshot", "\\cluster snapshot".cyan());
        println!(
            "║    {:<48} Purge logs up to index",
            "\\cluster purge --upto <index>".cyan()
        );
        println!(
            "║    {:<48} Trigger cluster election",
            "\\cluster trigger-election".cyan()
        );
        println!(
            "║    {:<48} Transfer cluster leadership",
            "\\cluster transfer-leader <node_id>".cyan()
        );
        println!("║    {:<48} Leader stepdown", "\\cluster stepdown".cyan());
        println!("║    {:<48} Clear old snapshots", "\\cluster clear".cyan());
        println!("║    {:<48} List cluster nodes", "\\cluster list".cyan());
        println!(
            "║    {:<48} List all raft groups",
            "\\cluster list groups".cyan()
        );
        println!("║    {:<48} Cluster status", "\\cluster status".cyan());
        println!(
            "║    {:<48} Join node {}",
            "\\cluster join <addr>".cyan(),
            "(not implemented)".yellow()
        );
        println!(
            "║    {:<48} Leave cluster {}",
            "\\cluster leave".cyan(),
            "(not implemented)".yellow()
        );
        println!(
            "║    {:<48} Live per-node stats",
            "\\subscribe SELECT * FROM system.cluster".cyan()
        );

        // Credentials
        println!(
            "{}",
            "╠───────────────────────────────────────────────────────────╣"
                .bright_blue()
                .bold()
        );
        println!("{}", "║  Credentials".bright_blue().bold());
        println!(
            "║    {:<32} Show stored credentials",
            "\\show-credentials, \\credentials".cyan()
        );
        println!(
            "║    {:<32} Update credentials",
            "\\update-credentials <u> <p>".cyan()
        );
        println!(
            "║    {:<32} Delete stored credentials",
            "\\delete-credentials".cyan()
        );

        // Topic Consumption
        println!(
            "{}",
            "╠───────────────────────────────────────────────────────────╣"
                .bright_blue()
                .bold()
        );
        println!("{}", "║  Topic Consumption".bright_blue().bold());
        println!("║    {:<48} Basic consume", "\\consume app.events".cyan());
        println!(
            "║    {:<48} With consumer group",
            "\\consume app.events --group my-group".cyan()
        );
        println!(
            "║    {:<48} From earliest offset",
            "\\consume app.events --from earliest --limit 10".cyan()
        );
        println!(
            "║    {}",
            "CLI args: kalam --consume --topic app.events --group my-group".green()
        );

        // Tips & examples
        println!(
            "{}",
            "╠───────────────────────────────────────────────────────────╣"
                .bright_blue()
                .bold()
        );
        println!("{}", "║  Examples".bright_blue().bold());
        println!("║    {}", "SELECT * FROM system.tables LIMIT 5;".green());
        println!("║    {}", "SELECT name FROM system.namespaces;".green());
        println!("║    {}", "\\cluster status".green());

        println!(
            "{}",
            "╚═══════════════════════════════════════════════════════════╝"
                .bright_blue()
                .bold()
        );
        println!();
    }

    /// Consume messages from a topic
    pub async fn cmd_consume(
        &mut self,
        topic: &str,
        group: Option<&str>,
        from: Option<&str>,
        limit: Option<usize>,
        timeout: Option<u64>,
    ) -> Result<()> {
        use kalam_client::consumer::AutoOffsetReset;
        use tokio::signal;
        use tokio::time::{sleep, Duration};

        // Warn if no consumer group specified
        if group.is_none() {
            println!(
                "{}",
                "⚠️  Running without consumer group - offsets will not be saved".yellow()
            );
            println!("{}", "   Use --group NAME to persist progress".dimmed());
            println!();
        }

        // Build consumer
        let mut builder = self.client.consumer().topic(topic);

        if let Some(group_id) = group {
            builder = builder.group_id(group_id);
        }

        // Parse from offset
        if let Some(from_str) = from {
            let auto_offset = match from_str.to_lowercase().as_str() {
                "earliest" => AutoOffsetReset::Earliest,
                "latest" => AutoOffsetReset::Latest,
                offset_str => {
                    if let Ok(offset) = offset_str.parse::<u64>() {
                        AutoOffsetReset::Offset(offset)
                    } else {
                        return Err(CLIError::ParseError(format!(
                            "Invalid --from value: {}. Use 'earliest', 'latest', or a numeric offset",
                            from_str
                        )));
                    }
                },
            };
            builder = builder.auto_offset_reset(auto_offset);
        }

        let mut consumer = builder.build().map_err(|e| CLIError::LinkError(e))?;

        // Print header
        println!("{}", format!("Consuming from topic: {}", topic).bright_green().bold());
        if let Some(group_id) = group {
            println!("{}", format!("  Consumer group: {}", group_id).dimmed());
        }
        if let Some(from_str) = from {
            println!("{}", format!("  Starting from: {}", from_str).dimmed());
        }
        if let Some(limit_val) = limit {
            println!("{}", format!("  Limit: {} messages", limit_val).dimmed());
        }
        if let Some(timeout_val) = timeout {
            println!("{}", format!("  Timeout: {}s", timeout_val).dimmed());
        }
        println!("{}", "Press Ctrl+C to stop...".dimmed());
        println!();

        // CSV header for CSV format
        if matches!(self.format, OutputFormat::Csv) {
            println!("offset,operation,payload");
        }

        let start_time = Instant::now();
        let mut total_consumed = 0_usize;
        let mut last_offset = 0_u64;
        let mut error_count = 0;

        // Set up Ctrl+C handler
        let ctrl_c = signal::ctrl_c();
        tokio::pin!(ctrl_c);

        // Poll loop - library handles long polling (30s default)
        loop {
            // Check timeout
            if let Some(timeout_seconds) = timeout {
                if start_time.elapsed().as_secs() >= timeout_seconds {
                    println!();
                    println!("{}", "⏱️  Timeout reached".yellow());
                    break;
                }
            }

            // Check limit
            if let Some(limit_val) = limit {
                if total_consumed >= limit_val {
                    break;
                }
            }

            // Poll with long polling (30s) - library handles the HTTP request timeout
            // Use tokio::select to handle Ctrl+C during the poll
            let poll_result = tokio::select! {
                _ = &mut ctrl_c => {
                    println!();
                    println!("{}", "⚠️  Interrupted by user (Ctrl+C)".yellow());
                    break;
                }
                result = consumer.poll() => result
            };

            let records = match poll_result {
                Ok(records) => {
                    error_count = 0; // Reset error count on success
                    records
                },
                Err(e) => {
                    error_count += 1;

                    // Format detailed error message
                    let error_msg = format!("{}", e);
                    let detailed_error = if error_msg.contains("404") {
                        format!(
                            "❌ Topic '{}' not found or consume endpoint not available.\n   {}",
                            topic,
                            "Create the topic with: CREATE TOPIC <name> SOURCE TABLE <namespace>.<table>".dimmed()
                        )
                    } else if error_msg.contains("401") || error_msg.contains("403") {
                        format!(
                            "❌ Authentication failed or insufficient permissions.\n   Error: {}",
                            error_msg
                        )
                    } else if error_msg.contains("500") {
                        format!(
                            "❌ Server error while consuming from topic '{}'.\n   Error: {}",
                            topic, error_msg
                        )
                    } else {
                        format!("❌ Poll error: {}", error_msg)
                    };

                    eprintln!("{}", detailed_error.red());

                    // Exit after 3 consecutive errors instead of infinite retry
                    if error_count >= 3 {
                        eprintln!();
                        eprintln!("{}", "❌ Too many consecutive errors. Exiting.".red().bold());
                        break;
                    }

                    sleep(Duration::from_secs(1)).await;
                    continue;
                },
            };

            if records.is_empty() {
                // No messages, continue polling
                continue;
            }

            // Process and display records
            for record in records {
                last_offset = record.offset;

                // Format operation as string
                let op_str = match record.op {
                    kalam_client::consumer::TopicOp::Insert => "INSERT",
                    kalam_client::consumer::TopicOp::Update => "UPDATE",
                    kalam_client::consumer::TopicOp::Delete => "DELETE",
                };

                // Format and display record
                let formatted =
                    self.formatter.format_consumer_record(record.offset, op_str, &record.payload);
                println!("{}", formatted);

                // Mark as processed
                consumer.mark_processed(&record);

                total_consumed += 1;

                // Check limit after each record
                if let Some(limit_val) = limit {
                    if total_consumed >= limit_val {
                        break;
                    }
                }
            }
        }

        // Commit offsets before exit
        println!();
        if group.is_some() {
            print!("Committing offsets... ");
            match consumer.commit_sync().await {
                Ok(result) => {
                    println!(
                        "{}",
                        format!("✓ Committed offset: {}", result.acknowledged_offset).green()
                    );
                },
                Err(e) => {
                    println!("{}", format!("✗ Failed: {}", e).red());
                },
            }
        }

        // Print summary
        println!();
        println!(
            "{}",
            format!(
                "Consumed {} message(s). Last offset: {}. Duration: {:.2}s",
                total_consumed,
                last_offset,
                start_time.elapsed().as_secs_f64()
            )
            .bright_cyan()
        );

        Ok(())
    }
}
