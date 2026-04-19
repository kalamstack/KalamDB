use super::CLISession;
use crate::history::CommandHistory;
use crate::CLI_VERSION;
use colored::Colorize;
use kalam_client::KalamLinkError;

impl CLISession {
    pub(super) fn normalize_server_field(value: String) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    /// Show current session information
    ///
    /// Displays detailed information about the current CLI session
    pub(super) async fn show_session_info(&mut self) {
        let health_status = match self.client.health_check().await {
            Ok(health) => {
                self.connected = true;
                self.server_version = Self::normalize_server_field(health.version);
                self.server_api_version = Self::normalize_server_field(health.api_version);
                self.server_build_date = health.build_date.and_then(Self::normalize_server_field);
                None
            },
            Err(KalamLinkError::ServerError {
                status_code: 403, ..
            }) => {
                // Health endpoint is localhost-only; server is reachable but we can't
                // refresh version info. Preserve the current connected state.
                Some(
                    "Health endpoint is restricted to localhost (remote connection detected)"
                        .to_string(),
                )
            },
            Err(e) => {
                self.connected = false;
                self.server_version = None;
                self.server_api_version = None;
                self.server_build_date = None;
                Some(e.to_string())
            },
        };

        // Only fetch cluster details if the server is reachable right now
        let cluster_info = if self.connected {
            self.fetch_cluster_info().await
        } else {
            None
        };

        println!();
        println!("{}", "═══════════════════════════════════════".cyan().bold());
        println!("{}", "    Session Information".white().bold());
        println!("{}", "═══════════════════════════════════════".cyan().bold());
        println!();

        // Connection info
        println!("{}", "Connection:".yellow().bold());
        println!("  Server URL:     {}", self.server_url.green());
        println!("  User ID:        {}", self.username.green());
        println!(
            "  Connected:      {}",
            if self.connected {
                "Yes".green()
            } else {
                "No".red()
            }
        );
        if let Some(ref err) = health_status {
            if self.connected {
                // Server is reachable but health detail could not be retrieved (e.g. localhost-only restriction)
                println!("  Health check:   {}", format!("Note ({})", err).yellow());
            } else {
                println!("  Last check:     {}", format!("Failed ({})", err).red());
            }
        }

        // Session timing
        let uptime = self.connected_at.elapsed();
        let hours = uptime.as_secs() / 3600;
        let minutes = (uptime.as_secs() % 3600) / 60;
        let seconds = uptime.as_secs() % 60;
        let uptime_str = if hours > 0 {
            format!("{}h {}m {}s", hours, minutes, seconds)
        } else if minutes > 0 {
            format!("{}m {}s", minutes, seconds)
        } else {
            format!("{}s", seconds)
        };
        println!("  Session time:   {}", uptime_str.green());
        println!();

        // Server info
        println!("{}", "Server:".yellow().bold());
        if let Some(ref version) = self.server_version {
            println!("  Version:        {}", version.green());
        } else {
            println!("  Version:        {}", "Unknown".dimmed());
        }
        if let Some(ref api_version) = self.server_api_version {
            println!("  API Version:    {}", api_version.green());
        } else {
            println!("  API Version:    {}", "Unknown".dimmed());
        }
        if let Some(ref build_date) = self.server_build_date {
            println!("  Build Date:     {}", build_date.green());
        } else {
            println!("  Build Date:     {}", "Unknown".dimmed());
        }
        println!();

        // Cluster info (if server provided it)
        println!("{}", "Cluster:".yellow().bold());
        if let Some(ref info) = cluster_info {
            println!(
                "  Mode:           {}",
                if info.is_cluster_mode {
                    "Cluster".green()
                } else {
                    "Standalone".dimmed()
                }
            );
            if info.is_cluster_mode {
                // Show current node (the one we're connected to)
                if let Some(ref current_node) = info.current_node {
                    println!(
                        "  Connected Node: {}",
                        format!("Node {}", current_node.node_id).green()
                    );
                    println!("  Node Role:      {}", current_node.role.green());
                    println!("  Node API:       {}", current_node.api_addr.green());
                }
                // Show all cluster nodes
                println!();
                println!("  {}", "Cluster Nodes:".yellow());
                for node in &info.nodes {
                    let self_marker = if node.is_self { " (connected)" } else { "" };
                    let leader_marker = if node.is_leader { " [LEADER]" } else { "" };
                    println!(
                        "    Node {}: {} | {} | {}{}{}",
                        node.node_id,
                        node.role,
                        node.status,
                        node.api_addr,
                        leader_marker.yellow(),
                        self_marker.cyan()
                    );
                }
            }
        } else {
            println!("  Mode:           {}", "Standalone".dimmed());
            if self.connected {
                println!("  {}", "(Could not fetch cluster info)".dimmed());
            } else {
                println!("  {}", "(Server is currently unreachable)".dimmed());
            }
        }
        println!("  {}", "Use 'SELECT * FROM system.cluster' for full details".dimmed());
        println!();

        // CLI config
        let server_cfg = self.config.resolved_server();
        let conn_cfg = self.config.resolved_connection();
        let ui_cfg = self.config.resolved_ui();
        println!("{}", "CLI Config:".yellow().bold());
        println!("  Config File:   {}", self.config_path.display().to_string().green());
        println!(
            "  File Exists:   {}",
            if self.config_path.exists() {
                "Yes".green()
            } else {
                "No".red()
            }
        );
        println!("  Timeout:       {}s", server_cfg.timeout.to_string().green());
        println!("  Max Retries:   {}", server_cfg.max_retries.to_string().green());
        println!("  HTTP Version: {}", server_cfg.http_version.to_string().green());
        println!(
            "  Auto Reconnect: {}",
            if conn_cfg.auto_reconnect {
                "Yes".green()
            } else {
                "No".red()
            }
        );
        println!("  Reconnect Delay: {} ms", conn_cfg.reconnect_delay_ms.to_string().green());
        println!(
            "  Max Reconnect Delay: {} ms",
            conn_cfg.max_reconnect_delay_ms.to_string().green()
        );
        println!(
            "  Max Reconnect Attempts: {}",
            conn_cfg.max_reconnect_attempts.to_string().green()
        );
        println!("  Output Format: {}", ui_cfg.format.green());
        println!(
            "  Colors:        {}",
            if ui_cfg.color {
                "Enabled".green()
            } else {
                "Disabled".red()
            }
        );
        println!("  Timestamp Format: {}", ui_cfg.timestamp_format.green());
        println!("  History Size:  {}", ui_cfg.history_size.to_string().green());
        println!();

        // History info
        let history = CommandHistory::new(ui_cfg.history_size);
        let history_count = history.entry_count().unwrap_or(0);
        println!("{}", "History:".yellow().bold());
        println!("  History File:  {}", history.path().display().to_string().green());
        println!("  Entries:       {}", history_count.to_string().green());
        println!("  Max Entries:   {}", ui_cfg.history_size.to_string().green());
        println!();

        // Client info
        println!("{}", "Client:".yellow().bold());
        println!("  CLI Version:    {}", CLI_VERSION.green());
        println!("  Build Date:     {}", env!("BUILD_DATE").green());
        println!("  Git Branch:     {}", env!("GIT_BRANCH").green());
        println!("  Git Commit:     {}", env!("GIT_COMMIT_HASH").green());
        println!();

        // Session statistics
        println!("{}", "Statistics:".yellow().bold());
        println!("  Queries:        {}", self.queries_executed.to_string().green());
        println!("  Format:         {}", format!("{:?}", self.format).green());
        println!(
            "  Colors:         {}",
            if self.color {
                "Enabled".green()
            } else {
                "Disabled".red()
            }
        );
        println!();

        // Credentials info
        println!("{}", "Credentials:".yellow().bold());
        if let Some(ref instance) = self.instance {
            println!("  Instance:       {}", instance.green());
        }
        println!(
            "  Loaded:         {}",
            if self.credentials_loaded {
                "Yes (from stored credentials)".green()
            } else {
                "No (provided via CLI args)".dimmed()
            }
        );
        if self.credential_store.is_some() {
            println!(
                "  Storage:        {}",
                crate::credentials::FileCredentialStore::default_path()
                    .display()
                    .to_string()
                    .dimmed()
            );
        }
        println!();

        println!("{}", "═══════════════════════════════════════".cyan().bold());
        println!();
    }
}
