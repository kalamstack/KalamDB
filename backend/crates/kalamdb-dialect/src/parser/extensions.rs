//! KalamDB-specific SQL extensions.
//!
//! This module contains parsers for commands that are specific to KalamDB
//! and don't fit into standard SQL syntax:
//!
//! - **CREATE STORAGE**: Define cloud storage locations
//! - **ALTER STORAGE**: Modify storage configuration
//! - **STORAGE FLUSH / STORAGE COMPACT**: Trigger storage maintenance operations
//! - **KILL JOB**: Cancel background jobs
//! - **KILL LIVE QUERY**: Terminate WebSocket subscriptions
//!
//! These parsers complement the standard SQL parser and are invoked
//! when the standard parser doesn't recognize the syntax.

// Re-export SubscriptionOptions from kalamdb_commons
pub use kalamdb_commons::websocket::SubscriptionOptions;

// Re-export flush/compact commands
pub use crate::ddl::compact_commands::{CompactAllTablesStatement, CompactTableStatement};
pub use crate::ddl::flush_commands::{FlushAllTablesStatement, FlushTableStatement};
// Job commands (KILL JOB)
pub use crate::ddl::job_commands::{parse_job_command, JobCommand};
// Manifest cache commands (SHOW MANIFEST CACHE)
pub use crate::ddl::manifest_commands::ShowManifestStatement;
/// Re-export existing KalamDB-specific parsers for convenience.
///
/// These parsers handle commands that are unique to KalamDB and not part
/// of standard SQL. They are implemented in separate modules and re-exported
/// here for a unified parser interface.
// Re-export storage commands
pub use crate::ddl::storage_commands::{
    AlterStorageStatement, CreateStorageStatement, DropStorageStatement, ShowStoragesStatement,
};
// Subscribe commands (SUBSCRIBE TO)
pub use crate::ddl::subscribe_commands::SubscribeStatement;
// Topic pub/sub commands
pub use crate::ddl::topic_commands::{
    AddTopicSourceStatement, ClearTopicStatement, ConsumePosition, ConsumeStatement,
    CreateTopicStatement, DropTopicStatement,
};
// User commands (CREATE USER, ALTER USER, DROP USER)
pub use crate::ddl::user_commands::{
    AlterUserStatement, CreateUserStatement, DropUserStatement, UserModification,
};

/// Extension statement types that don't fit into standard SQL.
///
/// This enum represents KalamDB-specific commands that extend SQL
/// with custom functionality.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtensionStatement {
    /// CREATE STORAGE command
    CreateStorage(CreateStorageStatement),
    /// ALTER STORAGE command
    AlterStorage(AlterStorageStatement),
    /// DROP STORAGE command
    DropStorage(DropStorageStatement),
    /// SHOW STORAGES command
    ShowStorages(ShowStoragesStatement),
    /// STORAGE FLUSH TABLE command
    FlushTable(FlushTableStatement),
    /// STORAGE FLUSH ALL command
    FlushAllTables(FlushAllTablesStatement),
    /// STORAGE COMPACT TABLE command
    CompactTable(CompactTableStatement),
    /// STORAGE COMPACT ALL command
    CompactAllTables(CompactAllTablesStatement),
    /// KILL JOB command
    KillJob(JobCommand),
    /// SUBSCRIBE TO command (for live query subscriptions)
    Subscribe(SubscribeStatement),
    /// CREATE TOPIC command (pub/sub)
    CreateTopic(CreateTopicStatement),
    /// DROP TOPIC command (pub/sub)
    DropTopic(DropTopicStatement),
    /// CLEAR TOPIC command (pub/sub)
    ClearTopic(ClearTopicStatement),
    /// ALTER TOPIC ADD SOURCE command (pub/sub)
    AddTopicSource(AddTopicSourceStatement),
    /// CONSUME FROM command (pub/sub)
    ConsumeTopic(ConsumeStatement),
    /// CREATE USER command
    CreateUser(CreateUserStatement),
    /// ALTER USER command
    AlterUser(AlterUserStatement),
    /// DROP USER command
    DropUser(DropUserStatement),
    /// SHOW MANIFEST CACHE command
    ShowManifest(ShowManifestStatement),
    /// CLUSTER SNAPSHOT command
    ClusterSnapshot,
    /// CLUSTER PURGE command
    ClusterPurge { upto: u64 },
    /// CLUSTER TRIGGER ELECTION command
    ClusterTriggerElection,
    /// CLUSTER TRANSFER-LEADER command
    ClusterTransferLeader { node_id: u64 },
    /// CLUSTER JOIN command
    ClusterJoin {
        node_id: u64,
        rpc_addr: String,
        api_addr: String,
    },
    /// CLUSTER REBALANCE command
    ClusterRebalance,
    /// CLUSTER STEPDOWN command
    ClusterStepdown,
    /// CLUSTER CLEAR command
    ClusterClear,
    /// CLUSTER LIST command
    ClusterList,
}

impl ExtensionStatement {
    fn parse_with_prefix<T, E, F, G>(
        sql: &str,
        sql_upper: &str,
        prefixes: &[&str],
        parser: F,
        map: G,
        label: &str,
    ) -> Option<Result<Self, String>>
    where
        E: std::fmt::Display,
        F: FnOnce(&str) -> Result<T, E>,
        G: FnOnce(T) -> Self,
    {
        if prefixes.iter().any(|prefix| sql_upper.starts_with(prefix)) {
            Some(parser(sql).map(map).map_err(|e| format!("{} parsing failed: {}", label, e)))
        } else {
            None
        }
    }

    /// Parse a KalamDB-specific extension statement.
    ///
    /// Attempts to parse the SQL as one of the supported extension commands.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL string to parse
    ///
    /// # Returns
    ///
    /// The parsed extension statement, or an error if the syntax is invalid
    pub fn parse(sql: &str) -> Result<Self, String> {
        let sql_upper = sql.trim().to_uppercase();

        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["CREATE STORAGE"],
            CreateStorageStatement::parse,
            ExtensionStatement::CreateStorage,
            "CREATE STORAGE",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["ALTER STORAGE"],
            AlterStorageStatement::parse,
            ExtensionStatement::AlterStorage,
            "ALTER STORAGE",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["DROP STORAGE"],
            DropStorageStatement::parse,
            ExtensionStatement::DropStorage,
            "DROP STORAGE",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["SHOW STORAGES", "SHOW STORAGE"],
            ShowStoragesStatement::parse,
            ExtensionStatement::ShowStorages,
            "SHOW STORAGES",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["STORAGE FLUSH TABLE"],
            FlushTableStatement::parse,
            ExtensionStatement::FlushTable,
            "STORAGE FLUSH TABLE",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["STORAGE FLUSH ALL"],
            FlushAllTablesStatement::parse,
            ExtensionStatement::FlushAllTables,
            "STORAGE FLUSH ALL",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["STORAGE COMPACT TABLE"],
            CompactTableStatement::parse,
            ExtensionStatement::CompactTable,
            "STORAGE COMPACT TABLE",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["STORAGE COMPACT ALL"],
            CompactAllTablesStatement::parse,
            ExtensionStatement::CompactAllTables,
            "STORAGE COMPACT ALL",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["KILL JOB"],
            parse_job_command,
            ExtensionStatement::KillJob,
            "KILL JOB",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["SUBSCRIBE TO"],
            SubscribeStatement::parse,
            ExtensionStatement::Subscribe,
            "SUBSCRIBE TO",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["CREATE TOPIC"],
            crate::ddl::topic_commands::parse_create_topic,
            ExtensionStatement::CreateTopic,
            "CREATE TOPIC",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["DROP TOPIC"],
            crate::ddl::topic_commands::parse_drop_topic,
            ExtensionStatement::DropTopic,
            "DROP TOPIC",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["CLEAR TOPIC"],
            crate::ddl::topic_commands::parse_clear_topic,
            ExtensionStatement::ClearTopic,
            "CLEAR TOPIC",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["ALTER TOPIC"],
            crate::ddl::topic_commands::parse_alter_topic_add_source,
            ExtensionStatement::AddTopicSource,
            "ALTER TOPIC ADD SOURCE",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["CONSUME FROM", "CONSUME "],
            crate::ddl::topic_commands::parse_consume,
            ExtensionStatement::ConsumeTopic,
            "CONSUME FROM",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["CREATE USER"],
            CreateUserStatement::parse,
            ExtensionStatement::CreateUser,
            "CREATE USER",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["ALTER USER"],
            AlterUserStatement::parse,
            ExtensionStatement::AlterUser,
            "ALTER USER",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["DROP USER"],
            DropUserStatement::parse,
            ExtensionStatement::DropUser,
            "DROP USER",
        ) {
            return result;
        }
        if let Some(result) = Self::parse_with_prefix(
            sql,
            &sql_upper,
            &["SHOW MANIFEST"],
            ShowManifestStatement::parse,
            ExtensionStatement::ShowManifest,
            "SHOW MANIFEST",
        ) {
            return result;
        }

        // Try CLUSTER commands
        if sql_upper.starts_with("CLUSTER") {
            let parts: Vec<&str> = sql_upper.split_whitespace().collect();
            if parts.len() >= 2 {
                match parts[1] {
                    "SNAPSHOT" => return Ok(ExtensionStatement::ClusterSnapshot),
                    "PURGE" => {
                        let original_parts: Vec<&str> = sql.split_whitespace().collect();
                        let upto = original_parts
                            .iter()
                            .skip(2)
                            .find(|part| part.trim_start_matches('-').eq_ignore_ascii_case("upto"))
                            .and_then(|_| {
                                original_parts
                                    .iter()
                                    .skip(3)
                                    .find(|p| p.parse::<u64>().is_ok())
                                    .and_then(|p| p.parse::<u64>().ok())
                            })
                            .or_else(|| original_parts.get(2).and_then(|p| p.parse::<u64>().ok()));

                        if let Some(upto) = upto {
                            return Ok(ExtensionStatement::ClusterPurge { upto });
                        }
                        return Err(
                            "CLUSTER PURGE requires --upto <index> or a numeric index".to_string()
                        );
                    },
                    "TRIGGER" => {
                        if parts.get(2) == Some(&"ELECTION") {
                            return Ok(ExtensionStatement::ClusterTriggerElection);
                        }
                    },
                    "TRIGGER-ELECTION" => return Ok(ExtensionStatement::ClusterTriggerElection),
                    "TRANSFER" => {
                        if parts.get(2) == Some(&"LEADER") {
                            let node_id = parts.get(3).and_then(|id| id.parse::<u64>().ok());
                            if let Some(node_id) = node_id {
                                return Ok(ExtensionStatement::ClusterTransferLeader { node_id });
                            }
                            return Err("CLUSTER TRANSFER LEADER requires a node id".to_string());
                        }
                    },
                    "TRANSFER-LEADER" => {
                        let node_id = parts.get(2).and_then(|id| id.parse::<u64>().ok());
                        if let Some(node_id) = node_id {
                            return Ok(ExtensionStatement::ClusterTransferLeader { node_id });
                        }
                        return Err("CLUSTER TRANSFER-LEADER requires a node id".to_string());
                    },
                    "JOIN" => {
                        let original_parts: Vec<&str> = sql.split_whitespace().collect();
                        let node_id = original_parts
                            .get(2)
                            .and_then(|id| id.parse::<u64>().ok())
                            .ok_or_else(|| {
                            "CLUSTER JOIN requires a numeric node id".to_string()
                        })?;
                        let upper_parts: Vec<String> =
                            original_parts.iter().map(|part| part.to_ascii_uppercase()).collect();
                        let (rpc_addr, api_addr) =
                            if upper_parts.get(3).map(String::as_str) == Some("RPC") {
                                let rpc_addr = original_parts.get(4).ok_or_else(|| {
                                    "CLUSTER JOIN requires RPC address".to_string()
                                })?;
                                if upper_parts.get(5).map(String::as_str) != Some("API") {
                                    return Err("CLUSTER JOIN requires API address".to_string());
                                }
                                let api_addr = original_parts.get(6).ok_or_else(|| {
                                    "CLUSTER JOIN requires API address".to_string()
                                })?;
                                ((*rpc_addr).to_string(), (*api_addr).to_string())
                            } else {
                                let rpc_addr = original_parts.get(3).ok_or_else(|| {
                                    "CLUSTER JOIN requires RPC address".to_string()
                                })?;
                                let api_addr = original_parts.get(4).ok_or_else(|| {
                                    "CLUSTER JOIN requires API address".to_string()
                                })?;
                                ((*rpc_addr).to_string(), (*api_addr).to_string())
                            };

                        return Ok(ExtensionStatement::ClusterJoin {
                            node_id,
                            rpc_addr,
                            api_addr,
                        });
                    },
                    "REBALANCE" => return Ok(ExtensionStatement::ClusterRebalance),
                    "STEPDOWN" | "STEP-DOWN" => return Ok(ExtensionStatement::ClusterStepdown),
                    "CLEAR" => return Ok(ExtensionStatement::ClusterClear),
                    "LIST" | "LS" | "STATUS" => return Ok(ExtensionStatement::ClusterList),
                    "LEAVE" => {
                        return Err("CLUSTER LEAVE is not supported yet".to_string());
                    },
                    _ => {
                        return Err("Unknown CLUSTER subcommand. Supported: SNAPSHOT, PURGE, \
                                    TRIGGER ELECTION, TRANSFER-LEADER, JOIN, REBALANCE, STEPDOWN, \
                                    CLEAR, LIST"
                            .to_string())
                    },
                }
            }
        }

        Err("Unknown KalamDB extension command. Supported commands: CREATE/ALTER/DROP/SHOW \
             STORAGE, STORAGE FLUSH, STORAGE COMPACT, KILL JOB, SUBSCRIBE TO, CREATE/ALTER/DROP \
             USER, SHOW MANIFEST"
            .to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_storage() {
        let sql = "CREATE STORAGE my_storage TYPE filesystem NAME 'My Storage' BASE_DIRECTORY \
                   '/data' SHARED_TABLES_TEMPLATE '{namespace}/{table}/' USER_TABLES_TEMPLATE \
                   '{namespace}/{table}/{userId}/'";
        let result = ExtensionStatement::parse(sql);
        if let Err(ref e) = result {
            eprintln!("Parse error: {}", e);
        }
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ExtensionStatement::CreateStorage(_)));
    }

    #[test]
    fn test_parse_flush_table() {
        let sql = "STORAGE FLUSH TABLE prod.events";
        let result = ExtensionStatement::parse(sql);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ExtensionStatement::FlushTable(_)));
    }

    #[test]
    fn test_parse_compact_table() {
        let sql = "STORAGE COMPACT TABLE prod.events";
        let result = ExtensionStatement::parse(sql);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ExtensionStatement::CompactTable(_)));
    }

    #[test]
    fn test_parse_kill_job() {
        let sql = "KILL JOB 'job-123'";
        let result = ExtensionStatement::parse(sql);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ExtensionStatement::KillJob(_)));
    }

    #[test]
    fn test_parse_subscribe_to() {
        let sql = "SUBSCRIBE TO app.messages";
        let result = ExtensionStatement::parse(sql);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ExtensionStatement::Subscribe(_)));
    }

    #[test]
    fn test_parse_subscribe_with_where() {
        let sql = "SUBSCRIBE TO app.messages WHERE user_id = CURRENT_USER()";
        let result = ExtensionStatement::parse(sql);

        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), ExtensionStatement::Subscribe(_)));
    }

    #[test]
    fn test_parse_unknown_extension() {
        let sql = "CREATE FOOBAR something";
        let result = ExtensionStatement::parse(sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown KalamDB extension command"));
    }

    #[test]
    fn test_parse_cluster_join() {
        let sql = "CLUSTER JOIN 2 10.0.0.2:9188 http://10.0.0.2:8080";
        let result = ExtensionStatement::parse(sql);

        match result.unwrap() {
            ExtensionStatement::ClusterJoin {
                node_id,
                rpc_addr,
                api_addr,
            } => {
                assert_eq!(node_id, 2);
                assert_eq!(rpc_addr, "10.0.0.2:9188");
                assert_eq!(api_addr, "http://10.0.0.2:8080");
            },
            other => panic!("unexpected statement: {:?}", other),
        }
    }

    #[test]
    fn test_parse_cluster_rebalance() {
        let sql = "CLUSTER REBALANCE";
        let result = ExtensionStatement::parse(sql);

        assert!(matches!(result.unwrap(), ExtensionStatement::ClusterRebalance));
    }

    #[test]
    fn test_parse_cluster_leave_removed() {
        let sql = "CLUSTER LEAVE";
        let result = ExtensionStatement::parse(sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("CLUSTER LEAVE is not supported yet"));
    }
}
