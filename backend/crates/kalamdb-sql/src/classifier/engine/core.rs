use crate::classifier::types::{SqlStatement, SqlStatementKind, StatementClassificationError};
use crate::ddl::*;
use crate::parser::utils::{collect_non_whitespace_tokens, parse_sql_statements, tokens_to_words};
use kalamdb_commons::models::NamespaceId;
use kalamdb_commons::Role;
use kalamdb_session::is_admin_role;

impl SqlStatement {
    /// Wrap a parsed statement into SqlStatement with sql_text
    fn wrap<F, E>(sql: &str, parser: F) -> Result<Self, StatementClassificationError>
    where
        F: FnOnce() -> Result<SqlStatementKind, E>,
        E: std::fmt::Display,
    {
        parser().map(|kind| Self::new(sql.to_string(), kind)).map_err(|err| {
            StatementClassificationError::InvalidSql {
                sql: sql.to_string(),
                message: err.to_string(),
            }
        })
    }

    /// Classify and parse SQL statement in one pass with authorization check
    ///
    /// This method combines classification, authorization, and parsing:
    /// - Hot path: SELECT/INSERT/DELETE check authorization then return immediately
    /// - DDL: Check authorization first, then parse and embed AST in enum variant
    /// - Authorization failures: Return Err immediately (fail-fast)
    /// - Parse errors: Return StatementClassificationError::InvalidSql
    ///
    /// Performance: 99% of queries (SELECT/INSERT/DELETE) bypass DDL parsing entirely.
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to classify and parse
    /// * `default_namespace` - Default namespace for unqualified table names
    /// * `role` - The user's role for authorization checking
    ///
    /// # Returns
    /// * `Ok(SqlStatement)` if authorized and parsed successfully
    /// * `Err(String)` if authorization failed
    pub fn classify_and_parse(
        sql: &str,
        default_namespace: &NamespaceId,
        role: Role,
    ) -> Result<Self, StatementClassificationError> {
        // Use sqlparser's parser lookahead to tokenize without full parsing
        let dialect = sqlparser::dialect::GenericDialect {};
        let tokens = collect_non_whitespace_tokens(sql, &dialect).unwrap_or_default();

        // Build words list from tokens for pattern matching
        let mut words: Vec<String> = if !tokens.is_empty() {
            tokens_to_words(&tokens)
        } else {
            Vec::new()
        };

        if words.is_empty() {
            let mut sql_upper = sql.trim().to_string();
            sql_upper.make_ascii_uppercase();
            words = sql_upper.split_whitespace().map(|s| s.to_string()).collect();
        }

        let first_keyword_upper = words.first().cloned().unwrap_or_default();
        let word_refs: Vec<&str> = words.iter().map(|s| s.as_str()).collect();

        if word_refs.is_empty() {
            return Ok(Self::new(sql.to_string(), SqlStatementKind::Unknown));
        }

        // Admin users (DBA, System) can do anything - skip authorization checks
        let is_admin = is_admin_role(role);

        // Hot path: Check SELECT/INSERT/DELETE first (99% of queries)
        // DML statements - create typed markers for handler pattern
        // CTEs (WITH clause) are treated as SELECT queries
        match first_keyword_upper.as_str() {
            "SELECT" | "WITH" => {
                return Ok(Self::new(sql.to_string(), SqlStatementKind::Select));
            },
            "INSERT" => {
                return Ok(Self::new(
                    sql.to_string(),
                    SqlStatementKind::Insert(crate::ddl::InsertStatement),
                ));
            },
            "DELETE" => {
                return Ok(Self::new(
                    sql.to_string(),
                    SqlStatementKind::Delete(crate::ddl::DeleteStatement),
                ));
            },
            "UPDATE" => {
                return Ok(Self::new(
                    sql.to_string(),
                    SqlStatementKind::Update(crate::ddl::UpdateStatement),
                ));
            },
            _ => {},
        }

        // Check multi-word prefixes and parse DDL statements
        match word_refs.as_slice() {
            // Namespace operations - require admin
            ["CREATE", "NAMESPACE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for namespace operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    CreateNamespaceStatement::parse(sql).map(SqlStatementKind::CreateNamespace)
                })
            },
            ["ALTER", "NAMESPACE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for namespace operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    AlterNamespaceStatement::parse(sql).map(SqlStatementKind::AlterNamespace)
                })
            },
            ["DROP", "NAMESPACE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for namespace operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    DropNamespaceStatement::parse(sql).map(SqlStatementKind::DropNamespace)
                })
            },
            ["SHOW", "NAMESPACES", ..] => {
                // Read-only, allowed for all users
                Self::wrap(sql, || {
                    ShowNamespacesStatement::parse(sql).map(SqlStatementKind::ShowNamespaces)
                })
            },

            // USE NAMESPACE / USE / SET NAMESPACE - switch default schema
            ["USE", "NAMESPACE", ..] | ["SET", "NAMESPACE", ..] => {
                // Allowed for all users (actual table access is checked per-query)
                Self::wrap(sql, || {
                    UseNamespaceStatement::parse(sql).map(SqlStatementKind::UseNamespace)
                })
            },
            ["USE", ..] => {
                // USE <name> shorthand (USE NAMESPACE is handled above)
                Self::wrap(sql, || {
                    UseNamespaceStatement::parse(sql).map(SqlStatementKind::UseNamespace)
                })
            },

            // Storage operations - require admin
            ["CREATE", "STORAGE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for storage operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    CreateStorageStatement::parse(sql).map(SqlStatementKind::CreateStorage)
                })
            },
            ["ALTER", "STORAGE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for storage operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    AlterStorageStatement::parse(sql).map(SqlStatementKind::AlterStorage)
                })
            },
            ["DROP", "STORAGE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for storage operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    DropStorageStatement::parse(sql).map(SqlStatementKind::DropStorage)
                })
            },
            ["SHOW", "STORAGES", ..] => {
                // Read-only, allowed for all users
                Self::wrap(sql, || {
                    ShowStoragesStatement::parse(sql).map(SqlStatementKind::ShowStorages)
                })
            },

            // View operations - DataFusion compatibility (authorization handled in handler)
            _ if is_create_view(&word_refs) => {
                match CreateViewStatement::parse(sql, default_namespace) {
                    Ok(stmt) => Ok(Self::new(sql.to_string(), SqlStatementKind::CreateView(stmt))),
                    Err(e) => {
                        log::error!(
                            target: "sql::parse",
                            "❌ CREATE VIEW parsing failed | sql='{}' | error='{}'",
                            sql,
                            e
                        );
                        Err(StatementClassificationError::InvalidSql {
                            sql: sql.to_string(),
                            message: format!("Invalid CREATE VIEW statement: {}", e),
                        })
                    },
                }
            },

            // Table operations - authorization deferred to table ownership checks
            ["CREATE", "USER", "TABLE", ..]
            | ["CREATE", "SHARED", "TABLE", ..]
            | ["CREATE", "STREAM", "TABLE", ..]
            | ["CREATE", "TABLE", ..] => {
                // Parse CREATE TABLE statement with detailed error logging
                match CreateTableStatement::parse(sql, default_namespace.as_str()) {
                    Ok(stmt) => Ok(Self::new(sql.to_string(), SqlStatementKind::CreateTable(stmt))),
                    Err(e) => {
                        log::error!(
                            target: "sql::parse",
                            "❌ CREATE TABLE parsing failed | sql='{}' | error='{}'",
                            sql,
                            e
                        );
                        // Return the error to the user instead of Unknown
                        Err(StatementClassificationError::InvalidSql {
                            sql: sql.to_string(),
                            message: format!("Invalid CREATE TABLE statement: {}", e),
                        })
                    },
                }
            },
            ["ALTER", "TABLE", ..]
            | ["ALTER", "USER", "TABLE", ..]
            | ["ALTER", "SHARED", "TABLE", ..]
            | ["ALTER", "STREAM", "TABLE", ..] => Self::wrap(sql, || {
                AlterTableStatement::parse(sql, default_namespace).map(SqlStatementKind::AlterTable)
            }),
            ["DROP", "USER", "TABLE", ..]
            | ["DROP", "SHARED", "TABLE", ..]
            | ["DROP", "STREAM", "TABLE", ..]
            | ["DROP", "TABLE", ..] => Self::wrap(sql, || {
                DropTableStatement::parse(sql, default_namespace).map(SqlStatementKind::DropTable)
            }),
            ["SHOW", "TABLES", ..] => {
                // Read-only, allowed for all users
                Self::wrap(sql, || {
                    ShowTablesStatement::parse(sql).map(SqlStatementKind::ShowTables)
                })
            },
            ["DESCRIBE", "TABLE", ..] | ["DESC", "TABLE", ..] => {
                // Read-only, allowed for all users
                Self::wrap(sql, || {
                    DescribeTableStatement::parse(sql).map(SqlStatementKind::DescribeTable)
                })
            },
            ["SHOW", "STATS", ..] => {
                // Read-only, allowed for all users
                Self::wrap(sql, || {
                    ShowTableStatsStatement::parse(sql).map(SqlStatementKind::ShowStats)
                })
            },

            // Storage maintenance operations - authorization deferred to table ownership checks
            ["STORAGE", "FLUSH", "ALL", ..] => Self::wrap(sql, || {
                FlushAllTablesStatement::parse_with_default(sql, default_namespace)
                    .map(SqlStatementKind::FlushAllTables)
            }),
            ["STORAGE", "FLUSH", "TABLE", ..] => Self::wrap(sql, || {
                FlushTableStatement::parse(sql).map(SqlStatementKind::FlushTable)
            }),
            ["STORAGE", "COMPACT", "ALL", ..] => Self::wrap(sql, || {
                CompactAllTablesStatement::parse_with_default(sql, default_namespace)
                    .map(SqlStatementKind::CompactAllTables)
            }),
            ["STORAGE", "COMPACT", "TABLE", ..] => Self::wrap(sql, || {
                CompactTableStatement::parse(sql).map(SqlStatementKind::CompactTable)
            }),
            ["STORAGE", "CHECK", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for storage operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    CheckStorageStatement::parse(sql).map(SqlStatementKind::CheckStorage)
                })
            },
            ["SHOW", "MANIFEST"] | ["SHOW", "MANIFEST", "CACHE", ..] => {
                Self::wrap(sql, || {
                    ShowManifestStatement::parse(sql).map(SqlStatementKind::ShowManifest)
                })
            },
            // Show user export status / download link
            ["SHOW", "EXPORT", ..] => Self::wrap(sql, || {
                crate::ddl::export_commands::ShowExportStatement::parse(sql)
                    .map(SqlStatementKind::ShowExport)
            }),

            // Job management - require admin
            ["KILL", "JOB", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for job management"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || parse_job_command(sql).map(SqlStatementKind::KillJob))
            },
            ["KILL", "LIVE", "QUERY", ..] => {
                // Users can kill their own live queries
                Self::wrap(sql, || {
                    KillLiveQueryStatement::parse(sql).map(SqlStatementKind::KillLiveQuery)
                })
            },

            // Cluster operations - require admin (except CLUSTER LIST which is read-only)
            ["CLUSTER", "SNAPSHOT", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for cluster operations"
                            .to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterSnapshot))
            },
            ["CLUSTER", "PURGE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for cluster operations"
                            .to_string(),
                    ));
                }

                let tokens_no_ws: Vec<&sqlparser::tokenizer::Token> = tokens
                    .iter()
                    .filter(|tok| !matches!(tok, sqlparser::tokenizer::Token::Whitespace(_)))
                    .collect();

                let parse_u64 = |token: &sqlparser::tokenizer::Token| -> Option<u64> {
                    match token {
                        sqlparser::tokenizer::Token::Number(value, _) => value.parse::<u64>().ok(),
                        sqlparser::tokenizer::Token::Word(word) => word.value.parse::<u64>().ok(),
                        _ => None,
                    }
                };

                let is_upto = |token: &sqlparser::tokenizer::Token| match token {
                    sqlparser::tokenizer::Token::Word(word) => {
                        word.value.eq_ignore_ascii_case("upto")
                    },
                    _ => false,
                };

                let upto = tokens_no_ws
                    .iter()
                    .enumerate()
                    .find_map(|(idx, _)| {
                        if idx + 3 < tokens_no_ws.len()
                            && matches!(tokens_no_ws[idx], sqlparser::tokenizer::Token::Minus)
                            && matches!(tokens_no_ws[idx + 1], sqlparser::tokenizer::Token::Minus)
                            && is_upto(tokens_no_ws[idx + 2])
                        {
                            parse_u64(tokens_no_ws[idx + 3])
                        } else {
                            None
                        }
                    })
                    .or_else(|| tokens_no_ws.iter().find_map(|t| parse_u64(t)));

                let Some(upto) = upto else {
                    return Err(StatementClassificationError::InvalidSql {
                        sql: sql.to_string(),
                        message: "CLUSTER PURGE requires --upto <index> or a numeric index"
                            .to_string(),
                    });
                };

                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterPurge(upto)))
            },
            ["CLUSTER", "TRIGGER", "ELECTION", ..] | ["CLUSTER", "TRIGGER-ELECTION", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for cluster operations"
                            .to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterTriggerElection))
            },
            ["CLUSTER", "TRANSFER", "LEADER", node_id, ..]
            | ["CLUSTER", "TRANSFER-LEADER", node_id, ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for cluster operations"
                            .to_string(),
                    ));
                }
                let node_id = node_id.parse::<u64>().map_err(|_| {
                    StatementClassificationError::InvalidSql {
                        sql: sql.to_string(),
                        message: "CLUSTER TRANSFER-LEADER requires a numeric node id".to_string(),
                    }
                })?;
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterTransferLeader(node_id)))
            },
            ["CLUSTER", "STEPDOWN", ..] | ["CLUSTER", "STEP-DOWN", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for cluster operations"
                            .to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterStepdown))
            },
            ["CLUSTER", "CLEAR", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for cluster operations"
                            .to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterClear))
            },
            ["CLUSTER", "LIST", ..] => {
                // Read-only, allowed for all users
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterList))
            },
            ["CLUSTER", "STATUS", ..] | ["CLUSTER", "LS", ..] => {
                // Read-only, allowed for all users
                Ok(Self::new(sql.to_string(), SqlStatementKind::ClusterList))
            },
            ["CLUSTER", "JOIN", ..] | ["CLUSTER", "LEAVE", ..] => {
                Err(StatementClassificationError::InvalidSql {
                    sql: sql.to_string(),
                    message: "CLUSTER JOIN/LEAVE commands were removed".to_string(),
                })
            },

            // Transaction control (no parsing needed - just markers)
            ["BEGIN", ..] | ["START", "TRANSACTION", ..] => {
                Ok(Self::new(sql.to_string(), SqlStatementKind::BeginTransaction))
            },
            ["COMMIT", ..] => Ok(Self::new(sql.to_string(), SqlStatementKind::CommitTransaction)),
            ["ROLLBACK", ..] => {
                Ok(Self::new(sql.to_string(), SqlStatementKind::RollbackTransaction))
            },

            // Live query subscriptions - allowed for all users
            ["SUBSCRIBE", "TO", ..] => {
                Self::wrap(sql, || SubscribeStatement::parse(sql).map(SqlStatementKind::Subscribe))
            },

            // Topic pub/sub commands
            ["CREATE", "TOPIC", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for topic management"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    crate::ddl::topic_commands::parse_create_topic(sql)
                        .map(SqlStatementKind::CreateTopic)
                })
            },
            ["DROP", "TOPIC", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for topic management"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    crate::ddl::topic_commands::parse_drop_topic(sql)
                        .map(SqlStatementKind::DropTopic)
                })
            },
            ["CLEAR", "TOPIC", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for topic management"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    crate::ddl::topic_commands::parse_clear_topic(sql)
                        .map(SqlStatementKind::ClearTopic)
                })
            },
            ["ALTER", "TOPIC", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for topic management"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    crate::ddl::topic_commands::parse_alter_topic_add_source(sql)
                        .map(SqlStatementKind::AddTopicSource)
                })
            },
            ["CONSUME", "FROM", ..] | ["CONSUME", ..] => {
                // All authenticated users can consume from topics
                Self::wrap(sql, || {
                    crate::ddl::topic_commands::parse_consume(sql)
                        .map(SqlStatementKind::ConsumeTopic)
                })
            },
            ["ACK", ..] => {
                // All authenticated users can acknowledge topic offsets
                Self::wrap(sql, || {
                    crate::ddl::topic_commands::parse_ack(sql).map(SqlStatementKind::AckTopic)
                })
            },

            // Backup and restore operations - require admin
            ["BACKUP", "DATABASE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for backup operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    BackupDatabaseStatement::parse(sql).map(SqlStatementKind::BackupDatabase)
                })
            },
            ["RESTORE", "DATABASE", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for restore operations"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    RestoreDatabaseStatement::parse(sql).map(SqlStatementKind::RestoreDatabase)
                })
            },

            // Export user data - any authenticated user can export their own data
            ["EXPORT", "USER", "DATA", ..] => Self::wrap(sql, || {
                crate::ddl::export_commands::ExportUserDataStatement::parse(sql)
                    .map(SqlStatementKind::ExportUserData)
            }),

            // User management - require admin (except ALTER USER for self)
            ["CREATE", "USER", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for user management"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || {
                    CreateUserStatement::parse(sql).map(SqlStatementKind::CreateUser)
                })
            },
            ["ALTER", "USER", ..] => {
                // Authorization deferred to handler (users can alter their own account)
                Self::wrap(sql, || AlterUserStatement::parse(sql).map(SqlStatementKind::AlterUser))
            },
            ["DROP", "USER", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for user management"
                            .to_string(),
                    ));
                }
                Self::wrap(sql, || DropUserStatement::parse(sql).map(SqlStatementKind::DropUser))
            },

            // DataFusion Meta Commands - Admin only
            // These commands are passed directly to DataFusion for parsing and execution.
            // No custom parsing needed - DataFusion handles them natively.
            // Supported: EXPLAIN, SET, SHOW (options), SHOW ALL, SHOW COLUMNS, DESCRIBE/DESC
            ["EXPLAIN", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for EXPLAIN commands"
                            .to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::DataFusionMetaCommand))
            },
            ["SET", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for SET commands"
                            .to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::DataFusionMetaCommand))
            },
            ["SHOW", "COLUMNS", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for SHOW COLUMNS"
                            .to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::DataFusionMetaCommand))
            },
            ["SHOW", "ALL", ..] => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for SHOW ALL".to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::DataFusionMetaCommand))
            },
            // DESCRIBE <table> or DESC <table> (without TABLE keyword) - DataFusion style
            // Note: DESCRIBE TABLE is handled above as KalamDB custom command
            ["DESCRIBE", next, ..] if *next != "TABLE" => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for DESCRIBE".to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::DataFusionMetaCommand))
            },
            ["DESC", next, ..] if *next != "TABLE" => {
                if !is_admin {
                    return Err(StatementClassificationError::Unauthorized(
                        "Admin privileges (DBA or System role) required for DESC".to_string(),
                    ));
                }
                Ok(Self::new(sql.to_string(), SqlStatementKind::DataFusionMetaCommand))
            },

            // Unknown
            _ => {
                let dialect = sqlparser::dialect::PostgreSqlDialect {};
                match parse_sql_statements(sql, &dialect) {
                    Ok(statements) => {
                        if let Some(statement) = statements.first() {
                            Err(StatementClassificationError::InvalidSql {
                                sql: sql.to_string(),
                                message: format!("Unsupported statement type: {}", statement),
                            })
                        } else {
                            Ok(Self::new(sql.to_string(), SqlStatementKind::Unknown))
                        }
                    },
                    Err(err) => Err(StatementClassificationError::InvalidSql {
                        sql: sql.to_string(),
                        message: err.to_string(),
                    }),
                }
            },
        }
    }

    /// Check if the given role is authorized to execute this statement
    ///
    /// Implements role-based access control (RBAC) with the following hierarchy:
    /// - System: Full access to all operations
    /// - DBA: Administrative operations (user management, namespace DDL, storage)
    /// - Service: Service account operations (limited DDL, full DML)
    /// - User: Standard user operations (DML only, table-level authorization)
    ///
    /// # Arguments
    /// * `role` - The user's role
    ///
    /// # Returns
    /// * `Ok(())` if authorized
    /// * `Err(String)` with error message if not authorized
    ///
    /// # Authorization Rules
    /// 1. Admin users (DBA, System) can execute any statement
    /// 2. DDL operations (CREATE/ALTER/DROP) require DBA+ role
    /// 3. User management (CREATE/ALTER/DROP USER) requires DBA+ role
    /// 4. Storage operations require DBA+ role
    /// 5. Read-only operations (SELECT, SHOW, DESCRIBE) allowed for all authenticated users
    /// 6. Table-level operations (CREATE/ALTER/DROP TABLE) defer to per-table authorization
    /// 7. DML operations defer to table-level access control
    pub fn check_authorization(&self, role: Role) -> Result<(), String> {
        // Admin users (DBA, System) can do anything
        if matches!(role, Role::Dba | Role::System) {
            return Ok(());
        }

        match &self.kind {
            // Storage and global operations require admin privileges
            SqlStatementKind::CreateStorage(_)
            | SqlStatementKind::AlterStorage(_)
            | SqlStatementKind::DropStorage(_)
            | SqlStatementKind::KillJob(_)
            | SqlStatementKind::ClusterSnapshot
            | SqlStatementKind::ClusterPurge(_)
            | SqlStatementKind::ClusterTriggerElection
            | SqlStatementKind::ClusterTransferLeader(_)
            | SqlStatementKind::ClusterStepdown
            | SqlStatementKind::ClusterClear
            | SqlStatementKind::ClusterList => Err(
                "Admin privileges (DBA or System role) required for storage and cluster operations"
                    .to_string(),
            ),

            // User management requires admin privileges (except for self-modification in ALTER USER)
            SqlStatementKind::CreateUser(_) | SqlStatementKind::DropUser(_) => {
                Err("Admin privileges (DBA or System role) required for user management"
                    .to_string())
            },

            // ALTER USER allowed for self (changing own password), admin for others
            // The actual target user check is deferred to execute_alter_user method
            SqlStatementKind::AlterUser(_) => Ok(()),

            // Namespace DDL requires admin privileges
            SqlStatementKind::CreateNamespace(_)
            | SqlStatementKind::AlterNamespace(_)
            | SqlStatementKind::DropNamespace(_) => {
                Err("Admin privileges (DBA or System role) required for namespace operations"
                    .to_string())
            },

            // Read-only operations on system tables are allowed for all authenticated users
            SqlStatementKind::ShowNamespaces(_)
            | SqlStatementKind::ShowTables(_)
            | SqlStatementKind::ShowStorages(_)
            | SqlStatementKind::CheckStorage(_)
            | SqlStatementKind::ShowStats(_)
            | SqlStatementKind::ShowManifest(_)
            | SqlStatementKind::DescribeTable(_)
            | SqlStatementKind::UseNamespace(_) => Ok(()),

            // CREATE TABLE/VIEW, DROP TABLE, STORAGE FLUSH/COMPACT, ALTER TABLE - defer to ownership checks
            SqlStatementKind::CreateTable(_)
            | SqlStatementKind::CreateView(_)
            | SqlStatementKind::AlterTable(_)
            | SqlStatementKind::DropTable(_)
            | SqlStatementKind::FlushTable(_)
            | SqlStatementKind::FlushAllTables(_)
            | SqlStatementKind::CompactTable(_)
            | SqlStatementKind::CompactAllTables(_) => {
                // Table-level authorization will be checked in the execution methods
                // Users can only create/modify/drop tables they own
                // Admin users can operate on any table (already returned above)
                Ok(())
            },

            // SELECT, INSERT, UPDATE, DELETE - defer to table access control
            SqlStatementKind::Select
            | SqlStatementKind::Insert(_)
            | SqlStatementKind::Update(_)
            | SqlStatementKind::Delete(_) => {
                // Query-level authorization will be enforced by using per-user sessions
                // User tables are filtered by user_id in UserTableProvider
                // Shared tables enforce access control based on access_level
                Ok(())
            },

            // Subscriptions, transactions, and other operations allowed for all users
            SqlStatementKind::Subscribe(_)
            | SqlStatementKind::ConsumeTopic(_)
            | SqlStatementKind::AckTopic(_)
            | SqlStatementKind::KillLiveQuery(_)
            | SqlStatementKind::ExportUserData(_)
            | SqlStatementKind::ShowExport(_)
            | SqlStatementKind::BeginTransaction
            | SqlStatementKind::CommitTransaction
            | SqlStatementKind::RollbackTransaction => Ok(()),

            // Topic management requires admin
            SqlStatementKind::CreateTopic(_)
            | SqlStatementKind::DropTopic(_)
            | SqlStatementKind::ClearTopic(_)
            | SqlStatementKind::AddTopicSource(_) => {
                Err("Admin privileges (DBA or System role) required for topic management"
                    .to_string())
            },

            // Backup/Restore requires admin
            SqlStatementKind::BackupDatabase(_) | SqlStatementKind::RestoreDatabase(_) => {
                Err("Admin privileges (DBA or System role) required for backup/restore operations"
                    .to_string())
            },

            // DataFusion meta commands are already admin-checked in classify_from_tokens
            // This branch should only be reached by admin users (DBA/System)
            SqlStatementKind::DataFusionMetaCommand => Ok(()),

            SqlStatementKind::Unknown => {
                // Unknown statements will fail in execute anyway
                // Allow them through so we can return a better error message
                Ok(())
            },
        }
    }
}

fn is_create_view(tokens: &[&str]) -> bool {
    if tokens.first().copied() != Some("CREATE") {
        return false;
    }

    let Some(view_pos) = tokens.iter().position(|t| *t == "VIEW") else {
        return false;
    };

    tokens[1..view_pos]
        .iter()
        .all(|token| matches!(*token, "OR" | "REPLACE" | "TEMP" | "TEMPORARY" | "MATERIALIZED"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_select_keeps_original_sql() {
        let sql = "SELECT * FROM default.tasks WHERE id = 1";
        let stmt = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User)
            .expect("SELECT should classify");

        assert!(matches!(stmt.kind(), SqlStatementKind::Select));
        assert_eq!(stmt.as_str(), sql);
    }

    #[test]
    fn classify_select_with_current_user_call_defers_to_datafusion() {
        let sql = "SELECT current_user()";
        let stmt = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User)
            .expect("SELECT should classify without custom parser rejection");

        assert!(matches!(stmt.kind(), SqlStatementKind::Select));
        assert_eq!(stmt.as_str(), sql);
    }

    #[test]
    fn classify_select_with_custom_udf_call_defers_to_datafusion() {
        let sql = "SELECT SNOWFLAKE_ID()";
        let stmt = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User)
            .expect("SELECT should classify without custom parser rejection");

        assert!(matches!(stmt.kind(), SqlStatementKind::Select));
        assert_eq!(stmt.as_str(), sql);
    }

    #[test]
    fn classify_dml_defers_parse_errors_to_datafusion() {
        let sql = "UPDATE default.tasks SET value = WHERE id = 1";
        let stmt = SqlStatement::classify_and_parse(sql, &NamespaceId::new("default"), Role::User)
            .expect("UPDATE should classify and let DataFusion own parse/plan errors");

        assert!(matches!(stmt.kind(), SqlStatementKind::Update(_)));
        assert_eq!(stmt.as_str(), sql);
    }
}
