use super::types::CreateTableStatement;
use crate::compatibility::map_sql_type_to_kalam;
use crate::parser::utils::{format_span, parse_sql_statements};
use arrow::datatypes::{Field, Schema};
use kalamdb_commons::conversions::with_kalam_data_type_metadata;
use kalamdb_commons::models::datatypes::ToArrowType;
use kalamdb_commons::models::{NamespaceId, StorageId, TableAccess, TableName};
use kalamdb_commons::schemas::policy::FlushPolicy;
use kalamdb_commons::schemas::{ColumnDefault, TableType};
use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast::{ColumnOption, CreateTable, ObjectNamePart, Statement, TableConstraint};
use std::collections::HashMap;
use std::sync::Arc;

static RE_ALPHANUMERIC: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-zA-Z0-9_]+$").unwrap());
static RE_STORAGE_ID: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap());
static LEGACY_CREATE_PREFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*CREATE\s+(USER|SHARED|STREAM)\s+TABLE").unwrap());

impl CreateTableStatement {
    /// Parse a SQL statement into a CreateTableStatement
    pub fn parse(sql: &str, default_namespace: &str) -> Result<Self, String> {
        let (mut normalized_sql, legacy_table_type) = normalize_create_table_sql(sql);

        // Rewrite MySQL-style AUTO_INCREMENT into an explicit DEFAULT expression
        // so the parser consistently assigns SNOWFLAKE_ID() as the default value.
        // This makes AUTO_INCREMENT work even when the dialect treats it as
        // dialect-specific tokens or splits it into separate words.
        normalized_sql = normalized_sql.replace("AUTO_INCREMENT", "DEFAULT SNOWFLAKE_ID()");
        normalized_sql = normalized_sql.replace("auto_increment", "DEFAULT SNOWFLAKE_ID()");
        normalized_sql = normalized_sql.replace("AUTO INCREMENT", "DEFAULT SNOWFLAKE_ID()");
        normalized_sql = normalized_sql.replace("auto increment", "DEFAULT SNOWFLAKE_ID()");

        // Use PostgreSqlDialect because GenericDialect has issues with TEXT/STRING PRIMARY KEY
        // in sqlparser 0.59.0. PostgreSqlDialect properly handles TEXT as a data type.
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut statements =
            parse_sql_statements(&normalized_sql, &dialect).map_err(|e| e.to_string())?;
        if statements.len() != 1 {
            return Err("Expected exactly one statement".to_string());
        }
        let statement = statements.remove(0);

        match statement {
            Statement::CreateTable(CreateTable {
                name,
                columns,
                constraints,
                table_options,
                if_not_exists,
                ..
            }) => {
                // 1. Parse table name and namespace
                let (namespace_id, table_name) = if name.0.len() == 1 {
                    (
                        NamespaceId::from(default_namespace),
                        TableName::from(name.0[0].to_string().as_str()),
                    )
                } else if name.0.len() == 2 {
                    (
                        NamespaceId::from(name.0[0].to_string().as_str()),
                        TableName::from(name.0[1].to_string().as_str()),
                    )
                } else {
                    return Err("Invalid table name format. Expected 'table_name' or 'namespace.table_name'".to_string());
                };

                // Validate names
                if !RE_ALPHANUMERIC.is_match(namespace_id.as_str()) {
                    let span = name.0.get(0).and_then(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.span),
                        _ => None,
                    });
                    let location = span.map(format_span);
                    return Err(format!(
                        "Invalid namespace name '{}'. Only alphanumeric characters and underscores are allowed{}.",
                        namespace_id,
                        location
                            .as_deref()
                            .map(|s| format!(" ({})", s))
                            .unwrap_or_default()
                    ));
                }
                if !RE_ALPHANUMERIC.is_match(table_name.as_str()) {
                    let span = name.0.last().and_then(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.span),
                        _ => None,
                    });
                    let location = span.map(format_span);
                    return Err(format!(
                        "Invalid table name '{}'. Only alphanumeric characters and underscores are allowed{}.",
                        table_name,
                        location
                            .as_deref()
                            .map(|s| format!(" ({})", s))
                            .unwrap_or_default()
                    ));
                }

                // 2. Parse options (TYPE, STORAGE, FLUSH_POLICY, etc.)
                let mut table_type = legacy_table_type.unwrap_or(TableType::Shared);
                let mut storage_id = None;
                let mut use_user_storage = false;
                let mut flush_policy = None;
                let mut deleted_retention_hours = None;
                let mut ttl_seconds = None;
                let mut access_level = None;

                // Handle options (was with_options)
                let options_vec = match table_options {
                    sqlparser::ast::CreateTableOptions::With(opts) => opts,
                    sqlparser::ast::CreateTableOptions::Options(opts) => opts,
                    _ => vec![],
                };

                for option in options_vec {
                    if let sqlparser::ast::SqlOption::KeyValue { key, value } = option {
                        let key_str = key.value.to_uppercase();
                        let value_str = value.to_string().replace('\'', ""); // Remove quotes

                        match key_str.as_str() {
                            "TYPE" => {
                                let requested_type = TableType::from_str_opt(&value_str).ok_or_else(|| {
                                    format!("Invalid TYPE option '{}'. Supported: USER, SHARED, STREAM", value_str)
                                })?;

                                if let Some(ref legacy_type) = legacy_table_type {
                                    if requested_type != *legacy_type {
                                        return Err(format!(
                                            "Conflicting table type definitions: legacy prefix {:?} vs TYPE option {:?}",
                                            legacy_type, requested_type
                                        ));
                                    }
                                }

                                table_type = requested_type;
                            },
                            "STORAGE_ID" => {
                                if !RE_STORAGE_ID.is_match(&value_str) {
                                    return Err(format!("Invalid STORAGE_ID '{}'. Only alphanumeric, underscore, and hyphen allowed.", value_str));
                                }
                                storage_id = Some(StorageId::from(value_str));
                            },
                            "USE_USER_STORAGE" => {
                                use_user_storage = value_str.to_uppercase() == "TRUE";
                            },
                            "FLUSH_POLICY" => {
                                // Format: "rows:1000" or "interval:60" or "rows:1000,interval:60"
                                let mut rows = 0;
                                let mut interval = 0;

                                for part in value_str.split(',') {
                                    let mut kv = part.splitn(2, ':');
                                    let key = kv.next();
                                    let value = kv.next();
                                    if key.is_none() || value.is_none() {
                                        return Err(format!("Invalid FLUSH_POLICY format '{}'. Expected 'key:value'", part));
                                    }
                                    match key.unwrap().to_uppercase().as_str() {
                                        "ROWS" => {
                                            rows = value
                                                .unwrap()
                                                .parse()
                                                .map_err(|_| "Invalid row limit in FLUSH_POLICY")?;
                                        },
                                        "INTERVAL" => {
                                            interval = value
                                                .unwrap()
                                                .parse()
                                                .map_err(|_| "Invalid interval in FLUSH_POLICY")?;
                                        },
                                        _ => {
                                            return Err(format!(
                                                "Unknown FLUSH_POLICY key '{}'",
                                                key.unwrap()
                                            ))
                                        },
                                    }
                                }

                                let policy = if rows > 0 && interval > 0 {
                                    FlushPolicy::Combined {
                                        row_limit: rows,
                                        interval_seconds: interval,
                                    }
                                } else if rows > 0 {
                                    FlushPolicy::RowLimit { row_limit: rows }
                                } else if interval > 0 {
                                    FlushPolicy::TimeInterval {
                                        interval_seconds: interval,
                                    }
                                } else {
                                    return Err(
                                        "FLUSH_POLICY must specify 'rows' or 'interval' > 0"
                                            .to_string(),
                                    );
                                };

                                // Validate policy immediately
                                policy.validate()?;
                                flush_policy = Some(policy);
                            },
                            "DELETED_RETENTION_HOURS" => {
                                let hours: u32 = value_str
                                    .parse()
                                    .map_err(|_| "Invalid DELETED_RETENTION_HOURS")?;
                                deleted_retention_hours = Some(hours);
                            },
                            "TTL_SECONDS" => {
                                let seconds: u64 =
                                    value_str.parse().map_err(|_| "Invalid TTL_SECONDS")?;
                                ttl_seconds = Some(seconds);
                            },
                            "ACCESS_LEVEL" => {
                                access_level = match value_str.to_uppercase().as_str() {
                                    "PUBLIC" => Some(TableAccess::Public),
                                    "PRIVATE" => Some(TableAccess::Private),
                                    "RESTRICTED" => Some(TableAccess::Restricted),
                                    "DBA" => Some(TableAccess::Dba),
                                    _ => return Err(format!("Invalid ACCESS_LEVEL '{}'. Supported: PUBLIC, PRIVATE, RESTRICTED, DBA", value_str)),
                                };
                            },
                            _ => return Err(format!("Unknown table option '{}'", key_str)),
                        }
                    }
                }

                // 3. Validate options based on table type
                if table_type == TableType::Stream && ttl_seconds.is_none() {
                    return Err("STREAM tables must specify 'TTL_SECONDS'".to_string());
                }
                if table_type != TableType::Stream && ttl_seconds.is_some() {
                    return Err("TTL_SECONDS is only supported for STREAM tables".to_string());
                }
                if table_type != TableType::Shared && access_level.is_some() {
                    return Err("ACCESS_LEVEL is only supported for SHARED tables".to_string());
                }
                if table_type != TableType::User && use_user_storage {
                    return Err("USE_USER_STORAGE is only supported for USER tables".to_string());
                }

                // 4. Parse columns and constraints
                let mut arrow_fields = Vec::new();
                let mut column_defaults = HashMap::new();
                let mut primary_key_column = None;

                // Check table constraints for PRIMARY KEY
                for constraint in constraints {
                    match constraint {
                        TableConstraint::PrimaryKey(pk) => {
                            let columns = &pk.columns;
                            if columns.len() != 1 {
                                return Err(
                                    "Composite PRIMARY KEYs are not supported yet".to_string()
                                );
                            }
                            if primary_key_column.is_some() {
                                return Err("Multiple PRIMARY KEY definitions found".to_string());
                            }
                            // Handle OrderByExpr
                            let col_expr = &columns[0].column.expr;
                            if let sqlparser::ast::Expr::Identifier(ident) = col_expr {
                                primary_key_column = Some(ident.value.clone());
                            } else {
                                return Err(
                                    "Complex expressions in PRIMARY KEY not supported".to_string()
                                );
                            }
                        },
                        TableConstraint::Unique { .. } => {},
                        _ => {},
                    }
                }

                for col in columns {
                    let col_name = col.name.value;
                    if !RE_ALPHANUMERIC.is_match(&col_name) {
                        return Err(format!(
                            "Invalid column name '{}'. Only alphanumeric characters and underscores are allowed.",
                            col_name
                        ));
                    }

                    let kalam_type = map_sql_type_to_kalam(&col.data_type)?;
                    let data_type = kalam_type
                        .to_arrow_type()
                        .map_err(|e| format!("Unsupported SQL data type: {}", e))?;
                    let is_nullable = true;

                    // Check column options (PRIMARY KEY, DEFAULT, NOT NULL)
                    let mut col_is_nullable = is_nullable; // Default from type mapping

                    for option in col.options {
                        match &option.option {
                            ColumnOption::PrimaryKey(..) => {
                                if primary_key_column.is_some() {
                                    return Err(
                                        "Multiple PRIMARY KEY definitions found".to_string()
                                    );
                                }
                                primary_key_column = Some(col_name.clone());
                                col_is_nullable = false; // PKs cannot be null
                            },
                            ColumnOption::Unique(_) => {},
                            ColumnOption::NotNull => {
                                col_is_nullable = false;
                            },
                            ColumnOption::Null => {},
                            ColumnOption::Default(expr) => {
                                let default_spec = expr_to_column_default(expr);
                                column_defaults.insert(col_name.clone(), default_spec);
                            },
                            ColumnOption::DialectSpecific(tokens) => {
                                // Check for AUTO_INCREMENT
                                let s = tokens
                                    .iter()
                                    .map(|t| t.to_string())
                                    .collect::<Vec<_>>()
                                    .join(" ");
                                if s.to_uppercase().contains("AUTO_INCREMENT") {
                                    // Set default to SNOWFLAKE_ID()
                                    column_defaults.insert(
                                        col_name.clone(),
                                        ColumnDefault::function("SNOWFLAKE_ID", vec![]),
                                    );
                                }
                            },
                            _ => {
                                println!("  - OTHER");
                            },
                        }
                    }

                    // Create the field and attach KalamDataType metadata for types
                    // that aren't recoverable from Arrow (like FILE, JSON)
                    let field = Field::new(&col_name, data_type, col_is_nullable);
                    let field = with_kalam_data_type_metadata(field, &kalam_type);
                    arrow_fields.push(field);
                }

                if arrow_fields.is_empty() {
                    return Err("Table must have at least one column".to_string());
                }

                // Ensure PK column exists and is not null
                if let Some(ref pk) = primary_key_column {
                    let mut found = false;
                    for field in &mut arrow_fields {
                        if field.name() == pk {
                            found = true;
                            // Force PK to be non-nullable
                            if field.is_nullable() {
                                *field = Field::new(pk, field.data_type().clone(), false);
                            }
                            break;
                        }
                    }
                    if !found {
                        return Err(format!(
                            "PRIMARY KEY column '{}' not found in column list",
                            pk
                        ));
                    }
                }

                Ok(CreateTableStatement {
                    table_name,
                    namespace_id,
                    table_type,
                    schema: Arc::new(Schema::new(arrow_fields)),
                    column_defaults,
                    primary_key_column,
                    storage_id,
                    use_user_storage,
                    flush_policy,
                    deleted_retention_hours,
                    ttl_seconds,
                    if_not_exists,
                    access_level,
                })
            },
            _ => Err("Not a CREATE TABLE statement".to_string()),
        }
    }
}

fn normalize_create_table_sql(sql: &str) -> (String, Option<TableType>) {
    // Replace CURRENT_USER() with CURRENT_USER to satisfy sqlparser GenericDialect
    // which doesn't support function calls for this keyword in DEFAULT clause
    let re_current_user = Regex::new(r"(?i)CURRENT_USER\s*\(\s*\)").unwrap();
    let sql_cow = re_current_user.replace_all(sql, "CURRENT_USER");
    let sql_ref = sql_cow.as_ref();

    if let Some(caps) = LEGACY_CREATE_PREFIX_RE.captures(sql_ref) {
        let requested_type = caps[1].to_ascii_uppercase();
        let table_type = TableType::from_str_opt(&requested_type).unwrap_or(TableType::User);

        // let table_type = match requested_type.as_str() {
        //     "USER" => TableType::User,
        //     "SHARED" => TableType::Shared,
        //     "STREAM" => TableType::Stream,
        //     _ => TableType::User,
        // };
        let normalized = LEGACY_CREATE_PREFIX_RE.replace(sql_ref, "CREATE TABLE ").into_owned();
        (normalized, Some(table_type))
    } else {
        (sql_ref.to_string(), None)
    }
}

fn expr_to_column_default(expr: &sqlparser::ast::Expr) -> ColumnDefault {
    match expr {
        // Handle function calls
        sqlparser::ast::Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            match name.as_str() {
                "NOW" | "CURRENT_TIMESTAMP" | "SNOWFLAKE_ID" | "UUID_V7" | "ULID"
                | "CURRENT_USER" => ColumnDefault::function(&name, vec![]),
                _ => {
                    // Fallback to string literal for unknown functions
                    ColumnDefault::literal(serde_json::Value::String(func.to_string()))
                },
            }
        },
        // Handle literals
        sqlparser::ast::Expr::Value(val) => match &val.value {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    ColumnDefault::literal(serde_json::Value::Number(i.into()))
                } else if let Ok(f) = n.parse::<f64>() {
                    ColumnDefault::literal(serde_json::json!(f))
                } else {
                    ColumnDefault::literal(serde_json::Value::String(n.clone()))
                }
            },
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => {
                ColumnDefault::literal(serde_json::Value::String(s.clone()))
            },
            sqlparser::ast::Value::Boolean(b) => {
                ColumnDefault::literal(serde_json::Value::Bool(*b))
            },
            sqlparser::ast::Value::Null => ColumnDefault::literal(serde_json::Value::Null),
            _ => ColumnDefault::literal(serde_json::Value::String(val.to_string())),
        },
        // Handle identifiers (e.g. CURRENT_TIMESTAMP without parens)
        sqlparser::ast::Expr::Identifier(ident) => {
            let s = ident.value.to_uppercase();
            match s.as_str() {
                "CURRENT_TIMESTAMP" => ColumnDefault::function("NOW", vec![]),
                "CURRENT_USER" => ColumnDefault::function("CURRENT_USER", vec![]),
                "NULL" => ColumnDefault::literal(serde_json::Value::Null),
                _ => ColumnDefault::literal(serde_json::Value::String(ident.value.clone())),
            }
        },
        _ => {
            let default_val = expr.to_string();
            let upper_val = default_val.to_uppercase();
            if upper_val == "NULL" {
                ColumnDefault::literal(serde_json::Value::Null)
            } else if upper_val == "CURRENT_TIMESTAMP" || upper_val == "NOW()" {
                ColumnDefault::function("NOW", vec![])
            } else {
                // Strip quotes if present
                let val = default_val.trim_matches('\'').to_string();
                ColumnDefault::literal(serde_json::Value::String(val))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_NS: &str = "sales";

    #[test]
    fn modern_create_table_parses() {
        let sql = r#"
CREATE TABLE sales.orders2 (
    order_id        INT,
    customer_id     STRING NOT NULL,
    ordered_at      TIMESTAMP
)
WITH (
    TYPE = 'USER',
    STORAGE_ID = 's3-us',
    FLUSH_POLICY = 'rows:1000,interval:60'
);
"#;

        let stmt = CreateTableStatement::parse(sql, DEFAULT_NS).unwrap();
        assert_eq!(stmt.table_type, TableType::User);
        assert_eq!(stmt.table_name.as_str(), "orders2");
        assert_eq!(stmt.namespace_id.as_str(), "sales");
        assert_eq!(stmt.storage_id.unwrap().as_str(), "s3-us");
        assert!(matches!(stmt.flush_policy, Some(FlushPolicy::Combined { .. })));
    }

    #[test]
    fn stream_table_requires_ttl() {
        let sql = r#"
CREATE TABLE sales.activity (
    event_id STRING PRIMARY KEY,
    payload STRING
) WITH (
    TYPE = 'STREAM'
);
"#;

        let err = CreateTableStatement::parse(sql, DEFAULT_NS).unwrap_err();
        assert!(err.contains("STREAM tables must specify"));
    }

    #[test]
    fn test_text_primary_key_shared() {
        // Test STRING PRIMARY KEY
        let sql_string = r#"
CREATE TABLE sales.system_config (
    key STRING PRIMARY KEY,
    value STRING
) WITH (
    TYPE = 'SHARED'
)
"#;
        let stmt = CreateTableStatement::parse(sql_string, DEFAULT_NS).unwrap();
        assert_eq!(stmt.primary_key_column.as_deref(), Some("key"));
        assert_eq!(stmt.table_type, TableType::Shared);

        // Test TEXT PRIMARY KEY (common in tests and docs)
        let sql_text = r#"
CREATE TABLE sales.config2 (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITH (
    TYPE = 'SHARED'
)
"#;
        let stmt = CreateTableStatement::parse(sql_text, DEFAULT_NS).unwrap();
        assert_eq!(stmt.primary_key_column.as_deref(), Some("key"));
        assert_eq!(stmt.table_type, TableType::Shared);
    }

    #[test]
    fn test_current_user_default() {
        let sql = r#"
CREATE TABLE concurrent.user_data (
    id INTEGER,
    message TEXT,
    timestamp BIGINT,
    current_user_id TEXT DEFAULT CURRENT_USER()
) WITH (TYPE='USER', FLUSH_POLICY='rows:100')
"#;
        let stmt = CreateTableStatement::parse(sql, DEFAULT_NS).unwrap();
        assert!(stmt.column_defaults.contains_key("current_user_id"));
    }

    #[test]
    fn test_current_user_no_parens() {
        let sql = r#"
CREATE TABLE concurrent.user_data_no_parens (
    id INTEGER,
    message TEXT,
    timestamp BIGINT,
    current_user_id TEXT DEFAULT CURRENT_USER
) WITH (TYPE='USER')
"#;
        let stmt = CreateTableStatement::parse(sql, DEFAULT_NS).unwrap();
        assert!(stmt.column_defaults.contains_key("current_user_id"));
    }
}
