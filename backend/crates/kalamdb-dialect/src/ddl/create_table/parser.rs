use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::{Field, Schema};
use kalamdb_commons::{
    conversions::with_kalam_data_type_metadata,
    models::{datatypes::ToArrowType, NamespaceId, StorageId, TableName},
    schemas::{policy::FlushPolicy, ColumnDefault, TableCompression, TableType},
};
use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast::{ColumnOption, CreateTable, ObjectNamePart, Statement, TableConstraint};

use super::types::CreateTableStatement;
use crate::{
    compatibility::map_sql_type_to_kalam,
    ddl::column_default::expr_to_column_default,
    parser::utils::{format_span, parse_sql_statements},
    validation::validate_column_name,
};

static RE_STORAGE_ID: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap());
static CREATE_TYPED_PREFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*CREATE\s+(USER|SHARED|STREAM)\s+TABLE").unwrap());
/// Matches `USING <identifier>` clause after the closing paren of column definitions.
/// PostgreSQL uses this for table access methods, e.g. `CREATE TABLE t (...) USING kalamdb`.
static USING_ACCESS_METHOD_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\)\s*USING\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*").unwrap());

impl CreateTableStatement {
    /// Parse a SQL statement into a CreateTableStatement
    pub fn parse(sql: &str, default_namespace: &NamespaceId) -> Result<Self, String> {
        crate::ddl::reject_access_level_sql(sql)?;
        let (mut normalized_sql, create_prefix_table_type) = normalize_create_table_sql(sql);

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
                    (default_namespace.clone(), TableName::from(name.0[0].to_string().as_str()))
                } else if name.0.len() == 2 {
                    (
                        NamespaceId::from(name.0[0].to_string().as_str()),
                        TableName::from(name.0[1].to_string().as_str()),
                    )
                } else {
                    return Err("Invalid table name format. Expected 'table_name' or \
                                'namespace.table_name'"
                        .to_string());
                };

                // Validate names using shared commons rules.
                if let Err(error) = NamespaceId::try_parse_reference(namespace_id.as_str()) {
                    let span = name.0.first().and_then(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.span),
                        _ => None,
                    });
                    let location = span.map(format_span);
                    return Err(format!(
                        "Invalid namespace name '{}': {}{}",
                        namespace_id,
                        error,
                        location.as_deref().map(|s| format!(" ({s})")).unwrap_or_default()
                    ));
                }
                if let Err(error) = TableName::try_new(table_name.as_str()) {
                    let span = name.0.last().and_then(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.span),
                        _ => None,
                    });
                    let location = span.map(format_span);
                    return Err(format!(
                        "Invalid table name '{}': {}{}",
                        table_name,
                        error,
                        location.as_deref().map(|s| format!(" ({s})")).unwrap_or_default()
                    ));
                }

                // 2. Parse options (TYPE, STORAGE, FLUSH_POLICY, etc.)
                let mut table_type = create_prefix_table_type.unwrap_or(TableType::Shared);
                let mut storage_id = None;
                let mut use_user_storage = false;
                let mut flush_policy = None;
                let mut deleted_retention_hours = None;
                let mut ttl_seconds = None;
                let mut compression = None;
                let mut eviction_strategy = None;
                let mut max_stream_size_bytes = None;

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
                                let requested_type = TableType::from_str_opt(&value_str)
                                    .ok_or_else(|| {
                                        format!(
                                            "Invalid TYPE option '{}'. Supported: USER, SHARED, \
                                             STREAM",
                                            value_str
                                        )
                                    })?;

                                if let Some(prefix_type) = create_prefix_table_type {
                                    if requested_type != prefix_type {
                                        return Err(format!(
                                            "Conflicting table type definitions: CREATE {:?} \
                                             TABLE vs TYPE option {:?}",
                                            prefix_type, requested_type
                                        ));
                                    }
                                }

                                table_type = requested_type;
                            },
                            "STORAGE_ID" => {
                                if !RE_STORAGE_ID.is_match(&value_str) {
                                    return Err(format!(
                                        "Invalid STORAGE_ID '{}'. Only alphanumeric, underscore, \
                                         and hyphen allowed.",
                                        value_str
                                    ));
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
                                        return Err(format!(
                                            "Invalid FLUSH_POLICY format '{}'. Expected \
                                             'key:value'",
                                            part
                                        ));
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
                                        row_limit:        rows,
                                        interval_seconds: interval,
                                    }
                                } else if rows > 0 {
                                    FlushPolicy::RowLimit { row_limit: rows }
                                } else if interval > 0 {
                                    FlushPolicy::TimeInterval {
                                        interval_seconds: interval,
                                    }
                                } else {
                                    return Err("FLUSH_POLICY must specify 'rows' or 'interval' \
                                                > 0"
                                    .to_string());
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
                                return Err(crate::ddl::ACCESS_LEVEL_UNSUPPORTED.to_string());
                            },
                            "COMPRESSION" => {
                                compression = Some(parse_compression_option(&value_str)?);
                            },
                            "EVICTION_STRATEGY" => {
                                eviction_strategy =
                                    Some(parse_eviction_strategy_option(&value_str)?);
                            },
                            "MAX_STREAM_SIZE_BYTES" => {
                                let bytes: u64 = value_str
                                    .parse()
                                    .map_err(|_| "Invalid MAX_STREAM_SIZE_BYTES")?;
                                max_stream_size_bytes = Some(bytes);
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
                if table_type == TableType::Stream && storage_id.is_some() {
                    return Err(
                        "STORAGE_ID is only supported for USER and SHARED tables".to_string()
                    );
                }
                if table_type == TableType::Stream && flush_policy.is_some() {
                    return Err(
                        "FLUSH_POLICY is only supported for USER and SHARED tables".to_string()
                    );
                }
                if table_type == TableType::Stream && compression.is_some() {
                    return Err(
                        "COMPRESSION is only supported for USER and SHARED tables".to_string()
                    );
                }
                if table_type != TableType::User && use_user_storage {
                    return Err("USE_USER_STORAGE is only supported for USER tables".to_string());
                }
                if table_type != TableType::Stream && eviction_strategy.is_some() {
                    return Err("EVICTION_STRATEGY is only supported for STREAM tables".to_string());
                }
                if table_type != TableType::Stream && max_stream_size_bytes.is_some() {
                    return Err(
                        "MAX_STREAM_SIZE_BYTES is only supported for STREAM tables".to_string()
                    );
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

                let mut seen_column_names: HashSet<String> = HashSet::new();
                for col in columns {
                    let col_name = col.name.value;
                    if let Err(error) = validate_column_name(&col_name) {
                        return Err(format!("Invalid column name '{}': {}", col_name, error));
                    }

                    // Column names are case-insensitive (folded to lowercase), so
                    // `name` / `Name` / `NAME` all refer to the same column and
                    // would produce a broken table with duplicate columns.
                    let col_name_lower = col_name.to_ascii_lowercase();
                    if !seen_column_names.insert(col_name_lower.clone()) {
                        return Err(format!(
                            "Duplicate column name '{}'. Column names are case-insensitive.",
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
                                let default_spec = expr_to_column_default(expr, &namespace_id)?;
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
                                    column_defaults.insert(
                                        col_name.clone(),
                                        ColumnDefault::function("SNOWFLAKE_ID", vec![]),
                                    );
                                }
                            },
                            // GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY
                            ColumnOption::Generated {
                                generated_as:
                                    sqlparser::ast::GeneratedAs::Always
                                    | sqlparser::ast::GeneratedAs::ByDefault,
                                generation_expr: None,
                                ..
                            } => {
                                column_defaults.insert(
                                    col_name.clone(),
                                    ColumnDefault::function("SNOWFLAKE_ID", vec![]),
                                );
                            },
                            ColumnOption::Identity(..) => {
                                column_defaults.insert(
                                    col_name.clone(),
                                    ColumnDefault::function("SNOWFLAKE_ID", vec![]),
                                );
                            },
                            _ => {},
                        }
                    }

                    // SERIAL/BIGSERIAL/SMALLSERIAL types imply auto-increment.
                    // Add SNOWFLAKE_ID() default if no explicit default was set.
                    if !column_defaults.contains_key(&col_name) && is_serial_type(&col.data_type) {
                        column_defaults.insert(
                            col_name.clone(),
                            ColumnDefault::function("SNOWFLAKE_ID", vec![]),
                        );
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
                    compression,
                    eviction_strategy,
                    max_stream_size_bytes,
                    if_not_exists,
                })
            },
            _ => Err("Not a CREATE TABLE statement".to_string()),
        }
    }
}

fn parse_compression_option(value: &str) -> Result<TableCompression, String> {
    value.parse()
}

fn parse_eviction_strategy_option(value: &str) -> Result<String, String> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "time_based" | "size_based" | "hybrid" => Ok(normalized),
        _ => Err(format!(
            "Invalid EVICTION_STRATEGY '{}'. Supported: time_based, size_based, hybrid",
            value
        )),
    }
}

fn normalize_create_table_sql(sql: &str) -> (String, Option<TableType>) {
    // Replace CURRENT_USER() with CURRENT_USER to satisfy sqlparser GenericDialect
    // which doesn't support function calls for this keyword in DEFAULT clause
    let re_current_user = Regex::new(r"(?i)CURRENT_USER\s*\(\s*\)").unwrap();
    let mut normalized = re_current_user.replace_all(sql, "CURRENT_USER").into_owned();

    // Strip `USING <access_method>` clause (PostgreSQL table access method syntax).
    // e.g. `CREATE TABLE t (...) USING kalamdb WITH (...)` → `CREATE TABLE t (...) WITH (...)`
    // We accept the access method for compatibility but KalamDB always uses its own engine.
    normalized = USING_ACCESS_METHOD_RE.replace(&normalized, ") ").into_owned();

    if let Some(caps) = CREATE_TYPED_PREFIX_RE.captures(&normalized) {
        let requested_type = caps[1].to_ascii_uppercase();
        let table_type = TableType::from_str_opt(&requested_type).unwrap_or(TableType::User);
        let normalized_sql =
            CREATE_TYPED_PREFIX_RE.replace(&normalized, "CREATE TABLE").into_owned();
        (normalized_sql, Some(table_type))
    } else {
        (normalized, None)
    }
}

/// Returns true if the SQL data type is a PostgreSQL SERIAL type that implies
/// auto-increment semantics (SERIAL, BIGSERIAL, SMALLSERIAL and their aliases).
fn is_serial_type(data_type: &sqlparser::ast::DataType) -> bool {
    if let sqlparser::ast::DataType::Custom(name, _) = data_type {
        let ident = name
            .0
            .iter()
            .map(|id| id.to_string().to_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        matches!(
            ident.as_str(),
            "serial" | "serial2" | "serial4" | "serial8" | "bigserial" | "smallserial"
        )
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_NS: &str = "sales";

    fn default_ns() -> NamespaceId {
        NamespaceId::new(DEFAULT_NS)
    }

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

        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
        assert_eq!(stmt.table_type, TableType::User);
        assert_eq!(stmt.table_name.as_str(), "orders2");
        assert_eq!(stmt.namespace_id.as_str(), "sales");
        assert_eq!(stmt.storage_id.unwrap().as_str(), "s3-us");
        assert!(matches!(stmt.flush_policy, Some(FlushPolicy::Combined { .. })));
    }

    #[test]
    fn create_table_parses_type_specific_options() {
        let user_sql = r#"
CREATE TABLE sales.user_profile (
    id BIGINT PRIMARY KEY,
    name TEXT
) WITH (
    TYPE = 'USER',
    STORAGE_ID = 'local-ssd',
    USE_USER_STORAGE = true,
    FLUSH_POLICY = 'rows:1000',
    COMPRESSION = 'zstd'
);
"#;
        let stmt = CreateTableStatement::parse(user_sql, &default_ns()).unwrap();
        assert_eq!(stmt.table_type, TableType::User);
        assert_eq!(stmt.storage_id.unwrap().as_str(), "local-ssd");
        assert!(stmt.use_user_storage);
        assert!(matches!(stmt.flush_policy, Some(FlushPolicy::RowLimit { .. })));
        assert_eq!(stmt.compression, Some(TableCompression::Zstd));

        let stream_sql = r#"
CREATE TABLE sales.events (
    event_id TEXT,
    payload JSON
) WITH (
    TYPE = 'STREAM',
    TTL_SECONDS = 3600,
    EVICTION_STRATEGY = 'hybrid',
    MAX_STREAM_SIZE_BYTES = 1048576
);
"#;
        let stmt = CreateTableStatement::parse(stream_sql, &default_ns()).unwrap();
        assert_eq!(stmt.table_type, TableType::Stream);
        assert_eq!(stmt.ttl_seconds, Some(3600));
        assert_eq!(stmt.eviction_strategy.as_deref(), Some("hybrid"));
        assert_eq!(stmt.max_stream_size_bytes, Some(1_048_576));
        assert_eq!(stmt.compression, None);

        let none_sql = "CREATE TABLE sales.raw (id BIGINT PRIMARY KEY) WITH (TYPE='SHARED', \
                        COMPRESSION='none')";
        let stmt = CreateTableStatement::parse(none_sql, &default_ns()).unwrap();
        assert_eq!(stmt.table_type, TableType::Shared);
        assert_eq!(stmt.compression, Some(TableCompression::None));
    }

    #[test]
    fn create_table_rejects_unsupported_compression() {
        let sql = "CREATE TABLE sales.bad_compression (id BIGINT PRIMARY KEY) WITH (TYPE='USER', \
                   COMPRESSION='lz4')";
        let err = CreateTableStatement::parse(sql, &default_ns()).unwrap_err();
        assert!(err.contains("Supported: none, snappy, zstd"));
    }

    #[test]
    fn create_table_rejects_stream_compression() {
        let sql = "CREATE TABLE sales.bad_stream (id BIGINT PRIMARY KEY) WITH (TYPE='STREAM', \
                   TTL_SECONDS=60, COMPRESSION='snappy')";
        let err = CreateTableStatement::parse(sql, &default_ns()).unwrap_err();
        assert!(err.contains("COMPRESSION is only supported for USER and SHARED tables"));
    }

    #[test]
    fn create_table_rejects_unsupported_type_options() {
        let err = CreateTableStatement::parse(
            "CREATE TABLE sales.bad_user (id BIGINT PRIMARY KEY) WITH (TYPE='USER', \
             TTL_SECONDS=60)",
            &default_ns(),
        )
        .unwrap_err();
        assert!(err.contains("TTL_SECONDS is only supported for STREAM tables"));

        let err = CreateTableStatement::parse(
            "CREATE TABLE sales.bad_shared (id TEXT) WITH (TYPE='SHARED', ACCESS_LEVEL='PUBLIC')",
            &default_ns(),
        )
        .unwrap_err();
        assert!(err.contains("ACCESS_LEVEL is not supported"));
        assert!(err.contains("CREATE POLICY"));

        let err = CreateTableStatement::parse(
            "CREATE SHARED TABLE sales.legacy (id TEXT) ACCESS LEVEL private",
            &default_ns(),
        )
        .unwrap_err();
        assert!(err.contains("ACCESS_LEVEL is not supported"));
    }

    #[test]
    fn duplicate_column_names_are_rejected_case_insensitively() {
        // KalamDB folds column names to lowercase, so `name`, `Name`, and `NAME`
        // all refer to the same column. Without this check, CREATE TABLE would
        // succeed but produce a permanently broken table.
        let sql = r#"
CREATE TABLE sales.widgets (
    id BIGINT PRIMARY KEY,
    name STRING,
    Name STRING
);
"#;

        let err = CreateTableStatement::parse(sql, &default_ns()).unwrap_err();
        assert!(
            err.contains("Duplicate column name"),
            "expected duplicate-column error, got: {err}"
        );
    }

    #[test]
    fn exact_duplicate_column_names_are_rejected() {
        let sql = r#"
CREATE TABLE sales.widgets (
    id BIGINT PRIMARY KEY,
    label STRING,
    label STRING
);
"#;

        let err = CreateTableStatement::parse(sql, &default_ns()).unwrap_err();
        assert!(
            err.contains("Duplicate column name"),
            "expected duplicate-column error, got: {err}"
        );
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

        let err = CreateTableStatement::parse(sql, &default_ns()).unwrap_err();
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
        let stmt = CreateTableStatement::parse(sql_string, &default_ns()).unwrap();
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
        let stmt = CreateTableStatement::parse(sql_text, &default_ns()).unwrap();
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
        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
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
        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
        assert!(stmt.column_defaults.contains_key("current_user_id"));
    }

    #[test]
    fn test_serial_auto_increment() {
        let sql = r#"
CREATE TABLE sales.orders (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL
) WITH (TYPE = 'SHARED')
"#;
        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
        assert!(
            stmt.column_defaults.contains_key("id"),
            "BIGSERIAL should auto-add SNOWFLAKE_ID() default"
        );
        assert_eq!(stmt.primary_key_column.as_deref(), Some("id"));
    }

    #[test]
    fn test_serial_variants_get_auto_increment() {
        for (serial_type, label) in &[
            ("SERIAL", "SERIAL"),
            ("BIGSERIAL", "BIGSERIAL"),
            ("SMALLSERIAL", "SMALLSERIAL"),
        ] {
            let sql = format!(
                "CREATE TABLE sales.t_{} (id {} PRIMARY KEY, name TEXT) WITH (TYPE = 'SHARED')",
                label.to_lowercase(),
                serial_type
            );
            let stmt = CreateTableStatement::parse(&sql, &default_ns())
                .unwrap_or_else(|e| panic!("{label} failed: {e}"));
            assert!(
                stmt.column_defaults.contains_key("id"),
                "{label} should auto-add SNOWFLAKE_ID() default"
            );
        }
    }

    #[test]
    fn test_using_access_method_stripped() {
        let sql = r#"
CREATE TABLE sales.compression_test (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    data TEXT
) USING kalamdb
  WITH (TYPE = 'USER')
"#;
        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
        assert_eq!(stmt.table_type, TableType::User);
        assert_eq!(stmt.table_name.as_str(), "compression_test");
    }

    #[test]
    fn test_using_access_method_with_options() {
        let sql = r#"
CREATE TABLE sales.my_table (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
) USING kalamdb
  WITH (TYPE = 'USER', STORAGE_ID = 'local-ssd')
"#;
        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
        assert_eq!(stmt.table_type, TableType::User);
        assert_eq!(stmt.storage_id.unwrap().as_str(), "local-ssd");
        assert!(stmt.column_defaults.contains_key("id"), "BIGSERIAL auto-increment");
        assert!(stmt.column_defaults.contains_key("created_at"), "NOW() default");
    }

    #[test]
    fn test_generated_always_as_identity() {
        let sql = r#"
CREATE TABLE sales.identity_test (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT
) WITH (TYPE = 'SHARED')
"#;
        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
        assert!(
            stmt.column_defaults.contains_key("id"),
            "GENERATED ALWAYS AS IDENTITY should add SNOWFLAKE_ID() default"
        );
    }

    #[test]
    fn default_procedure_uses_call_models() {
        use kalamdb_commons::{CallArgument, RoutineId};

        let sql = r#"
CREATE TABLE sales.orders (
    id BIGINT PRIMARY KEY DEFAULT next_id('v1'),
    created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER')
"#;
        let stmt = CreateTableStatement::parse(sql, &default_ns()).unwrap();
        assert_eq!(
            stmt.column_defaults.get("id"),
            Some(&ColumnDefault::procedure(
                RoutineId::from_parts(
                    Some(&kalamdb_commons::models::NamespaceId::new("sales")),
                    "next_id"
                ),
                vec![CallArgument::text("v1")],
            ))
        );
        assert_eq!(
            stmt.column_defaults.get("created_at"),
            Some(&ColumnDefault::function("NOW", vec![]))
        );

        let ns = kalamdb_commons::models::NamespaceId::new(DEFAULT_NS);
        let call = crate::ddl::CallStatement::parse("CALL next_id('v1')", &ns).unwrap();
        assert_eq!(stmt.column_defaults.get("id").unwrap().as_routine_call(), Some(&call.call));
    }
}
