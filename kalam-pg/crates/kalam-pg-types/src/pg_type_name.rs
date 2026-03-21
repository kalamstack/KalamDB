use kalam_pg_common::KalamPgError;
use kalamdb_commons::models::datatypes::KalamDataType;

/// Map a KalamDB type into the PostgreSQL type name used by foreign tables.
pub fn pg_type_name_for(data_type: &KalamDataType) -> Result<String, KalamPgError> {
    let type_name = match data_type {
        KalamDataType::Boolean => "BOOLEAN".to_string(),
        KalamDataType::SmallInt => "SMALLINT".to_string(),
        KalamDataType::Int => "INTEGER".to_string(),
        KalamDataType::BigInt => "BIGINT".to_string(),
        KalamDataType::Float => "REAL".to_string(),
        KalamDataType::Double => "DOUBLE PRECISION".to_string(),
        KalamDataType::Text => "TEXT".to_string(),
        KalamDataType::Bytes => "BYTEA".to_string(),
        KalamDataType::Date => "DATE".to_string(),
        KalamDataType::Time => "TIME".to_string(),
        KalamDataType::Timestamp => "TIMESTAMP".to_string(),
        KalamDataType::DateTime => "TIMESTAMPTZ".to_string(),
        KalamDataType::Uuid => "UUID".to_string(),
        KalamDataType::Json | KalamDataType::File => "JSONB".to_string(),
        KalamDataType::Decimal { precision, scale } => {
            format!("NUMERIC({}, {})", precision, scale)
        },
        KalamDataType::Embedding(dimension) => format!("VECTOR({})", dimension),
    };

    Ok(type_name)
}
