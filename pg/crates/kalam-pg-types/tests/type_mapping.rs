use kalam_pg_types::{foreign_column_definition, pg_type_name_for};
use kalamdb_commons::models::{datatypes::KalamDataType, schemas::ColumnDefinition};

#[test]
fn maps_embedding_to_pgvector_type_name() {
    let type_name = pg_type_name_for(&KalamDataType::Embedding(384)).expect("map embedding");
    assert_eq!(type_name, "VECTOR(384)");
}

#[test]
fn maps_decimal_to_numeric_type_name() {
    let type_name = pg_type_name_for(&KalamDataType::Decimal {
        precision: 10,
        scale: 2,
    })
    .expect("map decimal");
    assert_eq!(type_name, "NUMERIC(10, 2)");
}

#[test]
fn builds_foreign_column_definition() {
    let column = ColumnDefinition::new(
        1,
        "created_at",
        1,
        KalamDataType::Timestamp,
        false,
        false,
        false,
        kalamdb_commons::models::schemas::ColumnDefault::None,
        None,
    );

    let definition = foreign_column_definition(&column).expect("build column definition");
    assert_eq!(definition, "\"created_at\" TIMESTAMP NOT NULL");
}
