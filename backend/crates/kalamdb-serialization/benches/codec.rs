//! Ordinal KOBJ row codec vs the previous name-keyed FlexBuffers row.
//!
//! Reports encoded size and throughput. Run:
//! `cargo bench -p kalamdb-serialization --bench codec`

use std::{collections::BTreeMap, hint::black_box, sync::Arc, time::Duration};

use arrow::{
    array::{Int64Array, ListArray, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field},
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use datafusion_common::ScalarValue;
use kalamdb_commons::{
    ids::SeqId,
    models::{
        rows::{Row, UserTableRow},
        UserId,
    },
};
use kalamdb_serialization::{
    decode_object, decode_user_row, encode_object, encode_user_row, StorageDataType, StorageField,
    StorageSchema,
};
use serde::{Deserialize, Serialize};

const BATCH_SIZE: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogObject {
    name:    String,
    version: u32,
    tags:    Vec<String>,
}

fn customer_type() -> StorageDataType {
    StorageDataType::Struct(vec![
        StorageField::new("id", StorageDataType::Int64),
        StorageField::new("name", StorageDataType::Utf8),
    ])
}

fn nested_schema() -> StorageSchema {
    StorageSchema::new(
        1,
        vec![
            StorageField::new("id", StorageDataType::Int64),
            StorageField::new("customer", customer_type()),
            StorageField::new("tags", StorageDataType::List(Box::new(StorageDataType::Utf8))),
        ],
    )
}

fn scalar_schema() -> StorageSchema {
    StorageSchema::new(
        1,
        vec![
            StorageField::new("id", StorageDataType::Int64),
            StorageField::new("conversation_id", StorageDataType::Int64),
            StorageField::new("body", StorageDataType::Utf8),
        ],
    )
}

fn struct_scalar(id: i64, name: &str) -> ScalarValue {
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(id)])) as arrow::array::ArrayRef,
        ),
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some(name)])) as arrow::array::ArrayRef,
        ),
    ]);
    ScalarValue::Struct(Arc::new(struct_array))
}

fn utf8_list(values: &[&str]) -> ScalarValue {
    let items: Vec<ScalarValue> = values
        .iter()
        .map(|value| ScalarValue::Utf8(Some((*value).to_string())))
        .collect();
    if items.is_empty() {
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list = ListArray::try_new(
            field,
            OffsetBuffer::from_lengths([0]),
            Arc::new(StringArray::from(Vec::<String>::new())),
            None,
        )
        .expect("empty list");
        return ScalarValue::List(Arc::new(list));
    }
    ScalarValue::List(ScalarValue::new_list(&items, &DataType::Utf8, true))
}

fn nested_row() -> UserTableRow {
    let mut values = BTreeMap::new();
    values.insert("id".to_string(), ScalarValue::Int64(Some(9)));
    values.insert("customer".to_string(), struct_scalar(7, "ada"));
    values.insert("tags".to_string(), utf8_list(&["vip", "west"]));
    UserTableRow {
        user_id:     UserId::new("user-1"),
        _seq:        SeqId::from_i64(100),
        _commit_seq: 3,
        _deleted:    false,
        fields:      Row { values },
    }
}

fn scalar_row() -> UserTableRow {
    let mut values = BTreeMap::new();
    values.insert("id".to_string(), ScalarValue::Int64(Some(42)));
    values.insert("conversation_id".to_string(), ScalarValue::Int64(Some(7)));
    values.insert(
        "body".to_string(),
        ScalarValue::Utf8(Some("hello from kalamdb 0.7".to_string())),
    );
    UserTableRow {
        user_id:     UserId::new("user-1"),
        _seq:        SeqId::from_i64(100),
        _commit_seq: 3,
        _deleted:    false,
        fields:      Row { values },
    }
}

fn encode_name_keyed(row: &UserTableRow) -> Vec<u8> {
    flexbuffers::to_vec(row).expect("name-keyed flexbuffers encode")
}

fn catalog_object() -> CatalogObject {
    CatalogObject {
        name:    "app.messages".to_string(),
        version: 4,
        tags:    vec!["chat".to_string(), "rls".to_string()],
    }
}

fn print_sizes() {
    let nested = nested_row();
    let scalar = scalar_row();
    let nested_kobj = encode_user_row(&nested, &nested_schema()).unwrap();
    let scalar_kobj = encode_user_row(&scalar, &scalar_schema()).unwrap();
    let nested_legacy = encode_name_keyed(&nested);
    let scalar_legacy = encode_name_keyed(&scalar);
    let object = encode_object(&catalog_object()).unwrap();
    eprintln!(
        "codec sizes: nested_kobj={} nested_name_keyed={} scalar_kobj={} scalar_name_keyed={} \
         object={}",
        nested_kobj.len(),
        nested_legacy.len(),
        scalar_kobj.len(),
        scalar_legacy.len(),
        object.len()
    );
}

fn bench_codec(c: &mut Criterion) {
    print_sizes();

    let nested = nested_row();
    let scalar = scalar_row();
    let nested_schema = nested_schema();
    let scalar_schema = scalar_schema();
    let nested_bytes = encode_user_row(&nested, &nested_schema).unwrap().into_bytes();
    let scalar_bytes = encode_user_row(&scalar, &scalar_schema).unwrap().into_bytes();
    let nested_legacy = encode_name_keyed(&nested);
    let scalar_legacy = encode_name_keyed(&scalar);
    let catalog = catalog_object();
    let catalog_bytes = encode_object(&catalog).unwrap().into_bytes();

    let mut group = c.benchmark_group("serialization_codec");
    group.measurement_time(Duration::from_secs(8));
    group.warm_up_time(Duration::from_secs(2));

    group.throughput(Throughput::Bytes(nested_bytes.len() as u64));
    group.bench_function("nested_row_encode_kobj", |b| {
        b.iter(|| encode_user_row(black_box(&nested), black_box(&nested_schema)).unwrap())
    });
    group.bench_function("nested_row_decode_kobj", |b| {
        b.iter(|| {
            decode_user_row(
                black_box(&nested_bytes),
                black_box(&nested_schema),
                UserId::new("user-1"),
                SeqId::from_i64(100),
            )
            .unwrap()
        })
    });
    group.throughput(Throughput::Bytes(nested_legacy.len() as u64));
    group.bench_function("nested_row_encode_name_keyed", |b| {
        b.iter(|| encode_name_keyed(black_box(&nested)))
    });
    group.bench_function("nested_row_decode_name_keyed", |b| {
        b.iter(|| {
            let decoded: UserTableRow =
                flexbuffers::from_slice(black_box(&nested_legacy)).expect("name-keyed decode");
            decoded
        })
    });

    group.throughput(Throughput::Bytes(scalar_bytes.len() as u64));
    group.bench_function("scalar_row_encode_kobj", |b| {
        b.iter(|| encode_user_row(black_box(&scalar), black_box(&scalar_schema)).unwrap())
    });
    group.bench_function("scalar_row_decode_kobj", |b| {
        b.iter(|| {
            decode_user_row(
                black_box(&scalar_bytes),
                black_box(&scalar_schema),
                UserId::new("user-1"),
                SeqId::from_i64(100),
            )
            .unwrap()
        })
    });
    group.throughput(Throughput::Bytes(scalar_legacy.len() as u64));
    group.bench_function("scalar_row_encode_name_keyed", |b| {
        b.iter(|| encode_name_keyed(black_box(&scalar)))
    });
    group.bench_function("scalar_row_decode_name_keyed", |b| {
        b.iter(|| {
            let decoded: UserTableRow =
                flexbuffers::from_slice(black_box(&scalar_legacy)).expect("name-keyed decode");
            decoded
        })
    });

    group.throughput(Throughput::Bytes((scalar_bytes.len() * BATCH_SIZE) as u64));
    group.bench_function("batch_scalar_encode_decode_kobj", |b| {
        b.iter_batched(
            || Vec::with_capacity(BATCH_SIZE),
            |mut encoded| {
                encoded.clear();
                for _ in 0..BATCH_SIZE {
                    encoded.push(encode_user_row(&scalar, &scalar_schema).unwrap().into_bytes());
                }
                for bytes in &encoded {
                    decode_user_row(
                        bytes,
                        &scalar_schema,
                        UserId::new("user-1"),
                        SeqId::from_i64(100),
                    )
                    .unwrap();
                }
                encoded
            },
            BatchSize::SmallInput,
        )
    });

    group.throughput(Throughput::Bytes(catalog_bytes.len() as u64));
    group.bench_function("object_encode", |b| {
        b.iter(|| encode_object(black_box(&catalog)).unwrap())
    });
    group.bench_function("object_decode", |b| {
        b.iter(|| {
            let _: CatalogObject = decode_object(black_box(&catalog_bytes)).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_codec);
criterion_main!(benches);
