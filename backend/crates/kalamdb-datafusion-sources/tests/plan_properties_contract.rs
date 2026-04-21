//! `PlanProperties` contract: pin the defaults that every source family will
//! receive from the shared builders so downstream optimizer behavior stays
//! predictable.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::Partitioning;
use kalamdb_datafusion_sources::stats::{
    multi_partition_plan_properties, single_partition_plan_properties,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
}

#[test]
fn single_partition_has_unknown_partitioning_of_one() {
    let props = single_partition_plan_properties(schema());
    match props.partitioning {
        Partitioning::UnknownPartitioning(n) => assert_eq!(n, 1),
        other => panic!("unexpected partitioning {other:?}"),
    }
}

#[test]
fn multi_partition_collapses_to_single_when_count_is_zero_or_one() {
    for count in [0usize, 1] {
        let props = multi_partition_plan_properties(schema(), count);
        match props.partitioning {
            Partitioning::UnknownPartitioning(n) => assert_eq!(n, 1),
            other => panic!("unexpected partitioning at count={count}: {other:?}"),
        }
    }
}

#[test]
fn multi_partition_reports_requested_count() {
    let props = multi_partition_plan_properties(schema(), 7);
    match props.partitioning {
        Partitioning::UnknownPartitioning(n) => assert_eq!(n, 7),
        other => panic!("unexpected partitioning {other:?}"),
    }
}
