//! Partition statistics and [`PlanProperties`] builders.
//!
//! Kept narrow on purpose: these helpers wrap DataFusion's own
//! `Statistics` and `PlanProperties` types so providers can emit trustworthy
//! metadata without duplicating builder code.

use datafusion::{
    common::stats::Precision,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        Partitioning, PlanProperties, Statistics,
    },
};

/// Build conservative [`PlanProperties`] for a single-partition, bounded
/// source. Sources with richer guarantees should call [`PlanProperties::new`]
/// directly with their own equivalence properties.
pub fn single_partition_plan_properties(schema: arrow_schema::SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Build [`PlanProperties`] for a bounded source that emits a known number of
/// independent partitions (for example, one per Parquet segment).
pub fn multi_partition_plan_properties(
    schema: arrow_schema::SchemaRef,
    partition_count: usize,
) -> PlanProperties {
    let partitioning = if partition_count <= 1 {
        Partitioning::UnknownPartitioning(1)
    } else {
        Partitioning::UnknownPartitioning(partition_count)
    };

    PlanProperties::new(
        EquivalenceProperties::new(schema),
        partitioning,
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}

/// Build conservative [`Statistics`] when row-count and size are unknown.
///
/// Use when the source cannot cheaply estimate cardinality (e.g., live
/// RocksDB tail reads). The optimizer will treat the source as unknown
/// instead of accepting a fabricated estimate.
pub fn unknown_statistics(num_columns: usize) -> Statistics {
    Statistics {
        num_rows: Precision::Absent,
        total_byte_size: Precision::Absent,
        column_statistics: Statistics::unknown_column(&arrow_schema::Schema::new(
            (0..num_columns)
                .map(|i| {
                    arrow_schema::Field::new(
                        format!("__col_{i}"),
                        arrow_schema::DataType::Null,
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        )),
    }
}

/// Build [`Statistics`] from an exact row count and an optional byte size
/// estimate. Sources that already track accurate cardinality (e.g., count
/// metadata from flushed manifests) should prefer this helper.
pub fn exact_row_statistics(
    num_columns: usize,
    num_rows: usize,
    total_byte_size: Option<usize>,
) -> Statistics {
    let mut stats = unknown_statistics(num_columns);
    stats.num_rows = Precision::Exact(num_rows);
    if let Some(bytes) = total_byte_size {
        stats.total_byte_size = Precision::Exact(bytes);
    }
    stats
}
