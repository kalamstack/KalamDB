//! Shared [`ExecutionPlan`] scaffolding built on the DataFusion 53.x surface.
//!
//! This module intentionally stays thin: it provides helpers that consumers
//! embed inside their own `ExecutionPlan` implementations, instead of forcing a
//! single monolithic plan type across families with very different semantics
//! (MVCC merge, one-shot views, vector TVFs, overlay).

use std::{
    any::Any,
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
};

use arrow::{
    array::{Array, BooleanArray, Int64Array, StringArray, UInt64Array},
    compute,
    record_batch::RecordBatch,
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::PhysicalExpr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
    scalar::ScalarValue,
};
use kalamdb_commons::{
    constants::SystemColumnNames, conversions::arrow_json_conversion::arrow_value_to_scalar,
    ids::SeqId, models::rows::Row, serialization::row_codec::RowMetadata,
};

use crate::{stats::single_partition_plan_properties, stream::one_shot_batch_stream};

/// Apply provider-side filter, projection, and limit handling to a deferred
/// source batch after the source has materialized its raw rows.
pub fn finalize_deferred_batch(
    mut batch: RecordBatch,
    physical_filter: Option<&Arc<dyn PhysicalExpr>>,
    projection: Option<&[usize]>,
    limit: Option<usize>,
    source_name: &str,
) -> DataFusionResult<RecordBatch> {
    if let Some(predicate) = physical_filter {
        let evaluated = predicate.evaluate(&batch)?.into_array(batch.num_rows())?;
        let Some(mask) = evaluated.as_any().downcast_ref::<BooleanArray>() else {
            return Err(DataFusionError::Execution(format!(
                "{source_name} filter expression did not evaluate to BooleanArray",
            )));
        };
        batch = compute::filter_record_batch(&batch, mask)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    }

    if let Some(projection) = projection {
        batch = batch
            .project(projection)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    }

    if let Some(limit) = limit {
        batch = batch.slice(0, limit.min(batch.num_rows()));
    }

    Ok(batch)
}

/// Project a schema with the requested column indices, or return the original
/// schema when no projection was requested.
pub fn projected_schema(
    input_schema: &SchemaRef,
    projection: Option<&[usize]>,
) -> DataFusionResult<SchemaRef> {
    match projection {
        Some(indices) => input_schema
            .project(indices)
            .map(Arc::new)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
        None => Ok(Arc::clone(input_schema)),
    }
}

/// Shared MVCC version ordering: `(commit_seq, seq_id)` with `commit_seq` as
/// the primary sort key and `seq_id` as the tiebreaker.
pub fn version_ordering<S>(
    candidate_commit_seq: u64,
    candidate_seq: S,
    current_commit_seq: u64,
    current_seq: S,
) -> Ordering
where
    S: Ord,
{
    candidate_commit_seq
        .cmp(&current_commit_seq)
        .then_with(|| candidate_seq.cmp(&current_seq))
}

/// Return `true` when the candidate version should replace the current one.
pub fn prefers_version<S>(
    candidate_commit_seq: u64,
    candidate_seq: S,
    current_commit_seq: u64,
    current_seq: S,
) -> bool
where
    S: Ord,
{
    version_ordering(candidate_commit_seq, candidate_seq, current_commit_seq, current_seq).is_gt()
}

/// Shared version candidate used by metadata-first MVCC merge helpers.
pub struct VersionCandidate<P, S> {
    pub pk_key: String,
    pub commit_seq: u64,
    pub seq_id: S,
    pub deleted: bool,
    pub payload: P,
}

impl<P, S> VersionCandidate<P, S> {
    pub fn new(pk_key: String, commit_seq: u64, seq_id: S, deleted: bool, payload: P) -> Self {
        Self {
            pk_key,
            commit_seq,
            seq_id,
            deleted,
            payload,
        }
    }
}

/// Selected latest visible version per primary-key bucket.
pub enum SelectedVersion<H, C> {
    Hot(H),
    Cold(C),
}

enum Candidate<H, C, S> {
    Hot(VersionCandidate<H, S>),
    Cold(VersionCandidate<C, S>),
}

impl<H, C, S> Candidate<H, C, S>
where
    S: Copy,
{
    fn pk_key(&self) -> &str {
        match self {
            Candidate::Hot(candidate) => candidate.pk_key.as_str(),
            Candidate::Cold(candidate) => candidate.pk_key.as_str(),
        }
    }

    fn commit_seq(&self) -> u64 {
        match self {
            Candidate::Hot(candidate) => candidate.commit_seq,
            Candidate::Cold(candidate) => candidate.commit_seq,
        }
    }

    fn seq_id(&self) -> S {
        match self {
            Candidate::Hot(candidate) => candidate.seq_id,
            Candidate::Cold(candidate) => candidate.seq_id,
        }
    }

    fn deleted(&self) -> bool {
        match self {
            Candidate::Hot(candidate) => candidate.deleted,
            Candidate::Cold(candidate) => candidate.deleted,
        }
    }
}

#[inline]
fn is_visible_at_snapshot(commit_seq: u64, snapshot_commit_seq: Option<u64>) -> bool {
    snapshot_commit_seq.is_none_or(|snapshot| commit_seq <= snapshot)
}

/// Select the latest visible version for each primary-key bucket while keeping
/// cold inputs metadata-only until the caller decides which winners to
/// materialize.
pub fn select_latest_versions<H, C, S, HI, CI>(
    hot_candidates: HI,
    cold_candidates: CI,
    snapshot_commit_seq: Option<u64>,
    keep_deleted: bool,
) -> Vec<SelectedVersion<H, C>>
where
    HI: IntoIterator<Item = VersionCandidate<H, S>>,
    CI: IntoIterator<Item = VersionCandidate<C, S>>,
    S: Ord + Copy,
{
    let hot_iter = hot_candidates.into_iter();
    let cold_iter = cold_candidates.into_iter();
    let estimated_capacity = hot_iter.size_hint().0.saturating_add(cold_iter.size_hint().0).max(64);
    let mut best: HashMap<String, Candidate<H, C, S>> = HashMap::with_capacity(estimated_capacity);

    for candidate in hot_iter.map(Candidate::Hot).chain(cold_iter.map(Candidate::Cold)) {
        if !is_visible_at_snapshot(candidate.commit_seq(), snapshot_commit_seq) {
            continue;
        }

        let pk_key = candidate.pk_key().to_owned();
        match best.entry(pk_key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if prefers_version(
                    candidate.commit_seq(),
                    candidate.seq_id(),
                    entry.get().commit_seq(),
                    entry.get().seq_id(),
                ) {
                    entry.insert(candidate);
                }
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(candidate);
            },
        }
    }

    best.into_values()
        .filter_map(|winner| {
            if !keep_deleted && winner.deleted() {
                return None;
            }

            Some(match winner {
                Candidate::Hot(candidate) => SelectedVersion::Hot(candidate.payload),
                Candidate::Cold(candidate) => SelectedVersion::Cold(candidate.payload),
            })
        })
        .collect()
}

/// Parsed representation of a Parquet row used for MVCC version resolution.
#[derive(Debug, Clone)]
pub struct ParquetRowData {
    pub seq_id: SeqId,
    pub commit_seq: u64,
    pub deleted: bool,
    pub fields: Row,
}

/// Minimal row surface required by the shared MVCC merge helpers.
pub trait VersionedRow {
    fn seq_id(&self) -> SeqId;
    fn commit_seq(&self) -> u64;
    fn deleted(&self) -> bool;
    fn pk_value(&self, pk_name: &str) -> Option<String>;
}

impl VersionedRow for RowMetadata {
    fn seq_id(&self) -> SeqId {
        self.seq
    }

    fn commit_seq(&self) -> u64 {
        self.commit_seq
    }

    fn deleted(&self) -> bool {
        self.deleted
    }

    fn pk_value(&self, _pk_name: &str) -> Option<String> {
        self.pk_value.clone()
    }
}

pub fn candidate_pk_key<R: VersionedRow>(pk_name: &str, row: &R) -> String {
    row.pk_value(pk_name)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("_seq:{}", row.seq_id().as_i64()))
}

pub fn version_candidate_from_row<R, P>(
    pk_name: &str,
    row: &R,
    payload: P,
) -> VersionCandidate<P, SeqId>
where
    R: VersionedRow,
{
    VersionCandidate::new(
        candidate_pk_key(pk_name, row),
        row.commit_seq(),
        row.seq_id(),
        row.deleted(),
        payload,
    )
}

pub fn count_merged_rows<R, I, J>(
    pk_name: &str,
    hot_rows: I,
    cold_rows: J,
    snapshot_commit_seq: Option<u64>,
) -> usize
where
    I: IntoIterator<Item = R>,
    J: IntoIterator<Item = R>,
    R: VersionedRow,
{
    select_latest_versions(
        hot_rows.into_iter().map(|row| version_candidate_from_row(pk_name, &row, ())),
        cold_rows.into_iter().map(|row| version_candidate_from_row(pk_name, &row, ())),
        snapshot_commit_seq,
        false,
    )
    .len()
}

pub fn count_resolved_from_metadata(
    pk_name: &str,
    hot_metadata: Vec<RowMetadata>,
    cold_batch: &RecordBatch,
    snapshot_commit_seq: Option<u64>,
) -> DataFusionResult<usize> {
    let cold_metadata = parquet_batch_to_metadata(cold_batch, pk_name)?;

    Ok(count_merged_rows(pk_name, hot_metadata, cold_metadata, snapshot_commit_seq))
}

pub fn merge_versioned_rows<K, R, I, J>(
    pk_name: &str,
    hot_rows: I,
    cold_rows: J,
    keep_deleted: bool,
    snapshot_commit_seq: Option<u64>,
) -> Vec<(K, R)>
where
    I: IntoIterator<Item = (K, R)>,
    J: IntoIterator<Item = (K, R)>,
    K: Clone,
    R: VersionedRow,
{
    select_latest_versions(
        hot_rows.into_iter().map(|(key, row)| {
            let pk_key = candidate_pk_key(pk_name, &row);
            let commit_seq = row.commit_seq();
            let seq_id = row.seq_id();
            let deleted = row.deleted();
            VersionCandidate::new(pk_key, commit_seq, seq_id, deleted, (key, row))
        }),
        cold_rows.into_iter().map(|(key, row)| {
            let pk_key = candidate_pk_key(pk_name, &row);
            let commit_seq = row.commit_seq();
            let seq_id = row.seq_id();
            let deleted = row.deleted();
            VersionCandidate::new(pk_key, commit_seq, seq_id, deleted, (key, row))
        }),
        snapshot_commit_seq,
        keep_deleted,
    )
    .into_iter()
    .map(|winner| match winner {
        SelectedVersion::Hot(row) | SelectedVersion::Cold(row) => row,
    })
    .collect()
}

pub fn resolve_latest_kvs_from_cold_batch<K, R, I, F>(
    pk_name: &str,
    hot_rows: I,
    cold_batch: &RecordBatch,
    keep_deleted: bool,
    snapshot_commit_seq: Option<u64>,
    build_cold_row: F,
) -> DataFusionResult<Vec<(K, R)>>
where
    I: IntoIterator<Item = (K, R)>,
    F: Fn(ParquetRowData) -> DataFusionResult<(K, R)>,
    K: Clone,
    R: VersionedRow,
{
    let decoder = ParquetBatchDecoder::new(cold_batch, Some(pk_name))?;
    let winners = select_latest_versions(
        hot_rows.into_iter().map(|(key, row)| {
            let pk_key = candidate_pk_key(pk_name, &row);
            let commit_seq = row.commit_seq();
            let seq_id = row.seq_id();
            let deleted = row.deleted();
            VersionCandidate::new(pk_key, commit_seq, seq_id, deleted, (key, row))
        }),
        (0..cold_batch.num_rows()).map(|row_idx| {
            let metadata = decoder.metadata_at(row_idx);
            let pk_key = metadata
                .pk_value
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| format!("_seq:{}", metadata.seq.as_i64()));
            VersionCandidate::new(
                pk_key,
                metadata.commit_seq,
                metadata.seq,
                metadata.deleted,
                row_idx,
            )
        }),
        snapshot_commit_seq,
        keep_deleted,
    );

    let mut resolved = Vec::with_capacity(winners.len());
    for winner in winners {
        match winner {
            SelectedVersion::Hot(row) => resolved.push(row),
            SelectedVersion::Cold(row_idx) => {
                resolved.push(build_cold_row(decoder.row_at(row_idx)?)?)
            },
        }
    }

    Ok(resolved)
}

/// Decode a Parquet batch into full row payloads for version-resolution callers.
pub fn parquet_batch_to_rows(batch: &RecordBatch) -> DataFusionResult<Vec<ParquetRowData>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let decoder = ParquetBatchDecoder::new(batch, None)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        rows.push(decoder.row_at(row_idx)?);
    }

    Ok(rows)
}

/// Decode only the metadata needed for count-only and winner-selection MVCC paths.
pub fn parquet_batch_to_metadata(
    batch: &RecordBatch,
    pk_name: &str,
) -> DataFusionResult<Vec<RowMetadata>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let decoder = ParquetBatchDecoder::new(batch, Some(pk_name))?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        rows.push(decoder.metadata_at(row_idx));
    }

    Ok(rows)
}

/// Shared decoder for metadata-first MVCC merge callers that need to delay
/// cold-row materialization until after winner selection.
#[derive(Debug)]
pub struct ParquetBatchDecoder<'a> {
    batch: &'a RecordBatch,
    seq_array: &'a Int64Array,
    commit_seq_array: Option<&'a UInt64Array>,
    deleted_array: Option<&'a BooleanArray>,
    pk_idx: Option<usize>,
    pk_string_array: Option<&'a StringArray>,
    value_column_indices: Vec<usize>,
}

impl<'a> ParquetBatchDecoder<'a> {
    pub fn new(batch: &'a RecordBatch, pk_name: Option<&str>) -> DataFusionResult<Self> {
        let schema = batch.schema();
        let seq_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == SystemColumnNames::SEQ)
            .ok_or_else(|| {
                DataFusionError::Execution("Missing _seq column in Parquet batch".to_string())
            })?;
        let deleted_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == SystemColumnNames::DELETED);
        let commit_seq_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == SystemColumnNames::COMMIT_SEQ);
        let pk_idx =
            pk_name.and_then(|name| schema.fields().iter().position(|field| field.name() == name));

        let seq_array =
            batch.column(seq_idx).as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("_seq column is not Int64Array".to_string())
            })?;
        let deleted_array =
            deleted_idx.and_then(|idx| batch.column(idx).as_any().downcast_ref::<BooleanArray>());
        let commit_seq_array =
            commit_seq_idx.and_then(|idx| batch.column(idx).as_any().downcast_ref::<UInt64Array>());
        let pk_string_array =
            pk_idx.and_then(|idx| batch.column(idx).as_any().downcast_ref::<StringArray>());
        let value_column_indices = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() != SystemColumnNames::SEQ
                    && field.name() != SystemColumnNames::COMMIT_SEQ
                    && field.name() != SystemColumnNames::DELETED
            })
            .map(|(idx, _)| idx)
            .collect();

        Ok(Self {
            batch,
            seq_array,
            commit_seq_array,
            deleted_array,
            pk_idx,
            pk_string_array,
            value_column_indices,
        })
    }

    pub fn metadata_at(&self, row_idx: usize) -> RowMetadata {
        let seq = SeqId::from_i64(self.seq_array.value(row_idx));
        let deleted = self
            .deleted_array
            .and_then(|array| (!array.is_null(row_idx)).then(|| array.value(row_idx)))
            .unwrap_or(false);
        let commit_seq = self
            .commit_seq_array
            .and_then(|array| (!array.is_null(row_idx)).then(|| array.value(row_idx)))
            .unwrap_or(0);
        let pk_value = if let Some(string_array) = self.pk_string_array {
            if string_array.is_null(row_idx) {
                None
            } else {
                let value = string_array.value(row_idx);
                if value.is_empty() {
                    None
                } else {
                    Some(value.to_owned())
                }
            }
        } else {
            self.pk_idx.and_then(|idx| {
                let array = self.batch.column(idx);
                arrow_value_to_scalar(array.as_ref(), row_idx)
                    .ok()
                    .and_then(|value| match &value {
                        ScalarValue::Utf8(Some(string)) | ScalarValue::LargeUtf8(Some(string)) => {
                            Some(string.clone())
                        },
                        other if other.is_null() => None,
                        other => Some(other.to_string()),
                    })
            })
        };

        RowMetadata {
            seq,
            commit_seq,
            deleted,
            pk_value,
        }
    }

    pub fn row_at(&self, row_idx: usize) -> DataFusionResult<ParquetRowData> {
        let metadata = self.metadata_at(row_idx);
        let mut values = BTreeMap::new();
        let schema = self.batch.schema();

        for &col_idx in &self.value_column_indices {
            let col_name = schema.field(col_idx).name();
            let array = self.batch.column(col_idx);
            match arrow_value_to_scalar(array.as_ref(), row_idx) {
                Ok(value) => {
                    values.insert(col_name.clone(), value);
                },
                Err(error) => {
                    tracing::warn!(
                        column = %col_name,
                        row_idx,
                        error = %error,
                        "Failed to convert column while decoding Parquet MVCC row"
                    );
                },
            }
        }

        Ok(ParquetRowData {
            seq_id: metadata.seq,
            commit_seq: metadata.commit_seq,
            deleted: metadata.deleted,
            fields: Row::new(values),
        })
    }
}

/// Shared handle to the target schema so exec nodes built on top of the
/// shared substrate can share an `Arc<Schema>` instead of cloning it.
pub type SharedSchema = Arc<arrow_schema::Schema>;

/// Deferred source that produces a single [`RecordBatch`] during
/// [`ExecutionPlan::execute`] instead of doing source I/O during planning.
///
/// This is the first shared building block for provider families that can
/// describe their work cheaply in `TableProvider::scan()` and materialize the
/// batch only when execution actually begins.
#[async_trait]
pub trait DeferredBatchSource: Send + Sync {
    fn source_name(&self) -> &'static str;

    fn schema(&self) -> SchemaRef;

    async fn produce_batch(&self) -> DataFusionResult<RecordBatch>;
}

/// Shared execution node for one-shot sources that defer batch creation until
/// execution time.
pub struct DeferredBatchExec {
    source: Arc<dyn DeferredBatchSource>,
    properties: Arc<PlanProperties>,
}

impl DeferredBatchExec {
    pub fn new(source: Arc<dyn DeferredBatchSource>) -> Self {
        let properties = Arc::new(single_partition_plan_properties(source.schema()));
        Self { source, properties }
    }
}

impl fmt::Debug for DeferredBatchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeferredBatchExec")
            .field("source", &self.source.source_name())
            .finish()
    }
}

impl DisplayAs for DeferredBatchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeferredBatchExec: source={}", self.source.source_name())
            },
            DisplayFormatType::TreeRender => write!(f, "source={}", self.source.source_name()),
        }
    }
}

impl ExecutionPlan for DeferredBatchExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "DeferredBatchExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "DeferredBatchExec only supports partition 0, got {partition}",
            )));
        }

        let source = Arc::clone(&self.source);
        let schema = source.schema();
        Ok(one_shot_batch_stream(schema, async move { source.produce_batch().await }))
    }
}
