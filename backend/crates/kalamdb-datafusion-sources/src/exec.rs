//! Shared [`ExecutionPlan`] scaffolding built on the DataFusion 55.x surface.
//!
//! This module intentionally stays thin: it provides helpers that consumers
//! embed inside their own `ExecutionPlan` implementations, instead of forcing a
//! single monolithic plan type across families with very different semantics
//! (MVCC merge, one-shot views, vector TVFs, overlay).

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
};

use arrow::{
    array::{
        Array, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
        StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    compute,
    record_batch::RecordBatch,
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::PhysicalExpr,
    physical_plan::{
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        ReplaceChildrenOptions,
    },
};
use kalamdb_commons::{
    constants::SystemColumnNames,
    conversions::arrow_json_conversion::arrow_value_to_scalar,
    ids::SeqId,
    models::rows::{Row, RowMetadata, SharedTableRow},
};
pub use kalamdb_commons::{
    pk_bucket_key_from_array, pk_bucket_key_from_row, pk_bucket_key_from_scalar, PkBucketKey,
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
                "{source_name} filter expression did not evaluate to BooleanArray"
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
    pub pk_key:     PkBucketKey,
    pub commit_seq: u64,
    pub seq_id:     S,
    pub deleted:    bool,
    pub payload:    P,
}

impl<P, S> VersionCandidate<P, S> {
    pub fn new(
        pk_key: impl Into<PkBucketKey>,
        commit_seq: u64,
        seq_id: S,
        deleted: bool,
        payload: P,
    ) -> Self {
        Self {
            pk_key: pk_key.into(),
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
    Hot(VersionMeta<H, S>),
    Cold(VersionMeta<C, S>),
}

struct VersionMeta<P, S> {
    commit_seq: u64,
    seq_id:     S,
    deleted:    bool,
    payload:    P,
}

impl<H, C, S> Candidate<H, C, S>
where
    S: Copy,
{
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

#[inline]
fn consider_candidate<H, C, S>(
    best: &mut HashMap<PkBucketKey, Candidate<H, C, S>>,
    pk_key: PkBucketKey,
    candidate: Candidate<H, C, S>,
    snapshot_commit_seq: Option<u64>,
) where
    S: Ord + Copy,
{
    if !is_visible_at_snapshot(candidate.commit_seq(), snapshot_commit_seq) {
        return;
    }

    match best.entry(pk_key) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let current = entry.get();
            if prefers_version(
                candidate.commit_seq(),
                candidate.seq_id(),
                current.commit_seq(),
                current.seq_id(),
            ) {
                entry.insert(candidate);
            }
        },
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(candidate);
        },
    }
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
    let mut best: HashMap<PkBucketKey, Candidate<H, C, S>> =
        HashMap::with_capacity(estimated_capacity);

    for VersionCandidate {
        pk_key,
        commit_seq,
        seq_id,
        deleted,
        payload,
    } in hot_iter
    {
        consider_candidate(
            &mut best,
            pk_key,
            Candidate::Hot(VersionMeta {
                commit_seq,
                seq_id,
                deleted,
                payload,
            }),
            snapshot_commit_seq,
        );
    }

    for VersionCandidate {
        pk_key,
        commit_seq,
        seq_id,
        deleted,
        payload,
    } in cold_iter
    {
        consider_candidate(
            &mut best,
            pk_key,
            Candidate::Cold(VersionMeta {
                commit_seq,
                seq_id,
                deleted,
                payload,
            }),
            snapshot_commit_seq,
        );
    }

    let mut winners = Vec::with_capacity(best.len());
    for candidate in best.into_values() {
        if !keep_deleted && candidate.deleted() {
            continue;
        }
        winners.push(match candidate {
            Candidate::Hot(meta) => SelectedVersion::Hot(meta.payload),
            Candidate::Cold(meta) => SelectedVersion::Cold(meta.payload),
        });
    }
    winners
}

/// Parsed representation of a Parquet row used for MVCC version resolution.
#[derive(Debug, Clone)]
pub struct ParquetRowData {
    pub seq_id:     SeqId,
    pub commit_seq: u64,
    pub deleted:    bool,
    pub fields:     Row,
}

/// Minimal row surface required by the shared MVCC merge helpers.
pub trait VersionedRow {
    fn seq_id(&self) -> SeqId;
    fn commit_seq(&self) -> u64;
    fn deleted(&self) -> bool;
    fn pk_value(&self, pk_name: &str) -> Option<String>;

    fn pk_bucket_key(&self, pk_name: &str) -> PkBucketKey {
        match self.pk_value(pk_name) {
            Some(value) if !value.is_empty() => PkBucketKey::Text(value),
            _ => PkBucketKey::Seq(self.seq_id().as_i64()),
        }
    }
}

impl VersionedRow for SharedTableRow {
    fn seq_id(&self) -> SeqId {
        self._seq
    }

    fn commit_seq(&self) -> u64 {
        self._commit_seq
    }

    fn deleted(&self) -> bool {
        self._deleted
    }

    fn pk_value(&self, pk_name: &str) -> Option<String> {
        match self.pk_bucket_key(pk_name) {
            PkBucketKey::Seq(_) => None,
            key => Some(key.to_string()),
        }
    }

    fn pk_bucket_key(&self, pk_name: &str) -> PkBucketKey {
        pk_bucket_key_from_row(&self.fields, pk_name, self._seq)
    }
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
        match &self.pk_bucket {
            PkBucketKey::Seq(_) => None,
            key => Some(key.to_string()),
        }
    }

    fn pk_bucket_key(&self, _pk_name: &str) -> PkBucketKey {
        self.pk_bucket.clone()
    }
}

pub fn candidate_pk_key<R: VersionedRow>(pk_name: &str, row: &R) -> PkBucketKey {
    row.pk_bucket_key(pk_name)
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
            VersionCandidate::new(
                decoder.pk_bucket_at(row_idx),
                decoder.commit_seq_at(row_idx),
                decoder.seq_at(row_idx),
                decoder.deleted_at(row_idx),
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
    batch:                &'a RecordBatch,
    seq_array:            &'a Int64Array,
    commit_seq_array:     Option<&'a UInt64Array>,
    deleted_array:        Option<&'a BooleanArray>,
    pk_column:            Option<PkColumn<'a>>,
    value_column_indices: Vec<usize>,
}

#[derive(Clone, Copy, Debug)]
enum PkColumn<'a> {
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Generic(usize),
}

fn downcast_pk_column(batch: &RecordBatch, idx: usize) -> PkColumn<'_> {
    let any = batch.column(idx).as_any();
    if let Some(array) = any.downcast_ref::<Int64Array>() {
        PkColumn::Int64(array)
    } else if let Some(array) = any.downcast_ref::<Int32Array>() {
        PkColumn::Int32(array)
    } else if let Some(array) = any.downcast_ref::<Int16Array>() {
        PkColumn::Int16(array)
    } else if let Some(array) = any.downcast_ref::<Int8Array>() {
        PkColumn::Int8(array)
    } else if let Some(array) = any.downcast_ref::<UInt64Array>() {
        PkColumn::UInt64(array)
    } else if let Some(array) = any.downcast_ref::<UInt32Array>() {
        PkColumn::UInt32(array)
    } else if let Some(array) = any.downcast_ref::<UInt16Array>() {
        PkColumn::UInt16(array)
    } else if let Some(array) = any.downcast_ref::<UInt8Array>() {
        PkColumn::UInt8(array)
    } else if let Some(array) = any.downcast_ref::<StringArray>() {
        PkColumn::Utf8(array)
    } else if let Some(array) = any.downcast_ref::<LargeStringArray>() {
        PkColumn::LargeUtf8(array)
    } else {
        PkColumn::Generic(idx)
    }
}

#[inline]
fn null_or_key<T>(
    is_null: bool,
    seq: SeqId,
    value: T,
    to_key: impl FnOnce(T) -> PkBucketKey,
) -> PkBucketKey {
    if is_null {
        PkBucketKey::Seq(seq.as_i64())
    } else {
        to_key(value)
    }
}

fn utf8_pk_bucket(is_null: bool, value: &str, seq: SeqId) -> PkBucketKey {
    if is_null || value.is_empty() {
        PkBucketKey::Seq(seq.as_i64())
    } else {
        PkBucketKey::Text(value.to_owned())
    }
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
        let pk_column = pk_idx.map(|idx| downcast_pk_column(batch, idx));
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
            pk_column,
            value_column_indices,
        })
    }

    #[inline]
    fn seq_at(&self, row_idx: usize) -> SeqId {
        SeqId::from_i64(self.seq_array.value(row_idx))
    }

    #[inline]
    fn deleted_at(&self, row_idx: usize) -> bool {
        self.deleted_array
            .and_then(|array| (!array.is_null(row_idx)).then(|| array.value(row_idx)))
            .unwrap_or(false)
    }

    #[inline]
    fn commit_seq_at(&self, row_idx: usize) -> u64 {
        self.commit_seq_array
            .and_then(|array| (!array.is_null(row_idx)).then(|| array.value(row_idx)))
            .unwrap_or(0)
    }

    #[inline]
    fn pk_bucket_at(&self, row_idx: usize) -> PkBucketKey {
        let seq = self.seq_at(row_idx);
        let Some(pk_column) = self.pk_column else {
            return PkBucketKey::Seq(seq.as_i64());
        };

        match pk_column {
            PkColumn::Int8(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), |value| {
                    PkBucketKey::Int(i64::from(value))
                })
            },
            PkColumn::Int16(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), |value| {
                    PkBucketKey::Int(i64::from(value))
                })
            },
            PkColumn::Int32(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), |value| {
                    PkBucketKey::Int(i64::from(value))
                })
            },
            PkColumn::Int64(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), PkBucketKey::Int)
            },
            PkColumn::UInt8(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), |value| {
                    PkBucketKey::UInt(u64::from(value))
                })
            },
            PkColumn::UInt16(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), |value| {
                    PkBucketKey::UInt(u64::from(value))
                })
            },
            PkColumn::UInt32(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), |value| {
                    PkBucketKey::UInt(u64::from(value))
                })
            },
            PkColumn::UInt64(array) => {
                null_or_key(array.is_null(row_idx), seq, array.value(row_idx), PkBucketKey::UInt)
            },
            PkColumn::Utf8(array) => {
                utf8_pk_bucket(array.is_null(row_idx), array.value(row_idx), seq)
            },
            PkColumn::LargeUtf8(array) => {
                utf8_pk_bucket(array.is_null(row_idx), array.value(row_idx), seq)
            },
            PkColumn::Generic(idx) => {
                pk_bucket_key_from_array(self.batch.column(idx).as_ref(), row_idx, seq)
            },
        }
    }

    pub fn metadata_at(&self, row_idx: usize) -> RowMetadata {
        RowMetadata {
            seq:        self.seq_at(row_idx),
            commit_seq: self.commit_seq_at(row_idx),
            deleted:    self.deleted_at(row_idx),
            pk_bucket:  self.pk_bucket_at(row_idx),
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
            seq_id:     metadata.seq,
            commit_seq: metadata.commit_seq,
            deleted:    metadata.deleted,
            fields:     Row::new(values),
        })
    }
}

/// Shared handle to the target schema so exec nodes built on top of the
/// shared substrate can share an `Arc<Schema>` instead of cloning it.
pub type SharedSchema = Arc<arrow_schema::Schema>;

const MAX_RECORDED_SCAN_FILES: usize = 16;

/// Runtime diagnostics recorded by deferred scan sources.
///
/// Values are optional so sources only report measurements they can collect
/// cheaply and accurately.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeferredScanDiagnostics {
    pub hot_rows_scanned:   Option<usize>,
    pub cold_rows_scanned:  Option<usize>,
    pub cold_files_total:   Option<usize>,
    pub cold_files_skipped: Option<usize>,
    pub cold_files_scanned: Option<usize>,
    pub cold_files:         Vec<String>,
}

/// A materialized deferred batch with optional scan diagnostics.
#[derive(Debug)]
pub struct DeferredBatchOutput {
    pub batch:       RecordBatch,
    pub diagnostics: DeferredScanDiagnostics,
}

impl DeferredBatchOutput {
    pub fn new(batch: RecordBatch) -> Self {
        Self {
            batch,
            diagnostics: DeferredScanDiagnostics::default(),
        }
    }

    pub fn with_diagnostics(mut self, diagnostics: DeferredScanDiagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

#[derive(Clone)]
struct DeferredBatchMetrics {
    set:                Arc<ExecutionPlanMetricsSet>,
    output_rows:        Count,
    output_batches:     Count,
    hot_rows_scanned:   Count,
    cold_rows_scanned:  Count,
    cold_files_total:   Count,
    cold_files_skipped: Count,
    cold_files_scanned: Count,
}

impl DeferredBatchMetrics {
    fn new() -> Self {
        let set = Arc::new(ExecutionPlanMetricsSet::new());
        let output_rows = MetricBuilder::new(&set).global_counter("output_rows");
        let output_batches = MetricBuilder::new(&set).global_counter("output_batches");
        let hot_rows_scanned = MetricBuilder::new(&set).global_counter("hot_rows_scanned");
        let cold_rows_scanned = MetricBuilder::new(&set).global_counter("cold_rows_scanned");
        let cold_files_total = MetricBuilder::new(&set).global_counter("cold_files_total");
        let cold_files_skipped = MetricBuilder::new(&set).global_counter("cold_files_skipped");
        let cold_files_scanned = MetricBuilder::new(&set).global_counter("cold_files_scanned");

        Self {
            set,
            output_rows,
            output_batches,
            hot_rows_scanned,
            cold_rows_scanned,
            cold_files_total,
            cold_files_skipped,
            cold_files_scanned,
        }
    }

    fn record(&self, output: &DeferredBatchOutput) {
        self.output_rows.add(output.batch.num_rows());
        self.output_batches.add(1);

        let diagnostics = &output.diagnostics;
        if let Some(value) = diagnostics.hot_rows_scanned {
            self.hot_rows_scanned.add(value);
        }
        if let Some(value) = diagnostics.cold_rows_scanned {
            self.cold_rows_scanned.add(value);
        }
        if let Some(value) = diagnostics.cold_files_total {
            self.cold_files_total.add(value);
        }
        if let Some(value) = diagnostics.cold_files_skipped {
            self.cold_files_skipped.add(value);
        }
        if let Some(value) = diagnostics.cold_files_scanned {
            self.cold_files_scanned.add(value);
        }

        let recorded_files = diagnostics.cold_files.len().min(MAX_RECORDED_SCAN_FILES);
        for file in diagnostics.cold_files.iter().take(recorded_files) {
            MetricBuilder::new(&self.set)
                .with_new_label("file", file.clone())
                .global_counter("cold_file_visited")
                .add(1);
        }

        let truncated_by_source =
            diagnostics.cold_files.len().saturating_sub(MAX_RECORDED_SCAN_FILES);
        let truncated_by_scan_count =
            diagnostics.cold_files_scanned.unwrap_or(0).saturating_sub(recorded_files);
        let truncated = truncated_by_source.max(truncated_by_scan_count);
        if truncated > 0 {
            MetricBuilder::new(&self.set)
                .global_counter("cold_file_visited_truncated")
                .add(truncated);
        }
    }
}

/// Deferred source that produces a single [`RecordBatch`] during
/// [`ExecutionPlan::execute`] instead of doing source I/O during planning.
///
/// This is the first shared building block for provider families that can
/// describe their work cheaply in `TableProvider::scan()` and materialize the
/// batch only when execution actually begins.
#[async_trait]
pub trait DeferredBatchSource: Send + Sync {
    fn source_name(&self) -> &'static str;

    fn plan_details(&self) -> Option<String> {
        None
    }

    fn schema(&self) -> SchemaRef;

    async fn produce_batch(&self) -> DataFusionResult<RecordBatch>;

    async fn produce_batch_with_diagnostics(&self) -> DataFusionResult<DeferredBatchOutput> {
        Ok(DeferredBatchOutput::new(self.produce_batch().await?))
    }
}

/// Shared execution node for one-shot sources that defer batch creation until
/// execution time.
pub struct DeferredBatchExec {
    source:     Arc<dyn DeferredBatchSource>,
    properties: Arc<PlanProperties>,
    metrics:    Option<DeferredBatchMetrics>,
}

impl DeferredBatchExec {
    pub fn new(source: Arc<dyn DeferredBatchSource>) -> Self {
        let properties = Arc::new(single_partition_plan_properties(source.schema()));
        Self {
            source,
            properties,
            metrics: None,
        }
    }

    pub fn new_with_scan_diagnostics(source: Arc<dyn DeferredBatchSource>) -> Self {
        let properties = Arc::new(single_partition_plan_properties(source.schema()));
        Self {
            source,
            properties,
            metrics: Some(DeferredBatchMetrics::new()),
        }
    }

    /// Produce this one-shot node's batch without constructing a DataFusion
    /// task context and stream wrapper.
    pub async fn produce_batch_direct(&self) -> DataFusionResult<RecordBatch> {
        match &self.metrics {
            Some(metrics) => {
                let output = self.source.produce_batch_with_diagnostics().await?;
                metrics.record(&output);
                Ok(output.batch)
            },
            None => self.source.produce_batch().await,
        }
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
                write!(f, "DeferredBatchExec: source={}", self.source.source_name())?;
                if let Some(details) = self.source.plan_details() {
                    write!(f, ", {details}")?;
                }
                Ok(())
            },
            DisplayFormatType::TreeRender => {
                write!(f, "source={}", self.source.source_name())?;
                if let Some(details) = self.source.plan_details() {
                    write!(f, ", {details}")?;
                }
                Ok(())
            },
        }
    }
}

impl ExecutionPlan for DeferredBatchExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "DeferredBatchExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "DeferredBatchExec only supports partition 0, got {partition}"
            )));
        }

        let source = Arc::clone(&self.source);
        let schema = source.schema();
        let metrics = self.metrics.clone();
        Ok(one_shot_batch_stream(schema, async move {
            match metrics {
                Some(metrics) => {
                    let output = source.produce_batch_with_diagnostics().await?;
                    metrics.record(&output);
                    Ok(output.batch)
                },
                None => source.produce_batch().await,
            }
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.metrics.as_ref().map(|metrics| metrics.set.clone_inner())
    }
}
