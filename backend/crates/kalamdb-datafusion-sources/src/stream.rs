//! Record-batch stream adapters.
//!
//! Consumers compose these adapters on top of DataFusion's
//! [`SendableRecordBatchStream`][datafusion::execution::SendableRecordBatchStream]
//! rather than reimplementing ad-hoc row buffers. Arrow buffer sharing is
//! preserved wherever possible: the adapters forward `RecordBatch` slices and
//! never copy the underlying buffers.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::RecordBatchStream,
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
};
use futures_util::{stream, Stream};

/// Wrap a one-shot async batch producer as a [`SendableRecordBatchStream`].
///
/// Shared execution nodes use this when they produce a single materialized
/// batch at execute time but still need to satisfy DataFusion's streaming API.
pub fn one_shot_batch_stream<F>(schema: SchemaRef, future: F) -> SendableRecordBatchStream
where
    F: Future<Output = Result<RecordBatch, DataFusionError>> + Send + 'static,
{
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::once(future)))
}

/// Stream adapter that applies a soft limit at emission time without
/// materializing rows. The adapter slices the final batch when the limit is
/// reached so upstream Arrow buffers stay shared.
pub struct LimitedRecordBatchStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    remaining: usize,
    done: bool,
}

impl LimitedRecordBatchStream {
    pub fn new(inner: SendableRecordBatchStream, limit: usize) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            remaining: limit,
            done: false,
        }
    }
}

impl Stream for LimitedRecordBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done || self.remaining == 0 {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let rows = batch.num_rows();
                if rows == 0 {
                    return Poll::Ready(Some(Ok(batch)));
                }
                if rows <= self.remaining {
                    self.remaining -= rows;
                    if self.remaining == 0 {
                        self.done = true;
                    }
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    let take = self.remaining;
                    self.remaining = 0;
                    self.done = true;
                    Poll::Ready(Some(Ok(batch.slice(0, take))))
                }
            },
            other => other,
        }
    }
}

impl RecordBatchStream for LimitedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
