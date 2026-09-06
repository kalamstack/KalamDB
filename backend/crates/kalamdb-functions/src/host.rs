//! Host callbacks injected into a V8 isolate. Implemented by `kalamdb-core`.

use std::{future::Future, pin::Pin};

use crate::{error::Result, value::RoutineValue, InvocationMetadata};

pub type HostFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Host-created invocation metadata. Callers cannot forge this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationSource {
    Call,
    Topic {
        topic_name: String,
        event_id:   String,
        partition:  u32,
        offset:     u64,
        attempt:    u32,
    },
}

/// Synchronous host surface used by `ctx.db` / `ctx.functions` / `ctx.topics` / `ctx.http`.
///
/// Nested work must not re-enter the calling isolate. Core runs host methods
/// via `Handle::block_on` from the V8 worker thread.
pub trait FunctionHost: Send + Sync {
    fn sql(&self, sql: &str) -> Result<RoutineValue>;
    fn call(&self, procedure: &str, args: &[RoutineValue]) -> Result<RoutineValue>;
    fn publish(&self, topic: &str, payload: &RoutineValue) -> Result<()>;
    fn http_request_header(&self, name: &str) -> Result<Option<String>>;
    fn http_set_status(&self, status: i32) -> Result<()>;
    fn http_set_header(&self, name: &str, value: &str) -> Result<()>;
    fn is_http_root(&self) -> bool;
    fn query(&self, sql: String, params: Vec<RoutineValue>) -> HostFuture<'_, RoutineValue> {
        Box::pin(async move {
            if !params.is_empty() {
                return Err(crate::FunctionsError::Invalid(
                    "host does not support parameters".into(),
                ));
            }
            self.sql(&sql)
        })
    }
    fn execute(&self, sql: String, params: Vec<RoutineValue>) -> HostFuture<'_, RoutineValue> {
        self.query(sql, params)
    }
    fn call_async(
        &self,
        procedure: String,
        args: Vec<RoutineValue>,
    ) -> HostFuture<'_, RoutineValue> {
        Box::pin(async move { self.call(&procedure, &args) })
    }
    fn publish_async(&self, topic: String, payload: RoutineValue) -> HostFuture<'_, ()> {
        Box::pin(async move { self.publish(&topic, &payload) })
    }
    fn metadata(&self) -> Option<InvocationMetadata> {
        None
    }
    fn log(&self, level: &str, message: &str) -> Result<()> {
        let _ = (level, message);
        Ok(())
    }
    fn invocation_source(&self) -> InvocationSource {
        InvocationSource::Call
    }
    fn parent_procedure(&self) -> Option<String> {
        None
    }
}
