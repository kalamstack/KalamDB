//! Sandboxed V8 isolate adapter.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Once,
    },
    time::Instant,
};

use kalamdb_commons::RoutineId;
use tokio_util::sync::CancellationToken;
use v8::{self, ScriptOrigin};

use crate::{
    convert::{routine_to_v8, v8_to_routine},
    deadline::DeadlineGuard,
    error::{FunctionsError, Result},
    host::InvocationSource,
    limits::{RuntimeLimits, ABI_VERSION},
    revision::ModuleRevision,
    value::RoutineValue,
};

static V8_INIT: Once = Once::new();

fn ensure_v8() {
    V8_INIT.call_once(|| {
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    });
}

/// Pinned V8 session for one module revision.
pub struct V8Session {
    pub(crate) isolate:  v8::OwnedIsolate,
    pub(crate) context:  v8::Global<v8::Context>,
    pub(crate) revision: ModuleRevision,
    pub(crate) limits:   RuntimeLimits,
    heap_watch:          *mut HeapWatch,
    pub(crate) detached: bool,
    compiled:            Option<v8::Global<v8::UnboundScript>>,
}

struct HeapWatch {
    handle: v8::IsolateHandle,
    hit:    AtomicBool,
}

impl V8Session {
    pub fn load(revision: ModuleRevision, limits: RuntimeLimits) -> Result<Self> {
        ensure_v8();
        if !matches!(revision.abi_version, 1 | 2) {
            return Err(FunctionsError::AbiMismatch {
                artifact: revision.abi_version,
                runtime:  ABI_VERSION,
            });
        }
        if revision.runtime != kalamdb_commons::FunctionRuntime::Typescript {
            return Err(FunctionsError::Invalid(format!(
                "runtime {} is not available in v1",
                revision.runtime
            )));
        }

        let mut isolate = v8::Isolate::new(
            v8::CreateParams::default()
                .heap_limits(0, limits.max_heap_bytes)
                .set_max_old_generation_size_in_bytes(limits.max_heap_bytes),
        );
        let heap_watch = Box::into_raw(Box::new(HeapWatch {
            handle: isolate.thread_safe_handle(),
            hit:    AtomicBool::new(false),
        }));
        isolate.add_near_heap_limit_callback(near_heap_limit, heap_watch.cast());

        isolate.set_slot(ActiveHost { host: None });

        let deadline = DeadlineGuard::new(
            isolate.thread_safe_handle(),
            Instant::now() + limits.timeout,
            CancellationToken::new(),
        );
        let context_result = (|| {
            v8::scope!(let handle_scope, &mut isolate);
            let context = v8::Context::new(handle_scope, Default::default());
            let mut scope = v8::ContextScope::new(handle_scope, context);
            install_host_functions(&mut scope)?;
            compile_and_run(&mut scope, crate::wrap::HOST_BOOTSTRAP)?;
            compile_and_run(&mut scope, &revision.source)?;
            Ok(v8::Global::new(&scope, context))
        })();
        let context_result: Result<v8::Global<v8::Context>> = context_result;
        let context_result = deadline.check().and(context_result);
        drop(deadline);
        let context = match context_result {
            Ok(context) => context,
            Err(error) => {
                isolate.remove_near_heap_limit_callback(near_heap_limit, 0);
                // SAFETY: registration has been removed and this is its unique allocation.
                unsafe {
                    drop(Box::from_raw(heap_watch));
                }
                return Err(error);
            },
        };

        Ok(Self {
            isolate,
            context,
            revision,
            limits,
            heap_watch,
            detached: false,
            compiled: None,
        })
    }

    /// Leave the isolate before a worker polls another local invocation.
    pub(crate) fn detach(&mut self) {
        if !self.detached {
            // SAFETY: this session is exclusively owned by its worker and currently entered.
            unsafe {
                self.isolate.exit();
            }
            self.detached = true;
        }
    }

    pub(crate) fn attach(&mut self) {
        if self.detached {
            // SAFETY: sessions never move across workers; entry and exit are balanced.
            unsafe {
                self.isolate.enter();
            }
            self.detached = false;
        }
    }

    pub(crate) fn rebind(&mut self, revision: ModuleRevision) {
        self.attach();
        if self.revision != revision {
            self.compiled = None;
            self.revision = revision;
        }
    }

    pub fn revision(&self) -> &ModuleRevision {
        &self.revision
    }

    pub fn invoke(
        &mut self,
        procedure_id: &RoutineId,
        args: &[RoutineValue],
        cancel: &CancellationToken,
    ) -> Result<RoutineValue> {
        self.invoke_with_host(procedure_id, args, cancel, None)
    }

    pub fn invoke_with_host(
        &mut self,
        procedure_id: &RoutineId,
        args: &[RoutineValue],
        cancel: &CancellationToken,
        host: Option<Arc<dyn crate::host::FunctionHost>>,
    ) -> Result<RoutineValue> {
        if cancel.is_cancelled() {
            return Err(FunctionsError::Cancelled);
        }
        if let Some(slot) = self.isolate.get_slot_mut::<ActiveHost>() {
            slot.host = host;
        }
        let guard = DeadlineGuard::new(
            self.isolate.thread_safe_handle(),
            Instant::now() + self.limits.timeout,
            cancel.clone(),
        );
        let invoke_result = (|| {
            // Every invocation gets fresh globals, including module-level state.
            self.reset_context()?;
            self.invoke_inner(procedure_id, args)
        })();
        if let Some(slot) = self.isolate.get_slot_mut::<ActiveHost>() {
            slot.host = None;
        }
        guard.check()?;
        if self.heap_limit_hit() {
            return Err(FunctionsError::MemoryLimit);
        }
        invoke_result
    }

    pub(crate) fn reset_context(&mut self) -> Result<()> {
        v8::scope!(let scope, &mut self.isolate);
        let context = v8::Context::new(scope, Default::default());
        let mut scope = v8::ContextScope::new(scope, context);
        install_host_functions(&mut scope)?;
        let bootstrap = if self.revision.abi_version == 2 {
            crate::wrap::ASYNC_HOST_BOOTSTRAP
        } else {
            crate::wrap::HOST_BOOTSTRAP
        };
        compile_and_run(&mut scope, bootstrap)?;
        if self.revision.abi_version == 2 {
            crate::v8_async::install(&mut scope)?;
        }
        let compiled = match &self.compiled {
            Some(script) => v8::Local::new(&scope, script),
            None => {
                let code = v8::String::new(&scope, &self.revision.source)
                    .ok_or_else(|| FunctionsError::ResourceLimit("module source".into()))?;
                let script = v8::Script::compile(&scope, code, None).ok_or_else(|| {
                    FunctionsError::Javascript("module compilation failed".into())
                })?;
                let compiled = script.get_unbound_script(&scope);
                self.compiled = Some(v8::Global::new(&scope, compiled));
                compiled
            },
        };
        compiled
            .bind_to_current_context(&scope)
            .run(&scope)
            .ok_or_else(|| FunctionsError::Javascript("module initialization failed".into()))?;
        self.context = v8::Global::new(&scope, context);
        Ok(())
    }

    pub(crate) fn heap_limit_hit(&self) -> bool {
        // SAFETY: `heap_watch` is allocated in `load` and freed in `Drop`.
        !self.heap_watch.is_null() && unsafe { (*self.heap_watch).hit.load(Ordering::Relaxed) }
    }

    fn invoke_inner(
        &mut self,
        procedure_id: &RoutineId,
        args: &[RoutineValue],
    ) -> Result<RoutineValue> {
        v8::scope!(let handle_scope, &mut self.isolate);
        let context = v8::Local::new(handle_scope, &self.context);
        let mut scope = v8::ContextScope::new(handle_scope, context);
        invoke_in_scope(&mut scope, procedure_id, args)
    }
}

pub(crate) struct ActiveHost {
    pub(crate) host: Option<Arc<dyn crate::host::FunctionHost>>,
}

impl Drop for V8Session {
    fn drop(&mut self) {
        self.attach();
        self.isolate.remove_near_heap_limit_callback(near_heap_limit, 0);
        if !self.heap_watch.is_null() {
            // SAFETY: allocated in `load`; unique owner until this drop.
            unsafe {
                drop(Box::from_raw(self.heap_watch));
            }
            self.heap_watch = std::ptr::null_mut();
        }
    }
}

fn invoke_in_scope(
    scope: &mut v8::PinScope,
    procedure_id: &RoutineId,
    args: &[RoutineValue],
) -> Result<RoutineValue> {
    bind_ctx(scope)?;
    v8::tc_scope!(let try_catch, scope);
    let global = try_catch.get_current_context().global(try_catch);
    let name = v8::String::new(try_catch, "kalamInvoke")
        .ok_or_else(|| FunctionsError::Invalid("kalamInvoke name".to_string()))?;
    let func_value = global
        .get(try_catch, name.into())
        .ok_or_else(|| FunctionsError::Invalid("kalamInvoke is not defined".to_string()))?;
    let func = v8::Local::<v8::Function>::try_from(func_value)
        .map_err(|_| FunctionsError::Invalid("kalamInvoke is not a function".to_string()))?;

    let procedure = v8::String::new(try_catch, procedure_id.as_str())
        .ok_or_else(|| FunctionsError::Invalid("procedure name too large".to_string()))?;
    let js_args = v8::Array::new(try_catch, args.len() as i32);
    for (index, arg) in args.iter().enumerate() {
        let js_value = routine_to_v8(try_catch, arg)?;
        js_args
            .set_index(try_catch, index as u32, js_value)
            .ok_or_else(|| FunctionsError::Invalid("failed to set argument".to_string()))?;
    }

    let recv = v8::undefined(try_catch).into();
    let call_args: [v8::Local<v8::Value>; 2] = [procedure.into(), js_args.into()];
    let Some(mut result) = func.call(try_catch, recv, &call_args) else {
        return Err(js_exception(try_catch));
    };
    if try_catch.has_caught() {
        return Err(js_exception(try_catch));
    }

    if result.is_promise() {
        let promise = v8::Local::<v8::Promise>::try_from(result)
            .map_err(|_| FunctionsError::Invalid("invalid promise".into()))?;
        try_catch.perform_microtask_checkpoint();
        match promise.state() {
            v8::PromiseState::Fulfilled => result = promise.result(try_catch),
            v8::PromiseState::Rejected => {
                return Err(FunctionsError::Javascript(
                    promise.result(try_catch).to_rust_string_lossy(try_catch),
                ))
            },
            v8::PromiseState::Pending => {
                return Err(FunctionsError::Invalid(
                    "pending host operations require ABI v2".into(),
                ))
            },
        }
    }

    let template = args
        .first()
        .cloned()
        .unwrap_or_else(|| RoutineValue::new(datafusion_common::ScalarValue::Null));
    v8_to_routine(try_catch, result, &template)
}

pub(crate) fn bind_ctx(scope: &mut v8::PinScope) -> Result<()> {
    v8::tc_scope!(let try_catch, scope);
    let global = try_catch.get_current_context().global(try_catch);
    let name = v8::String::new(try_catch, "__kalamMakeCtx")
        .ok_or_else(|| FunctionsError::Invalid("__kalamMakeCtx name".to_string()))?;
    let func_value = global
        .get(try_catch, name.into())
        .ok_or_else(|| FunctionsError::Invalid("__kalamMakeCtx is not defined".to_string()))?;
    let func = v8::Local::<v8::Function>::try_from(func_value)
        .map_err(|_| FunctionsError::Invalid("__kalamMakeCtx is not a function".to_string()))?;
    let recv = v8::undefined(try_catch).into();
    let Some(ctx) = func.call(try_catch, recv, &[]) else {
        return Err(js_exception(try_catch));
    };
    let ctx_name = v8::String::new(try_catch, "__kalamCtx")
        .ok_or_else(|| FunctionsError::Invalid("__kalamCtx name".to_string()))?;
    global
        .set(try_catch, ctx_name.into(), ctx)
        .ok_or_else(|| FunctionsError::Invalid("failed to bind __kalamCtx".to_string()))?;
    Ok(())
}

fn install_host_functions(scope: &mut v8::PinScope) -> Result<()> {
    bind_native(scope, "kalamHostSql", host_sql)?;
    bind_native(scope, "kalamHostCall", host_call)?;
    bind_native(scope, "kalamHostPublish", host_publish)?;
    bind_native(scope, "kalamHostHttpHeader", host_http_header)?;
    bind_native(scope, "kalamHostHttpSetStatus", host_http_set_status)?;
    bind_native(scope, "kalamHostHttpSetHeader", host_http_set_header)?;
    bind_native(scope, "kalamHostIsHttpRoot", host_is_http_root)?;
    bind_native(scope, "kalamHostSource", host_source)?;
    bind_native(scope, "kalamHostParent", host_parent)?;
    Ok(())
}

pub(crate) fn bind_native(
    scope: &mut v8::PinScope,
    name: &str,
    callback: impl v8::MapFnTo<v8::FunctionCallback>,
) -> Result<()> {
    let global = scope.get_current_context().global(scope);
    let function = v8::Function::new(scope, callback)
        .ok_or_else(|| FunctionsError::Invalid(format!("failed to bind {name}")))?;
    let key = v8::String::new(scope, name)
        .ok_or_else(|| FunctionsError::Invalid(format!("{name} too large")))?;
    global
        .set(scope, key.into(), function.into())
        .ok_or_else(|| FunctionsError::Invalid(format!("failed to set {name}")))?;
    Ok(())
}

pub(crate) fn current_host(scope: &v8::PinScope) -> Result<Arc<dyn crate::host::FunctionHost>> {
    let slot = scope
        .get_slot::<ActiveHost>()
        .ok_or_else(|| FunctionsError::Invalid("function host slot missing".to_string()))?;
    slot.host
        .clone()
        .ok_or_else(|| FunctionsError::Invalid("function host is not bound".to_string()))
}

pub(crate) fn throw_host_error(scope: &mut v8::PinScope, error: FunctionsError) {
    let message = v8::String::new(scope, &error.to_string())
        .unwrap_or_else(|| v8::String::new(scope, "function host error").expect("fallback error"));
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}

pub(crate) fn arg_string(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    index: i32,
) -> String {
    if args.length() <= index {
        return String::new();
    }
    args.get(index)
        .to_string(scope)
        .map(|value| value.to_rust_string_lossy(scope))
        .unwrap_or_default()
}

fn host_sql(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let sql = arg_string(scope, &args, 0);
    match current_host(scope).and_then(|host| host.sql(&sql)) {
        Ok(value) => match routine_to_v8(scope, &value) {
            Ok(js) => rv.set(js),
            Err(error) => throw_host_error(scope, error),
        },
        Err(error) => throw_host_error(scope, error),
    }
}

fn host_call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let procedure = arg_string(scope, &args, 0);
    let mut call_args = Vec::new();
    if args.length() > 1 {
        let raw = args.get(1);
        if raw.is_array() {
            if let Ok(array) = v8::Local::<v8::Array>::try_from(raw) {
                for index in 0..array.length() {
                    let element =
                        array.get_index(scope, index).unwrap_or_else(|| v8::null(scope).into());
                    match crate::convert::infer_v8_value(scope, element) {
                        Ok(scalar) => call_args.push(RoutineValue::new(scalar)),
                        Err(error) => {
                            throw_host_error(scope, error);
                            return;
                        },
                    }
                }
            }
        } else if !raw.is_null_or_undefined() {
            match crate::convert::infer_v8_value(scope, raw) {
                Ok(scalar) => call_args.push(RoutineValue::new(scalar)),
                Err(error) => {
                    throw_host_error(scope, error);
                    return;
                },
            }
        }
    }
    match current_host(scope).and_then(|host| host.call(&procedure, &call_args)) {
        Ok(value) => match routine_to_v8(scope, &value) {
            Ok(js) => rv.set(js),
            Err(error) => throw_host_error(scope, error),
        },
        Err(error) => throw_host_error(scope, error),
    }
}

fn host_publish(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let topic = arg_string(scope, &args, 0);
    let payload = if args.length() > 1 {
        match crate::convert::infer_v8_value(scope, args.get(1)) {
            Ok(scalar) => RoutineValue::new(scalar),
            Err(error) => {
                throw_host_error(scope, error);
                return;
            },
        }
    } else {
        RoutineValue::new(datafusion_common::ScalarValue::Null)
    };
    if let Err(error) = current_host(scope).and_then(|host| host.publish(&topic, &payload)) {
        throw_host_error(scope, error);
    }
}

fn host_http_header(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let name = arg_string(scope, &args, 0);
    match current_host(scope).and_then(|host| host.http_request_header(&name)) {
        Ok(Some(value)) => {
            if let Some(js) = v8::String::new(scope, &value) {
                rv.set(js.into());
            }
        },
        Ok(None) => rv.set(v8::null(scope).into()),
        Err(error) => throw_host_error(scope, error),
    }
}

fn host_http_set_status(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let status = if args.length() > 0 {
        args.get(0).int32_value(scope).unwrap_or(200)
    } else {
        200
    };
    if let Err(error) = current_host(scope).and_then(|host| host.http_set_status(status)) {
        throw_host_error(scope, error);
    }
}

fn host_http_set_header(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let name = arg_string(scope, &args, 0);
    let value = arg_string(scope, &args, 1);
    if let Err(error) = current_host(scope).and_then(|host| host.http_set_header(&name, &value)) {
        throw_host_error(scope, error);
    }
}

fn host_is_http_root(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    match current_host(scope) {
        Ok(host) => rv.set(v8::Boolean::new(scope, host.is_http_root()).into()),
        Err(error) => throw_host_error(scope, error),
    }
}

fn host_parent(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    match current_host(scope) {
        Ok(host) => match host.parent_procedure() {
            Some(name) => {
                if let Some(value) = v8::String::new(scope, &name) {
                    rv.set(value.into());
                } else {
                    rv.set(v8::null(scope).into());
                }
            },
            None => rv.set(v8::null(scope).into()),
        },
        Err(_) => rv.set(v8::null(scope).into()),
    }
}

fn host_source(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let source = match current_host(scope) {
        Ok(host) => host.invocation_source(),
        Err(_) => InvocationSource::Call,
    };
    match source_to_v8(scope, &source) {
        Ok(value) => rv.set(value),
        Err(error) => throw_host_error(scope, error),
    }
}

fn source_to_v8<'s, 'i>(
    scope: &mut v8::PinScope<'s, 'i>,
    source: &InvocationSource,
) -> Result<v8::Local<'s, v8::Value>> {
    let object = v8::Object::new(scope);
    match source {
        InvocationSource::Call => {
            set_object_string(scope, &object, "kind", "call")?;
        },
        InvocationSource::Topic {
            topic_name,
            event_id,
            partition,
            offset,
            attempt,
        } => {
            set_object_string(scope, &object, "kind", "topic")?;
            set_object_string(scope, &object, "topicName", topic_name)?;
            set_object_string(scope, &object, "eventId", event_id)?;
            set_object_number(scope, &object, "partition", *partition as f64)?;
            set_object_number(scope, &object, "offset", *offset as f64)?;
            set_object_number(scope, &object, "attempt", *attempt as f64)?;
        },
    }
    Ok(object.into())
}

fn set_object_string(
    scope: &mut v8::PinScope,
    object: &v8::Local<v8::Object>,
    key: &str,
    value: &str,
) -> Result<()> {
    let key = v8::String::new(scope, key)
        .ok_or_else(|| FunctionsError::Invalid("source key too large".to_string()))?;
    let value = v8::String::new(scope, value)
        .ok_or_else(|| FunctionsError::Invalid("source value too large".to_string()))?;
    object
        .set(scope, key.into(), value.into())
        .ok_or_else(|| FunctionsError::Invalid("failed to set source field".to_string()))?;
    Ok(())
}

fn set_object_number(
    scope: &mut v8::PinScope,
    object: &v8::Local<v8::Object>,
    key: &str,
    value: f64,
) -> Result<()> {
    let key = v8::String::new(scope, key)
        .ok_or_else(|| FunctionsError::Invalid("source key too large".to_string()))?;
    let number = v8::Number::new(scope, value);
    object
        .set(scope, key.into(), number.into())
        .ok_or_else(|| FunctionsError::Invalid("failed to set source field".to_string()))?;
    Ok(())
}

fn compile_and_run(scope: &mut v8::PinScope, source: &str) -> Result<()> {
    v8::tc_scope!(let try_catch, scope);
    let code = v8::String::new(try_catch, source)
        .ok_or_else(|| FunctionsError::Invalid("module source too large".to_string()))?;
    let origin_name = v8::String::new(try_catch, "module.js").unwrap();
    let origin = ScriptOrigin::new(
        try_catch,
        origin_name.into(),
        0,
        0,
        false,
        0,
        None,
        false,
        false,
        false,
        None,
    );
    let Some(script) = v8::Script::compile(try_catch, code, Some(&origin)) else {
        return Err(js_exception(try_catch));
    };
    if script.run(try_catch).is_none() {
        return Err(js_exception(try_catch));
    }
    Ok(())
}

pub(crate) fn js_exception(
    try_catch: &mut v8::PinnedRef<'_, v8::TryCatch<v8::HandleScope>>,
) -> FunctionsError {
    if let Some(exception) = try_catch.exception() {
        let message = exception
            .to_string(try_catch)
            .map(|value| value.to_rust_string_lossy(try_catch))
            .unwrap_or_else(|| "javascript exception".to_string());
        if message.contains("heap") || message.contains("memory") {
            return FunctionsError::MemoryLimit;
        }
        return FunctionsError::Javascript(message);
    }
    FunctionsError::Javascript("terminated".to_string())
}

unsafe extern "C" fn near_heap_limit(
    data: *mut std::ffi::c_void,
    current_heap_limit: usize,
    _initial_heap_limit: usize,
) -> usize {
    if !data.is_null() {
        // SAFETY: `data` is the `HeapWatch` box registered in `V8Session::load`.
        let watch = unsafe { &*data.cast::<HeapWatch>() };
        watch.hit.store(true, Ordering::Relaxed);
        watch.handle.terminate_execution();
    }
    // V8 fatals unless this callback raises the limit enough for the
    // in-flight allocation to unwind after TerminateExecution.
    current_heap_limit
        .saturating_mul(2)
        .max(current_heap_limit.saturating_add(8 * 1024 * 1024))
}

pub const FIXTURE_SOURCE: &str = include_str!("../fixtures/module.js");

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use arrow::{
        array::StructArray,
        datatypes::{DataType, Field},
    };
    use datafusion_common::ScalarValue;
    use kalamdb_commons::RoutineId;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        error::FunctionsError, limits::RuntimeLimits, revision::ModuleRevision, value::RoutineValue,
    };

    fn echo_id() -> RoutineId {
        RoutineId::new("echo")
    }

    fn load_fixture(limits: RuntimeLimits) -> V8Session {
        V8Session::load(ModuleRevision::typescript_fixture(FIXTURE_SOURCE), limits).unwrap()
    }

    fn invoke_echo(session: &mut V8Session, value: RoutineValue) -> RoutineValue {
        session.invoke(&echo_id(), &[value], &CancellationToken::new()).unwrap()
    }

    #[test]
    fn echo_scalar_struct_list_and_json() {
        let cold = Instant::now();
        let mut session = load_fixture(RuntimeLimits::default());
        let cold_secs = cold.elapsed().as_secs_f64();

        let warm = Instant::now();
        let scalar = invoke_echo(
            &mut session,
            RoutineValue::new(ScalarValue::Utf8(Some("hello".to_string()))),
        );
        let warm_secs = warm.elapsed().as_secs_f64();
        assert_eq!(scalar.value, ScalarValue::Utf8(Some("hello".to_string())));

        let city = Arc::new(Field::new("city", DataType::Utf8, true));
        let zip = Arc::new(Field::new("zip", DataType::Int32, true));
        let struct_value = ScalarValue::Struct(Arc::new(StructArray::from(vec![
            (
                Arc::clone(&city),
                ScalarValue::Utf8(Some("Austin".to_string())).to_array().unwrap(),
            ),
            (Arc::clone(&zip), ScalarValue::Int32(Some(78701)).to_array().unwrap()),
        ])));
        let echoed_struct = invoke_echo(&mut session, RoutineValue::new(struct_value.clone()));
        assert_eq!(echoed_struct.value, struct_value);

        let list = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Utf8(Some("a".to_string())),
                ScalarValue::Utf8(Some("b".to_string())),
            ],
            &DataType::Utf8,
            true,
        ));
        let echoed_list = invoke_echo(&mut session, RoutineValue::new(list.clone()));
        assert_eq!(echoed_list.value, list);

        let json = invoke_echo(
            &mut session,
            RoutineValue::json(ScalarValue::Utf8(Some(r#"{"ok":true,"n":1}"#.to_string()))),
        );
        assert!(json.json_sql);
        let ScalarValue::Utf8(Some(text)) = json.value else {
            panic!("expected utf8 json");
        };
        assert!(text.contains("\"ok\":true"));
        assert!(text.contains("\"n\":1"));

        eprintln!(
            "kalamdb-functions v8 timings: cold_start={cold_secs:.4}s warm_invoke={warm_secs:.4}s"
        );
    }

    #[test]
    fn hang_times_out() {
        let mut session = load_fixture(RuntimeLimits {
            timeout: Duration::from_millis(200),
            ..RuntimeLimits::default()
        });
        let error = session
            .invoke(&RoutineId::new("hang"), &[], &CancellationToken::new())
            .unwrap_err();
        assert!(matches!(error, FunctionsError::Timeout), "{error}");
    }

    #[test]
    fn oom_hits_memory_limit() {
        let mut session = load_fixture(RuntimeLimits {
            timeout: Duration::from_secs(5),
            max_heap_bytes: 10 * 1024 * 1024,
            ..RuntimeLimits::default()
        });
        let error = session
            .invoke(&RoutineId::new("oom"), &[], &CancellationToken::new())
            .unwrap_err();
        assert!(matches!(error, FunctionsError::MemoryLimit), "{error}");
    }

    #[test]
    fn cancel_stops_hang() {
        let mut session = load_fixture(RuntimeLimits {
            timeout: Duration::from_secs(5),
            ..RuntimeLimits::default()
        });
        let cancel = CancellationToken::new();
        let cancel_watch = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            cancel_watch.cancel();
        });
        let error = session.invoke(&RoutineId::new("hang"), &[], &cancel).unwrap_err();
        assert!(matches!(error, FunctionsError::Cancelled), "{error}");
    }

    struct SqlHost;

    impl crate::FunctionHost for SqlHost {
        fn sql(&self, sql: &str) -> crate::Result<RoutineValue> {
            assert!(sql.contains("SELECT 1"));
            Ok(RoutineValue::new(ScalarValue::Int64(Some(1))))
        }
        fn call(&self, _procedure: &str, _args: &[RoutineValue]) -> crate::Result<RoutineValue> {
            Err(FunctionsError::Invalid("unexpected nested call".to_string()))
        }
        fn publish(&self, _topic: &str, _payload: &RoutineValue) -> crate::Result<()> {
            Ok(())
        }
        fn http_request_header(&self, _name: &str) -> crate::Result<Option<String>> {
            Ok(None)
        }
        fn http_set_status(&self, _status: i32) -> crate::Result<()> {
            Ok(())
        }
        fn http_set_header(&self, _name: &str, _value: &str) -> crate::Result<()> {
            Ok(())
        }
        fn is_http_root(&self) -> bool {
            false
        }
    }

    #[test]
    fn wrapped_body_can_call_host_sql() {
        let source = crate::wrap_procedure_source("return ctx.db.sql('SELECT 1');");
        let mut session =
            V8Session::load(ModuleRevision::typescript_fixture(source), RuntimeLimits::default())
                .unwrap();
        let host: Arc<dyn crate::FunctionHost> = Arc::new(SqlHost);
        let value = session
            .invoke_with_host(&echo_id(), &[], &CancellationToken::new(), Some(host))
            .unwrap();
        assert!(
            matches!(value.value, ScalarValue::Int32(Some(1)) | ScalarValue::Int64(Some(1))),
            "host sql SELECT 1 should round-trip as 1, got {:?}",
            value.value
        );
    }
}
