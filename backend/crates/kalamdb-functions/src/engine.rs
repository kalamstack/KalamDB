//! Shared admission, revision caching, and thread-affine runtime workers.

use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};

use kalamdb_commons::{FunctionRevisionId, FunctionRuntime};
use moka::future::Cache;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::{
    EngineConfig, FunctionHost, FunctionsError, Invocation, ModuleRevision, Result, RoutineValue,
    RuntimeLimits, V8Session,
};

struct Work {
    invocation: Invocation,
    host:       Arc<dyn FunctionHost>,
    reply:      oneshot::Sender<Result<RoutineValue>>,
    _active:    OwnedSemaphorePermit,
    _root:      Option<OwnedSemaphorePermit>,
    #[cfg(feature = "wasm-runtime")]
    wasm:       Arc<crate::wasm_adapter::WasmAdapter>,
}

pub struct FunctionEngine {
    config:      EngineConfig,
    #[cfg(feature = "wasm-runtime")]
    wasm:        Arc<crate::wasm_adapter::WasmAdapter>,
    workers:     Vec<mpsc::Sender<Work>>,
    next_worker: AtomicUsize,
    active:      Arc<Semaphore>,
    roots:       Arc<Semaphore>,
    queued:      Arc<Semaphore>,
    revisions:   Cache<FunctionRevisionId, Arc<ModuleRevision>>,
}

impl FunctionEngine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        config.validate()?;
        #[cfg(feature = "wasm-runtime")]
        let wasm = Arc::new(crate::wasm_adapter::WasmAdapter::new(&config)?);
        let mut workers = Vec::with_capacity(config.workers);
        for index in 0..config.workers {
            let (sender, mut receiver) = mpsc::channel::<Work>(config.max_active);
            let worker_config = config.clone();
            thread::Builder::new()
                .name(format!("functions-{index}"))
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("function worker runtime");
                    let local = tokio::task::LocalSet::new();
                    local.block_on(&runtime, async move {
                        let idle = Rc::new(RefCell::new(Vec::<V8Session>::new()));
                        while let Some(work) = receiver.recv().await {
                            let idle = Rc::clone(&idle);
                            let config = worker_config.clone();
                            tokio::task::spawn_local(async move {
                                let result = run_v8(&work, &config, &idle).await;
                                let result = result.and_then(|value| {
                                    if value.value.size() > config.max_value_bytes {
                                        Err(FunctionsError::ResourceLimit(
                                            "procedure result bytes".into(),
                                        ))
                                    } else {
                                        Ok(value)
                                    }
                                });
                                let _ = work.reply.send(result);
                            });
                        }
                    });
                    runtime.block_on(local);
                })
                .map_err(|error| {
                    FunctionsError::Invalid(format!("start function worker: {error}"))
                })?;
            workers.push(sender);
        }
        Ok(Self {
            roots: Arc::new(Semaphore::new(config.max_active - config.nested_reserve)),
            active: Arc::new(Semaphore::new(config.max_active)),
            queued: Arc::new(Semaphore::new(config.max_queued)),
            revisions: Cache::builder()
                .max_capacity(config.cache_bytes)
                .weigher(|_: &FunctionRevisionId, revision: &Arc<ModuleRevision>| {
                    revision.byte_len().min(u32::MAX as usize) as u32
                })
                .build(),
            config,
            #[cfg(feature = "wasm-runtime")]
            wasm,
            workers,
            next_worker: AtomicUsize::new(0),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub async fn load_revision<F>(
        &self,
        id: FunctionRevisionId,
        loader: F,
    ) -> Result<Arc<ModuleRevision>>
    where
        F: std::future::Future<Output = Result<ModuleRevision>>,
    {
        self.revisions
            .try_get_with(id, async {
                let revision = loader.await?;
                if revision.byte_len() > self.config.max_artifact_bytes {
                    return Err(FunctionsError::ResourceLimit("artifact bytes".into()));
                }
                Ok(Arc::new(revision))
            })
            .await
            .map_err(|error: Arc<FunctionsError>| FunctionsError::Invalid(error.to_string()))
    }

    pub async fn invoke(
        &self,
        invocation: Invocation,
        host: Arc<dyn FunctionHost>,
    ) -> Result<RoutineValue> {
        invocation.scope.check()?;
        if invocation.scope.depth > self.config.max_depth {
            return Err(FunctionsError::ResourceLimit("procedure call depth".into()));
        }
        if invocation.revision.byte_len() > self.config.max_artifact_bytes
            || invocation.args.iter().map(|v| v.value.size()).sum::<usize>()
                > self.config.max_value_bytes
        {
            return Err(FunctionsError::ResourceLimit("procedure input bytes".into()));
        }
        let root = if invocation.scope.depth == 0 {
            match Arc::clone(&self.roots).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    let _queued = Arc::clone(&self.queued)
                        .try_acquire_owned()
                        .map_err(|_| FunctionsError::Capacity)?;
                    Some(tokio::select! {
                        biased;
                        _ = invocation.scope.cancel.cancelled() => return Err(FunctionsError::Cancelled),
                        _ = tokio::time::sleep_until(invocation.scope.deadline.into()) => return Err(FunctionsError::Timeout),
                        permit = Arc::clone(&self.roots).acquire_owned() => permit.map_err(|_| FunctionsError::Capacity)?,
                    })
                },
            }
        } else {
            None
        };
        // Nested acquisition never waits behind a parent which owns the last slot.
        let active = Arc::clone(&self.active)
            .try_acquire_owned()
            .map_err(|_| FunctionsError::Capacity)?;
        let (reply, response) = oneshot::channel();
        let cancel = invocation.scope.cancel.clone();
        let cancellation = cancel.clone().drop_guard();
        let index = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[index]
            .try_send(Work {
                invocation,
                host,
                reply,
                _active: active,
                _root: root,
                #[cfg(feature = "wasm-runtime")]
                wasm: Arc::clone(&self.wasm),
            })
            .map_err(|_| FunctionsError::Capacity)?;
        // The worker acknowledges cleanup before the caller can roll back.
        let result = response
            .await
            .map_err(|_| FunctionsError::Invalid("function worker stopped".into()))?;
        cancellation.disarm();
        result
    }
}

async fn run_v8(
    work: &Work,
    config: &EngineConfig,
    idle: &Rc<RefCell<Vec<V8Session>>>,
) -> Result<RoutineValue> {
    work.invocation.scope.check()?;
    if work.invocation.revision.runtime != FunctionRuntime::Typescript {
        #[cfg(feature = "wasm-runtime")]
        return work.wasm.invoke(&work.invocation, Arc::clone(&work.host)).await;
        #[cfg(not(feature = "wasm-runtime"))]
        return Err(FunctionsError::Invalid("server was built without wasm-runtime".into()));
    }
    let limits = RuntimeLimits {
        timeout:        work
            .invocation
            .scope
            .deadline
            .saturating_duration_since(std::time::Instant::now()),
        max_heap_bytes: config.max_heap_bytes,
        abi_version:    work.invocation.revision.abi_version,
    };
    if work.invocation.revision.abi_version == 1 {
        let revision = (*work.invocation.revision).clone();
        let name = work.invocation.routine_id.clone();
        let args = work.invocation.args.clone();
        let cancel = work.invocation.scope.cancel.clone();
        let host = Arc::clone(&work.host);
        return tokio::task::spawn_blocking(move || {
            let mut session = V8Session::load(revision, limits)?;
            session.invoke_with_host(&name, &args, &cancel, Some(host))
        })
        .await
        .map_err(|error| FunctionsError::Invalid(error.to_string()))?;
    }
    let cached = idle.borrow_mut().pop();
    let mut session = match cached {
        Some(mut session) => {
            session.rebind((*work.invocation.revision).clone());
            session.limits = limits;
            session
        },
        None => V8Session::load((*work.invocation.revision).clone(), limits)?,
    };
    let result = session.invoke_async(&work.invocation, Arc::clone(&work.host)).await;
    if result.is_ok() && idle.borrow().is_empty() {
        session.detach();
        idle.borrow_mut().push(session);
    }
    result
}
