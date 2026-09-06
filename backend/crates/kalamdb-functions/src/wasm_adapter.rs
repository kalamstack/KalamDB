//! Wasmtime component adapter with fresh stores and a pooled allocator.

use std::{sync::Arc, thread, time::Duration};

use kalamdb_commons::FunctionRevisionId;
use moka::future::Cache;
use wasmtime::{
    component::{Component, Linker},
    Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store, StoreLimits,
    StoreLimitsBuilder,
};

use crate::{
    wasm_values::{decode_values, encode_values},
    EngineConfig, FunctionHost, FunctionsError, Invocation, Result, RoutineValue,
};

struct HostState {
    host:      Arc<dyn FunctionHost>,
    limits:    StoreLimits,
    max_bytes: usize,
}

pub(crate) struct WasmAdapter {
    engine:     Engine,
    components: Cache<FunctionRevisionId, Component>,
    config:     EngineConfig,
}

impl WasmAdapter {
    pub(crate) fn new(config: &EngineConfig) -> Result<Self> {
        let mut pool = PoolingAllocationConfig::default();
        pool.total_component_instances(config.max_active as u32)
            .total_core_instances(config.max_active as u32 * 4)
            .total_memories(config.max_active as u32 * 4)
            .max_memory_size(config.max_heap_bytes);
        let mut settings = Config::new();
        settings
            .async_support(true)
            .epoch_interruption(true)
            .allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
        let engine = Engine::new(&settings).map_err(invalid)?;
        let weak = engine.weak();
        thread::Builder::new()
            .name("wasm-epochs".into())
            .spawn(move || loop {
                thread::sleep(Duration::from_millis(1));
                let Some(engine) = weak.upgrade() else {
                    break;
                };
                engine.increment_epoch();
            })
            .map_err(invalid)?;
        Ok(Self {
            engine,
            config: config.clone(),
            components: Cache::builder().max_capacity(32).build(),
        })
    }

    pub(crate) async fn invoke(
        &self,
        invocation: &Invocation,
        host: Arc<dyn FunctionHost>,
    ) -> Result<RoutineValue> {
        if invocation.revision.abi_version != 2 {
            return Err(FunctionsError::Invalid("WASM requires ABI v2".into()));
        }
        let bytes = invocation.revision.wasm.clone();
        let engine = self.engine.clone();
        let component = self
            .components
            .try_get_with(invocation.revision.revision_id.clone(), async move {
                tokio::task::spawn_blocking(move || {
                    Component::new(&engine, &bytes).map_err(invalid)
                })
                .await
                .map_err(invalid)?
            })
            .await
            .map_err(|error: Arc<FunctionsError>| invalid(error))?;
        invocation.scope.check()?;
        let mut linker = Linker::<HostState>::new(&self.engine);
        register_host(&mut linker).map_err(invalid)?;
        let mut store = Store::new(
            &self.engine,
            HostState {
                host,
                max_bytes: self.config.max_value_bytes,
                limits: StoreLimitsBuilder::new()
                    .memory_size(self.config.max_heap_bytes)
                    .memories(1)
                    .instances(4)
                    .tables(4)
                    .build(),
            },
        );
        store.limiter(|state| &mut state.limits);
        store.set_epoch_deadline(1);
        store.epoch_deadline_async_yield_and_update(1);
        let args = encode_values(&invocation.args)?;
        let run = async {
            let instance =
                linker.instantiate_async(&mut store, &component).await.map_err(invalid)?;
            let function = instance
                .get_typed_func::<(String, Vec<u8>), (std::result::Result<Vec<u8>, String>,)>(
                    &mut store, "invoke",
                )
                .map_err(invalid)?;
            let (result,) = function
                .call_async(&mut store, (invocation.routine_id.to_string(), args))
                .await
                .map_err(invalid)?;
            function.post_return_async(&mut store).await.map_err(invalid)?;
            let result = result.map_err(FunctionsError::Javascript)?;
            let mut values = decode_values(&result, self.config.max_value_bytes)?;
            if values.len() != 1 {
                return Err(FunctionsError::Invalid(
                    "WASM invoke must return exactly one value".into(),
                ));
            }
            Ok(values.remove(0))
        };
        tokio::select! {
            biased;
            _ = invocation.scope.cancel.cancelled() => Err(FunctionsError::Cancelled),
            _ = tokio::time::sleep_until(invocation.scope.deadline.into()) => Err(FunctionsError::Timeout),
            result = run => result,
        }
    }
}

fn register_host(linker: &mut Linker<HostState>) -> wasmtime::Result<()> {
    let mut interface = linker.instance("kalam:procedure/host@2.0.0")?;
    for kind in ["query", "execute", "call"] {
        interface.func_wrap_async(kind, move |store, (name, bytes): (String, Vec<u8>)| {
            Box::new(async move {
                let state = store.data();
                let result = async {
                    let args = decode_values(&bytes, state.max_bytes)?;
                    let value = match kind {
                        "query" => state.host.query(name, args).await?,
                        "execute" => state.host.execute(name, args).await?,
                        _ => state.host.call_async(name, args).await?,
                    };
                    if value.value.size() > state.max_bytes {
                        return Err(FunctionsError::ResourceLimit("host result bytes".into()));
                    }
                    encode_values(&[value])
                }
                .await;
                Ok((result.map_err(|error: FunctionsError| error.to_string()),))
            })
        })?;
    }
    interface.func_wrap_async("publish", |store, (topic, bytes): (String, Vec<u8>)| {
        Box::new(async move {
            let result = async {
                let mut values = decode_values(&bytes, store.data().max_bytes)?;
                if values.len() != 1 {
                    return Err(FunctionsError::Invalid("publish requires one value".into()));
                }
                store.data().host.publish_async(topic, values.remove(0)).await
            }
            .await;
            Ok((result.map_err(|error| error.to_string()),))
        })
    })?;
    interface.func_wrap("metadata", |store, (): ()| {
        Ok(
            (serde_json::to_string(&store.data().host.metadata())
                .unwrap_or_else(|_| "null".into()),),
        )
    })?;
    interface.func_wrap("log", |store, (level, message): (String, String)| {
        let result = if message.len() > 64 * 1024 {
            Err(FunctionsError::ResourceLimit("log message".into()))
        } else {
            store.data().host.log(&level, &message)
        };
        Ok((result.map_err(|e| e.to_string()),))
    })?;
    interface.func_wrap("request-header", |store, (name,): (String,)| {
        Ok((store.data().host.http_request_header(&name).map_err(|e| e.to_string()),))
    })?;
    interface.func_wrap("response-status", |store, (status,): (u16,)| {
        Ok((store.data().host.http_set_status(status as i32).map_err(|e| e.to_string()),))
    })?;
    interface.func_wrap("response-header", |store, (name, value): (String, String)| {
        Ok((store.data().host.http_set_header(&name, &value).map_err(|e| e.to_string()),))
    })?;
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> FunctionsError {
    FunctionsError::Invalid(error.to_string())
}
