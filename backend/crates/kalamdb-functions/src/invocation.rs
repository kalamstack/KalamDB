//! Invocation lifetime shared across nested calls and runtime adapters.

use std::{sync::Arc, time::Instant};

use kalamdb_commons::RoutineId;
use tokio_util::sync::CancellationToken;

use crate::{FunctionsError, ModuleRevision, Result, RoutineValue};

#[derive(Clone)]
pub struct InvocationScope {
    pub deadline: Instant,
    pub cancel:   CancellationToken,
    pub depth:    usize,
}

impl InvocationScope {
    pub fn child(&self, max_depth: usize) -> Result<Self> {
        self.check()?;
        if self.depth >= max_depth {
            return Err(FunctionsError::ResourceLimit("procedure call depth".into()));
        }
        Ok(Self {
            deadline: self.deadline,
            cancel:   self.cancel.clone(),
            depth:    self.depth + 1,
        })
    }

    pub fn check(&self) -> Result<()> {
        if self.cancel.is_cancelled() {
            Err(FunctionsError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(FunctionsError::Timeout)
        } else {
            Ok(())
        }
    }
}

pub struct Invocation {
    pub routine_id:      RoutineId,
    pub revision:        Arc<ModuleRevision>,
    pub args:            Vec<RoutineValue>,
    pub scope:           InvocationScope,
    pub return_template: Option<RoutineValue>,
}
