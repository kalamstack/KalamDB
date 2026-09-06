//! One process-wide deadline supervisor; registrations never outlive an invocation.

use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, OnceLock},
    thread,
    time::{Duration, Instant},
};

use tokio_util::sync::CancellationToken;

use crate::{FunctionsError, Result};

#[derive(Default)]
struct State {
    next_id: u64,
    entries: HashMap<u64, Entry>,
}

struct Entry {
    handle:   v8::IsolateHandle,
    deadline: Instant,
    cancel:   CancellationToken,
}

struct Supervisor {
    state:   Mutex<State>,
    changed: Condvar,
}

fn supervisor() -> &'static Arc<Supervisor> {
    static SUPERVISOR: OnceLock<Arc<Supervisor>> = OnceLock::new();
    SUPERVISOR.get_or_init(|| {
        let supervisor = Arc::new(Supervisor {
            state:   Mutex::new(State::default()),
            changed: Condvar::new(),
        });
        let worker = Arc::clone(&supervisor);
        thread::Builder::new()
            .name("function-deadlines".into())
            .spawn(move || {
                let mut state = worker.state.lock().expect("deadline state");
                loop {
                    if state.entries.is_empty() {
                        state = worker.changed.wait(state).expect("deadline wakeup");
                        continue;
                    }
                    let now = Instant::now();
                    for entry in state.entries.values() {
                        if now >= entry.deadline || entry.cancel.is_cancelled() {
                            // Hold the lock through termination. Removing a registration
                            // therefore acknowledges that no late interrupt can reach reuse.
                            entry.handle.terminate_execution();
                        }
                    }
                    state = worker
                        .changed
                        .wait_timeout(state, Duration::from_millis(1))
                        .expect("deadline wakeup")
                        .0;
                }
            })
            .expect("start function deadline supervisor");
        supervisor
    })
}

pub(crate) struct DeadlineGuard {
    id:       u64,
    deadline: Instant,
    cancel:   CancellationToken,
}

impl DeadlineGuard {
    pub(crate) fn new(
        handle: v8::IsolateHandle,
        deadline: Instant,
        cancel: CancellationToken,
    ) -> Self {
        let supervisor = supervisor();
        let mut state = supervisor.state.lock().expect("deadline state");
        state.next_id = state.next_id.checked_add(1).expect("deadline registration exhausted");
        let id = state.next_id;
        state.entries.insert(
            id,
            Entry {
                handle,
                deadline,
                cancel: cancel.clone(),
            },
        );
        supervisor.changed.notify_one();
        Self {
            id,
            deadline,
            cancel,
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.cancel.is_cancelled() {
            Err(FunctionsError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(FunctionsError::Timeout)
        } else {
            Ok(())
        }
    }
}

impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        supervisor().state.lock().expect("deadline state").entries.remove(&self.id);
    }
}
