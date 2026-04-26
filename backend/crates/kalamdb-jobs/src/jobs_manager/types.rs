use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Weak,
};

use kalamdb_commons::{JobId, NodeId};
use kalamdb_core::app_context::AppContext;
use kalamdb_system::{JobNodesTableProvider, JobsTableProvider};
use tokio::sync::mpsc;

use crate::executors::JobRegistry;

/// Unified Job Manager
///
/// Provides centralized job creation, execution, tracking, and lifecycle management.
///
/// ## Job Awakening
///
/// When a `CreateJobNode` command is applied for this node, the applier calls
/// `awake_job()` to immediately queue the job for execution. This provides
/// instant job dispatch rather than relying on polling intervals.
pub struct JobsManager {
    /// System table provider for job persistence
    pub(crate) jobs_provider: Arc<JobsTableProvider>,
    /// System table provider for per-node job state
    pub(crate) job_nodes_provider: Arc<JobNodesTableProvider>,

    /// Registry of job executors (trait-based dispatch)
    pub(crate) job_registry: Arc<JobRegistry>,

    /// Node ID for this instance
    pub(crate) node_id: NodeId,

    /// Flag for graceful shutdown (AtomicBool for lock-free access in hot loop)
    pub(crate) shutdown: AtomicBool,
    /// AppContext for global services - uses Weak to avoid Arc cycle
    /// (AppContext holds Arc<JobsManager>, so we use Weak here)
    pub(crate) app_context: Weak<AppContext>,

    /// Channel sender for awakening jobs immediately when CreateJobNode is applied.
    /// The state machine applier sends job_ids here to wake up the run_loop.
    pub(crate) awake_sender: mpsc::UnboundedSender<JobId>,
    /// Channel receiver for job awakening (consumed by run_loop)
    pub(crate) awake_receiver: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<JobId>>>,
}

impl JobsManager {
    /// Create a new JobsManager
    ///
    /// # Arguments
    /// * `jobs_provider` - System table provider for job persistence
    /// * `job_registry` - Registry of job executors
    /// * `app_ctx` - AppContext for accessing shared services
    pub fn new(
        jobs_provider: Arc<JobsTableProvider>,
        job_nodes_provider: Arc<JobNodesTableProvider>,
        job_registry: Arc<JobRegistry>,
        app_ctx: Arc<AppContext>,
    ) -> Self {
        let node_id = *app_ctx.node_id().as_ref();
        let (awake_sender, awake_receiver) = mpsc::unbounded_channel();
        Self {
            jobs_provider,
            job_nodes_provider,
            job_registry,
            node_id,
            shutdown: AtomicBool::new(false),
            app_context: Arc::downgrade(&app_ctx),
            awake_sender,
            awake_receiver: parking_lot::Mutex::new(Some(awake_receiver)),
        }
    }

    /// Get attached AppContext (panics if AppContext was dropped)
    pub(crate) fn get_attached_app_context(&self) -> Arc<AppContext> {
        self.app_context
            .upgrade()
            .expect("AppContext was dropped - JobsManager outlived AppContext")
    }

    /// Awake a job for execution on this node.
    ///
    /// Called by the state machine applier when `CreateJobNode` is applied for this node.
    /// This immediately queues the job for execution rather than waiting for polling.
    pub fn awake_job(&self, job_id: JobId) {
        if let Err(e) = self.awake_sender.send(job_id.clone()) {
            log::warn!("[{}] Failed to awake job: {}", job_id.as_str(), e);
        } else {
            log::debug!("[{}] Job awakened for execution", job_id.as_str());
        }
    }

    /// Request graceful shutdown
    pub fn shutdown(&self) {
        log::debug!("Initiating job manager shutdown");
        self.shutdown.store(true, Ordering::Release);
    }
}
