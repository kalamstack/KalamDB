use kalam_pg_api::UpdateRequest;

/// Planned FDW update request ready for backend execution.
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub request: UpdateRequest,
}
