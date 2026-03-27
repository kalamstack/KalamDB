use kalam_pg_api::DeleteRequest;

/// Planned FDW delete request ready for backend execution.
#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub request: DeleteRequest,
}
