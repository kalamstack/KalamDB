use kalam_pg_api::InsertRequest;

/// Planned FDW insert request ready for backend execution.
#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub request: InsertRequest,
}
