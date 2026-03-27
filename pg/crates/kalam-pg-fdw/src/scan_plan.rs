use crate::virtual_column::VirtualColumn;
use kalam_pg_api::ScanRequest;

/// Planned FDW scan request plus FDW-only virtual-column metadata.
#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub request: ScanRequest,
    pub virtual_columns: Vec<VirtualColumn>,
}
