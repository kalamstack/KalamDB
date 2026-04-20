#[derive(Debug, Clone, Default)]
pub struct AllocatorMetrics {
    pub allocator_name: &'static str,
}

impl AllocatorMetrics {
    pub fn as_pairs(&self) -> Vec<(String, String)> {
        vec![("allocator_name".to_string(), self.allocator_name.to_string())]
    }
}

#[cfg(feature = "mimalloc")]
mod imp {
    use super::AllocatorMetrics;

    pub fn collect_allocator_metrics() -> Option<AllocatorMetrics> {
        Some(AllocatorMetrics {
            allocator_name: "mimalloc",
        })
    }

    pub fn force_allocator_collection(_force: bool) {}
}

#[cfg(not(feature = "mimalloc"))]
mod imp {
    use super::AllocatorMetrics;

    pub fn collect_allocator_metrics() -> Option<AllocatorMetrics> {
        None
    }

    pub fn force_allocator_collection(_force: bool) {}
}

pub use imp::{collect_allocator_metrics, force_allocator_collection};
