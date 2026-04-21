//! Vector SQL providers and table functions.
//!
//! `vector_search` now uses the shared deferred execution substrate so index
//! lookup and candidate ranking execute at runtime rather than during planning.

pub mod cosine_distance;
pub mod vector_search;

pub use cosine_distance::CosineDistanceFunction;
pub use vector_search::{
    UnavailableVectorSearchRuntime, VectorSearchRuntime, VectorSearchScope,
    VectorSearchTableFunction,
};
