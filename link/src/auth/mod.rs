//! Authentication provider and models for KalamDB client.
//!
//! Handles JWT tokens, HTTP Basic Auth, async dynamic auth providers,
//! and authentication-related data models.

pub mod models;

#[cfg(feature = "tokio-runtime")]
mod provider;
#[cfg(feature = "tokio-runtime")]
pub use provider::{ArcDynAuthProvider, AuthProvider, DynamicAuthProvider, ResolvedAuth};
