//! App-facing KalamDB client crate.
//!
//! This is the preferred Rust client surface for applications. It wraps the
//! shared `link-common` implementation with lighter default features than the
//! full compatibility surface that previously lived at the root of `link/`.

pub use link_common::*;
