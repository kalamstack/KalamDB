// KalamDB API Library
//
// This crate provides the REST API layer for KalamDB,
// including HTTP handlers, routes, and request/response models.

pub mod compression;
#[cfg(feature = "embedded-ui")]
pub mod embedded_ui;
pub mod handlers;
pub mod limiter;
pub mod repositories;
pub mod routes;
