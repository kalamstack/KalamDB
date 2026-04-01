//! HTTP transport surface for KalamDB.
//!
//! This module groups REST endpoints and HTTP-only helpers by transport
//! instead of keeping them under a generic handlers bucket.

pub mod auth;
pub mod cluster;
pub mod files;
pub mod sql;
pub mod topics;

mod health;

pub(crate) use health::healthcheck_handler;
