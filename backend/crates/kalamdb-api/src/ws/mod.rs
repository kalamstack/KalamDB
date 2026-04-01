//! WebSocket handlers for live query subscriptions
//!
//! ## Endpoints
//! - GET /v1/ws - Establish WebSocket connection for live queries
//!
//! ## Connection Lifecycle
//! 1. Client connects to /v1/ws (unauthenticated initially)
//! 2. Client sends Authenticate message within 3 seconds
//! 3. Server validates credentials and marks connection as authenticated
//! 4. Client can then subscribe to live queries
//! 5. Server pushes notifications when data changes
//!
//! ## Message Types
//! - Authenticate: JWT/token-based authentication
//! - Subscribe: Subscribe to a live query
//! - Unsubscribe: Unsubscribe from a live query
//! - NextBatch: Request next batch of results
//!
//! ## Security Features
//! - Origin header validation (configurable)
//! - Message size limits (configurable, default 64KB)
//! - Rate limiting per connection
//! - Authentication timeout (3 seconds)
//! - Heartbeat monitoring

pub mod events;
pub mod models;

mod compression;
mod context;
mod handler;
mod messages;
mod protocol;
mod runtime;

pub(crate) use handler::websocket_handler;
