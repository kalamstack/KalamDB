//! Authentication request and response models
//!
//! This module contains type-safe models for all authentication endpoints.

mod error_response;
mod login_request;
mod login_response;
mod setup_request;
mod setup_response;
mod user_info;

pub use error_response::AuthErrorResponse;
pub use login_request::LoginRequest;
pub use login_response::LoginResponse;
pub use setup_request::ServerSetupRequest;
pub use setup_response::ServerSetupResponse;
pub use user_info::UserInfo;
