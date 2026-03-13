//! Authenticated Session Context
//!
//! This module provides `AuthSession` - a unified session object that combines
//! user identity, role, and optional session metadata (request_id, IP address, etc.).
//!
//! ## Purpose
//!
//! `AuthSession` serves as the bridge between HTTP handlers and the execution layer,
//! carrying all necessary authentication and audit information through the request lifecycle.
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Create from authenticated user
//! let session = AuthSession::new(user_id, role);
//!
//! // Add optional metadata
//! let session = session
//!     .with_namespace(namespace_id)
//!     .with_request_id(request_id)
//!     .with_ip(client_ip);
//!
//! // Use in ExecutionContext
//! let exec_ctx = ExecutionContext::from_session(session, base_session);
//! ```

use crate::UserContext;
use kalamdb_commons::models::{ConnectionInfo, ReadContext, Role, UserId};
use std::time::SystemTime;

/// Authentication method used for the session
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// HTTP Basic Authentication (username:password base64 encoded)
    Basic,
    /// JWT Bearer token
    Bearer,
    /// Direct username/password (for WebSocket)
    Direct,
}

/// Authenticated session with user identity and optional metadata
///
/// This struct consolidates user authentication information (user_id, role)
/// with optional session metadata (namespace, request_id, IP address) for
/// audit logging and execution context creation.
///
/// # Fields
/// - `user_context`: Core user identity (user_id, role, read_context)
/// - `request_id`: Request tracking ID (optional, for audit logging)
/// - `ip_address`: Client IP address (optional, for audit logging)
/// - `connection_info`: Connection information (IP address, localhost check)
/// - `auth_method`: Authentication method used (Bearer, Basic, Direct)
/// - `timestamp`: Session creation timestamp
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthSession {
    /// Core user identity and read context
    pub user_context: UserContext,
    /// Request tracking ID (optional)
    pub request_id: Option<String>,
    /// Client IP address (optional)
    pub ip_address: Option<String>,
    /// Connection information (IP address, localhost check)
    pub connection_info: ConnectionInfo,
    /// Authentication method used
    pub auth_method: AuthMethod,
    /// Session creation timestamp
    pub timestamp: SystemTime,
}

impl AuthSession {
    /// Create a new authenticated session for client requests
    ///
    /// Creates a session with ReadContext::Client (requires leader for reads).
    ///
    /// # Arguments
    /// * `user_id` - The authenticated user's ID
    /// * `role` - The user's role (User, Service, Dba, System)
    ///
    /// # Example
    /// ```rust,ignore
    /// let session = AuthSession::new(UserId::new("alice"), Role::User);
    /// ```
    pub fn new(user_id: UserId, role: Role) -> Self {
        Self {
            user_context: UserContext::client(user_id, role),
            request_id: None,
            ip_address: None,
            connection_info: ConnectionInfo::new(None),
            auth_method: AuthMethod::Bearer,
            timestamp: SystemTime::now(),
        }
    }

    /// Create a new authenticated session with connection info and auth method
    pub fn with_auth_details(
        user_id: UserId,
        role: Role,
        connection_info: ConnectionInfo,
        auth_method: AuthMethod,
    ) -> Self {
        Self {
            user_context: UserContext::client(user_id, role),
            request_id: None,
            ip_address: connection_info.remote_addr.clone(),
            connection_info,
            auth_method,
            timestamp: SystemTime::now(),
        }
    }

    /// Create a new authenticated session with username, connection info, and auth method
    pub fn with_username_and_auth_details(
        user_id: UserId,
        username: kalamdb_commons::UserName,
        role: Role,
        connection_info: ConnectionInfo,
        auth_method: AuthMethod,
    ) -> Self {
        Self {
            user_context: UserContext::client_with_username(user_id, username, role),
            request_id: None,
            ip_address: connection_info.remote_addr.clone(),
            connection_info,
            auth_method,
            timestamp: SystemTime::now(),
        }
    }

    /// Create a session with a specific read context
    ///
    /// Use ReadContext::Internal for background jobs and notifications
    /// that can read from followers.
    pub fn with_read_context(user_id: UserId, role: Role, read_context: ReadContext) -> Self {
        Self {
            user_context: UserContext::new(user_id, role, read_context),
            request_id: None,
            ip_address: None,
            connection_info: ConnectionInfo::new(None),
            auth_method: AuthMethod::Bearer,
            timestamp: SystemTime::now(),
        }
    }

    /// Create a session for internal operations (can read from followers)
    pub fn internal(user_id: UserId, role: Role) -> Self {
        Self::with_read_context(user_id, role, ReadContext::Internal)
    }

    /// Create an anonymous session (not authenticated)
    pub fn anonymous() -> Self {
        Self {
            user_context: UserContext::client(UserId::anonymous(), Role::Anonymous),
            request_id: None,
            ip_address: None,
            connection_info: ConnectionInfo::new(None),
            auth_method: AuthMethod::Bearer,
            timestamp: SystemTime::now(),
        }
    }

    /// Create a session with all metadata
    pub fn with_audit_info(
        user_id: UserId,
        role: Role,
        request_id: Option<String>,
        ip_address: Option<String>,
    ) -> Self {
        let connection_info = ConnectionInfo::new(ip_address.clone());
        Self {
            user_context: UserContext::client(user_id, role),
            request_id,
            ip_address,
            connection_info,
            auth_method: AuthMethod::Bearer,
            timestamp: SystemTime::now(),
        }
    }

    // Builder methods

    /// Set the request tracking ID
    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Set the client IP address
    pub fn with_ip(mut self, ip_address: String) -> Self {
        self.ip_address = Some(ip_address);
        self
    }

    /// Update the read context
    pub fn with_read_context_mode(mut self, read_context: ReadContext) -> Self {
        self.user_context.read_context = read_context;
        self
    }

    // Accessor methods

    #[inline]
    pub fn user_id(&self) -> &UserId {
        &self.user_context.user_id
    }

    #[inline]
    pub fn role(&self) -> Role {
        self.user_context.role
    }

    #[inline]
    pub fn read_context(&self) -> ReadContext {
        self.user_context.read_context
    }

    #[inline]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    #[inline]
    pub fn ip_address(&self) -> Option<&str> {
        self.ip_address.as_deref()
    }

    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    #[inline]
    pub fn user_context(&self) -> &UserContext {
        &self.user_context
    }

    #[inline]
    pub fn is_admin(&self) -> bool {
        self.user_context.is_admin()
    }

    #[inline]
    pub fn is_system(&self) -> bool {
        self.user_context.is_system()
    }

    #[inline]
    pub fn is_anonymous(&self) -> bool {
        self.user_context.user_id.is_anonymous()
    }

    #[inline]
    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }

    #[inline]
    pub fn auth_method(&self) -> AuthMethod {
        self.auth_method
    }

    #[inline]
    pub fn is_localhost(&self) -> bool {
        self.connection_info.is_localhost()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_session_new() {
        let session = AuthSession::new(UserId::new("alice"), Role::User);
        assert_eq!(session.user_id().as_str(), "alice");
        assert_eq!(session.role(), Role::User);
        assert_eq!(session.read_context(), ReadContext::Client);
        assert!(!session.is_admin());
    }

    #[test]
    fn test_auth_session_builder() {
        let session = AuthSession::new(UserId::new("bob"), Role::User)
            .with_request_id("req-123".to_string())
            .with_ip("127.0.0.1".to_string());

        assert_eq!(session.request_id(), Some("req-123"));
        assert_eq!(session.ip_address(), Some("127.0.0.1"));
    }

    #[test]
    fn test_auth_session_internal() {
        let session = AuthSession::internal(UserId::new("worker"), Role::Service);
        assert_eq!(session.read_context(), ReadContext::Internal);
    }

    #[test]
    fn test_auth_session_anonymous() {
        let session = AuthSession::anonymous();
        assert!(session.is_anonymous());
        assert_eq!(session.role(), Role::Anonymous);
    }

    #[test]
    fn test_auth_session_admin_checks() {
        let dba = AuthSession::new(UserId::new("admin"), Role::Dba);
        assert!(dba.is_admin());
        assert!(!dba.is_system());

        let system = AuthSession::new(UserId::system(), Role::System);
        assert!(system.is_admin());
        assert!(system.is_system());
    }
}
