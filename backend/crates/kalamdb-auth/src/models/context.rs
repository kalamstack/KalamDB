// Authenticated user context for request handling

use kalamdb_commons::{
    models::{ConnectionInfo, UserId},
    Role,
};

/// Authenticated user context for a request.
///
/// Contains all information about the authenticated user needed for
/// authorization decisions and audit logging.
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    /// User's unique identifier
    pub user_id: UserId,
    /// User's role (User, Service, Dba, System)
    pub role: Role,
    /// Email address (if available)
    pub email: Option<String>,
    /// Account creation timestamp in milliseconds since epoch
    pub created_at: i64,
    /// Last update timestamp in milliseconds since epoch
    pub updated_at: i64,
    /// Connection information (IP address, localhost check)
    pub connection_info: ConnectionInfo,
}

impl AuthenticatedUser {
    pub fn new(
        user_id: UserId,
        role: Role,
        email: Option<String>,
        created_at: i64,
        updated_at: i64,
        connection_info: ConnectionInfo,
    ) -> Self {
        Self {
            user_id,
            role,
            email,
            created_at,
            updated_at,
            connection_info,
        }
    }

    pub fn is_admin(&self) -> bool {
        matches!(self.role, Role::Dba | Role::System)
    }

    pub fn is_system(&self) -> bool {
        matches!(self.role, Role::System)
    }

    pub fn is_localhost(&self) -> bool {
        self.connection_info.is_localhost()
    }

    pub fn can_access_user_resource(&self, resource_user_id: &UserId) -> bool {
        if self.is_admin() {
            return true;
        }
        &self.user_id == resource_user_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_user(role: Role, is_localhost: bool) -> AuthenticatedUser {
        let addr = if is_localhost {
            Some("127.0.0.1".to_string())
        } else {
            Some("192.168.1.100".to_string())
        };

        AuthenticatedUser::new(
            UserId::new("user_123"),
            role,
            Some("test@example.com".to_string()),
            0,
            0,
            ConnectionInfo::new(addr),
        )
    }

    #[test]
    fn test_is_admin() {
        assert!(create_test_user(Role::Dba, true).is_admin());
        assert!(create_test_user(Role::System, true).is_admin());
        assert!(!create_test_user(Role::User, true).is_admin());
        assert!(!create_test_user(Role::Service, true).is_admin());
    }

    #[test]
    fn test_is_system() {
        assert!(create_test_user(Role::System, true).is_system());
        assert!(!create_test_user(Role::Dba, true).is_system());
        assert!(!create_test_user(Role::User, true).is_system());
    }

    #[test]
    fn test_can_access_own_resource() {
        let user = create_test_user(Role::User, true);
        assert!(user.can_access_user_resource(&UserId::new("user_123")));
        assert!(!user.can_access_user_resource(&UserId::new("user_456")));
    }

    #[test]
    fn test_admin_can_access_all_resources() {
        let dba = create_test_user(Role::Dba, true);
        assert!(dba.can_access_user_resource(&UserId::new("user_123")));
        assert!(dba.can_access_user_resource(&UserId::new("user_456")));

        let system = create_test_user(Role::System, true);
        assert!(system.can_access_user_resource(&UserId::new("user_123")));
        assert!(system.can_access_user_resource(&UserId::new("user_456")));
    }
}
