use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use kalamdb_commons::models::{ReadContext, Role, UserId};
use kalamdb_commons::UserName;
use kalamdb_session::UserContext;
use std::any::Any;

/// Session-level user context stored in DataFusion config extensions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionUserContext {
    pub user_id: UserId,
    pub username: Option<UserName>,
    pub role: Role,
    pub read_context: ReadContext,
}

impl Default for SessionUserContext {
    fn default() -> Self {
        Self::from(UserContext::default())
    }
}

impl SessionUserContext {
    #[inline]
    pub fn new(user_id: UserId, role: Role, read_context: ReadContext) -> Self {
        Self {
            user_id,
            username: None,
            role,
            read_context,
        }
    }

    #[inline]
    pub fn with_username(
        user_id: UserId,
        username: UserName,
        role: Role,
        read_context: ReadContext,
    ) -> Self {
        Self {
            user_id,
            username: Some(username),
            role,
            read_context,
        }
    }

    #[inline]
    pub fn into_user_context(self) -> UserContext {
        match self.username {
            Some(username) => {
                UserContext::with_username(self.user_id, username, self.role, self.read_context)
            },
            None => UserContext::new(self.user_id, self.role, self.read_context),
        }
    }
}

impl From<UserContext> for SessionUserContext {
    fn from(value: UserContext) -> Self {
        Self {
            user_id: value.user_id,
            username: value.username,
            role: value.role,
            read_context: value.read_context,
        }
    }
}

impl From<&UserContext> for SessionUserContext {
    fn from(value: &UserContext) -> Self {
        Self {
            user_id: value.user_id.clone(),
            username: value.username.clone(),
            role: value.role,
            read_context: value.read_context,
        }
    }
}

impl ExtensionOptions for SessionUserContext {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, _key: &str, _value: &str) -> datafusion::common::Result<()> {
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

impl ConfigExtension for SessionUserContext {
    const PREFIX: &'static str = "kalamdb";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_user_context() {
        let ctx = UserContext::with_username(
            UserId::new("alice"),
            UserName::new("alice"),
            Role::User,
            ReadContext::Client,
        );

        let extension = SessionUserContext::from(&ctx);
        assert_eq!(extension.user_id, ctx.user_id);
        assert_eq!(extension.username, ctx.username);
        assert_eq!(extension.role, ctx.role);
        assert_eq!(extension.read_context, ctx.read_context);

        assert_eq!(extension.into_user_context(), ctx);
    }
}
