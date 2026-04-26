use std::any::Any;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use kalamdb_commons::models::{ReadContext, Role, UserId};
use kalamdb_session::UserContext;

/// Session-level user context stored in DataFusion config extensions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionUserContext {
    pub user_id: UserId,
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
            role,
            read_context,
        }
    }

    #[inline]
    pub fn into_user_context(self) -> UserContext {
        UserContext::new(self.user_id, self.role, self.read_context)
    }
}

impl From<UserContext> for SessionUserContext {
    fn from(value: UserContext) -> Self {
        Self {
            user_id: value.user_id,
            role: value.role,
            read_context: value.read_context,
        }
    }
}

impl From<&UserContext> for SessionUserContext {
    fn from(value: &UserContext) -> Self {
        Self {
            user_id: value.user_id.clone(),
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
    const PREFIX: &'static str = "kalamdb_user";
}
