//! CURRENT_USER_ID() function implementation
//!
//! This module provides a user-defined function for DataFusion that returns the current user ID
//! from the session context. This function is restricted to system/dba/service roles.

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use kalamdb_commons::arrow_utils::{arrow_utf8, ArrowDataType};
use kalamdb_commons::{Role, UserId};
use kalamdb_session_datafusion::SessionUserContext;
use std::any::Any;
use std::sync::Arc;

/// CURRENT_USER_ID() scalar function implementation
///
/// Returns the user ID of the current session user.
/// This function takes no arguments and returns a String (Utf8).
/// **Security**: This function can only be called by system/dba/service roles.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CurrentUserIdFunction {
    user_id: Option<UserId>,
    role: Option<Role>,
}

impl CurrentUserIdFunction {
    /// Create a new CURRENT_USER_ID function with no user bound
    pub fn new() -> Self {
        Self {
            user_id: None,
            role: None,
        }
    }

    /// Create a CURRENT_USER_ID function bound to a specific user id and role
    pub fn with_user(user_id: &UserId, role: Role) -> Self {
        Self {
            user_id: Some(user_id.clone()),
            role: Some(role),
        }
    }

    fn is_authorized(role: Role) -> bool {
        matches!(role, Role::System | Role::Dba | Role::Service)
    }
}

impl Default for CurrentUserIdFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for CurrentUserIdFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kdb_current_user_id"
    }

    fn signature(&self) -> &Signature {
        // Static signature with no arguments
        static SIGNATURE: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::exact(vec![], Volatility::Stable))
    }

    fn return_type(&self, _args: &[ArrowDataType]) -> DataFusionResult<ArrowDataType> {
        Ok(arrow_utf8())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if !args.args.is_empty() {
            return Err(DataFusionError::Plan("CURRENT_USER_ID() takes no arguments".to_string()));
        }

        let (resolved_user_id, resolved_role) = if let (Some(user_id), Some(role)) =
            (self.user_id.as_ref(), self.role)
        {
            (user_id.clone(), role)
        } else if let Some(session_ctx) = args.config_options.extensions.get::<SessionUserContext>()
        {
            (session_ctx.user_id.clone(), session_ctx.role)
        } else {
            return Err(DataFusionError::Execution(
                "CURRENT_USER_ID() failed: session user context not found".to_string(),
            ));
        };

        if !Self::is_authorized(resolved_role) {
            return Err(DataFusionError::Plan(
                "CURRENT_USER_ID() can only be called by system, dba, or service roles".to_string(),
            ));
        }

        let array = StringArray::from(vec![resolved_user_id.as_str()]);
        Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::ScalarUDF;

    #[test]
    fn test_current_user_id_function_creation() {
        let func_impl = CurrentUserIdFunction::new();
        let func = ScalarUDF::new_from_impl(func_impl);
        assert_eq!(func.name(), "kdb_current_user_id");
    }

    #[test]
    fn test_current_user_id_with_authorized_role() {
        let user_id = UserId::new("u_123");
        let func_impl = CurrentUserIdFunction::with_user(&user_id, Role::Dba);
        let func = ScalarUDF::new_from_impl(func_impl.clone());
        assert_eq!(func.name(), "kdb_current_user_id");

        // Verify configured user_id and role
        assert_eq!(func_impl.user_id, Some(user_id));
        assert_eq!(func_impl.role, Some(Role::Dba));
        assert!(CurrentUserIdFunction::is_authorized(Role::Dba));
    }

    #[test]
    fn test_current_user_id_authorization() {
        // Test authorized roles
        assert!(CurrentUserIdFunction::is_authorized(Role::System));
        assert!(CurrentUserIdFunction::is_authorized(Role::Dba));
        assert!(CurrentUserIdFunction::is_authorized(Role::Service));

        // Test unauthorized roles
        assert!(!CurrentUserIdFunction::is_authorized(Role::User));
        assert!(!CurrentUserIdFunction::is_authorized(Role::Anonymous));
    }

    #[test]
    fn test_current_user_id_return_type() {
        let func_impl = CurrentUserIdFunction::new();
        let return_type = func_impl.return_type(&[]);
        assert!(return_type.is_ok());
        assert_eq!(return_type.unwrap(), ArrowDataType::Utf8);
    }
}
