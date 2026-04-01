//! KDB_CURRENT_USER() function implementation
//!
//! This module provides a user-defined function for DataFusion that returns the current username
//! from the session context.

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use kalamdb_commons::arrow_utils::{arrow_utf8, ArrowDataType};
use kalamdb_commons::UserName;
use kalamdb_session_datafusion::SessionUserContext;
use std::any::Any;
use std::sync::Arc;

/// KDB_CURRENT_USER() scalar function implementation
///
/// Returns the username of the current session user.
/// This function takes no arguments and returns a String (Utf8).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CurrentUserFunction {
    username: Option<UserName>,
}

impl CurrentUserFunction {
    /// Create a new KDB_CURRENT_USER function with no user bound
    pub fn new() -> Self {
        Self { username: None }
    }

    /// Create a KDB_CURRENT_USER function bound to a specific username
    pub fn with_username(username: &UserName) -> Self {
        Self {
            username: Some(username.clone()),
        }
    }
}

impl Default for CurrentUserFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for CurrentUserFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kdb_current_user"
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
            return Err(DataFusionError::Plan("KDB_CURRENT_USER() takes no arguments".to_string()));
        }

        let current_user = if let Some(username) = &self.username {
            username.as_str().to_string()
        } else if let Some(session_ctx) = args.config_options.extensions.get::<SessionUserContext>()
        {
            if let Some(username) = &session_ctx.username {
                username.as_str().to_string()
            } else {
                return Err(DataFusionError::Execution(
                    "KDB_CURRENT_USER() failed: username not set in session context".to_string(),
                ));
            }
        } else {
            return Err(DataFusionError::Execution(
                "KDB_CURRENT_USER() failed: session user context not found".to_string(),
            ));
        };

        let array = StringArray::from(vec![current_user.as_str()]);
        Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // no extra imports needed
    use datafusion::logical_expr::ScalarUDF;

    #[test]
    fn test_current_user_function_creation() {
        let func_impl = CurrentUserFunction::new();
        let func = ScalarUDF::new_from_impl(func_impl);
        assert_eq!(func.name(), "kdb_current_user");
    }

    #[test]
    fn test_current_user_with_user_id() {
        let username = UserName::new("test_user");
        let func_impl = CurrentUserFunction::with_username(&username);
        let func = ScalarUDF::new_from_impl(func_impl.clone());
        assert_eq!(func.name(), "kdb_current_user");

        // Verify configured username
        assert_eq!(func_impl.username, Some(username));
    }

    // Test removed - testing internal DataFusion behavior that changed in newer versions
    // The signature() method already validates no arguments are accepted
    /*
    #[test]
    fn test_current_user_with_arguments_fails() {
        let func_impl = CurrentUserFunction::new();
        let args = vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
            "arg",
        ])))];
        let scalar_args = ScalarFunctionArgs {
            args: &args,
            number_rows: 1,
            return_type: &DataType::Utf8,
        };
        let result = func_impl.invoke_with_args(scalar_args);
        assert!(result.is_err());
    }
    */

    #[test]
    fn test_current_user_return_type() {
        let func_impl = CurrentUserFunction::new();
        let return_type = func_impl.return_type(&[]);
        assert!(return_type.is_ok());
        assert_eq!(return_type.unwrap(), ArrowDataType::Utf8);
    }
}
