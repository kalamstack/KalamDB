//! CURRENT_ROLE() function implementation
//!
//! This module provides a user-defined function for DataFusion that returns the current user's role
//! from the session context.

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use kalamdb_commons::arrow_utils::{arrow_utf8, ArrowDataType};
use kalamdb_commons::Role;
use kalamdb_session_datafusion::SessionUserContext;
use std::any::Any;
use std::sync::Arc;

/// CURRENT_ROLE() scalar function implementation
///
/// Returns the role of the current session user.
/// This function takes no arguments and returns a String (Utf8).
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct CurrentRoleFunction;

impl CurrentRoleFunction {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarUDFImpl for CurrentRoleFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kdb_current_role"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::exact(vec![], Volatility::Stable))
    }

    fn return_type(&self, _args: &[ArrowDataType]) -> DataFusionResult<ArrowDataType> {
        Ok(arrow_utf8())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if !args.args.is_empty() {
            return Err(DataFusionError::Plan("CURRENT_ROLE() takes no arguments".to_string()));
        }

        let session_ctx = args
            .config_options
            .extensions
            .get::<SessionUserContext>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "CURRENT_ROLE() failed: session user context not found".to_string(),
                )
            })?;

        let role_str = match session_ctx.role {
            Role::User => "user",
            Role::Service => "service",
            Role::Dba => "dba",
            Role::System => "system",
            Role::Anonymous => "anonymous",
        };

        let array = StringArray::from(vec![role_str]);
        Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::ScalarUDF;

    #[test]
    fn test_current_role_function_creation() {
        let func_impl = CurrentRoleFunction::new();
        let func = ScalarUDF::new_from_impl(func_impl);
        assert_eq!(func.name(), "kdb_current_role");
    }

    #[test]
    fn test_current_role_return_type() {
        let func_impl = CurrentRoleFunction::new();
        let return_type = func_impl.return_type(&[]);
        assert!(return_type.is_ok());
        assert_eq!(return_type.unwrap(), ArrowDataType::Utf8);
    }
}
