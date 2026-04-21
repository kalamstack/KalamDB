//! KDB_CURRENT_USER() function implementation
//!
//! Returns the current user_id from the session context.

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use kalamdb_commons::arrow_utils::{arrow_utf8, ArrowDataType};
use kalamdb_session_datafusion::SessionUserContext;
use std::any::Any;
use std::sync::Arc;

/// KDB_CURRENT_USER() scalar function implementation
///
/// Returns the user_id of the current session user.
/// `CURRENT_USER_ID()` is rewritten to this function at the dialect layer.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct CurrentUserFunction;

impl CurrentUserFunction {
    pub fn new() -> Self {
        Self
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

        let session_ctx = args
            .config_options
            .extensions
            .get::<SessionUserContext>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "KDB_CURRENT_USER() failed: session user context not found".to_string(),
                )
            })?;

        let array = StringArray::from(vec![session_ctx.user_id.as_str()]);
        Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
    }
}
