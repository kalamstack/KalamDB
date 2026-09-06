//! Function-boundary values over DataFusion scalars.

use datafusion_common::ScalarValue;
use kalamdb_commons::TypeId;

/// Thin wrapper around the shared Arrow/DataFusion value model.
///
/// JSON/JSONB values set [`RoutineValue::json_sql`] so the V8 ABI may use
/// `JSON.parse` / `JSON.stringify`. Other types convert field-by-field.
#[derive(Debug, Clone, PartialEq)]
pub struct RoutineValue {
    pub type_id:  Option<TypeId>,
    pub value:    ScalarValue,
    pub json_sql: bool,
}

impl RoutineValue {
    pub fn new(value: ScalarValue) -> Self {
        Self {
            type_id: None,
            value,
            json_sql: false,
        }
    }

    pub fn json(value: ScalarValue) -> Self {
        Self {
            type_id: None,
            value,
            json_sql: true,
        }
    }

    pub fn with_type_id(mut self, type_id: TypeId) -> Self {
        self.type_id = Some(type_id);
        self
    }
}
