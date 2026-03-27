use datafusion_common::ScalarValue;

/// Minimal scan predicate model used by the pg FDW planner.
///
/// The remote extension path currently only needs column-equals-literal filters,
/// primarily to strip `_userid` before request execution.
#[derive(Debug, Clone, PartialEq)]
pub enum ScanFilter {
    Eq {
        column: String,
        value: ScalarValue,
    },
}

impl ScanFilter {
    pub fn eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::Eq {
            column: column.into(),
            value,
        }
    }

    pub fn column_name(&self) -> &str {
        match self {
            Self::Eq { column, .. } => column.as_str(),
        }
    }

    pub fn value(&self) -> &ScalarValue {
        match self {
            Self::Eq { value, .. } => value,
        }
    }
}