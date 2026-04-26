use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub type FieldFlags = BTreeSet<FieldFlag>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldFlag {
    #[serde(rename = "pk")]
    PrimaryKey,
    #[serde(rename = "nn")]
    NonNull,
    #[serde(rename = "uq")]
    Unique,
}
