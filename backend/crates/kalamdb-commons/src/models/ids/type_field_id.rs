//! Composite identity for one field (or enum label) on a catalog type.

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::TypeId;
#[cfg(feature = "storage")]
use crate::StorageKey;

/// `{type_id}:{field_name}` identity for `system.type_fields`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TypeFieldId(String);

impl TypeFieldId {
    pub fn new(type_id: &TypeId, field_name: impl AsRef<str>) -> Result<Self, String> {
        let field_name = field_name.as_ref();
        if field_name.is_empty() || field_name.contains(':') {
            return Err("type field name must be non-empty and cannot contain ':'".to_string());
        }
        Ok(Self(format!("{}:{field_name}", type_id.as_str())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn type_id(&self) -> TypeId {
        let (type_id, _) = self.split();
        TypeId::new(type_id)
    }

    pub fn field_name(&self) -> &str {
        self.split().1
    }

    fn split(&self) -> (&str, &str) {
        self.0.rsplit_once(':').expect("TypeFieldId always contains ':'")
    }
}

impl fmt::Display for TypeFieldId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TypeFieldId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(feature = "storage")]
impl StorageKey for TypeFieldId {
    fn storage_key(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    fn from_storage_key(bytes: &[u8]) -> Result<Self, String> {
        let value = String::from_utf8(bytes.to_vec()).map_err(|error| error.to_string())?;
        let (type_id, field_name) = value
            .rsplit_once(':')
            .ok_or_else(|| "invalid type field storage key".to_string())?;
        Self::new(&TypeId::new(type_id), field_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_field_id_splits_schema_qualified_type() {
        let id = TypeFieldId::new(
            &TypeId::from_parts(Some(&crate::models::NamespaceId::new("chat")), "message"),
            "body",
        )
        .unwrap();
        assert_eq!(id.as_str(), "chat.message:body");
        assert_eq!(id.type_id().as_str(), "chat.message");
        assert_eq!(id.field_name(), "body");
    }
}
