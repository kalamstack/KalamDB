//! Generic object encode/decode using the central envelope.

use serde::{de::DeserializeOwned, Serialize};

use super::{
    decode_envelope, decode_flexbuffers, encode_envelope, encode_flexbuffers, EncodedObject,
    ObjectKind,
};
use crate::error::Result;

/// Default schema version for generic objects without a caller-supplied version.
pub const GENERIC_SCHEMA_VERSION: u16 = 1;

/// Encode a serde value as a generic persisted object.
pub fn encode_object<T: Serialize + ?Sized>(value: &T) -> Result<EncodedObject> {
    encode_object_versioned(value, GENERIC_SCHEMA_VERSION)
}

/// Encode a serde value with an explicit logical schema version.
pub fn encode_object_versioned<T: Serialize + ?Sized>(
    value: &T,
    schema_version: u16,
) -> Result<EncodedObject> {
    let payload = encode_flexbuffers(value)?;
    encode_envelope(ObjectKind::Generic, schema_version, &payload)
}

/// Decode a generic persisted object in the current envelope.
pub fn decode_object<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (_header, payload) = decode_envelope(bytes, ObjectKind::Generic)?;
    decode_flexbuffers(payload)
}

/// Encode a list of strings (non-unique secondary-index PK arrays).
pub fn encode_string_list(values: &[String]) -> Result<EncodedObject> {
    encode_object(values)
}

/// Decode a string list from the object envelope.
pub fn decode_string_list(bytes: &[u8]) -> Result<Vec<String>> {
    decode_object(bytes)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::object::encode_flexbuffers;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        id:   String,
        n:    i64,
        flag: bool,
    }

    fn sample() -> Sample {
        Sample {
            id:   "ns.types".to_string(),
            n:    42,
            flag: true,
        }
    }

    #[test]
    fn generic_object_roundtrip() {
        let value = sample();
        let encoded = encode_object(&value).unwrap();
        let decoded: Sample = decode_object(encoded.as_slice()).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn decode_object_rejects_unenveloped_flexbuffers() {
        let value = sample();
        let legacy = encode_flexbuffers(&value).unwrap();
        assert!(decode_object::<Sample>(&legacy).is_err());
        assert!(decode_string_list(&legacy).is_err());
    }

    #[test]
    fn string_list_roundtrip_rejects_json() {
        let values = vec!["a".to_string(), "b".to_string()];
        let encoded = encode_string_list(&values).unwrap();
        assert_eq!(decode_string_list(encoded.as_slice()).unwrap(), values);

        let json = serde_json::to_vec(&values).unwrap();
        assert!(decode_string_list(&json).is_err(), "JSON PK arrays are not a storage format");
    }
}
