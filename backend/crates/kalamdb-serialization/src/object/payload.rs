//! Private FlexBuffers payload codec. Callers never choose this by name.

use serde::{de::DeserializeOwned, Serialize};

use crate::error::{Result, SerializationError};

pub(crate) fn encode_flexbuffers<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    flexbuffers::to_vec(value)
        .map_err(|err| SerializationError::Encode(format!("flexbuffers encode failed: {err}")))
}

pub(crate) fn decode_flexbuffers<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    flexbuffers::from_slice(bytes)
        .map_err(|err| SerializationError::Decode(format!("flexbuffers decode failed: {err}")))
}
