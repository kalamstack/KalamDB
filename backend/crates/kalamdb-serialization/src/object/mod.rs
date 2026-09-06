//! Generic catalog/system objects: KOBJ envelope plus FlexBuffers payload.

mod envelope;
mod generic;
mod payload;

pub(crate) use envelope::{decode_envelope, encode_envelope};
pub use envelope::{has_object_magic, EncodedObject, ObjectKind};
pub use generic::{
    decode_object, decode_string_list, encode_object, encode_object_versioned, encode_string_list,
};
pub(crate) use payload::{decode_flexbuffers, encode_flexbuffers};
