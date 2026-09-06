//! Recursive scalar/value tags for ordinal row payloads.
//!
//! There is no fallback/string tag. Unsupported values fail encode/decode.

pub(crate) const TAG_NULL: u8 = 0;
pub(crate) const TAG_BOOL: u8 = 1;
pub(crate) const TAG_I8: u8 = 2;
pub(crate) const TAG_I16: u8 = 3;
pub(crate) const TAG_I32: u8 = 4;
pub(crate) const TAG_I64: u8 = 5;
pub(crate) const TAG_U8: u8 = 6;
pub(crate) const TAG_U16: u8 = 7;
pub(crate) const TAG_U32: u8 = 8;
pub(crate) const TAG_U64: u8 = 9;
pub(crate) const TAG_F32: u8 = 10;
pub(crate) const TAG_F64: u8 = 11;
pub(crate) const TAG_UTF8: u8 = 12;
pub(crate) const TAG_BYTES: u8 = 13;
pub(crate) const TAG_DATE32: u8 = 14;
pub(crate) const TAG_TIME64_US: u8 = 15;
pub(crate) const TAG_TS_MS: u8 = 16;
pub(crate) const TAG_TS_US: u8 = 17;
pub(crate) const TAG_TS_NS: u8 = 18;
pub(crate) const TAG_DECIMAL128: u8 = 19;
pub(crate) const TAG_EMBEDDING: u8 = 20;
pub(crate) const TAG_STRUCT: u8 = 21;
pub(crate) const TAG_LIST: u8 = 22;
