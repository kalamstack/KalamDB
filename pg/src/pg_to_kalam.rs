//! PostgreSQL Datum → KalamDB ScalarValue conversion for INSERT/UPDATE.

use datafusion_common::ScalarValue;
use pgrx::pg_sys;
use std::ffi::CStr;

/// Convert a PostgreSQL datum to a DataFusion ScalarValue based on the column's type OID.
///
/// # Safety
/// The `datum` must be a valid PostgreSQL Datum for the given `type_oid`.
pub unsafe fn datum_to_scalar(
    datum: pg_sys::Datum,
    type_oid: pg_sys::Oid,
    is_null: bool,
) -> ScalarValue {
    if is_null {
        return match type_oid {
            pg_sys::BOOLOID => ScalarValue::Boolean(None),
            pg_sys::INT2OID => ScalarValue::Int16(None),
            pg_sys::INT4OID => ScalarValue::Int32(None),
            pg_sys::INT8OID => ScalarValue::Int64(None),
            pg_sys::FLOAT4OID => ScalarValue::Float32(None),
            pg_sys::FLOAT8OID => ScalarValue::Float64(None),
            _ => ScalarValue::Utf8(None),
        };
    }

    match type_oid {
        pg_sys::BOOLOID => ScalarValue::Boolean(Some(datum.value() != 0)),
        pg_sys::INT2OID => ScalarValue::Int16(Some(datum.value() as i16)),
        pg_sys::INT4OID => ScalarValue::Int32(Some(datum.value() as i32)),
        pg_sys::INT8OID => ScalarValue::Int64(Some(datum.value() as i64)),
        pg_sys::FLOAT4OID => {
            let bits = datum.value() as u32;
            ScalarValue::Float32(Some(f32::from_bits(bits)))
        },
        pg_sys::FLOAT8OID => {
            let bits = datum.value() as u64;
            ScalarValue::Float64(Some(f64::from_bits(bits)))
        },
        pg_sys::TEXTOID | pg_sys::VARCHAROID => {
            let text_ptr = datum.cast_mut_ptr::<pg_sys::varlena>();
            let cstr = pg_sys::text_to_cstring(text_ptr);
            let s = CStr::from_ptr(cstr).to_string_lossy().into_owned();
            pg_sys::pfree(cstr as *mut std::ffi::c_void);
            ScalarValue::Utf8(Some(s))
        },
        pg_sys::JSONBOID | pg_sys::JSONOID => {
            let text_ptr = datum.cast_mut_ptr::<pg_sys::varlena>();
            let cstr = pg_sys::text_to_cstring(text_ptr);
            let s = CStr::from_ptr(cstr).to_string_lossy().into_owned();
            pg_sys::pfree(cstr as *mut std::ffi::c_void);
            ScalarValue::Utf8(Some(s))
        },
        _ => {
            // Fallback: attempt text conversion via Postgres output function
            ScalarValue::Utf8(Some(datum_to_text_via_output(datum, type_oid)))
        },
    }
}

/// Convert any datum to text using PostgreSQL's type output function.
unsafe fn datum_to_text_via_output(datum: pg_sys::Datum, type_oid: pg_sys::Oid) -> String {
    let mut typoutput: pg_sys::Oid = pg_sys::Oid::INVALID;
    let mut typisvarlena: bool = false;
    pg_sys::getTypeOutputInfo(type_oid, &mut typoutput, &mut typisvarlena);
    let output_cstr = pg_sys::OidOutputFunctionCall(typoutput, datum);
    let s = CStr::from_ptr(output_cstr).to_string_lossy().into_owned();
    pg_sys::pfree(output_cstr as *mut std::ffi::c_void);
    s
}
