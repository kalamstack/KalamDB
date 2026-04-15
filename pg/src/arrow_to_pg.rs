//! Arrow record batch value → PostgreSQL Datum conversion.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use pgrx::pg_sys;
use pgrx::IntoDatum;
use std::ffi::CString;

/// Days between Unix epoch (1970-01-01) and PostgreSQL epoch (2000-01-01).
const UNIX_TO_PG_EPOCH_DAYS: i32 = 10_957;
/// Microseconds between Unix epoch and PostgreSQL epoch.
const UNIX_TO_PG_EPOCH_MICROSECONDS: i64 = 946_684_800_000_000;

/// Convert an Arrow array value at the given row to a PostgreSQL Datum.
///
/// Returns `(datum, is_null)`.
///
/// # Safety
/// Must be called within a valid PostgreSQL memory context (e.g., per-tuple context).
pub unsafe fn arrow_value_to_datum(
    array: &dyn Array,
    row: usize,
    target_type_oid: pg_sys::Oid,
) -> (pg_sys::Datum, bool) {
    if array.is_null(row) {
        return (pg_sys::Datum::from(0usize), true);
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            (arr.value(row).into_datum().unwrap(), false)
        },
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            (arr.value(row).into_datum().unwrap(), false)
        },
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            (arr.value(row).into_datum().unwrap(), false)
        },
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            (arr.value(row).into_datum().unwrap(), false)
        },
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            (arr.value(row).into_datum().unwrap(), false)
        },
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            (arr.value(row).into_datum().unwrap(), false)
        },
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let val = arr.value(row);
            match datum_from_str_for_target_type(val, target_type_oid) {
                Some(datum) => (datum, false),
                None => (pg_sys::Datum::from(0usize), true),
            }
        },
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let val = arr.value(row);
            match datum_from_str_for_target_type(val, target_type_oid) {
                Some(datum) => (datum, false),
                None => (pg_sys::Datum::from(0usize), true),
            }
        },
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let val = arr.value(row);
            let bytea = pg_bytea_from_slice(val);
            (pg_sys::Datum::from(bytea as usize), false)
        },
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days_since_unix = arr.value(row);
            let pg_date = days_since_unix - UNIX_TO_PG_EPOCH_DAYS;
            (pg_sys::Datum::from(pg_date as usize), false)
        },
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let us_since_unix = arr.value(row);
            let pg_timestamp = us_since_unix - UNIX_TO_PG_EPOCH_MICROSECONDS;
            (pg_sys::Datum::from(pg_timestamp as usize), false)
        },
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            let ms_since_unix = arr.value(row);
            let us_since_unix = ms_since_unix * 1_000;
            let pg_timestamp = us_since_unix - UNIX_TO_PG_EPOCH_MICROSECONDS;
            (pg_sys::Datum::from(pg_timestamp as usize), false)
        },
        _ => {
            // Fallback: render as text via Arrow's display formatting
            let scalar = ScalarValue::try_from_array(array, row);
            match scalar {
                Ok(s) => {
                    let text = s.to_string();
                    match datum_from_str_for_target_type(&text, target_type_oid) {
                        Some(datum) => (datum, false),
                        None => (pg_sys::Datum::from(0usize), true),
                    }
                },
                Err(_) => (pg_sys::Datum::from(0usize), true),
            }
        },
    }
}

unsafe fn datum_from_str_for_target_type(
    value: &str,
    target_type_oid: pg_sys::Oid,
) -> Option<pg_sys::Datum> {
    match target_type_oid {
        pg_sys::JSONOID | pg_sys::JSONBOID => parse_text_via_type_input(value, target_type_oid),
        _ => text_datum_from_str(value),
    }
}

unsafe fn text_datum_from_str(value: &str) -> Option<pg_sys::Datum> {
    let cstr = CString::new(value).ok()?;
    let pg_text = pg_sys::cstring_to_text(cstr.as_ptr());
    Some(pg_sys::Datum::from(pg_text as usize))
}

unsafe fn parse_text_via_type_input(
    value: &str,
    target_type_oid: pg_sys::Oid,
) -> Option<pg_sys::Datum> {
    let cstr = CString::new(value).ok()?;
    let mut typinput = pg_sys::Oid::INVALID;
    let mut typioparam = pg_sys::Oid::INVALID;
    pg_sys::getTypeInputInfo(target_type_oid, &mut typinput, &mut typioparam);

    Some(pg_sys::OidInputFunctionCall(
        typinput,
        cstr.as_ptr() as *mut std::ffi::c_char,
        typioparam,
        -1,
    ))
}

/// Allocate a PostgreSQL `bytea` from a byte slice.
unsafe fn pg_bytea_from_slice(data: &[u8]) -> *mut pg_sys::varlena {
    let total_size = data.len() + pg_sys::VARHDRSZ;
    let result = pg_sys::palloc(total_size) as *mut pg_sys::varlena;
    set_varsize(result, total_size);
    let dest = (result as *mut u8).add(pg_sys::VARHDRSZ);
    std::ptr::copy_nonoverlapping(data.as_ptr(), dest, data.len());
    result
}

/// Manual SET_VARSIZE implementation (the C macro is not exposed by pgrx).
unsafe fn set_varsize(ptr: *mut pg_sys::varlena, len: usize) {
    let header_ptr = ptr as *mut u32;
    *header_ptr = (len as u32) << 2;
}
