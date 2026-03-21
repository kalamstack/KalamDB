//! Parse FDW options from PostgreSQL `List` of `DefElem` nodes.

use pgrx::pg_sys;
use std::collections::BTreeMap;
use std::ffi::CStr;

/// Parse FDW options from a PostgreSQL `List*` of `DefElem*` nodes into a Rust map.
///
/// # Safety
/// `options` must be a valid PostgreSQL `List*` of `DefElem*` or null.
pub unsafe fn parse_options(options: *mut pg_sys::List) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    if options.is_null() {
        return result;
    }
    let len = (*options).length as usize;
    for i in 0..len {
        let cell = (*options).elements.add(i);
        let def_elem = (*cell).ptr_value as *mut pg_sys::DefElem;
        if def_elem.is_null() || (*def_elem).defname.is_null() {
            continue;
        }
        let name = CStr::from_ptr((*def_elem).defname).to_string_lossy().into_owned();
        let val_ptr = pg_sys::defGetString(def_elem);
        if !val_ptr.is_null() {
            let value = CStr::from_ptr(val_ptr).to_string_lossy().into_owned();
            result.insert(name, value);
        }
    }
    result
}
