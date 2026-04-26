//! Parse FDW options from PostgreSQL `List` of `DefElem` nodes.

use std::{collections::BTreeMap, ffi::CStr};

use pgrx::pg_sys;

/// Parse FDW options from a PostgreSQL `List*` of `DefElem*` nodes into a Rust map.
pub fn parse_options(options: *mut pg_sys::List) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    if options.is_null() {
        return result;
    }
    let len = unsafe { (*options).length as usize };
    for i in 0..len {
        let def_elem = unsafe {
            let cell = (*options).elements.add(i);
            (*cell).ptr_value as *mut pg_sys::DefElem
        };
        if def_elem.is_null() || unsafe { (*def_elem).defname.is_null() } {
            continue;
        }
        let name = unsafe { CStr::from_ptr((*def_elem).defname) }.to_string_lossy().into_owned();
        let val_ptr = unsafe { pg_sys::defGetString(def_elem) };
        if !val_ptr.is_null() {
            let value = unsafe { CStr::from_ptr(val_ptr) }.to_string_lossy().into_owned();
            result.insert(name, value);
        }
    }
    result
}
