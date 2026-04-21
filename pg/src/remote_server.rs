use crate::fdw_options::parse_options;
use crate::remote_state::{self, RemoteExtensionState};
use kalam_pg_common::KalamPgError;
use kalam_pg_fdw::ServerOptions;
use pgrx::pg_sys;
use std::ffi::CString;
use std::sync::Arc;

unsafe fn remote_state_for_server(
    server: *mut pg_sys::ForeignServer,
) -> Result<Arc<RemoteExtensionState>, KalamPgError> {
    if server.is_null() {
        return Err(KalamPgError::Execution("foreign server not found".to_string()));
    }

    let server_options = parse_options((*server).options);
    let parsed_server = ServerOptions::parse(&server_options)?;
    let remote_config = parsed_server.remote.ok_or_else(|| {
        KalamPgError::Validation(
            "foreign server must have host and port options for remote mode".to_string(),
        )
    })?;

    remote_state::ensure_remote_extension_state(remote_config)
        .map_err(|error| KalamPgError::Execution(error.to_string()))
}

pub unsafe fn remote_state_for_server_id(
    server_id: pg_sys::Oid,
) -> Result<Arc<RemoteExtensionState>, KalamPgError> {
    let server = pg_sys::GetForeignServer(server_id);
    remote_state_for_server(server)
}

pub unsafe fn remote_state_for_server_name(
    server_name: &str,
) -> Result<Arc<RemoteExtensionState>, KalamPgError> {
    let c_name = CString::new(server_name).unwrap_or_default();
    let server = pg_sys::GetForeignServerByName(c_name.as_ptr(), true);
    if server.is_null() {
        return Err(KalamPgError::Execution(format!(
            "foreign server '{}' not found",
            server_name
        )));
    }

    remote_state_for_server(server)
}