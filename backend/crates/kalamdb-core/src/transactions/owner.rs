use crate::error::KalamDbError;

/// Compact internal execution owner identifier used on the transaction hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionOwnerKey {
    PgSession { backend_pid: u32, config_hash: u64 },
    SqlRequest { request_nonce: u64 },
    Internal { source_nonce: u64 },
}

impl ExecutionOwnerKey {
    #[inline]
    pub fn sql_request(request_nonce: u64) -> Self {
        Self::SqlRequest { request_nonce }
    }

    #[inline]
    pub fn internal(source_nonce: u64) -> Self {
        Self::Internal { source_nonce }
    }

    pub fn from_pg_session_id(session_id: &str) -> Result<Self, KalamDbError> {
        let Some(rest) = session_id.strip_prefix("pg-") else {
            return Err(KalamDbError::InvalidOperation(format!(
                "invalid pg session id '{}': expected pg-<pid>-<config_hash>",
                session_id
            )));
        };

        let mut parts = rest.splitn(2, '-');
        let backend_pid = parts
            .next()
            .ok_or_else(|| {
                KalamDbError::InvalidOperation(format!(
                    "invalid pg session id '{}': missing backend pid",
                    session_id
                ))
            })?
            .parse::<u32>()
            .map_err(|_| {
                KalamDbError::InvalidOperation(format!(
                    "invalid pg session id '{}': backend pid must be numeric",
                    session_id
                ))
            })?;

        let config_hash = match parts.next() {
            Some(value) if !value.is_empty() => u64::from_str_radix(value, 16).map_err(|_| {
                KalamDbError::InvalidOperation(format!(
                    "invalid pg session id '{}': config hash must be hexadecimal",
                    session_id
                ))
            })?,
            _ => 0,
        };

        Ok(Self::PgSession {
            backend_pid,
            config_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_config_scoped_pg_session_ids() {
        let owner = ExecutionOwnerKey::from_pg_session_id("pg-321-deadbeef").unwrap();
        assert_eq!(
            owner,
            ExecutionOwnerKey::PgSession {
                backend_pid: 321,
                config_hash: 0xdeadbeef,
            }
        );
    }

    #[test]
    fn parses_pg_session_ids_without_hash() {
        let owner = ExecutionOwnerKey::from_pg_session_id("pg-321").unwrap();
        assert_eq!(
            owner,
            ExecutionOwnerKey::PgSession {
                backend_pid: 321,
                config_hash: 0,
            }
        );
    }
}