use crate::error::KalamDbError;

#[inline]
fn invalid_pg_session_id(session_id: &str, reason: &str) -> KalamDbError {
    KalamDbError::InvalidOperation(format!("invalid pg session id '{}': {}", session_id, reason))
}

#[inline]
fn parse_u32_decimal(value: &[u8], session_id: &str) -> Result<u32, KalamDbError> {
    if value.is_empty() {
        return Err(invalid_pg_session_id(session_id, "missing backend pid"));
    }

    let mut parsed = 0u32;
    for &byte in value {
        if !byte.is_ascii_digit() {
            return Err(invalid_pg_session_id(session_id, "backend pid must be numeric"));
        }

        parsed = parsed
            .checked_mul(10)
            .and_then(|value| value.checked_add((byte - b'0') as u32))
            .ok_or_else(|| invalid_pg_session_id(session_id, "backend pid must be numeric"))?;
    }

    Ok(parsed)
}

#[inline]
fn parse_u64_hex(value: &[u8], session_id: &str) -> Result<u64, KalamDbError> {
    if value.is_empty() {
        return Ok(0);
    }

    let mut parsed = 0u64;
    for &byte in value {
        let digit = match byte {
            b'0'..=b'9' => (byte - b'0') as u64,
            b'a'..=b'f' => (byte - b'a' + 10) as u64,
            b'A'..=b'F' => (byte - b'A' + 10) as u64,
            _ => {
                return Err(invalid_pg_session_id(session_id, "config hash must be hexadecimal"));
            },
        };

        parsed = parsed
            .checked_mul(16)
            .and_then(|value| value.checked_add(digit))
            .ok_or_else(|| invalid_pg_session_id(session_id, "config hash must be hexadecimal"))?;
    }

    Ok(parsed)
}

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

    #[inline]
    pub fn from_pg_session_id(session_id: &str) -> Result<Self, KalamDbError> {
        let bytes = session_id.as_bytes();
        if !bytes.starts_with(b"pg-") {
            return Err(invalid_pg_session_id(session_id, "expected pg-<pid>-<config_hash>"));
        }

        let rest = &bytes[3..];
        let Some(separator_index) = rest.iter().position(|&byte| byte == b'-') else {
            let backend_pid = parse_u32_decimal(rest, session_id)?;
            return Ok(Self::PgSession {
                backend_pid,
                config_hash: 0,
            });
        };

        let backend_pid = parse_u32_decimal(&rest[..separator_index], session_id)?;
        let config_hash = parse_u64_hex(&rest[separator_index + 1..], session_id)?;

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

    #[test]
    fn parses_uppercase_pg_session_hashes() {
        let owner = ExecutionOwnerKey::from_pg_session_id("pg-321-DEADBEEF").unwrap();
        assert_eq!(
            owner,
            ExecutionOwnerKey::PgSession {
                backend_pid: 321,
                config_hash: 0xDEADBEEF,
            }
        );
    }

    #[test]
    fn rejects_non_numeric_backend_pid() {
        let error = ExecutionOwnerKey::from_pg_session_id("pg-abc-deadbeef").unwrap_err();
        assert!(error.to_string().contains("backend pid must be numeric"));
    }
}
