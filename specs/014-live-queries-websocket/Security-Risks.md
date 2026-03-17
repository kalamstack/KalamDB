# KalamDB Security Vulnerability Audit Report

> **Audit Date:** November 29, 2025  
> **Branch:** 014-live-queries-websocket  
> **Status:** Identified vulnerabilities pending review and remediation  
> **Last Updated:** Phase 6 Final Review Audit Complete

---

## Summary

| Severity | Count | Phase 1 | Phase 2 | Phase 3 | Phase 5 | Phase 6 (New) |
|----------|-------|---------|---------|---------|---------|---------------|
| 🔴 Critical | 6 | 3 | 2 | 0 | 1 | 0 |
| 🟠 High | 18 | 7 | 2 | 4 | 3 | 2 |
| 🟡 Medium | 27 | 8 | 9 | 6 | 3 | 1 |
| 🟢 Low | 9 | 7 | 2 | 0 | 0 | 0 |
| **Total** | **60** | 25 | 15 | 10 | 7 | 3 |

### Phase 6 New Findings Summary (Final Review)

| ID | Severity | Issue | File |
|----|----------|-------|------|
| #58 | 🟠 High | Rate Limit Bypass via X-Real-IP Spoofing | `middleware.rs:285` |
| #59 | 🟠 High | Fragile SQL Parsing in Live Queries | `query_parser.rs:14` |
| #60 | 🟡 Medium | Stack Overflow in Filter Evaluation | `filter_eval.rs:119` |

### Phase 5 New Findings Summary (Production Readiness Audit)

| ID | Severity | Issue | File |
|----|----------|-------|------|
| #51 | 🔴 Critical | Path Traversal via Namespace/Table Names | `path_utils.rs`, `namespace_id.rs`, `table_name.rs` |
| #52 | 🟠 High | No Input Validation on Type-Safe Wrappers | `namespace_id.rs`, `table_name.rs`, `user_id.rs` |
| #53 | 🟠 High | Username Enumeration via Auth Logs | `unified.rs:173,206,227,244` |
| #54 | 🟡 Medium | Integer Overflow in Retention Calculation | `retention.rs:94` |
| #55 | 🟠 High | Shell Command Injection Risk | `main.rs:76` |
| #56 | 🟡 Medium | Unbounded serde_json Deserialization | Multiple files |
| #57 | 🟡 Medium | Token Bucket State Memory Exhaustion | `rate_limiter.rs` |

### Phase 3 New Findings Summary (kalamdb-auth Deep Dive + File Access)

| ID | Severity | Issue | File |
|----|----------|-------|------|
| #41 | 🟠 High | JWT Role Escalation via Claims | `unified.rs:268` |
| #42 | 🟠 High | Empty Issuer List Accepts All | `jwt_auth.rs:99` |
| #43 | 🟠 High | Cross-User Parquet File Access | `planner.rs:120` |
| #44 | 🟡 Medium | User ID Collision via Truncation | `row_id.rs:88` |
| #45 | 🟡 Medium | Unvalidated extract_claims_unverified | `jwt_auth.rs:114` |
| #46 | 🟡 Medium | OAuth RS256 Not Implemented | `oauth.rs:80` |
| #47 | 🟡 Medium | Impersonation Without Existence Check | `impersonation.rs:62` |
| #48 | 🟡 Medium | No Auth Rate Limiting | `unified.rs` |
| #49 | 🟡 Medium | RBAC Bypass Risk | `rbac.rs:22` |
| #50 | 🟠 High | X-Forwarded-For Spoofing | `ip_extractor.rs:35` |

### Phase 2 New Findings Summary

| ID | Severity | Issue | File |
|----|----------|-------|------|
| #26 | 🔴 Critical | SQL Injection via INSERT | `link/src/wasm.rs:602` |
| #27 | 🟠 High | Unvalidated Column Names | `link/src/wasm.rs:580` |
| #28 | 🟠 High | AS USER Impersonation Audit Gap | `kalamdb-sql/parser.rs` |
| #29 | 🟡 Medium | Panic on Invalid WHERE Clause | `filter_eval.rs` |
| #30 | 🟡 Medium | Unbounded Live Query Registration | `live/manager.rs` |
| #31 | 🟡 Medium | Destructive DDL Without Confirmation | DDL handlers |
| #32 | 🟠 High | System Table Direct Modification | `tables/system/` |
| #33 | 🟡 Medium | Regex Injection (ReDoS) | `filter_eval.rs` |
| #34 | 🟡 Medium | Integer Overflow in Row Counts | `entity_store.rs` |
| #35 | 🟡 Medium | Production panic! Points | Multiple files |
| #36 | 🟢 Low | Timing Attack on Password Verify | `unified.rs` |
| #37 | 🟡 Medium | WebSocket Message Size Unbounded | `ws_handler.rs` |
| #38 | 🟡 Medium | Unvalidated Deserialization Depth | Multiple |
| #39 | 🟡 Medium | Sensitive Data in Debug Logs | Multiple |
| #40 | 🟡 Medium | Connection Exhaustion (Slow Loris) | `lifecycle.rs` |

---

## 🔴 CRITICAL SEVERITY

### 1. SQL Injection in WASM SDK

| Attribute | Value |
|-----------|-------|
| **File** | `link/src/wasm.rs` |
| **Lines** | 467-471, 484-485, 519 |
| **CVSS Score** | 9.8 (Critical) |

**Vulnerable Code:**
```rust
// DELETE - Line 484-485
let sql = format!("DELETE FROM {} WHERE id = {}", table_name, row_id);

// INSERT - Line 467-471
let sql = format!(
    "INSERT INTO {} ({}) VALUES ({})",
    table_name,           // ⚠️ User input - no validation
    columns.join(", "),   // ⚠️ Column names from JSON keys - no validation
    values.join(", ")
);

// SUBSCRIBE - Line 519
let sql = format!("SELECT * FROM {}", table_name);
```

**Attack Vector:**
- Malicious `row_id` like `1; DROP TABLE users; --` or `1 OR 1=1` would be directly executed
- Malicious `table_name` could inject arbitrary SQL
- Column names from JSON keys are not validated

**Recommendation:**
- Validate `table_name` against regex pattern: `^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`
- Validate `row_id` is a valid integer/UUID format before concatenation
- Quote identifiers properly using double-quotes (SQL standard)
- Consider using parameterized queries via the existing parameter binding infrastructure

---

### 2. Hardcoded Default JWT Secret with Insecure Fallback

| Attribute | Value |
|-----------|-------|
| **Files** | `backend/crates/kalamdb-auth/src/unified.rs`, `backend/crates/kalamdb-commons/src/config/defaults.rs` |
| **Lines** | 244-246, 234-235 |
| **CVSS Score** | 9.1 (Critical) |

**Vulnerable Code:**
```rust
// unified.rs - Lines 244-246
let secret = std::env::var("KALAMDB_JWT_SECRET")
    .unwrap_or_else(|_| "kalamdb-dev-secret-key-change-in-production".to_string());

// defaults.rs - Lines 234-235
pub fn default_auth_jwt_secret() -> String {
    "CHANGE_ME_IN_PRODUCTION".to_string()
}
```

**Impact:**
- If `KALAMDB_JWT_SECRET` environment variable is not set, the code silently falls back to a hardcoded, publicly known secret
- Attackers can forge valid JWT tokens using the known default secret
- Complete authentication bypass possible

**Recommendation:**
- Fail startup loudly if JWT secret is not explicitly configured
- Add startup validation to reject default/weak secrets
- Log a critical warning if running with default secret in any mode
- Implement minimum secret length requirement (at least 32 characters)

---

### 3. CORS Misconfiguration with Credentials

| Attribute | Value |
|-----------|-------|
| **File** | `backend/src/middleware.rs` |
| **Lines** | 33-41 |
| **CVSS Score** | 8.1 (High/Critical) |

**Vulnerable Code:**
```rust
pub fn build_cors() -> Cors {
    Cors::default()
        .allow_any_origin()
        .allow_any_method()
        .allow_any_header()
        .supports_credentials()  // ⚠️ Dangerous with allow_any_origin
        .max_age(3600)
}
```

**Impact:**
- Allows credentials (cookies, authorization headers) from ANY domain
- Enables cross-site request forgery (CSRF) attacks
- Any malicious website can make authenticated requests on behalf of users
- Violates CORS specification - browsers should reject this combination

**Recommendation:**
- Configure specific allowed origins for production
- Remove `.supports_credentials()` if wildcards are truly needed
- Add configurable CORS origins via server configuration
- Example fix:
  ```rust
  Cors::default()
      .allowed_origin("https://your-app.com")
      .allowed_methods(vec!["GET", "POST", "PUT", "DELETE"])
      .allowed_headers(vec![header::AUTHORIZATION, header::CONTENT_TYPE])
      .supports_credentials()
  ```

---

## 🟠 HIGH SEVERITY

### 4. Insecure JWT Token Extraction Function (Public API)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/jwt.rs` |
| **Lines** | 120-133 |
| **CVSS Score** | 7.5 (High) |

**Vulnerable Code:**
```rust
/// **WARNING**: This does NOT verify the signature! Only use for debugging
pub fn extract_claims_unverified(token: &str) -> AuthResult<JwtClaims> {
    let mut validation = Validation::new(Algorithm::HS256);
    #[allow(deprecated)]
    validation.insecure_disable_signature_validation(); // DANGEROUS
    validation.validate_exp = false;
    // ...
}
```

**Impact:**
- Bypasses all JWT security checks including signature and expiration
- Public API that could be accidentally or maliciously misused
- Could lead to authentication bypass if used incorrectly

**Recommendation:**
- Make this function `pub(crate)` to limit exposure
- Add `#[doc(hidden)]` annotation
- Consider removing entirely if not needed in production
- Add runtime checks to prevent use in production builds

---

### 5. Empty Trusted Issuers Accepts Any JWT Issuer

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/jwt.rs` |
| **Lines** | 87-90 |
| **CVSS Score** | 7.5 (High) |

**Vulnerable Code:**
```rust
fn verify_issuer(issuer: &str, trusted_issuers: &[String]) -> AuthResult<()> {
    if trusted_issuers.is_empty() {
        return Ok(());  // ⚠️ Accepts ANY issuer when empty
    }
    // ...
}
```

**Impact:**
- When `jwt_trusted_issuers` is empty (the default), ANY issuer is accepted
- Tokens from malicious issuers would be considered valid
- Weakens the JWT validation chain

**Recommendation:**
- Either require explicit issuer configuration when JWT auth is enabled
- Or reject all JWTs when no trusted issuers are configured
- Log a warning when running with empty trusted issuers

---

### 6. No TLS/HTTPS Enforcement

| Attribute | Value |
|-----------|-------|
| **File** | `backend/src/lifecycle.rs` |
| **Lines** | 327-350 |
| **CVSS Score** | 7.4 (High) |

**Vulnerable Code:**
```rust
let server = if config.server.enable_http2 {
    info!("HTTP/2 support enabled (h2c - HTTP/2 cleartext)");
    server.bind_auto_h2c(&bind_addr)?  // ⚠️ No TLS
} else {
    info!("HTTP/1.1 only mode");
    server.bind(&bind_addr)?
};
```

**Impact:**
- All traffic transmitted in plaintext including:
  - Authentication credentials (username/password)
  - JWT tokens
  - WebSocket messages
  - Query data and results
- Susceptible to man-in-the-middle attacks
- Session hijacking possible on shared networks

**Recommendation:**
- Add TLS configuration with `rustls` or `openssl`
- Document reverse proxy requirement with TLS termination
- Add `bind_rustls()` or `bind_openssl()` options
- Warn at startup when TLS is not configured

---

### 7. WebSocket Missing Origin Validation

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-api/src/websocket.rs` |
| **Lines** | 33-68 |
| **CVSS Score** | 7.1 (High) |

**Vulnerable Code:**
```rust
#[get("/ws")]
pub async fn websocket_handler(
    req: HttpRequest,
    stream: web::Payload,
    // ...
) -> Result<HttpResponse, Error> {
    // No Origin header validation
```

**Impact:**
- Any website can establish WebSocket connections
- Cross-site WebSocket hijacking possible
- Authenticated sessions can be exploited by malicious sites

**Recommendation:**
- Validate `Origin` header against allowed origins
- Reject connections from unknown origins
- Example:
  ```rust
  if let Some(origin) = req.headers().get("Origin") {
      if !allowed_origins.contains(&origin.to_str()?) {
          return Err(Error::Unauthorized);
      }
  }
  ```

---

### 8. RocksDB Unsafe Raw Pointer Usage

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-store/src/rocksdb_impl.rs` |
| **Lines** | 197-217, 246-253 |
| **CVSS Score** | 7.0 (High) |

**Vulnerable Code:**
```rust
unsafe {
    // SAFETY: This is safe because...
    let db_ptr = Arc::as_ptr(&self.db) as *mut DB;  // ⚠️ Mutable cast from shared ref
    match (*db_ptr).create_cf(partition.name(), &opts) {
```

**Impact:**
- Converting `Arc<DB>` to `*mut DB` violates Rust's aliasing rules
- Multiple threads calling `create_partition` concurrently could cause undefined behavior
- Memory corruption or data races possible

**Recommendation:**
- Use `Mutex<DB>` or `RwLock<DB>` instead of unsafe casting
- Consider using RocksDB's internal locking mechanisms properly
- Alternatively, use well-audited safe abstractions for RocksDB

---

### 9. WASM RefCell Panic Risk in Async Context

| Attribute | Value |
|-----------|-------|
| **File** | `link/src/wasm.rs` |
| **Lines** | 71-79, 116-120, 756 |
| **CVSS Score** | 6.5 (Medium-High) |

**Vulnerable Code:**
```rust
ws: Rc<RefCell<Option<WebSocket>>>,
subscription_state: Rc<RefCell<HashMap<String, SubscriptionState>>>,
connection_options: Rc<RefCell<ConnectionOptions>>,
reconnect_attempts: Rc<RefCell<u32>>,
is_reconnecting: Rc<RefCell<bool>>,

// Usage:
*is_reconnecting.borrow_mut() = false;  // Line 756
```

**Impact:**
- `RefCell` with `borrow()` and `borrow_mut()` will panic if already borrowed
- In async WASM contexts with callbacks, re-entrant borrowing is common
- Application crashes when borrow conflicts occur

**Recommendation:**
- Use `try_borrow_mut()` with proper error handling
- Consider using atomic types for simple flags (`AtomicBool`, `AtomicU32`)
- Implement proper state machine pattern to avoid re-entrancy

---

### 10. Production Unwrap on User Data Path

| Attribute | Value |
|-----------|-------|
| **File** | `backend/src/lifecycle.rs` |
| **Lines** | 49 |
| **CVSS Score** | 6.5 (Medium-High) |

**Vulnerable Code:**
```rust
let db_init = RocksDbInit::new(db_path.to_str().unwrap(), config.storage.rocksdb.clone());
```

**Impact:**
- `to_str()` returns `None` for paths with non-UTF8 characters
- Server will panic on startup if `rocksdb_path` contains invalid UTF-8
- Denial of service via malformed configuration

**Recommendation:**
- Use `to_string_lossy()` for graceful handling
- Or return proper error: `to_str().ok_or_else(|| ConfigError::InvalidPath)?`

---

## 🟡 MEDIUM SEVERITY

### 11. Path Traversal in Storage Path Resolution

| Attribute | Value |
|-----------|-------|
| **Files** | `backend/crates/kalamdb-filestore/src/path_resolver.rs`, `backend/crates/kalamdb-filestore/src/manifest_cache.rs` |
| **Lines** | 18-24, 261-269 |
| **CVSS Score** | 6.5 (Medium) |

**Vulnerable Code:**
```rust
// path_resolver.rs
if let Some(uid) = user_id {
    relative = relative.replace("{userId}", uid.as_str());  // ⚠️ No traversal check
}

// manifest_cache.rs
fn build_fallback_path(&self, table_id: &TableId, user_id: Option<&UserId>) -> PathBuf {
    let mut path = PathBuf::from(&self._base_path);
    path.push(table_id.namespace_id().as_str());  // ⚠️ No traversal check
    path.push(table_id.table_name().as_str());
    if let Some(uid) = user_id {
        path.push(uid.as_str());
    }
    path
}
```

**Impact:**
- User IDs, namespace IDs, and table names used directly in path construction
- Path traversal characters (`../`) could escape intended directories
- Potential access to files outside storage directory

**Recommendation:**
- Add explicit validation to reject `..`, `/`, and `\` in path components
- Canonicalize paths and verify they remain within base directory
- Example:
  ```rust
  fn sanitize_path_component(s: &str) -> Result<&str, Error> {
      if s.contains("..") || s.contains('/') || s.contains('\\') {
          return Err(Error::InvalidPathComponent);
      }
      Ok(s)
  }
  ```

---

### 12. Proxy Header Trust in Rate Limiter (IP Spoofing)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/src/middleware.rs` |
| **Lines** | 256-285 |
| **CVSS Score** | 6.1 (Medium) |

**Vulnerable Code:**
```rust
fn extract_client_ip(req: &ServiceRequest) -> IpAddr {
    // Try X-Forwarded-For header first (for reverse proxies)
    if let Some(forwarded) = req.headers().get("X-Forwarded-For") {
        // ... parses and trusts X-Forwarded-For unconditionally
    }
    // Try X-Real-IP header (nginx style)
    if let Some(real_ip) = req.headers().get("X-Real-IP") {
        // ... trusts X-Real-IP unconditionally
    }
```

**Impact:**
- Rate limiting can be bypassed by rotating `X-Forwarded-For` values
- Connection limits per IP can be circumvented
- Banned IPs can circumvent bans with forged headers

**Note:** The authentication flow uses a more secure extractor that validates against localhost spoofing, but the connection protection middleware does not.

**Recommendation:**
- Use the secure `extract_client_ip_from_request` from `kalamdb-auth` consistently
- Add configuration option to trust proxy headers only when behind a known proxy
- Validate that proxied IPs are from trusted proxy ranges

---

### 13. System User Empty Password on Localhost

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/unified.rs` |
| **Lines** | 166-176 |
| **CVSS Score** | 5.9 (Medium) |

**Vulnerable Code:**
```rust
if is_system_internal {
    if is_localhost {
        // Localhost system users: accept empty password OR valid password
        let password_ok = user.password_hash.is_empty()
            || password::verify_password(password, &user.password_hash)
                .await
                .unwrap_or(false);
```

**Impact:**
- System users can authenticate from localhost with no password if `password_hash` is empty
- Reduces security for local development environments
- Could be exploited if attacker gains local access

**Recommendation:**
- Warn/log when empty password is accepted
- Consider requiring password for system users even in development
- Add configuration flag to explicitly enable passwordless local auth

---

### 14. Username Logging on Failed Authentication

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/unified.rs` |
| **Lines** | 183, 204, 221 |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Code:**
```rust
warn!("Invalid password for system user: {}", username);
warn!("Invalid password for remote system user: {}", username);
warn!("Invalid password for user: {}", username);
```

**Impact:**
- Logging usernames on failed authentication allows attackers to enumerate valid usernames through log analysis
- Error messages to clients correctly don't distinguish between "user not found" and "wrong password", but logs reveal which users exist

**Recommendation:**
- Redact usernames from failed auth logs
- Use a hash or identifier instead of raw username
- Example: `warn!("Invalid password for user hash: {}", hash_username(username));`

---

### 15. Snowflake Generator Mutex Unwrap (Poison Risk)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-commons/src/ids/snowflake.rs` |
| **Lines** | 66 |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Code:**
```rust
let mut state = self.state.lock().unwrap();
```

**Impact:**
- Panics if the mutex is poisoned (previous holder panicked while holding the lock)
- In a concurrent ID generation system, this could cascade failures
- Single panic can bring down entire ID generation subsystem

**Recommendation:**
- Handle poisoned mutex gracefully:
  ```rust
  let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
  ```
- Or use `parking_lot::Mutex` which doesn't poison

---

### 16. Integer Overflow in Retention Calculation

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/jobs/executors/retention.rs` |
| **Lines** | 94 |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Code:**
```rust
let retention_ms = (retention_hours * 3600 * 1000) as i64;
```

**Impact:**
- If `retention_hours` is a large value, the multiplication can overflow before the cast to `i64`
- Could result in incorrect retention periods (data deleted too soon or never)

**Recommendation:**
- Use checked arithmetic:
  ```rust
  let retention_ms = retention_hours
      .checked_mul(3600)
      .and_then(|h| h.checked_mul(1000))
      .map(|ms| ms as i64)
      .ok_or(Error::RetentionOverflow)?;
  ```
- Or validate input bounds at configuration time

---

### 17. Information Disclosure in SQL Error Messages

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-api/src/routes/sql.rs` |
| **Lines** | 334-338 |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Code:**
```rust
return HttpResponse::BadRequest().json(SqlResponse::error_with_details(
    "SQL_EXECUTION_ERROR",
    &format!("Statement {} failed: {}", idx + 1, err),
    sql,  // ⚠️ Original SQL exposed in error response
    took,
));
```

**Impact:**
- Error responses include the original SQL statement
- Full database error messages exposed
- Can leak schema information (table/column names in error messages)
- Provides SQL injection debugging info to attackers

**Recommendation:**
- Remove SQL from production error responses
- Sanitize/generalize database error messages
- Log detailed errors server-side, return generic errors to clients

---

### 18. RocksDB Partition Creation Race Condition (TOCTOU)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-store/src/rocksdb_impl.rs` |
| **Lines** | 188-214 |
| **CVSS Score** | 4.7 (Medium) |

**Vulnerable Code:**
```rust
fn create_partition(&self, partition: &Partition) -> Result<()> {
    // Check if already exists
    if self.partition_exists(partition) {
        return Ok(());
    }

    // ⚠️ TOCTOU gap - another thread could create between check and create
    
    // Create new column family
    let opts = Options::default();
    unsafe {
        let db_ptr = Arc::as_ptr(&self.db) as *mut DB;
        match (*db_ptr).create_cf(partition.name(), &opts) {
```

**Impact:**
- Time-of-check to time-of-use race condition
- The check `partition_exists` and `create_cf` are not atomic
- Code handles this with error message parsing, but this is fragile

**Recommendation:**
- Use a proper mutex around partition operations
- Or rely solely on RocksDB's error handling (remove the pre-check)
- Use atomic check-and-create pattern

---

## 🟢 LOW SEVERITY

### 19. Secrets Checked into Source Control

| Attribute | Value |
|-----------|-------|
| **Files** | `examples/simple-typescript/.env`, `backend/server.toml`, `backend/server.example.toml` |
| **CVSS Score** | 3.7 (Low) |

**Details:**
- `examples/simple-typescript/.env` contains `VITE_KALAMDB_API_KEY=test-api-key-12345`
- `backend/server.toml` contains `jwt_secret = "CHANGE_ME_IN_PRODUCTION"`
- `backend/server.example.toml` contains placeholder secrets

**Recommendation:**
- Add `.env` to `.gitignore`
- Remove `server.toml` from source control, keep only `server.example.toml`
- Ensure example files clearly indicate values must be changed

---

### 20. Unnecessary Unsafe Send/Sync Implementations

| Attribute | Value |
|-----------|-------|
| **Files** | Multiple files in `backend/crates/kalamdb-commons/src/models/` |
| **CVSS Score** | 3.1 (Low) |

**Affected Files:**
- `models/user_name.rs:76-77`
- `models/ids/job_id.rs:70-71`
- `models/ids/audit_log_id.rs:67-68`
- `models/ids/row_id.rs:71-72`
- `models/ids/table_id.rs:108-109`
- `models/ids/user_row_id.rs:94-95`
- `models/ids/live_query_id.rs:139-140`

**Vulnerable Code:**
```rust
unsafe impl Send for UserName {}
unsafe impl Sync for UserName {}
```

**Impact:**
- Types contain only `String` which is already `Send + Sync`
- Manual `unsafe impl` is unnecessary and introduces potential for future bugs if the types are modified

**Recommendation:**
- Remove unnecessary `unsafe impl` blocks
- Let the compiler derive these traits automatically

---

### 21. Version/Build Info Disclosure

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-api/src/routes/health.rs` |
| **Lines** | 20-23 |
| **CVSS Score** | 3.1 (Low) |

**Details:**
```rust
HttpResponse::Ok().json(json!({
    "status": "healthy",
    "version": env!("CARGO_PKG_VERSION"),
    "api_version": "v1",
    "build_date": env!("BUILD_DATE")
}))
```

**Impact:**
- Healthcheck endpoint publicly exposes version and build date information
- While not critical alone, aids attackers in identifying known vulnerabilities

**Recommendation:**
- Consider adding authentication to healthcheck endpoint
- Or limit exposed information to just "healthy" status
- Separate internal metrics endpoint from external health endpoint

---

### 22. XSS in Browser Test HTML (Test File Only)

| Attribute | Value |
|-----------|-------|
| **File** | `link/tests/browser/test.html` |
| **Lines** | 117-118 |
| **CVSS Score** | 2.4 (Low) |

**Vulnerable Code:**
```javascript
div.innerHTML = `
    <strong>${passed ? '✓' : '✗'} ${name}</strong>
    ${message ? `<br><span class="log">${message}</span>` : ''}
`;
```

**Impact:**
- Test name and message inserted into innerHTML without sanitization
- Only affects test file, not production code

**Recommendation:**
- Use `textContent` instead of `innerHTML`
- Or sanitize input before insertion

---

### 23. WASM Callback Memory Leaks

| Attribute | Value |
|-----------|-------|
| **File** | `link/src/wasm.rs` |
| **Lines** | 255, 268, 275, 765 |
| **CVSS Score** | 2.4 (Low) |

**Vulnerable Code:**
```rust
onopen_callback.forget();
onerror_callback.forget();
onclose_callback.forget();
reconnect_fn.forget();
```

**Impact:**
- `forget()` intentionally leaks memory to keep callbacks alive
- While necessary for WASM, repeated reconnections could accumulate leaked closures
- Long-running applications may experience memory growth

**Recommendation:**
- Track closures and properly clean up on disconnect
- Consider using weak references where possible
- Document expected memory behavior

---

### 24. Snowflake ID i64 Cast (Future Overflow)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-commons/src/ids/snowflake.rs` |
| **Lines** | 95-98 |
| **CVSS Score** | 2.2 (Low) |

**Vulnerable Code:**
```rust
let id = ((timestamp - self.epoch) << 22)
    | ((self.worker_id as u64) << 12)
    | (state.sequence as u64);

Ok(id as i64)
```

**Impact:**
- Cast from `u64` to `i64` can produce negative IDs if the high bit is set
- This could happen after ~69 years from the epoch (2015), around year 2084

**Recommendation:**
- Add overflow checks
- Or document the expected lifetime of the system
- Consider using `u64` consistently for IDs

---

### 25. Row ID User ID Length Silent Truncation

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-store/src/keys/user_row_key.rs` |
| **Lines** | 85, 177 |
| **CVSS Score** | 2.2 (Low) |

**Vulnerable Code:**
```rust
let user_id_len = user_id_bytes.len().min(255) as u8;
// ...
key.extend_from_slice(&user_id_bytes[..user_id_len as usize]);
```

**Impact:**
- User IDs longer than 255 bytes are silently truncated
- Two different users with 256+ byte IDs sharing the first 255 bytes would have key collisions
- Could lead to data mixing between users

**Recommendation:**
- Return an error for overly long user IDs at validation time
- Log a warning if truncation occurs
- Document maximum user ID length

---

## 🔴 Additional Findings - Deep Audit (Phase 2)

The following vulnerabilities were discovered during the exhaustive codebase-wide security scan:

---

### 26. SQL Injection via INSERT Statement in WASM

| Attribute | Value |
|-----------|-------|
| **File** | `link/src/wasm.rs` |
| **Lines** | 602-610 |
| **CVSS Score** | 9.8 (Critical) |

**Vulnerable Code:**
```rust
let sql = format!(
    "INSERT INTO {} ({}) VALUES ({})",
    table, columns_str, placeholders
);
```

**Impact:**
- The `table` variable comes from user input via `table_name` parameter
- Attacker can inject: `malicious_table; DROP TABLE users; --`
- Full SQL injection allowing arbitrary DDL/DML execution

**Recommendation:**
```rust
let validated_table = validate_table_name(table)
    .map_err(|e| JsValue::from_str(&e.to_string()))?;
```

---

### 27. Unvalidated Column Names in SQL Construction

| Attribute | Value |
|-----------|-------|
| **File** | `link/src/wasm.rs` |
| **Lines** | 580-600 |
| **CVSS Score** | 8.1 (High) |

**Vulnerable Code:**
```rust
let columns_str = columns.join(", ");
// Later used in: format!("INSERT INTO {} ({}) VALUES ...", table, columns_str, ...)
```

**Impact:**
- Column names from user input are directly concatenated
- Attacker can inject malicious SQL via column names: `id); DROP TABLE users; --`
- Bypass of any table-level validation

**Recommendation:**
```rust
fn validate_column_name(name: &str) -> Result<&str, String> {
    let pattern = regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
    if pattern.is_match(name) && name.len() <= 128 {
        Ok(name)
    } else {
        Err(format!("Invalid column name: {}", name))
    }
}

let validated_columns: Result<Vec<&str>, _> = columns
    .iter()
    .map(|c| validate_column_name(c))
    .collect();
```

---

### 28. AS USER Impersonation Without Audit Trail

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-sql/src/parser.rs` |
| **Lines** | 120-145 |
| **CVSS Score** | 7.5 (High) |

**Vulnerable Pattern:**
```rust
// AS USER parsing allows privileged users to impersonate others
// But impersonation actions may not be consistently logged
if let Some(as_user) = &self.as_user {
    // Execute as impersonated user
}
```

**Impact:**
- Privileged users (dba, system) can execute queries as any user
- If audit logging is incomplete, malicious actions become untraceable
- Privilege escalation attack vector

**Recommendation:**
- Always log impersonation with BOTH original and impersonated user
- Add dedicated audit event type for impersonation
- Consider rate-limiting impersonation attempts

```rust
audit_log!(
    event: "impersonation",
    original_user: original_user_id,
    impersonated_user: as_user,
    action: sql_statement,
    timestamp: now,
);
```

---

### 29. Panic on Invalid WHERE Clause Parsing

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/live/filter_eval.rs` |
| **Lines** | 50-81 |
| **CVSS Score** | 6.5 (Medium) |

**Vulnerable Code:**
```rust
pub fn parse_where_clause(sql: &str) -> Option<Expr> {
    // Uses DataFusion SQL parser
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    // ...
}
```

**Impact:**
- Malformed WHERE clauses could cause parser panics
- Denial of service for live query subscriptions
- Need to ensure all parsing is wrapped in catch_unwind or Result handling

**Recommendation:**
```rust
pub fn parse_where_clause(sql: &str) -> Result<Option<Expr>, ParseError> {
    // Validate input length first
    if sql.len() > MAX_WHERE_CLAUSE_LENGTH {
        return Err(ParseError::TooLong);
    }
    
    // Use result handling, not panics
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ParseError::InvalidSyntax(e.to_string()))?;
    // ...
}
```

---

### 30. Unbounded Live Query Registration

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/live/manager.rs` |
| **Lines** | (throughout) |
| **CVSS Score** | 6.5 (Medium) |

**Vulnerable Pattern:**
- No apparent limit on number of live queries per user/connection
- No maximum filter complexity validation
- Memory exhaustion possible via many subscriptions

**Impact:**
- Attacker creates thousands of live query subscriptions
- Each subscription consumes memory for filter state
- DoS via resource exhaustion

**Recommendation:**
```rust
const MAX_LIVE_QUERIES_PER_CONNECTION: usize = 100;
const MAX_LIVE_QUERIES_PER_USER: usize = 500;
const MAX_FILTER_COMPLEXITY: usize = 50; // max AST nodes

fn register_live_query(&self, conn_id: &str, query: LiveQuery) -> Result<(), Error> {
    let conn_count = self.queries_by_connection(conn_id).len();
    if conn_count >= MAX_LIVE_QUERIES_PER_CONNECTION {
        return Err(Error::TooManySubscriptions);
    }
    // ...
}
```

---

### 31. Destructive DDL Without Confirmation

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/sql/executor/handlers/` |
| **Lines** | Multiple DDL handlers |
| **CVSS Score** | 5.5 (Medium) |

**Vulnerable Pattern:**
```rust
// DROP TABLE, DROP NAMESPACE execute immediately without confirmation
// No "soft delete" period for recovery
```

**Impact:**
- Accidental or malicious DROP commands cause immediate data loss
- No recovery mechanism for dropped tables
- Combined with SQL injection = catastrophic data loss

**Recommendation:**
- Add `DROP TABLE ... SOFT` with configurable retention period
- Require `DROP TABLE ... FORCE` for immediate deletion
- Add confirmation delay for destructive DDL (5 seconds)
- Log all destructive DDL with extra audit detail

---

### 32. System Table Direct Modification Risk

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/tables/system/` |
| **Lines** | Various |
| **CVSS Score** | 7.8 (High) |

**Vulnerable Pattern:**
```rust
// System tables (system.users, system.jobs) may be modifiable via SQL
// Need to verify INSERT/UPDATE/DELETE are properly blocked
```

**Impact:**
- If system tables are writable via SQL, attacker could:
  - Create admin users: `INSERT INTO system.users (role) VALUES ('system')`
  - Modify job schedules for persistence
  - Tamper with audit logs

**Recommendation:**
```rust
// In DDL/DML handlers
fn validate_target_table(table: &TableId) -> Result<(), Error> {
    if table.namespace().as_str() == "system" {
        return Err(Error::SystemTableReadOnly(table.clone()));
    }
    Ok(())
}
```

---

### 33. Regex Injection in Search/Filter

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/live/filter_eval.rs` |
| **Lines** | Pattern matching sections |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Pattern:**
```rust
// If LIKE or SIMILAR TO clauses compile user input as regex
// ReDoS (Regular Expression Denial of Service) possible
```

**Impact:**
- Specially crafted regex patterns cause exponential backtracking
- CPU exhaustion via: `LIKE '(a+)+b'` with long input
- DoS for the entire server

**Recommendation:**
```rust
use regex::RegexBuilder;

fn compile_like_pattern(pattern: &str) -> Result<Regex, Error> {
    // Convert SQL LIKE to regex safely
    let regex_pattern = like_to_regex(pattern)?;
    
    RegexBuilder::new(&regex_pattern)
        .size_limit(10_000)  // Limit compiled regex size
        .dfa_size_limit(100_000)  // Limit DFA size
        .build()
        .map_err(|_| Error::InvalidPattern)
}
```

---

### 34. Integer Overflow in Row Count Calculations

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-store/src/entity_store.rs` |
| **Lines** | Various count/size methods |
| **CVSS Score** | 4.3 (Medium) |

**Vulnerable Pattern:**
```rust
// Row counts use u64 but intermediate calculations may overflow
total_rows = segment1_count + segment2_count + buffer_count;
```

**Impact:**
- With very large tables, row counts could wrap around
- Incorrect statistics affecting query optimization
- Billing/quota bypass if row counts are used for limits

**Recommendation:**
```rust
fn total_row_count(&self) -> Result<u64, Error> {
    let mut total: u64 = 0;
    for segment in &self.segments {
        total = total.checked_add(segment.row_count())
            .ok_or(Error::RowCountOverflow)?;
    }
    Ok(total)
}
```

---

### 35. Production panic! Points

| Attribute | Value |
|-----------|-------|
| **Files** | Multiple production files |
| **CVSS Score** | 5.9 (Medium) |

**Locations Found:**
```
backend/crates/kalamdb-core/src/schema_registry/registry.rs: panic!("...")
backend/crates/kalamdb-store/src/rocksdb_impl.rs: expect("should not fail")
backend/crates/kalamdb-api/src/handlers/sql_handler.rs: unwrap()
backend/src/lifecycle.rs: panic!("Server initialization failed")
```

**Impact:**
- Production panics cause complete server crashes
- DoS via triggering panic conditions
- Loss of in-flight transactions and connections

**Recommendation:**
- Replace all `panic!`, `unwrap()`, `expect()` in production code paths
- Use `Result<T, E>` and proper error propagation
- Add `#[deny(clippy::unwrap_used, clippy::expect_used)]` to lib.rs

---

### 36. Timing Attack on Password Verification

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/unified.rs` |
| **Lines** | Password verification logic |
| **CVSS Score** | 3.7 (Low) |

**Vulnerable Pattern:**
```rust
// Early return if user not found vs bcrypt verification
if user.is_none() {
    return Err(AuthError::InvalidCredentials);  // Fast
}
let user = user.unwrap();
bcrypt::verify(password, &user.password_hash)?;  // Slow
```

**Impact:**
- Timing difference reveals whether username exists
- Username enumeration attack vector
- Helps targeted attacks

**Recommendation:**
```rust
// Always perform a bcrypt verification to maintain constant time
let dummy_hash = "$2b$12$K4z5..."; // Pre-computed dummy hash

match user {
    Some(u) => bcrypt::verify(password, &u.password_hash),
    None => {
        // Perform dummy verification to maintain timing
        let _ = bcrypt::verify("dummy", dummy_hash);
        Err(AuthError::InvalidCredentials)
    }
}
```

---

### 37. WebSocket Message Size Unbounded

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-api/src/handlers/ws_handler.rs` |
| **Lines** | Message handling |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Pattern:**
```rust
// WebSocket message handling may not limit message size
async fn handle_message(msg: ws::Message) -> Result<...> {
    let text = msg.to_text()?;  // Could be arbitrarily large
    let request: WsRequest = serde_json::from_str(text)?;
    // ...
}
```

**Impact:**
- Client sends multi-GB WebSocket message
- Server attempts to parse, causing OOM
- DoS via memory exhaustion

**Recommendation:**
```rust
const MAX_WS_MESSAGE_SIZE: usize = 1_048_576; // 1MB

async fn handle_message(msg: ws::Message) -> Result<...> {
    let bytes = msg.into_bytes();
    if bytes.len() > MAX_WS_MESSAGE_SIZE {
        return Err(WsError::MessageTooLarge);
    }
    let request: WsRequest = serde_json::from_slice(&bytes)?;
    // ...
}
```

---

### 38. Unvalidated Deserialization Depth

| Attribute | Value |
|-----------|-------|
| **Files** | Multiple (serde_json usage) |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Pattern:**
```rust
// Deep nesting in JSON could cause stack overflow
let value: serde_json::Value = serde_json::from_str(input)?;
```

**Impact:**
- Deeply nested JSON (`{"a":{"a":{"a":...}}}` 10000 levels)
- Stack overflow during parsing
- DoS via crafted payloads

**Recommendation:**
```rust
use serde_json::de::Deserializer;

fn safe_deserialize<T: DeserializeOwned>(input: &str) -> Result<T, Error> {
    let mut deserializer = Deserializer::from_str(input);
    deserializer.disable_recursion_limit();  // Actually need custom limit
    
    // Use streaming parser with depth tracking
    let value = T::deserialize(&mut deserializer)?;
    
    if deserializer.end().is_err() {
        return Err(Error::TrailingData);
    }
    Ok(value)
}
```

---

### 39. Sensitive Data in Debug Logs

| Attribute | Value |
|-----------|-------|
| **Files** | Multiple (debug! and trace! macros) |
| **CVSS Score** | 4.0 (Medium) |

**Vulnerable Pattern:**
```rust
debug!("Authenticating user: {:?}", user);  // May include password_hash
trace!("Query: {}", sql);  // May include sensitive WHERE clauses
```

**Impact:**
- Password hashes logged at debug level
- User data visible in log files
- Compliance violations (GDPR, HIPAA)

**Recommendation:**
```rust
// Use custom Debug implementations that redact sensitive fields
#[derive(Debug)]
pub struct User {
    pub id: UserId,
    pub username: String,
    #[debug(skip)]  // Use derivative or manual impl
    pub password_hash: String,
}

// Or redact in logging
debug!("Authenticating user: {}", user.username);  // Not {:?}
```

---

### 40. Connection Exhaustion via Slow Loris

| Attribute | Value |
|-----------|-------|
| **File** | `backend/src/lifecycle.rs` |
| **Lines** | Server configuration |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Pattern:**
```rust
// No apparent connection timeout or slow-read protection
HttpServer::new(|| ...)
    .bind(addr)?
    .run()
```

**Impact:**
- Attacker opens many connections, sends data very slowly
- Server holds connections open waiting for data
- Connection pool exhausted, legitimate users denied

**Recommendation:**
```rust
HttpServer::new(|| ...)
    .client_request_timeout(Duration::from_secs(30))
    .client_disconnect_timeout(Duration::from_secs(5))
    .keep_alive(Duration::from_secs(60))
    .max_connections(10_000)
    .max_connection_rate(256)
    .bind(addr)?
    .run()
```

---

## 🛡️ Remediation Priority

### Immediate (Critical + High)

1. **SQL Injection in WASM SDK** - Validate and escape all user inputs in `link/src/wasm.rs`
2. **Hardcoded JWT Secret** - Fail startup if JWT secret is not explicitly configured
3. **CORS Misconfiguration** - Configure specific allowed origins in production
4. **TLS/HTTPS** - Add TLS support or document reverse proxy requirement
5. **WebSocket Origin Validation** - Validate Origin header before accepting connections
6. **RocksDB Unsafe Pointer** - Replace with proper mutex-based synchronization
7. **SQL Injection via INSERT** (NEW #26) - Validate table names in WASM SDK
8. **Unvalidated Column Names** (NEW #27) - Validate column names before SQL construction
9. **AS USER Impersonation Audit** (NEW #28) - Ensure all impersonation is logged
10. **System Table Direct Modification** (NEW #32) - Block writes to system.* tables

### Short-term (Medium)

7. **Path Traversal** - Add validation for path components
8. **Proxy Header Trust** - Use consistent secure IP extraction
9. **Empty Password Auth** - Add warnings and configuration flags
10. **Username Logging** - Redact sensitive information from logs
11. **Integer Overflow** - Add checked arithmetic
12. **Error Information Disclosure** - Sanitize error responses
13. **Live Query Limits** (NEW #30) - Add per-connection/user subscription limits
14. **Destructive DDL Protection** (NEW #31) - Add soft delete and confirmation
15. **Regex Injection** (NEW #33) - Use bounded regex compilation
16. **Production Panics** (NEW #35) - Replace unwrap/expect with Result handling
17. **WebSocket Message Size** (NEW #37) - Limit maximum message size
18. **Deserialization Depth** (NEW #38) - Limit JSON nesting depth
19. **Debug Log Sensitivity** (NEW #39) - Redact sensitive data from logs
20. **Connection Exhaustion** (NEW #40) - Add timeouts and connection limits

### Long-term (Low)

13. **Remove secrets from source control**
14. **Clean up unnecessary unsafe impls**
15. **Limit version disclosure**
16. **Fix test file XSS**
17. **Address memory leaks in WASM**
18. **Document ID generation limitations**
19. **Timing Attack** (NEW #36) - Use constant-time password verification

---

## 🔴 Additional Findings - Deep Audit (Phase 3)

The following vulnerabilities were discovered during the comprehensive authentication and file access audit:

---

### 41. JWT Role Escalation via Claims Override

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/unified.rs` |
| **Lines** | 268-279 |
| **CVSS Score** | 8.5 (High) |

**Vulnerable Code:**
```rust
// Override role from claims if present
let role = claims
    .role
    .as_deref()
    .and_then(|r| match r.to_lowercase().as_str() {
        "system" => Some(Role::System),
        "dba" => Some(Role::Dba),
        "service" => Some(Role::Service),
        "user" => Some(Role::User),
        _ => None,
    })
    .unwrap_or(user.role);
```

**Impact:**
- If an attacker can forge a JWT (via weak secret, stolen key, or algorithm confusion)
- They can set `role: "system"` in the claims to escalate privileges
- The code trusts JWT claims to override database role without verification
- Enables complete privilege escalation to System role

**Recommendation:**
```rust
// Don't allow role override via JWT claims - use database role
let role = user.role;

// OR if role override is intentional, verify it's not an escalation:
let claimed_role = claims.role.as_deref().and_then(parse_role);
let role = match claimed_role {
    Some(claimed) if claimed.level() <= user.role.level() => claimed,
    _ => user.role,  // Cannot escalate above database role
};
```

---

### 42. Empty Trusted Issuer List Accepts All JWTs

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/jwt_auth.rs` |
| **Lines** | 99-102 |
| **CVSS Score** | 7.8 (High) |

**Vulnerable Code:**
```rust
fn verify_issuer(issuer: &str, trusted_issuers: &[String]) -> AuthResult<()> {
    if trusted_issuers.is_empty() {
        // If no issuers configured, accept any
        return Ok(());
    }
    // ...
}
```

**Impact:**
- If `KALAMDB_JWT_TRUSTED_ISSUERS` is not set or empty
- ANY issuer is accepted in JWT tokens
- Attacker can create tokens with `iss: "evil.com"` and they'll be accepted
- Combined with weak secret = complete authentication bypass

**Recommendation:**
```rust
fn verify_issuer(issuer: &str, trusted_issuers: &[String]) -> AuthResult<()> {
    if trusted_issuers.is_empty() {
        // FAIL CLOSED: No trusted issuers = reject all tokens
        return Err(AuthError::UntrustedIssuer(
            "No trusted issuers configured".to_string()
        ));
    }
    // ...
}
```

---

### 43. Cross-User Data Access via Parquet File Path Manipulation

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/manifest/planner.rs` |
| **Lines** | 120 |
| **CVSS Score** | 7.5 (High) |

**Vulnerable Code:**
```rust
for parquet_file in &parquet_files {
    let file = fs::File::open(parquet_file).map_err(KalamDbError::Io)?;
    // No validation that path is within expected storage directory
}
```

**Impact:**
- If `parquet_file` path can be manipulated (via manifest tampering or injection)
- Attacker could read arbitrary Parquet files from other users
- Path like `../../other_user/table/batch.parquet` could bypass user isolation
- User A reads User B's private data

**Recommendation:**
```rust
fn validate_parquet_path(file_path: &Path, expected_base: &Path) -> Result<(), Error> {
    let canonical = file_path.canonicalize()?;
    let expected_canonical = expected_base.canonicalize()?;
    
    if !canonical.starts_with(&expected_canonical) {
        return Err(Error::PathTraversal(format!(
            "Path {} is outside expected directory {}",
            file_path.display(), expected_base.display()
        )));
    }
    Ok(())
}
```

---

### 44. User ID Collision via Key Truncation

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-commons/src/ids/row_id.rs` |
| **Lines** | 88, 180 |
| **CVSS Score** | 6.5 (Medium) |

**Vulnerable Code:**
```rust
let user_id_len = user_id_bytes.len().min(255) as u8;
// ...
key.extend_from_slice(&user_id_bytes[..user_id_len as usize]);
```

**Impact:**
- User IDs longer than 255 bytes are silently truncated
- Two users with IDs differing only after byte 255 would share storage keys
- Example: `user_a_<250 chars>_alice` and `user_a_<250 chars>_bob` = same key
- Data from User A visible to User B

**Recommendation:**
```rust
const MAX_USER_ID_LENGTH: usize = 128;  // Reasonable limit

fn validate_user_id(user_id: &str) -> Result<(), ValidationError> {
    if user_id.len() > MAX_USER_ID_LENGTH {
        return Err(ValidationError::UserIdTooLong(user_id.len()));
    }
    Ok(())
}

// In storage_key():
if user_id_bytes.len() > 255 {
    panic!("User ID exceeds maximum storage key length");  // Or return Error
}
```

---

### 45. Unvalidated extract_claims_unverified Function

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/jwt_auth.rs` |
| **Lines** | 114-129 |
| **CVSS Score** | 6.0 (Medium) |

**Vulnerable Code:**
```rust
/// Extract claims from a JWT token without full validation.
///
/// **WARNING**: This does NOT verify the signature! Only use for debugging
/// or when you need to inspect a token before validation.
pub fn extract_claims_unverified(token: &str) -> AuthResult<JwtClaims> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.insecure_disable_signature_validation(); // DANGEROUS
    validation.validate_exp = false;
    // ...
}
```

**Impact:**
- If this function is used in any authentication flow (accidentally or intentionally)
- Attackers can craft arbitrary JWT claims without knowing the secret
- Complete authentication bypass
- Function is public and could be misused

**Recommendation:**
```rust
/// FOR DEBUGGING ONLY - DO NOT USE IN PRODUCTION
#[cfg(debug_assertions)]
pub fn extract_claims_unverified(token: &str) -> AuthResult<JwtClaims> {
    // Only available in debug builds
}

// Or mark as unsafe:
#[deprecated(note = "DANGEROUS: Do not use in authentication flows")]
pub unsafe fn extract_claims_unverified(token: &str) -> AuthResult<JwtClaims>
```

---

### 46. OAuth RS256 Not Implemented - Fallback to Error

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/oauth.rs` |
| **Lines** | 80-85 |
| **CVSS Score** | 4.0 (Medium) |

**Vulnerable Code:**
```rust
Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => {
    // For RS256, we would need JWKS support
    // For now, return error - JWKS support can be added later
    return Err(AuthError::MalformedAuthorization(
        "RS256 tokens require JWKS support (not yet implemented)".to_string(),
    ));
}
```

**Impact:**
- Most OAuth providers (Google, GitHub, Azure) use RS256 by default
- Users trying to configure OAuth will hit this error in production
- May lead to workarounds that weaken security (forcing HS256)
- Documentation claims OAuth support but RS256 isn't functional

**Recommendation:**
- Implement JWKS support for RS256/RS384/RS512
- Or document this limitation clearly in OAuth setup guide
- Add integration tests that verify OAuth actually works with real providers

---

### 47. Impersonation Without Subject User Existence Check

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/impersonation.rs` |
| **Lines** | 62-67 |
| **CVSS Score** | 5.5 (Medium) |

**Vulnerable Pattern:**
```rust
pub fn new(
    actor_user_id: UserId,
    actor_role: Role,
    subject_user_id: UserId,  // No validation that this user exists
    session_id: String,
    origin: ImpersonationOrigin,
) -> Self {
    Self { ... }
}
```

**Impact:**
- Service/DBA can impersonate non-existent users
- Creates phantom operations in user tables that can't be attributed
- Audit logs show impersonation of `user_does_not_exist`
- Could be used to hide malicious activity

**Recommendation:**
```rust
pub async fn validate_impersonation(
    &self,
    user_repo: &Arc<dyn UserRepository>,
) -> AuthResult<()> {
    // Verify subject user exists
    let subject = user_repo.get_user_by_username(
        &self.subject_user_id.as_str()
    ).await?;
    
    if subject.deleted_at.is_some() {
        return Err(AuthError::ImpersonationFailed(
            "Cannot impersonate deleted user".to_string()
        ));
    }
    Ok(())
}
```

---

### 48. No Rate Limiting on Password Authentication

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/unified.rs` |
| **Lines** | Throughout authenticate_username_password |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Pattern:**
- No tracking of failed authentication attempts
- No lockout after N failures
- No delay between attempts
- Bcrypt provides some protection but brute force still possible

**Impact:**
- Attackers can brute force passwords without restriction
- No account lockout mechanism
- Dictionary attacks viable against weak passwords
- No alerts for security team

**Recommendation:**
```rust
// Track failed attempts per user/IP
struct AuthAttemptTracker {
    attempts: DashMap<String, (u32, Instant)>,  // (count, first_attempt_time)
}

impl AuthAttemptTracker {
    fn check_and_record(&self, key: &str) -> Result<(), AuthError> {
        let entry = self.attempts.entry(key.to_string());
        match entry {
            Entry::Occupied(mut e) => {
                let (count, first_time) = e.get();
                if first_time.elapsed() < Duration::from_secs(300) && *count >= 5 {
                    return Err(AuthError::TooManyAttempts);
                }
                e.insert((*count + 1, *first_time));
            }
            Entry::Vacant(e) => {
                e.insert((1, Instant::now()));
            }
        }
        Ok(())
    }
}
```

---

### 49. RBAC Bypass via Direct System Table Access

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/rbac.rs` |
| **Lines** | 22-26 |
| **CVSS Score** | 6.5 (Medium) |

**Vulnerable Pattern:**
```rust
pub fn can_access_table_type(role: Role, table_type: TableType) -> bool {
    match role {
        Role::System | Role::Dba => true,
        Role::Service | Role::User => {
            !matches!(table_type, TableType::System)  // Blocks system tables
        }
    }
}
// But this check may not be applied to all code paths
```

**Impact:**
- RBAC checks exist but may not be consistently applied
- Need to verify ALL SQL execution paths check permissions
- Direct EntityStore access might bypass RBAC

**Recommendation:**
- Audit all code paths that access system tables
- Add centralized permission check at EntityStore layer
- Add integration tests specifically for RBAC bypass attempts

---

### 50. Connection Info Spoofing via Missing Validation

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/ip_extractor.rs` |
| **Lines** | 35-50 |
| **CVSS Score** | 5.0 (Medium) |

**Vulnerable Code:**
```rust
if let Some(forwarded_for) = req.headers().get("X-Forwarded-For") {
    if let Ok(header_value) = forwarded_for.to_str() {
        let first_ip = header_value.split(',').next().unwrap_or("").trim();
        
        if is_localhost_address(first_ip) {
            warn!("Security: Rejected localhost value...");
            // Falls through to peer_addr
        } else if !first_ip.is_empty() {
            return ConnectionInfo::new(Some(first_ip.to_string()));
            // ⚠️ Trusts X-Forwarded-For for non-localhost IPs!
        }
    }
}
```

**Impact:**
- Attacker sends `X-Forwarded-For: 10.0.0.1` (internal IP)
- Server trusts this as the real client IP
- IP-based access controls or rate limiting bypassed
- Audit logs show wrong IP

**Recommendation:**
```rust
// Only trust X-Forwarded-For if request comes from known proxy
fn extract_client_ip_secure(req: &HttpRequest, config: &ProxyConfig) -> ConnectionInfo {
    let peer_addr = req.peer_addr().map(|a| a.ip().to_string());
    
    // Only trust headers if peer is a trusted proxy
    if let Some(peer) = &peer_addr {
        if !config.trusted_proxies.contains(peer) {
            return ConnectionInfo::new(peer_addr);  // Don't trust headers
        }
    }
    
    // Now safe to trust X-Forwarded-For
    // ...
}
```

---

## 🛡️ Updated Remediation Priority (Including Phase 5)

### Immediate (Critical + High)

1. **SQL Injection in WASM SDK** - Validate all user inputs in `link/src/wasm.rs`
2. **Hardcoded JWT Secret** - Fail startup if not explicitly configured
3. **CORS Misconfiguration** - Configure specific allowed origins
4. **JWT Role Escalation** (NEW #41) - Don't allow role override via claims
5. **Empty Issuer List** (NEW #42) - Fail closed on empty trusted issuers
6. **Path Traversal via Namespace/Table Names** (NEW #51) - **CRITICAL** - Validate all identifier inputs
7. **No Input Validation on Type-Safe Wrappers** (NEW #52) - Add validation to all ID types
8. **TLS/HTTPS** - Add TLS support or document reverse proxy requirement
9. **WebSocket Origin Validation** - Validate Origin header
10. **Cross-User Parquet Access** (NEW #43) - Validate file paths
11. **RocksDB Unsafe Pointer** - Replace with proper synchronization
12. **Username Enumeration via Logs** (NEW #53) - Anonymize usernames in logs

### Short-term (Medium)

13. **User ID Collision** (NEW #44) - Validate and limit user ID length
14. **Unverified Claims Function** (NEW #45) - Restrict to debug builds only
15. **Impersonation Validation** (NEW #47) - Verify subject user exists
16. **Auth Rate Limiting** (NEW #48) - Add brute force protection
17. **RBAC Enforcement** (NEW #49) - Audit all code paths
18. **Integer Overflow in Retention** (NEW #54) - Use checked arithmetic
19. **Shell Command Pattern** (NEW #55) - Use native Rust APIs instead
20. **Unbounded JSON Deserialization** (NEW #56) - Add size/depth limits
21. **Token Bucket Memory Exhaustion** (NEW #57) - Limit tracked IPs
22. **Live Query Limits** - Add per-connection subscription limits
23. **WebSocket Message Size** - Limit maximum message size

### Long-term (Low + Documentation)

24. **OAuth RS256** (NEW #46) - Implement JWKS support or document limitation
25. **Connection Info Spoofing** (NEW #50) - Only trust headers from known proxies
26. **Timing Attack** - Use constant-time password verification
27. Remove secrets from source control
28. Clean up unnecessary unsafe impls
29. Limit version disclosure

> **IMPORTANT**: When implementing security fixes, ensure they don't break localhost development and CI/CD test suites. Many tests run against `localhost:8080` without TLS or with default credentials.

### Environment-Aware Security Configuration

Implement fixes using environment detection to maintain test compatibility:

```rust
/// Determine if running in development/test mode
fn is_development_mode() -> bool {
    std::env::var("KALAMDB_ENV")
        .map(|v| v == "development" || v == "test")
        .unwrap_or(false)
    || std::env::var("CARGO_TEST").is_ok()  // Running under cargo test
    || cfg!(test)
}
```

### Fix Implementation Guidelines

#### 1. JWT Secret (Critical #2)

**Production-safe fix that preserves test compatibility:**

```rust
// In unified.rs
let secret = std::env::var("KALAMDB_JWT_SECRET").ok();

match (secret, is_development_mode()) {
    (Some(s), _) => s,  // Explicit secret always used
    (None, true) => {
        warn!("Using default JWT secret - DEVELOPMENT MODE ONLY");
        "kalamdb-dev-secret-key-change-in-production".to_string()
    },
    (None, false) => {
        panic!("KALAMDB_JWT_SECRET must be set in production mode. \
                Set KALAMDB_ENV=development for local testing.");
    }
}
```

**Test configuration:**
```bash
# In test setup or CI
export KALAMDB_ENV=development
# OR
export KALAMDB_JWT_SECRET=test-secret-for-ci-minimum-32-chars
```

#### 2. CORS Configuration (Critical #3)

**Production-safe fix:**

```rust
pub fn build_cors(config: &ServerConfig) -> Cors {
    if is_development_mode() || config.cors.allow_any_origin {
        // Development: permissive CORS for localhost testing
        warn!("CORS: Allowing any origin - DEVELOPMENT MODE");
        Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .supports_credentials()
            .max_age(3600)
    } else {
        // Production: strict CORS
        let mut cors = Cors::default()
            .allowed_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"])
            .allowed_headers(vec![
                header::AUTHORIZATION,
                header::CONTENT_TYPE,
                header::ACCEPT,
            ])
            .supports_credentials()
            .max_age(3600);
        
        for origin in &config.cors.allowed_origins {
            cors = cors.allowed_origin(origin);
        }
        cors
    }
}
```

**Test compatibility:**
- Tests running on `localhost` will work with `KALAMDB_ENV=development`
- CI can set specific origins if needed

#### 3. TLS/HTTPS (High #6)

**Don't enforce TLS in development:**

```rust
// In lifecycle.rs
if !is_development_mode() && !config.server.tls.enabled {
    warn!(
        "⚠️  TLS is DISABLED in production mode! \
         Set [server.tls] or use a reverse proxy with TLS termination."
    );
}

// Allow HTTP for localhost testing
let server = if config.server.tls.enabled {
    server.bind_rustls(&bind_addr, tls_config)?
} else {
    if is_development_mode() {
        info!("TLS disabled - development mode");
    }
    server.bind(&bind_addr)?
};
```

#### 4. WebSocket Origin Validation (High #7)

**Allow localhost origins in development:**

```rust
fn validate_websocket_origin(req: &HttpRequest, config: &ServerConfig) -> bool {
    let origin = req.headers().get("Origin")
        .and_then(|h| h.to_str().ok());
    
    match origin {
        None => true,  // No origin = same-origin or non-browser client
        Some(origin) => {
            // Always allow localhost in development
            if is_development_mode() {
                let localhost_patterns = [
                    "http://localhost",
                    "http://127.0.0.1",
                    "http://[::1]",
                    "http://0.0.0.0",
                ];
                if localhost_patterns.iter().any(|p| origin.starts_with(p)) {
                    return true;
                }
            }
            
            // Check configured allowed origins
            config.websocket.allowed_origins.iter()
                .any(|allowed| origin == allowed || allowed == "*")
        }
    }
}
```

#### 5. Empty Password on Localhost (Medium #13)

**This is intentional for development - just add logging:**

```rust
if is_localhost && user.password_hash.is_empty() {
    if is_development_mode() {
        debug!("Accepting empty password for localhost system user - development mode");
    } else {
        warn!(
            "Empty password accepted for localhost system user. \
             This should not happen in production!"
        );
    }
}
```

#### 6. SQL Injection in WASM (Critical #1)

**This fix should apply everywhere - no development bypass needed:**

```rust
/// Validate table name is safe SQL identifier
fn validate_table_name(name: &str) -> Result<&str, JsValue> {
    // Allow: namespace.table or just table
    let pattern = regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")
        .unwrap();
    
    if pattern.is_match(name) {
        Ok(name)
    } else {
        Err(JsValue::from_str(&format!(
            "Invalid table name '{}'. Must be alphanumeric with underscores.",
            name
        )))
    }
}

/// Validate row ID is safe (integer or UUID)
fn validate_row_id(id: &str) -> Result<&str, JsValue> {
    // Accept integers
    if id.parse::<i64>().is_ok() {
        return Ok(id);
    }
    // Accept UUIDs
    if uuid::Uuid::parse_str(id).is_ok() {
        return Ok(id);
    }
    // Accept quoted strings (for string PKs)
    if id.starts_with('\'') && id.ends_with('\'') && !id[1..id.len()-1].contains('\'') {
        return Ok(id);
    }
    
    Err(JsValue::from_str(&format!(
        "Invalid row ID '{}'. Must be integer, UUID, or quoted string.",
        id
    )))
}
```

### Test Environment Setup

Add to your test setup (e.g., `tests/common/mod.rs`):

```rust
/// Initialize test environment with development-safe security settings
pub fn setup_test_environment() {
    std::env::set_var("KALAMDB_ENV", "test");
    std::env::set_var("KALAMDB_JWT_SECRET", "test-jwt-secret-for-automated-testing-min-32-chars");
    
    // Ensure tests know they're in test mode
    std::env::set_var("CARGO_TEST", "1");
}
```

### CI/CD Configuration

```yaml
# .github/workflows/test.yml
env:
  KALAMDB_ENV: test
  KALAMDB_JWT_SECRET: ${{ secrets.TEST_JWT_SECRET }}
  KALAMDB_LOG_LEVEL: warn,kalamdb=debug

jobs:
  test:
    steps:
      - name: Run tests
        run: cargo test --all
        env:
          KALAMDB_ENV: test
```

### Configuration File Example

```toml
# server.toml - Development defaults
[security]
# Set to "production" in prod, enables strict security checks
environment = "development"  

[cors]
# Only used when environment != "development"
allowed_origins = ["https://your-app.com"]
allow_any_origin = false  # Set true only for development

[server.tls]
enabled = false  # Enable in production or use reverse proxy

[auth]
# Will fail startup in production if not changed
jwt_secret = "CHANGE_ME_IN_PRODUCTION"
allow_empty_localhost_password = true  # Disable in production
```

### Smoke Test Compatibility Checklist

Before merging security fixes, verify:

- [ ] `cargo test` passes with `KALAMDB_ENV=test`
- [ ] Smoke tests pass: `cd cli && cargo test --test smoke`
- [ ] Integration tests pass: `cd backend && cargo test --test integration`
- [ ] Server starts without JWT secret in development mode
- [ ] Server FAILS to start without JWT secret when `KALAMDB_ENV=production`
- [ ] WebSocket connections work from localhost
- [ ] Basic auth works with test credentials on localhost
- [ ] CORS allows localhost origins in development mode

---

## Phase 5: Production Readiness Audit Findings

### 51. Path Traversal via Namespace/Table Names (CRITICAL)

| Attribute | Value |
|-----------|-------|
| **Files** | `kalamdb-filestore/src/path_utils.rs`, `kalamdb-commons/src/models/ids/namespace_id.rs`, `table_name.rs` |
| **CVSS Score** | 9.1 (Critical) |

**Vulnerable Code:**
```rust
// namespace_id.rs - No validation on input
impl NamespaceId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())  // ⚠️ Accepts any string including "../"
    }
}

// path_utils.rs - Uses unsanitized input in paths
pub fn namespace_path(storage_id: &StorageId, namespace_id: &NamespaceId) -> PathBuf {
    Self::storage_base_path(storage_id).join(namespace_id.as_str())  // ⚠️ Path traversal
}
```

**Attack Vector:**
- Create namespace: `CREATE NAMESPACE "../../../etc"` 
- Create table: `CREATE TABLE "../../../etc".passwd (data TEXT)`
- Could read/write files outside storage directory

**Impact:**
- Arbitrary file read/write on server filesystem
- Configuration file disclosure
- Potential remote code execution via file overwrite

**Recommendation:**
```rust
impl NamespaceId {
    pub fn new(id: impl Into<String>) -> Result<Self, ValidationError> {
        let id = id.into();
        // Validate: alphanumeric, underscores, hyphens only
        if !id.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
            return Err(ValidationError::InvalidCharacters);
        }
        // No path separators
        if id.contains('/') || id.contains('\\') || id.contains("..") {
            return Err(ValidationError::PathTraversal);
        }
        // Length limits
        if id.is_empty() || id.len() > 64 {
            return Err(ValidationError::InvalidLength);
        }
        Ok(Self(id))
    }
}
```

---

### 52. No Input Validation on Type-Safe Wrappers (HIGH)

| Attribute | Value |
|-----------|-------|
| **Files** | `namespace_id.rs`, `table_name.rs`, `user_id.rs`, `storage_id.rs` |
| **CVSS Score** | 7.5 (High) |

**Vulnerable Code:**
```rust
// All type-safe wrappers have the same pattern - no validation
impl From<String> for NamespaceId {
    fn from(s: String) -> Self {
        Self(s)  // ⚠️ Accepts ANY string
    }
}

impl From<&str> for TableName {
    fn from(s: &str) -> Self {
        Self(s.to_string())  // ⚠️ Accepts ANY string
    }
}
```

**Attack Vectors:**
- Null bytes: `table\x00name` could cause string truncation issues
- Unicode exploits: Homoglyph attacks, zero-width characters
- Control characters: Could break log parsing or monitoring
- Very long strings: Memory exhaustion

**Impact:**
- Log injection attacks
- Database corruption
- Security monitoring bypass
- Denial of service

**Recommendation:**
- Add validation to all `new()`, `From<String>`, `From<&str>` implementations
- Validate: ASCII alphanumeric + underscore, no control characters
- Enforce maximum length (64-128 characters)
- Reject reserved names (system, root, admin, etc.)

---

### 53. Username Enumeration via Auth Logs (HIGH)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-auth/src/unified.rs` |
| **Lines** | 173, 206, 227, 244 |
| **CVSS Score** | 5.3 (Medium) but elevated due to production exposure |

**Vulnerable Code:**
```rust
// Line 173
warn!("Attempt to authenticate deleted user: {}", username);

// Line 206
warn!("Invalid password for system user: {}", username);

// Line 227
warn!("Invalid password for remote system user: {}", username);

// Line 244
warn!("Invalid password for user: {}", username);
```

**Impact:**
- Usernames are logged in plaintext on failed auth attempts
- Logs may be exposed via log aggregation, SIEM, or misconfigured access
- Enables targeted attacks against known usernames
- GDPR/privacy implications - usernames are PII

**Recommendation:**
```rust
// Use hashed/truncated identifier instead
fn anonymize_username(username: &str) -> String {
    use sha2::{Sha256, Digest};
    let hash = Sha256::digest(username.as_bytes());
    format!("user_{}", hex::encode(&hash[..4]))  // "user_a1b2c3d4"
}

warn!("Invalid password attempt for: {}", anonymize_username(&username));
```

---

### 54. Integer Overflow in Retention Calculation (MEDIUM)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/jobs/executors/retention.rs` |
| **Line** | 94 |
| **CVSS Score** | 4.3 (Medium) |

**Vulnerable Code:**
```rust
let retention_ms = (retention_hours * 3600 * 1000) as i64;
// If retention_hours is u64 > 2^31 / 3600000, this overflows
```

**Attack Vector:**
- Set extremely high retention value via configuration or SQL
- Overflow causes negative or zero retention period
- Could cause premature data deletion or infinite retention

**Impact:**
- Data loss (premature deletion)
- Storage exhaustion (no deletion)

**Recommendation:**
```rust
let retention_ms = retention_hours
    .checked_mul(3600)
    .and_then(|h| h.checked_mul(1000))
    .map(|ms| ms as i64)
    .ok_or(RetentionError::InvalidDuration)?;
```

---

### 55. Shell Command Execution Pattern (HIGH)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/src/main.rs` |
| **Line** | 76 |
| **CVSS Score** | 6.5 (Medium) - Limited impact but dangerous pattern |

**Vulnerable Code:**
```rust
#[cfg(unix)]
{
    use std::process::Command;
    if let Ok(output) = Command::new("sh").arg("-c").arg("ulimit -n").output() {
        // ...
    }
}
```

**Risk:**
- While this specific case is safe (hardcoded command), the pattern establishes a precedent
- Any future command with user input would be exploitable
- The `sh -c` pattern is especially dangerous

**Impact:**
- Current: Minimal (hardcoded command)
- Future: Any extension with user input could cause RCE

**Recommendation:**
- Use Rust's built-in resource limit checking instead:
```rust
#[cfg(unix)]
{
    use nix::sys::resource::{getrlimit, Resource};
    if let Ok((soft, _hard)) = getrlimit(Resource::RLIMIT_NOFILE) {
        info!("File descriptor limit: {}", soft);
    }
}
```

---

### 56. Unbounded serde_json Deserialization (MEDIUM)

| Attribute | Value |
|-----------|-------|
| **Files** | Multiple (ws_handler.rs, unified.rs, etc.) |
| **CVSS Score** | 4.3 (Medium) |

**Vulnerable Patterns:**
```rust
// ws_handler.rs
let msg: ClientMessage = serde_json::from_str(text)?;

// unified.rs
serde_json::from_str::<serde_json::Value>(&payload_str)
```

**Attack Vector:**
- Deeply nested JSON: `{"a":{"a":{"a":{"a":...}}}}` (1000+ levels)
- Large arrays: `[1,1,1,1,1,...]` (millions of elements)
- Causes stack overflow or memory exhaustion

**Impact:**
- Denial of service
- Server crash
- Memory exhaustion

**Recommendation:**
- Add depth/size limits before deserialization:
```rust
// Limit message size before parsing
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;  // 1MB
if text.len() > MAX_MESSAGE_SIZE {
    return Err("Message too large".into());
}

// Use serde_json with recursion limit (feature: arbitrary_precision)
let msg: ClientMessage = serde_json::from_str(text)
    .map_err(|e| format!("Invalid JSON: {}", e))?;
```

---

### 57. Token Bucket State Memory Exhaustion (MEDIUM)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-api/src/rate_limiter.rs` |
| **CVSS Score** | 4.3 (Medium) |

**Vulnerable Code:**
```rust
pub struct ConnectionGuard {
    ip_states: Arc<RwLock<HashMap<IpAddr, IpState>>>,
}

// No limit on number of tracked IPs
let state = states
    .entry(ip)
    .or_insert_with(|| IpState::new(&self.config));
```

**Attack Vector:**
- Attacker sends requests from many spoofed IPs (IPv6 space is huge)
- Each unique IP creates a new entry in the HashMap
- Memory grows unbounded

**Impact:**
- Memory exhaustion
- Server crash
- Denial of service

**Recommendation:**
```rust
impl ConnectionGuard {
    const MAX_TRACKED_IPS: usize = 100_000;

    pub fn check_request(&self, ip: IpAddr, body_size: Option<usize>) -> ConnectionGuardResult {
        let mut states = self.ip_states.write().unwrap();
        
        // Limit total tracked IPs
        if states.len() >= Self::MAX_TRACKED_IPS && !states.contains_key(&ip) {
            // Either reject, or evict oldest entry
            self.cleanup_oldest_entries(&mut states);
        }
        // ...
    }
}
```

---

## Phase 6: Final Review Audit Findings

### 58. Rate Limit Bypass via X-Real-IP Spoofing (HIGH)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/src/middleware.rs` |
| **Line** | 285 |
| **CVSS Score** | 7.5 (High) |

**Vulnerable Code:**
```rust
// middleware.rs
fn extract_client_ip(req: &ServiceRequest) -> IpAddr {
    // ...
    // Try X-Real-IP header (nginx style)
    if let Some(real_ip) = req.headers().get("X-Real-IP") {
        if let Ok(real_ip_str) = real_ip.to_str() {
            if let Ok(ip) = real_ip_str.trim().parse::<IpAddr>() {
                return ip; // ⚠️ Trusts header blindly
            }
        }
    }
    // ...
}

// ConnectionProtection::check_request
if ip.is_loopback() {
    return ConnectionGuardResult::Allowed; // ⚠️ Bypasses rate limit
}
```

**Attack Vector:**
- Attacker sends request with `X-Real-IP: 127.0.0.1`
- `extract_client_ip` returns `127.0.0.1`
- `check_request` sees loopback address and bypasses all rate limits
- Allows DoS attacks even if rate limiting is enabled

**Impact:**
- Denial of Service (DoS)
- Resource exhaustion
- Bypass of IP bans

**Recommendation:**
- Use `kalamdb_auth::extract_client_ip_secure` which validates headers
- Or implement similar validation in `middleware.rs`:
```rust
if let Some(real_ip) = req.headers().get("X-Real-IP") {
    // ...
    if is_localhost_address(ip_str) {
        warn!("Rejected localhost value in X-Real-IP");
        // Fallback to peer_addr
    }
}
```

---

### 59. Fragile SQL Parsing in Live Queries (HIGH)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/live/query_parser.rs` |
| **Line** | 14 |
| **CVSS Score** | 7.5 (High) |

**Vulnerable Code:**
```rust
pub fn extract_table_name(query: &str) -> Result<String, KalamDbError> {
    let query_upper = query.to_uppercase();
    let from_pos = query_upper.find(" FROM ").ok_or_else(|| ...)?;
    let after_from = &query[(from_pos + 6)..];
    // ...
}
```

**Attack Vector:**
- SQL Injection / Parser Differential
- Query: `SELECT ' FROM ' FROM secret_table`
- Parser finds first ` FROM ` (inside string literal)
- Extracts wrong table name
- Could bypass permission checks if one parser is used for auth and another for execution

**Impact:**
- Authorization bypass
- Potential SQL injection if extracted parts are re-assembled

**Recommendation:**
- Use `datafusion::sql::parser::DFParser` or `sqlparser` crate for robust parsing
- Do not use string matching for SQL parsing

---

### 60. Stack Overflow in Filter Evaluation (MEDIUM)

| Attribute | Value |
|-----------|-------|
| **File** | `backend/crates/kalamdb-core/src/live/filter_eval.rs` |
| **Line** | 119 |
| **CVSS Score** | 5.3 (Medium) |

**Vulnerable Code:**
```rust
fn evaluate_expr(expr: &Expr, row_data: &Row) -> Result<bool, KalamDbError> {
    match expr {
        // ...
        Expr::Nested(inner) => evaluate_expr(inner, row_data), // ⚠️ Unbounded recursion
        // ...
    }
}
```

**Attack Vector:**
- Attacker registers subscription with deeply nested WHERE clause
- `WHERE (((... 10000 times ...)))`
- `evaluate_expr` recurses until stack overflow
- Crashes the server thread/process

**Impact:**
- Denial of Service (DoS)
- Server crash

**Recommendation:**
- Implement recursion depth limit in `evaluate_expr`
- Or validate expression depth during subscription registration
```rust
const MAX_DEPTH: usize = 100;
fn evaluate_expr(expr: &Expr, row_data: &Row, depth: usize) -> Result<bool, KalamDbError> {
    if depth > MAX_DEPTH {
        return Err(KalamDbError::InvalidOperation("Expression too deep".to_string()));
    }
    // ...
    evaluate_expr(inner, row_data, depth + 1)
}
```

---

## 🚀 Production Hardening Checklist

Before deploying KalamDB to production, complete the following security hardening steps:

### Pre-Deployment (Required)

- [ ] **Set JWT Secret**: Configure `KALAMDB_JWT_SECRET` environment variable (minimum 32 chars)
- [ ] **Set Root Password**: Run `ALTER USER root SET PASSWORD 'your-secure-password'`
- [ ] **Configure CORS**: Set `allowed_origins` to specific domains in `server.toml`
- [ ] **Enable TLS**: Either enable TLS directly or use reverse proxy (nginx/traefik)
- [ ] **Set Environment**: `KALAMDB_ENV=production`
- [ ] **Increase File Descriptor Limit**: `ulimit -n 65536` or configure in systemd
- [ ] **Configure Logging**: Set `level = "info"` or `"warn"` (not `"debug"`)

### Configuration Review

- [ ] **Review server.toml**:
  ```toml
  [auth]
  jwt_secret = "your-32-char-minimum-secret"
  enforce_password_complexity = true
  allow_empty_localhost_password = false
  
  [cors]
  allowed_origins = ["https://your-app.com"]
  allow_any_origin = false
  
  [rate_limit]
  enable_connection_protection = true
  max_connections_per_ip = 100
  max_requests_per_ip_per_sec = 100
  ban_duration_seconds = 300
  ```

### Network Security

- [ ] **Firewall**: Only expose port 8080 (or configured port) if needed externally
- [ ] **Reverse Proxy**: Deploy behind nginx/traefik for additional protection
- [ ] **IP Whitelisting**: Consider restricting access to known IP ranges
- [ ] **Load Balancer**: Configure health checks and connection limits

### Monitoring & Alerts

- [ ] **Log Aggregation**: Forward logs to SIEM/log management
- [ ] **Alerting**: Set up alerts for:
  - [ ] Failed authentication attempts (>10/min)
  - [ ] Rate limit hits
  - [ ] Server errors (5xx responses)
  - [ ] Memory/CPU spikes
- [ ] **Metrics**: Enable and monitor Prometheus metrics endpoint

### Backup & Recovery

- [ ] **Backup Strategy**: Configure automated backups of RocksDB and Parquet files
- [ ] **Backup Testing**: Verify backups can be restored
- [ ] **Point-in-Time Recovery**: Configure WAL retention for PITR

### Known Limitations (Document for Operators)

| Issue | Impact | Mitigation |
|-------|--------|------------|
| OAuth RS256 not implemented | Can't use Google/GitHub RS256 tokens directly | Use proxy that converts to HS256, or wait for JWKS support |
| No built-in encryption at rest | Data on disk is unencrypted | Use encrypted filesystem (LUKS, dm-crypt) |
| Single-node only | No HA/clustering | Deploy multiple instances with external load balancer |

---

## References

- [OWASP Top 10](https://owasp.org/Top10/)
- [CWE/SANS Top 25](https://cwe.mitre.org/top25/)
- [Rust Security Guidelines](https://anssi-fr.github.io/rust-guide/)
- [CORS Security](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS)
- [JWT Best Practices](https://datatracker.ietf.org/doc/html/rfc8725)
