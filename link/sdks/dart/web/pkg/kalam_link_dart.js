/* @ts-self-types="./kalam_link_dart.d.ts" */

/**
 * WASM-compatible KalamDB client with auto-reconnection support
 *
 * Supports multiple authentication methods:
 * - Basic Auth: `new KalamClient(url, username, password)`
 * - JWT Token: `KalamClient.withJwt(url, token)`
 * - Anonymous: `KalamClient.anonymous(url)`
 * - Dynamic Auth: `KalamClient.anonymous(url)` + `setAuthProvider(async () => ({ jwt: { token } }))`
 *
 * # Example (JavaScript)
 * ```js
 * import init, { KalamClient, KalamClientWithJwt, KalamClientAnonymous } from './pkg/kalam_link.js';
 *
 * await init();
 *
 * // Basic Auth (username/password)
 * const client = new KalamClient(
 *   "http://localhost:8080",
 *   "username",
 *   "password"
 * );
 *
 * // JWT Token Auth
 * const jwtClient = KalamClient.withJwt(
 *   "http://localhost:8080",
 *   "eyJhbGciOiJIUzI1NiIs..."
 * );
 *
 * // Anonymous (localhost bypass)
 * const anonClient = KalamClient.anonymous("http://localhost:8080");
 *
 * // Dynamic async auth provider (e.g. refresh token flow)
 * const dynClient = KalamClient.anonymous("http://localhost:8080");
 * dynClient.setAuthProvider(async () => {
 *   const token = await myApp.getOrRefreshToken();
 *   return { jwt: { token } };
 * });
 *
 * // Configure auto-reconnect (enabled by default)
 * client.setAutoReconnect(true);
 * client.setReconnectDelay(1000, 30000);
 *
 * // WebSocket connects automatically on first subscribe (wsLazyConnect=true by default)
 * const subId = await client.subscribeWithSql(
 *   "SELECT * FROM chat.messages",
 *   JSON.stringify({
 *     batch_size: 100,
 *     include_old_values: true
 *   }),
 *   (event) => console.log('Change:', event)
 * );
 * ```
 */
export class KalamClient {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(KalamClient.prototype);
        obj.__wbg_ptr = ptr;
        KalamClientFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        KalamClientFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_kalamclient_free(ptr, 0);
    }
    /**
     * Acknowledge processed messages on a topic
     *
     * # Arguments
     * * `topic` - Topic name
     * * `group_id` - Consumer group ID
     * * `partition_id` - Partition ID
     * * `upto_offset` - Acknowledge all messages up to and including this offset
     *
     * # Returns
     * A type-safe AckResponse as JsValue
     *
     * # Example (JavaScript)
     * ```js
     * const result = await client.ack("chat.new_messages", "my-group", 0, 42);
     * console.log(result.success, result.acknowledged_offset);
     * ```
     * @param {string} topic
     * @param {string} group_id
     * @param {number} partition_id
     * @param {bigint} upto_offset
     * @returns {Promise<any>}
     */
    ack(topic, group_id, partition_id, upto_offset) {
        const ptr0 = passStringToWasm0(topic, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(group_id, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_ack(this.__wbg_ptr, ptr0, len0, ptr1, len1, partition_id, upto_offset);
        return takeObject(ret);
    }
    /**
     * Create a new KalamDB client with no authentication
     *
     * Useful for localhost connections where the server allows
     * unauthenticated access, or for development/testing scenarios.
     *
     * # Arguments
     * * `url` - KalamDB server URL (required, e.g., "http://localhost:8080")
     *
     * # Errors
     * Returns JsValue error if url is empty
     *
     * # Example (JavaScript)
     * ```js
     * const client = KalamClient.anonymous("http://localhost:8080");
     * await client.connect();
     * ```
     * @param {string} url
     * @returns {KalamClient}
     */
    static anonymous(url) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(url, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            wasm.kalamclient_anonymous(retptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return KalamClient.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Clear a previously set auth provider, reverting to the static auth
     * configured at construction time.
     */
    clearAuthProvider() {
        wasm.kalamclient_clearAuthProvider(this.__wbg_ptr);
    }
    /**
     *
     * # Returns
     * Promise that resolves when connection is established and authenticated
     * @returns {Promise<void>}
     */
    connect() {
        const ret = wasm.kalamclient_connect(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Consume messages from a topic via HTTP API
     *
     * # Arguments
     * * `options` - Type-safe ConsumeRequest with topic, group_id, batch_size, etc.
     *
     * # Returns
     * A type-safe ConsumeResponse as JsValue (includes messages, next_offset, has_more)
     *
     * # Example (JavaScript)
     * ```js
     * const result = await client.consume({
     *   topic: "chat.new_messages",
     *   group_id: "my-consumer-group",
     *   batch_size: 10,
     *   start: "latest",
     * });
     * for (const msg of result.messages) {
     *   console.log(msg.value);
     * }
     * ```
     * @param {any} options
     * @returns {Promise<any>}
     */
    consume(options) {
        const ret = wasm.kalamclient_consume(this.__wbg_ptr, addHeapObject(options));
        return takeObject(ret);
    }
    /**
     * Delete a row from a table (T049, T063H)
     *
     * # Arguments
     * * `table_name` - Name of the table
     * * `row_id` - ID of the row to delete
     * @param {string} table_name
     * @param {string} row_id
     * @returns {Promise<void>}
     */
    delete(table_name, row_id) {
        const ptr0 = passStringToWasm0(table_name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(row_id, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_delete(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return takeObject(ret);
    }
    /**
     * Disconnect from KalamDB server (T046, T063E)
     * @returns {Promise<void>}
     */
    disconnect() {
        const ret = wasm.kalamclient_disconnect(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Get the current authentication type
     *
     * Returns one of: "basic", "jwt", or "none"
     * @returns {string}
     */
    getAuthType() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.kalamclient_getAuthType(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export5(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Get the last received seq_id for a subscription
     *
     * Useful for debugging or manual resumption tracking
     * @param {string} subscription_id
     * @returns {string | undefined}
     */
    getLastSeqId(subscription_id) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(subscription_id, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            wasm.kalamclient_getLastSeqId(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            let v2;
            if (r0 !== 0) {
                v2 = getStringFromWasm0(r0, r1).slice();
                wasm.__wbindgen_export5(r0, r1 * 1, 1);
            }
            return v2;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Get the current reconnection attempt count
     * @returns {number}
     */
    getReconnectAttempts() {
        const ret = wasm.kalamclient_getReconnectAttempts(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Return a JSON array describing all active subscriptions.
     *
     * Each element contains `id`, `query`, `lastSeqId`, `lastEventTimeMs`,
     * `createdAtMs`, and `closed`.  The WASM layer surfaces its own
     * reconnection state, so `lastSeqId` reflects the latest seq received.
     *
     * # Example (JavaScript)
     * ```js
     * const subs = client.getSubscriptions();
     * // subs = [{ id: "sub-abc", query: "SELECT ...", lastSeqId: "123", ... }]
     * ```
     * @returns {any}
     */
    getSubscriptions() {
        const ret = wasm.kalamclient_getSubscriptions(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Insert data into a table (T048, T063G)
     *
     * # Arguments
     * * `table_name` - Name of the table to insert into
     * * `data` - JSON string representing the row data
     *
     * # Example (JavaScript)
     * ```js
     * await client.insert("todos", JSON.stringify({
     *   title: "Buy groceries",
     *   completed: false
     * }));
     * ```
     * @param {string} table_name
     * @param {string} data
     * @returns {Promise<string>}
     */
    insert(table_name, data) {
        const ptr0 = passStringToWasm0(table_name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(data, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_insert(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return takeObject(ret);
    }
    /**
     * Check if client is currently connected (T047)
     *
     * # Returns
     * true if WebSocket connection is active, false otherwise
     * @returns {boolean}
     */
    isConnected() {
        const ret = wasm.kalamclient_isConnected(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Check if currently reconnecting
     * @returns {boolean}
     */
    isReconnecting() {
        const ret = wasm.kalamclient_isReconnecting(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Subscribe to a SQL query and receive materialized live rows.
     *
     * The callback receives JSON strings with one of these shapes:
     * - `{ type: "rows", subscription_id, rows }`
     * - `{ type: "error", subscription_id, code, message }`
     * @param {string} sql
     * @param {string | null | undefined} options
     * @param {Function} callback
     * @returns {Promise<string>}
     */
    liveQueryRowsWithSql(sql, options, callback) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(options) ? 0 : passStringToWasm0(options, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_liveQueryRowsWithSql(this.__wbg_ptr, ptr0, len0, ptr1, len1, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Login with current Basic Auth credentials and switch to JWT authentication
     *
     * Sends a POST request to `/v1/api/auth/login` with the stored username/password
     * and updates the client to use JWT authentication on success.
     *
     * # Returns
     * The full LoginResponse as a JsValue (includes access_token, refresh_token, user info, etc.)
     *
     * # Errors
     * - If the client doesn't use Basic Auth
     * - If login request fails
     * - If the response doesn't contain an access_token
     *
     * # Example (JavaScript)
     * ```js
     * const client = new KalamClient("http://localhost:8080", "user", "pass");
     * const response = await client.login();
     * console.log(response.access_token, response.refresh_token);
     * await client.connect(); // Now uses JWT for WebSocket
     * ```
     * @returns {Promise<any>}
     */
    login() {
        const ret = wasm.kalamclient_login(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Create a new KalamDB client with HTTP Basic Authentication (T042, T043, T044)
     *
     * # Arguments
     * * `url` - KalamDB server URL (required, e.g., "http://localhost:8080")
     * * `username` - Username for authentication (required)
     * * `password` - Password for authentication (required)
     *
     * # Errors
     * Returns JsValue error if url, username, or password is empty
     * @param {string} url
     * @param {string} username
     * @param {string} password
     */
    constructor(url, username, password) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(url, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(username, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len1 = WASM_VECTOR_LEN;
            const ptr2 = passStringToWasm0(password, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len2 = WASM_VECTOR_LEN;
            wasm.kalamclient_new(retptr, ptr0, len0, ptr1, len1, ptr2, len2);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            this.__wbg_ptr = r0 >>> 0;
            KalamClientFinalization.register(this, this.__wbg_ptr, this);
            return this;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Register a callback invoked when the WebSocket connection is established.
     *
     * The callback receives no arguments.
     *
     * # Example (JavaScript)
     * ```js
     * client.onConnect(() => console.log('Connected!'));
     * ```
     * @param {Function} callback
     */
    onConnect(callback) {
        wasm.kalamclient_onConnect(this.__wbg_ptr, addHeapObject(callback));
    }
    /**
     * Register a callback invoked when the WebSocket connection is closed.
     *
     * The callback receives an object: `{ message: string, code?: number }`.
     *
     * # Example (JavaScript)
     * ```js
     * client.onDisconnect((reason) => console.log('Disconnected:', reason.message));
     * ```
     * @param {Function} callback
     */
    onDisconnect(callback) {
        wasm.kalamclient_onDisconnect(this.__wbg_ptr, addHeapObject(callback));
    }
    /**
     * Register a callback invoked when a connection error occurs.
     *
     * The callback receives an object: `{ message: string, recoverable: boolean }`.
     *
     * # Example (JavaScript)
     * ```js
     * client.onError((err) => console.error('Error:', err.message, 'recoverable:', err.recoverable));
     * ```
     * @param {Function} callback
     */
    onError(callback) {
        wasm.kalamclient_onError(this.__wbg_ptr, addHeapObject(callback));
    }
    /**
     * Register a callback invoked for every raw message received from the server.
     *
     * This is a debug/tracing hook. The callback receives the raw JSON string.
     *
     * # Example (JavaScript)
     * ```js
     * client.onReceive((msg) => console.log('[RECV]', msg));
     * ```
     * @param {Function} callback
     */
    onReceive(callback) {
        wasm.kalamclient_onReceive(this.__wbg_ptr, addHeapObject(callback));
    }
    /**
     * Register a callback invoked for every raw message sent to the server.
     *
     * This is a debug/tracing hook. The callback receives the raw JSON string.
     *
     * # Example (JavaScript)
     * ```js
     * client.onSend((msg) => console.log('[SEND]', msg));
     * ```
     * @param {Function} callback
     */
    onSend(callback) {
        wasm.kalamclient_onSend(this.__wbg_ptr, addHeapObject(callback));
    }
    /**
     * Execute a SQL query (T050, T063F)
     *
     * # Arguments
     * * `sql` - SQL query string
     *
     * # Returns
     * JSON string with query results
     *
     * # Example (JavaScript)
     * ```js
     * const result = await client.query("SELECT * FROM todos WHERE completed = false");
     * const data = JSON.parse(result);
     * ```
     * @param {string} sql
     * @returns {Promise<string>}
     */
    query(sql) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_query(this.__wbg_ptr, ptr0, len0);
        return takeObject(ret);
    }
    /**
     * Execute a SQL query with parameters
     *
     * # Arguments
     * * `sql` - SQL query string with placeholders ($1, $2, ...)
     * * `params` - JSON array string of parameter values
     *
     * # Returns
     * JSON string with query results
     *
     * # Example (JavaScript)
     * ```js
     * const result = await client.queryWithParams(
     *   "SELECT * FROM users WHERE id = $1 AND age > $2",
     *   JSON.stringify([42, 18])
     * );
     * const data = JSON.parse(result);
     * ```
     * @param {string} sql
     * @param {string | null} [params]
     * @returns {Promise<string>}
     */
    queryWithParams(sql, params) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(params) ? 0 : passStringToWasm0(params, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_queryWithParams(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return takeObject(ret);
    }
    /**
     * Refresh the access token using a refresh token
     *
     * Sends a POST request to `/v1/api/auth/refresh` with the refresh token
     * in the Authorization Bearer header, and updates the client to use the new JWT.
     *
     * # Arguments
     * * `refresh_token` - The refresh token obtained from a previous login
     *
     * # Returns
     * The full LoginResponse as a JsValue (includes new access_token, refresh_token, etc.)
     *
     * # Errors
     * - If the refresh request fails
     * - If the response doesn't contain a valid token
     *
     * # Example (JavaScript)
     * ```js
     * const client = new KalamClient("http://localhost:8080", "user", "pass");
     * const loginResp = await client.login();
     * // Later, when access_token expires:
     * const refreshResp = await client.refresh_access_token(loginResp.refresh_token);
     * console.log(refreshResp.access_token);
     * ```
     * @param {string} refresh_token
     * @returns {Promise<any>}
     */
    refresh_access_token(refresh_token) {
        const ptr0 = passStringToWasm0(refresh_token, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_refresh_access_token(this.__wbg_ptr, ptr0, len0);
        return takeObject(ret);
    }
    /**
     * Send a single application-level keepalive ping to the server.
     *
     * Usually called automatically by the internal ping timer; exposed so
     * callers can send an ad-hoc ping if needed.
     */
    sendPing() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.kalamclient_sendPing(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Set an async authentication provider callback.
     *
     * When set, this callback is invoked before each (re-)connection attempt
     * to obtain a fresh JWT token.  This is the recommended approach for
     * applications that implement refresh-token flows.
     *
     * The callback must be an `async function` (or any function returning a
     * `Promise`) that resolves to **either**:
     * - `{ jwt: { token: "eyJ..." } }` — authenticates with the given JWT
     * - `null` / `undefined` — treated as anonymous (no authentication)
     *
     * The static `auth` set at construction time is ignored once a provider
     * is registered.
     *
     * # Example (JavaScript)
     * ```js
     * client.setAuthProvider(async () => {
     *   const token = await myApp.getOrRefreshJwt();
     *   return { jwt: { token } };
     * });
     * ```
     * @param {Function} callback
     */
    setAuthProvider(callback) {
        wasm.kalamclient_setAuthProvider(this.__wbg_ptr, addHeapObject(callback));
    }
    /**
     * Enable or disable automatic reconnection
     *
     * # Arguments
     * * `enabled` - Whether to automatically reconnect on connection loss
     * @param {boolean} enabled
     */
    setAutoReconnect(enabled) {
        wasm.kalamclient_setAutoReconnect(this.__wbg_ptr, enabled);
    }
    /**
     * Enable or disable compression for WebSocket messages.
     *
     * When set to `true` (default) the server sends gzip-compressed binary
     * frames for large payloads.  Set to `false` during development to receive
     * plain-text JSON frames that are easier to inspect.
     *
     * Takes effect on the **next** `connect()` call.
     *
     * # Example (JavaScript)
     * ```js
     * client.setDisableCompression(true); // plain-text frames
     * await client.connect();
     * ```
     * @param {boolean} disable
     */
    setDisableCompression(disable) {
        wasm.kalamclient_setDisableCompression(this.__wbg_ptr, disable);
    }
    /**
     * Set maximum reconnection attempts
     *
     * # Arguments
     * * `max_attempts` - Maximum number of attempts (0 = infinite)
     * @param {number} max_attempts
     */
    setMaxReconnectAttempts(max_attempts) {
        wasm.kalamclient_setMaxReconnectAttempts(this.__wbg_ptr, max_attempts);
    }
    /**
     * Set the application-level keepalive ping interval in milliseconds.
     *
     * Browser WebSocket APIs do not expose protocol-level Ping frames, so
     * the WASM client sends a JSON `{"type":"ping"}` message at this
     * interval. Set to `0` to disable. Default: 30 000 ms.
     *
     * The change takes effect on the next `connect()` or reconnect.
     *
     * # Note
     * Takes `u32` (maps to TypeScript `number`); the internal store is `u64`.
     * @param {number} ms
     */
    setPingInterval(ms) {
        wasm.kalamclient_setPingInterval(this.__wbg_ptr, ms);
    }
    /**
     * Set reconnection delay parameters
     *
     * # Arguments
     * * `initial_delay_ms` - Initial delay in milliseconds between reconnection attempts
     * * `max_delay_ms` - Maximum delay (for exponential backoff)
     * @param {bigint} initial_delay_ms
     * @param {bigint} max_delay_ms
     */
    setReconnectDelay(initial_delay_ms, max_delay_ms) {
        wasm.kalamclient_setReconnectDelay(this.__wbg_ptr, initial_delay_ms, max_delay_ms);
    }
    /**
     * Control lazy WebSocket connections.
     *
     * When `true` (the default), the WebSocket connection is deferred until
     * the first `subscribe()` / `subscribeWithSql()` call. The SDK manages
     * the connection lifecycle automatically.
     *
     * When `false`, the caller should call `connect()` before subscribing.
     *
     * Default: `true`.
     *
     * # Example (JavaScript)
     * ```js
     * // Eager connection (override the default lazy behaviour)
     * client.setWsLazyConnect(false);
     * await client.connect();
     * const subId = await client.subscribeWithSql('SELECT * FROM messages', null, cb);
     * ```
     * @param {boolean} lazy
     */
    setWsLazyConnect(lazy) {
        wasm.kalamclient_setWsLazyConnect(this.__wbg_ptr, lazy);
    }
    /**
     * Subscribe to table changes (T051, T063I-T063J)
     *
     * # Arguments
     * * `table_name` - Name of the table to subscribe to
     * * `callback` - JavaScript function to call when changes occur
     *
     * # Returns
     * Subscription ID for later unsubscribe
     * @param {string} table_name
     * @param {Function} callback
     * @returns {Promise<string>}
     */
    subscribe(table_name, callback) {
        const ptr0 = passStringToWasm0(table_name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_subscribe(this.__wbg_ptr, ptr0, len0, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Subscribe to a SQL query with optional subscription options
     *
     * # Arguments
     * * `sql` - SQL SELECT query to subscribe to
     * * `options` - Optional JSON string with subscription options:
     *   - `batch_size`: Number of rows per batch (default: server-configured)
     *   - `auto_reconnect`: Override client auto-reconnect for this subscription (default: true)
     *   - `include_old_values`: Include old values in UPDATE/DELETE events (default: false)
     *   - `from`: Resume from a specific sequence ID (internal use)
     * * `callback` - JavaScript function to call when changes occur
     *
     * # Returns
     * Subscription ID for later unsubscribe
     *
     * # Example (JavaScript)
     * ```js
     * // Subscribe with options
     * const subId = await client.subscribeWithSql(
     *   "SELECT * FROM chat.messages WHERE conversation_id = 1",
     *   JSON.stringify({ batch_size: 50, from: 42 }),
     *   (event) => console.log('Change:', event)
     * );
     * ```
     * @param {string} sql
     * @param {string | null | undefined} options
     * @param {Function} callback
     * @returns {Promise<string>}
     */
    subscribeWithSql(sql, options, callback) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(options) ? 0 : passStringToWasm0(options, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_subscribeWithSql(this.__wbg_ptr, ptr0, len0, ptr1, len1, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Unsubscribe from table changes (T052, T063M)
     *
     * # Arguments
     * * `subscription_id` - ID returned from subscribe()
     * @param {string} subscription_id
     * @returns {Promise<void>}
     */
    unsubscribe(subscription_id) {
        const ptr0 = passStringToWasm0(subscription_id, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_unsubscribe(this.__wbg_ptr, ptr0, len0);
        return takeObject(ret);
    }
    /**
     * Create a new KalamDB client with JWT Token Authentication
     *
     * # Arguments
     * * `url` - KalamDB server URL (required, e.g., "http://localhost:8080")
     * * `token` - JWT token for authentication (required)
     *
     * # Errors
     * Returns JsValue error if url or token is empty
     *
     * # Example (JavaScript)
     * ```js
     * const client = KalamClient.withJwt(
     *   "http://localhost:8080",
     *   "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
     * );
     * await client.connect();
     * ```
     * @param {string} url
     * @param {string} token
     * @returns {KalamClient}
     */
    static withJwt(url, token) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(url, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            const ptr1 = passStringToWasm0(token, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len1 = WASM_VECTOR_LEN;
            wasm.kalamclient_withJwt(retptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return KalamClient.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
}
if (Symbol.dispose) KalamClient.prototype[Symbol.dispose] = KalamClient.prototype.free;

/**
 * WASM wrapper for TimestampFormatter
 */
export class WasmTimestampFormatter {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmTimestampFormatter.prototype);
        obj.__wbg_ptr = ptr;
        WasmTimestampFormatterFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmTimestampFormatterFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmtimestampformatter_free(ptr, 0);
    }
    /**
     * Format a timestamp (milliseconds since epoch) to a string
     *
     * # Arguments
     * * `milliseconds` - Timestamp in milliseconds since Unix epoch (or null)
     *
     * # Returns
     * Formatted string, or "null" if input is null/undefined
     *
     * # Example
     * ```javascript
     * const formatter = new WasmTimestampFormatter();
     * console.log(formatter.format(1734191445123)); // "2024-12-14T15:30:45.123Z"
     * ```
     * @param {number | null} [milliseconds]
     * @returns {string}
     */
    format(milliseconds) {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.wasmtimestampformatter_format(retptr, this.__wbg_ptr, !isLikeNone(milliseconds), isLikeNone(milliseconds) ? 0 : milliseconds);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export5(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Format a timestamp as relative time (e.g., "2 hours ago")
     *
     * # Arguments
     * * `milliseconds` - Timestamp in milliseconds since Unix epoch
     *
     * # Returns
     * Relative time string (e.g., "just now", "5 minutes ago", "2 days ago")
     * @param {number} milliseconds
     * @returns {string}
     */
    formatRelative(milliseconds) {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.wasmtimestampformatter_formatRelative(retptr, this.__wbg_ptr, milliseconds);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export5(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Create a new timestamp formatter with ISO 8601 format
     */
    constructor() {
        const ret = wasm.wasmtimestampformatter_new();
        this.__wbg_ptr = ret >>> 0;
        WasmTimestampFormatterFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Create a formatter with a specific format
     *
     * # Arguments
     * * `format` - One of: "iso8601", "iso8601-date", "iso8601-datetime", "unix-ms", "unix-sec", "relative", "rfc2822", "rfc3339"
     * @param {string} format
     * @returns {WasmTimestampFormatter}
     */
    static withFormat(format) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(format, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            wasm.wasmtimestampformatter_withFormat(retptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return WasmTimestampFormatter.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
}
if (Symbol.dispose) WasmTimestampFormatter.prototype[Symbol.dispose] = WasmTimestampFormatter.prototype.free;

/**
 * Parse an ISO 8601 timestamp string to milliseconds since epoch
 *
 * # Arguments
 * * `iso_string` - ISO 8601 formatted string (e.g., "2024-12-14T15:30:45.123Z")
 *
 * # Returns
 * Milliseconds since Unix epoch
 *
 * # Errors
 * Returns JsValue error if parsing fails
 *
 * # Example
 * ```javascript
 * const ms = parseIso8601("2024-12-14T15:30:45.123Z");
 * console.log(ms); // 1734191445123
 * ```
 * @param {string} iso_string
 * @returns {number}
 */
export function parseIso8601(iso_string) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        const ptr0 = passStringToWasm0(iso_string, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        wasm.parseIso8601(retptr, ptr0, len0);
        var r0 = getDataViewMemory0().getFloat64(retptr + 8 * 0, true);
        var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
        var r3 = getDataViewMemory0().getInt32(retptr + 4 * 3, true);
        if (r3) {
            throw takeObject(r2);
        }
        return r0;
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

/**
 * Get the current timestamp in milliseconds since epoch
 *
 * # Returns
 * Current time in milliseconds
 *
 * # Example
 * ```javascript
 * const now = timestampNow();
 * console.log(now); // 1734191445123
 * ```
 * @returns {number}
 */
export function timestampNow() {
    const ret = wasm.timestampNow();
    return ret;
}

function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg_Error_55538483de6e3abe: function(arg0, arg1) {
            const ret = Error(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbg_Number_f257194b7002d6f9: function(arg0) {
            const ret = Number(getObject(arg0));
            return ret;
        },
        __wbg_String_8564e559799eccda: function(arg0, arg1) {
            const ret = String(getObject(arg1));
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_bigint_get_as_i64_a738e80c0fe6f6a7: function(arg0, arg1) {
            const v = getObject(arg1);
            const ret = typeof(v) === 'bigint' ? v : undefined;
            getDataViewMemory0().setBigInt64(arg0 + 8 * 1, isLikeNone(ret) ? BigInt(0) : ret, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
        },
        __wbg___wbindgen_boolean_get_fe2a24fdfdb4064f: function(arg0) {
            const v = getObject(arg0);
            const ret = typeof(v) === 'boolean' ? v : undefined;
            return isLikeNone(ret) ? 0xFFFFFF : ret ? 1 : 0;
        },
        __wbg___wbindgen_debug_string_d89627202d0155b7: function(arg0, arg1) {
            const ret = debugString(getObject(arg1));
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_in_fe3eb6a509f75744: function(arg0, arg1) {
            const ret = getObject(arg0) in getObject(arg1);
            return ret;
        },
        __wbg___wbindgen_is_bigint_ca270ac12ef71091: function(arg0) {
            const ret = typeof(getObject(arg0)) === 'bigint';
            return ret;
        },
        __wbg___wbindgen_is_function_2a95406423ea8626: function(arg0) {
            const ret = typeof(getObject(arg0)) === 'function';
            return ret;
        },
        __wbg___wbindgen_is_null_8d90524c9e0af183: function(arg0) {
            const ret = getObject(arg0) === null;
            return ret;
        },
        __wbg___wbindgen_is_object_59a002e76b059312: function(arg0) {
            const val = getObject(arg0);
            const ret = typeof(val) === 'object' && val !== null;
            return ret;
        },
        __wbg___wbindgen_is_string_624d5244bb2bc87c: function(arg0) {
            const ret = typeof(getObject(arg0)) === 'string';
            return ret;
        },
        __wbg___wbindgen_is_undefined_87a3a837f331fef5: function(arg0) {
            const ret = getObject(arg0) === undefined;
            return ret;
        },
        __wbg___wbindgen_jsval_eq_eedd705f9f2a4f35: function(arg0, arg1) {
            const ret = getObject(arg0) === getObject(arg1);
            return ret;
        },
        __wbg___wbindgen_jsval_loose_eq_cf851f110c48f9ba: function(arg0, arg1) {
            const ret = getObject(arg0) == getObject(arg1);
            return ret;
        },
        __wbg___wbindgen_number_get_769f3676dc20c1d7: function(arg0, arg1) {
            const obj = getObject(arg1);
            const ret = typeof(obj) === 'number' ? obj : undefined;
            getDataViewMemory0().setFloat64(arg0 + 8 * 1, isLikeNone(ret) ? 0 : ret, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
        },
        __wbg___wbindgen_string_get_f1161390414f9b59: function(arg0, arg1) {
            const obj = getObject(arg1);
            const ret = typeof(obj) === 'string' ? obj : undefined;
            var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            var len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_throw_5549492daedad139: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbg___wbindgen_typeof_de716ff95389461d: function(arg0) {
            const ret = typeof getObject(arg0);
            return addHeapObject(ret);
        },
        __wbg__wbg_cb_unref_fbe69bb076c16bad: function(arg0) {
            getObject(arg0)._wbg_cb_unref();
        },
        __wbg_addEventListener_ee34fcb181ae85b2: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            getObject(arg0).addEventListener(getStringFromWasm0(arg1, arg2), getObject(arg3));
        }, arguments); },
        __wbg_call_6ae20895a60069a2: function() { return handleError(function (arg0, arg1) {
            const ret = getObject(arg0).call(getObject(arg1));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_call_8f5d7bb070283508: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = getObject(arg0).call(getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_clearInterval_dc7c095375cbb5e8: function(arg0) {
            clearInterval(arg0);
        },
        __wbg_close_1bf0654059764e94: function() { return handleError(function (arg0) {
            getObject(arg0).close();
        }, arguments); },
        __wbg_code_7eb5b8af0cea9f25: function(arg0) {
            const ret = getObject(arg0).code;
            return ret;
        },
        __wbg_data_7de671a92a650aba: function(arg0) {
            const ret = getObject(arg0).data;
            return addHeapObject(ret);
        },
        __wbg_fetch_bfa1cef2301a3f47: function(arg0) {
            const ret = fetch(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_get_ff5f1fb220233477: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.get(getObject(arg0), getObject(arg1));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_get_with_ref_key_6412cf3094599694: function(arg0, arg1) {
            const ret = getObject(arg0)[getObject(arg1)];
            return addHeapObject(ret);
        },
        __wbg_instanceof_ArrayBuffer_8d855993947fc3a2: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof ArrayBuffer;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Blob_0ba6040bc29f038a: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Blob;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_JsString_ff340baa1398adfc: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof String;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Response_fece7eabbcaca4c3: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Response;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Uint8Array_ce24d58a5f4bdcc3: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Uint8Array;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_isSafeInteger_1dfae065cbfe1915: function(arg0) {
            const ret = Number.isSafeInteger(getObject(arg0));
            return ret;
        },
        __wbg_is_8d0dad0e07b754c5: function(arg0, arg1) {
            const ret = Object.is(getObject(arg0), getObject(arg1));
            return ret;
        },
        __wbg_length_e6e1633fbea6cfa9: function(arg0) {
            const ret = getObject(arg0).length;
            return ret;
        },
        __wbg_log_fc68a0ab2a22b939: function(arg0, arg1) {
            console.log(getStringFromWasm0(arg0, arg1));
        },
        __wbg_new_0934b88171ef61b0: function() {
            const ret = new Map();
            return addHeapObject(ret);
        },
        __wbg_new_1d96678aaacca32e: function(arg0) {
            const ret = new Uint8Array(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_new_210ef5849ab6cf48: function() { return handleError(function () {
            const ret = new Headers();
            return addHeapObject(ret);
        }, arguments); },
        __wbg_new_4370be21fa2b2f80: function() {
            const ret = new Array();
            return addHeapObject(ret);
        },
        __wbg_new_48e1d86cfd30c8e7: function() {
            const ret = new Object();
            return addHeapObject(ret);
        },
        __wbg_new_694161c660bbefba: function(arg0, arg1) {
            try {
                var state0 = {a: arg0, b: arg1};
                var cb0 = (arg0, arg1) => {
                    const a = state0.a;
                    state0.a = 0;
                    try {
                        return __wasm_bindgen_func_elem_2789(a, state0.b, arg0, arg1);
                    } finally {
                        state0.a = a;
                    }
                };
                const ret = new Promise(cb0);
                return addHeapObject(ret);
            } finally {
                state0.a = 0;
            }
        },
        __wbg_new_69642b0f6c3151cc: function() { return handleError(function (arg0, arg1) {
            const ret = new WebSocket(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_new_typed_25dda2388d7e5e9f: function(arg0, arg1) {
            try {
                var state0 = {a: arg0, b: arg1};
                var cb0 = (arg0, arg1) => {
                    const a = state0.a;
                    state0.a = 0;
                    try {
                        return __wasm_bindgen_func_elem_2789(a, state0.b, arg0, arg1);
                    } finally {
                        state0.a = a;
                    }
                };
                const ret = new Promise(cb0);
                return addHeapObject(ret);
            } finally {
                state0.a = 0;
            }
        },
        __wbg_new_with_str_and_init_cb3df438bf62964e: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = new Request(getStringFromWasm0(arg0, arg1), getObject(arg2));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_ok_5865d0a94e185135: function(arg0) {
            const ret = getObject(arg0).ok;
            return ret;
        },
        __wbg_prototypesetcall_3875d54d12ef2eec: function(arg0, arg1, arg2) {
            Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), getObject(arg2));
        },
        __wbg_queueMicrotask_8868365114fe23b5: function(arg0) {
            queueMicrotask(getObject(arg0));
        },
        __wbg_queueMicrotask_cfc5a0e62f9ebdbe: function(arg0) {
            const ret = getObject(arg0).queueMicrotask;
            return addHeapObject(ret);
        },
        __wbg_readyState_a08d25cc57214030: function(arg0) {
            const ret = getObject(arg0).readyState;
            return ret;
        },
        __wbg_reason_30c85ca866e286f0: function(arg0, arg1) {
            const ret = getObject(arg1).reason;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_resolve_d8059bc113e215bf: function(arg0) {
            const ret = Promise.resolve(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_send_73e9cb70b2a23e05: function() { return handleError(function (arg0, arg1, arg2) {
            getObject(arg0).send(getStringFromWasm0(arg1, arg2));
        }, arguments); },
        __wbg_send_da543a379e952bc6: function() { return handleError(function (arg0, arg1, arg2) {
            getObject(arg0).send(getArrayU8FromWasm0(arg1, arg2));
        }, arguments); },
        __wbg_setInterval_9fb9796764da28b4: function(arg0, arg1) {
            const ret = setInterval(getObject(arg0), arg1);
            return ret;
        },
        __wbg_setTimeout_f88ed8cee395b93b: function(arg0, arg1) {
            const ret = setTimeout(getObject(arg0), arg1);
            return ret;
        },
        __wbg_set_0b4302959e9491f2: function() { return handleError(function (arg0, arg1, arg2, arg3, arg4) {
            getObject(arg0).set(getStringFromWasm0(arg1, arg2), getStringFromWasm0(arg3, arg4));
        }, arguments); },
        __wbg_set_4702dfa37c77f492: function(arg0, arg1, arg2) {
            getObject(arg0)[arg1 >>> 0] = takeObject(arg2);
        },
        __wbg_set_6be42768c690e380: function(arg0, arg1, arg2) {
            getObject(arg0)[takeObject(arg1)] = takeObject(arg2);
        },
        __wbg_set_8c6629931852a4a5: function(arg0, arg1, arg2) {
            const ret = getObject(arg0).set(getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        },
        __wbg_set_991082a7a49971cf: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = Reflect.set(getObject(arg0), getObject(arg1), getObject(arg2));
            return ret;
        }, arguments); },
        __wbg_set_binaryType_0675f0e51c055ca8: function(arg0, arg1) {
            getObject(arg0).binaryType = __wbindgen_enum_BinaryType[arg1];
        },
        __wbg_set_body_e2cf9537a2f3e0be: function(arg0, arg1) {
            getObject(arg0).body = getObject(arg1);
        },
        __wbg_set_headers_22d4b01224273a83: function(arg0, arg1) {
            getObject(arg0).headers = getObject(arg1);
        },
        __wbg_set_method_4a4ab3faba8a018c: function(arg0, arg1, arg2) {
            getObject(arg0).method = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_mode_7b856ab49b64c0db: function(arg0, arg1) {
            getObject(arg0).mode = __wbindgen_enum_RequestMode[arg1];
        },
        __wbg_set_onclose_f791ef701be808a0: function(arg0, arg1) {
            getObject(arg0).onclose = getObject(arg1);
        },
        __wbg_set_onerror_e23002e9224d353b: function(arg0, arg1) {
            getObject(arg0).onerror = getObject(arg1);
        },
        __wbg_set_onmessage_d2fe701a9ce80846: function(arg0, arg1) {
            getObject(arg0).onmessage = getObject(arg1);
        },
        __wbg_set_onopen_0556381d0db30cbb: function(arg0, arg1) {
            getObject(arg0).onopen = getObject(arg1);
        },
        __wbg_static_accessor_GLOBAL_8dfb7f5e26ebe523: function() {
            const ret = typeof global === 'undefined' ? null : global;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_static_accessor_GLOBAL_THIS_941154efc8395cdd: function() {
            const ret = typeof globalThis === 'undefined' ? null : globalThis;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_static_accessor_SELF_58dac9af822f561f: function() {
            const ret = typeof self === 'undefined' ? null : self;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_static_accessor_WINDOW_ee64f0b3d8354c0b: function() {
            const ret = typeof window === 'undefined' ? null : window;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_status_1ae443dc56281de7: function(arg0) {
            const ret = getObject(arg0).status;
            return ret;
        },
        __wbg_stringify_b67e2c8c60b93f69: function() { return handleError(function (arg0) {
            const ret = JSON.stringify(getObject(arg0));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_text_6d3a70da69d27961: function() { return handleError(function (arg0) {
            const ret = getObject(arg0).text();
            return addHeapObject(ret);
        }, arguments); },
        __wbg_then_0150352e4ad20344: function(arg0, arg1, arg2) {
            const ret = getObject(arg0).then(getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        },
        __wbg_then_5160486c67ddb98a: function(arg0, arg1) {
            const ret = getObject(arg0).then(getObject(arg1));
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000001: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [Externref], shim_idx: 222, ret: Result(Unit), inner_ret: Some(Result(Unit)) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, __wasm_bindgen_func_elem_2779);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000002: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("CloseEvent")], shim_idx: 6, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, __wasm_bindgen_func_elem_1481);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000003: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("ErrorEvent")], shim_idx: 6, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, __wasm_bindgen_func_elem_1481_2);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000004: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [NamedExternref("MessageEvent")], shim_idx: 6, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, __wasm_bindgen_func_elem_1481_3);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000005: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { owned: true, function: Function { arguments: [], shim_idx: 7, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, __wasm_bindgen_func_elem_1480);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000006: function(arg0) {
            // Cast intrinsic for `F64 -> Externref`.
            const ret = arg0;
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000007: function(arg0) {
            // Cast intrinsic for `I64 -> Externref`.
            const ret = arg0;
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000008: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000009: function(arg0) {
            // Cast intrinsic for `U64 -> Externref`.
            const ret = BigInt.asUintN(64, arg0);
            return addHeapObject(ret);
        },
        __wbindgen_object_clone_ref: function(arg0) {
            const ret = getObject(arg0);
            return addHeapObject(ret);
        },
        __wbindgen_object_drop_ref: function(arg0) {
            takeObject(arg0);
        },
    };
    return {
        __proto__: null,
        "./kalam_link_dart_bg.js": import0,
    };
}

function __wasm_bindgen_func_elem_1480(arg0, arg1) {
    wasm.__wasm_bindgen_func_elem_1480(arg0, arg1);
}

function __wasm_bindgen_func_elem_1481(arg0, arg1, arg2) {
    wasm.__wasm_bindgen_func_elem_1481(arg0, arg1, addHeapObject(arg2));
}

function __wasm_bindgen_func_elem_1481_2(arg0, arg1, arg2) {
    wasm.__wasm_bindgen_func_elem_1481_2(arg0, arg1, addHeapObject(arg2));
}

function __wasm_bindgen_func_elem_1481_3(arg0, arg1, arg2) {
    wasm.__wasm_bindgen_func_elem_1481_3(arg0, arg1, addHeapObject(arg2));
}

function __wasm_bindgen_func_elem_2779(arg0, arg1, arg2) {
    try {
        const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
        wasm.__wasm_bindgen_func_elem_2779(retptr, arg0, arg1, addHeapObject(arg2));
        var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
        var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
        if (r1) {
            throw takeObject(r0);
        }
    } finally {
        wasm.__wbindgen_add_to_stack_pointer(16);
    }
}

function __wasm_bindgen_func_elem_2789(arg0, arg1, arg2, arg3) {
    wasm.__wasm_bindgen_func_elem_2789(arg0, arg1, addHeapObject(arg2), addHeapObject(arg3));
}


const __wbindgen_enum_BinaryType = ["blob", "arraybuffer"];


const __wbindgen_enum_RequestMode = ["same-origin", "no-cors", "cors", "navigate"];
const KalamClientFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_kalamclient_free(ptr >>> 0, 1));
const WasmTimestampFormatterFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmtimestampformatter_free(ptr >>> 0, 1));

function addHeapObject(obj) {
    if (heap_next === heap.length) heap.push(heap.length + 1);
    const idx = heap_next;
    heap_next = heap[idx];

    heap[idx] = obj;
    return idx;
}

const CLOSURE_DTORS = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(state => wasm.__wbindgen_export4(state.a, state.b));

function debugString(val) {
    // primitive types
    const type = typeof val;
    if (type == 'number' || type == 'boolean' || val == null) {
        return  `${val}`;
    }
    if (type == 'string') {
        return `"${val}"`;
    }
    if (type == 'symbol') {
        const description = val.description;
        if (description == null) {
            return 'Symbol';
        } else {
            return `Symbol(${description})`;
        }
    }
    if (type == 'function') {
        const name = val.name;
        if (typeof name == 'string' && name.length > 0) {
            return `Function(${name})`;
        } else {
            return 'Function';
        }
    }
    // objects
    if (Array.isArray(val)) {
        const length = val.length;
        let debug = '[';
        if (length > 0) {
            debug += debugString(val[0]);
        }
        for(let i = 1; i < length; i++) {
            debug += ', ' + debugString(val[i]);
        }
        debug += ']';
        return debug;
    }
    // Test for built-in
    const builtInMatches = /\[object ([^\]]+)\]/.exec(toString.call(val));
    let className;
    if (builtInMatches && builtInMatches.length > 1) {
        className = builtInMatches[1];
    } else {
        // Failed to match the standard '[object ClassName]'
        return toString.call(val);
    }
    if (className == 'Object') {
        // we're a user defined class or Object
        // JSON.stringify avoids problems with cycles, and is generally much
        // easier than looping through ownProperties of `val`.
        try {
            return 'Object(' + JSON.stringify(val) + ')';
        } catch (_) {
            return 'Object';
        }
    }
    // errors
    if (val instanceof Error) {
        return `${val.name}: ${val.message}\n${val.stack}`;
    }
    // TODO we could test for more things here, like `Set`s and `Map`s.
    return className;
}

function dropObject(idx) {
    if (idx < 1028) return;
    heap[idx] = heap_next;
    heap_next = idx;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}

let cachedDataViewMemory0 = null;
function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return decodeText(ptr, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function getObject(idx) { return heap[idx]; }

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        wasm.__wbindgen_export3(addHeapObject(e));
    }
}

let heap = new Array(1024).fill(undefined);
heap.push(undefined, null, true, false);

let heap_next = heap.length;

function isLikeNone(x) {
    return x === undefined || x === null;
}

function makeMutClosure(arg0, arg1, f) {
    const state = { a: arg0, b: arg1, cnt: 1 };
    const real = (...args) => {

        // First up with a closure we increment the internal reference
        // count. This ensures that the Rust closure environment won't
        // be deallocated while we're invoking it.
        state.cnt++;
        const a = state.a;
        state.a = 0;
        try {
            return f(a, state.b, ...args);
        } finally {
            state.a = a;
            real._wbg_cb_unref();
        }
    };
    real._wbg_cb_unref = () => {
        if (--state.cnt === 0) {
            wasm.__wbindgen_export4(state.a, state.b);
            state.a = 0;
            CLOSURE_DTORS.unregister(state);
        }
    };
    CLOSURE_DTORS.register(real, state, state);
    return real;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function takeObject(idx) {
    const ret = getObject(idx);
    dropObject(idx);
    return ret;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    };
}

let WASM_VECTOR_LEN = 0;

let wasmModule, wasm;
function __wbg_finalize_init(instance, module) {
    wasm = instance.exports;
    wasmModule = module;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;
    return wasm;
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = module.ok && expectedResponseType(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else { throw e; }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }

    function expectedResponseType(type) {
        switch (type) {
            case 'basic': case 'cors': case 'default': return true;
        }
        return false;
    }
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (module !== undefined) {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (module_or_path !== undefined) {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (module_or_path === undefined) {
        module_or_path = new URL('kalam_link_dart_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync, __wbg_init as default };
