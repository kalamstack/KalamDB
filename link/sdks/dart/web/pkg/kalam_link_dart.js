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
        const ptr0 = passStringToWasm0(topic, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(group_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_ack(this.__wbg_ptr, ptr0, len0, ptr1, len1, partition_id, upto_offset);
        return ret;
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
        const ptr0 = passStringToWasm0(url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_anonymous(ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return KalamClient.__wrap(ret[0]);
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
        return ret;
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
        const ret = wasm.kalamclient_consume(this.__wbg_ptr, options);
        return ret;
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
        const ptr0 = passStringToWasm0(table_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(row_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_delete(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
    }
    /**
     * Disconnect from KalamDB server (T046, T063E)
     * @returns {Promise<void>}
     */
    disconnect() {
        const ret = wasm.kalamclient_disconnect(this.__wbg_ptr);
        return ret;
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
            const ret = wasm.kalamclient_getAuthType(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
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
        const ptr0 = passStringToWasm0(subscription_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_getLastSeqId(this.__wbg_ptr, ptr0, len0);
        let v2;
        if (ret[0] !== 0) {
            v2 = getStringFromWasm0(ret[0], ret[1]).slice();
            wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
        }
        return v2;
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
        return ret;
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
        const ptr0 = passStringToWasm0(table_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(data, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_insert(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
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
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(options) ? 0 : passStringToWasm0(options, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_liveQueryRowsWithSql(this.__wbg_ptr, ptr0, len0, ptr1, len1, callback);
        return ret;
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
        return ret;
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
        const ptr0 = passStringToWasm0(url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(username, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(password, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_new(ptr0, len0, ptr1, len1, ptr2, len2);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        this.__wbg_ptr = ret[0] >>> 0;
        KalamClientFinalization.register(this, this.__wbg_ptr, this);
        return this;
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
        wasm.kalamclient_onConnect(this.__wbg_ptr, callback);
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
        wasm.kalamclient_onDisconnect(this.__wbg_ptr, callback);
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
        wasm.kalamclient_onError(this.__wbg_ptr, callback);
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
        wasm.kalamclient_onReceive(this.__wbg_ptr, callback);
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
        wasm.kalamclient_onSend(this.__wbg_ptr, callback);
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
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_query(this.__wbg_ptr, ptr0, len0);
        return ret;
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
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(params) ? 0 : passStringToWasm0(params, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_queryWithParams(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        return ret;
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
        const ptr0 = passStringToWasm0(refresh_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_refresh_access_token(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Send a single application-level keepalive ping to the server.
     *
     * Usually called automatically by the internal ping timer; exposed so
     * callers can send an ad-hoc ping if needed.
     */
    sendPing() {
        const ret = wasm.kalamclient_sendPing(this.__wbg_ptr);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
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
        wasm.kalamclient_setAuthProvider(this.__wbg_ptr, callback);
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
        const ptr0 = passStringToWasm0(table_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_subscribe(this.__wbg_ptr, ptr0, len0, callback);
        return ret;
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
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(options) ? 0 : passStringToWasm0(options, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_subscribeWithSql(this.__wbg_ptr, ptr0, len0, ptr1, len1, callback);
        return ret;
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
        const ptr0 = passStringToWasm0(subscription_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_unsubscribe(this.__wbg_ptr, ptr0, len0);
        return ret;
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
        const ptr0 = passStringToWasm0(url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.kalamclient_withJwt(ptr0, len0, ptr1, len1);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return KalamClient.__wrap(ret[0]);
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
            const ret = wasm.wasmtimestampformatter_format(this.__wbg_ptr, !isLikeNone(milliseconds), isLikeNone(milliseconds) ? 0 : milliseconds);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
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
            const ret = wasm.wasmtimestampformatter_formatRelative(this.__wbg_ptr, milliseconds);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
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
        const ptr0 = passStringToWasm0(format, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmtimestampformatter_withFormat(ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return WasmTimestampFormatter.__wrap(ret[0]);
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
    const ptr0 = passStringToWasm0(iso_string, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.parseIso8601(ptr0, len0);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return ret[0];
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
        __wbg_Error_83742b46f01ce22d: function(arg0, arg1) {
            const ret = Error(getStringFromWasm0(arg0, arg1));
            return ret;
        },
        __wbg_Number_a5a435bd7bbec835: function(arg0) {
            const ret = Number(arg0);
            return ret;
        },
        __wbg_String_8564e559799eccda: function(arg0, arg1) {
            const ret = String(arg1);
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_bigint_get_as_i64_447a76b5c6ef7bda: function(arg0, arg1) {
            const v = arg1;
            const ret = typeof(v) === 'bigint' ? v : undefined;
            getDataViewMemory0().setBigInt64(arg0 + 8 * 1, isLikeNone(ret) ? BigInt(0) : ret, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
        },
        __wbg___wbindgen_boolean_get_c0f3f60bac5a78d1: function(arg0) {
            const v = arg0;
            const ret = typeof(v) === 'boolean' ? v : undefined;
            return isLikeNone(ret) ? 0xFFFFFF : ret ? 1 : 0;
        },
        __wbg___wbindgen_debug_string_5398f5bb970e0daa: function(arg0, arg1) {
            const ret = debugString(arg1);
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_in_41dbb8413020e076: function(arg0, arg1) {
            const ret = arg0 in arg1;
            return ret;
        },
        __wbg___wbindgen_is_bigint_e2141d4f045b7eda: function(arg0) {
            const ret = typeof(arg0) === 'bigint';
            return ret;
        },
        __wbg___wbindgen_is_function_3c846841762788c1: function(arg0) {
            const ret = typeof(arg0) === 'function';
            return ret;
        },
        __wbg___wbindgen_is_null_0b605fc6b167c56f: function(arg0) {
            const ret = arg0 === null;
            return ret;
        },
        __wbg___wbindgen_is_object_781bc9f159099513: function(arg0) {
            const val = arg0;
            const ret = typeof(val) === 'object' && val !== null;
            return ret;
        },
        __wbg___wbindgen_is_string_7ef6b97b02428fae: function(arg0) {
            const ret = typeof(arg0) === 'string';
            return ret;
        },
        __wbg___wbindgen_is_undefined_52709e72fb9f179c: function(arg0) {
            const ret = arg0 === undefined;
            return ret;
        },
        __wbg___wbindgen_jsval_eq_ee31bfad3e536463: function(arg0, arg1) {
            const ret = arg0 === arg1;
            return ret;
        },
        __wbg___wbindgen_jsval_loose_eq_5bcc3bed3c69e72b: function(arg0, arg1) {
            const ret = arg0 == arg1;
            return ret;
        },
        __wbg___wbindgen_number_get_34bb9d9dcfa21373: function(arg0, arg1) {
            const obj = arg1;
            const ret = typeof(obj) === 'number' ? obj : undefined;
            getDataViewMemory0().setFloat64(arg0 + 8 * 1, isLikeNone(ret) ? 0 : ret, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
        },
        __wbg___wbindgen_string_get_395e606bd0ee4427: function(arg0, arg1) {
            const obj = arg1;
            const ret = typeof(obj) === 'string' ? obj : undefined;
            var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            var len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_throw_6ddd609b62940d55: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbg___wbindgen_typeof_470610f940b2a8e0: function(arg0) {
            const ret = typeof arg0;
            return ret;
        },
        __wbg__wbg_cb_unref_6b5b6b8576d35cb1: function(arg0) {
            arg0._wbg_cb_unref();
        },
        __wbg_addEventListener_2d985aa8a656f6dc: function() { return handleError(function (arg0, arg1, arg2, arg3) {
            arg0.addEventListener(getStringFromWasm0(arg1, arg2), arg3);
        }, arguments); },
        __wbg_call_2d781c1f4d5c0ef8: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = arg0.call(arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_call_e133b57c9155d22c: function() { return handleError(function (arg0, arg1) {
            const ret = arg0.call(arg1);
            return ret;
        }, arguments); },
        __wbg_clearInterval_777a768385c930b1: function(arg0) {
            clearInterval(arg0);
        },
        __wbg_close_af26905c832a88cb: function() { return handleError(function (arg0) {
            arg0.close();
        }, arguments); },
        __wbg_code_aea376e2d265a64f: function(arg0) {
            const ret = arg0.code;
            return ret;
        },
        __wbg_data_a3d9ff9cdd801002: function(arg0) {
            const ret = arg0.data;
            return ret;
        },
        __wbg_fetch_0f5a5bbf552d3a01: function(arg0) {
            const ret = fetch(arg0);
            return ret;
        },
        __wbg_get_3ef1eba1850ade27: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.get(arg0, arg1);
            return ret;
        }, arguments); },
        __wbg_get_with_ref_key_6412cf3094599694: function(arg0, arg1) {
            const ret = arg0[arg1];
            return ret;
        },
        __wbg_instanceof_ArrayBuffer_101e2bf31071a9f6: function(arg0) {
            let result;
            try {
                result = arg0 instanceof ArrayBuffer;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Blob_c91af000f11c2d0b: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Blob;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Response_9b4d9fd451e051b1: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Response;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Uint8Array_740438561a5b956d: function(arg0) {
            let result;
            try {
                result = arg0 instanceof Uint8Array;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_isSafeInteger_ecd6a7f9c3e053cd: function(arg0) {
            const ret = Number.isSafeInteger(arg0);
            return ret;
        },
        __wbg_is_a166b9958c2438ad: function(arg0, arg1) {
            const ret = Object.is(arg0, arg1);
            return ret;
        },
        __wbg_length_ea16607d7b61445b: function(arg0) {
            const ret = arg0.length;
            return ret;
        },
        __wbg_log_32cfdb82f3cc0bf6: function(arg0, arg1) {
            console.log(getStringFromWasm0(arg0, arg1));
        },
        __wbg_new_0837727332ac86ba: function() { return handleError(function () {
            const ret = new Headers();
            return ret;
        }, arguments); },
        __wbg_new_49d5571bd3f0c4d4: function() {
            const ret = new Map();
            return ret;
        },
        __wbg_new_5f486cdf45a04d78: function(arg0) {
            const ret = new Uint8Array(arg0);
            return ret;
        },
        __wbg_new_a70fbab9066b301f: function() {
            const ret = new Array();
            return ret;
        },
        __wbg_new_ab79df5bd7c26067: function() {
            const ret = new Object();
            return ret;
        },
        __wbg_new_d098e265629cd10f: function(arg0, arg1) {
            try {
                var state0 = {a: arg0, b: arg1};
                var cb0 = (arg0, arg1) => {
                    const a = state0.a;
                    state0.a = 0;
                    try {
                        return wasm_bindgen__convert__closures_____invoke__h11188c184bbe4ba9(a, state0.b, arg0, arg1);
                    } finally {
                        state0.a = a;
                    }
                };
                const ret = new Promise(cb0);
                return ret;
            } finally {
                state0.a = state0.b = 0;
            }
        },
        __wbg_new_dd50bcc3f60ba434: function() { return handleError(function (arg0, arg1) {
            const ret = new WebSocket(getStringFromWasm0(arg0, arg1));
            return ret;
        }, arguments); },
        __wbg_new_typed_aaaeaf29cf802876: function(arg0, arg1) {
            try {
                var state0 = {a: arg0, b: arg1};
                var cb0 = (arg0, arg1) => {
                    const a = state0.a;
                    state0.a = 0;
                    try {
                        return wasm_bindgen__convert__closures_____invoke__h11188c184bbe4ba9(a, state0.b, arg0, arg1);
                    } finally {
                        state0.a = a;
                    }
                };
                const ret = new Promise(cb0);
                return ret;
            } finally {
                state0.a = state0.b = 0;
            }
        },
        __wbg_new_with_str_and_init_b4b54d1a819bc724: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = new Request(getStringFromWasm0(arg0, arg1), arg2);
            return ret;
        }, arguments); },
        __wbg_ok_7ec8b94facac7704: function(arg0) {
            const ret = arg0.ok;
            return ret;
        },
        __wbg_prototypesetcall_d62e5099504357e6: function(arg0, arg1, arg2) {
            Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), arg2);
        },
        __wbg_queueMicrotask_0c399741342fb10f: function(arg0) {
            const ret = arg0.queueMicrotask;
            return ret;
        },
        __wbg_queueMicrotask_a082d78ce798393e: function(arg0) {
            queueMicrotask(arg0);
        },
        __wbg_readyState_1f1e7f1bdf9f4d42: function(arg0) {
            const ret = arg0.readyState;
            return ret;
        },
        __wbg_reason_cbcb9911796c4714: function(arg0, arg1) {
            const ret = arg1.reason;
            const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg_resolve_ae8d83246e5bcc12: function(arg0) {
            const ret = Promise.resolve(arg0);
            return ret;
        },
        __wbg_send_4a1dc66e8653e5ed: function() { return handleError(function (arg0, arg1, arg2) {
            arg0.send(getStringFromWasm0(arg1, arg2));
        }, arguments); },
        __wbg_setInterval_509364263db7ee24: function(arg0, arg1) {
            const ret = setInterval(arg0, arg1);
            return ret;
        },
        __wbg_setTimeout_f3d1c2a77f863cd4: function(arg0, arg1) {
            const ret = setTimeout(arg0, arg1);
            return ret;
        },
        __wbg_set_282384002438957f: function(arg0, arg1, arg2) {
            arg0[arg1 >>> 0] = arg2;
        },
        __wbg_set_6be42768c690e380: function(arg0, arg1, arg2) {
            arg0[arg1] = arg2;
        },
        __wbg_set_7eaa4f96924fd6b3: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = Reflect.set(arg0, arg1, arg2);
            return ret;
        }, arguments); },
        __wbg_set_bf7251625df30a02: function(arg0, arg1, arg2) {
            const ret = arg0.set(arg1, arg2);
            return ret;
        },
        __wbg_set_binaryType_3dcf8281ec100a8f: function(arg0, arg1) {
            arg0.binaryType = __wbindgen_enum_BinaryType[arg1];
        },
        __wbg_set_body_a3d856b097dfda04: function(arg0, arg1) {
            arg0.body = arg1;
        },
        __wbg_set_e09648bea3f1af1e: function() { return handleError(function (arg0, arg1, arg2, arg3, arg4) {
            arg0.set(getStringFromWasm0(arg1, arg2), getStringFromWasm0(arg3, arg4));
        }, arguments); },
        __wbg_set_headers_3c8fecc693b75327: function(arg0, arg1) {
            arg0.headers = arg1;
        },
        __wbg_set_method_8c015e8bcafd7be1: function(arg0, arg1, arg2) {
            arg0.method = getStringFromWasm0(arg1, arg2);
        },
        __wbg_set_mode_5a87f2c809cf37c2: function(arg0, arg1) {
            arg0.mode = __wbindgen_enum_RequestMode[arg1];
        },
        __wbg_set_onclose_8da801226bdd7a7b: function(arg0, arg1) {
            arg0.onclose = arg1;
        },
        __wbg_set_onerror_901ca711f94a5bbb: function(arg0, arg1) {
            arg0.onerror = arg1;
        },
        __wbg_set_onmessage_6f80ab771bf151aa: function(arg0, arg1) {
            arg0.onmessage = arg1;
        },
        __wbg_set_onopen_34e3e24cf9337ddd: function(arg0, arg1) {
            arg0.onopen = arg1;
        },
        __wbg_static_accessor_GLOBAL_8adb955bd33fac2f: function() {
            const ret = typeof global === 'undefined' ? null : global;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_GLOBAL_THIS_ad356e0db91c7913: function() {
            const ret = typeof globalThis === 'undefined' ? null : globalThis;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_SELF_f207c857566db248: function() {
            const ret = typeof self === 'undefined' ? null : self;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_static_accessor_WINDOW_bb9f1ba69d61b386: function() {
            const ret = typeof window === 'undefined' ? null : window;
            return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
        },
        __wbg_status_318629ab93a22955: function(arg0) {
            const ret = arg0.status;
            return ret;
        },
        __wbg_stringify_5ae93966a84901ac: function() { return handleError(function (arg0) {
            const ret = JSON.stringify(arg0);
            return ret;
        }, arguments); },
        __wbg_text_372f5b91442c50f9: function() { return handleError(function (arg0) {
            const ret = arg0.text();
            return ret;
        }, arguments); },
        __wbg_then_098abe61755d12f6: function(arg0, arg1) {
            const ret = arg0.then(arg1);
            return ret;
        },
        __wbg_then_9e335f6dd892bc11: function(arg0, arg1, arg2) {
            const ret = arg0.then(arg1, arg2);
            return ret;
        },
        __wbindgen_cast_0000000000000001: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { dtor_idx: 134, function: Function { arguments: [NamedExternref("CloseEvent")], shim_idx: 135, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h3082ecb8e5442115, wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471);
            return ret;
        },
        __wbindgen_cast_0000000000000002: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { dtor_idx: 134, function: Function { arguments: [NamedExternref("ErrorEvent")], shim_idx: 135, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h3082ecb8e5442115, wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471_1);
            return ret;
        },
        __wbindgen_cast_0000000000000003: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { dtor_idx: 134, function: Function { arguments: [NamedExternref("MessageEvent")], shim_idx: 135, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h3082ecb8e5442115, wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471_2);
            return ret;
        },
        __wbindgen_cast_0000000000000004: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { dtor_idx: 134, function: Function { arguments: [], shim_idx: 138, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h3082ecb8e5442115, wasm_bindgen__convert__closures_____invoke__h3bc7e3c5702cdef5);
            return ret;
        },
        __wbindgen_cast_0000000000000005: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { dtor_idx: 237, function: Function { arguments: [Externref], shim_idx: 238, ret: Result(Unit), inner_ret: Some(Result(Unit)) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__hc4784aa82de56652, wasm_bindgen__convert__closures_____invoke__h6537501fed6ccdff);
            return ret;
        },
        __wbindgen_cast_0000000000000006: function(arg0) {
            // Cast intrinsic for `F64 -> Externref`.
            const ret = arg0;
            return ret;
        },
        __wbindgen_cast_0000000000000007: function(arg0) {
            // Cast intrinsic for `I64 -> Externref`.
            const ret = arg0;
            return ret;
        },
        __wbindgen_cast_0000000000000008: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return ret;
        },
        __wbindgen_cast_0000000000000009: function(arg0) {
            // Cast intrinsic for `U64 -> Externref`.
            const ret = BigInt.asUintN(64, arg0);
            return ret;
        },
        __wbindgen_init_externref_table: function() {
            const table = wasm.__wbindgen_externrefs;
            const offset = table.grow(4);
            table.set(0, undefined);
            table.set(offset + 0, undefined);
            table.set(offset + 1, null);
            table.set(offset + 2, true);
            table.set(offset + 3, false);
        },
    };
    return {
        __proto__: null,
        "./kalam_link_dart_bg.js": import0,
    };
}

function wasm_bindgen__convert__closures_____invoke__h3bc7e3c5702cdef5(arg0, arg1) {
    wasm.wasm_bindgen__convert__closures_____invoke__h3bc7e3c5702cdef5(arg0, arg1);
}

function wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471_1(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471_1(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471_2(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__hc08ba1fc1bd11471_2(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h6537501fed6ccdff(arg0, arg1, arg2) {
    const ret = wasm.wasm_bindgen__convert__closures_____invoke__h6537501fed6ccdff(arg0, arg1, arg2);
    if (ret[1]) {
        throw takeFromExternrefTable0(ret[0]);
    }
}

function wasm_bindgen__convert__closures_____invoke__h11188c184bbe4ba9(arg0, arg1, arg2, arg3) {
    wasm.wasm_bindgen__convert__closures_____invoke__h11188c184bbe4ba9(arg0, arg1, arg2, arg3);
}


const __wbindgen_enum_BinaryType = ["blob", "arraybuffer"];


const __wbindgen_enum_RequestMode = ["same-origin", "no-cors", "cors", "navigate"];
const KalamClientFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_kalamclient_free(ptr >>> 0, 1));
const WasmTimestampFormatterFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmtimestampformatter_free(ptr >>> 0, 1));

function addToExternrefTable0(obj) {
    const idx = wasm.__externref_table_alloc();
    wasm.__wbindgen_externrefs.set(idx, obj);
    return idx;
}

const CLOSURE_DTORS = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(state => state.dtor(state.a, state.b));

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

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        const idx = addToExternrefTable0(e);
        wasm.__wbindgen_exn_store(idx);
    }
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

function makeMutClosure(arg0, arg1, dtor, f) {
    const state = { a: arg0, b: arg1, cnt: 1, dtor };
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
            state.dtor(state.a, state.b);
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

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_externrefs.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
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
    wasm.__wbindgen_start();
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
