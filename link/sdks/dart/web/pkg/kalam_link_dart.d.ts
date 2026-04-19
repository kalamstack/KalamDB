/* tslint:disable */
/* eslint-disable */

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
 * import init, { KalamClient, KalamClientWithJwt, KalamClientAnonymous } from './pkg/kalam_client.js';
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
    free(): void;
    [Symbol.dispose](): void;
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
     */
    static anonymous(url: string): KalamClient;
    /**
     * Clear a previously set auth provider, reverting to the static auth
     * configured at construction time.
     */
    clearAuthProvider(): void;
    /**
     *
     * # Returns
     * Promise that resolves when connection is established and authenticated
     */
    connect(): Promise<void>;
    /**
     * Delete a row from a table (T049, T063H)
     *
     * # Arguments
     * * `table_name` - Name of the table
     * * `row_id` - ID of the row to delete
     */
    delete(table_name: string, row_id: string): Promise<void>;
    /**
     * Disconnect from KalamDB server (T046, T063E)
     */
    disconnect(): Promise<void>;
    /**
     * Get the current authentication type
     *
     * Returns one of: "basic", "jwt", or "none"
     */
    getAuthType(): string;
    /**
     * Get the last received seq_id for a subscription
     *
     * Useful for debugging or manual resumption tracking
     */
    getLastSeqId(subscription_id: string): string | undefined;
    /**
     * Get the current reconnection attempt count
     */
    getReconnectAttempts(): number;
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
     */
    getSubscriptions(): any;
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
     */
    insert(table_name: string, data: string): Promise<string>;
    /**
     * Check if client is currently connected (T047)
     *
     * # Returns
     * true if WebSocket connection is active, false otherwise
     */
    isConnected(): boolean;
    /**
     * Check if currently reconnecting
     */
    isReconnecting(): boolean;
    /**
     * Subscribe to a SQL query and receive materialized live rows.
     *
     * The callback receives JSON strings with one of these shapes:
     * - `{ type: "rows", subscription_id, rows }`
     * - `{ type: "error", subscription_id, code, message }`
     */
    liveQueryRowsWithSql(sql: string, options: string | null | undefined, callback: Function): Promise<string>;
    /**
     * Login with current Basic Auth credentials and switch to JWT authentication
     *
     * Sends a POST request to `/v1/api/auth/login` with the stored user/password
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
     */
    login(): Promise<any>;
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
     */
    constructor(url: string, username: string, password: string);
    /**
     * Register a callback invoked when the WebSocket connection is established.
     *
     * The callback receives no arguments.
     *
     * # Example (JavaScript)
     * ```js
     * client.onConnect(() => console.log('Connected!'));
     * ```
     */
    onConnect(callback: Function): void;
    /**
     * Register a callback invoked when the WebSocket connection is closed.
     *
     * The callback receives an object: `{ message: string, code?: number }`.
     *
     * # Example (JavaScript)
     * ```js
     * client.onDisconnect((reason) => console.log('Disconnected:', reason.message));
     * ```
     */
    onDisconnect(callback: Function): void;
    /**
     * Register a callback invoked when a connection error occurs.
     *
     * The callback receives an object: `{ message: string, recoverable: boolean }`.
     *
     * # Example (JavaScript)
     * ```js
     * client.onError((err) => console.error('Error:', err.message, 'recoverable:', err.recoverable));
     * ```
     */
    onError(callback: Function): void;
    /**
     * Register a callback invoked for every raw message received from the server.
     *
     * This is a debug/tracing hook. The callback receives the raw JSON string.
     *
     * # Example (JavaScript)
     * ```js
     * client.onReceive((msg) => console.log('[RECV]', msg));
     * ```
     */
    onReceive(callback: Function): void;
    /**
     * Register a callback invoked for every raw message sent to the server.
     *
     * This is a debug/tracing hook. The callback receives the raw JSON string.
     *
     * # Example (JavaScript)
     * ```js
     * client.onSend((msg) => console.log('[SEND]', msg));
     * ```
     */
    onSend(callback: Function): void;
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
     */
    query(sql: string): Promise<string>;
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
     */
    queryWithParams(sql: string, params?: string | null): Promise<string>;
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
     */
    refresh_access_token(refresh_token: string): Promise<any>;
    /**
     * Send a single application-level keepalive ping to the server.
     *
     * Usually called automatically by the internal ping timer; exposed so
     * callers can send an ad-hoc ping if needed.
     */
    sendPing(): void;
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
     */
    setAuthProvider(callback: Function): void;
    /**
     * Enable or disable automatic reconnection
     *
     * # Arguments
     * * `enabled` - Whether to automatically reconnect on connection loss
     */
    setAutoReconnect(enabled: boolean): void;
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
     */
    setDisableCompression(disable: boolean): void;
    /**
     * Set maximum reconnection attempts
     *
     * # Arguments
     * * `max_attempts` - Maximum number of attempts (0 = infinite)
     */
    setMaxReconnectAttempts(max_attempts: number): void;
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
     */
    setPingInterval(ms: number): void;
    /**
     * Set reconnection delay parameters
     *
     * # Arguments
     * * `initial_delay_ms` - Initial delay in milliseconds between reconnection attempts
     * * `max_delay_ms` - Maximum delay (for exponential backoff)
     */
    setReconnectDelay(initial_delay_ms: bigint, max_delay_ms: bigint): void;
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
     */
    setWsLazyConnect(lazy: boolean): void;
    /**
     * Subscribe to table changes (T051, T063I-T063J)
     *
     * # Arguments
     * * `table_name` - Name of the table to subscribe to
     * * `callback` - JavaScript function to call when changes occur
     *
     * # Returns
     * Subscription ID for later unsubscribe
     */
    subscribe(table_name: string, callback: Function): Promise<string>;
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
     */
    subscribeWithSql(sql: string, options: string | null | undefined, callback: Function): Promise<string>;
    /**
     * Unsubscribe from table changes (T052, T063M)
     *
     * # Arguments
     * * `subscription_id` - ID returned from subscribe()
     */
    unsubscribe(subscription_id: string): Promise<void>;
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
     */
    static withJwt(url: string, token: string): KalamClient;
}

/**
 * WASM wrapper for TimestampFormatter
 */
export class WasmTimestampFormatter {
    free(): void;
    [Symbol.dispose](): void;
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
     */
    format(milliseconds?: number | null): string;
    /**
     * Format a timestamp as relative time (e.g., "2 hours ago")
     *
     * # Arguments
     * * `milliseconds` - Timestamp in milliseconds since Unix epoch
     *
     * # Returns
     * Relative time string (e.g., "just now", "5 minutes ago", "2 days ago")
     */
    formatRelative(milliseconds: number): string;
    /**
     * Create a new timestamp formatter with ISO 8601 format
     */
    constructor();
    /**
     * Create a formatter with a specific format
     *
     * # Arguments
     * * `format` - One of: "iso8601", "iso8601-date", "iso8601-datetime", "unix-ms", "unix-sec", "relative", "rfc2822", "rfc3339"
     */
    static withFormat(format: string): WasmTimestampFormatter;
}

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
 */
export function parseIso8601(iso_string: string): number;

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
 */
export function timestampNow(): number;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_kalamclient_free: (a: number, b: number) => void;
    readonly __wbg_wasmtimestampformatter_free: (a: number, b: number) => void;
    readonly kalamclient_anonymous: (a: number, b: number, c: number) => void;
    readonly kalamclient_clearAuthProvider: (a: number) => void;
    readonly kalamclient_connect: (a: number) => number;
    readonly kalamclient_delete: (a: number, b: number, c: number, d: number, e: number) => number;
    readonly kalamclient_disconnect: (a: number) => number;
    readonly kalamclient_getAuthType: (a: number, b: number) => void;
    readonly kalamclient_getLastSeqId: (a: number, b: number, c: number, d: number) => void;
    readonly kalamclient_getReconnectAttempts: (a: number) => number;
    readonly kalamclient_getSubscriptions: (a: number) => number;
    readonly kalamclient_insert: (a: number, b: number, c: number, d: number, e: number) => number;
    readonly kalamclient_isConnected: (a: number) => number;
    readonly kalamclient_isReconnecting: (a: number) => number;
    readonly kalamclient_liveQueryRowsWithSql: (a: number, b: number, c: number, d: number, e: number, f: number) => number;
    readonly kalamclient_login: (a: number) => number;
    readonly kalamclient_new: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly kalamclient_onConnect: (a: number, b: number) => void;
    readonly kalamclient_onDisconnect: (a: number, b: number) => void;
    readonly kalamclient_onError: (a: number, b: number) => void;
    readonly kalamclient_onReceive: (a: number, b: number) => void;
    readonly kalamclient_onSend: (a: number, b: number) => void;
    readonly kalamclient_query: (a: number, b: number, c: number) => number;
    readonly kalamclient_queryWithParams: (a: number, b: number, c: number, d: number, e: number) => number;
    readonly kalamclient_refresh_access_token: (a: number, b: number, c: number) => number;
    readonly kalamclient_sendPing: (a: number, b: number) => void;
    readonly kalamclient_setAuthProvider: (a: number, b: number) => void;
    readonly kalamclient_setAutoReconnect: (a: number, b: number) => void;
    readonly kalamclient_setDisableCompression: (a: number, b: number) => void;
    readonly kalamclient_setMaxReconnectAttempts: (a: number, b: number) => void;
    readonly kalamclient_setPingInterval: (a: number, b: number) => void;
    readonly kalamclient_setReconnectDelay: (a: number, b: bigint, c: bigint) => void;
    readonly kalamclient_setWsLazyConnect: (a: number, b: number) => void;
    readonly kalamclient_subscribe: (a: number, b: number, c: number, d: number) => number;
    readonly kalamclient_subscribeWithSql: (a: number, b: number, c: number, d: number, e: number, f: number) => number;
    readonly kalamclient_unsubscribe: (a: number, b: number, c: number) => number;
    readonly kalamclient_withJwt: (a: number, b: number, c: number, d: number, e: number) => void;
    readonly parseIso8601: (a: number, b: number, c: number) => void;
    readonly wasmtimestampformatter_format: (a: number, b: number, c: number, d: number) => void;
    readonly wasmtimestampformatter_formatRelative: (a: number, b: number, c: number) => void;
    readonly wasmtimestampformatter_new: () => number;
    readonly wasmtimestampformatter_withFormat: (a: number, b: number, c: number) => void;
    readonly timestampNow: () => number;
    readonly __wasm_bindgen_func_elem_716: (a: number, b: number, c: number, d: number) => void;
    readonly __wasm_bindgen_func_elem_738: (a: number, b: number, c: number, d: number) => void;
    readonly __wasm_bindgen_func_elem_3555: (a: number, b: number, c: number) => void;
    readonly __wasm_bindgen_func_elem_3555_2: (a: number, b: number, c: number) => void;
    readonly __wasm_bindgen_func_elem_3555_3: (a: number, b: number, c: number) => void;
    readonly __wasm_bindgen_func_elem_3554: (a: number, b: number) => void;
    readonly __wbindgen_export: (a: number, b: number) => number;
    readonly __wbindgen_export2: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_export3: (a: number) => void;
    readonly __wbindgen_export4: (a: number, b: number) => void;
    readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
    readonly __wbindgen_export5: (a: number, b: number, c: number) => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
