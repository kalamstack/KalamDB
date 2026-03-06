/* tslint:disable */
/* eslint-disable */
/**
 * A field in the result schema returned by SQL queries
 *
 * Contains all the information a client needs to properly interpret
 * column data, including the name, data type, and index.
 *
 * # Example (JSON representation)
 *
 * ```json
 * {
 *   \"name\": \"user_id\",
 *   \"data_type\": \"BigInt\",
 *   \"index\": 0,
 *   \"flags\": [\"pk\", \"nn\", \"uq\"]
 * }
 * ```
 */
export interface SchemaField {
    /**
     * Column name
     */
    name: string;
    /**
     * Data type using KalamDB\'s unified type system
     */
    data_type: KalamDataType;
    /**
     * Column position (0-indexed) in the result set
     */
    index: number;
    /**
     * Structured field flags (e.g. [\"pk\", \"nn\", \"uq\"]).
     */
    flags?: FieldFlags;
}

/**
 * A single cell value in a query result row or subscription notification.
 *
 * Thin wrapper around [`serde_json::Value`] with `#[serde(transparent)]`
 * for zero-cost JSON (de)serialization. Implements [`Deref`] to `JsonValue`
 * so all existing `serde_json` accessor methods (`.as_str()`, `.is_null()`,
 * `.as_i64()`, …) continue to work unchanged.
 *
 * Use the typed accessor methods (e.g. [`as_big_int`][Self::as_big_int],
 * [`as_file`][Self::as_file]) to get values in the correct Rust type
 * matching the column\'s [`KalamDataType`][super::kalam_data_type::KalamDataType].
 */
export type KalamCellValue = JsonValue;

/**
 * A single consumed message from a topic.
 *
 * Contains the message payload (decoded from base64), metadata about the
 * source operation, and positioning information for acknowledgment.
 */
export interface ConsumeMessage {
    /**
     * Unique message identifier
     */
    message_id?: string;
    /**
     * Source table that produced this message
     */
    source_table?: string;
    /**
     * Operation type: \"insert\", \"update\", or \"delete\
     */
    op?: string;
    /**
     * Message timestamp in milliseconds since epoch
     */
    timestamp_ms?: number;
    /**
     * Message offset in the partition (used for acknowledgment)
     */
    offset: number;
    /**
     * Partition this message belongs to
     */
    partition_id: number;
    /**
     * Topic this message belongs to
     */
    topic: string;
    /**
     * Consumer group ID
     */
    group_id: string;
    /**
     * Username of the user who produced this message/event
     */
    username?: Username;
    /**
     * Decoded message payload as a named-column row (`column → value`).
     * Mirrors the subscription row shape: `HashMap<String, KalamCellValue>`.
     */
    value: RowData;
}

/**
 * Batch control metadata for paginated initial data loading
 *
 * Note: We don\'t include total_batches because we can\'t know it upfront
 * without counting all rows first (expensive). The `has_more` field is
 * sufficient for clients to know whether to request more batches.
 */
export interface BatchControl {
    /**
     * Current batch number (0-indexed)
     */
    batch_num: number;
    /**
     * Whether more batches are available to fetch
     */
    has_more: boolean;
    /**
     * Loading status for the subscription
     */
    status: BatchStatus;
    /**
     * The SeqId of the last row in this batch (used for subsequent requests)
     */
    last_seq_id?: SeqId;
    /**
     * Snapshot boundary SeqId captured at subscription time
     */
    snapshot_end_seq?: SeqId;
}

/**
 * Contains query results, execution metadata, and optional error information.
 * Matches the server\'s SqlResponse structure.
 */
export interface QueryResponse {
    /**
     * Query execution status (\"success\" or \"error\")
     */
    status: ResponseStatus;
    /**
     * Array of result sets, one per executed statement
     */
    results?: QueryResult[];
    /**
     * Query execution time in milliseconds (with fractional precision)
     */
    took?: number;
    /**
     * Error details if status is \"error\
     */
    error?: ErrorDetail;
}

/**
 * Data type for schema fields in query results
 *
 * Represents the KalamDB data type system. Each variant maps to
 * an underlying storage representation.
 *
 * # Example JSON
 *
 * ```json
 * \"BigInt\"           // Simple type
 * {\"Embedding\": 384} // Parameterized type
 * {\"Decimal\": {\"precision\": 10, \"scale\": 2}} // Complex type
 * ```
 */
export type KalamDataType = "Boolean" | "Int" | "BigInt" | "Double" | "Float" | "Text" | "Timestamp" | "Date" | "DateTime" | "Time" | "Json" | "Bytes" | { Embedding: number } | "Uuid" | { Decimal: { precision: number; scale: number } } | "SmallInt" | "File";

/**
 * Error details for failed SQL execution
 */
export interface ErrorDetail {
    /**
     * Error code
     */
    code: string;
    /**
     * Human-readable error message
     */
    message: string;
    /**
     * Optional additional details
     */
    details?: string;
}

/**
 * File reference stored as JSON in FILE columns.
 *
 * Contains all metadata needed to locate and serve the file.
 * The server stores this as a JSON string inside FILE-typed columns.
 *
 * # JSON example
 *
 * ```json
 * {
 *   \"id\": \"1234567890123456789\",
 *   \"sub\": \"f0001\",
 *   \"name\": \"document.pdf\",
 *   \"size\": 1048576,
 *   \"mime\": \"application/pdf\",
 *   \"sha256\": \"abc123...\
 * }
 * ```
 */
export interface FileRef {
    /**
     * Unique file identifier (Snowflake ID).
     */
    id: string;
    /**
     * Subfolder name (e.g., `\"f0001\"`, `\"f0002\"`).
     */
    sub: string;
    /**
     * Original filename (preserved for display/download).
     */
    name: string;
    /**
     * File size in bytes.
     */
    size: number;
    /**
     * MIME type (e.g., `\"image/png\"`, `\"application/pdf\"`).
     */
    mime: string;
    /**
     * SHA-256 hash of file content (hex-encoded).
     */
    sha256: string;
    /**
     * Optional shard ID for shared tables.
     */
    shard?: number;
}

/**
 * HTTP protocol version to use for connections.
 *
 * HTTP/2 provides benefits like multiplexing multiple requests over a single
 * connection, header compression, and improved performance for multiple
 * concurrent requests.
 *
 * # Example
 *
 * ```rust
 * use kalam_link::{ConnectionOptions, HttpVersion};
 *
 * let options = ConnectionOptions::new()
 *     .with_http_version(HttpVersion::Http2);
 * ```
 */
export type HttpVersion = "http1" | "http2" | "auto";

/**
 * Health check response from the server
 */
export interface HealthCheckResponse {
    /**
     * Health status (e.g., \"healthy\")
     */
    status: string;
    /**
     * Server version
     */
    version?: string;
    /**
     * API version (e.g., \"v1\")
     */
    api_version: string;
    /**
     * Server build date
     */
    build_date?: string | undefined;
}

/**
 * Individual query result within a SQL response.
 */
export interface QueryResult {
    /**
     * Schema describing the columns in the result set.
     * Each field contains: name, data_type (KalamDataType), and index.
     */
    schema?: SchemaField[];
    /**
     * The result rows as arrays of values (ordered by schema index).
     * Populated by the server; cleared client-side once `named_rows` is built.
     */
    rows?: KalamCellValue[][];
    /**
     * Rows as maps keyed by column name (column → KalamCellValue).
     *
     * Populated client-side by [`populate_named_rows`] from `schema` + `rows`.
     * When present, SDKs should prefer this over positional `rows`.
     */
    named_rows?: RowData[];
    /**
     * Number of rows affected or returned.
     */
    row_count: number;
    /**
     * Optional message for non-query statements.
     */
    message?: string;
}

/**
 * Login response from the server
 */
export interface LoginResponse {
    /**
     * Authenticated user information
     */
    user: LoginUserInfo;
    /**
     * Token expiration time in RFC3339 format
     */
    expires_at: string;
    /**
     * JWT access token for subsequent API calls
     */
    access_token: string;
    /**
     * Refresh token for obtaining new access tokens (longer-lived)
     */
    refresh_token?: string | undefined;
    /**
     * Refresh token expiration time in RFC3339 format
     */
    refresh_expires_at?: string | undefined;
}

/**
 * Options for consuming messages from a topic.
 *
 * Controls polling behavior including batch size, partition targeting,
 * start offset, and long-poll timeout.
 *
 * # Example
 *
 * ```json
 * {
 *   \"topic\": \"orders\",
 *   \"group_id\": \"billing\",
 *   \"start\": \"latest\",
 *   \"batch_size\": 10,
 *   \"partition_id\": 0
 * }
 * ```
 */
export interface ConsumeRequest {
    /**
     * Topic to consume from (e.g., \"orders\" or \"chat.messages\")
     */
    topic: string;
    /**
     * Consumer group ID for coordinated consumption
     */
    group_id: string;
    /**
     * Where to start consuming: \"earliest\", \"latest\", or a numeric offset
     */
    start?: string;
    /**
     * Max messages to return per poll (default: 10)
     */
    batch_size?: number;
    /**
     * Partition to consume from (default: 0)
     */
    partition_id?: number;
    /**
     * Long-poll timeout in seconds (server holds connection until messages arrive)
     */
    timeout_seconds?: number;
    /**
     * Whether to automatically acknowledge messages after the handler returns
     */
    auto_ack?: boolean;
    /**
     * Max concurrent message handlers per partition (default: 1)
     */
    concurrency_per_partition?: number;
}

/**
 * Response status enum
 */
export type ResponseStatus = "success" | "error";

/**
 * Result of acknowledging consumed messages.
 */
export interface AckResponse {
    /**
     * Whether the acknowledgment was successful
     */
    success: boolean;
    /**
     * The offset that was acknowledged
     */
    acknowledged_offset: number;
}

/**
 * Result of consuming from a topic.
 *
 * Contains the batch of messages and metadata for pagination.
 */
export interface ConsumeResponse {
    /**
     * Consumed messages in this batch
     */
    messages: ConsumeMessage[];
    /**
     * Next offset to consume from (for subsequent polls)
     */
    next_offset: number;
    /**
     * Whether more messages are available beyond this batch
     */
    has_more: boolean;
}

/**
 * Sequence ID for MVCC versioning (Snowflake layout: timestamp | worker | seq)
 */
export type SeqId = number;

/**
 * Status of the initial data loading process
 */
export type BatchStatus = "loading" | "loading_batch" | "ready";

/**
 * Subscription options for a live query.
 *
 * These options control individual subscription behavior including:
 * - Initial data loading (batch_size, last_rows)
 * - Data resumption after reconnection (from_seq_id)
 *
 * Aligned with backend\'s SubscriptionOptions in kalamdb-commons/websocket.rs.
 *
 * # Example
 *
 * ```rust
 * use kalam_link::{SeqId, SubscriptionOptions};
 *
 * // Fetch last 100 rows with batch size of 50
 * let options = SubscriptionOptions::default()
 *     .with_batch_size(50)
 *     .with_last_rows(100);
 *
 * // Resume from a specific sequence ID after reconnection
 * let some_seq_id = SeqId::new(123);
 * let options = SubscriptionOptions::default()
 *     .with_from_seq_id(some_seq_id);
 * ```
 */
export interface SubscriptionOptions {
    /**
     * Hint for server-side batch sizing during initial data load.
     * Default: server-configured (typically 1000 rows per batch).
     */
    batch_size?: number;
    /**
     * Number of last (newest) rows to fetch for initial data.
     * Default: None (fetch all matching rows).
     */
    last_rows?: number;
    /**
     * Resume subscription from a specific sequence ID.
     * When set, the server will only send changes after this seq_id.
     * Typically set automatically during reconnection to resume from last received event.
     */
    from_seq_id?: SeqId;
}

/**
 * Timestamp format options.
 *
 * Controls how timestamps are displayed in query results and subscriptions.
 * Default format is ISO 8601 with milliseconds (`2024-12-14T15:30:45.123Z`).
 */
export type TimestampFormat = "iso8601" | "iso8601-date" | "iso8601-datetime" | "unix-ms" | "unix-sec" | "relative" | "rfc2822" | "rfc3339";

/**
 * Type of change that occurred in the database
 */
export type ChangeTypeRaw = "insert" | "update" | "delete";

/**
 * Type-safe wrapper for usernames in the kalam-link SDK.
 *
 * Prevents confusion between usernames and other string identifiers
 * (user IDs, topic names, group IDs, etc.) at compile time.
 *
 * # Example (Rust)
 * ```rust
 * let username = Username::new(\"alice\");
 * assert_eq!(username.as_str(), \"alice\");
 * ```
 *
 * # TypeScript
 * Generated via tsify as a branded `string` newtype.
 */
export type Username = string;

/**
 * Upload progress information for a single file.
 */
export interface UploadProgress {
    /**
     * 1-based index of the current file being uploaded.
     */
    file_index: number;
    /**
     * Total number of files in the upload.
     */
    total_files: number;
    /**
     * Filename being uploaded.
     */
    file_name: string;
    /**
     * Bytes sent so far for this file.
     */
    bytes_sent: number;
    /**
     * Total bytes for this file.
     */
    total_bytes: number;
    /**
     * Percent complete for this file (0-100).
     */
    percent: number;
}

/**
 * User information returned in login response
 */
export interface LoginUserInfo {
    /**
     * User ID
     */
    id: string;
    /**
     * Username
     */
    username: string;
    /**
     * User role (user, service, dba, system)
     */
    role: string;
    /**
     * User email (optional)
     */
    email: string | undefined;
    /**
     * Account creation time in RFC3339 format
     */
    created_at: string;
    /**
     * Account update time in RFC3339 format
     */
    updated_at: string;
}

/**
 * WebSocket message types sent from server to client
 */
export type ServerMessage = { type: "auth_success"; user_id: string; role: string } | { type: "auth_error"; message: string } | { type: "subscription_ack"; subscription_id: string; total_rows: number; batch_control: BatchControl; schema: SchemaField[] } | { type: "initial_data_batch"; subscription_id: string; rows: Map<string, KalamCellValue>[]; batch_control: BatchControl } | { type: "change"; subscription_id: string; change_type: ChangeTypeRaw; rows?: Map<string, KalamCellValue>[]; old_values?: Map<string, KalamCellValue>[] } | { type: "error"; subscription_id: string; code: string; message: string };

export type FieldFlag = "pk" | "nn" | "uq";


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
    free(): void;
    [Symbol.dispose](): void;
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
     */
    ack(topic: string, group_id: string, partition_id: number, upto_offset: bigint): Promise<any>;
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
     */
    consume(options: any): Promise<any>;
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
     *   - `resume_from_seq_id`: Resume from a specific sequence ID (internal use)
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
     *   JSON.stringify({ batch_size: 50, include_old_values: true }),
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
    readonly kalamclient_ack: (a: number, b: number, c: number, d: number, e: number, f: number, g: bigint) => any;
    readonly kalamclient_anonymous: (a: number, b: number) => [number, number, number];
    readonly kalamclient_clearAuthProvider: (a: number) => void;
    readonly kalamclient_connect: (a: number) => any;
    readonly kalamclient_consume: (a: number, b: any) => any;
    readonly kalamclient_delete: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly kalamclient_disconnect: (a: number) => any;
    readonly kalamclient_getAuthType: (a: number) => [number, number];
    readonly kalamclient_getLastSeqId: (a: number, b: number, c: number) => [number, number];
    readonly kalamclient_getReconnectAttempts: (a: number) => number;
    readonly kalamclient_getSubscriptions: (a: number) => any;
    readonly kalamclient_insert: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly kalamclient_isConnected: (a: number) => number;
    readonly kalamclient_isReconnecting: (a: number) => number;
    readonly kalamclient_login: (a: number) => any;
    readonly kalamclient_new: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number];
    readonly kalamclient_onConnect: (a: number, b: any) => void;
    readonly kalamclient_onDisconnect: (a: number, b: any) => void;
    readonly kalamclient_onError: (a: number, b: any) => void;
    readonly kalamclient_onReceive: (a: number, b: any) => void;
    readonly kalamclient_onSend: (a: number, b: any) => void;
    readonly kalamclient_query: (a: number, b: number, c: number) => any;
    readonly kalamclient_queryWithParams: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly kalamclient_refresh_access_token: (a: number, b: number, c: number) => any;
    readonly kalamclient_sendPing: (a: number) => [number, number];
    readonly kalamclient_setAuthProvider: (a: number, b: any) => void;
    readonly kalamclient_setAutoReconnect: (a: number, b: number) => void;
    readonly kalamclient_setDisableCompression: (a: number, b: number) => void;
    readonly kalamclient_setMaxReconnectAttempts: (a: number, b: number) => void;
    readonly kalamclient_setPingInterval: (a: number, b: number) => void;
    readonly kalamclient_setReconnectDelay: (a: number, b: bigint, c: bigint) => void;
    readonly kalamclient_setWsLazyConnect: (a: number, b: number) => void;
    readonly kalamclient_subscribe: (a: number, b: number, c: number, d: any) => any;
    readonly kalamclient_subscribeWithSql: (a: number, b: number, c: number, d: number, e: number, f: any) => any;
    readonly kalamclient_unsubscribe: (a: number, b: number, c: number) => any;
    readonly kalamclient_withJwt: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly __wbg_wasmtimestampformatter_free: (a: number, b: number) => void;
    readonly parseIso8601: (a: number, b: number) => [number, number, number];
    readonly timestampNow: () => number;
    readonly wasmtimestampformatter_format: (a: number, b: number, c: number) => [number, number];
    readonly wasmtimestampformatter_formatRelative: (a: number, b: number) => [number, number];
    readonly wasmtimestampformatter_new: () => number;
    readonly wasmtimestampformatter_withFormat: (a: number, b: number) => [number, number, number];
    readonly wasm_bindgen__closure__destroy__h2e71e468c9717f03: (a: number, b: number) => void;
    readonly wasm_bindgen__closure__destroy__hc4784aa82de56652: (a: number, b: number) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h6537501fed6ccdff: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h11188c184bbe4ba9: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h7b0911d7f5b0bb2d: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h7b0911d7f5b0bb2d_1: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h7b0911d7f5b0bb2d_2: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h96917d2f4337dacb: (a: number, b: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_start: () => void;
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
