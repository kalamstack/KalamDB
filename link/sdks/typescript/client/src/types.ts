/**
 * Type definitions for the @kalamdb/client SDK
 *
 * Public SDK types are defined here so the published package does not depend
 * on generated wasm model declarations for its TypeScript surface.
 */

/* ================================================================== */
/*  Shared transport/model types                                       */
/* ================================================================== */

/**
 * Any JSON-compatible value exchanged with the SDK.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type JsonValue = any;

import type { SeqId as TypedSeqId } from './seq_id.js';

type WireSeqId = TypedSeqId | number | string;
type WireRowData = Record<string, JsonValue>;

type CompressionType = 'none' | 'gzip';
type SerializationType = 'json' | 'msgpack';

interface ProtocolOptions {
  serialization: SerializationType;
  compression: CompressionType;
}

// SeqId: SDK-level typed wrapper (replaces WASM's plain `number` alias)
export { SeqId } from './seq_id.js';

export type FieldFlag = 'pk' | 'nn' | 'uq';
export type FieldFlags = FieldFlag[];

export type BatchStatus = 'loading' | 'loading_batch' | 'ready';

export interface BatchControl {
  batch_num: number;
  has_more: boolean;
  status: BatchStatus;
  last_seq_id?: WireSeqId;
  snapshot_end_seq?: WireSeqId;
}

export type ChangeTypeRaw = 'insert' | 'update' | 'delete';

export interface ErrorDetail {
  code: string;
  message: string;
  details?: string;
}

export interface HealthCheckResponse {
  status: string;
  version?: string;
  api_version: string;
  build_date?: string;
}

export type HttpVersion = 'http1' | 'http2' | 'auto';

export type KalamDataType =
  | 'Boolean'
  | 'Int'
  | 'BigInt'
  | 'Double'
  | 'Float'
  | 'Text'
  | 'Timestamp'
  | 'Date'
  | 'DateTime'
  | 'Time'
  | 'Json'
  | 'Bytes'
  | { Embedding: number }
  | 'Uuid'
  | { Decimal: { precision: number; scale: number } }
  | 'SmallInt'
  | 'File';

export interface LoginUserInfo {
  id: string;
  username: string;
  role: string;
  email?: string;
  created_at: string;
  updated_at: string;
}

export interface LoginResponse {
  user: LoginUserInfo;
  expires_at: string;
  access_token: string;
  refresh_token?: string;
  refresh_expires_at?: string;
}

export interface SchemaField {
  name: string;
  data_type: KalamDataType;
  index: number;
  flags?: FieldFlags;
}

export interface QueryResult {
  schema?: SchemaField[];
  rows?: JsonValue[][] | WireRowData[];
  named_rows?: WireRowData[];
  row_count: number;
  message?: string;
}

export type ResponseStatus = 'success' | 'error';

export interface QueryResponse {
  status: ResponseStatus;
  results?: QueryResult[];
  took?: number;
  error?: ErrorDetail;
}

export type TimestampFormat =
  | 'iso8601'
  | 'iso8601-date'
  | 'iso8601-datetime'
  | 'unix-ms'
  | 'unix-sec'
  | 'relative'
  | 'rfc2822'
  | 'rfc3339';

export interface UploadProgress {
  file_index: number;
  total_files: number;
  file_name: string;
  bytes_sent: number;
  total_bytes: number;
  percent: number;
}

export type ServerMessage =
  | { type: 'auth_success'; user_id: string; role: string; protocol: ProtocolOptions }
  | { type: 'auth_error'; message: string }
  | {
      type: 'subscription_ack';
      subscription_id: string;
      total_rows: number;
      batch_control: BatchControl;
      schema: SchemaField[];
    }
  | {
      type: 'initial_data_batch';
      subscription_id: string;
      rows: WireRowData[];
      batch_control: BatchControl;
    }
  | {
      type: 'change';
      subscription_id: string;
      change_type: ChangeTypeRaw;
      rows?: WireRowData[];
      old_values?: WireRowData[];
    }
  | { type: 'error'; subscription_id: string; code: string; message: string };

/**
 * Type-safe subscription options exposed by the SDK.
 *
 * The SDK accepts a real `SeqId` for resume checkpoints and normalizes it to
 * the wire format expected by the underlying transport.
 */
export interface SubscriptionOptions {
  /** Hint for server-side batch sizing during initial data load. */
  batch_size?: number;
  /** Number of newest rows to fetch for the initial snapshot. */
  last_rows?: number;
  /** Resume from a specific sequence ID. */
  from?: WireSeqId;
}

/* ================================================================== */
/*  KalamCellValue & RowData (SDK-level typed cell wrapper)          */
/* ================================================================== */

export { KalamCellValue, wrapRowMap } from './cell_value.js';
export type { RowData } from './cell_value.js';

/* ================================================================== */
/*  Convenience enums (for runtime values, not just types)            */
/* ================================================================== */

/**
 * Message type enum for WebSocket subscription events.
 * Use these constants when comparing `ServerMessage.type` at runtime.
 */
export enum MessageType {
  SubscriptionAck = 'subscription_ack',
  InitialDataBatch = 'initial_data_batch',
  Change = 'change',
  Error = 'error',
}

/**
 * Change type enum for live subscription change events.
 * Runtime-usable values matching `ChangeTypeRaw`.
 */
export enum ChangeType {
  Insert = 'insert',
  Update = 'update',
  Delete = 'delete',
}

/**
 * Log severity levels, ordered from most verbose to least.
 */
export enum LogLevel {
  /** Fine-grained messages useful only when diagnosing SDK internals. */
  Verbose = 0,
  /** Developer-oriented messages for debugging connectivity issues. */
  Debug = 1,
  /** Noteworthy lifecycle events (connect, disconnect, auth refresh). */
  Info = 2,
  /** Potential problems that do not prevent normal operation. */
  Warn = 3,
  /** Failures that require attention (auth errors, connection loss). */
  Error = 4,
  /** Logging disabled — no messages are emitted. */
  None = 5,
}

/**
 * A single log entry emitted by the SDK.
 */
export interface LogEntry {
  /** Severity level. */
  level: LogLevel;
  /** Human-readable log message. */
  message: string;
  /** The SDK component that produced this message (e.g. `'connection'`). */
  tag: string;
  /** ISO-8601 timestamp when the log was created. */
  timestamp: string;
}

/**
 * Callback invoked for every log entry at or above the configured log level.
 *
 * @param entry - The log entry
 */
export type LogListener = (entry: LogEntry) => void;

/* ================================================================== */
/*  Client-only types (TypeScript SDK specific, no Rust equivalent)   */
/* ================================================================== */

/**
 * Subscription callback function type
 */
export type SubscriptionCallback = (event: ServerMessage) => void;

/**
 * Typed subscription callback for convenience.
 *
 * @example
 * ```typescript
 * interface ChatMessage { id: string; content: string; sender: string }
 *
 * const handleEvent: TypedSubscriptionCallback<ChatMessage> = (event) => {
 *   if (event.type === 'change' && event.rows) {
 *     const messages: ChatMessage[] = event.rows;
 *   }
 * };
 * ```
 */
export type TypedSubscriptionCallback<T extends Record<string, unknown>> = (
  event: ServerMessage & { rows?: T[]; old_values?: T[] },
) => void;

/**
 * Subscription error event shape.
 */
export type SubscriptionErrorEvent = Extract<
  ServerMessage,
  { type: 'error' }
>;

/**
 * Callback that receives the fully materialized row set for a live query.
 */
export type LiveRowsCallback<T> = (rows: T[]) => void;

/**
 * Options for SDK-managed live query row materialization.
 */
export interface LiveRowsOptions<T> {
  /**
   * Map each incoming `RowData` into an application-level shape.
   * Defaults to the raw `RowData` object.
   */
  mapRow?: (row: import('./cell_value.js').RowData) => T;
  /**
   * Column names that together identify a stable row in Rust-side materialization.
   *
   * Use this when your query does not expose a plain `id` column but does
   * expose a stable natural or composite key.
   */
  keyColumns?: string[];
  /**
   * Deprecated alias for `keyColumns`.
   */
  keyColumn?: string | string[];
  /**
   * Resolve a stable key for upsert/delete handling.
   *
   * When this is provided, live row reconciliation falls back to the
   * TypeScript layer because arbitrary JavaScript callbacks cannot be shared
   * with the Rust core.
   */
  getKey?: (row: T) => string | null | undefined;
  /**
   * Subscription-level options passed through to the server.
   */
  subscriptionOptions?: SubscriptionOptions;
  /**
   * Optional error callback for post-start subscription failures.
   */
  onError?: (event: SubscriptionErrorEvent) => void;
}

/**
 * Function to unsubscribe from a subscription (Firebase/Supabase style)
 */
export type Unsubscribe = () => Promise<void>;

/* ================================================================== */
/*  Connection Lifecycle Event Handlers                               */
/* ================================================================== */

/**
 * Reason for a disconnect event.
 */
export interface DisconnectReason {
  /** Human-readable description of why the connection closed. */
  message: string;
  /** WebSocket close code, if available (e.g. 1000 = normal, 1006 = abnormal). */
  code?: number;
}

/**
 * Error information passed to the onError handler.
 */
export interface ConnectionError {
  /** Human-readable error message. */
  message: string;
  /** Whether this error is recoverable (i.e. auto-reconnect may succeed). */
  recoverable: boolean;
}

/**
 * Callback invoked when the WebSocket connection is established and authenticated.
 */
export type OnConnectCallback = () => void;

/**
 * Callback invoked when the WebSocket connection is closed.
 *
 * @param reason - Details about why the connection was closed
 */
export type OnDisconnectCallback = (reason: DisconnectReason) => void;

/**
 * Callback invoked when a connection error occurs.
 *
 * @param error - Details about the error
 */
export type OnErrorCallback = (error: ConnectionError) => void;

/**
 * Callback invoked for every raw message received from the server.
 *
 * Debug/tracing hook — receives the raw JSON string before parsing.
 *
 * @param message - Raw message string
 */
export type OnReceiveCallback = (message: string) => void;

/**
 * Callback invoked for every raw message sent to the server.
 *
 * Debug/tracing hook — receives the raw JSON string of outgoing messages.
 *
 * @param message - Raw message string
 */
export type OnSendCallback = (message: string) => void;

/**
 * Information about an active subscription
 */
export interface SubscriptionInfo {
  /** Unique subscription ID */
  id: string;
  /** Table name or SQL query being subscribed to */
  tableName: string;
  /** Timestamp when subscription was created */
  createdAt: Date;
  /** Last received sequence ID (for resume on reconnect), if any */
  lastSeqId?: import('./seq_id.js').SeqId;
  /** Whether this subscription has been closed */
  closed: boolean;
}

/**
 * Type-safe username wrapper.
 *
 * Prevents confusion between usernames and other string identifiers.
 * Use `.toString()` or template literals to get the raw string value.
 */
export type Username = string & { readonly __brand: unique symbol };

/**
 * Create a type-safe Username from a raw string.
 */
export function Username(value: string): Username {
  return value as Username;
}

/* ================================================================== */
/*  Client Options                                                    */
/* ================================================================== */

/**
 * Configuration options for KalamDB client
 *
 * @example
 * ```typescript
 * const client = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'admin'),
 * });
 * ```
 */
export interface ClientOptions {
  /** Server URL (e.g., 'http://localhost:8080') */
  url: string;
  /**
   * Authentication provider callback.
   *
   * Called before each (re-)connection to obtain credentials — supports
   * JWT, basic, and anonymous auth. Ideal for refresh-token flows where
   * the callback is responsible for refreshing expired tokens.
   *
   * Return `Auth.jwt(token)` for JWT-based auth, `Auth.basic(user, pass)`
   * for username/password (the SDK automatically exchanges these for a JWT
   * before opening the WebSocket), or `Auth.none()` for anonymous access.
   *
   * @example
   * ```typescript
  * import { Auth, type AuthProvider } from '@kalamdb/client';
   *
   * // JWT with auto-refresh
   * const authProvider: AuthProvider = async () => {
   *   const token = await myApp.getOrRefreshJwt();
   *   return Auth.jwt(token);
   * };
   *
   * // Basic credentials (SDK handles JWT exchange internally)
   * const authProvider: AuthProvider = async () =>
   *   Auth.basic('admin', 'secret');
   *
   * const client = createClient({ url: '...', authProvider });
   * ```
   */
  authProvider: import('./auth.js').AuthProvider;
  /**
   * Maximum attempts used when resolving `authProvider` credentials on
   * transient failures (timeouts/network hiccups).
   *
   * Defaults to `3`.
   */
  authProviderMaxAttempts?: number;
  /**
   * Backoff before the first `authProvider` retry, in milliseconds.
   *
   * Defaults to `250`.
   */
  authProviderInitialBackoffMs?: number;
  /**
   * Maximum backoff cap for `authProvider` retries, in milliseconds.
   *
   * Defaults to `2000`.
   */
  authProviderMaxBackoffMs?: number;
  /**
   * Disable server-side gzip compression for WebSocket messages.
   *
   * When `true`, the client passes `?compress=false` in the WebSocket URL and
   * the server sends plain-text JSON frames instead of gzip-compressed binary
   * frames.  Useful during development for easier message inspection.
   *
   * Defaults to `false` (compression enabled).
   */
  disableCompression?: boolean;
  /**
   * Explicit URL or buffer for the WASM file.
  * - Browser: string URL like '/wasm/kalam_client_bg.wasm'
   * - Node.js: BufferSource (fs.readFileSync result)
   * Required in bundled environments where import.meta.url doesn't resolve.
   */
  wasmUrl?: string | BufferSource;
  /**
   * Control when the WebSocket connection is established.
   *
   * When `true` (the default), the WebSocket connection is deferred until
   * the first `subscribe()` or `subscribeWithSql()` call. This avoids
   * unnecessary connections when the client is only used for HTTP queries.
   *
   * When `false`, the WebSocket connection is established eagerly during
   * initialization (before any subscribe call). Use this when you want the
   * connection ready immediately.
   *
   * Authentication uses the `authProvider` configured on the client.
   * There is no need to call `connect()` manually — the SDK manages
   * the connection lifecycle automatically.
   *
   * Defaults to `true`.
   */
  wsLazyConnect?: boolean;

  /**
   * Interval in milliseconds at which the client sends an application-level
   * keepalive ping over the WebSocket connection.
   *
   * Browser WebSocket APIs do not expose protocol-level Ping frames, so the
   * SDK sends a JSON `{"type":"ping"}` message instead to prevent the
   * server-side heartbeat timeout from closing idle connections.
   *
  * Defaults to `5000` (5 seconds). Set to `0` to disable.
   */
  pingIntervalMs?: number;

  /**
   * Called when the WebSocket connection is established and authenticated.
   *
   * @example
   * ```typescript
   * const client = createClient({
   *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'secret'),
   *   onConnect: () => console.log('Connected!'),
   * });
   * ```
   */
  onConnect?: OnConnectCallback;

  /**
   * Called when the WebSocket connection is closed.
   *
   * @example
   * ```typescript
   * const client = createClient({
   *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'secret'),
   *   onDisconnect: (reason) => console.log('Disconnected:', reason.message),
   * });
   * ```
   */
  onDisconnect?: OnDisconnectCallback;

  /**
   * Called when a connection error occurs.
   *
   * @example
   * ```typescript
   * const client = createClient({
   *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'secret'),
   *   onError: (err) => console.error('Error:', err.message, 'recoverable:', err.recoverable),
   * });
   * ```
   */
  onError?: OnErrorCallback;

  /**
   * Called for every raw message received from the server.
   * Debug/tracing hook — not needed for normal operation.
   *
   * @example
   * ```typescript
   * const client = createClient({
   *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'secret'),
   *   onReceive: (msg) => console.log('[RECV]', msg),
   * });
   * ```
   */
  onReceive?: OnReceiveCallback;

  /**
   * Called for every raw message sent to the server.
   * Debug/tracing hook — not needed for normal operation.
   *
   * @example
   * ```typescript
   * const client = createClient({
   *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'secret'),
   *   onSend: (msg) => console.log('[SEND]', msg),
   * });
   * ```
   */
  onSend?: OnSendCallback;

  /**
   * Minimum log level for SDK diagnostic messages.
   *
   * Defaults to `LogLevel.Warn`. Set to `LogLevel.Debug` to see
   * connection lifecycle, keepalive pings, auth refreshes, etc.
   *
   * @example
   * ```typescript
   * createClient({ url, auth, logLevel: LogLevel.Debug });
   * ```
   */
  logLevel?: LogLevel;

  /**
   * Optional callback invoked for every log entry at or above `logLevel`.
   *
   * When not set, messages are logged via `console.log` / `console.warn`
   * / `console.error` depending on severity.
   *
   * @example
   * ```typescript
   * createClient({
   *   url, auth,
   *   logLevel: LogLevel.Debug,
   *   logListener: (entry) => myLogger.log(entry.level, entry.message),
   * });
   * ```
   */
  logListener?: LogListener;
}

