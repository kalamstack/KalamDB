/**
 * Type definitions for kalam-link SDK
 *
 * Model types are auto-generated from Rust via tsify-next and exported from the
 * WASM bindings. Client-only types (no Rust equivalent) are defined here.
 */

/* ================================================================== */
/*  Re-exported WASM-generated types (single source of truth in Rust) */
/* ================================================================== */

/**
 * `JsonValue` is used by tsify for `serde_json::Value` fields.
 * In TypeScript, this corresponds to any valid JSON value.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type JsonValue = any;

import type { FieldFlag as WasmFieldFlag } from '../wasm/kalam_link.js';

export type {
  AckResponse,
  BatchControl,
  BatchStatus,
  ChangeTypeRaw,
  ConsumeMessage,
  ConsumeRequest,
  ConsumeResponse,
  ErrorDetail,
  HealthCheckResponse,
  HttpVersion,
  KalamDataType,
  LoginResponse,
  LoginUserInfo,
  QueryResponse,
  QueryResult,
  ResponseStatus,
  SchemaField,
  ServerMessage,
  SubscriptionOptions,
  TimestampFormat,
  UploadProgress,
} from '../wasm/kalam_link.js';

// SeqId: SDK-level typed wrapper (replaces WASM's plain `number` alias)
export { SeqId } from './seq_id.js';

export type FieldFlag = WasmFieldFlag;
export type FieldFlags = FieldFlag[];

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
export type SubscriptionCallback = (event: import('../wasm/kalam_link.js').ServerMessage) => void;

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
  event: import('../wasm/kalam_link.js').ServerMessage & { rows?: T[]; old_values?: T[] },
) => void;

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

/* ================================================================== */
/*  Consumer Types (TypeScript SDK specific)                          */
/* ================================================================== */

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

/**
 * Context passed to the consumer handler callback.
 *
 * Contains the username of the user who triggered the event and
 * a method for manual message acknowledgment.
 */
export interface ConsumeContext {
  /** Username of the user who produced this message/event */
  readonly username: Username | undefined;
  /** The consumed message with decoded payload */
  readonly message: import('../wasm/kalam_link.js').ConsumeMessage;
  /** Acknowledge the current message (manual ack mode) */
  ack: () => Promise<void>;
}

/**
 * Consumer handler function signature.
 *
 * @param ctx - Context with username, message, and ack method
 */
export type ConsumerHandler = (
  ctx: ConsumeContext,
) => Promise<void>;

/**
 * Handle returned by `client.consumer()`. Call `.run()` to start consuming.
 */
export interface ConsumerHandle {
  /**
   * Start consuming messages, invoking the handler for each message.
   *
   * If `autoAck` was set in the consumer options, messages are acknowledged
   * automatically after the handler resolves. Otherwise, call `ctx.ack()`
   * inside the handler for manual acknowledgment.
   *
   * The returned promise resolves when `stop()` is called or the consumer
   * encounters an unrecoverable error.
   *
   * @example Auto-ack:
   * ```typescript
   * await client.consumer({ topic: "orders", group_id: "billing", auto_ack: true })
   *   .run(async (ctx) => {
   *     console.log("Order from:", ctx.username);
   *     console.log("Data:", ctx.message.value);
   *   });
   * ```
   *
   * @example Manual ack:
   * ```typescript
   * await client.consumer({ topic: "orders", group_id: "billing" })
   *   .run(async (ctx) => {
   *     await processOrder(ctx.message.value);
   *     await ctx.ack();
   *   });
   * ```
   */
  run: (handler: ConsumerHandler) => Promise<void>;

  /** Stop consuming (signals the run loop to break after the current batch) */
  stop: () => void;
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
   * import { Auth, type AuthProvider } from 'kalam-link';
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
   * - Browser: string URL like '/wasm/kalam_link_bg.wasm'
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
   * Defaults to `30000` (30 seconds). Set to `0` to disable.
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

/* ================================================================== */
/*  Agent Runtime Types                                               */
/* ================================================================== */

export type {
  AgentContext,
  AgentFailureContext,
  AgentFailureHandler,
  AgentLLMAdapter,
  AgentLLMContext,
  AgentLLMInput,
  AgentLLMMessage,
  AgentLLMRole,
  AgentRetryPolicy,
  AgentRowParser,
  AgentRunKeyFactory,
  LangChainChatModelLike,
  RunAgentOptions,
  RunConsumerOptions,
} from './agent.js';
