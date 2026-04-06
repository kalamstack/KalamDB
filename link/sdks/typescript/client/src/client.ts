/**
 * KalamDBClient — TypeScript wrapper around the WASM bindings
 *
 * Most network I/O (queries, subscriptions, authentication)
 * goes through the compiled Rust/WASM `KalamClient`. File uploads use direct
 * fetch() + FormData for better browser compatibility.
 */

import init, { KalamClient as WasmClient } from '../wasm/kalam_client.js';

import type { AuthCredentials, AuthProvider } from './auth.js';
import { buildAuthHeader } from './auth.js';
import { resolveAuthProviderWithRetry } from './helpers/auth_provider_retry.js';
import {
  defaultRowKey,
  type NormalizedSubscriptionEvent,
  normalizeLiveRowsOptions,
  normalizeLiveRowsWasmEvent,
  normalizeSubscriptionEvent,
  normalizeSubscriptionOptions,
  readSubscriptionInfos,
  removeMaterializedRows,
  trackSubscriptionMetadata,
  upsertLimited,
} from './helpers/subscription_helpers.js';

import type {
  ClientOptions,
  LiveRowsCallback,
  LiveRowsOptions,
  LogEntry,
  LogListener,
  LoginResponse,
  OnConnectCallback,
  OnDisconnectCallback,
  OnErrorCallback,
  OnReceiveCallback,
  OnSendCallback,
  QueryResponse,
  SubscriptionCallback,
  SubscriptionErrorEvent,
  SubscriptionOptions,
  Unsubscribe,
  UploadProgress,
  Username,
  ServerMessage,
  SubscriptionInfo,
} from './types.js';

import { LogLevel } from './types.js';
import { SeqId } from './seq_id.js';

import { wrapRowMap } from './cell_value.js';
import type { RowData } from './cell_value.js';

import {
  FileRefContext,
  KalamChange,
  KalamRow,
  wrapRows,
} from './file_ref.js';

// Re-export so consumers of client.ts don't need a separate file_ref import.
export { KalamChange, KalamRow, wrapRows };
export type { FileRefContext };

type DynamicImport = (specifier: string) => Promise<Record<string, unknown>>;

type NodeProcessShim = {
  versions?: {
    node?: string;
  };
};

type NodeWindowShim = {
  location?: {
    protocol: string;
    hostname: string;
    port: string;
    href: string;
  };
  fetch?: typeof fetch;
};

type NodeFsPromisesShim = {
  readFile(path: string): Promise<BufferSource>;
};

type NodeUrlShim = {
  fileURLToPath(url: URL): string;
};

type WasmAuthProviderResult = { jwt: { token: string } } | null;

const dynamicImport = new Function(
  'specifier',
  'return import(specifier)',
) as DynamicImport;

function getNodeProcess(): NodeProcessShim | undefined {
  const runtime = globalThis as typeof globalThis & {
    process?: NodeProcessShim;
  };
  return runtime.process;
}

/* ================================================================== */
/*  KalamDBClient                                                     */
/* ================================================================== */

/**
 * KalamDB Client — type-safe interface to KalamDB
 *
 * Features:
 * - SQL query execution (via WASM)
 * - Real-time WebSocket subscriptions
 * - Multiple auth methods (Basic, JWT, Anonymous)
 * - Cross-platform (Node.js & Browser)
 * - Subscription tracking & management
 *
 * @example
 * ```typescript
 * import { createClient, Auth } from '@kalamdb/client';
 *
 * const client = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('alice', 'password123'),
 * });
 *
 * const users = await client.query('SELECT * FROM users WHERE active = true');
 * console.log(users.results[0].rows);
 *
 * const unsubscribe = await client.subscribe('messages', (event) => {
 *   if (event.type === 'change') console.log('New:', event.rows);
 * });
 *
 * await unsubscribe();
 * await client.disconnect();
 * ```
 */
export class KalamDBClient {
  private wasmClient: WasmClient | null = null;
  private initialized = false;
  private connecting: Promise<void> | null = null;
  private url: string;
  /** Current auth state — updated by initialize() and mutated by login()/refreshToken(). */
  private auth: AuthCredentials = { type: 'none' };
  private _authProvider: AuthProvider;
  private authProviderMaxAttempts: number;
  private authProviderInitialBackoffMs: number;
  private authProviderMaxBackoffMs: number;
  private disableCompression: boolean;
  private wasmUrl?: string | BufferSource;
  private wsLazyConnect: boolean;
  private pingIntervalMs: number;
  private autoReconnectEnabled = true;

  /** Current minimum log level. */
  private _logLevel: LogLevel;
  /** Optional log listener for redirecting SDK logs. */
  private _logListener?: LogListener;

  /** Local metadata only; WASM remains the source of truth for live subscription state. */
  private subscriptionMetadata = new Map<string, { tableName: string; createdAtMs: number }>();

  /** Connection lifecycle event handlers */
  private _onConnect?: OnConnectCallback;
  private _onDisconnect?: OnDisconnectCallback;
  private _onError?: OnErrorCallback;
  private _onReceive?: OnReceiveCallback;
  private _onSend?: OnSendCallback;

  /**
   * Create a new KalamDB client
   *
   * @param options - Client options with URL and authProvider
   * @throws Error if url or authProvider is missing
   *
   * @example
   * ```typescript
  * import { KalamDBClient, Auth } from '@kalamdb/client';
   *
   * const client = new KalamDBClient({
   *   url: 'http://localhost:8080',
   *   authProvider: async () => Auth.basic('admin', 'secret'),
   * });
   * ```
   */
  constructor(options: ClientOptions) {
    if (!options.url) throw new Error('KalamDBClient: url is required');
    if (!options.authProvider) {
      throw new Error('KalamDBClient: authProvider is required');
    }

    this.url = options.url;
    this._authProvider = options.authProvider;
    this.authProviderMaxAttempts = options.authProviderMaxAttempts ?? 3;
    this.authProviderInitialBackoffMs = options.authProviderInitialBackoffMs ?? 250;
    this.authProviderMaxBackoffMs = options.authProviderMaxBackoffMs ?? 2000;
    this.disableCompression = options.disableCompression ?? false;
    this.wasmUrl = options.wasmUrl;
    this.wsLazyConnect = options.wsLazyConnect ?? true;
    this.pingIntervalMs = options.pingIntervalMs ?? 5_000;
    this._logLevel = options.logLevel ?? LogLevel.Warn;
    this._logListener = options.logListener;
    this._onConnect = options.onConnect;
    this._onDisconnect = options.onDisconnect;
    this._onError = options.onError;
    this._onReceive = options.onReceive;
    this._onSend = options.onSend;
  }

  /* ---------------------------------------------------------------- */
  /*  Logging                                                         */
  /* ---------------------------------------------------------------- */

  /**
   * Set the minimum log level at runtime.
   *
   * @example
   * ```typescript
   * client.setLogLevel(LogLevel.Debug);
   * ```
   */
  setLogLevel(level: LogLevel): void {
    this._logLevel = level;
  }

  /**
   * Set a log listener to redirect SDK logs.
   *
   * @example
   * ```typescript
   * client.setLogListener((entry) => myLogger.log(entry.message));
   * ```
   */
  setLogListener(listener: LogListener | undefined): void {
    this._logListener = listener;
  }

  /** @internal Emit a log entry if at or above the configured level. */
  private log(level: LogLevel, tag: string, message: string): void {
    if (level < this._logLevel) return;
    const entry: LogEntry = {
      level,
      tag,
      message,
      timestamp: new Date().toISOString(),
    };
    if (this._logListener) {
      this._logListener(entry);
      return;
    }
    const prefix = `[@kalamdb/client] [${LogLevel[level]?.toUpperCase() ?? level}] [${tag}]`;
    switch (level) {
      case LogLevel.Error:
        console.error(prefix, message);
        break;
      case LogLevel.Warn:
        console.warn(prefix, message);
        break;
      case LogLevel.Debug:
      case LogLevel.Verbose:
        // console.debug may be swallowed in some environments
        console.log(prefix, message);
        break;
      default:
        console.log(prefix, message);
    }
  }

  /* ---------------------------------------------------------------- */
  /*  Auth helpers                                                    */
  /* ---------------------------------------------------------------- */

  /** Get the current authentication type */
  getAuthType(): 'basic' | 'jwt' | 'none' {
    return this.auth.type;
  }

  /* ---------------------------------------------------------------- */
  /*  Lifecycle                                                       */
  /* ---------------------------------------------------------------- */

  /**
   * Initialize WASM module and create client instance.
   * Called automatically before the first operation that requires it.
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      await this.ensureNodeRuntimeCompat();
      this.log(LogLevel.Debug, 'init', 'Starting WASM initialization...');

      if (this.wasmUrl) {
        const kind = typeof this.wasmUrl === 'string' ? 'URL' : 'buffer';
        this.log(LogLevel.Debug, 'init', `Loading WASM from explicit ${kind}`);
        await init({ module_or_path: this.wasmUrl });
      } else {
        this.log(LogLevel.Debug, 'init', 'Loading WASM from default path');
        await init();
      }

      // Resolve initial credentials from the authProvider.
      this.log(LogLevel.Debug, 'init', 'Resolving initial credentials from authProvider...');
      const initialCreds = await resolveAuthProviderWithRetry(this._authProvider, {
        maxAttempts: this.authProviderMaxAttempts,
        initialBackoffMs: this.authProviderInitialBackoffMs,
        maxBackoffMs: this.authProviderMaxBackoffMs,
      });
      this.auth = initialCreds;
      this.log(LogLevel.Debug, 'init', `Initial credentials resolved: type=${initialCreds.type}`);

      switch (initialCreds.type) {
        case 'basic':
          // Keep basic credentials in TypeScript and exchange them for JWT
          // before the first authenticated operation.
          this.wasmClient = WasmClient.anonymous(this.url);
          this.log(LogLevel.Debug, 'init', 'Created anonymous WASM client placeholder for basic auth');
          break;
        case 'jwt':
          this.wasmClient = WasmClient.withJwt(this.url, initialCreds.token);
          this.log(LogLevel.Debug, 'init', 'Created WASM client with JWT auth');
          break;
        case 'none':
          this.wasmClient = WasmClient.anonymous(this.url);
          this.log(LogLevel.Debug, 'init', 'Created WASM client with anonymous auth');
          break;
      }

      // Wire authProvider for WebSocket (re-)connections.
      this.log(LogLevel.Debug, 'init', 'Wiring async auth provider');
      this.attachWasmClientState();

      this.initialized = true;
      this.log(LogLevel.Info, 'init', 'WASM initialization complete');

      // Eager connect: when wsLazyConnect is false, connect immediately.
      if (!this.wsLazyConnect) {
        this.log(LogLevel.Debug, 'init', 'wsLazyConnect=false — connecting eagerly...');
        await this.connectInternal();
      }
    } catch (error) {
      this.log(LogLevel.Error, 'init', `WASM initialization failed: ${error}`);
      throw new Error(`Failed to initialize WASM client: ${error}`);
    }
  }

  /**
   * Connect to KalamDB server via WebSocket.
   *
   * This is managed internally — you do not need to call it.
   * When `wsLazyConnect` is `true` (default), the SDK connects
   * automatically on the first `subscribe()` / `subscribeWithSql()` call.
   * When `wsLazyConnect` is `false`, the SDK connects during
   * `initialize()`.
   *
   * When using Basic auth, `login()` is called automatically to exchange
   * credentials for a JWT before opening the WebSocket.
   *
   * @internal
   */
  private async connectInternal(): Promise<void> {
    // Deduplicate concurrent connectInternal() calls — only one handshake at a time.
    if (this.connecting) return this.connecting;

    this.connecting = (async () => {
      try {
        this.log(LogLevel.Info, 'connection', `Connecting to ${this.url}...`);
        await this.initialize();
        if (!this.wasmClient) throw new Error('WASM client not initialized');
        this.wasmClient.setAutoReconnect(this.autoReconnectEnabled);

        // Auto-login: exchange Basic credentials for JWT before WebSocket connect.
        if (this.auth.type === 'basic') {
          this.log(LogLevel.Debug, 'auth', 'Auto-login: exchanging basic creds for JWT...');
          await this.login();
          this.log(LogLevel.Debug, 'auth', 'Auto-login successful');
        }

        // Always forward the effective ping interval so the TS and WASM
        // defaults cannot silently drift apart.
        this.log(LogLevel.Debug, 'connection', `Setting ping interval to ${this.pingIntervalMs}ms`);
        this.wasmClient.setPingInterval?.(this.pingIntervalMs);

        await this.wasmClient.connect();
        this.log(LogLevel.Info, 'connection', `Connected to ${this.url} successfully`);
      } finally {
        this.connecting = null;
      }
    })();

    return this.connecting;
  }

  /**
   * Ensure the WebSocket connection is established.
   *
   * Safe to call multiple times — deduplicates concurrent connect attempts.
   * @internal
   */
  private async ensureConnected(): Promise<void> {
    if (!this.isConnected()) {
      await this.connectInternal();
    }
  }

  /** Disconnect and clean up all subscriptions */
  async disconnect(): Promise<void> {
    this.log(LogLevel.Info, 'connection', 'Disconnecting...');
    this.clearSubscriptionMetadata();
    if (!this.wasmClient) {
      return;
    }

    this.wasmClient.setAutoReconnect(false);
    try {
      await this.wasmClient.disconnect();
      await new Promise((resolve) => setTimeout(resolve, 0));
    } finally {
      this.log(LogLevel.Debug, 'connection', 'Disconnected and subscriptions cleared');
    }
  }

  /**
   * Permanently tear down the current WASM client instance and release resources.
   *
   * Unlike `disconnect()`, this also frees the underlying WASM object and clears
   * local client state so long-lived Node.js processes and test runners do not
   * retain dormant sockets or callbacks after a suite completes.
   *
   * The client may be initialized again later if needed.
   */
  async shutdown(): Promise<void> {
    this.log(LogLevel.Info, 'connection', 'Shutting down client...');

    const wasmClient = this.wasmClient;
    this.wasmClient = null;
    this.initialized = false;
    this.connecting = null;
    this.clearSubscriptionMetadata();

    if (!wasmClient) {
      return;
    }

    try {
      wasmClient.setAutoReconnect(false);
      await wasmClient.disconnect();
      await new Promise((resolve) => setTimeout(resolve, 0));
    } finally {
      if (typeof wasmClient.free === 'function') {
        wasmClient.free();
      }
      this.log(LogLevel.Debug, 'connection', 'Client shutdown complete');
    }
  }

  /**
   * Async dispose — enables `await using client = createClient(...)`.
   *
   * Automatically disconnects and cleans up all subscriptions when the
   * variable goes out of scope. Requires Node 20+ or a modern browser
   * with Explicit Resource Management support.
   *
   * @example
   * ```typescript
   * async function demo() {
   *   await using client = createClient({ url, auth });
   *   // ... use client ...
   * } // client.disconnect() called automatically here
   * ```
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.shutdown();
  }

  /**
   * Sync dispose — enables `using client = createClient(...)`.
   *
   * Fires disconnect as fire-and-forget (best-effort) since `using`
   * does not await. Prefer `await using` for reliable cleanup.
   */
  [Symbol.dispose](): void {
    void this.shutdown();
  }

  /** Whether the WebSocket connection is active */
  isConnected(): boolean {
    return this.wasmClient?.isConnected() ?? false;
  }

  /* ---------------------------------------------------------------- */
  /*  Connection Event Handlers                                       */
  /* ---------------------------------------------------------------- */

  /**
   * Register a handler called when the WebSocket connection is established.
   *
   * Can also be set via `ClientOptions.onConnect` at construction time.
   *
   * @example
   * ```typescript
   * client.onConnect(() => console.log('Connected!'));
   * ```
   */
  onConnect(callback: OnConnectCallback): void {
    this._onConnect = callback;
    if (this.wasmClient) {
      this.wasmClient.onConnect(callback);
    }
  }

  /**
   * Register a handler called when the WebSocket connection is closed.
   *
   * Can also be set via `ClientOptions.onDisconnect` at construction time.
   *
   * @example
   * ```typescript
   * client.onDisconnect((reason) => console.log('Disconnected:', reason.message));
   * ```
   */
  onDisconnect(callback: OnDisconnectCallback): void {
    this._onDisconnect = callback;
    if (this.wasmClient) {
      this.wasmClient.onDisconnect(callback as unknown as Function);
    }
  }

  /**
   * Register a handler called when a connection error occurs.
   *
   * Can also be set via `ClientOptions.onError` at construction time.
   *
   * @example
   * ```typescript
   * client.onError((err) => console.error('Error:', err.message));
   * ```
   */
  onError(callback: OnErrorCallback): void {
    this._onError = callback;
    if (this.wasmClient) {
      this.wasmClient.onError(callback as unknown as Function);
    }
  }

  /**
   * Register a debug handler called for every raw message received from the server.
   *
   * Can also be set via `ClientOptions.onReceive` at construction time.
   *
   * @example
   * ```typescript
   * client.onReceive((msg) => console.log('[RECV]', msg));
   * ```
   */
  onReceive(callback: OnReceiveCallback): void {
    this._onReceive = callback;
    if (this.wasmClient) {
      this.wasmClient.onReceive(callback as unknown as Function);
    }
  }

  /**
   * Register a debug handler called for every raw message sent to the server.
   *
   * Can also be set via `ClientOptions.onSend` at construction time.
   *
   * @example
   * ```typescript
   * client.onSend((msg) => console.log('[SEND]', msg));
   * ```
   */
  onSend(callback: OnSendCallback): void {
    this._onSend = callback;
    if (this.wasmClient) {
      this.wasmClient.onSend(callback as unknown as Function);
    }
  }

  /** @internal Wire stored event handler callbacks to the WASM client */
  private applyEventHandlers(): void {
    if (!this.wasmClient) return;
    this.log(LogLevel.Debug, 'events', 'Wiring connection event handlers to WASM client');
    if (this._onConnect) this.wasmClient.onConnect(this._onConnect);
    if (this._onDisconnect) this.wasmClient.onDisconnect(this._onDisconnect as unknown as Function);
    if (this._onError) this.wasmClient.onError(this._onError as unknown as Function);
    if (this._onReceive) this.wasmClient.onReceive(this._onReceive as unknown as Function);
    if (this._onSend) this.wasmClient.onSend(this._onSend as unknown as Function);
  }

  /* ---------------------------------------------------------------- */
  /*  Reconnection                                                    */
  /* ---------------------------------------------------------------- */

  /** Enable/disable automatic reconnection */
  setAutoReconnect(enabled: boolean): void {
    this.autoReconnectEnabled = enabled;
    this.requireInit();
    this.wasmClient!.setAutoReconnect(enabled);
  }

  /** Set reconnection delay (initial + max for exponential back-off) */
  setReconnectDelay(initialDelayMs: number, maxDelayMs: number): void {
    this.requireInit();
    this.wasmClient!.setReconnectDelay(BigInt(initialDelayMs), BigInt(maxDelayMs));
  }

  /** Set max reconnection attempts (0 = infinite) */
  setMaxReconnectAttempts(maxAttempts: number): void {
    this.requireInit();
    this.wasmClient!.setMaxReconnectAttempts(maxAttempts);
  }

  /** Current reconnection attempt count (resets after success) */
  getReconnectAttempts(): number {
    this.requireInit();
    return this.wasmClient!.getReconnectAttempts();
  }

  /** Whether a reconnection is currently in progress */
  isReconnecting(): boolean {
    this.requireInit();
    return this.wasmClient!.isReconnecting();
  }

  /** Last received sequence ID for a subscription */
  getLastSeqId(subscriptionId: string): import('./seq_id.js').SeqId | undefined {
    this.requireInit();
    const raw = this.wasmClient!.getLastSeqId(subscriptionId);
    if (raw === null || raw === undefined) return undefined;
    try {
      return SeqId.from(raw);
    } catch {
      return undefined;
    }
  }

  /* ---------------------------------------------------------------- */
  /*  Queries                                                         */
  /* ---------------------------------------------------------------- */

  /**
   * Execute a SQL query with optional parameters.
   *
   * @example
   * ```typescript
   * const result = await client.query('SELECT * FROM users WHERE id = $1', [42]);
   * ```
   */
  async query(sql: string, params?: unknown[]): Promise<QueryResponse> {
    await this.initialize();
    await this.ensureJwtForBasicAuth();
    if (!this.wasmClient) throw new Error('WASM client not initialized');

    const resultStr = params?.length
      ? await this.wasmClient.queryWithParams(sql, JSON.stringify(params))
      : await this.wasmClient.query(sql);

    return JSON.parse(resultStr) as QueryResponse;
  }

  private static escapeSqlStringLiteral(value: string): string {
    return value.replace(/'/g, "''");
  }

  private static wrapExecuteAsUser(sql: string, username: string): string {
    const inner = sql.trim().replace(/;+\s*$/g, '');
    if (!inner) {
      throw new Error('executeAsUser requires a non-empty SQL statement');
    }
    const escapedUsername = KalamDBClient.escapeSqlStringLiteral(username.trim());
    if (!escapedUsername) {
      throw new Error('executeAsUser requires a non-empty username');
    }
    return `EXECUTE AS USER '${escapedUsername}' (${inner})`;
  }

  /**
   * Execute a single SQL statement as another user.
   *
   * Wraps the SQL using:
   * `EXECUTE AS USER 'username' ( <single statement> )`
   */
  async executeAsUser(
    sql: string,
    username: Username | string,
    params?: unknown[],
  ): Promise<QueryResponse> {
    const wrappedSql = KalamDBClient.wrapExecuteAsUser(sql, String(username));
    return this.query(wrappedSql, params);
  }

  /**
   * Execute a SQL query with file uploads (FILE datatype).
   *
   * Uses `fetch()` + multipart/form-data for reliable file upload handling.
   * Auth header is added automatically from WASM client's auth state.
   *
   * @example
   * ```typescript
   * await client.queryWithFiles(
   *   'INSERT INTO users (name, avatar) VALUES ($1, FILE("avatar"))',
   *   { avatar: fileBlob },
   *   ['Alice'],
   * );
   * ```
   */
  async queryWithFiles(
    sql: string,
    files: Record<string, File | Blob>,
    params?: unknown[],
    onProgress?: (progress: UploadProgress) => void,
  ): Promise<QueryResponse> {
    await this.initialize();
    await this.ensureJwtForBasicAuth();

    const formData = new FormData();
    formData.append('sql', sql);

    if (params?.length) {
      formData.append('params', JSON.stringify(params));
    }

    for (const [name, file] of Object.entries(files)) {
      const filename = file instanceof File ? file.name : name;
      formData.append(`file:${name}`, file, filename);
    }

    const headers: Record<string, string> = {};
    const authHeader = buildAuthHeader(this.auth);
    if (authHeader) headers['Authorization'] = authHeader;

    if (onProgress && typeof XMLHttpRequest !== 'undefined') {
      const entries = Object.entries(files);
      const sizes = entries.map(([, file]) => file.size || 0);
      const totalBytes = sizes.reduce((sum, size) => sum + size, 0);

      return await new Promise<QueryResponse>((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', `${this.url}/v1/api/sql`);
        if (authHeader) {
          xhr.setRequestHeader('Authorization', authHeader);
        }
        xhr.upload.onprogress = (event) => {
          if (!event.lengthComputable || totalBytes === 0) return;
          const loaded = Math.min(event.loaded, totalBytes);
          let cumulative = 0;
          let fileIndex = 0;
          while (fileIndex < sizes.length && loaded > cumulative + sizes[fileIndex]) {
            cumulative += sizes[fileIndex];
            fileIndex += 1;
          }
          const currentSize = sizes[fileIndex] || 1;
          const currentLoaded = Math.min(Math.max(loaded - cumulative, 0), currentSize);
          const percent = Math.min(100, Math.round((currentLoaded / currentSize) * 100));
          const [fileName] = entries[fileIndex] || ['unknown'];
          onProgress({
            file_index: fileIndex + 1,
            total_files: entries.length,
            file_name: fileName,
            bytes_sent: currentLoaded,
            total_bytes: currentSize,
            percent,
          });
        };
        xhr.onload = () => {
          try {
            const result = JSON.parse(xhr.responseText) as QueryResponse;
            if (xhr.status < 200 || xhr.status >= 300 || result.status !== 'success') {
              reject(new Error(result.error?.message || `Upload failed: ${xhr.status}`));
              return;
            }
            resolve(result);
          } catch (error) {
            reject(error instanceof Error ? error : new Error('Upload failed'));
          }
        };
        xhr.onerror = () => reject(new Error('Upload failed'));
        xhr.send(formData);
      });
    }

    const response = await fetch(`${this.url}/v1/api/sql`, {
      method: 'POST',
      headers,
      body: formData,
    });

    const result = (await response.json()) as QueryResponse;

    // Auto-refresh on TOKEN_EXPIRED: re-login and refresh auth for next requests.
    // Multipart uploads can't be replayed so we don't retry, but we refresh auth
    // so subsequent requests succeed.
    if (result.status === 'error' && result.error?.code === 'TOKEN_EXPIRED') {
      this.log(LogLevel.Warn, 'auth', 'TOKEN_EXPIRED on file upload — refreshing credentials');
      await this.reauthenticateFromAuthProvider();
    }

    if (!response.ok && result.status !== 'success') {
      throw new Error(result.error?.message || `Upload failed: ${response.status}`);
    }

    return result;
  }

  /* ---------------------------------------------------------------- */
  /*  Convenience Helpers                                             */
  /* ---------------------------------------------------------------- */

  /**
   * Insert data into a table.
   *
   * @example
   * ```typescript
   * await client.insert('todos', { title: 'Buy groceries', completed: false });
   * ```
   */
  async insert(tableName: string, data: Record<string, unknown>): Promise<QueryResponse> {
    await this.initialize();
    await this.ensureJwtForBasicAuth();
    this.requireInit();
    const resultStr = await this.wasmClient!.insert(tableName, JSON.stringify(data));
    return JSON.parse(resultStr) as QueryResponse;
  }

  /**
   * Delete a row by ID.
   *
   * @example
   * ```typescript
   * await client.delete('todos', '123456789');
   * ```
   */
  async delete(tableName: string, rowId: string | number): Promise<void> {
    await this.initialize();
    await this.ensureJwtForBasicAuth();
    this.requireInit();
    await this.wasmClient!.delete(tableName, String(rowId));
  }

  /**
   * Update a row by ID.
   *
   * @example
   * ```typescript
   * await client.update('chat.messages', '123', { status: 'delivered' });
   * ```
   */
  async update(
    tableName: string,
    rowId: string | number,
    data: Record<string, unknown>,
  ): Promise<QueryResponse> {
    const setClauses = Object.entries(data)
      .map(([key, value]) => {
        if (value === null) return `${key} = NULL`;
        if (typeof value === 'string') return `${key} = '${value.replace(/'/g, "''")}'`;
        if (typeof value === 'boolean') return `${key} = ${value}`;
        return `${key} = ${value}`;
      })
      .join(', ');

    return this.query(`UPDATE ${tableName} SET ${setClauses} WHERE id = ${rowId}`);
  }

  /**
   * Extract `named_rows` from a query response and wrap each cell as `KalamCellValue`.
   *
   * The Rust WASM layer pre-computes `named_rows` (schema → map) so SDKs
   * don't need to perform the transformation themselves.
   * @internal
   */
  private static namedRows(response: QueryResponse): RowData[] {
    const result = response.results?.[0];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const raw = (result as any)?.named_rows as Record<string, unknown>[] | undefined;
    if (!raw || !Array.isArray(raw)) return [];
    return raw.map(row => wrapRowMap(row));
  }

  /**
   * Query and return the first row as a `RowData`, or `null`.
   *
   * Every cell value is wrapped as a `KalamCellValue` with type-safe
   * accessors (`.asString()`, `.asInt()`, `.asFile()`, etc.).
   *
   * @example
   * ```typescript
   * const row = await client.queryOne('SELECT * FROM users WHERE id = $1', [42]);
   * if (row) {
   *   console.log(row['name'].asString(), row['email'].asString());
   * }
   * ```
   */
  async queryOne(
    sql: string,
    params?: unknown[],
  ): Promise<RowData | null> {
    const response = await this.query(sql, params);
    const rows = KalamDBClient.namedRows(response);
    return rows[0] ?? null;
  }

  /**
   * Query and return all rows as `RowData[]`.
   *
   * Every cell value is wrapped as a `KalamCellValue` with type-safe
   * accessors (`.asString()`, `.asInt()`, `.asFile()`, etc.).
   *
   * @example
   * ```typescript
   * const rows = await client.queryAll('SELECT * FROM users');
   * for (const row of rows) {
   *   console.log(row['name'].asString());
   *   console.log(row['age'].asInt());
   *   const url = row['avatar'].asFileUrl('http://localhost:8080', 'default', 'users');
   * }
   * ```
   */
  async queryAll(
    sql: string,
    params?: unknown[],
  ): Promise<RowData[]> {
    const response = await this.query(sql, params);
    return KalamDBClient.namedRows(response);
  }

  /**
   * Parse a "namespace.table" or "table" string into a `FileRefContext`.
   * Unqualified names fall back to the "default" namespace.
   * @internal
   */
  private tableContext(qualifiedName: string): FileRefContext {
    const dot = qualifiedName.indexOf('.');
    if (dot !== -1) {
      return {
        baseUrl: this.url,
        namespace: qualifiedName.slice(0, dot),
        table: qualifiedName.slice(dot + 1),
      };
    }
    return { baseUrl: this.url, namespace: 'default', table: qualifiedName };
  }

  /**
   * Wrap a single raw row object as a `KalamRow<T>` with FILE column access.
   *
   * Supply the table as a `"namespace.table"` string (or plain `"table"` for the
   * `"default"` namespace). The resulting row's `.file(column)` returns a
   * `BoundFileRef` whose `downloadUrl()` / `relativeUrl()` need no extra args.
   *
   * @param row       - Raw row object (e.g., result of `queryOne`)
   * @param tableName - Qualified table name, e.g. `"default.users"` or `"users"`
   *
   * @example
   * ```typescript
   * const raw = await client.queryOne<User>('SELECT * FROM default.users WHERE id = $1', [42]);
   * if (raw) {
   *   const row = client.wrapRow<User>(raw, 'default.users');
   *   const avatar = row.file('avatar');
   *   if (avatar) console.log(avatar.downloadUrl());
   * }
   * ```
   */
  wrapRow<T extends Record<string, unknown> = Record<string, unknown>>(
    row: T,
    tableName: string,
  ): KalamRow<T> {
    return new KalamRow<T>(row, this.tableContext(tableName));
  }

  /**
   * Execute a SQL query and return rows wrapped as `KalamRow<T>`.
   *
   * Each row exposes `.data` for raw column values and `.file(column)` for
   * FILE columns — returning a `BoundFileRef` with no-arg `downloadUrl()`.
   *
   * @param sql       - SQL query string
   * @param tableName - Qualified table name for file URL context, e.g. `"default.users"`
   * @param params    - Optional query parameters
   *
   * @example
   * ```typescript
   * interface User { id: string; name: string; avatar: string }
   *
   * const rows = await client.queryRows<User>(
   *   'SELECT * FROM default.users WHERE active = true',
   *   'default.users',
   * );
   *
   * rows.forEach(row => {
   *   console.log(row.data.name);
   *   const avatar = row.file('avatar');
   *   if (avatar) {
   *     img.src = avatar.downloadUrl();
   *     console.log(avatar.isImage(), avatar.formatSize());
   *   }
   * });
   * ```
   */
  async queryRows<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    tableName: string,
    params?: unknown[],
  ): Promise<KalamRow<T>[]> {
    const ctx = this.tableContext(tableName);
    const response = await this.query(sql, params);
    const rows = KalamDBClient.namedRows(response) as unknown as T[];
    return wrapRows<T>(rows, ctx);
  }

  /**
   * Subscribe to a table and receive change events with rows pre-wrapped as `KalamRow<T>`.
   *
   * The callback receives a `KalamChange<T>` where both `rows` (new values) and
   * `oldValues` (pre-change values for updates/deletes) are `KalamRow<T>` instances.
   * Call `.file(column)` on any row to get a `BoundFileRef` with no-arg URL methods.
   *
   * @param tableName - Qualified table name, e.g. `"default.users"` or `"users"`.
   *                    Used both as the subscription target and as the file URL context.
   * @param callback  - Invoked for every change event
   * @param options   - Optional subscription options (batch_size, resume, etc.)
   * @returns An unsubscribe function
   *
   * @example
   * ```typescript
   * interface User { id: string; name: string; avatar: string }
   *
   * const unsub = await client.subscribeRows<User>('default.users', (change) => {
   *   if (change.type === 'insert' || change.type === 'update') {
   *     change.rows.forEach(row => {
   *       console.log('User:', row.data.name);
   *       const avatar = row.file('avatar');
   *       if (avatar) {
   *         img.src = avatar.downloadUrl();           // full URL
   *         console.log(avatar.relativeUrl());         // /api/v1/files/default/users/…
   *         console.log(avatar.name, avatar.formatSize(), avatar.isImage());
   *       }
   *     });
   *   }
   * });
   *
   * // Unsubscribe when done
   * await unsub();
   * ```
   */
  async subscribeRows<T extends Record<string, unknown> = Record<string, unknown>>(
    tableName: string,
    callback: (change: KalamChange<T>) => void,
    options?: SubscriptionOptions,
  ): Promise<Unsubscribe> {
    const ctx = this.tableContext(tableName);
    return this.subscribe(tableName, (event) => {
      callback(new KalamChange<T>(event, ctx));
    }, options);
  }

  /* ---------------------------------------------------------------- */
  /*  Authentication                                                  */
  /* ---------------------------------------------------------------- */

  /**
   * Login with Basic Auth credentials and switch to JWT.
   *
   * Uses WASM binding to POST to `/v1/api/auth/login`, obtain a JWT token,
   * and automatically update the client to use JWT auth. Should be called
   * before subscribing to WebSocket queries.
   *
   * @returns The full LoginResponse (access_token, refresh_token, user info, etc.)
   */
  async login(): Promise<LoginResponse> {
    await this.initialize();
    if (this.auth.type !== 'basic') {
      throw new Error('login() requires Basic auth credentials. Use authProvider returning Auth.basic(username, password)');
    }
    const response = await this.performDirectBasicLogin(this.auth.username, this.auth.password);

    this.wasmClient = WasmClient.withJwt(this.url, response.access_token);
    this.attachWasmClientState();

    // Update TypeScript client's auth state to match
    this.auth = { type: 'jwt', token: response.access_token };
    this.log(LogLevel.Info, 'auth', 'Login successful, switched to JWT auth');

    return response;
  }

  /**
   * Refresh the access token using a refresh token.
   *
   * Sends the refresh token to the server to obtain a new access token.
   * The client's auth state is automatically updated.
   *
   * @param refreshToken - The refresh token obtained from a previous login
   * @returns The full LoginResponse with new tokens
   */
  async refreshToken(refreshToken: string): Promise<LoginResponse> {
    this.log(LogLevel.Debug, 'auth', 'Refreshing access token...');
    await this.initialize();
    if (!this.wasmClient) throw new Error('WASM client not initialized');

    const response = (await this.wasmClient.refresh_access_token(refreshToken)) as LoginResponse;

    // Update TypeScript client's auth state with new access token
    this.auth = { type: 'jwt', token: response.access_token };
    this.log(LogLevel.Info, 'auth', 'Access token refreshed successfully');

    return response;
  }

  /* ---------------------------------------------------------------- */
  /*  Subscriptions                                                   */
  /* ---------------------------------------------------------------- */

  /**
   * Subscribe to real-time changes on a table.
   *
   * @returns An unsubscribe function
   *
   * @example
   * ```typescript
   * const unsub = await client.subscribe('messages', (event) => {
   *   if (event.type === 'change') console.log(event.rows);
   * });
   * await unsub();
   * ```
   */
  async subscribe(
    tableName: string,
    callback: SubscriptionCallback,
    options?: SubscriptionOptions,
  ): Promise<Unsubscribe> {
    return this.subscribeWithSql(`SELECT * FROM ${tableName}`, callback, options);
  }

  /**
   * Subscribe to a SQL query with real-time updates.
   *
   * @example
   * ```typescript
   * const unsub = await client.subscribeWithSql(
   *   'SELECT * FROM chat.messages WHERE conversation_id = 1',
   *   (event) => { ... },
   *   { batch_size: 50 },
   * );
   * ```
   */
  async subscribeWithSql(
    sql: string,
    callback: SubscriptionCallback,
    options?: SubscriptionOptions,
  ): Promise<Unsubscribe> {
    this.log(LogLevel.Debug, 'subscription', `Subscribing to: ${sql.substring(0, 120)}`);
    const optionsJson = this.stringifyOptions(normalizeSubscriptionOptions(options));
    const subscriptionId = await this.startTrackedSubscription(
      sql,
      (wasmClient) => wasmClient.subscribeWithSql(
        sql,
        optionsJson,
        this.createRawSubscriptionCallback(callback) as unknown as Function,
      ),
    );

    return async () => {
      await this.unsubscribe(subscriptionId);
    };
  }

  /**
   * Subscribe to a SQL query and receive the SDK-maintained current row set.
   *
   * The SDK applies `initial_data_batch`, `insert`, `update`, and `delete`
   * events internally so application code only handles the latest row array.
   */
  async live<T = RowData>(
    sql: string,
    callback: LiveRowsCallback<T>,
    options: LiveRowsOptions<T> = {},
  ): Promise<Unsubscribe> {
    if (options.getKey) {
      return this.liveFallback(sql, callback, options);
    }

    const mapRow = options.mapRow ?? ((row: RowData) => row as unknown as T);
    const normalizedOptions = normalizeLiveRowsOptions(options);
    const optionsJson = this.stringifyOptions(normalizedOptions);
    const subscriptionId = await this.startTrackedSubscription(
      sql,
      (wasmClient) => wasmClient.liveQueryRowsWithSql(
        sql,
        optionsJson,
        this.createLiveRowsCallback(callback, mapRow, options.onError) as unknown as Function,
      ),
    );

    return async () => {
      await this.unsubscribe(subscriptionId);
    };
  }

  private async liveFallback<T = RowData>(
    sql: string,
    callback: LiveRowsCallback<T>,
    options: LiveRowsOptions<T> = {},
  ): Promise<Unsubscribe> {
    const mapRow = options.mapRow ?? ((row: RowData) => row as unknown as T);
    const getKey = options.getKey ?? ((row: T) => defaultRowKey(row));
    let rows: T[] = [];

    return this.subscribeWithSql(
      sql,
      (event) => {
        const normalizedEvent = event as NormalizedSubscriptionEvent;

        if (normalizedEvent.type === 'subscription_ack') {
          return;
        }

        if (normalizedEvent.type === 'error') {
          options.onError?.(normalizedEvent as SubscriptionErrorEvent);
          return;
        }

        if (normalizedEvent.type === 'initial_data_batch') {
          rows = (normalizedEvent.rows ?? []).map(mapRow);
          callback([...rows]);
          return;
        }

        if (normalizedEvent.type !== 'change') {
          return;
        }

        if (normalizedEvent.change_type === 'delete') {
          rows = removeMaterializedRows(rows, (normalizedEvent.old_values ?? []).map(mapRow), getKey);
          callback([...rows]);
          return;
        }

        rows = upsertLimited(rows, (normalizedEvent.rows ?? []).map(mapRow), getKey);
        callback([...rows]);
      },
      options.subscriptionOptions,
    );
  }

  /**
   * Subscribe to a table and receive the SDK-maintained current row set.
   */
  async liveTableRows<T = RowData>(
    tableName: string,
    callback: LiveRowsCallback<T>,
    options: LiveRowsOptions<T> = {},
  ): Promise<Unsubscribe> {
    return this.live(`SELECT * FROM ${tableName}`, callback, options);
  }

  /** Unsubscribe by subscription ID */
  async unsubscribe(subscriptionId: string): Promise<void> {
    this.log(LogLevel.Debug, 'subscription', `Unsubscribing: id=${subscriptionId}`);
    if (!this.wasmClient) throw new Error('WASM client not initialized');
    this.forgetSubscriptionMetadata(subscriptionId);
    try {
      await this.wasmClient.unsubscribe(subscriptionId);
      this.log(LogLevel.Info, 'subscription', `Unsubscribed: id=${subscriptionId}`);
    } catch (error) {
      this.log(LogLevel.Warn, 'subscription', `Unsubscribe failed for ${subscriptionId}: ${error}`);
      throw error;
    }
  }

  /** Number of active subscriptions */
  getSubscriptionCount(): number {
    return this.getSubscriptions().filter((subscription) => !subscription.closed).length;
  }

  /** Info about active subscriptions, using Rust/WASM state as the source of truth. */
  getSubscriptions(): SubscriptionInfo[] {
    if (!this.wasmClient) {
      return readSubscriptionInfos(undefined, this.subscriptionMetadata);
    }

    return readSubscriptionInfos(this.wasmClient.getSubscriptions(), this.subscriptionMetadata);
  }

  /** Whether there is an active subscription for the given table/SQL */
  isSubscribedTo(tableName: string): boolean {
    return this.getSubscriptions().some((subscription) => (
      !subscription.closed && subscription.tableName === tableName
    ));
  }

  /** Unsubscribe from all active subscriptions */
  async unsubscribeAll(): Promise<void> {
    const ids = Array.from(new Set(
      this.getSubscriptions()
        .filter((subscription) => !subscription.closed)
        .map((subscription) => subscription.id),
    ));
    for (const id of ids) {
      await this.unsubscribe(id);
    }
  }

  /* ---------------------------------------------------------------- */
  /*  Private helpers                                                 */
  /* ---------------------------------------------------------------- */

  private async startTrackedSubscription(
    tableName: string,
    register: (wasmClient: WasmClient) => Promise<string>,
  ): Promise<string> {
    await this.ensureConnected();
    if (!this.wasmClient) {
      throw new Error('WASM client not initialized');
    }

    const subscriptionId = await register(this.wasmClient);
    this.trackSubscriptionMetadata(subscriptionId, tableName);
    this.log(LogLevel.Info, 'subscription', `Subscribed: id=${subscriptionId}`);
    return subscriptionId;
  }

  private createRawSubscriptionCallback(
    callback: SubscriptionCallback,
  ): (eventJson: string) => void {
    return this.createJsonCallback(
      'subscription',
      (eventJson) => normalizeSubscriptionEvent(JSON.parse(eventJson) as ServerMessage),
      (event) => {
        this.log(LogLevel.Verbose, 'subscription', `Event: type=${event.type}`);
        callback(event);
      },
    );
  }

  private createLiveRowsCallback<T>(
    callback: LiveRowsCallback<T>,
    mapRow: (row: RowData) => T,
    onError?: (event: SubscriptionErrorEvent) => void,
  ): (eventJson: string) => void {
    return this.createJsonCallback(
      'subscription',
      (eventJson) => normalizeLiveRowsWasmEvent(JSON.parse(eventJson) as {
        type: 'rows' | 'error';
        subscription_id: string;
        rows?: unknown;
        code?: string;
        message?: string;
      }),
      (event) => {
        if (event.type === 'error') {
          onError?.({
            type: 'error',
            subscription_id: event.subscription_id,
            code: event.code ?? 'unknown',
            message: event.message ?? 'Live query failed',
          });
          return;
        }

        callback((event.rows ?? []).map(mapRow));
      },
    );
  }

  private createJsonCallback<T>(
    tag: string,
    parse: (payload: string) => T,
    handle: (value: T) => void,
  ): (payload: string) => void {
    return (payload: string) => {
      try {
        handle(parse(payload));
      } catch (error) {
        this.log(LogLevel.Error, tag, `Failed to parse callback payload: ${error}`);
      }
    };
  }

  private stringifyOptions<T>(options: T | undefined): string | undefined {
    return options === undefined ? undefined : JSON.stringify(options);
  }

  private trackSubscriptionMetadata(subscriptionId: string, tableName: string): void {
    trackSubscriptionMetadata(this.subscriptionMetadata, subscriptionId, tableName);
  }

  private forgetSubscriptionMetadata(subscriptionId: string): void {
    this.subscriptionMetadata.delete(subscriptionId);
  }

  private clearSubscriptionMetadata(): void {
    this.subscriptionMetadata.clear();
  }

  private requireInit(): void {
    if (!this.wasmClient) {
      throw new Error('WASM client not initialized. Call initialize() first.');
    }
  }

  private async performDirectBasicLogin(username: string, password: string): Promise<LoginResponse> {
    const response = await fetch(`${this.url}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });

    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(body || `Login failed: HTTP ${response.status}`);
    }

    return (await response.json()) as LoginResponse;
  }

  private async resolveWasmAuthProvider(): Promise<WasmAuthProviderResult> {
    this.log(LogLevel.Debug, 'auth', 'Auth provider invoked for credentials...');
    const creds = await resolveAuthProviderWithRetry(this._authProvider, {
      maxAttempts: this.authProviderMaxAttempts,
      initialBackoffMs: this.authProviderInitialBackoffMs,
      maxBackoffMs: this.authProviderMaxBackoffMs,
    });
    this.log(LogLevel.Debug, 'auth', `Auth provider resolved: type=${creds.type}`);

    if (creds.type === 'jwt') {
      return { jwt: { token: creds.token } };
    }

    if (creds.type === 'none') {
      return null;
    }

    if (creds.type === 'basic') {
      this.log(LogLevel.Debug, 'auth', 'Auth provider returned basic — performing direct HTTP login for JWT...');
      const loginResp = await this.performDirectBasicLogin(creds.username, creds.password);
      this.auth = { type: 'jwt', token: loginResp.access_token };
      this.log(LogLevel.Debug, 'auth', 'Login successful via authProvider bridge (direct fetch)');
      return { jwt: { token: loginResp.access_token } };
    }

    throw new Error('authProvider returned an unknown credential type');
  }

  private attachWasmClientState(): void {
    this.requireInit();
    this.wasmClient!.setAuthProvider(async () => this.resolveWasmAuthProvider());

    if (this.disableCompression) {
      this.wasmClient!.setDisableCompression(true);
    }

    this.wasmClient!.setAutoReconnect(this.autoReconnectEnabled);
    this.wasmClient!.setWsLazyConnect(this.wsLazyConnect);
    this.applyEventHandlers();
  }

  private async ensureJwtForBasicAuth(): Promise<void> {
    if (this.auth.type === 'basic') {
      await this.login();
    }
  }

  /**
   * Re-resolve credentials from the authProvider and update internal auth state.
   * Used to recover from TOKEN_EXPIRED in code paths that bypass the WASM client
   * (e.g. queryWithFiles which uses fetch() directly).
   */
  private async reauthenticateFromAuthProvider(): Promise<void> {
    try {
      const result = await this.resolveWasmAuthProvider();
      if (result?.jwt?.token) {
        this.auth = { type: 'jwt', token: result.jwt.token };
        if (this.wasmClient) {
          this.wasmClient = WasmClient.withJwt(this.url, result.jwt.token);
          this.attachWasmClientState();
        }
      }
    } catch (error) {
      this.log(LogLevel.Warn, 'auth', `Failed to reauthenticate: ${error}`);
    }
  }

  private async ensureNodeRuntimeCompat(): Promise<void> {
    const isNodeRuntime = Boolean(getNodeProcess()?.versions?.node);
    if (!isNodeRuntime) {
      return;
    }

    const runtime = globalThis as unknown as {
      WebSocket?: typeof WebSocket;
      window?: NodeWindowShim;
    };

    if (typeof runtime.WebSocket === 'undefined') {
      try {
        const wsModuleName = 'ws';
        const wsModule = (await dynamicImport(wsModuleName)) as {
          WebSocket?: typeof WebSocket;
          default?: typeof WebSocket;
        };
        const wsCtor = wsModule.WebSocket ?? wsModule.default;
        if (!wsCtor || typeof wsCtor !== 'function') {
          throw new Error('ws module did not export a WebSocket constructor');
        }
        runtime.WebSocket = wsCtor;
      } catch (error) {
        throw new Error(
          `Node.js runtime is missing WebSocket support. Install "ws" or assign globalThis.WebSocket before creating the client. Cause: ${error}`,
        );
      }
    }

    if (typeof globalThis.fetch !== 'function') {
      throw new Error('Node.js runtime is missing fetch() support. Node.js 18+ is required.');
    }

    if (!this.wasmUrl) {
      try {
        const [{ readFile }, { fileURLToPath }] = await Promise.all([
          dynamicImport('node:fs/promises') as Promise<NodeFsPromisesShim>,
          dynamicImport('node:url') as Promise<NodeUrlShim>,
        ]);
        const wasmFileUrl = new URL('../wasm/kalam_client_bg.wasm', import.meta.url);
        this.wasmUrl = await readFile(fileURLToPath(wasmFileUrl));
      } catch (error) {
        throw new Error(
          `Node.js runtime could not load bundled WASM file. Build the SDK and ensure dist/wasm/kalam_client_bg.wasm exists. Cause: ${error}`,
        );
      }
    }

    const parsedUrl = new URL(this.url);
    const location = {
      protocol: parsedUrl.protocol,
      hostname: parsedUrl.hostname,
      port: parsedUrl.port,
      href: parsedUrl.href,
    };

    if (!runtime.window) {
      runtime.window = { location, fetch: globalThis.fetch.bind(globalThis) };
      return;
    }

    runtime.window.location = location;
    if (!runtime.window.fetch) {
      runtime.window.fetch = globalThis.fetch.bind(globalThis);
    }
  }
}

/* ================================================================== */
/*  Factory                                                           */
/* ================================================================== */

/**
 * Create a KalamDB client with the given configuration.
 *
 * @example
 * ```typescript
 * import { createClient, Auth } from '@kalamdb/client';
 *
 * const client = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'admin'),
 * });
 * ```
 */
export function createClient(options: ClientOptions): KalamDBClient {
  return new KalamDBClient(options);
}
