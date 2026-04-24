/**
 * KalamDB Client wrapper for Admin UI
 * 
 * Uses the @kalamdb/client SDK with JWT authentication.
 * The JWT token is obtained from the cookie-based auth flow,
 * then used for all SQL queries via the WASM SDK.
 * 
 * This is modeled after the example at link/sdks/typescript/client/example/app.js
 * which demonstrates proper WASM initialization and query execution.
 */

import {
  KalamDBClient,
  Auth,
  LogLevel,
  type KalamCellValue,
  type ConnectionError,
  type DisconnectReason,
  type LiveRowsOptions,
  type LogEntry,
  type LogListener,
  type OnConnectCallback,
  type OnDisconnectCallback,
  type OnErrorCallback,
  type OnReceiveCallback,
  type OnSendCallback,
  type QueryResponse,
  type RowData,
  type ServerMessage,
  type SubscriptionOptions,
  type Unsubscribe,
} from '@kalamdb/client';
import { getBackendOrigin } from "./backend-url";

let client: KalamDBClient | null = null;
let subscriptionClient: KalamDBClient | null = null;
let currentToken: string | null = null;
let isInitialized = false;
let isSubscriptionInitialized = false;
let currentConnectListener: OnConnectCallback | undefined;
let currentDisconnectListener: OnDisconnectCallback | undefined;
let currentErrorListener: OnErrorCallback | undefined;
let currentReceiveListener: OnReceiveCallback | undefined;
let currentSendListener: OnSendCallback | undefined;
const connectivityListeners = new Set<ClientConnectivityListener>();
const isDebugLoggingEnabled = import.meta.env.DEV;
const ADMIN_UI_PING_INTERVAL_MS = 5_000;

export type ClientConnectivityEvent =
  | { type: 'connect' }
  | { type: 'disconnect'; reason: DisconnectReason }
  | { type: 'error'; error: ConnectionError };

export type ClientConnectivityListener = (event: ClientConnectivityEvent) => void;

function debugLog(...args: unknown[]): void {
  if (isDebugLoggingEnabled) {
    console.log(...args);
  }
}

/**
 * Query queue to prevent concurrent WASM calls which cause "recursive borrow" errors.
 * WASM clients use RefCell internally which doesn't support concurrent access.
 */
let queryQueue: Promise<unknown> = Promise.resolve();
let lifecycleQueue: Promise<unknown> = Promise.resolve();
function requireToken(): string {
  if (!currentToken) {
    throw new Error('Not authenticated. Please log in first.');
  }

  return currentToken;
}
/**
 * Serializes concurrent live()/subscribe() setups so they never overlap.
 * React StrictMode double-mounts fire two subscription effects nearly simultaneously;
 * without this lock both would call live() concurrently and crash the WASM RefCell.
 */
let subscriptionSetupLock: Promise<unknown> = Promise.resolve();

/**
 * Queue a query to ensure sequential execution and prevent WASM borrow errors.
 * @param fn The async function to execute
 * @returns Promise that resolves with the function result
 */
async function queueQuery<T>(fn: () => Promise<T>): Promise<T> {
  // Chain the new query after the current queue
  const result = queryQueue.then(
    () => fn(),
    () => fn() // Even if previous fails, try this one
  );
  // Update the queue to wait for this query (ignore errors to not block next queries)
  queryQueue = result.catch(() => {});
  return result;
}

/**
 * Serialize live()/subscribe() setup on the dedicated subscription client.
 *
 * Query execution uses a separate WASM client instance, so we no longer need to block
 * the query queue here. We only need to ensure we do not start two subscription setup
 * calls concurrently on the same subscription client.
 */
async function serializedSubscriptionCall<T>(fn: () => Promise<T>): Promise<T> {
  const runSetup = async (): Promise<T> => {
    try {
      return await fn();
    } catch (err) {
      console.error('[kalam-client] Subscription setup failed:', err);
      throw err;
    }
  };

  const result = subscriptionSetupLock.then(runSetup, runSetup);
  subscriptionSetupLock = result.catch(() => {});
  return result;
}

function createSdkClient(jwtToken: string, eagerWebSocket = false): KalamDBClient {
  return new KalamDBClient({
    url: getBackendUrl(),
    authProvider: async () => Auth.jwt(jwtToken),
    pingIntervalMs: ADMIN_UI_PING_INTERVAL_MS,
    wsLazyConnect: !eagerWebSocket,
  });
}

function notifyConnectivityListeners(event: ClientConnectivityEvent): void {
  for (const listener of connectivityListeners) {
    try {
      listener(event);
    } catch (error) {
      console.error('[kalam-client] Connectivity listener failed:', error);
    }
  }
}

function handleSubscriptionClientConnect(): void {
  currentConnectListener?.();
  notifyConnectivityListeners({ type: 'connect' });
}

function handleSubscriptionClientDisconnect(reason: DisconnectReason): void {
  currentDisconnectListener?.(reason);
  notifyConnectivityListeners({ type: 'disconnect', reason });
}

function handleSubscriptionClientError(error: ConnectionError): void {
  currentErrorListener?.(error);
  notifyConnectivityListeners({ type: 'error', error });
}

function applySubscriptionClientListeners(targetClient: KalamDBClient): void {
  targetClient.setLogListener(undefined);
  targetClient.onConnect(handleSubscriptionClientConnect);
  if (currentReceiveListener) {
    targetClient.onReceive(currentReceiveListener);
  }
  if (currentSendListener) {
    targetClient.onSend(currentSendListener);
  }
  targetClient.onDisconnect(handleSubscriptionClientDisconnect);
  targetClient.onError(handleSubscriptionClientError);
}

async function disconnectSdkClient(targetClient: KalamDBClient | null): Promise<void> {
  if (!targetClient) {
    return;
  }

  try {
    await targetClient.disconnect();
  } catch {
    // Ignore disconnect errors during teardown/token rotation.
  }
}

async function initializeSubscriptionClient(jwtToken: string): Promise<KalamDBClient> {
  return queueLifecycle(async () => {
    if (jwtToken === currentToken && subscriptionClient && isSubscriptionInitialized) {
      applySubscriptionClientListeners(subscriptionClient);

      if (!subscriptionClient.isConnected()) {
        debugLog('[kalam-client] Reconnecting existing subscription client...');
        await subscriptionClient.connect();
      } else {
        debugLog('[kalam-client] Returning existing initialized subscription client');
      }

      return subscriptionClient;
    }

    if (subscriptionClient) {
      await disconnectSdkClient(subscriptionClient);
      subscriptionClient = null;
      isSubscriptionInitialized = false;
    }

    const nextClient = createSdkClient(jwtToken, true);
    debugLog('[kalam-client] Initializing subscription WASM client...');
    await nextClient.initialize();
    applySubscriptionClientListeners(nextClient);

    subscriptionClient = nextClient;
    currentToken = jwtToken;
    isSubscriptionInitialized = true;
    debugLog('[kalam-client] Subscription WASM initialized successfully');
    return nextClient;
  });
}

async function ensureQueryClient(): Promise<KalamDBClient> {
  const token = requireToken();

  if (!client || !isInitialized) {
    debugLog('[kalam-client] Query client not initialized, initializing now...');
    return initializeClient(token);
  }

  return client;
}

async function ensureSubscriptionClient(): Promise<KalamDBClient> {
  const token = requireToken();

  if (!subscriptionClient || !isSubscriptionInitialized) {
    return initializeSubscriptionClient(token);
  }

  return subscriptionClient;
}

/**
 * Queue client lifecycle changes (init/disconnect) so token switches are strictly ordered.
 */
async function queueLifecycle<T>(fn: () => Promise<T>): Promise<T> {
  const result = lifecycleQueue.then(
    () => fn(),
    () => fn(),
  );
  lifecycleQueue = result.catch(() => {});
  return result;
}

/**
 * Get the backend URL.
 * Use VITE_API_URL when provided, otherwise default to localhost in development
 * and the current origin in production.
 */
function getBackendUrl(): string {
  return getBackendOrigin();
}

/**
 * Initialize the KalamDB client with JWT token
 * 
 * Creates a new client with JWT auth and initializes the WASM module.
 * Uses a promise lock to prevent concurrent initialization (which causes WASM crashes).
 */
export async function initializeClient(jwtToken: string): Promise<KalamDBClient> {
  return queueLifecycle(async () => {
    if (jwtToken === currentToken && client && isInitialized) {
      debugLog('[kalam-client] Returning existing initialized client');
      return client;
    }

    debugLog('[kalam-client] initializeClient called');

    if (client && jwtToken !== currentToken) {
      debugLog('[kalam-client] Token changed, disconnecting old client');
      await disconnectSdkClient(client);
      await disconnectSdkClient(subscriptionClient);
      client = null;
      subscriptionClient = null;
      isInitialized = false;
      isSubscriptionInitialized = false;
    }

    const nextClient = createSdkClient(jwtToken);

    debugLog('[kalam-client] Initializing WASM...');
    await nextClient.initialize();

    client = nextClient;
    currentToken = jwtToken;
    isInitialized = true;
    debugLog('[kalam-client] WASM initialized successfully');
    return nextClient;
  });
}

/**
 * Get the current client instance (must be initialized first)
 */
export function getClient(): KalamDBClient | null {
  return isInitialized ? client : null;
}

/**
 * Set the JWT token for the client (called on login/refresh)
 * Creates and initializes the client immediately
 */
export async function setClientToken(token: string): Promise<void> {
  debugLog('[kalam-client] setClientToken called');
  await initializeClient(token);
}

/**
 * Clear the client when user logs out
 */
export async function clearClient(): Promise<void> {
  await queueLifecycle(async () => {
    debugLog('[kalam-client] clearClient called');
    const existingClient = client;
    const existingSubscriptionClient = subscriptionClient;
    client = null;
    subscriptionClient = null;
    currentToken = null;
    isInitialized = false;
    isSubscriptionInitialized = false;

    await disconnectSdkClient(existingClient);
    await disconnectSdkClient(existingSubscriptionClient);
  });
}

/**
 * Get current token (for debugging)
 */
export function getCurrentToken(): string | null {
  return currentToken;
}

/**
 * Execute SQL query using the WASM SDK
 * Returns the full QueryResponse
 * 
 * Uses a query queue to prevent concurrent WASM calls which cause borrow errors.
 */
export async function executeQuery(sql: string): Promise<QueryResponse> {
  const queryClient = await ensureQueryClient();
  
  debugLog("[kalam-client] Executing query via SDK:", sql.substring(0, 120) + (sql.length > 120 ? "..." : ""));
  
  // Queue the query to prevent concurrent WASM access
  return queueQuery(async () => {
    try {
      const response = await queryClient.query(sql);
      if (response.status === "error") {
        console.warn("[kalam-client] Query failed:", response.error?.message ?? "Unknown error");
      } else {
        const first = response.results?.[0];
        const resultCount = response.results?.length ?? 0;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const rowCount = first?.row_count ?? (first as any)?.named_rows?.length ?? first?.rows?.length ?? 0;
        const columnCount = first?.schema?.length ?? 0;
        debugLog(
          "[kalam-client] Query success:",
          `${resultCount} result set(s), ${rowCount} row(s), ${columnCount} column(s), took ${response.took ?? 0}ms`,
        );
      }
      return response;
    } catch (err) {
      console.error('[kalam-client] Query execution error:', err);
      // Convert WASM errors to a proper error response
      const errorMessage = err instanceof Error ? err.message : String(err);
      throw new Error(errorMessage);
    }
  });
}

function unwrapRowData(rows: RowData[]): Record<string, unknown>[] {
  return rows.map((row) => {
    const record: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(row)) {
      record[key] = (value as KalamCellValue).toJson();
    }

    return record;
  });
}

/**
 * Execute SQL and return rows from the first result set
 * Convenience function for hooks that just need rows
 * Converts the new array-based row format to Record objects for backwards compatibility
 */
export async function executeSql(sql: string): Promise<Record<string, unknown>[]> {
  try {
    const queryClient = await ensureQueryClient();
    const rows = await queueQuery(() => queryClient.queryAll(sql));
    if (!rows.length) {
      return [];
    }

    return unwrapRowData(rows);
  } catch (err) {
    console.error('[kalam-client] executeSql failed:', err);
    throw err;
  }
}

/**
 * Insert data into a table using SDK's insert method
 */
export async function insert(tableName: string, data: Record<string, unknown>): Promise<QueryResponse> {
  const queryClient = await ensureQueryClient();
  return queueQuery(() => queryClient.insert(tableName, data));
}

/**
 * Delete a row from a table using SDK's delete method
 */
export async function deleteRow(tableName: string, rowId: string | number): Promise<void> {
  const queryClient = await ensureQueryClient();
  return queueQuery(() => queryClient.delete(tableName, rowId));
}

/**
 * Check if client is connected (WebSocket)
 */
export function isClientConnected(): boolean {
  return Boolean(
    (client !== null && isInitialized && client.isConnected())
    || (subscriptionClient !== null && isSubscriptionInitialized && subscriptionClient.isConnected()),
  );
}

export function isSubscriptionClientConnected(): boolean {
  return Boolean(
    subscriptionClient !== null
    && isSubscriptionInitialized
    && subscriptionClient.isConnected(),
  );
}

/**
 * Connect WebSocket for real-time subscriptions
 */
export async function connectWebSocket(): Promise<void> {
  const token = requireToken();
  await initializeSubscriptionClient(token);
}

/**
 * Parse CLI-style OPTIONS from SQL query
 * Example: "SELECT * FROM chat.messages OPTIONS (last_rows=20);" 
 * Returns: { sql: "SELECT * FROM chat.messages", options: { last_rows: 20 } }
 */
function parseOptionsFromSql(sql: string): { sql: string; options: SubscriptionOptions } {
  const options: SubscriptionOptions = {};
  let cleanSql = sql;
  
  // Match OPTIONS (...) at the end of the query (before optional semicolon)
  const optionsMatch = sql.match(/\s+OPTIONS\s*\(([^)]+)\)\s*;?\s*$/i);
  
  if (optionsMatch) {
    // Remove OPTIONS clause from SQL
    cleanSql = sql.substring(0, optionsMatch.index).trim();
    // Remove trailing semicolon if present
    if (cleanSql.endsWith(';')) {
      cleanSql = cleanSql.slice(0, -1).trim();
    }
    
    // Parse options: "last_rows=20, batch_size=100"
    const optionsStr = optionsMatch[1];
    const optionPairs = optionsStr.split(',');
    
    for (const pair of optionPairs) {
      const [key, value] = pair.split('=').map(s => s.trim());
      if (key && value) {
        const keyLower = key.toLowerCase();
        if (keyLower === 'last_rows') {
          options.last_rows = parseInt(value, 10);
        } else if (keyLower === 'batch_size') {
          options.batch_size = parseInt(value, 10);
        } else if (keyLower === 'from') {
          options.from = value;
        }
      }
    }
    
    debugLog('[kalam-client] Parsed OPTIONS from SQL:', options);
  }
  
  return { sql: cleanSql, options };
}

function isSqlQueryTarget(sql: string): boolean {
  const trimmed = sql.trim().toLowerCase();
  return trimmed.startsWith('select ')
    || trimmed.startsWith('select\n')
    || trimmed.startsWith('select\t');
}

function normalizeSubscriptionTarget(
  tableOrQuery: string,
  options?: SubscriptionOptions,
): {
  cleanSql: string;
  subscribeOptions: SubscriptionOptions;
  isSqlQuery: boolean;
} {
  const { sql: cleanSql, options: parsedOptions } = parseOptionsFromSql(tableOrQuery);

  return {
    cleanSql,
    subscribeOptions: {
      last_rows: parsedOptions.last_rows ?? options?.last_rows,
      batch_size: parsedOptions.batch_size ?? options?.batch_size,
      from: parsedOptions.from ?? options?.from,
    },
    isSqlQuery: isSqlQueryTarget(cleanSql),
  };
}

/**
 * Subscribe to table changes
 * Returns an unsubscribe function (Firebase/Supabase style)
 * 
 * Accepts either:
 * - A table name: "chat.messages" or "namespace.table"
 * - A SQL query: "SELECT * FROM chat.messages WHERE ..."
 * - A SQL query with OPTIONS: "SELECT * FROM chat.messages OPTIONS (last_rows=20);"
 */
export async function subscribe(
  tableOrQuery: string,
  callback: (event: ServerMessage) => void,
  options?: SubscriptionOptions
): Promise<Unsubscribe> {
  const liveClient = await ensureSubscriptionClient();
  const { cleanSql, subscribeOptions, isSqlQuery } = normalizeSubscriptionTarget(tableOrQuery, options);
  
  debugLog('[kalam-client] Subscribing to:', cleanSql, 'with options:', subscribeOptions);
  
  let unsubscribe: Unsubscribe;
  if (isSqlQuery) {
    // Full SQL query - use subscribeWithSql
    debugLog('[kalam-client] Detected SQL query, using subscribeWithSql');
    unsubscribe = await serializedSubscriptionCall(
      () => liveClient.subscribeWithSql(cleanSql, callback, subscribeOptions),
    );
  } else {
    // Just a table name - use subscribe (which wraps in SELECT * FROM)
    debugLog('[kalam-client] Detected table name, using subscribe');
    unsubscribe = await serializedSubscriptionCall(
      () => liveClient.subscribe(cleanSql, callback, subscribeOptions),
    );
  }
  
  debugLog('[kalam-client] Subscription registered successfully');
  
  return unsubscribe;
}

/**
 * Subscribe to a materialized live row set.
 *
 * Accepts either a full SQL query or a table name, mirroring `subscribe()`.
 */
export async function subscribeRows<T = RowData>(
  tableOrQuery: string,
  callback: (rows: T[]) => void,
  options?: LiveRowsOptions<T>,
): Promise<Unsubscribe> {
  const liveClient = await ensureSubscriptionClient();
  const { cleanSql, isSqlQuery } = normalizeSubscriptionTarget(tableOrQuery);

  if (isSqlQuery) {
    debugLog('[kalam-client] Detected SQL query, using live');
    return serializedSubscriptionCall(() => liveClient.live(cleanSql, callback, options));
  }

  debugLog('[kalam-client] Detected table name, using liveTableRows');
  return serializedSubscriptionCall(() => liveClient.liveTableRows(cleanSql, callback, options));
}

/**
 * Get subscription count
 */
export function getSubscriptionCount(): number {
  return subscriptionClient?.getSubscriptionCount() ?? 0;
}

/**
 * Set a log listener on the SDK client to capture internal SDK logs.
 * Pass undefined to remove the listener.
 */
export function setClientLogListener(listener: LogListener | undefined): void {
  if (subscriptionClient) {
    subscriptionClient.setLogListener(listener);
  }
}

export function setClientReceiveListener(listener: OnReceiveCallback | undefined): void {
  currentReceiveListener = listener;
  if (subscriptionClient) {
    subscriptionClient.onReceive(listener ?? (() => {}));
  }
}

export function setClientSendListener(listener: OnSendCallback | undefined): void {
  currentSendListener = listener;
  if (subscriptionClient) {
    subscriptionClient.onSend(listener ?? (() => {}));
  }
}

export function setClientConnectListener(listener: OnConnectCallback | undefined): void {
  currentConnectListener = listener;
  if (subscriptionClient) {
    subscriptionClient.onConnect(handleSubscriptionClientConnect);
  }
}

export function setClientDisconnectListener(listener: OnDisconnectCallback | undefined): void {
  currentDisconnectListener = listener;
  if (subscriptionClient) {
    subscriptionClient.onDisconnect(handleSubscriptionClientDisconnect);
  }
}

export function setClientErrorListener(listener: OnErrorCallback | undefined): void {
  currentErrorListener = listener;
  if (subscriptionClient) {
    subscriptionClient.onError(handleSubscriptionClientError);
  }
}

export function subscribeToClientConnectivity(listener: ClientConnectivityListener): () => void {
  connectivityListeners.add(listener);
  return () => {
    connectivityListeners.delete(listener);
  }
}

/**
 * Get the SDK LogLevel enum for external use.
 */
export { LogLevel };
export type {
  ConnectionError,
  DisconnectReason,
  LogEntry,
  LogListener,
  OnConnectCallback,
  OnDisconnectCallback,
  OnErrorCallback,
  OnReceiveCallback,
  OnSendCallback,
};

// Re-export types for convenience
export type { LiveRowsOptions, QueryResponse, RowData, ServerMessage, SubscriptionOptions, Unsubscribe };
