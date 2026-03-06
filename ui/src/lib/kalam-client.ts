/**
 * KalamDB Client wrapper for Admin UI
 * 
 * Uses the kalam-link SDK with JWT authentication.
 * The JWT token is obtained from the cookie-based auth flow,
 * then used for all SQL queries via the WASM SDK.
 * 
 * This is modeled after the example at link/sdks/typescript/example/app.js
 * which demonstrates proper WASM initialization and query execution.
 */

import { KalamDBClient, Auth, type QueryResponse, type ServerMessage, type Unsubscribe, type SubscriptionOptions } from 'kalam-link';

let client: KalamDBClient | null = null;
let currentToken: string | null = null;
let isInitialized = false;

/**
 * Query queue to prevent concurrent WASM calls which cause "recursive borrow" errors.
 * WASM clients use RefCell internally which doesn't support concurrent access.
 */
let queryQueue: Promise<unknown> = Promise.resolve();
let lifecycleQueue: Promise<unknown> = Promise.resolve();

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
 * Get the backend URL
 * In development, Vite runs on port 5173 but backend is on 8080
 * In production, both are served from the same origin
 */
function getBackendUrl(): string {
  if (import.meta.env.DEV) {
    return 'http://localhost:8080';
  }
  return window.location.origin;
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
      console.log('[kalam-client] Returning existing initialized client');
      return client;
    }

    console.log('[kalam-client] initializeClient called');

    if (client && jwtToken !== currentToken) {
      console.log('[kalam-client] Token changed, disconnecting old client');
      try {
        await client.disconnect();
      } catch {
        // Ignore disconnect errors
      }
      client = null;
      isInitialized = false;
    }

    const nextClient = new KalamDBClient({
      url: getBackendUrl(),
      authProvider: async () => Auth.jwt(jwtToken),
    });

    console.log('[kalam-client] Initializing WASM...');
    await nextClient.initialize();

    client = nextClient;
    currentToken = jwtToken;
    isInitialized = true;
    console.log('[kalam-client] WASM initialized successfully');
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
  console.log('[kalam-client] setClientToken called');
  await initializeClient(token);
}

/**
 * Clear the client when user logs out
 */
export async function clearClient(): Promise<void> {
  await queueLifecycle(async () => {
    console.log('[kalam-client] clearClient called');
    const existingClient = client;
    client = null;
    currentToken = null;
    isInitialized = false;

    if (existingClient) {
      try {
        await existingClient.disconnect();
      } catch {
        // Ignore disconnect errors on logout
      }
    }
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
  if (!currentToken) {
    throw new Error('Not authenticated. Please log in first.');
  }
  
  if (!client || !isInitialized) {
    console.log('[kalam-client] Client not initialized, initializing now...');
    await initializeClient(currentToken);
  }
  
  console.log("[kalam-client] Executing query via SDK:", sql.substring(0, 120) + (sql.length > 120 ? "..." : ""));
  
  // Queue the query to prevent concurrent WASM access
  return queueQuery(async () => {
    try {
      const response = await client!.query(sql);
      if (response.status === "error") {
        console.warn("[kalam-client] Query failed:", response.error?.message ?? "Unknown error");
      } else {
        const first = response.results?.[0];
        const resultCount = response.results?.length ?? 0;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const rowCount = first?.row_count ?? (first as any)?.named_rows?.length ?? first?.rows?.length ?? 0;
        const columnCount = first?.schema?.length ?? 0;
        console.log(
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

/**
 * Convert array-based rows to Record objects using schema
 * New API format: { schema: [{name, data_type, index}], rows: [[val1, val2], ...] }
 * Old/convenience format: [{col1: val1, col2: val2}, ...]
 */
function convertRowsToObjects(
  schema: { name: string; data_type: string; index: number }[] | undefined,
  rows: unknown[][] | undefined
): Record<string, unknown>[] {
  if (!rows || !schema || schema.length === 0) {
    return [];
  }
  
  return rows.map((row) => {
    const obj: Record<string, unknown> = {};
    schema.forEach((field) => {
      obj[field.name] = row[field.index] ?? null;
    });
    return obj;
  });
}

/**
 * Extract named_rows from a query result (new Rust WASM format).
 * The WASM layer pre-computes named_rows (schema → map) so we don't need
 * to reconstruct from positional rows + schema.
 */
function extractNamedRows(
  result: unknown
): Record<string, unknown>[] | undefined {
  const r = result as Record<string, unknown> | undefined;
  const named = r?.named_rows;
  return Array.isArray(named) ? (named as Record<string, unknown>[]) : undefined;
}

/**
 * Execute SQL and return rows from the first result set
 * Convenience function for hooks that just need rows
 * Converts the new array-based row format to Record objects for backwards compatibility
 */
export async function executeSql(sql: string): Promise<Record<string, unknown>[]> {
  try {
    const response = await executeQuery(sql);
    
    if (response.status === 'error' && response.error) {
      console.error('[kalam-client] SQL execution error:', response.error);
      throw new Error(response.error.message);
    }
    
    const result = response.results?.[0];
    if (!result) {
      return [];
    }
    
    // Prefer named_rows: Rust WASM pre-computes the schema→map transformation.
    const namedRows = extractNamedRows(result);
    if (namedRows) {
      return namedRows;
    }

    // Fallback: positional rows + schema (older server versions)
    const schema = (result as unknown as { schema?: { name: string; data_type: string; index: number }[] }).schema;
    const rows = result.rows as unknown[][] | undefined;
    return convertRowsToObjects(schema, rows);
  } catch (err) {
    console.error('[kalam-client] executeSql failed:', err);
    throw err;
  }
}

/**
 * Insert data into a table using SDK's insert method
 */
export async function insert(tableName: string, data: Record<string, unknown>): Promise<QueryResponse> {
  if (!currentToken) {
    throw new Error('Not authenticated. Please log in first.');
  }
  
  if (!client || !isInitialized) {
    await initializeClient(currentToken);
  }
  
  return queueQuery(() => client!.insert(tableName, data));
}

/**
 * Delete a row from a table using SDK's delete method
 */
export async function deleteRow(tableName: string, rowId: string | number): Promise<void> {
  if (!currentToken) {
    throw new Error('Not authenticated. Please log in first.');
  }
  
  if (!client || !isInitialized) {
    await initializeClient(currentToken);
  }
  
  return queueQuery(() => client!.delete(tableName, rowId));
}

/**
 * Check if client is connected (WebSocket)
 */
export function isClientConnected(): boolean {
  return client !== null && isInitialized && client.isConnected();
}

/**
 * Connect WebSocket for real-time subscriptions
 */
export async function connectWebSocket(): Promise<void> {
  if (!currentToken) {
    throw new Error('Not authenticated. Please log in first.');
  }
  
  if (!client || !isInitialized) {
    await initializeClient(currentToken);
  }
  
  //await client!.connect();
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
        } else if (keyLower === 'from_seq_id') {
          options.from_seq_id = parseInt(value, 10);
        }
      }
    }
    
    console.log('[kalam-client] Parsed OPTIONS from SQL:', options);
  }
  
  return { sql: cleanSql, options };
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
  if (!currentToken) {
    throw new Error('Not authenticated. Please log in first.');
  }
  
  if (!client || !isInitialized) {
    await initializeClient(currentToken);
  }
  
  // // Ensure WebSocket is connected
  // if (!client!.isConnected()) {
  //   console.log('[kalam-client] Connecting WebSocket for subscription...');
  //   await client!.connect();
  // }
  
  // Parse OPTIONS from SQL if present (CLI-style syntax)
  const { sql: cleanSql, options: parsedOptions } = parseOptionsFromSql(tableOrQuery);
  
  // Merge options: parsed from SQL > passed options > defaults
  const subscribeOptions: SubscriptionOptions = {
    last_rows: parsedOptions.last_rows ?? options?.last_rows ?? 100,
    batch_size: parsedOptions.batch_size ?? options?.batch_size ?? 1000,
    from_seq_id: parsedOptions.from_seq_id ?? options?.from_seq_id,
  };
  
  console.log('[kalam-client] Subscribing to:', cleanSql, 'with options:', subscribeOptions);
  
  // Detect if input is a SQL query or just a table name
  const trimmed = cleanSql.trim().toLowerCase();
  const isSqlQuery = trimmed.startsWith('select ') || 
                     trimmed.startsWith('select\n') || 
                     trimmed.startsWith('select\t');
  
  let unsubscribe: Unsubscribe;
  if (isSqlQuery) {
    // Full SQL query - use subscribeWithSql
    console.log('[kalam-client] Detected SQL query, using subscribeWithSql');
    unsubscribe = await client!.subscribeWithSql(cleanSql, callback, subscribeOptions);
  } else {
    // Just a table name - use subscribe (which wraps in SELECT * FROM)
    console.log('[kalam-client] Detected table name, using subscribe');
    unsubscribe = await client!.subscribe(cleanSql, callback, subscribeOptions);
  }
  
  console.log('[kalam-client] Subscription registered successfully');
  
  return unsubscribe;
}

/**
 * Get subscription count
 */
export function getSubscriptionCount(): number {
  return client?.getSubscriptionCount() ?? 0;
}

// Re-export types for convenience
export type { QueryResponse, ServerMessage, Unsubscribe, SubscriptionOptions };
