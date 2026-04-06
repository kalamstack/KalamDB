/**
 * @kalamdb/client — Official TypeScript/JavaScript client for KalamDB
 *
 * @example
 * ```typescript
 * import { createClient, Auth } from '@kalamdb/client';
 *
 * const client = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'admin'),
 * });
 *
 * const unsub = await client.subscribe('messages', (event) => {
 *   console.log('Change:', event);
 * });
 *
 * await unsub();
 * await client.disconnect();
 * ```
 *
 * @packageDocumentation
 */

/* ------------------------------------------------------------------ */
/*  Re-exports                                                        */
/* ------------------------------------------------------------------ */

// Auth
export {
  Auth,
  buildAuthHeader,
  encodeBasicAuth,
  isAuthenticated,
  isBasicAuth,
  isJwtAuth,
  isNoAuth,
} from './auth.js';

export type {
  AuthCredentials,
  BasicAuthCredentials,
  JwtAuthCredentials,
  NoAuthCredentials,
  AuthProvider,
} from './auth.js';

// Types & enums
export {
  ChangeType,
  KalamCellValue,
  LogLevel,
  MessageType,
  SeqId,
  Username,
  wrapRowMap,
} from './types.js';

export type {
  BatchControl,
  BatchStatus,
  ChangeTypeRaw,
  ClientOptions,
  ConnectionError,
  DisconnectReason,
  ErrorDetail,
  FieldFlag,
  FieldFlags,
  HealthCheckResponse,
  HttpVersion,
  JsonValue,
  KalamDataType,
  LogEntry,
  LogListener,
  LoginResponse,
  LoginUserInfo,
  LiveRowsCallback,
  LiveRowsOptions,
  OnConnectCallback,
  OnDisconnectCallback,
  OnErrorCallback,
  OnReceiveCallback,
  OnSendCallback,
  QueryResponse,
  QueryResult,
  ResponseStatus,
  RowData,
  SchemaField,
  ServerMessage,
  SubscriptionCallback,
  SubscriptionErrorEvent,
  SubscriptionInfo,
  SubscriptionOptions,
  TimestampFormat,
  TypedSubscriptionCallback,
  Unsubscribe,
  UploadProgress,
} from './types.js';

// Client
export { createClient, KalamDBClient } from './client.js';

// Query helpers
export {
  normalizeQueryResponse,
  sortColumns,
  SYSTEM_TABLES_ORDER,
} from './helpers/query_helpers.js';

export {
  isLikelyTransientAuthProviderError,
  resolveAuthProviderWithRetry,
} from './helpers/auth_provider_retry.js';

export type {
  AuthProviderRetryOptions,
} from './helpers/auth_provider_retry.js';

// FileRef helpers
export {
  BoundFileRef,
  FileRef,
  KalamChange,
  KalamRow,
  parseFileRef,
  parseFileRefs,
  wrapRows,
} from './file_ref.js';

export type {
  FileRefContext,
  FileRefData,
} from './file_ref.js';

// WASM bindings (re-exported so advanced users can access low-level API)
export type { KalamClient as WasmKalamClient } from '../wasm/kalam_client.js';

// Default export
export { KalamDBClient as default } from './client.js';
