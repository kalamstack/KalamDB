/**
 * kalam-link — Official TypeScript/JavaScript client for KalamDB
 *
 * @example
 * ```typescript
 * import { createClient, Auth } from 'kalam-link';
 *
 * const client = createClient({
 *   url: 'http://localhost:8080',
 *   auth: Auth.basic('admin', 'admin'),
 * });
 *
 * await client.login();
 * await client.connect();
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
  Username,
  wrapRowMap,
} from './types.js';

export type {
  AckResponse,
  BatchControl,
  BatchStatus,
  ChangeTypeRaw,
  ClientOptions,
  ConnectionError,
  ConsumeContext,
  ConsumerHandle,
  ConsumerHandler,
  ConsumeMessage,
  ConsumeRequest,
  ConsumeResponse,
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
  SeqId,
  ServerMessage,
  SubscriptionCallback,
  SubscriptionInfo,
  SubscriptionOptions,
  TimestampFormat,
  TypedSubscriptionCallback,
  Unsubscribe,
  UploadProgress,
} from './types.js';

// Agent runtime
export {
  createLangChainAdapter,
  runAgent,
  runConsumer,
} from './agent.js';

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

// Client
export { createClient, KalamDBClient } from './client.js';

// Query helpers
export {
  normalizeQueryResponse,
  sortColumns,
  SYSTEM_TABLES_ORDER,
} from './helpers/query_helpers.js';

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
export type { KalamClient as WasmKalamClient } from '../.wasm-out/kalam_link.js';

// Default export
export { KalamDBClient as default } from './client.js';
