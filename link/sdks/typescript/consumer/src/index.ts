/**
 * @packageDocumentation
 *
 * Worker-facing topic consumer APIs for KalamDB.
 *
 * Use `@kalamdb/client` for app-facing queries, live rows, subscriptions, and file uploads.
 * Use `@kalamdb/consumer` when you need topic consumption or the agent runtime.
 * This package ships a separate worker-focused WASM bundle on top of `@kalamdb/client`.
 */

export {
  Auth,
  buildAuthHeader,
  encodeBasicAuth,
  isAuthenticated,
  isBasicAuth,
  isJwtAuth,
  isNoAuth,
} from '@kalamdb/client';

export type {
  AuthCredentials,
  AuthProvider,
  BasicAuthCredentials,
  ClientOptions,
  JwtAuthCredentials,
  LoginResponse,
  LoginUserInfo,
  NoAuthCredentials,
  QueryResponse,
  RowData,
  Username,
} from '@kalamdb/client';

export { createConsumerClient, KalamConsumerClient } from './client.js';

export {
  createLangChainAdapter,
  runAgent,
  runConsumer,
} from './agent.js';

export type {
  AckResponse,
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
  ConsumeContext,
  ConsumerClientLike,
  ConsumerClientOptions,
  ConsumerHandle,
  ConsumerHandler,
  ConsumeMessage,
  ConsumeRequest,
  ConsumeResponse,
  ConsumeStart,
  LangChainChatModelLike,
  RunAgentOptions,
  RunConsumerOptions,
} from './types.js';

export { KalamConsumerClient as default } from './client.js';