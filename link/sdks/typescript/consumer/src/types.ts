import type {
  ClientOptions,
  LoginResponse,
  QueryResponse,
  RowData,
  UserId,
} from '@kalamdb/client';

export type {
  AuthCredentials,
  AuthProvider,
  BasicAuthCredentials,
  ClientOptions,
  JwtAuthCredentials,
  LoginResponse,
  LoginUserInfo,
  QueryResponse,
  RowData,
  UserId,
} from '@kalamdb/client';

export interface ConsumerClientOptions extends ClientOptions {
  /**
   * Explicit URL or buffer for the consumer-only WASM file.
   * - Browser: string URL like '/wasm/kalam_consumer_bg.wasm'
   * - Node.js: BufferSource from fs.readFile
   */
  consumerWasmUrl?: string | BufferSource;
}

export type ConsumeStart = 'latest' | 'earliest' | number | { offset: number } | { Offset: number };

export interface ConsumeRequest {
  topic: string;
  group_id: string;
  start?: ConsumeStart;
  batch_size?: number;
  partition_id?: number;
  timeout_seconds?: number;
  auto_ack?: boolean;
  concurrency_per_partition?: number;
}

export type ConsumePayload = Record<string, unknown>;

export interface ConsumeMessage<TPayload extends ConsumePayload = ConsumePayload> {
  key?: string;
  op?: string;
  timestamp_ms?: number;
  offset: number;
  partition_id: number;
  topic: string;
  group_id: string;
  user?: UserId;
  payload: TPayload;
  /**
   * @deprecated Use `payload` instead.
   * Kept as a compatibility alias while callers migrate from the older SDK shape.
   */
  value: TPayload;
}

export interface ConsumeResponse<TPayload extends ConsumePayload = ConsumePayload> {
  messages: ConsumeMessage<TPayload>[];
  next_offset: number;
  has_more: boolean;
}

export interface AckResponse {
  success: boolean;
  acknowledged_offset: number;
}

export interface ConsumeContext<TPayload extends ConsumePayload = ConsumePayload> {
  readonly user: UserId | undefined;
  readonly message: ConsumeMessage<TPayload>;
  ack: () => Promise<void>;
}

export type ConsumerHandler<TPayload extends ConsumePayload = ConsumePayload> = (
  ctx: ConsumeContext<TPayload>,
) => Promise<void>;

export interface ConsumerHandle<TPayload extends ConsumePayload = ConsumePayload> {
  run: (handler: ConsumerHandler<TPayload>) => Promise<void>;
  stop: () => void;
}

export interface ConsumerClientLike {
  query: (sql: string, params?: unknown[]) => Promise<QueryResponse>;
  queryOne: (sql: string, params?: unknown[]) => Promise<RowData | null>;
  queryAll: (sql: string, params?: unknown[]) => Promise<RowData[]>;
  consumer: <TPayload extends ConsumePayload = ConsumePayload>(
    options: ConsumeRequest,
  ) => ConsumerHandle<TPayload>;
}

export type AgentLLMRole = 'system' | 'user' | 'assistant';

export interface AgentLLMMessage {
  role: AgentLLMRole;
  content: string;
}

export interface AgentLLMInput {
  prompt?: string;
  messages?: AgentLLMMessage[];
  systemPrompt?: string;
  runKey?: string;
  row?: Record<string, unknown>;
}

export interface AgentLLMAdapter {
  complete: (input: AgentLLMInput) => Promise<string>;
  stream?: (input: AgentLLMInput) => AsyncIterable<string>;
}

export interface AgentLLMContext {
  complete: (input: string | Omit<AgentLLMInput, 'systemPrompt'>) => Promise<string>;
  stream: (input: string | Omit<AgentLLMInput, 'systemPrompt'>) => AsyncIterable<string>;
}

export interface LangChainChatModelLike {
  invoke: (...args: any[]) => Promise<any>;
  stream?: (...args: any[]) => AsyncIterable<any> | Promise<AsyncIterable<any>>;
}

export interface AgentRetryPolicy {
  maxAttempts?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
  multiplier?: number;
  jitterRatio?: number;
  shouldRetry?: (error: unknown, attempt: number) => boolean;
}

export interface AgentContext<TRow extends Record<string, unknown>, TPayload extends ConsumePayload = ConsumePayload> {
  readonly name: string;
  readonly topic: string;
  readonly groupId: string;
  readonly runKey: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly message: ConsumeMessage<TPayload>;
  readonly row: TRow;
  readonly user: UserId | undefined;
  readonly systemPrompt: string | undefined;
  readonly llm: AgentLLMContext | null;
  sql: (sql: string, params?: unknown[]) => Promise<QueryResponse>;
  queryOne: (sql: string, params?: unknown[]) => Promise<RowData | null>;
  queryAll: (sql: string, params?: unknown[]) => Promise<RowData[]>;
  ack: () => Promise<void>;
}

export interface AgentFailureContext<TRow extends Record<string, unknown>, TPayload extends ConsumePayload = ConsumePayload>
  extends AgentContext<TRow, TPayload> {
  readonly error: unknown;
}

export type AgentRowParser<
  TRow extends Record<string, unknown>,
  TPayload extends ConsumePayload = ConsumePayload,
> = (message: ConsumeMessage<TPayload>) => TRow | null;

export type AgentRunKeyFactory = (args: {
  name: string;
  message: ConsumeMessage;
}) => string;

export type AgentFailureHandler<
  TRow extends Record<string, unknown>,
  TPayload extends ConsumePayload = ConsumePayload,
> = (ctx: AgentFailureContext<TRow, TPayload>) => Promise<void>;

export interface RunAgentOptions<
  TRow extends Record<string, unknown> = Record<string, unknown>,
  TPayload extends ConsumePayload = ConsumePayload,
> {
  client: ConsumerClientLike;
  name: string;
  topic: string;
  groupId: string;
  start?: ConsumeRequest['start'];
  batchSize?: number;
  partitionId?: number;
  timeoutSeconds?: number;
  systemPrompt?: string;
  llm?: AgentLLMAdapter;
  retry?: AgentRetryPolicy;
  runKeyFactory?: AgentRunKeyFactory;
  rowParser?: AgentRowParser<TRow, TPayload>;
  onRow: (ctx: AgentContext<TRow, TPayload>, row: TRow) => Promise<void>;
  onFailed?: AgentFailureHandler<TRow, TPayload>;
  ackOnFailed?: boolean;
  stopSignal?: AbortSignal;
  onRetry?: (args: {
    error: unknown;
    attempt: number;
    maxAttempts: number;
    backoffMs: number;
    runKey: string;
    message: ConsumeMessage<TPayload>;
  }) => void;
  onError?: (args: {
    error: unknown;
    runKey: string;
    message: ConsumeMessage<TPayload>;
  }) => void;
}

export interface RunConsumerOptions<TPayload extends ConsumePayload = ConsumePayload> {
  client: ConsumerClientLike;
  name: string;
  topic: string;
  groupId: string;
  start?: ConsumeRequest['start'];
  batchSize?: number;
  partitionId?: number;
  timeoutSeconds?: number;
  retry?: AgentRetryPolicy;
  stopSignal?: AbortSignal;
  onMessage: (ctx: AgentContext<Record<string, unknown>, TPayload>) => Promise<void>;
  onFailed?: AgentFailureHandler<Record<string, unknown>, TPayload>;
  ackOnFailed?: boolean;
  onRetry?: RunAgentOptions<Record<string, unknown>, TPayload>['onRetry'];
  onError?: RunAgentOptions<Record<string, unknown>, TPayload>['onError'];
}