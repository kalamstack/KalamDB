import type { KalamDBClient } from './client.js';
import type {
  ConsumeContext,
  ConsumeMessage,
  ConsumeRequest,
  QueryResponse,
  RowData,
} from './types.js';

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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  invoke: (...args: any[]) => Promise<any>;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  stream?: (...args: any[]) => AsyncIterable<any> | Promise<AsyncIterable<any>>;
}

export interface AgentRetryPolicy {
  /** Total attempts including the first attempt. */
  maxAttempts?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
  multiplier?: number;
  /** Randomization factor applied to backoff. 0 disables jitter. */
  jitterRatio?: number;
  /** Optional predicate for classifying retryable errors. */
  shouldRetry?: (error: unknown, attempt: number) => boolean;
}

export interface AgentContext<TRow extends Record<string, unknown>> {
  readonly name: string;
  readonly topic: string;
  readonly groupId: string;
  readonly runKey: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly message: ConsumeMessage;
  readonly row: TRow;
  readonly username: string | undefined;
  readonly systemPrompt: string | undefined;
  readonly llm: AgentLLMContext | null;
  sql: (sql: string, params?: unknown[]) => Promise<QueryResponse>;
  queryOne: (
    sql: string,
    params?: unknown[],
  ) => Promise<RowData | null>;
  queryAll: (
    sql: string,
    params?: unknown[],
  ) => Promise<RowData[]>;
  ack: () => Promise<void>;
}

export interface AgentFailureContext<TRow extends Record<string, unknown>>
  extends AgentContext<TRow> {
  readonly error: unknown;
}

export type AgentRowParser<TRow extends Record<string, unknown>> = (
  message: ConsumeMessage,
) => TRow | null;

export type AgentRunKeyFactory = (args: {
  name: string;
  message: ConsumeMessage;
}) => string;

export type AgentFailureHandler<TRow extends Record<string, unknown>> = (
  ctx: AgentFailureContext<TRow>,
) => Promise<void>;

export interface RunAgentOptions<TRow extends Record<string, unknown> = Record<string, unknown>> {
  client: KalamDBClient;
  /** Logical agent name used in run key generation and logs. */
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
  rowParser?: AgentRowParser<TRow>;
  /**
   * Called for each consumed row.
   * If this throws, retry policy is applied.
   */
  onRow: (ctx: AgentContext<TRow>, row: TRow) => Promise<void>;
  /**
   * Called once retries are exhausted.
   * If this throws, no ack is sent (message is retried by server on next poll).
   */
  onFailed?: AgentFailureHandler<TRow>;
  /**
   * Whether to ack after `onFailed` succeeds.
   * Defaults to true when `onFailed` is set.
   */
  ackOnFailed?: boolean;
  stopSignal?: AbortSignal;
  onRetry?: (args: {
    error: unknown;
    attempt: number;
    maxAttempts: number;
    backoffMs: number;
    runKey: string;
    message: ConsumeMessage;
  }) => void;
  onError?: (args: {
    error: unknown;
    runKey: string;
    message: ConsumeMessage;
  }) => void;
}

export interface RunConsumerOptions {
  client: KalamDBClient;
  name: string;
  topic: string;
  groupId: string;
  start?: ConsumeRequest['start'];
  batchSize?: number;
  partitionId?: number;
  timeoutSeconds?: number;
  retry?: AgentRetryPolicy;
  stopSignal?: AbortSignal;
  onMessage: (ctx: AgentContext<Record<string, unknown>>) => Promise<void>;
  onFailed?: AgentFailureHandler<Record<string, unknown>>;
  ackOnFailed?: boolean;
  onRetry?: RunAgentOptions<Record<string, unknown>>['onRetry'];
  onError?: RunAgentOptions<Record<string, unknown>>['onError'];
}

const DEFAULT_RETRY: Required<Pick<AgentRetryPolicy, 'maxAttempts' | 'initialBackoffMs' | 'maxBackoffMs' | 'multiplier' | 'jitterRatio'>> = {
  maxAttempts: 3,
  initialBackoffMs: 300,
  maxBackoffMs: 5_000,
  multiplier: 2,
  jitterRatio: 0,
};

function normalizeRetryPolicy(retry?: AgentRetryPolicy): Required<AgentRetryPolicy> {
  const maxAttempts = Math.max(1, Math.floor(retry?.maxAttempts ?? DEFAULT_RETRY.maxAttempts));
  const initialBackoffMs = Math.max(0, Math.floor(retry?.initialBackoffMs ?? DEFAULT_RETRY.initialBackoffMs));
  const maxBackoffMs = Math.max(initialBackoffMs, Math.floor(retry?.maxBackoffMs ?? DEFAULT_RETRY.maxBackoffMs));
  const multiplier = Math.max(1, retry?.multiplier ?? DEFAULT_RETRY.multiplier);
  const jitterRatio = Math.min(1, Math.max(0, retry?.jitterRatio ?? DEFAULT_RETRY.jitterRatio));

  return {
    maxAttempts,
    initialBackoffMs,
    maxBackoffMs,
    multiplier,
    jitterRatio,
    shouldRetry: retry?.shouldRetry ?? (() => true),
  };
}

function backoffMsForAttempt(attempt: number, policy: Required<AgentRetryPolicy>): number {
  if (attempt <= 1 || policy.initialBackoffMs <= 0) {
    return 0;
  }
  const exponent = attempt - 2;
  const base = policy.initialBackoffMs * Math.pow(policy.multiplier, exponent);
  const clamped = Math.min(policy.maxBackoffMs, Math.floor(base));

  if (policy.jitterRatio <= 0) {
    return clamped;
  }

  const jitterWindow = Math.floor(clamped * policy.jitterRatio);
  const min = Math.max(0, clamped - jitterWindow);
  const max = clamped + jitterWindow;
  return Math.floor(min + Math.random() * (max - min + 1));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function defaultRunKeyFactory({ name, message }: { name: string; message: ConsumeMessage }): string {
  return `${name}:${message.topic}:${message.partition_id}:${message.offset}`;
}

function defaultRowParser<TRow extends Record<string, unknown>>(message: ConsumeMessage): TRow | null {
  const payload = message.value as unknown;
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const envelope = payload as Record<string, unknown>;
  const row = envelope.row;
  if (row && typeof row === 'object') {
    return row as TRow;
  }

  return envelope as TRow;
}

function normalizeLLMInput(
  input: string | Omit<AgentLLMInput, 'systemPrompt'>,
  systemPrompt?: string,
  runKey?: string,
  row?: Record<string, unknown>,
): AgentLLMInput {
  if (typeof input === 'string') {
    return {
      prompt: input,
      systemPrompt,
      runKey,
      row,
    };
  }

  return {
    ...input,
    systemPrompt,
    runKey,
    row,
  };
}

function createLLMContext(
  llm: AgentLLMAdapter | undefined,
  systemPrompt: string | undefined,
  runKey: string,
  row: Record<string, unknown>,
): AgentLLMContext | null {
  if (!llm) {
    return null;
  }

  return {
    complete: async (input) => llm.complete(normalizeLLMInput(input, systemPrompt, runKey, row)),
    stream: async function* (input) {
      if (!llm.stream) {
        throw new Error('LLM adapter does not support streaming');
      }
      const stream = llm.stream(normalizeLLMInput(input, systemPrompt, runKey, row));
      for await (const chunk of stream) {
        yield chunk;
      }
    },
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function extractText(value: unknown): string {
  if (typeof value === 'string') {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map(extractText).join('');
  }

  if (!isRecord(value)) {
    return '';
  }

  if (typeof value.text === 'string') {
    return value.text;
  }

  if (typeof value.content === 'string') {
    return value.content;
  }

  if (Array.isArray(value.content)) {
    return value.content
      .map((item) => {
        if (typeof item === 'string') {
          return item;
        }
        if (isRecord(item) && typeof item.text === 'string') {
          return item.text;
        }
        return extractText(item);
      })
      .join('');
  }

  if (isRecord(value.message)) {
    return extractText(value.message);
  }

  return '';
}

function toLangChainInput(input: AgentLLMInput): Array<{ role: AgentLLMRole; content: string }> {
  const messages: Array<{ role: AgentLLMRole; content: string }> = [];

  const systemPrompt = input.systemPrompt?.trim();
  if (systemPrompt) {
    messages.push({ role: 'system', content: systemPrompt });
  }

  if (input.messages && input.messages.length > 0) {
    for (const message of input.messages) {
      const content = message.content?.trim();
      if (!content) {
        continue;
      }
      messages.push({ role: message.role, content });
    }
  } else if (typeof input.prompt === 'string') {
    const prompt = input.prompt.trim();
    if (prompt) {
      messages.push({ role: 'user', content: prompt });
    }
  }

  return messages;
}

/**
 * Adapter helper for LangChain chat models (duck-typed `invoke` + optional `stream`).
 * Keeps `kalam-link` free from hard LangChain runtime dependency while enabling direct usage.
 */
export function createLangChainAdapter(model: LangChainChatModelLike): AgentLLMAdapter {
  return {
    complete: async (input: AgentLLMInput): Promise<string> => {
      const response = await model.invoke(toLangChainInput(input));
      return extractText(response).trim();
    },
    stream: async function* (input: AgentLLMInput): AsyncIterable<string> {
      if (!model.stream) {
        throw new Error('Provided LangChain model does not expose stream()');
      }
      const stream = await model.stream(toLangChainInput(input));
      for await (const chunk of stream) {
        const text = extractText(chunk);
        if (text) {
          yield text;
        }
      }
    },
  };
}

export async function runAgent<TRow extends Record<string, unknown> = Record<string, unknown>>(
  options: RunAgentOptions<TRow>,
): Promise<void> {
  if (!options.name.trim()) {
    throw new Error('runAgent: name is required');
  }
  if (!options.topic.trim()) {
    throw new Error('runAgent: topic is required');
  }
  if (!options.groupId.trim()) {
    throw new Error('runAgent: groupId is required');
  }

  const retryPolicy = normalizeRetryPolicy(options.retry);
  const runKeyFactory = options.runKeyFactory ?? defaultRunKeyFactory;
  const rowParser = options.rowParser ?? defaultRowParser<TRow>;

  const consumerOptions: ConsumeRequest = {
    topic: options.topic,
    group_id: options.groupId,
    start: options.start ?? 'latest',
    batch_size: options.batchSize ?? 20,
    partition_id: options.partitionId ?? 0,
    timeout_seconds: options.timeoutSeconds ?? 30,
    auto_ack: false,
  };

  const consumer = options.client.consumer(consumerOptions);
  const abortHandler = () => consumer.stop();
  options.stopSignal?.addEventListener('abort', abortHandler, { once: true });

  try {
    await consumer.run(async (consumeCtx: ConsumeContext) => {
      const row = rowParser(consumeCtx.message);
      if (!row) {
        return;
      }

      const runKey = runKeyFactory({
        name: options.name,
        message: consumeCtx.message,
      });

      let acked = false;
      const ack = async (): Promise<void> => {
        if (acked) {
          return;
        }
        await consumeCtx.ack();
        acked = true;
      };

      let lastError: unknown;

      for (let attempt = 1; attempt <= retryPolicy.maxAttempts; attempt += 1) {
        const ctx: AgentContext<TRow> = {
          name: options.name,
          topic: options.topic,
          groupId: options.groupId,
          runKey,
          attempt,
          maxAttempts: retryPolicy.maxAttempts,
          message: consumeCtx.message,
          row,
          username: consumeCtx.username as string | undefined,
          systemPrompt: options.systemPrompt,
          llm: createLLMContext(options.llm, options.systemPrompt, runKey, row),
          sql: async (sql: string, params?: unknown[]) => options.client.query(sql, params),
          queryOne: async (
            sql: string,
            params?: unknown[],
          ) => options.client.queryOne(sql, params),
          queryAll: async (
            sql: string,
            params?: unknown[],
          ) => options.client.queryAll(sql, params),
          ack,
        };

        try {
          await options.onRow(ctx, row);
          await ack();
          return;
        } catch (error) {
          lastError = error;
          if (acked) {
            options.onError?.({ error, runKey, message: consumeCtx.message });
            return;
          }

          const shouldRetry =
            attempt < retryPolicy.maxAttempts && retryPolicy.shouldRetry(error, attempt);

          if (!shouldRetry) {
            break;
          }

          const backoffMs = backoffMsForAttempt(attempt + 1, retryPolicy);
          options.onRetry?.({
            error,
            attempt,
            maxAttempts: retryPolicy.maxAttempts,
            backoffMs,
            runKey,
            message: consumeCtx.message,
          });
          if (backoffMs > 0) {
            await sleep(backoffMs);
          }
        }
      }

      if (!options.onFailed) {
        options.onError?.({
          error: lastError ?? new Error('Agent message failed with unknown error'),
          runKey,
          message: consumeCtx.message,
        });
        return;
      }

      const failedCtx: AgentFailureContext<TRow> = {
        name: options.name,
        topic: options.topic,
        groupId: options.groupId,
        runKey,
        attempt: retryPolicy.maxAttempts,
        maxAttempts: retryPolicy.maxAttempts,
        message: consumeCtx.message,
        row,
        username: consumeCtx.username as string | undefined,
        systemPrompt: options.systemPrompt,
        llm: createLLMContext(options.llm, options.systemPrompt, runKey, row),
        sql: async (sql: string, params?: unknown[]) => options.client.query(sql, params),
        queryOne: async (
          sql: string,
          params?: unknown[],
        ) => options.client.queryOne(sql, params),
        queryAll: async (
          sql: string,
          params?: unknown[],
        ) => options.client.queryAll(sql, params),
        ack,
        error: lastError,
      };

      try {
        await options.onFailed(failedCtx);
      } catch (failureHandlerError) {
        options.onError?.({
          error: failureHandlerError,
          runKey,
          message: consumeCtx.message,
        });
        return;
      }

      const shouldAckAfterFailure = options.ackOnFailed ?? true;
      if (shouldAckAfterFailure) {
        try {
          await ack();
        } catch (error) {
          options.onError?.({
            error,
            runKey,
            message: consumeCtx.message,
          });
        }
      }
    });
  } finally {
    options.stopSignal?.removeEventListener('abort', abortHandler);
  }
}

export async function runConsumer(options: RunConsumerOptions): Promise<void> {
  await runAgent<Record<string, unknown>>({
    client: options.client,
    name: options.name,
    topic: options.topic,
    groupId: options.groupId,
    start: options.start,
    batchSize: options.batchSize,
    partitionId: options.partitionId,
    timeoutSeconds: options.timeoutSeconds,
    retry: options.retry,
    onRow: async (ctx) => {
      await options.onMessage(ctx);
    },
    onFailed: options.onFailed,
    ackOnFailed: options.ackOnFailed,
    stopSignal: options.stopSignal,
    onRetry: options.onRetry,
    onError: options.onError,
  });
}
