import {
  KalamDBClient,
  buildAuthHeader,
  resolveAuthProviderWithRetry,
} from '@kalamdb/client';

import type {
  AuthCredentials,
  ClientOptions,
  LoginResponse,
  QueryResponse,
  RowData,
  UserId,
} from '@kalamdb/client';
import type {
  AckResponse,
  ConsumePayload,
  ConsumeMessage,
  ConsumeContext,
  ConsumerClientOptions,
  ConsumerHandle,
  ConsumerHandler,
  ConsumeRequest,
  ConsumeResponse,
} from './types.js';
import {
  ConsumerWasmTransport,
  type AckTransportRequest,
  type ConsumeTransportRequest,
  type ConsumeWireMessage,
  type ConsumeWireResponse,
} from './wasm_transport.js';

type TopicStartPayload = 'Latest' | 'Earliest' | { Offset: number };

type TopicAuthCache = {
  sourceKey: string;
  auth: AuthCredentials;
};

const DEFAULT_BATCH_SIZE = 10;
const DEFAULT_IDLE_DELAY_MS = 1000;

class TopicRequestError extends Error {
  readonly status: number;
  readonly code?: string;

  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = 'TopicRequestError';
    this.status = status;
    this.code = code;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type TopicErrorLike = {
  message?: unknown;
  status?: unknown;
  code?: unknown;
};

function isTopicErrorLike(value: unknown): value is TopicErrorLike {
  return Boolean(value) && typeof value === 'object';
}

function normalizeConsumeMessage<TPayload extends ConsumePayload>(
  message: ConsumeWireMessage<TPayload>,
): ConsumeMessage<TPayload> {
  return {
    ...message,
    value: message.payload,
  };
}

function normalizeConsumeResponse<TPayload extends ConsumePayload>(
  response: ConsumeWireResponse<TPayload>,
): ConsumeResponse<TPayload> {
  return {
    ...response,
    messages: response.messages.map(normalizeConsumeMessage),
  };
}

function normalizeStart(start: ConsumeRequest['start']): TopicStartPayload {
  if (typeof start === 'number' && Number.isFinite(start)) {
    return { Offset: Math.max(0, Math.floor(start)) };
  }

  if (typeof start === 'string') {
    const normalized = start.trim().toLowerCase();
    if (!normalized || normalized === 'latest') {
      return 'Latest';
    }
    if (normalized === 'earliest') {
      return 'Earliest';
    }
    if (/^\d+$/.test(normalized)) {
      return { Offset: Number.parseInt(normalized, 10) };
    }
    throw new Error(`Invalid consume start value: ${start}`);
  }

  if (start && typeof start === 'object') {
    const offset = 'offset' in start ? start.offset : start.Offset;
    if (typeof offset === 'number' && Number.isFinite(offset)) {
      return { Offset: Math.max(0, Math.floor(offset)) };
    }
  }

  return 'Latest';
}

export class KalamConsumerClient {
  private readonly sqlClient: KalamDBClient;
  private readonly url: string;
  private readonly authProvider: ClientOptions['authProvider'];
  private readonly authProviderMaxAttempts: number;
  private readonly authProviderInitialBackoffMs: number;
  private readonly authProviderMaxBackoffMs: number;
  private readonly topicTransport: ConsumerWasmTransport;
  private cachedTopicAuth: TopicAuthCache | null = null;

  constructor(options: ConsumerClientOptions) {
    if (!options.url) {
      throw new Error('KalamConsumerClient: url is required');
    }
    if (!options.authProvider) {
      throw new Error('KalamConsumerClient: authProvider is required');
    }

    this.sqlClient = new KalamDBClient(options);
    this.url = options.url;
    this.authProvider = options.authProvider;
    this.authProviderMaxAttempts = options.authProviderMaxAttempts ?? 3;
    this.authProviderInitialBackoffMs = options.authProviderInitialBackoffMs ?? 250;
    this.authProviderMaxBackoffMs = options.authProviderMaxBackoffMs ?? 2000;
    this.topicTransport = new ConsumerWasmTransport(
      options.url,
      options.consumerWasmUrl,
    );
  }

  get baseClient(): KalamDBClient {
    return this.sqlClient;
  }

  getAuthType(): 'basic' | 'jwt' | null {
    return this.sqlClient.getAuthType();
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResponse> {
    return this.sqlClient.query(sql, params);
  }

  async queryOne(sql: string, params?: unknown[]): Promise<RowData | null> {
    return this.sqlClient.queryOne(sql, params);
  }

  async queryAll(sql: string, params?: unknown[]): Promise<RowData[]> {
    return this.sqlClient.queryAll(sql, params);
  }

  async executeAsUser(
    sql: string,
    user: UserId | string,
    params?: unknown[],
  ): Promise<QueryResponse> {
    return this.sqlClient.executeAsUser(sql, user, params);
  }

  async login(): Promise<LoginResponse> {
    const response = await this.sqlClient.login();
    this.cachedTopicAuth = {
      sourceKey: `jwt:${response.access_token}`,
      auth: { type: 'jwt', token: response.access_token },
    };
    return response;
  }

  async refreshToken(refreshToken: string): Promise<LoginResponse> {
    const response = await this.sqlClient.refreshToken(refreshToken);
    this.cachedTopicAuth = {
      sourceKey: `jwt:${response.access_token}`,
      auth: { type: 'jwt', token: response.access_token },
    };
    return response;
  }

  async disconnect(): Promise<void> {
    this.cachedTopicAuth = null;
    await this.sqlClient.disconnect();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.disconnect();
  }

  [Symbol.dispose](): void {
    void this.disconnect();
  }

  consumer<TPayload extends ConsumePayload = ConsumePayload>(
    options: ConsumeRequest,
  ): ConsumerHandle<TPayload> {
    let stopRequested = false;
    let nextStart = options.start;

    return {
      run: async (handler: ConsumerHandler<TPayload>): Promise<void> => {
        stopRequested = false;
        nextStart = options.start;

        while (!stopRequested) {
          const response = await this.consumeBatch<TPayload>({
            ...options,
            ...(nextStart === undefined ? {} : { start: nextStart }),
          });

          // Keep the server cursor after empty polls so start='latest'
          // continues from the observed high-water mark instead of skipping
          // later inserts by recalculating "latest" on every loop.
          if (response.messages.length === 0) {
            nextStart = { offset: response.next_offset };
          }

          for (const message of response.messages) {
            if (stopRequested) {
              break;
            }

            let acked = false;
            const ctx: ConsumeContext<TPayload> = {
              user: message.user,
              message,
              ack: async () => {
                if (acked) {
                  return;
                }
                acked = true;
                await this.ack(
                  message.topic,
                  message.group_id,
                  message.partition_id,
                  message.offset,
                );
              },
            };

            await handler(ctx);

            if (options.auto_ack && !acked) {
              await ctx.ack();
            }
          }

          if (!stopRequested && !response.has_more) {
            await sleep(DEFAULT_IDLE_DELAY_MS);
          }
        }
      },
      stop: () => {
        stopRequested = true;
      },
    };
  }

  async consumeBatch<TPayload extends ConsumePayload = ConsumePayload>(
    options: ConsumeRequest,
  ): Promise<ConsumeResponse<TPayload>> {
    const request: ConsumeTransportRequest = {
        topic_id: options.topic,
        group_id: options.group_id,
        start: normalizeStart(options.start),
        limit: options.batch_size ?? DEFAULT_BATCH_SIZE,
        partition_id: options.partition_id ?? 0,
        ...(typeof options.timeout_seconds === 'number'
          ? { timeout_seconds: options.timeout_seconds }
          : {}),
      };

    const response = await this.requestTopic<ConsumeWireResponse<TPayload>, ConsumeTransportRequest>(
      request,
      (authHeader, body) => this.topicTransport.consume<TPayload>(authHeader, body),
    );

    return normalizeConsumeResponse(response);
  }

  async ack(
    topic: string,
    groupId: string,
    partitionId: number,
    uptoOffset: number,
  ): Promise<AckResponse> {
    const request: AckTransportRequest = {
        topic_id: topic,
        group_id: groupId,
        partition_id: partitionId,
        upto_offset: uptoOffset,
      };

    return this.requestTopic<AckResponse, AckTransportRequest>(
      request,
      (authHeader, body) => this.topicTransport.ack(authHeader, body),
    );
  }

  private async requestTopic<TResponse, TBody>(
    body: TBody,
    operation: (authHeader: string | undefined, body: TBody) => Promise<TResponse>,
  ): Promise<TResponse> {
    try {
      return await this.performTopicRequest(operation, body, false);
    } catch (error) {
      const normalizedError = this.coerceTopicRequestError(error);
      if (!this.isRetryableTopicAuthError(normalizedError)) {
        throw normalizedError;
      }

      return this.performTopicRequest(operation, body, true);
    }
  }

  private async performTopicRequest<TResponse, TBody>(
    operation: (authHeader: string | undefined, body: TBody) => Promise<TResponse>,
    body: TBody,
    forceRefresh: boolean,
  ): Promise<TResponse> {
    const auth = await this.resolveTopicAuth(forceRefresh);
    return operation(buildAuthHeader(auth), body);
  }

  private coerceTopicRequestError(error: unknown): unknown {
    if (error instanceof TopicRequestError) {
      return error;
    }

    if (!isTopicErrorLike(error)) {
      return error;
    }

    const status = typeof error.status === 'number'
      ? error.status
      : typeof error.status === 'string' && /^\d+$/.test(error.status)
        ? Number.parseInt(error.status, 10)
        : undefined;
    if (status === undefined) {
      return error;
    }

    return new TopicRequestError(
      typeof error.message === 'string' ? error.message : `Topic request failed: HTTP ${status}`,
      status,
      typeof error.code === 'string' ? error.code : undefined,
    );
  }

  private async resolveTopicAuth(forceRefresh: boolean): Promise<AuthCredentials> {
    const creds = await resolveAuthProviderWithRetry(this.authProvider, {
      maxAttempts: this.authProviderMaxAttempts,
      initialBackoffMs: this.authProviderInitialBackoffMs,
      maxBackoffMs: this.authProviderMaxBackoffMs,
    });
    const sourceKey = this.authSourceKey(creds);

    if (!forceRefresh && this.cachedTopicAuth?.sourceKey === sourceKey) {
      return this.cachedTopicAuth.auth;
    }

    const effectiveAuth = await this.normalizeTopicAuth(creds);
    this.cachedTopicAuth = {
      sourceKey,
      auth: effectiveAuth,
    };
    return effectiveAuth;
  }

  private async normalizeTopicAuth(auth: AuthCredentials): Promise<AuthCredentials> {
    if (auth.type !== 'basic') {
      return auth;
    }

    const response = await this.performDirectBasicLogin(auth.user, auth.password);
    return { type: 'jwt', token: response.access_token };
  }

  private authSourceKey(auth: AuthCredentials): string {
    switch (auth.type) {
      case 'basic':
        return `basic:${auth.user}:${auth.password}`;
      case 'jwt':
        return `jwt:${auth.token}`;
      default: {
        const exhaustive: never = auth;
        return String(exhaustive);
      }
    }
  }

  private async performDirectBasicLogin(user: string, password: string): Promise<LoginResponse> {
    const response = await fetch(`${this.url}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user, password }),
    });

    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(body || `Login failed: HTTP ${response.status}`);
    }

    return (await response.json()) as LoginResponse;
  }

  private isRetryableTopicAuthError(error: unknown): boolean {
    const normalizedError = this.coerceTopicRequestError(error);
    if (!(normalizedError instanceof TopicRequestError)) {
      return false;
    }

    return normalizedError.status === 401
      || normalizedError.code === 'TOKEN_EXPIRED'
      || normalizedError.code === 'UNAUTHENTICATED';
  }
}

export function createConsumerClient(options: ConsumerClientOptions): KalamConsumerClient {
  return new KalamConsumerClient(options);
}