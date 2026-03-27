import { KalamCellValue, wrapRowMap } from '../cell_value.js';
import type { RowData } from '../cell_value.js';
import { SeqId } from '../seq_id.js';
import type {
  LiveRowsOptions,
  ServerMessage,
  SubscriptionInfo,
  SubscriptionOptions,
} from '../types.js';

export function normalizeSubscriptionOptions(
  options?: SubscriptionOptions,
): { batch_size?: number; last_rows?: number; from?: string } | undefined {
  if (!options) {
    return undefined;
  }

  const normalized: { batch_size?: number; last_rows?: number; from?: string } = {};

  if (options.batch_size !== undefined) {
    normalized.batch_size = options.batch_size;
  }

  if (options.last_rows !== undefined) {
    normalized.last_rows = options.last_rows;
  }

  if (options.from !== undefined) {
    normalized.from = options.from instanceof SeqId ? options.from.toString() : SeqId.from(options.from).toString();
  }

  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

export type NormalizedSubscriptionEvent = ServerMessage & {
  rows?: RowData[];
  old_values?: RowData[];
};

export type LiveRowsWasmEvent = {
  type: 'rows' | 'error';
  subscription_id: string;
  rows?: RowData[];
  code?: string;
  message?: string;
};

export interface LocalSubscriptionMetadata {
  tableName: string;
  createdAtMs: number;
}

interface WasmSubscriptionSnapshot {
  id: string;
  query?: string;
  lastSeqId?: string;
  closed?: boolean;
}

interface ParsedWasmSubscriptions {
  parsed: boolean;
  subscriptions: WasmSubscriptionSnapshot[];
}

export function trackSubscriptionMetadata(
  metadata: Map<string, LocalSubscriptionMetadata>,
  subscriptionId: string,
  tableName: string,
): void {
  metadata.set(subscriptionId, {
    tableName,
    createdAtMs: Date.now(),
  });
}

export function normalizeSubscriptionEvent(event: ServerMessage): NormalizedSubscriptionEvent {
  const normalized = { ...event } as NormalizedSubscriptionEvent;

  if ('rows' in normalized) {
    normalized.rows = wrapSubscriptionRows(normalized.rows);
  }
  if ('old_values' in normalized) {
    normalized.old_values = wrapSubscriptionRows(normalized.old_values);
  }

  return normalized;
}

export function normalizeLiveRowsWasmEvent(event: {
  type: 'rows' | 'error';
  subscription_id: string;
  rows?: unknown;
  code?: string;
  message?: string;
}): LiveRowsWasmEvent {
  return {
    ...event,
    rows: wrapSubscriptionRows(event.rows),
  };
}

export function normalizeLiveRowsKeyColumns<T>(
  options: LiveRowsOptions<T>,
): string[] | undefined {
  const declaredColumns = options.keyColumns;
  if (declaredColumns === undefined) {
    return undefined;
  }

  const columns = Array.isArray(declaredColumns) ? declaredColumns : [declaredColumns];
  const normalized = columns
    .map((column) => column.trim())
    .filter((column, index, values) => column.length > 0 && values.indexOf(column) === index);

  return normalized.length > 0 ? normalized : undefined;
}

export function normalizeLiveRowsOptions<T>(
  options: LiveRowsOptions<T>,
): {
  key_columns?: string[];
  subscription_options?: { batch_size?: number; last_rows?: number; from?: string };
} | undefined {
  const keyColumns = normalizeLiveRowsKeyColumns(options);
  const subscriptionOptions = normalizeSubscriptionOptions(options.subscriptionOptions);
  const normalized: {
    key_columns?: string[];
    subscription_options?: { batch_size?: number; last_rows?: number; from?: string };
  } = {};

  if (keyColumns) {
    normalized.key_columns = keyColumns;
  }

  if (subscriptionOptions) {
    normalized.subscription_options = subscriptionOptions;
  }

  return Object.keys(normalized).length > 0 ? normalized : undefined;
}

export function defaultRowKey(value: unknown): string | null {
  if (!value || typeof value !== 'object') {
    return null;
  }

  const candidate = (value as { id?: unknown }).id;
  if (typeof candidate === 'string') {
    return candidate;
  }
  if (candidate instanceof KalamCellValue) {
    return candidate.asString();
  }

  return null;
}

export function upsertLimited<T>(
  current: T[],
  incoming: T[],
  getKey: (row: T) => string | null | undefined,
  limit?: number,
): T[] {
  const next = [...current];

  for (const item of incoming) {
    const key = getKey(item);
    if (key) {
      const existingIndex = next.findIndex((entry) => getKey(entry) === key);
      if (existingIndex >= 0) {
        next[existingIndex] = item;
        continue;
      }
    }

    next.push(item);
  }

  if (typeof limit === 'number' && limit >= 0 && next.length > limit) {
    return next.slice(-limit);
  }

  return next;
}

export function removeMaterializedRows<T>(
  current: T[],
  removed: T[],
  getKey: (row: T) => string | null | undefined,
): T[] {
  const keys = new Set(
    removed
      .map((row) => getKey(row))
      .filter((key): key is string => typeof key === 'string' && key.length > 0),
  );

  if (keys.size === 0) {
    return current;
  }

  return current.filter((row) => {
    const key = getKey(row);
    return !key || !keys.has(key);
  });
}

export function readSubscriptionInfos(
  raw: unknown,
  metadata: ReadonlyMap<string, LocalSubscriptionMetadata>,
): SubscriptionInfo[] {
  const parsed = parseWasmSubscriptions(raw);
  if (!parsed.parsed) {
    return localSubscriptionInfos(metadata);
  }

  return parsed.subscriptions.map((subscription) => {
    const local = metadata.get(subscription.id);
    return {
      id: subscription.id,
      tableName: subscription.query ?? local?.tableName ?? '',
      createdAt: new Date(local?.createdAtMs ?? 0),
      lastSeqId: parseSeqId(subscription.lastSeqId),
      closed: subscription.closed ?? false,
    };
  });
}

function parseWasmSubscriptions(raw: unknown): ParsedWasmSubscriptions {
  if (typeof raw !== 'string') {
    return {
      parsed: false,
      subscriptions: [],
    };
  }

  try {
    const parsed = JSON.parse(raw) as WasmSubscriptionSnapshot[];
    if (!Array.isArray(parsed)) {
      return {
        parsed: false,
        subscriptions: [],
      };
    }

    return {
      parsed: true,
      subscriptions: parsed,
    };
  } catch {
    return {
      parsed: false,
      subscriptions: [],
    };
  }
}

function localSubscriptionInfos(
  metadata: ReadonlyMap<string, LocalSubscriptionMetadata>,
): SubscriptionInfo[] {
  return Array.from(metadata.entries()).map(([id, local]) => ({
    id,
    tableName: local.tableName,
    createdAt: new Date(local.createdAtMs),
    lastSeqId: undefined,
    closed: false,
  }));
}

function parseSeqId(raw?: string): SeqId | undefined {
  if (!raw) {
    return undefined;
  }

  try {
    return SeqId.from(raw);
  } catch {
    return undefined;
  }
}

function wrapSubscriptionRows(rows: unknown): RowData[] | undefined {
  if (!Array.isArray(rows)) {
    return undefined;
  }

  return rows.map((row) => wrapRowMap((row ?? {}) as Record<string, unknown>));
}
