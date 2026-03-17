export interface LiveSubscriptionOptions {
  batch_size?: number;
  last_rows?: number;
  from?: string;
}

export interface StudioColumn {
  name: string;
  dataType: string;
  isNullable: boolean;
  isPrimaryKey: boolean;
  ordinal: number;
}

export interface StudioTable {
  database: string;
  namespace: string;
  name: string;
  tableType: string;
  columns: StudioColumn[];
}

export interface StudioNamespace {
  database: string;
  name: string;
  tables: StudioTable[];
}

export interface QueryTab {
  id: string;
  title: string;
  sql: string;
  isDirty: boolean;
  isLive: boolean;
  liveStatus: "idle" | "connecting" | "connected" | "error";
  resultView: SqlStudioResultView;
  lastSavedAt: string | null;
  savedQueryId: string | null;
  subscriptionOptions?: LiveSubscriptionOptions;
}

export type SqlStudioResultView = "results" | "log";

export interface SavedQuery {
  id: string;
  title: string;
  sql: string;
  lastSavedAt: string;
  isLive: boolean;
  subscriptionOptions?: LiveSubscriptionOptions;
}

export interface QueryRunSummary {
  id: string;
  tabTitle: string;
  sql: string;
  status: "success" | "error";
  executedAt: string;
  durationMs: number;
  rowCount: number;
  errorMessage?: string;
}

export interface QueryResultSchemaField {
  name: string;
  dataType: string;
  index: number;
  flags?: string[];
  isPrimaryKey?: boolean;
}

export interface QueryLogEntry {
  id: string;
  level: "info" | "error";
  message: string;
  response?: unknown;
  asUser?: string;
  rowCount?: number;
  statementIndex?: number;
  createdAt: string;
}

export interface QueryResultData {
  status: "success" | "error";
  rows: Record<string, unknown>[];
  schema: QueryResultSchemaField[];
  tookMs: number;
  rowCount: number;
  logs: QueryLogEntry[];
  message?: string;
  errorMessage?: string;
}

export type SqlStudioPanelLayout = [number, number];
