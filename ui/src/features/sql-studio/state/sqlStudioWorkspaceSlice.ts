import { createSlice, type PayloadAction } from "@reduxjs/toolkit";
import type {
  QueryLogEntry,
  QueryResultData,
  QueryResultSchemaField,
  QueryRunSummary,
  QueryTab,
  SavedQuery,
} from "@/components/sql-studio-v2/types";

interface SqlStudioWorkspaceState {
  tabs: QueryTab[];
  savedQueries: SavedQuery[];
  activeTabId: string | null;
  isRunning: boolean;
  tabResults: Record<string, QueryResultData | null>;
  history: QueryRunSummary[];
}

const initialState: SqlStudioWorkspaceState = {
  tabs: [],
  savedQueries: [],
  activeTabId: null,
  isRunning: false,
  tabResults: {},
  history: [],
};

/**
 * Live change metadata stored on each row for visual indicators.
 *
 * All metadata fields are prefixed with `_live_` so they're hidden from the schema.
 * The grid displays these as visual indicators instead of columns.
 */
const LIVE_META = {
  /** Change type: "insert" | "update" | "delete" | "initial" */
  CHANGE_TYPE: "_live_change_type",
  /** ISO timestamp when the change was received */
  CHANGED_AT: "_live_changed_at",
  /** Comma-separated list of columns that changed (for update highlighting) */
  CHANGED_COLS: "_live_changed_cols",
} as const;

/** How long (ms) a change highlight persists before fading. */
export const LIVE_HIGHLIGHT_DURATION_MS = 5_000;

/**
 * Find the primary key column names from the schema.
 * Falls back to common PK column names if none are explicitly flagged.
 */
function findPkColumns(schema: QueryResultSchemaField[]): string[] {
  const explicit = schema
    .filter((f) => f.isPrimaryKey)
    .map((f) => f.name);
  if (explicit.length > 0) return explicit;

  // Heuristic: look for common PK column names
  const common = ["id", "ID", "_id", "pk"];
  for (const name of common) {
    if (schema.some((f) => f.name === name)) return [name];
  }
  return [];
}

/**
 * Build a composite PK string for a row so we can look it up in O(1).
 */
function rowPk(row: Record<string, unknown>, pkCols: string[]): string | null {
  if (pkCols.length === 0) return null;
  return pkCols.map((c) => String(row[c] ?? "")).join("\x00");
}

/**
 * Smart live-row merge with PK-based dedup.
 *
 * Instead of blindly appending every incoming row, this function:
 *  - INSERT: appends the row (or updates if PK already exists – server may resend)
 *  - UPDATE: finds the existing row by PK and updates it in-place
 *  - DELETE: finds the existing row by PK and marks it as deleted
 *  - initial: bulk-appends (initial snapshot load)
 *
 * Each row carries `_live_change_type`, `_live_changed_at`, and
 * `_live_changed_cols` metadata that the grid uses for visual indicators.
 */
function mergeLiveRowsIntoResult(
  previous: QueryResultData | null,
  incomingRows: Record<string, unknown>[],
  changeType: string,
  incomingSchema?: QueryResultSchemaField[],
): QueryResultData {
  const now = new Date().toISOString();
  const existingRows = previous?.rows ?? [];

  // ── Resolve schema ──────────────────────────────────────────────
  const orderedIncoming = incomingSchema
    ? [...incomingSchema].sort((a, b) => a.index - b.index)
    : undefined;
  const baseSchema =
    orderedIncoming && orderedIncoming.length > 0
      ? orderedIncoming
      : (previous?.schema?.length ?? 0) > 0
        ? previous!.schema
        : buildFallbackSchema([...existingRows, ...incomingRows]);

  const schema = ensureLiveMetaCols(baseSchema);
  const pkCols = findPkColumns(schema);

  // ── Build PK → index map for O(1) lookups ──────────────────────
  const pkIndex = new Map<string, number>();
  if (pkCols.length > 0) {
    existingRows.forEach((row, idx) => {
      const key = rowPk(row, pkCols);
      if (key !== null) pkIndex.set(key, idx);
    });
  }

  // Clone rows so Redux Toolkit's Immer proxy doesn't cause issues
  let rows = existingRows.map((r) => ({ ...r }));

  for (const incoming of incomingRows) {
    const key = rowPk(incoming, pkCols);
    const existingIdx = key !== null ? pkIndex.get(key) : undefined;

    if (changeType === "delete") {
      if (existingIdx !== undefined) {
        // Mark existing row as deleted (preserve data for display)
        rows[existingIdx] = {
          ...rows[existingIdx],
          [LIVE_META.CHANGE_TYPE]: "delete",
          [LIVE_META.CHANGED_AT]: now,
          [LIVE_META.CHANGED_COLS]: "",
        };
      } else {
        // Row not in table yet — add it as deleted
        rows.push({
          ...incoming,
          [LIVE_META.CHANGE_TYPE]: "delete",
          [LIVE_META.CHANGED_AT]: now,
          [LIVE_META.CHANGED_COLS]: "",
        });
      }
    } else if (changeType === "update") {
      if (existingIdx !== undefined) {
        // Derive changed user columns from the incoming delta row keys
        // (the server sends only changed columns + PK + _seq in delta rows)
        const prev = rows[existingIdx];
        const changedCols: string[] = Object.keys(incoming).filter(
          (k) => !k.startsWith("_"),
        );
        rows[existingIdx] = {
          ...prev,
          ...incoming,
          [LIVE_META.CHANGE_TYPE]: "update",
          [LIVE_META.CHANGED_AT]: now,
          [LIVE_META.CHANGED_COLS]: changedCols.join(","),
        };
      } else {
        // PK not found — treat as insert
        rows.push({
          ...incoming,
          [LIVE_META.CHANGE_TYPE]: "update",
          [LIVE_META.CHANGED_AT]: now,
          [LIVE_META.CHANGED_COLS]: "",
        });
      }
    } else if (changeType === "insert") {
      if (existingIdx !== undefined) {
        // PK collision on insert — overwrite (server resend / reconnect)
        rows[existingIdx] = {
          ...incoming,
          [LIVE_META.CHANGE_TYPE]: "insert",
          [LIVE_META.CHANGED_AT]: now,
          [LIVE_META.CHANGED_COLS]: "",
        };
      } else {
        rows.push({
          ...incoming,
          [LIVE_META.CHANGE_TYPE]: "insert",
          [LIVE_META.CHANGED_AT]: now,
          [LIVE_META.CHANGED_COLS]: "",
        });
        if (key !== null) pkIndex.set(key, rows.length - 1);
      }
    } else {
      // "initial" or other — just append, no highlight
      rows.push({
        ...incoming,
        [LIVE_META.CHANGE_TYPE]: changeType,
        [LIVE_META.CHANGED_AT]: "",
        [LIVE_META.CHANGED_COLS]: "",
      });
      if (key !== null) pkIndex.set(key, rows.length - 1);
    }
  }

  return {
    status: "success",
    rows,
    schema,
    tookMs: previous?.tookMs ?? 0,
    rowCount: rows.length,
    logs: previous?.logs ?? [],
  };
}

/** Build a simple schema from row keys when no schema is available. */
function buildFallbackSchema(
  rows: Record<string, unknown>[],
): QueryResultSchemaField[] {
  const keys = rows.length > 0 ? Object.keys(rows[0]) : [];
  return keys
    .filter((k) => !k.startsWith("_live_"))
    .map((name, index) => ({
      name,
      dataType: "text",
      index,
      isPrimaryKey: false,
    }));
}

/** Ensure the schema includes the live-meta columns (hidden from display by the grid). */
function ensureLiveMetaCols(
  schema: QueryResultSchemaField[],
): QueryResultSchemaField[] {
  const metaCols = [LIVE_META.CHANGE_TYPE, LIVE_META.CHANGED_AT, LIVE_META.CHANGED_COLS];
  const missing = metaCols.filter((c) => !schema.some((f) => f.name === c));
  if (missing.length === 0) return schema;

  // Append meta columns at the end (they'll be hidden by the grid)
  const extra: QueryResultSchemaField[] = missing.map((name) => ({
    name,
    dataType: "text",
    index: -1,
    isPrimaryKey: false,
  }));

  return [...schema, ...extra].map((f, i) => ({ ...f, index: i }));
}

function appendLogToResult(
  previous: QueryResultData | null,
  entry: QueryLogEntry,
  statusOverride?: QueryResultData["status"],
): QueryResultData {
  const base: QueryResultData = previous ?? {
    status: "success",
    rows: [],
    schema: [],
    tookMs: 0,
    rowCount: 0,
    logs: [],
  };

  return {
    ...base,
    status: statusOverride ?? base.status,
    logs: [...base.logs, entry],
  };
}

function applyLiveSchemaToResult(
  previous: QueryResultData | null,
  schema: QueryResultSchemaField[],
): QueryResultData {
  const base: QueryResultData = previous ?? {
    status: "success",
    rows: [],
    schema: [],
    tookMs: 0,
    rowCount: 0,
    logs: [],
  };

  if (schema.length === 0) {
    return base;
  }

  const normalizedSchema = [...schema]
    .sort((left, right) => left.index - right.index)
    .map((field, index) => ({
      ...field,
      index,
      isPrimaryKey: field.isPrimaryKey ?? false,
    }));

  return {
    ...base,
    status: base.status === "error" ? "success" : base.status,
    schema: normalizedSchema,
  };
}

const sqlStudioWorkspaceSlice = createSlice({
  name: "sqlStudioWorkspace",
  initialState,
  reducers: {
    hydrateSqlStudioWorkspace(state, action: PayloadAction<{
      tabs: QueryTab[];
      savedQueries: SavedQuery[];
      activeTabId: string;
    }>) {
      state.tabs = action.payload.tabs;
      state.savedQueries = action.payload.savedQueries;
      state.activeTabId = action.payload.activeTabId;
    },
    setWorkspaceTabs(state, action: PayloadAction<QueryTab[]>) {
      state.tabs = action.payload;
    },
    addWorkspaceTab(state, action: PayloadAction<QueryTab>) {
      state.tabs.push(action.payload);
    },
    updateWorkspaceTab(
      state,
      action: PayloadAction<{ tabId: string; updates: Partial<QueryTab> }>,
    ) {
      const tab = state.tabs.find((item) => item.id === action.payload.tabId);
      if (!tab) {
        return;
      }
      Object.assign(tab, action.payload.updates);
    },
    closeWorkspaceTab(state, action: PayloadAction<string>) {
      if (state.tabs.length <= 1) {
        return;
      }

      const tabId = action.payload;
      const index = state.tabs.findIndex((item) => item.id === tabId);
      if (index < 0) {
        return;
      }

      const nextTabs = state.tabs.filter((item) => item.id !== tabId);
      state.tabs = nextTabs;
      delete state.tabResults[tabId];

      if (state.activeTabId === tabId) {
        const fallback = nextTabs[Math.max(0, index - 1)] ?? nextTabs[0] ?? null;
        state.activeTabId = fallback?.id ?? null;
      }
    },
    setWorkspaceActiveTabId(state, action: PayloadAction<string>) {
      state.activeTabId = action.payload;
    },
    setWorkspaceSavedQueries(state, action: PayloadAction<SavedQuery[]>) {
      state.savedQueries = action.payload;
    },
    setWorkspaceRunning(state, action: PayloadAction<boolean>) {
      state.isRunning = action.payload;
    },
    setWorkspaceTabResult(
      state,
      action: PayloadAction<{ tabId: string; result: QueryResultData | null }>,
    ) {
      state.tabResults[action.payload.tabId] = action.payload.result;
    },
    appendWorkspaceLiveRows(
      state,
      action: PayloadAction<{
        tabId: string;
        rows: Record<string, unknown>[];
        changeType: string;
        schema?: QueryResultSchemaField[];
      }>,
    ) {
      const {
        tabId,
        rows,
        changeType,
        schema,
      } = action.payload;
      state.tabResults[tabId] = mergeLiveRowsIntoResult(
        state.tabResults[tabId] ?? null,
        rows,
        changeType,
        schema,
      );
    },
    appendWorkspaceResultLog(
      state,
      action: PayloadAction<{
        tabId: string;
        entry: QueryLogEntry;
        statusOverride?: QueryResultData["status"];
      }>,
    ) {
      const { tabId, entry, statusOverride } = action.payload;
      state.tabResults[tabId] = appendLogToResult(state.tabResults[tabId] ?? null, entry, statusOverride);
    },
    setWorkspaceLiveSchema(
      state,
      action: PayloadAction<{ tabId: string; schema: QueryResultSchemaField[] }>,
    ) {
      const { tabId, schema } = action.payload;
      state.tabResults[tabId] = applyLiveSchemaToResult(state.tabResults[tabId] ?? null, schema);
    },
    prependWorkspaceHistory(state, action: PayloadAction<QueryRunSummary>) {
      state.history = [action.payload, ...state.history].slice(0, 50);
    },
  },
});

export const {
  hydrateSqlStudioWorkspace,
  setWorkspaceTabs,
  addWorkspaceTab,
  updateWorkspaceTab,
  closeWorkspaceTab,
  setWorkspaceActiveTabId,
  setWorkspaceSavedQueries,
  setWorkspaceRunning,
  setWorkspaceTabResult,
  appendWorkspaceLiveRows,
  appendWorkspaceResultLog,
  setWorkspaceLiveSchema,
  prependWorkspaceHistory,
} = sqlStudioWorkspaceSlice.actions;

export { LIVE_META };

export default sqlStudioWorkspaceSlice.reducer;
