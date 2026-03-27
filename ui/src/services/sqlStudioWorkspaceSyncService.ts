import type { QueryTab, SavedQuery } from "@/components/sql-studio-v2/types";
import type {
  SqlStudioPersistedQueryTab,
  SqlStudioPersistedSavedQuery,
} from "@/components/sql-studio-v2/workspaceState";
import { subscribeRows, executeSql, type Unsubscribe } from "@/lib/kalam-client";
import { toPersistedTab } from "@/features/sql-studio/utils/workspaceHelpers";

const SQL_STUDIO_WORKSPACE_TABLE = "dba.favorites";
const SQL_STUDIO_WORKSPACE_ROW_ID = "sql-studio-workspace";

export interface SqlStudioSyncedSavedQuery extends SqlStudioPersistedSavedQuery {
  openedRecently: boolean;
  isCurrentTab: boolean;
}

export interface SqlStudioSyncedWorkspaceState {
  version: 1;
  tabs: SqlStudioPersistedQueryTab[];
  savedQueries: SqlStudioSyncedSavedQuery[];
  activeTabId: string;
  updatedAt: string;
}

function normalizeSubscriptionOptions(value: unknown) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }

  const record = value as Record<string, unknown>;
  const result: Record<string, string | number> = {};

  if (typeof record.batch_size === "number" && Number.isFinite(record.batch_size)) {
    result.batch_size = record.batch_size;
  }
  if (typeof record.last_rows === "number" && Number.isFinite(record.last_rows)) {
    result.last_rows = record.last_rows;
  }
  if (typeof record.from === "string" || typeof record.from === "number") {
    result.from = String(record.from);
  }

  return Object.keys(result).length > 0 ? result : undefined;
}

function normalizeTabs(value: unknown): SqlStudioPersistedQueryTab[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((item): SqlStudioPersistedQueryTab | null => {
      if (!item || typeof item !== "object") {
        return null;
      }

      const record = item as Partial<SqlStudioPersistedQueryTab>;
      const id = typeof record.id === "string" ? record.id : "";
      const name = typeof record.name === "string" ? record.name : "";
      const query = typeof record.query === "string" ? record.query : "";

      if (!id || !name) {
        return null;
      }

      return {
        id,
        name,
        query,
        settings: {
          isDirty: Boolean(record.settings?.isDirty),
          isLive: Boolean(record.settings?.isLive),
          liveStatus:
            record.settings?.liveStatus === "connecting"
            || record.settings?.liveStatus === "error"
            || record.settings?.liveStatus === "connected"
              ? record.settings.liveStatus
              : "idle",
          resultView: record.settings?.resultView === "log" ? "log" : "results",
          lastSavedAt: typeof record.settings?.lastSavedAt === "string"
            ? record.settings.lastSavedAt
            : null,
          savedQueryId: typeof record.settings?.savedQueryId === "string"
            ? record.settings.savedQueryId
            : null,
          subscriptionOptions: normalizeSubscriptionOptions(record.settings?.subscriptionOptions),
        },
      };
    })
    .filter((item): item is SqlStudioPersistedQueryTab => item !== null);
}

function normalizeSavedQueries(value: unknown): SqlStudioSyncedSavedQuery[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((item): SqlStudioSyncedSavedQuery | null => {
      if (!item || typeof item !== "object") {
        return null;
      }

      const record = item as Partial<SqlStudioSyncedSavedQuery>;
      const id = typeof record.id === "string" ? record.id : "";
      const title = typeof record.title === "string" ? record.title : "";
      const sql = typeof record.sql === "string" ? record.sql : "";
      const lastSavedAt = typeof record.lastSavedAt === "string" ? record.lastSavedAt : "";

      if (!id || !title || !lastSavedAt) {
        return null;
      }

      return {
        id,
        title,
        sql,
        lastSavedAt,
        isLive: Boolean(record.isLive),
        subscriptionOptions: normalizeSubscriptionOptions(record.subscriptionOptions),
        openedRecently: Boolean(record.openedRecently),
        isCurrentTab: Boolean(record.isCurrentTab),
      };
    })
    .filter((item): item is SqlStudioSyncedSavedQuery => item !== null);
}

function normalizeWorkspacePayload(value: unknown): SqlStudioSyncedWorkspaceState | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Partial<SqlStudioSyncedWorkspaceState>;
  const tabs = normalizeTabs(record.tabs);
  if (tabs.length === 0) {
    return null;
  }

  const activeTabId = tabs.some((tab) => tab.id === record.activeTabId)
    ? String(record.activeTabId)
    : tabs[0].id;

  return {
    version: 1,
    tabs,
    savedQueries: normalizeSavedQueries(record.savedQueries),
    activeTabId,
    updatedAt: typeof record.updatedAt === "string" ? record.updatedAt : new Date().toISOString(),
  };
}

function buildWorkspaceSelectSql(): string {
  return `SELECT id, payload FROM ${SQL_STUDIO_WORKSPACE_TABLE} WHERE id = '${SQL_STUDIO_WORKSPACE_ROW_ID}' LIMIT 1`;
}

async function ensureWorkspaceTable(): Promise<void> {
  await executeSql(
    `CREATE TABLE IF NOT EXISTS ${SQL_STUDIO_WORKSPACE_TABLE} (id TEXT PRIMARY KEY, payload JSON) WITH (TYPE='USER')`,
  );
}

async function workspaceRowExists(): Promise<boolean> {
  const rows = await executeSql(buildWorkspaceSelectSql());
  return rows.length > 0;
}

function parseWorkspaceRow(row: Record<string, unknown> | undefined): SqlStudioSyncedWorkspaceState | null {
  if (!row) {
    return null;
  }

  // JSON column: server returns an already-parsed object; fall back to string parsing for compatibility.
  const payload = typeof row.payload === "object" && row.payload !== null
    ? row.payload
    : (() => {
      try {
        return typeof row.payload === "string" ? JSON.parse(row.payload) as unknown : null;
      } catch {
        return null;
      }
    })();

  if (!payload) {
    return null;
  }

  return normalizeWorkspacePayload(payload);
}

export function buildSyncedSqlStudioWorkspaceState(
  tabs: QueryTab[],
  savedQueries: SavedQuery[],
  activeTabId: string | null,
): SqlStudioSyncedWorkspaceState {
  const safeActiveTabId = tabs.find((tab) => tab.id === activeTabId)?.id ?? tabs[0]?.id ?? "";
  const savedQueryMeta = new Map<string, { openedRecently: boolean; isCurrentTab: boolean }>();

  tabs.forEach((tab) => {
    if (!tab.savedQueryId) {
      return;
    }

    const previous = savedQueryMeta.get(tab.savedQueryId) ?? {
      openedRecently: false,
      isCurrentTab: false,
    };

    savedQueryMeta.set(tab.savedQueryId, {
      openedRecently: true,
      isCurrentTab: previous.isCurrentTab || tab.id === safeActiveTabId,
    });
  });

  return {
    version: 1,
    tabs: tabs.map(toPersistedTab),
    savedQueries: savedQueries.map((item) => {
      const meta = savedQueryMeta.get(item.id);
      return {
        id: item.id,
        title: item.title,
        sql: item.sql,
        lastSavedAt: item.lastSavedAt,
        isLive: item.isLive,
        subscriptionOptions: item.subscriptionOptions,
        openedRecently: meta?.openedRecently ?? false,
        isCurrentTab: meta?.isCurrentTab ?? false,
      };
    }),
    activeTabId: safeActiveTabId,
    updatedAt: new Date().toISOString(),
  };
}

export async function loadSyncedSqlStudioWorkspaceState(): Promise<SqlStudioSyncedWorkspaceState | null> {
  try {
    const rows = await executeSql(buildWorkspaceSelectSql());
    return parseWorkspaceRow(rows[0]);
  } catch (error) {
    console.warn("Failed to load synced SQL Studio workspace", error);
    return null;
  }
}

export async function saveSyncedSqlStudioWorkspaceState(
  state: SqlStudioSyncedWorkspaceState,
): Promise<void> {
  const payload = JSON.stringify(state);

  await ensureWorkspaceTable();

  if (await workspaceRowExists()) {
    await executeSql(
      `UPDATE ${SQL_STUDIO_WORKSPACE_TABLE} SET payload = '${payload}' WHERE id = '${SQL_STUDIO_WORKSPACE_ROW_ID}'`,
    );
    return;
  }

  await executeSql(
    `INSERT INTO ${SQL_STUDIO_WORKSPACE_TABLE} (id, payload) VALUES ('${SQL_STUDIO_WORKSPACE_ROW_ID}', '${payload}')`,
  );
}

export async function subscribeToSyncedSqlStudioWorkspaceState(
  onChange: (state: SqlStudioSyncedWorkspaceState | null) => void,
): Promise<Unsubscribe> {
  return subscribeRows<Record<string, unknown>>(
    buildWorkspaceSelectSql(),
    (rows) => {
      onChange(parseWorkspaceRow(rows[0]));
    },
  );
}