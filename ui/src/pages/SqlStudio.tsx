import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { PanelRightClose, PanelRightOpen } from "lucide-react";
import { QueryTabStrip } from "@/components/sql-studio-v2/QueryTabStrip";
import { StudioEditorPanel } from "@/components/sql-studio-v2/StudioEditorPanel";
import { StudioExplorerPanel } from "@/components/sql-studio-v2/StudioExplorerPanel";
import { StudioInspectorPanel } from "@/components/sql-studio-v2/StudioInspectorPanel";
import { StudioResultsGrid } from "@/components/sql-studio-v2/StudioResultsGrid";
import { Button } from "@/components/ui/button";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";
import { useAppDispatch, useAppSelector } from "@/store/hooks";
import { useAuth } from "@/lib/auth";
import {
  subscribe,
  setClientDisconnectListener,
  setClientErrorListener,
  setClientLogListener,
  setClientReceiveListener,
  setClientSendListener,
  type Unsubscribe,
} from "@/lib/kalam-client";
import type { ServerMessage, ChangeTypeRaw, SchemaField } from "kalam-link";
import type {
  QueryRunSummary,
  QueryTab,
  StudioTable,
} from "@/components/sql-studio-v2/types";
import {
  loadSqlStudioWorkspaceState,
  saveSqlStudioWorkspaceState,
  type SqlStudioWorkspaceState,
} from "@/components/sql-studio-v2/workspaceState";
import {
  executeSqlStudioQuery,
  normalizeSchema,
} from "@/services/sqlStudioService";
import {
  addWorkspaceTab,
  appendWorkspaceLiveRows,
  appendWorkspaceResultLog,
  closeWorkspaceTab,
  hydrateSqlStudioWorkspace,
  prependWorkspaceHistory,
  setWorkspaceLiveSchema,
  setWorkspaceActiveTabId,
  setWorkspaceRunning,
  setWorkspaceSavedQueries,
  setWorkspaceTabResult,
  setWorkspaceTabs,
  updateWorkspaceTab,
} from "@/features/sql-studio/state/sqlStudioWorkspaceSlice";
import {
  hydrateSqlStudioUi,
  setHorizontalLayout,
  setInspectorCollapsed,
  setNamespaceExpanded,
  setSchemaFilter,
  setSelectedTableKey,
  setTableExpanded,
  setVerticalLayout,
  toggleFavoritesExpanded,
  toggleNamespaceSectionExpanded,
  toggleNamespaceExpanded,
  toggleTableExpanded,
} from "@/features/sql-studio/state/sqlStudioUiSlice";
import {
  selectExpandedNamespaces,
  selectExpandedTables,
  selectFavoritesExpanded,
  selectHorizontalLayout,
  selectIsInspectorCollapsed,
  selectNamespaceSectionExpanded,
  selectSchemaFilter,
  selectSelectedTableKey,
  selectVerticalLayout,
  selectWorkspaceActiveTabId,
  selectWorkspaceHistory,
  selectWorkspaceIsRunning,
  selectWorkspaceSavedQueries,
  selectWorkspaceTabResults,
  selectWorkspaceTabs,
} from "@/features/sql-studio/state/selectors";
import { useGetSqlStudioSchemaTreeQuery } from "@/store/apiSlice";
import {
  buildSelectFromTableSql,
  createLogEntry,
  createQueryTab,
  createSavedQueryId,
  createTabId,
  resolveResultView,
  toPersistedTab,
} from "@/features/sql-studio/utils/workspaceHelpers";
import { ExplorerTableContextMenu } from "@/features/sql-studio/components/ExplorerTableContextMenu";

const WORKSPACE_PERSIST_DEBOUNCE_MS = 250;

function normalizeLiveRows(rows: unknown[]): Record<string, unknown>[] {
  return rows.map((row) => {
    if (row instanceof Map) {
      return Object.fromEntries(row.entries()) as Record<string, unknown>;
    }
    if (typeof row === "object" && row !== null) {
      return row as Record<string, unknown>;
    }
    return { value: row };
  });
}

function parseWirePayload(message: string): unknown {
  try {
    return JSON.parse(message) as unknown;
  } catch {
    return undefined;
  }
}

function createWireLogEntry(
  direction: "send" | "receive",
  rawMessage: string,
  asUser?: string,
) {
  const parsed = parseWirePayload(rawMessage);
  const parsedRecord =
    parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed as Record<string, unknown>
      : undefined;
  const messageType =
    typeof parsedRecord?.type === "string"
      ? parsedRecord.type
      : "message";

  return createLogEntry(
    `WS ${direction.toUpperCase()} · ${messageType}`,
    "info",
    asUser,
    {
      raw: rawMessage,
      parsed,
    },
  );
}

export default function SqlStudio() {
  const location = useLocation();
  const navigate = useNavigate();
  const initialWorkspace = useMemo(() => {
    const fallbackTab = createQueryTab(1);
    return loadSqlStudioWorkspaceState(toPersistedTab(fallbackTab));
  }, []);

  const dispatch = useAppDispatch();
  const { user } = useAuth();
  const tabs = useAppSelector(selectWorkspaceTabs);
  const savedQueries = useAppSelector(selectWorkspaceSavedQueries);
  const activeTabId = useAppSelector(selectWorkspaceActiveTabId);
  const { data: schema = [] } = useGetSqlStudioSchemaTreeQuery();
  const schemaFilter = useAppSelector(selectSchemaFilter);
  const favoritesExpanded = useAppSelector(selectFavoritesExpanded);
  const namespaceSectionExpanded = useAppSelector(selectNamespaceSectionExpanded);
  const expandedNamespaces = useAppSelector(selectExpandedNamespaces);
  const expandedTables = useAppSelector(selectExpandedTables);
  const selectedTableKey = useAppSelector(selectSelectedTableKey);
  const isRunning = useAppSelector(selectWorkspaceIsRunning);
  const tabResults = useAppSelector(selectWorkspaceTabResults);
  const history = useAppSelector(selectWorkspaceHistory);
  const isInspectorCollapsed = useAppSelector(selectIsInspectorCollapsed);
  const horizontalLayout = useAppSelector(selectHorizontalLayout);
  const verticalLayout = useAppSelector(selectVerticalLayout);
  const [explorerContextMenu, setExplorerContextMenu] = useState<{
    x: number;
    y: number;
    table: StudioTable;
  } | null>(null);
  const [isUiHydrated, setIsUiHydrated] = useState(false);
  const liveUnsubscribeRef = useRef<Record<string, Unsubscribe>>({});
  const consumedPrefillKeyRef = useRef<string | null>(null);

  const activeTab = useMemo(
    () => {
      if (tabs.length === 0) {
        return null;
      }
      return tabs.find((item) => item.id === activeTabId) ?? tabs[0];
    },
    [tabs, activeTabId],
  );
  const activeResult = activeTab ? (tabResults[activeTab.id] ?? null) : null;
  const selectedTable = useMemo(() => {
    if (!selectedTableKey) {
      return null;
    }
    const [namespaceName, tableName] = selectedTableKey.split(".");
    const namespace = schema.find((item) => item.name === namespaceName);
    return namespace?.tables.find((item) => item.name === tableName) ?? null;
  }, [schema, selectedTableKey]);

  useEffect(() => {
    const workspaceTabs = initialWorkspace.tabs.map((tab) => ({
      id: tab.id,
      title: tab.name,
      sql: tab.query,
      isDirty: tab.settings.isDirty,
      isLive: tab.settings.isLive,
      liveStatus: tab.settings.liveStatus,
      resultView: tab.settings.resultView,
      lastSavedAt: tab.settings.lastSavedAt,
      savedQueryId: tab.settings.savedQueryId,
      subscriptionOptions: tab.settings.subscriptionOptions,
    }));
    const workspaceSavedQueries = initialWorkspace.savedQueries.map((item) => ({
      id: item.id,
      title: item.title,
      sql: item.sql,
      lastSavedAt: item.lastSavedAt,
      isLive: item.isLive,
      subscriptionOptions: item.subscriptionOptions,
    }));

    dispatch(hydrateSqlStudioWorkspace({
      tabs: workspaceTabs,
      savedQueries: workspaceSavedQueries,
      activeTabId: initialWorkspace.activeTabId,
    }));

    dispatch(hydrateSqlStudioUi({
      schemaFilter: initialWorkspace.explorerTree.filter,
      favoritesExpanded: initialWorkspace.explorerTree.favoritesExpanded,
      namespaceSectionExpanded: initialWorkspace.explorerTree.namespaceSectionExpanded,
      expandedNamespaces: initialWorkspace.explorerTree.expandedNamespaces,
      expandedTables: initialWorkspace.explorerTree.expandedTables,
      selectedTableKey: initialWorkspace.selectedTableKey,
      isInspectorCollapsed: initialWorkspace.inspectorCollapsed,
      horizontalLayout: initialWorkspace.sizes.explorerMain,
      verticalLayout: initialWorkspace.sizes.editorResults,
    }));
    setIsUiHydrated(true);
  }, [dispatch, initialWorkspace]);

  useEffect(() => {
    if (!selectedTableKey && schema.length > 0 && schema[0].tables.length > 0) {
      const firstTable = schema[0].tables[0];
      dispatch(setSelectedTableKey(`${firstTable.namespace}.${firstTable.name}`));
    }
  }, [dispatch, schema, selectedTableKey]);

  useEffect(() => {
    if (schema.length === 0 || Object.keys(expandedNamespaces).length > 0) {
      return;
    }

    const initialExpanded: Record<string, boolean> = {};
    schema.slice(0, 3).forEach((namespace) => {
      initialExpanded[namespace.name] = true;
    });
    dispatch(hydrateSqlStudioUi({ expandedNamespaces: initialExpanded }));
  }, [dispatch, schema, expandedNamespaces]);

  useEffect(() => {
    if (!selectedTableKey) {
      return;
    }

    const [namespaceName] = selectedTableKey.split(".");
    dispatch(setNamespaceExpanded({ namespaceName, expanded: true }));
    dispatch(setTableExpanded({ tableKey: selectedTableKey, expanded: true }));
  }, [dispatch, selectedTableKey]);

  useEffect(() => {
    if (!isUiHydrated) {
      return;
    }

    const state = location.state as { prefillSql?: string; prefillTitle?: string } | null;
    const prefillSql = state?.prefillSql?.trim();
    if (!prefillSql) {
      return;
    }

    const prefillKey = `${location.key}:${prefillSql}`;
    if (consumedPrefillKeyRef.current === prefillKey) {
      return;
    }
    consumedPrefillKeyRef.current = prefillKey;

    const tab: QueryTab = {
      id: createTabId(tabs.length + 1),
      title: state?.prefillTitle?.trim() || `Query ${tabs.length + 1}`,
      sql: prefillSql,
      isDirty: true,
      isLive: false,
      liveStatus: "idle",
      resultView: "results",
      lastSavedAt: null,
      savedQueryId: null,
    };
    dispatch(addWorkspaceTab(tab));
    dispatch(setWorkspaceActiveTabId(tab.id));
    navigate(location.pathname, { replace: true, state: null });
  }, [dispatch, isUiHydrated, location.key, location.pathname, location.state, navigate, tabs.length]);

  const updateTab = useCallback((tabId: string, updates: Partial<QueryTab>) => {
    dispatch(updateWorkspaceTab({ tabId, updates }));
  }, [dispatch]);

  const updateActiveTab = useCallback((updates: Partial<QueryTab>) => {
    if (!activeTab) {
      return;
    }
    updateTab(activeTab.id, updates);
  }, [activeTab, updateTab]);

  const addTab = () => {
    const nextIndex = tabs.length + 1;
    const tab = createQueryTab(nextIndex);
    dispatch(addWorkspaceTab(tab));
    dispatch(setWorkspaceActiveTabId(tab.id));
  };

  const closeTab = useCallback((tabId: string) => {
    if (tabs.length === 1) {
      return;
    }

    const unsubscribe = liveUnsubscribeRef.current[tabId];
    if (unsubscribe) {
      unsubscribe();
      delete liveUnsubscribeRef.current[tabId];
    }

    dispatch(closeWorkspaceTab(tabId));
  }, [dispatch, tabs]);

  const openQueryInNewTab = (query: string, title: string) => {
    const tab: QueryTab = {
      id: createTabId(tabs.length + 1),
      title,
      sql: query,
      isDirty: true,
      isLive: false,
      liveStatus: "idle",
      resultView: "results",
      lastSavedAt: null,
      savedQueryId: null,
    };
    dispatch(addWorkspaceTab(tab));
    dispatch(setWorkspaceActiveTabId(tab.id));
  };

  const executeQueryForTab = async (tabId: string, sql: string, tabTitle: string) => {
    if (!sql.trim()) {
      return;
    }

    dispatch(setWorkspaceRunning(true));
    const startedAt = Date.now();

    try {
      const queryResult = await executeSqlStudioQuery(sql);
      const durationMs = Math.round(Date.now() - startedAt);
      const historyEntry: QueryRunSummary = {
        id: `${tabId}-${startedAt}`,
        tabTitle,
        sql,
        status: queryResult.status,
        executedAt: new Date().toISOString(),
        durationMs,
        rowCount: queryResult.rowCount,
        errorMessage: queryResult.errorMessage,
      };

      startTransition(() => {
        dispatch(setWorkspaceTabResult({ tabId, result: queryResult }));
        dispatch(prependWorkspaceHistory(historyEntry));
        updateTab(tabId, {
          isDirty: false,
          resultView: resolveResultView(queryResult),
        });
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Query execution failed";
      dispatch(setWorkspaceTabResult({ tabId, result: {
        status: "error",
        rows: [],
        schema: [],
        tookMs: 0,
        rowCount: 0,
        logs: [createLogEntry(message, "error", user?.username, error)],
        errorMessage: message,
      } }));
      updateTab(tabId, { resultView: "log" });
    } finally {
      dispatch(setWorkspaceRunning(false));
    }
  };

  const stopLiveQuery = useCallback((tabId: string) => {
    const unsubscribe = liveUnsubscribeRef.current[tabId];
    if (unsubscribe) {
      unsubscribe();
      delete liveUnsubscribeRef.current[tabId];
    }
    // Clear the SDK log listener when stopping
    setClientLogListener(undefined);
    setClientReceiveListener(undefined);
    setClientSendListener(undefined);
    setClientDisconnectListener(undefined);
    setClientErrorListener(undefined);
    updateTab(tabId, { liveStatus: "idle" });
  }, [updateTab]);

  const startLiveQuery = useCallback(async (tab: QueryTab) => {
    if (!tab.sql.trim()) {
      return;
    }

    dispatch(setWorkspaceTabResult({
      tabId: tab.id,
      result: {
        status: "success",
        rows: [],
        schema: [],
        tookMs: 0,
        rowCount: 0,
        logs: [createLogEntry("Starting live subscription.", "info", user?.username, {
          event: "live_start",
          sql: tab.sql,
          options: tab.subscriptionOptions,
        })],
      },
    }));
    updateTab(tab.id, { liveStatus: "connecting", isLive: true });

    // Set up SDK log listener to capture all WS-level logs for this subscription
    setClientLogListener((entry) => {
      dispatch(appendWorkspaceResultLog({
        tabId: tab.id,
        entry: createLogEntry(
          `[SDK ${entry.tag}] ${entry.message}`,
          entry.level >= 4 ? "error" : "info",
          user?.username,
          entry,
        ),
      }));
    });
    setClientSendListener((message) => {
      dispatch(appendWorkspaceResultLog({
        tabId: tab.id,
        entry: createWireLogEntry("send", message, user?.username),
      }));
    });
    setClientReceiveListener((message) => {
      dispatch(appendWorkspaceResultLog({
        tabId: tab.id,
        entry: createWireLogEntry("receive", message, user?.username),
      }));
    });
    setClientDisconnectListener((reason) => {
      dispatch(appendWorkspaceResultLog({
        tabId: tab.id,
        entry: createLogEntry(
          `Live connection closed${reason.message ? `: ${reason.message}` : "."}`,
          "error",
          user?.username,
          reason,
        ),
        statusOverride: "error",
      }));
      updateTab(tab.id, { liveStatus: "error" });
    });
    setClientErrorListener((error) => {
      dispatch(appendWorkspaceResultLog({
        tabId: tab.id,
        entry: createLogEntry(
          `Live connection error: ${error.message}`,
          "error",
          user?.username,
          error,
        ),
        statusOverride: "error",
      }));
      updateTab(tab.id, { liveStatus: "error" });
    });

    try {
      const unsubscribe = await subscribe(tab.sql, (msg: ServerMessage) => {
        switch (msg.type) {
          case "error": {
            dispatch(appendWorkspaceResultLog({
              tabId: tab.id,
              entry: createLogEntry(msg.message ?? "Live query error", "error", user?.username, msg),
              statusOverride: "error",
            }));
            updateTab(tab.id, { liveStatus: "error" });
            return;
          }

          case "auth_success": {
            dispatch(appendWorkspaceResultLog({
              tabId: tab.id,
              entry: createLogEntry("Authentication successful.", "info", user?.username, msg),
            }));
            return;
          }

          case "auth_error": {
            dispatch(appendWorkspaceResultLog({
              tabId: tab.id,
              entry: createLogEntry(`Authentication failed: ${(msg as Record<string, unknown>).message ?? "unknown error"}`, "error", user?.username, msg),
              statusOverride: "error",
            }));
            updateTab(tab.id, { liveStatus: "error" });
            return;
          }

          case "subscription_ack": {
            if (msg.schema.length > 0) {
              dispatch(setWorkspaceLiveSchema({
                tabId: tab.id,
                schema: normalizeSchema(msg.schema as SchemaField[]),
              }));
            }
            dispatch(appendWorkspaceResultLog({
              tabId: tab.id,
              entry: createLogEntry("Live subscription connected.", "info", user?.username, msg),
            }));
            updateTab(tab.id, { liveStatus: "connected", resultView: "results" });
            return;
          }

          case "initial_data_batch": {
            const rows = normalizeLiveRows(msg.rows);
            dispatch(appendWorkspaceLiveRows({
              tabId: tab.id,
              rows,
              changeType: "initial",
              schema: undefined,
            }));
            dispatch(appendWorkspaceResultLog({
              tabId: tab.id,
              entry: createLogEntry(
                `Received ${rows.length} row${rows.length === 1 ? "" : "s"} (initial).`,
                "info",
                user?.username,
                msg,
              ),
            }));
            updateTab(tab.id, { liveStatus: "connected", resultView: "results" });
            return;
          }

          case "change": {
            const changeType: ChangeTypeRaw = msg.change_type;

            if (changeType === "delete") {
              // DELETE: data is in old_values, rows is null
              const deletedRows = normalizeLiveRows(msg.old_values ?? []);
              dispatch(appendWorkspaceLiveRows({
                tabId: tab.id,
                rows: deletedRows,
                changeType: "delete",
              }));
              dispatch(appendWorkspaceResultLog({
                tabId: tab.id,
                entry: createLogEntry(
                  `Deleted ${deletedRows.length} row${deletedRows.length === 1 ? "" : "s"}.`,
                  "info",
                  user?.username,
                  msg,
                ),
              }));
            } else if (changeType === "update") {
              // UPDATE: delta rows contain only changed columns + PK + _seq.
              // Changed user columns are the non-system keys (those not starting with '_').
              const updatedRows = normalizeLiveRows(msg.rows ?? []);
              dispatch(appendWorkspaceLiveRows({
                tabId: tab.id,
                rows: updatedRows,
                changeType: "update",
              }));
              dispatch(appendWorkspaceResultLog({
                tabId: tab.id,
                entry: createLogEntry(
                  `Updated ${updatedRows.length} row${updatedRows.length === 1 ? "" : "s"}.`,
                  "info",
                  user?.username,
                  msg,
                ),
              }));
            } else {
              // INSERT
              const insertedRows = normalizeLiveRows(msg.rows ?? []);
              dispatch(appendWorkspaceLiveRows({
                tabId: tab.id,
                rows: insertedRows,
                changeType: "insert",
              }));
              dispatch(appendWorkspaceResultLog({
                tabId: tab.id,
                entry: createLogEntry(
                  `Inserted ${insertedRows.length} row${insertedRows.length === 1 ? "" : "s"}.`,
                  "info",
                  user?.username,
                  msg,
                ),
              }));
            }
            updateTab(tab.id, { liveStatus: "connected", resultView: "results" });
            return;
          }

          default:
            // Unknown message type — log it for tracing
            dispatch(appendWorkspaceResultLog({
              tabId: tab.id,
              entry: createLogEntry(
                `Received message: ${(msg as { type: string }).type}`,
                "info",
                user?.username,
                msg,
              ),
            }));
            break;
        }
      }, tab.subscriptionOptions);

      liveUnsubscribeRef.current[tab.id] = unsubscribe;
    } catch (error) {
      console.error("Failed to subscribe to live query", error);
      dispatch(appendWorkspaceResultLog({
        tabId: tab.id,
        entry: createLogEntry(
          error instanceof Error ? error.message : "Failed to subscribe to live query",
          "error",
          user?.username,
          error,
        ),
        statusOverride: "error",
      }));
      updateTab(tab.id, { liveStatus: "error" });
    }
  }, [dispatch, updateTab, user?.username]);

  const runActiveQuery = async () => {
    if (!activeTab) {
      return;
    }

    if (activeTab.isLive) {
      if (activeTab.liveStatus === "connected") {
        stopLiveQuery(activeTab.id);
      } else {
        await startLiveQuery(activeTab);
      }
      return;
    }

    await executeQueryForTab(activeTab.id, activeTab.sql, activeTab.title);
  };

  const saveTab = useCallback((tabId: string, openAsCopy: boolean) => {
    const tab = tabs.find((item) => item.id === tabId);
    if (!tab) {
      return;
    }

    const nowIso = new Date().toISOString();
    const saveId = openAsCopy || !tab.savedQueryId ? createSavedQueryId() : tab.savedQueryId;
    const saveTitle = openAsCopy ? `${tab.title} Copy` : tab.title;

    const nextSavedQueries = (() => {
      const previous = savedQueries;
      const existing = previous.find((item) => item.id === saveId);
      if (existing) {
        return previous.map((item) =>
          item.id === saveId
            ? { ...item, title: saveTitle, sql: tab.sql, isLive: tab.isLive, lastSavedAt: nowIso, subscriptionOptions: tab.subscriptionOptions }
            : item,
        );
      }
      return [
        {
          id: saveId,
          title: saveTitle,
          sql: tab.sql,
          isLive: tab.isLive,
          lastSavedAt: nowIso,
          subscriptionOptions: tab.subscriptionOptions,
        },
        ...previous,
      ];
    })();
    dispatch(setWorkspaceSavedQueries(nextSavedQueries));

    if (openAsCopy) {
      const copiedTab: QueryTab = {
        ...tab,
        id: createTabId(tabs.length + 1),
        title: saveTitle,
        isDirty: false,
        savedQueryId: saveId,
        lastSavedAt: nowIso,
        liveStatus: "idle",
        resultView: tab.resultView,
      };
      dispatch(addWorkspaceTab(copiedTab));
      dispatch(setWorkspaceActiveTabId(copiedTab.id));
      if (tabResults[tab.id]) {
        dispatch(setWorkspaceTabResult({ tabId: copiedTab.id, result: tabResults[tab.id] ?? null }));
      }
      return;
    }

    updateTab(tab.id, {
      savedQueryId: saveId,
      lastSavedAt: nowIso,
      isDirty: false,
    });
  }, [dispatch, savedQueries, tabs, tabResults, updateTab]);

  const renameActiveTab = useCallback((title: string) => {
    if (!activeTab) {
      return;
    }

    updateTab(activeTab.id, { title });
    if (activeTab.savedQueryId) {
      dispatch(setWorkspaceSavedQueries(
        savedQueries.map((item) =>
          item.id === activeTab.savedQueryId ? { ...item, title } : item,
        ),
      ));
    }
  }, [activeTab, dispatch, savedQueries, updateTab]);

  const deleteActiveTab = useCallback(() => {
    if (!activeTab) {
      return;
    }

    const deletedSavedQueryId = activeTab.savedQueryId;
    closeTab(activeTab.id);
    if (deletedSavedQueryId) {
      dispatch(setWorkspaceSavedQueries(
        savedQueries.filter((item) => item.id !== deletedSavedQueryId),
      ));
      dispatch(setWorkspaceTabs(
        tabs.map((item) =>
          item.savedQueryId === deletedSavedQueryId
            ? { ...item, savedQueryId: null }
            : item,
        ),
      ));
    }
  }, [activeTab, closeTab, dispatch, savedQueries, tabs]);

  const openSavedQuery = useCallback((queryId: string) => {
    const savedQuery = savedQueries.find((item) => item.id === queryId);
    if (!savedQuery) {
      return;
    }

    const existingTab = tabs.find((item) => item.savedQueryId === queryId);
    if (existingTab) {
      dispatch(setWorkspaceActiveTabId(existingTab.id));
      return;
    }

    const tab: QueryTab = {
      id: createTabId(tabs.length + 1),
      title: savedQuery.title,
      sql: savedQuery.sql,
      isDirty: false,
      isLive: savedQuery.isLive,
      liveStatus: "idle",
      resultView: "results",
      lastSavedAt: savedQuery.lastSavedAt,
      savedQueryId: savedQuery.id,
      subscriptionOptions: savedQuery.subscriptionOptions,
    };
    dispatch(addWorkspaceTab(tab));
    dispatch(setWorkspaceActiveTabId(tab.id));
  }, [dispatch, savedQueries, tabs]);

  const toggleInspector = () => {
    if (isInspectorCollapsed) {
      dispatch(setInspectorCollapsed(false));
      return;
    }
    dispatch(setInspectorCollapsed(true));
  };

  useEffect(() => {
    if (!explorerContextMenu) {
      return;
    }

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setExplorerContextMenu(null);
      }
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [explorerContextMenu]);

  useEffect(() => {
    return () => {
      Object.values(liveUnsubscribeRef.current).forEach((unsubscribe) => unsubscribe());
      liveUnsubscribeRef.current = {};
      setClientLogListener(undefined);
    };
  }, []);

  useEffect(() => {
    if (tabs.length === 0 || !isUiHydrated) {
      return;
    }

    const persistedActiveTabId = tabs.find((tab) => tab.id === activeTabId)?.id ?? tabs[0].id;

    const nextState: SqlStudioWorkspaceState = {
      version: 1,
      tabs: tabs.map(toPersistedTab),
      savedQueries: savedQueries.map((item) => ({
        id: item.id,
        title: item.title,
        sql: item.sql,
        lastSavedAt: item.lastSavedAt,
        isLive: item.isLive,
        subscriptionOptions: item.subscriptionOptions,
      })),
      activeTabId: persistedActiveTabId,
      selectedTableKey,
      inspectorCollapsed: isInspectorCollapsed,
      sizes: {
        explorerMain: horizontalLayout,
        editorResults: verticalLayout,
      },
      explorerTree: {
        favoritesExpanded,
        namespaceSectionExpanded,
        expandedNamespaces,
        expandedTables,
        filter: schemaFilter,
      },
    };

    const timeoutId = window.setTimeout(() => {
      saveSqlStudioWorkspaceState(nextState);
    }, WORKSPACE_PERSIST_DEBOUNCE_MS);

    return () => window.clearTimeout(timeoutId);
  }, [
    tabs,
    activeTabId,
    selectedTableKey,
    isInspectorCollapsed,
    horizontalLayout,
    verticalLayout,
    favoritesExpanded,
    namespaceSectionExpanded,
    savedQueries,
    expandedNamespaces,
    expandedTables,
    schemaFilter,
    isUiHydrated,
  ]);

  if (!activeTab) {
    return (
      <div className="flex h-full items-center justify-center bg-background text-sm text-muted-foreground">
        Loading SQL workspace...
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden bg-background text-foreground">
      <div className="flex min-h-0 flex-1 overflow-hidden">
        <ResizablePanelGroup orientation="horizontal" className="min-h-0 flex-1">
          <ResizablePanel
            defaultSize={horizontalLayout[0]}
            minSize="12%"
            maxSize="36%"
            className="min-h-0"
            onResize={(size) => {
              const numericSize = Number(size);
              if (!Number.isFinite(numericSize)) {
                return;
              }
              const safeSize = Math.max(12, Math.min(36, numericSize));
              dispatch(setHorizontalLayout([safeSize, 100 - safeSize]));
            }}
          >
            <StudioExplorerPanel
              schema={schema}
              filter={schemaFilter}
              savedQueries={savedQueries}
              favoritesExpanded={favoritesExpanded}
              namespaceSectionExpanded={namespaceSectionExpanded}
              expandedNamespaces={expandedNamespaces}
              expandedTables={expandedTables}
              selectedTableKey={selectedTableKey}
              onFilterChange={(value) => dispatch(setSchemaFilter(value))}
              onToggleFavorites={() => dispatch(toggleFavoritesExpanded())}
              onToggleNamespaceSection={() => dispatch(toggleNamespaceSectionExpanded())}
              onToggleNamespace={(namespaceName) => dispatch(toggleNamespaceExpanded(namespaceName))}
              onToggleTable={(tableKey) => dispatch(toggleTableExpanded(tableKey))}
              onOpenSavedQuery={openSavedQuery}
              onSelectTable={(table) => dispatch(setSelectedTableKey(`${table.namespace}.${table.name}`))}
              onTableContextMenu={(table, position) => {
                setExplorerContextMenu({
                  x: position.x,
                  y: position.y,
                  table,
                });
              }}
            />
          </ResizablePanel>

          <ResizableHandle withHandle />

          <ResizablePanel defaultSize={horizontalLayout[1]} minSize="40%" className="min-h-0">
            <div className="relative flex h-full min-h-0 flex-col overflow-hidden bg-background">
              <div className="absolute right-2 top-1.5 z-20">
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-8 w-8 text-muted-foreground hover:text-foreground"
                  onClick={toggleInspector}
                  title={isInspectorCollapsed ? "Expand details panel" : "Collapse details panel"}
                >
                  {isInspectorCollapsed ? (
                    <PanelRightOpen className="h-4 w-4" />
                  ) : (
                    <PanelRightClose className="h-4 w-4" />
                  )}
                </Button>
              </div>

              <QueryTabStrip
                tabs={tabs}
                activeTabId={activeTab.id}
                onTabSelect={(tabId) => dispatch(setWorkspaceActiveTabId(tabId))}
                onAddTab={addTab}
                onCloseTab={closeTab}
              />

              <ResizablePanelGroup orientation="vertical" className="min-h-0 flex-1">
                <ResizablePanel
                  defaultSize={verticalLayout[0]}
                  minSize="20%"
                  className="min-h-0"
                  onResize={(size) => {
                    const numericSize = Number(size);
                    if (!Number.isFinite(numericSize)) {
                      return;
                    }
                    const safeSize = Math.max(20, Math.min(80, numericSize));
                    dispatch(setVerticalLayout([safeSize, 100 - safeSize]));
                  }}
                >
                  <StudioEditorPanel
                    schema={schema}
                    tabTitle={activeTab.title}
                    lastSavedAt={activeTab.lastSavedAt}
                    isLive={activeTab.isLive}
                    liveStatus={activeTab.liveStatus}
                    sql={activeTab.sql}
                    isRunning={isRunning}
                    subscriptionOptions={activeTab.subscriptionOptions}
                    onSqlChange={(value) => updateActiveTab({ sql: value, isDirty: true })}
                    onRun={runActiveQuery}
                    onToggleLive={(checked) => {
                      updateActiveTab({ isLive: checked, liveStatus: "idle" });
                      if (!checked && activeTab.liveStatus === "connected") {
                        stopLiveQuery(activeTab.id);
                      }
                    }}
                    onSubscriptionOptionsChange={(options) => updateActiveTab({ subscriptionOptions: options, isDirty: true })}
                    onRename={renameActiveTab}
                    onSave={() => saveTab(activeTab.id, false)}
                    onSaveCopy={() => saveTab(activeTab.id, true)}
                    onDelete={deleteActiveTab}
                  />
                </ResizablePanel>

                <ResizableHandle withHandle />

                <ResizablePanel defaultSize={verticalLayout[1]} minSize="20%" className="min-h-0">
                  <StudioResultsGrid
                    result={activeResult}
                    isRunning={isRunning}
                    isLiveMode={activeTab.isLive}
                    activeSql={activeTab.sql}
                    selectedTable={selectedTable}
                    currentUsername={user?.username ?? "admin"}
                    resultView={activeTab.resultView}
                    onResultViewChange={(view) => updateActiveTab({ resultView: view })}
                    onRefreshAfterCommit={() => executeQueryForTab(activeTab.id, activeTab.sql, activeTab.title)}
                  />
                </ResizablePanel>
              </ResizablePanelGroup>
            </div>
          </ResizablePanel>
        </ResizablePanelGroup>

        {!isInspectorCollapsed && (
          <div className="min-h-0 w-[320px] min-w-[240px] max-w-[420px] border-l border-border">
            <StudioInspectorPanel selectedTable={selectedTable} history={history} />
          </div>
        )}
      </div>

      <ExplorerTableContextMenu
        contextMenu={explorerContextMenu}
        onClose={() => setExplorerContextMenu(null)}
        onOpenQueryInNewTab={(table) => {
          const sql = buildSelectFromTableSql(table.namespace, table.name, true);
          openQueryInNewTab(sql, table.name);
          dispatch(setSelectedTableKey(`${table.namespace}.${table.name}`));
          setExplorerContextMenu(null);
        }}
        onSelectFromTable={(table) => {
          const sql = buildSelectFromTableSql(table.namespace, table.name, true);
          updateActiveTab({ sql, isDirty: true });
          dispatch(setSelectedTableKey(`${table.namespace}.${table.name}`));
          setExplorerContextMenu(null);
          executeQueryForTab(activeTab.id, sql, activeTab.title).catch(console.error);
        }}
        onInsertSelectQuery={(table) => {
          const sql = buildSelectFromTableSql(table.namespace, table.name, false);
          updateActiveTab({ sql, isDirty: true });
          dispatch(setSelectedTableKey(`${table.namespace}.${table.name}`));
          setExplorerContextMenu(null);
        }}
        onViewProperties={(table) => {
          dispatch(setSelectedTableKey(`${table.namespace}.${table.name}`));
          dispatch(setInspectorCollapsed(false));
          setExplorerContextMenu(null);
        }}
        onCopyQualifiedName={(table) => {
          navigator.clipboard
            .writeText(`${table.namespace}.${table.name}`)
            .catch(console.error);
          setExplorerContextMenu(null);
        }}
      />
    </div>
  );
}
