// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { configureStore } from "@reduxjs/toolkit";
import { Provider } from "react-redux";
import { MemoryRouter } from "react-router-dom";
import { useEffect, useRef } from "react";
import { SqlPreviewProvider } from "@/components/sql-preview";
import SqlStudio from "@/pages/SqlStudio";
import sqlStudioUiReducer from "@/features/sql-studio/state/sqlStudioUiSlice";
import sqlStudioWorkspaceReducer from "@/features/sql-studio/state/sqlStudioWorkspaceSlice";

const mockUseAuth = vi.fn();
const mockSchemaTreeQuery = vi.fn();
const mockExecuteSqlStudioQuery = vi.fn();
const mockSubscribe = vi.fn();
const mockSetClientLogListener = vi.fn();
const mockSetClientDisconnectListener = vi.fn();
const mockSetClientErrorListener = vi.fn();
const mockSetClientReceiveListener = vi.fn();
const mockSetClientSendListener = vi.fn();
const mockUnsubscribe = vi.fn();
const mockRemoteUnsubscribe = vi.fn();
const mockSaveSyncedSqlStudioWorkspaceState = vi.fn();
const mockSubscribeToSyncedSqlStudioWorkspaceState = vi.fn();
const mockRefetchSchemaTree = vi.fn();

let liveCallback: ((message: Record<string, unknown>) => void) | null = null;
let syncedWorkspaceCallback: ((workspace: Record<string, unknown> | null) => void) | null = null;
let clientDisconnectCallback: ((reason: Record<string, unknown>) => void) | null = null;
let clientReceiveCallback: ((message: string) => void) | null = null;
let clientSendCallback: ((message: string) => void) | null = null;
let latestEditorCommand: (() => void) | null = null;

vi.mock("@/lib/auth", () => ({
  useAuth: () => mockUseAuth(),
}));

vi.mock("@/store/apiSlice", () => ({
  useGetSqlStudioSchemaTreeQuery: () => mockSchemaTreeQuery(),
}));

vi.mock("@/services/sqlStudioService", async () => {
  const actual = await vi.importActual<typeof import("@/services/sqlStudioService")>("@/services/sqlStudioService");
  return {
    ...actual,
    executeSqlStudioQuery: (...args: unknown[]) => mockExecuteSqlStudioQuery(...args),
  };
});

vi.mock("@/lib/kalam-client", () => ({
  subscribe: (...args: unknown[]) => mockSubscribe(...args),
  setClientDisconnectListener: (...args: unknown[]) => mockSetClientDisconnectListener(...args),
  setClientErrorListener: (...args: unknown[]) => mockSetClientErrorListener(...args),
  setClientLogListener: (...args: unknown[]) => mockSetClientLogListener(...args),
  setClientReceiveListener: (...args: unknown[]) => mockSetClientReceiveListener(...args),
  setClientSendListener: (...args: unknown[]) => mockSetClientSendListener(...args),
  executeSql: vi.fn(),
}));

vi.mock("@/services/sqlStudioWorkspaceSyncService", () => ({
  buildSyncedSqlStudioWorkspaceState: (...args: unknown[]) => {
    const [tabs, savedQueries, activeTabId] = args as [Array<Record<string, unknown>>, Array<Record<string, unknown>>, string | null];
    const safeActiveTabId = tabs.find((tab) => tab.id === activeTabId)?.id ?? tabs[0]?.id ?? "";
    return {
      version: 1,
      tabs: tabs.map((tab) => ({
        id: tab.id,
        name: tab.title,
        query: tab.sql,
        settings: {
          isDirty: tab.isDirty,
          isLive: tab.isLive,
          liveStatus: tab.liveStatus,
          resultView: tab.resultView,
          lastSavedAt: tab.lastSavedAt,
          savedQueryId: tab.savedQueryId,
          subscriptionOptions: tab.subscriptionOptions,
        },
      })),
      savedQueries: savedQueries.map((query) => ({
        ...query,
        openedRecently: tabs.some((tab) => tab.savedQueryId === query.id),
        isCurrentTab: tabs.some((tab) => tab.savedQueryId === query.id && tab.id === safeActiveTabId),
      })),
      activeTabId: safeActiveTabId,
      updatedAt: "2026-03-27T00:00:00.000Z",
    };
  },
  saveSyncedSqlStudioWorkspaceState: (...args: unknown[]) => mockSaveSyncedSqlStudioWorkspaceState(...args),
  subscribeToSyncedSqlStudioWorkspaceState: (...args: unknown[]) => mockSubscribeToSyncedSqlStudioWorkspaceState(...args),
}));

vi.mock("@kalamdb/client", () => {
  class KalamCellValue {
    private value: unknown;

    constructor(value: unknown) {
      this.value = value;
    }

    static from(value: unknown) {
      return new KalamCellValue(value);
    }

    toJson() {
      return this.value;
    }
  }

  return { KalamCellValue };
});

vi.mock("@monaco-editor/react", () => ({
  default: ({
    value,
    onChange,
    onMount,
  }: {
    value?: string;
    onChange?: (value: string) => void;
    onMount?: (editor: {
      addCommand: (_keybinding: number, callback: () => void) => void;
      getSelection: () => {
        startLineNumber: number;
        startColumn: number;
        endLineNumber: number;
        endColumn: number;
        isEmpty: () => boolean;
      };
      getModel: () => { getValueInRange: () => string };
      onDidChangeCursorSelection: (listener: () => void) => { dispose: () => void };
      onDidChangeModelContent: (listener: () => void) => { dispose: () => void };
    }, monaco: {
      KeyMod: { CtrlCmd: number };
      KeyCode: { Enter: number };
      languages: {
        CompletionItemKind: Record<string, number>;
        registerCompletionItemProvider: () => { dispose: () => void };
      };
    }) => void;
  }) => {
    const selectionListenersRef = useRef(new Set<() => void>());
    const contentListenersRef = useRef(new Set<() => void>());

    useEffect(() => {
      onMount?.(
        {
          addCommand: (_keybinding: number, callback: () => void) => {
            latestEditorCommand = callback;
          },
          getSelection: () => {
            const textarea = document.querySelector("textarea[aria-label='SQL editor']") as HTMLTextAreaElement | null;
            const start = textarea?.selectionStart ?? 0;
            const end = textarea?.selectionEnd ?? 0;
            return {
              startLineNumber: 1,
              startColumn: start + 1,
              endLineNumber: 1,
              endColumn: end + 1,
              isEmpty: () => start === end,
            };
          },
          getModel: () => ({
            getValueInRange: () => {
              const textarea = document.querySelector("textarea[aria-label='SQL editor']") as HTMLTextAreaElement | null;
              const currentValue = textarea?.value ?? value ?? "";
              const start = textarea?.selectionStart ?? 0;
              const end = textarea?.selectionEnd ?? 0;
              return currentValue.slice(start, end);
            },
          }),
          onDidChangeCursorSelection: (listener: () => void) => {
            selectionListenersRef.current.add(listener);
            return { dispose: () => selectionListenersRef.current.delete(listener) };
          },
          onDidChangeModelContent: (listener: () => void) => {
            contentListenersRef.current.add(listener);
            return { dispose: () => contentListenersRef.current.delete(listener) };
          },
        },
        {
          KeyMod: { CtrlCmd: 1 },
          KeyCode: { Enter: 1 },
          languages: {
            CompletionItemKind: {
              Field: 1,
              Class: 2,
              Module: 3,
              Keyword: 4,
            },
            registerCompletionItemProvider: () => ({ dispose: () => {} }),
          },
        },
      );
    }, [onMount]);

    return (
      <textarea
        aria-label="SQL editor"
        value={value ?? ""}
        onChange={(event) => {
          onChange?.(event.target.value);
          contentListenersRef.current.forEach((listener) => listener());
        }}
        onSelect={() => {
          selectionListenersRef.current.forEach((listener) => listener());
        }}
      />
    );
  },
}));

function createTestStore() {
  return configureStore({
    reducer: {
      sqlStudioUi: sqlStudioUiReducer,
      sqlStudioWorkspace: sqlStudioWorkspaceReducer,
    },
  });
}

function renderSqlStudio(store = createTestStore()) {
  const view = render(
    <Provider store={store}>
      <MemoryRouter>
        <SqlPreviewProvider>
          <SqlStudio />
        </SqlPreviewProvider>
      </MemoryRouter>
    </Provider>,
  );
  return { store, ...view };
}

function getSqlEditor() {
  const editors = screen.getAllByLabelText("SQL editor");
  return editors[editors.length - 1];
}

describe("SqlStudio page", () => {
  beforeEach(() => {
    cleanup();
    vi.stubGlobal(
      "ResizeObserver",
      class ResizeObserver {
        observe() {}
        unobserve() {}
        disconnect() {}
      },
    );
    liveCallback = null;
    syncedWorkspaceCallback = null;
    clientDisconnectCallback = null;
    clientReceiveCallback = null;
    clientSendCallback = null;
    latestEditorCommand = null;
    mockUseAuth.mockReset();
    mockSchemaTreeQuery.mockReset();
    mockExecuteSqlStudioQuery.mockReset();
    mockSubscribe.mockReset();
    mockSetClientDisconnectListener.mockReset();
    mockSetClientErrorListener.mockReset();
    mockSetClientLogListener.mockReset();
    mockSetClientReceiveListener.mockReset();
    mockSetClientSendListener.mockReset();
    mockUnsubscribe.mockReset();
    mockRemoteUnsubscribe.mockReset();
    mockSaveSyncedSqlStudioWorkspaceState.mockReset();
    mockSubscribeToSyncedSqlStudioWorkspaceState.mockReset();
    mockRefetchSchemaTree.mockReset();
    window.localStorage.clear();

    mockUseAuth.mockReturnValue({
      user: { username: "root", role: "system" },
    });

    mockExecuteSqlStudioQuery.mockResolvedValue({
      status: "success",
      rows: [],
      schema: [],
      tookMs: 0,
      rowCount: 0,
      logs: [],
    });

    mockSchemaTreeQuery.mockReturnValue({
      data: [
        {
          database: "database",
          name: "default",
          tables: [
            {
              database: "database",
              namespace: "default",
              name: "events",
              tableType: "shared",
              columns: [
                { name: "id", dataType: "INT", isNullable: false, isPrimaryKey: true, ordinal: 1 },
                { name: "name", dataType: "TEXT", isNullable: false, isPrimaryKey: false, ordinal: 2 },
              ],
            },
          ],
        },
      ],
      isFetching: false,
      refetch: mockRefetchSchemaTree,
    });
    mockRefetchSchemaTree.mockResolvedValue({ data: [] });

    mockSubscribe.mockImplementation(async (_sql: string, callback: (message: Record<string, unknown>) => void) => {
      liveCallback = callback;
      return mockUnsubscribe;
    });
    mockSaveSyncedSqlStudioWorkspaceState.mockResolvedValue(undefined);
    mockSubscribeToSyncedSqlStudioWorkspaceState.mockImplementation(async (_username: string, callback: (workspace: Record<string, unknown> | null) => void) => {
      syncedWorkspaceCallback = callback;
      return mockRemoteUnsubscribe;
    });
    mockSetClientDisconnectListener.mockImplementation((callback?: (reason: Record<string, unknown>) => void) => {
      clientDisconnectCallback = callback ?? null;
    });
    mockSetClientErrorListener.mockImplementation(() => {});
    mockSetClientReceiveListener.mockImplementation((callback?: (message: string) => void) => {
      clientReceiveCallback = callback ?? null;
    });
    mockSetClientSendListener.mockImplementation((callback?: (message: string) => void) => {
      clientSendCallback = callback ?? null;
    });
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("runs a query from the SQL Studio page and renders the results grid", async () => {
    mockExecuteSqlStudioQuery.mockResolvedValue({
      status: "success",
      rows: [{ id: 1, name: "Ada" }],
      schema: [
        { name: "id", dataType: "INT", index: 0, isPrimaryKey: true },
        { name: "name", dataType: "TEXT", index: 1, isPrimaryKey: false },
      ],
      tookMs: 12,
      rowCount: 1,
      logs: [],
    });

    renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });
    fireEvent.click(screen.getByRole("button", { name: /^execute$/i }));

    await waitFor(() => {
      expect(mockExecuteSqlStudioQuery).toHaveBeenCalledWith("SELECT id, name FROM default.events");
    });

    expect(await screen.findByText("Ada")).toBeTruthy();
  });

  it("refreshes the Explorer when the refresh button is clicked", async () => {
    renderSqlStudio();

    fireEvent.click(screen.getByRole("button", { name: /refresh explorer/i }));

    await waitFor(() => {
      expect(mockRefetchSchemaTree).toHaveBeenCalledTimes(1);
    });
  });

  it("refetches the Explorer schema after a successful create table query", async () => {
    renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "CREATE TABLE default.audit_log (id INT PRIMARY KEY)" },
    });
    fireEvent.click(screen.getByRole("button", { name: /^execute$/i }));

    await waitFor(() => {
      expect(mockExecuteSqlStudioQuery).toHaveBeenCalledWith("CREATE TABLE default.audit_log (id INT PRIMARY KEY)");
    });

    await waitFor(() => {
      expect(mockRefetchSchemaTree).toHaveBeenCalledTimes(1);
    });
  });

  it("executes the current selection and exposes execute options", async () => {
    mockExecuteSqlStudioQuery.mockResolvedValue({
      status: "success",
      rows: [{ id: 1 }],
      schema: [
        { name: "id", dataType: "INT", index: 0, isPrimaryKey: true },
      ],
      tookMs: 5,
      rowCount: 1,
      logs: [],
    });

    renderSqlStudio();

    const fullSql = "SELECT id FROM default.events; SELECT name FROM default.events";
    const selectedSql = "SELECT id FROM default.events";

    fireEvent.change(getSqlEditor(), {
      target: { value: fullSql },
    });

    const editor = getSqlEditor() as HTMLTextAreaElement;
    editor.setSelectionRange(0, selectedSql.length);
    fireEvent.select(editor);

    expect(screen.getByRole("button", { name: /execute selected/i })).toBeTruthy();

    expect(screen.getByRole("button", { name: /execute options/i })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /execute selected/i }));

    await waitFor(() => {
      expect(mockExecuteSqlStudioQuery).toHaveBeenCalledWith(selectedSql);
    });
  });

  it("executes via Cmd/Ctrl+Enter using the current selection when present", async () => {
    mockExecuteSqlStudioQuery.mockResolvedValue({
      status: "success",
      rows: [{ id: 1 }],
      schema: [
        { name: "id", dataType: "INT", index: 0, isPrimaryKey: true },
      ],
      tookMs: 5,
      rowCount: 1,
      logs: [],
    });

    renderSqlStudio();

    const fullSql = "SELECT id FROM default.events; SELECT name FROM default.events";
    const selectedSql = "SELECT name FROM default.events";

    fireEvent.change(getSqlEditor(), {
      target: { value: fullSql },
    });

    const editor = getSqlEditor() as HTMLTextAreaElement;
    const selectionStart = fullSql.indexOf(selectedSql);
    editor.setSelectionRange(selectionStart, selectionStart + selectedSql.length);
    fireEvent.select(editor);

    expect(latestEditorCommand).toBeTypeOf("function");

    await act(async () => {
      latestEditorCommand?.();
    });

    await waitFor(() => {
      expect(mockExecuteSqlStudioQuery).toHaveBeenCalledWith(selectedSql);
    });
  });

  it("starts a live subscription from the SQL Studio page and renders incoming change rows", async () => {
    renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.click(screen.getByRole("button", { name: /subscribe/i }));

    await waitFor(() => {
      expect(mockSubscribe).toHaveBeenCalledWith(
        "SELECT id, name FROM default.events",
        expect.any(Function),
        undefined,
      );
    });

    await act(async () => {
      liveCallback?.({
        type: "subscription_ack",
        schema: [
          { name: "id", data_type: "BigInt", index: 0, flags: ["pk"] },
          { name: "name", data_type: "Text", index: 1 },
        ],
      });
      liveCallback?.({
        type: "change",
        change_type: "insert",
        rows: [{ id: 7, name: "stream row" }],
      });
    });

    expect(await screen.findByText("stream row")).toBeTruthy();
    expect(screen.getByText("insert")).toBeTruthy();
    expect(screen.getByRole("button", { name: /stop/i })).toBeTruthy();
  });

  it("passes the live subscription from option as an exact string checkpoint", async () => {
    renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.click(screen.getByTitle("Subscription options"));
    fireEvent.change(screen.getByLabelText("from"), {
      target: { value: "9223372036854775807" },
    });
    fireEvent.click(screen.getByRole("button", { name: /subscribe/i }));

    await waitFor(() => {
      expect(mockSubscribe).toHaveBeenCalledWith(
        "SELECT id, name FROM default.events",
        expect.any(Function),
        expect.objectContaining({
          from: "9223372036854775807",
        }),
      );
    });
  });

  it("shows connecting state and lets the user cancel a stalled live subscription", async () => {
    let resolveSubscribe: ((value: () => Promise<void>) => void) | null = null;
    mockSubscribe.mockImplementationOnce(() => new Promise((resolve) => {
      resolveSubscribe = resolve as (value: () => Promise<void>) => void;
    }));

    renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT * FROM dba.favorites LIMIT 100" },
    });

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.click(screen.getByRole("button", { name: /subscribe/i }));

    expect(await screen.findByRole("button", { name: /connecting\.\.\./i })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: /connecting\.\.\./i }));

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /subscribe/i })).toBeTruthy();
    });

    await act(async () => {
      resolveSubscribe?.(mockUnsubscribe);
    });

    expect(mockUnsubscribe).toHaveBeenCalled();
  });

  it("orders live subscription rows by _seq descending, including the initial batch", async () => {
    renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name, _seq FROM default.events" },
    });

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.click(screen.getByRole("button", { name: /subscribe/i }));

    await waitFor(() => {
      expect(mockSubscribe).toHaveBeenCalled();
    });

    await act(async () => {
      liveCallback?.({
        type: "subscription_ack",
        schema: [
          { name: "id", data_type: "BigInt", index: 0, flags: ["pk"] },
          { name: "name", data_type: "Text", index: 1 },
          { name: "_seq", data_type: "BigInt", index: 2 },
        ],
      });
      liveCallback?.({
        type: "initial_data_batch",
        rows: [
          { id: 1, name: "oldest", _seq: "10" },
          { id: 2, name: "newest", _seq: "30" },
          { id: 3, name: "middle", _seq: "20" },
        ],
      });
    });

    const newestCell = await screen.findByText("newest");
    const middleCell = await screen.findByText("middle");
    const oldestCell = await screen.findByText("oldest");

    expect(newestCell.compareDocumentPosition(middleCell) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(middleCell.compareDocumentPosition(oldestCell) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("appends raw websocket send and receive traces to the log", async () => {
    const { store } = renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.click(screen.getByRole("button", { name: /subscribe/i }));

    await waitFor(() => {
      expect(mockSetClientSendListener).toHaveBeenCalled();
      expect(mockSetClientReceiveListener).toHaveBeenCalled();
    });

    await act(async () => {
      clientSendCallback?.('{"type":"subscribe","sql":"SELECT id, name FROM default.events"}');
      clientReceiveCallback?.('{"type":"subscription_ack","subscription_id":"sub-1"}');
    });

    await waitFor(() => {
      const tabResults = store.getState().sqlStudioWorkspace.tabResults;
      const activeResult = Object.values(tabResults).find((result) => result !== null);
      const messages = activeResult?.logs.map((entry) => entry.message) ?? [];
      expect(messages).toContain("WS SEND · subscribe");
      expect(messages).toContain("WS RECEIVE · subscription_ack");
    });
  });

  it("marks the live query as errored when the websocket disconnects", async () => {
    const { store } = renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.click(screen.getByRole("button", { name: /subscribe/i }));

    await waitFor(() => {
      expect(mockSetClientDisconnectListener).toHaveBeenCalled();
    });

    await act(async () => {
      liveCallback?.({
        type: "subscription_ack",
        schema: [
          { name: "id", data_type: "BigInt", index: 0, flags: ["pk"] },
          { name: "name", data_type: "Text", index: 1 },
        ],
      });
      clientDisconnectCallback?.({
        message: "Heartbeat timeout",
        code: 1000,
      });
    });

    await waitFor(() => {
      const state = store.getState().sqlStudioWorkspace;
      const activeTabId = state.activeTabId;
      expect(activeTabId).toBeTruthy();
      expect(state.tabs.find((tab) => tab.id === activeTabId)?.liveStatus).toBe("error");
    });
  });

  it("shows a change badge on background subscription tabs and clears it when the tab is opened", async () => {
    renderSqlStudio();

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.click(screen.getByRole("button", { name: /subscribe/i }));

    await waitFor(() => {
      expect(mockSubscribe).toHaveBeenCalled();
    });

    await act(async () => {
      liveCallback?.({
        type: "subscription_ack",
        schema: [
          { name: "id", data_type: "BigInt", index: 0, flags: ["pk"] },
          { name: "name", data_type: "Text", index: 1 },
        ],
      });
    });

    fireEvent.click(screen.getByTitle("New query tab"));

    await act(async () => {
      liveCallback?.({
        type: "change",
        change_type: "insert",
        rows: [{ id: 9, name: "background row" }],
      });
    });

    const firstTab = screen.getByRole("button", { name: /untitled query/i });
    expect(within(firstTab).getByText("1")).toBeTruthy();

    fireEvent.click(firstTab);

    await waitFor(() => {
      expect(within(firstTab).queryByText("1")).toBeNull();
    });
  });

  it("hydrates and updates the synced workspace from dba.favorites", async () => {
    renderSqlStudio();

    await act(async () => {
      syncedWorkspaceCallback?.({
        version: 1,
        tabs: [
          {
            id: "synced-tab",
            name: "Synced Query",
            query: "SELECT * FROM default.events",
            settings: {
              isDirty: false,
              isLive: false,
              liveStatus: "idle",
              resultView: "results",
              lastSavedAt: null,
              savedQueryId: "saved-1",
            },
          },
        ],
        savedQueries: [
          {
            id: "saved-1",
            title: "Favorite Query",
            sql: "SELECT * FROM default.events",
            lastSavedAt: "2026-03-27T00:00:00.000Z",
            isLive: false,
            openedRecently: true,
            isCurrentTab: true,
          },
        ],
        activeTabId: "synced-tab",
        updatedAt: "2026-03-27T00:00:00.000Z",
      });
      await Promise.resolve();
    });

    expect(screen.getByRole("button", { name: /synced query/i })).toBeTruthy();
    expect(screen.getByText("Favorite Query")).toBeTruthy();
    const favoritesTree = screen.getByText("Favorite Query").closest("button")?.parentElement?.parentElement;
    expect(favoritesTree?.className).toContain("border-l");

    await act(async () => {
      syncedWorkspaceCallback?.({
        version: 1,
        tabs: [
          {
            id: "synced-tab",
            name: "Synced Query Updated",
            query: "SELECT id FROM default.events",
            settings: {
              isDirty: false,
              isLive: false,
              liveStatus: "idle",
              resultView: "results",
              lastSavedAt: null,
              savedQueryId: "saved-1",
            },
          },
        ],
        savedQueries: [
          {
            id: "saved-1",
            title: "Favorite Query",
            sql: "SELECT id FROM default.events",
            lastSavedAt: "2026-03-27T00:00:00.000Z",
            isLive: false,
            openedRecently: true,
            isCurrentTab: true,
          },
        ],
        activeTabId: "synced-tab",
        updatedAt: "2026-03-27T00:00:01.000Z",
      });
      await Promise.resolve();
    });

    expect(screen.getByRole("button", { name: /synced query updated/i })).toBeTruthy();
  });

  it("persists favorites through the synced workspace payload", async () => {
    renderSqlStudio();

    await act(async () => {
      syncedWorkspaceCallback?.(null);
      await Promise.resolve();
    });

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });

    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));

    await waitFor(() => {
      expect(mockSaveSyncedSqlStudioWorkspaceState).toHaveBeenCalled();
    });

    expect(mockSaveSyncedSqlStudioWorkspaceState).toHaveBeenLastCalledWith(
      expect.objectContaining({
        savedQueries: [
          expect.objectContaining({
            title: "Untitled query",
            openedRecently: true,
            isCurrentTab: true,
          }),
        ],
      }),
      "root",
    );
  });

  it("keeps the current workspace when the page remounts", async () => {
    const store = createTestStore();
    const firstRender = renderSqlStudio(store);

    await act(async () => {
      syncedWorkspaceCallback?.(null);
      await Promise.resolve();
    });

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });
    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));

    await waitFor(() => {
      expect(screen.getAllByText("Untitled query").length).toBeGreaterThan(0);
    });

    firstRender.unmount();
    renderSqlStudio(store);

    expect(screen.getAllByText("Untitled query").length).toBeGreaterThan(0);
    expect(screen.getByDisplayValue("SELECT id, name FROM default.events")).toBeTruthy();
  });

  it("debounces synced workspace writes", async () => {
    vi.useFakeTimers();

    renderSqlStudio();

    await act(async () => {
      syncedWorkspaceCallback?.(null);
      await Promise.resolve();
    });

    fireEvent.change(getSqlEditor(), {
      target: { value: "SELECT id, name FROM default.events" },
    });
    fireEvent.click(screen.getByRole("button", { name: /^save$/i }));

    expect(mockSaveSyncedSqlStudioWorkspaceState).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(700);
    });

    expect(mockSaveSyncedSqlStudioWorkspaceState).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(50);
    });

    expect(mockSaveSyncedSqlStudioWorkspaceState).toHaveBeenCalled();
    vi.useRealTimers();
  });
});
