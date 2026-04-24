import { beforeEach, describe, expect, it, vi } from "vitest";

const mockExecuteSql = vi.fn();
const mockSubscribeRows = vi.fn();

vi.mock("@/lib/kalam-client", () => ({
  executeSql: (...args: unknown[]) => mockExecuteSql(...args),
  subscribeRows: (...args: unknown[]) => mockSubscribeRows(...args),
}));

describe("sqlStudioWorkspaceSyncService", () => {
  beforeEach(() => {
    vi.resetModules();
    mockExecuteSql.mockReset();
    mockSubscribeRows.mockReset();
  });

  it("hydrates legacy favorites before opening the live subscription", async () => {
    const workspace = {
      version: 1,
      tabs: [
        {
          id: "tab-1",
          name: "Favorite Query",
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
          lastSavedAt: "2026-04-23T00:00:00.000Z",
          isLive: false,
          openedRecently: true,
          isCurrentTab: true,
        },
      ],
      activeTabId: "tab-1",
      updatedAt: "2026-04-23T00:00:00.000Z",
    };

    mockExecuteSql
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ id: "sql-studio-workspace", payload: workspace }])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    mockSubscribeRows.mockResolvedValue(async () => {});

    const { subscribeToSyncedSqlStudioWorkspaceState } = await import("@/services/sqlStudioWorkspaceSyncService");
    const onChange = vi.fn();

    await subscribeToSyncedSqlStudioWorkspaceState("admin", onChange);

    expect(onChange).toHaveBeenCalledWith(workspace);
    expect(mockSubscribeRows).toHaveBeenCalledWith(
      "SELECT id, payload FROM dba.favorites WHERE id = 'sql-studio-state:admin:workspace'",
      expect.any(Function),
    );
    expect(mockExecuteSql).toHaveBeenCalledWith(
      "INSERT INTO dba.favorites (id, payload) VALUES ('sql-studio-state:admin:workspace', '{\"version\":1,\"tabs\":[{\"id\":\"tab-1\",\"name\":\"Favorite Query\",\"query\":\"SELECT * FROM default.events\",\"settings\":{\"isDirty\":false,\"isLive\":false,\"liveStatus\":\"idle\",\"resultView\":\"results\",\"lastSavedAt\":null,\"savedQueryId\":\"saved-1\"}}],\"savedQueries\":[{\"id\":\"saved-1\",\"title\":\"Favorite Query\",\"sql\":\"SELECT * FROM default.events\",\"lastSavedAt\":\"2026-04-23T00:00:00.000Z\",\"isLive\":false,\"openedRecently\":true,\"isCurrentTab\":true}],\"activeTabId\":\"tab-1\",\"updatedAt\":\"2026-04-23T00:00:00.000Z\"}')",
    );
  });
});