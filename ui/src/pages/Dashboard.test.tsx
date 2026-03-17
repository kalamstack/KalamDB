// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import Dashboard from "@/pages/Dashboard";

const mockUseAuth = vi.fn();
const mockGetStatsQuery = vi.fn();
const mockGetDbaStatsQuery = vi.fn();
const mockGetStoragesQuery = vi.fn();
const mockCheckStorageHealth = vi.fn();
const mockCheckStorageHealthMutation = vi.fn();

vi.mock("@/lib/auth", () => ({
  useAuth: () => mockUseAuth(),
}));

vi.mock("@/store/apiSlice", () => ({
  useGetStatsQuery: () => mockGetStatsQuery(),
  useGetDbaStatsQuery: (timeRange: string) => mockGetDbaStatsQuery(timeRange),
  useGetStoragesQuery: () => mockGetStoragesQuery(),
  useCheckStorageHealthMutation: () => mockCheckStorageHealthMutation(),
}));

vi.mock("@/components/dashboard/MetricsChart", () => ({
  MetricsChart: ({ data }: { data: unknown[] }) => <div>Metrics chart {data.length}</div>,
}));

vi.mock("@/components/dashboard/StorageUsageChart", () => ({
  StorageUsageChart: ({ selectedStorageId }: { selectedStorageId: string }) => (
    <div>Storage usage {selectedStorageId}</div>
  ),
}));

describe("Dashboard page", () => {
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

    mockUseAuth.mockReset();
    mockGetStatsQuery.mockReset();
    mockGetDbaStatsQuery.mockReset();
    mockGetStoragesQuery.mockReset();
    mockCheckStorageHealth.mockReset();
    mockCheckStorageHealthMutation.mockReset();

    mockUseAuth.mockReturnValue({
      user: { username: "root", role: "system" },
    });

    mockGetStatsQuery.mockReturnValue({
      data: {
        server_version: "v1.2.3",
        total_namespaces: "9",
        total_tables: "42",
        active_connections: "3",
        active_subscriptions: "2",
        jobs_running: "1",
        jobs_queued: "4",
        total_storages: "2",
        server_uptime_human: "1h 10m",
      },
      isFetching: false,
      error: null,
      refetch: vi.fn().mockResolvedValue(undefined),
    });

    mockGetDbaStatsQuery.mockReturnValue({
      data: [{ sampled_at: 1, metric_name: "cpu_usage_percent", metric_value: 23 }],
      isFetching: false,
      refetch: vi.fn().mockResolvedValue(undefined),
    });

    mockGetStoragesQuery.mockReturnValue({
      data: [{ storage_id: "local", name: "Local" }],
      refetch: vi.fn().mockResolvedValue(undefined),
    });

    mockCheckStorageHealth.mockResolvedValue(undefined);
    mockCheckStorageHealthMutation.mockReturnValue([
      mockCheckStorageHealth,
      {
        data: { storage_id: "local", healthy: true },
        isLoading: false,
        error: null,
      },
    ]);
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    vi.unstubAllGlobals();
  });

  it("renders dashboard metrics from the admin stats queries", async () => {
    render(<Dashboard />);

    expect(screen.getByText("Dashboard")).toBeTruthy();
    expect(screen.getByText("Welcome back, root")).toBeTruthy();
    expect(screen.getByText("v1.2.3")).toBeTruthy();
    expect(screen.getByText("42")).toBeTruthy();
    expect(screen.getByText("9")).toBeTruthy();
    expect(screen.getByText("Metrics chart 1")).toBeTruthy();

    await waitFor(() => {
      expect(mockCheckStorageHealth).toHaveBeenCalledWith({ storageId: "local", extended: true });
    });
  });

  it("refreshes dashboard queries and rechecks storage health", async () => {
    const statsRefetch = vi.fn().mockResolvedValue(undefined);
    const dbaRefetch = vi.fn().mockResolvedValue(undefined);
    const storagesRefetch = vi.fn().mockResolvedValue(undefined);

    mockGetStatsQuery.mockReturnValue({
      data: {
        server_version: "v1.2.3",
        total_namespaces: "9",
        total_tables: "42",
        active_connections: "3",
        active_subscriptions: "2",
        jobs_running: "1",
        jobs_queued: "4",
        total_storages: "2",
        server_uptime_human: "1h 10m",
      },
      isFetching: false,
      error: null,
      refetch: statsRefetch,
    });
    mockGetDbaStatsQuery.mockReturnValue({
      data: [{ sampled_at: 1, metric_name: "cpu_usage_percent", metric_value: 23 }],
      isFetching: false,
      refetch: dbaRefetch,
    });
    mockGetStoragesQuery.mockReturnValue({
      data: [{ storage_id: "local", name: "Local" }],
      refetch: storagesRefetch,
    });

    render(<Dashboard />);

    await waitFor(() => {
      expect(mockCheckStorageHealth).toHaveBeenCalledTimes(1);
    });

    fireEvent.click(screen.getByRole("button", { name: /refresh/i }));

    await waitFor(() => {
      expect(statsRefetch).toHaveBeenCalledTimes(1);
      expect(dbaRefetch).toHaveBeenCalledTimes(1);
      expect(storagesRefetch).toHaveBeenCalledTimes(1);
      expect(mockCheckStorageHealth).toHaveBeenCalledTimes(2);
    });
  });
});
