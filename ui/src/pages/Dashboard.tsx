import { useEffect, useState } from "react";
import {
  Briefcase,
  Clock3,
  Database,
  FolderTree,
  HardDrive,
  RefreshCw,
  Server,
  Wifi,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DashboardClusterOverview } from "@/components/dashboard/ClusterOverview";
import { MetricsChart } from "@/components/dashboard/MetricsChart";
import { StorageUsageChart } from "@/components/dashboard/StorageUsageChart";
import { PageLayout } from "@/components/layout/PageLayout";
import { useAuth } from "@/lib/auth";
import {
  useCheckStorageHealthMutation,
  useGetClusterSnapshotQuery,
  useGetDbaStatsQuery,
  useGetStatsQuery,
  useGetStoragesQuery,
} from "@/store/apiSlice";

function parseInteger(value: string | undefined): number {
  if (!value) {
    return 0;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function formatUptime(seconds: string | undefined): string {
  const total = parseInteger(seconds);
  if (total <= 0) {
    return "-";
  }

  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const minutes = Math.floor((total % 3600) / 60);

  if (days > 0) {
    return `${days}d ${hours}h ${minutes}m`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }

  return `${minutes}m`;
}

export default function Dashboard() {
  const { user } = useAuth();
  const [timeRange, setTimeRange] = useState("24 HOURS");
  const [selectedStorageId, setSelectedStorageId] = useState("");

  const {
    data: stats = {},
    isFetching: isLoading,
    error,
    refetch: refetchStats,
  } = useGetStatsQuery();
  const {
    data: dbaStats = [],
    isFetching: isDbaStatsLoading,
    refetch: refetchDbaStats,
  } = useGetDbaStatsQuery(timeRange);
  const {
    data: storages = [],
    refetch: refetchStorages,
  } = useGetStoragesQuery();
  const {
    data: clusterSnapshot,
    isFetching: isClusterLoading,
    error: clusterError,
    refetch: refetchCluster,
  } = useGetClusterSnapshotQuery(undefined, {
    pollingInterval: 5000,
  });
  const [checkStorageHealth, { data: storageHealth, isLoading: isStorageHealthLoading, error: storageHealthError }] =
    useCheckStorageHealthMutation();

  useEffect(() => {
    if (!selectedStorageId && storages.length > 0) {
      setSelectedStorageId(storages[0].storage_id);
    }
  }, [selectedStorageId, storages]);

  useEffect(() => {
    if (!selectedStorageId) {
      return;
    }

    void checkStorageHealth({ storageId: selectedStorageId, extended: true });
  }, [checkStorageHealth, selectedStorageId]);

  async function handleRefresh(): Promise<void> {
    await Promise.all([refetchStats(), refetchDbaStats(), refetchStorages(), refetchCluster()]);

    if (selectedStorageId) {
      await checkStorageHealth({ storageId: selectedStorageId, extended: true });
    }
  }

  const clusterErrorMessage =
    clusterError && "error" in clusterError && typeof clusterError.error === "string"
      ? clusterError.error
      : clusterError
        ? "Failed to fetch cluster information"
        : null;

  const cards = [
    {
      title: "Version",
      value: stats.server_version || "v0.1.1",
      subtitle: "Server build",
      icon: Server,
    },
    {
      title: "Uptime",
      value: stats.server_uptime_human || formatUptime(stats.server_uptime_seconds),
      subtitle: "Current process lifetime",
      icon: Clock3,
    },
    {
      title: "Namespaces",
      value: parseInteger(stats.total_namespaces).toLocaleString(),
      subtitle: "Logical boundaries",
      icon: FolderTree,
    },
    {
      title: "Tables",
      value: parseInteger(stats.total_tables).toLocaleString(),
      subtitle: "Across all namespaces",
      icon: Database,
    },
    {
      title: "Connections",
      value: parseInteger(stats.active_connections).toLocaleString(),
      subtitle: "Active sessions",
      icon: Wifi,
    },
    {
      title: "Subscriptions",
      value: parseInteger(stats.active_subscriptions).toLocaleString(),
      subtitle: "Live query listeners",
      icon: Wifi,
    },
    {
      title: "Running Jobs",
      value: parseInteger(stats.jobs_running).toLocaleString(),
      subtitle: "Active background work",
      icon: Briefcase,
    },
    {
      title: "Queued Jobs",
      value: parseInteger(stats.jobs_queued).toLocaleString(),
      subtitle: "Pending execution",
      icon: Briefcase,
    },
    {
      title: "Storages",
      value: parseInteger(stats.total_storages).toLocaleString(),
      subtitle: "Configured backends",
      icon: HardDrive,
    },
  ];

  return (
    <PageLayout
      title="Dashboard"
      description={`Welcome back, ${user?.username ?? "admin"}`}
      actions={
        <div className="flex items-center gap-3">
          <Select value={timeRange} onValueChange={setTimeRange}>
            <SelectTrigger className="h-9 w-[140px]">
              <SelectValue placeholder="Time range" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="1 HOURS">Last 1 Hour</SelectItem>
              <SelectItem value="6 HOURS">Last 6 Hours</SelectItem>
              <SelectItem value="24 HOURS">Last 24 Hours</SelectItem>
              <SelectItem value="7 DAYS">Last 7 Days</SelectItem>
            </SelectContent>
          </Select>
          <Button
            variant="outline"
            size="sm"
            onClick={() => void handleRefresh()}
            disabled={isLoading || isDbaStatsLoading || isStorageHealthLoading}
          >
            <RefreshCw className={`mr-1.5 h-4 w-4 ${isLoading || isDbaStatsLoading || isStorageHealthLoading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>
      }
    >
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {cards.map((card) => (
          <Card key={card.title}>
            <CardContent className="pt-4">
              <div className="mb-2 flex items-center justify-between">
                <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">{card.title}</p>
                <card.icon className="h-4 w-4 text-muted-foreground" />
              </div>
              <p className="text-2xl font-semibold">{card.value}</p>
              <p className="text-xs text-muted-foreground">{card.subtitle}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      <MetricsChart data={dbaStats} isLoading={isDbaStatsLoading} />

      <div className="mt-6 grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(340px,0.95fr)]">
        <StorageUsageChart
          storages={storages}
          selectedStorageId={selectedStorageId}
          onStorageChange={setSelectedStorageId}
          health={storageHealth ?? null}
          isLoading={isStorageHealthLoading}
          error={storageHealthError && "error" in storageHealthError ? storageHealthError.error : null}
        />

        <DashboardClusterOverview
          health={clusterSnapshot?.health ?? null}
          nodes={clusterSnapshot?.nodes ?? []}
          isLoading={isClusterLoading}
          error={clusterErrorMessage}
        />
      </div>

      {error && (
        <Card className="border-destructive/30 bg-destructive/5">
          <CardContent className="pt-4 text-sm text-destructive">
            {"error" in error ? error.error : "Failed to fetch dashboard stats"}
          </CardContent>
        </Card>
      )}
    </PageLayout>
  );
}
