import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { ClusterHealth, ClusterNode } from "@/services/clusterService";
import {
  Activity,
  CheckCircle2,
  Crown,
  Loader2,
  Server,
  TriangleAlert,
} from "lucide-react";

interface DashboardClusterOverviewProps {
  health: ClusterHealth | null;
  nodes: ClusterNode[];
  isLoading?: boolean;
  error?: string | null;
}

const MAX_VISIBLE_NODES = 6;

function formatLabel(value: string): string {
  return value
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function formatMemory(memoryMb: number | null): string {
  if (memoryMb === null || !Number.isFinite(memoryMb)) {
    return "Memory n/a";
  }

  const precision = memoryMb >= 100 ? 0 : 1;
  return `${memoryMb.toFixed(precision)} MB memory`;
}

function formatCpu(cpuUsagePercent: string | number | null): string {
  if (cpuUsagePercent === null || !Number.isFinite(Number(cpuUsagePercent))) {
    return "CPU n/a";
  }

  return `${Number(cpuUsagePercent).toFixed(1)}% CPU`;
}

function formatUptime(uptimeHuman: string | null): string {
  return uptimeHuman ?? "Uptime n/a";
}

function getStatusBadgeClass(status: string): string {
  switch (status.toLowerCase()) {
    case "active":
      return "bg-green-100 text-green-800 hover:bg-green-100";
    case "offline":
      return "bg-red-100 text-red-800 hover:bg-red-100";
    case "joining":
      return "bg-amber-100 text-amber-800 hover:bg-amber-100";
    case "catching_up":
      return "bg-orange-100 text-orange-800 hover:bg-orange-100";
    default:
      return "bg-slate-100 text-slate-700 hover:bg-slate-100";
  }
}

function getRoleBadgeClass(role: string): string {
  switch (role.toLowerCase()) {
    case "leader":
      return "bg-blue-100 text-blue-800 hover:bg-blue-100";
    case "follower":
      return "bg-slate-100 text-slate-700 hover:bg-slate-100";
    case "learner":
      return "bg-violet-100 text-violet-800 hover:bg-violet-100";
    default:
      return "bg-slate-100 text-slate-700 hover:bg-slate-100";
  }
}

export function DashboardClusterOverview({
  health,
  nodes,
  isLoading,
  error,
}: DashboardClusterOverviewProps) {
  const visibleNodes = nodes.slice(0, MAX_VISIBLE_NODES);

  return (
    <Card className="h-full">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base font-medium">
          <Server className="h-4 w-4" />
          Cluster Nodes & Health
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          Live node health, leadership, and cluster availability.
        </p>
      </CardHeader>
      <CardContent>
        {error ? (
          <div className="rounded-lg border border-destructive/25 bg-destructive/5 p-4 text-sm text-destructive">
            {error}
          </div>
        ) : isLoading && nodes.length === 0 ? (
          <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            Loading cluster state...
          </div>
        ) : !health ? (
          <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">
            No cluster data available yet.
          </div>
        ) : (
          <div className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-2 2xl:grid-cols-4">
              <div className="rounded-lg border border-border/60 bg-muted/20 p-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">
                  <Activity className="h-3.5 w-3.5" />
                  Health
                </div>
                <p className="mt-2 text-2xl font-semibold">
                  {health.healthy ? "Healthy" : "Degraded"}
                </p>
                <p className="mt-1 text-xs text-muted-foreground">
                  {health.offlineNodes > 0
                    ? `${health.offlineNodes} offline`
                    : "All known nodes responding"}
                </p>
              </div>
              <div className="rounded-lg border border-border/60 bg-muted/20 p-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">
                  <Server className="h-3.5 w-3.5" />
                  Nodes
                </div>
                <p className="mt-2 text-2xl font-semibold">{health.totalNodes}</p>
                <p className="mt-1 text-xs text-muted-foreground">
                  {health.activeNodes} active, {health.joiningNodes} joining
                </p>
              </div>
              <div className="rounded-lg border border-border/60 bg-muted/20 p-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  Followers
                </div>
                <p className="mt-2 text-2xl font-semibold">{health.followerNodes}</p>
                <p className="mt-1 text-xs text-muted-foreground">
                  {health.catchingUpNodes} catching up
                </p>
              </div>
              <div className="rounded-lg border border-border/60 bg-muted/20 p-3">
                <div className="flex items-center gap-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">
                  <Crown className="h-3.5 w-3.5" />
                  Leaders
                </div>
                <p className="mt-2 text-2xl font-semibold">{health.leaderNodes}</p>
                <p className="mt-1 text-xs text-muted-foreground">
                  Expected leader nodes across the cluster
                </p>
              </div>
            </div>

            <div className="space-y-3">
              {visibleNodes.length === 0 ? (
                <div className="rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
                  No cluster nodes reported yet.
                </div>
              ) : (
                visibleNodes.map((node) => (
                  <div
                    key={node.node_id}
                    className="rounded-lg border border-border/60 bg-background p-3"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <p className="font-semibold">Node {node.node_id}</p>
                          {node.is_self && <Badge variant="outline">This node</Badge>}
                          {node.is_leader && (
                            <Badge className="bg-blue-100 text-blue-800 hover:bg-blue-100">
                              <Crown className="mr-1 h-3 w-3" />
                              Leader
                            </Badge>
                          )}
                        </div>
                        <p className="mt-1 truncate text-sm text-muted-foreground">
                          {node.api_addr || node.rpc_addr}
                        </p>
                      </div>
                      <div className="flex flex-wrap items-center justify-end gap-2">
                        <Badge className={getStatusBadgeClass(node.status)}>
                          {formatLabel(node.status)}
                        </Badge>
                        <Badge className={getRoleBadgeClass(node.role)}>
                          {formatLabel(node.role)}
                        </Badge>
                      </div>
                    </div>
                    <div className="mt-3 grid gap-2 text-xs text-muted-foreground sm:grid-cols-4">
                      <span>{formatMemory(node.memory_usage_mb ?? node.memory_mb)}</span>
                      <span>{formatCpu(node.cpu_usage_percent)}</span>
                      <span>{formatUptime(node.uptime_human)}</span>
                      <span>
                        {node.replication_lag === null
                          ? "Replication lag n/a"
                          : `Lag ${node.replication_lag.toLocaleString()}`}
                      </span>
                      <span className="truncate">
                        {node.hostname ?? node.rpc_addr ?? "Host unavailable"}
                      </span>
                    </div>
                  </div>
                ))
              )}

              {nodes.length > MAX_VISIBLE_NODES && (
                <div className="flex items-center gap-2 rounded-lg border border-border/60 bg-muted/20 p-3 text-xs text-muted-foreground">
                  <TriangleAlert className="h-3.5 w-3.5" />
                  Showing {MAX_VISIBLE_NODES} of {nodes.length} nodes. Open the Cluster page for
                  the full list.
                </div>
              )}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}