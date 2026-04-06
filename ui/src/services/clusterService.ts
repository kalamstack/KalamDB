import { executeSql } from "@/lib/kalam-client";
import { SYSTEM_CLUSTER_QUERY } from "@/services/sql/queries/clusterQueries";

export interface ClusterNode {
  cluster_id: string;
  node_id: number;
  role: string;
  status: string;
  rpc_addr: string;
  api_addr: string;
  is_self: boolean;
  is_leader: boolean;
  groups_leading: number;
  total_groups: number;
  current_term: number | null;
  last_applied_log: number | null;
  leader_last_log_index: number | null;
  snapshot_index: number | null;
  catchup_progress_pct: number | null;
  replication_lag: number | null;
  hostname: string | null;
  version: string | null;
  memory_mb: number | null;
  memory_usage_mb: number | null;
  cpu_usage_percent: number | null;
  uptime_seconds: number | null;
  uptime_human: string | null;
  os: string | null;
  arch: string | null;
}

export interface ClusterHealth {
  healthy: boolean;
  totalNodes: number;
  activeNodes: number;
  offlineNodes: number;
  leaderNodes: number;
  followerNodes: number;
  joiningNodes: number;
  catchingUpNodes: number;
}

export interface ClusterSnapshot {
  nodes: ClusterNode[];
  health: ClusterHealth;
}

export async function fetchClusterSnapshot(): Promise<ClusterSnapshot> {
  const rows = await executeSql(SYSTEM_CLUSTER_QUERY);
  const nodes = rows.map((row) => ({
    cluster_id: String(row.cluster_id ?? ""),
    node_id: Number(row.node_id ?? 0),
    role: String(row.role ?? ""),
    status: String(row.status ?? ""),
    rpc_addr: String(row.rpc_addr ?? ""),
    api_addr: String(row.api_addr ?? ""),
    is_self: Boolean(row.is_self),
    is_leader: Boolean(row.is_leader),
    groups_leading: Number(row.groups_leading ?? 0),
    total_groups: Number(row.total_groups ?? 0),
    current_term: row.current_term === null ? null : Number(row.current_term),
    last_applied_log: row.last_applied_log === null ? null : Number(row.last_applied_log),
    leader_last_log_index: row.leader_last_log_index === null ? null : Number(row.leader_last_log_index),
    snapshot_index: row.snapshot_index === null ? null : Number(row.snapshot_index),
    catchup_progress_pct: row.catchup_progress_pct === null ? null : Number(row.catchup_progress_pct),
    replication_lag: row.replication_lag === null ? null : Number(row.replication_lag),
    hostname: row.hostname === null ? null : String(row.hostname),
    version: row.version === null ? null : String(row.version),
    memory_mb: row.memory_mb === null ? null : Number(row.memory_mb),
    memory_usage_mb: row.memory_usage_mb === null ? null : Number(row.memory_usage_mb),
    cpu_usage_percent: row.cpu_usage_percent === null ? null : Number(row.cpu_usage_percent),
    uptime_seconds: row.uptime_seconds === null ? null : Number(row.uptime_seconds),
    uptime_human: row.uptime_human === null ? null : String(row.uptime_human),
    os: row.os === null ? null : String(row.os),
    arch: row.arch === null ? null : String(row.arch),
  }));

  const totalNodes = nodes.length;
  const activeNodes = nodes.filter((node) => node.status === "active").length;
  const offlineNodes = nodes.filter((node) => node.status === "offline").length;
  const leaderNodes = nodes.filter((node) => node.role === "leader").length;
  const followerNodes = nodes.filter((node) => node.role === "follower").length;
  const joiningNodes = nodes.filter((node) => node.status === "joining").length;
  const catchingUpNodes = nodes.filter((node) => node.status === "catching_up").length;

  return {
    nodes,
    health: {
      healthy: offlineNodes === 0 && joiningNodes === 0,
      totalNodes,
      activeNodes,
      offlineNodes,
      leaderNodes,
      followerNodes,
      joiningNodes,
      catchingUpNodes,
    },
  };
}
