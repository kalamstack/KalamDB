import { getDb } from "@/lib/db";
import { system_cluster } from "@/lib/schema";
import type { InferSelectModel } from "drizzle-orm";

export type ClusterNode = InferSelectModel<typeof system_cluster>;

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
  const db = getDb();
  const nodes = await db.select().from(system_cluster);

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
