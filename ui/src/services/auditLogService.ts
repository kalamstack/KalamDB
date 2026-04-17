import { getDb } from "@/lib/db";
import { system_audit_log } from "@/lib/schema";
import { like, eq, gte, lte, asc, desc, and, type SQL, type InferSelectModel } from "drizzle-orm";

export type AuditLog = InferSelectModel<typeof system_audit_log>;

export type AuditLogSortKey = "timestamp" | "actor_username" | "action" | "target" | "ip_address";

export interface AuditLogFilters {
  username?: string;
  action?: string;
  target?: string;
  startDate?: string;
  endDate?: string;
  limit?: number;
  offset?: number;
  sortBy?: AuditLogSortKey;
  sortDirection?: "asc" | "desc";
}

const sortColumnMap = {
  timestamp: system_audit_log.timestamp,
  actor_username: system_audit_log.actor_username,
  action: system_audit_log.action,
  target: system_audit_log.target,
  ip_address: system_audit_log.ip_address,
} as const;

export async function fetchAuditLogs(filters?: AuditLogFilters) {
  const db = getDb();
  const conditions: SQL[] = [];

  if (filters?.username) {
    conditions.push(like(system_audit_log.actor_username, `%${filters.username}%`));
  }
  if (filters?.action) {
    conditions.push(eq(system_audit_log.action, filters.action));
  }
  if (filters?.target) {
    conditions.push(like(system_audit_log.target, `%${filters.target}%`));
  }
  if (filters?.startDate) {
    const startMs = new Date(filters.startDate).getTime();
    if (!isNaN(startMs)) conditions.push(gte(system_audit_log.timestamp, startMs));
  }
  if (filters?.endDate) {
    const endMs = new Date(filters.endDate).getTime();
    if (!isNaN(endMs)) conditions.push(lte(system_audit_log.timestamp, endMs));
  }

  const sortCol = sortColumnMap[filters?.sortBy ?? "timestamp"];
  const sortDir = filters?.sortDirection === "asc" ? asc : desc;

  return db
    .select()
    .from(system_audit_log)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .orderBy(sortDir(sortCol))
    .limit(filters?.limit ?? 1000)
    .offset(filters?.offset ?? 0);
}
