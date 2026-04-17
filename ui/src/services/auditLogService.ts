import { executeSql } from "@/lib/kalam-client";
import {
  buildAuditLogsQuery,
  type AuditLogFilters,
  type AuditLogSortKey,
} from "@/services/sql/queries/auditLogQueries";

export interface AuditLog {
  audit_id: string;
  timestamp: string;
  actor_user_id: string;
  actor_username: string;
  action: string;
  target: string;
  details: string | null;
  ip_address: string | null;
}

export type { AuditLogFilters, AuditLogSortKey };

export async function fetchAuditLogs(filters?: AuditLogFilters): Promise<AuditLog[]> {
  const rows = await executeSql(buildAuditLogsQuery(filters));
  return rows.map((row) => ({
    audit_id: String(row.audit_id ?? ""),
    timestamp: String(row.timestamp ?? ""),
    actor_user_id: String(row.actor_user_id ?? ""),
    actor_username: String(row.actor_username ?? ""),
    action: String(row.action ?? ""),
    target: String(row.target ?? ""),
    details: row.details as string | null,
    ip_address: row.ip_address as string | null,
  }));
}
