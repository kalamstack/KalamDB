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

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function buildAuditLogsSelect(): string {
  return `
    SELECT audit_id, timestamp, actor_user_id, actor_username, action, target, details, ip_address
    FROM system.audit_log
  `;
}

function buildAuditLogsWhereClause(filters?: AuditLogFilters): string {
  const conditions: string[] = [];
  if (filters?.username) {
    conditions.push(`actor_username LIKE '%${escapeSqlLiteral(filters.username)}%'`);
  }
  if (filters?.action) {
    conditions.push(`action = '${escapeSqlLiteral(filters.action)}'`);
  }
  if (filters?.target) {
    conditions.push(`target LIKE '%${escapeSqlLiteral(filters.target)}%'`);
  }
  if (filters?.startDate) {
    conditions.push(`timestamp >= '${escapeSqlLiteral(filters.startDate)}'`);
  }
  if (filters?.endDate) {
    conditions.push(`timestamp <= '${escapeSqlLiteral(filters.endDate)}'`);
  }

  return conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
}

export function buildAuditLogsSubscriptionQuery(filters?: AuditLogFilters): string {
  return `${buildAuditLogsSelect()}${buildAuditLogsWhereClause(filters)}`;
}

const VALID_SORT_COLUMNS = new Set<AuditLogSortKey>([
  "timestamp", "actor_username", "action", "target", "ip_address",
]);

export function buildAuditLogsQuery(filters?: AuditLogFilters): string {
  let sql = buildAuditLogsSubscriptionQuery(filters);

  const sortCol = filters?.sortBy && VALID_SORT_COLUMNS.has(filters.sortBy)
    ? filters.sortBy
    : "timestamp";
  const sortDir = filters?.sortDirection === "asc" ? "ASC" : "DESC";
  sql += ` ORDER BY ${sortCol} ${sortDir}`;
  sql += ` LIMIT ${filters?.limit ?? 1000}`;
  if (filters?.offset) {
    sql += ` OFFSET ${filters.offset}`;
  }
  return sql;
}
