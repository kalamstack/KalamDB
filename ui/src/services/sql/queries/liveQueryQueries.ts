export interface LiveQueryFilters {
  user_id?: string;
  namespace_id?: string;
  table_name?: string;
  status?: string;
  limit?: number;
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function buildLiveQueriesSelect(): string {
  return `
    SELECT live_id, connection_id, subscription_id, namespace_id,
           table_name, user_id, query, options, status,
           created_at, last_update, changes, node_id
    FROM system.live
  `;
}

function buildLiveQueriesWhereClause(filters?: LiveQueryFilters): string {
  const conditions: string[] = [];
  if (filters?.user_id) {
    conditions.push(`user_id = '${escapeSqlLiteral(filters.user_id)}'`);
  }
  if (filters?.namespace_id) {
    conditions.push(`namespace_id = '${escapeSqlLiteral(filters.namespace_id)}'`);
  }
  if (filters?.table_name) {
    conditions.push(`table_name = '${escapeSqlLiteral(filters.table_name)}'`);
  }
  if (filters?.status) {
    conditions.push(`status = '${escapeSqlLiteral(filters.status)}'`);
  }

  return conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
}

export function buildLiveQueriesSubscriptionQuery(filters?: LiveQueryFilters): string {
  return `${buildLiveQueriesSelect()}${buildLiveQueriesWhereClause(filters)}`;
}

export function buildLiveQueriesQuery(filters?: LiveQueryFilters): string {
  let sql = buildLiveQueriesSubscriptionQuery(filters);

  sql += " ORDER BY created_at DESC";
  sql += ` LIMIT ${filters?.limit ?? 1000}`;
  return sql;
}

export function buildKillLiveQuerySql(liveId: string): string {
  return `KILL LIVE QUERY '${escapeSqlLiteral(liveId)}'`;
}
