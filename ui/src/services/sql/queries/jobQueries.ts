export type JobSortKey = "created_at" | "job_type" | "status" | "started_at" | "finished_at" | "node_id";

export interface JobFilters {
  status?: string;
  job_type?: string;
  limit?: number;
  offset?: number;
  sortBy?: JobSortKey;
  sortDirection?: "asc" | "desc";
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

export function buildSystemJobsQuery(filters?: JobFilters): string {
  let sql = `
    SELECT job_id, job_type, status, parameters, message, 
           exception_trace, memory_used, cpu_used, created_at, started_at, 
           finished_at, node_id
    FROM system.jobs
  `;

  const conditions: string[] = [];
  if (filters?.status) {
    conditions.push(`status = '${escapeSqlLiteral(filters.status)}'`);
  }
  if (filters?.job_type) {
    conditions.push(`job_type = '${escapeSqlLiteral(filters.job_type)}'`);
  }
  if (conditions.length > 0) {
    sql += ` WHERE ${conditions.join(" AND ")}`;
  }

  const validSortColumns = new Set<JobSortKey>(["created_at", "job_type", "status", "started_at", "finished_at", "node_id"]);
  const sortCol = filters?.sortBy && validSortColumns.has(filters.sortBy) ? filters.sortBy : "created_at";
  const sortDir = filters?.sortDirection === "asc" ? "ASC" : "DESC";
  sql += ` ORDER BY ${sortCol} ${sortDir}`;
  sql += ` LIMIT ${filters?.limit ?? 1000}`;
  if (filters?.offset) {
    sql += ` OFFSET ${filters.offset}`;
  }
  return sql;
}
