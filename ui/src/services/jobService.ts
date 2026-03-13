import { executeSql } from "@/lib/kalam-client";
import { buildSystemJobsQuery, type JobFilters } from "@/services/sql/queries/jobQueries";

export interface Job {
  job_id: string;
  job_type: string;
  status: string;
  parameters: string | null;
  result: string | null;
  trace: string | null;
  error_message: string | null;
  memory_used: number | null;
  cpu_used: number | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  node_id: string;
}

export async function fetchJobs(filters?: JobFilters): Promise<Job[]> {
  const rows = await executeSql(buildSystemJobsQuery(filters));
  return rows.map((row) => ({
    job_id: String(row.job_id ?? ""),
    job_type: String(row.job_type ?? ""),
    status: String(row.status ?? ""),
    parameters: row.parameters as string | null,
    result: row.result as string | null,
    trace: row.trace as string | null,
    error_message: row.error_message as string | null,
    memory_used: row.memory_used as number | null,
    cpu_used: row.cpu_used as number | null,
    created_at: String(row.created_at ?? ""),
    started_at: row.started_at as string | null,
    finished_at: row.finished_at as string | null,
    node_id: String(row.node_id ?? ""),
  }));
}
