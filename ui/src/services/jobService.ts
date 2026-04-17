import { getDb } from "@/lib/db";
import { system_jobs } from "@/lib/schema";
import { eq, asc, desc, and, type SQL, type InferSelectModel } from "drizzle-orm";

export type Job = InferSelectModel<typeof system_jobs>;

export type JobSortKey = "created_at" | "job_type" | "status" | "started_at" | "finished_at" | "node_id";

export interface JobFilters {
  status?: string;
  job_type?: string;
  limit?: number;
  offset?: number;
  sortBy?: JobSortKey;
  sortDirection?: "asc" | "desc";
}

const sortColumnMap = {
  created_at: system_jobs.created_at,
  job_type: system_jobs.job_type,
  status: system_jobs.status,
  started_at: system_jobs.started_at,
  finished_at: system_jobs.finished_at,
  node_id: system_jobs.node_id,
} as const;

export async function fetchJobs(filters?: JobFilters) {
  const db = getDb();
  const conditions: SQL[] = [];

  if (filters?.status) {
    conditions.push(eq(system_jobs.status, filters.status));
  }
  if (filters?.job_type) {
    conditions.push(eq(system_jobs.job_type, filters.job_type));
  }

  const sortCol = sortColumnMap[filters?.sortBy ?? "created_at"];
  const sortDir = filters?.sortDirection === "asc" ? asc : desc;

  return db
    .select()
    .from(system_jobs)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .orderBy(sortDir(sortCol))
    .limit(filters?.limit ?? 1000)
    .offset(filters?.offset ?? 0);
}
