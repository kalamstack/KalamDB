import { getDb } from "@/lib/db";
import { system_server_logs } from "@/lib/schema";
import { eq, like, desc, and, type SQL, type InferSelectModel } from "drizzle-orm";

export type ServerLog = InferSelectModel<typeof system_server_logs>;

export interface ServerLogFilters {
  level?: string;
  target?: string;
  message?: string;
  limit?: number;
}

export async function fetchServerLogs(filters?: ServerLogFilters) {
  const db = getDb();
  const conditions: SQL[] = [];

  if (filters?.level) {
    conditions.push(eq(system_server_logs.level, filters.level));
  }
  if (filters?.target) {
    conditions.push(like(system_server_logs.target, `%${filters.target}%`));
  }
  if (filters?.message) {
    conditions.push(like(system_server_logs.message, `%${filters.message}%`));
  }

  return db
    .select()
    .from(system_server_logs)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .orderBy(desc(system_server_logs.timestamp))
    .limit(filters?.limit ?? 500);
}
