import { executeSql } from "@/lib/kalam-client";
import { getDb } from "@/lib/db";
import { system_live } from "@/lib/schema";
import { eq, desc, and, type SQL, type InferSelectModel } from "drizzle-orm";
import { buildKillLiveQuerySql } from "@/services/sql/queries/liveQueryQueries";

export type LiveQuery = InferSelectModel<typeof system_live>;

export interface LiveQueryFilters {
  user_id?: string;
  namespace_id?: string;
  table_name?: string;
  status?: string;
  limit?: number;
}

export async function fetchLiveQueries(filters?: LiveQueryFilters) {
  const db = getDb();
  const conditions: SQL[] = [];

  if (filters?.user_id) {
    conditions.push(eq(system_live.user_id, filters.user_id));
  }
  if (filters?.namespace_id) {
    conditions.push(eq(system_live.namespace_id, filters.namespace_id));
  }
  if (filters?.table_name) {
    conditions.push(eq(system_live.table_name, filters.table_name));
  }
  if (filters?.status) {
    conditions.push(eq(system_live.status, filters.status));
  }

  return db
    .select()
    .from(system_live)
    .where(conditions.length > 0 ? and(...conditions) : undefined)
    .orderBy(desc(system_live.created_at))
    .limit(filters?.limit ?? 1000);
}

export async function killLiveQuery(liveId: string): Promise<void> {
  await executeSql(buildKillLiveQuerySql(liveId));
}
