import { executeSql } from "@/lib/kalam-client";
import { getDb } from "@/lib/db";
import { system_storages } from "@/lib/schema";
import type { InferSelectModel } from "drizzle-orm";
import {
  buildCreateStorageSql,
  buildStorageHealthCheckSql,
  buildUpdateStorageSql,
  type CreateStorageInput,
  type UpdateStorageInput,
} from "@/services/sql/queries/storageQueries";

export type Storage = InferSelectModel<typeof system_storages>;

export interface StorageHealthResult {
  storage_id: string;
  status: "healthy" | "degraded" | "unreachable";
  readable: boolean;
  writable: boolean;
  listable: boolean;
  deletable: boolean;
  latency_ms: number;
  total_bytes: number | null;
  used_bytes: number | null;
  error: string | null;
  tested_at: number | null;
}

export type { CreateStorageInput, UpdateStorageInput };

export async function fetchStorages() {
  const db = getDb();
  return db.select().from(system_storages);
}

export async function createStorage(input: CreateStorageInput): Promise<void> {
  await executeSql(buildCreateStorageSql(input));
}

export async function updateStorage(storageId: string, input: UpdateStorageInput): Promise<void> {
  const sql = buildUpdateStorageSql(storageId, input);
  if (!sql) {
    return;
  }
  await executeSql(sql);
}

export async function checkStorageHealth(storageId: string, extended = true): Promise<StorageHealthResult> {
  const rows = await executeSql(buildStorageHealthCheckSql(storageId, extended));
  if (!rows || rows.length === 0) {
    throw new Error("No health check result returned");
  }

  const row = rows[0];
  const toBool = (value: unknown): boolean =>
    value === true || value === "true" || value === 1 || value === "1";
  const toNumber = (value: unknown): number | null =>
    value === null || value === undefined ? null : Number(value);

  return {
    storage_id: String(row.storage_id ?? storageId),
    status: String(row.status ?? "unreachable") as StorageHealthResult["status"],
    readable: toBool(row.readable),
    writable: toBool(row.writable),
    listable: toBool(row.listable),
    deletable: toBool(row.deletable),
    latency_ms: Number(row.latency_ms ?? 0),
    total_bytes: toNumber(row.total_bytes),
    used_bytes: toNumber(row.used_bytes),
    error: row.error ? String(row.error) : null,
    tested_at: toNumber(row.tested_at),
  };
}
