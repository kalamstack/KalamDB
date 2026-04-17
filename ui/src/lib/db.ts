import { drizzle } from 'drizzle-orm/pg-proxy';
import { getClient } from './kalam-client';

let db: ReturnType<typeof createDb> | null = null;

function createDb() {
  const client = getClient();
  if (!client) throw new Error('KalamDB client not initialized');

  return drizzle(async (sql, params, method) => {
    const response = await client.query(sql.replace(/"/g, ''), params);
    if (method === 'execute') return { rows: [] };
    const result = response.results?.[0];
    const columns = result?.schema?.map((field: { name: string }) => field.name) ?? [];
    const rows = (result?.named_rows as Record<string, unknown>[] ?? []).map(
      (row) => columns.map((col) => row[col]),
    );
    return { rows };
  });
}

export function getDb() {
  if (!db) db = createDb();
  return db;
}
