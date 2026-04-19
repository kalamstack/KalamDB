import { drizzle } from 'drizzle-orm/pg-proxy';
import { kalamDriver } from '@kalamdb/orm';
import { getClient } from './kalam-client';

let db: ReturnType<typeof drizzle> | null = null;

export function getDb() {
  if (!db) {
    const client = getClient();
    if (!client) throw new Error('KalamDB client not initialized');
    db = drizzle(kalamDriver(client));
  }
  return db;
}
