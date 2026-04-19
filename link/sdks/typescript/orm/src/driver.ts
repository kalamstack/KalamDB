import type { RemoteCallback } from 'drizzle-orm/pg-proxy';
import type { KalamDBClient } from '@kalamdb/client';

export function stripDefaults(sql: string, params: unknown[]): { sql: string; params: unknown[] } {
  const match = sql.match(/^(INSERT\s+INTO\s+\S+)\s*\(([^)]+)\)\s*VALUES\s*/i);
  if (!match) return { sql, params };

  const prefix = match[1];
  const columns = match[2].split(',').map((c) => c.trim());
  const valuesSql = sql.slice(match[0].length);

  const valueGroups: string[][] = [];
  const remaining = valuesSql.trim();
  const groupRegex = /\(([^)]+)\)/g;
  let groupMatch;
  while ((groupMatch = groupRegex.exec(remaining)) !== null) {
    valueGroups.push(groupMatch[1].split(',').map((v) => v.trim()));
  }
  if (valueGroups.length === 0) return { sql, params };

  const firstGroup = valueGroups[0];
  const keepIndices: number[] = [];
  for (let i = 0; i < firstGroup.length; i++) {
    if (firstGroup[i].toUpperCase() !== 'DEFAULT') keepIndices.push(i);
  }
  if (keepIndices.length === columns.length) return { sql, params };

  const newColumns = keepIndices.map((i) => columns[i]);
  const newParams: unknown[] = [];
  const newValueGroups = valueGroups.map((group) => {
    const vals = keepIndices.map((i) => {
      const val = group[i];
      const paramMatch = val.match(/^\$(\d+)$/);
      if (paramMatch) {
        newParams.push(params[parseInt(paramMatch[1]) - 1]);
        return `$${newParams.length}`;
      }
      return val;
    });
    return `(${vals.join(', ')})`;
  });

  return {
    sql: `${prefix} (${newColumns.join(', ')}) VALUES ${newValueGroups.join(', ')}`,
    params: newParams,
  };
}

export function splitMultiRowInsert(sql: string, params: unknown[]): { sql: string; params: unknown[] }[] {
  const match = sql.match(/^(INSERT\s+INTO\s+\S+\s*\([^)]+\)\s*VALUES\s*)/i);
  if (!match) return [{ sql, params }];

  const prefix = match[1];
  const valuesSql = sql.slice(match[0].length);
  const groups: { values: string; paramIndices: number[] }[] = [];
  const groupRegex = /\(([^)]+)\)/g;
  let groupMatch;
  while ((groupMatch = groupRegex.exec(valuesSql)) !== null) {
    const values = groupMatch[1];
    const indices: number[] = [];
    const paramRegex = /\$(\d+)/g;
    let paramMatch;
    while ((paramMatch = paramRegex.exec(values)) !== null) {
      indices.push(parseInt(paramMatch[1]) - 1);
    }
    groups.push({ values, paramIndices: indices });
  }

  if (groups.length <= 1) return [{ sql, params }];

  return groups.map((group) => {
    const newParams = group.paramIndices.map((i) => params[i]);
    const newValues = group.values.replace(/\$(\d+)/g, (_, num) => {
      const oldIndex = parseInt(num) - 1;
      const newIndex = group.paramIndices.indexOf(oldIndex);
      return `$${newIndex + 1}`;
    });
    return { sql: `${prefix}(${newValues})`, params: newParams };
  });
}

export function kalamDriver(client: KalamDBClient): RemoteCallback {
  return async (sql, params, method) => {
    let cleanSql = sql.replace(/"/g, '');
    const stripped = stripDefaults(cleanSql, params);
    cleanSql = stripped.sql;

    const statements = splitMultiRowInsert(cleanSql, stripped.params);
    if (statements.length > 1) {
      for (const stmt of statements) {
        await client.query(stmt.sql, stmt.params);
      }
      return { rows: [] };
    }

    const response = await client.query(cleanSql, stripped.params);
    if (method === 'execute') return { rows: [] };
    const result = response.results?.[0];
    const columns = result?.schema?.map((field: { name: string }) => field.name) ?? [];
    const rows = (result?.named_rows as Record<string, unknown>[] ?? []).map(
      (row) => columns.map((col) => row[col]),
    );
    return { rows };
  };
}
