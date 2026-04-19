import type { KalamDBClient, QueryResponse } from '@kalamdb/client';

interface TableInfo {
  tableId: string;
  tableName: string;
  namespaceId: string;
  columns?: ColumnInfo[];
}

interface ColumnInfo {
  name: string;
  dataType: string;
  nullable: boolean;
  hasDefault?: boolean;
}

function mapKalamTypeToDrizzle(dataType: string): string {
  const normalized = dataType.toLowerCase();
  if (normalized.startsWith('timestamp')) return 'bigint';
  if (normalized === 'int64' || normalized === 'bigint') return 'text';
  if (normalized === 'int32' || normalized === 'int') return 'integer';
  if (normalized === 'float64' || normalized === 'double') return 'doublePrecision';
  if (normalized === 'boolean' || normalized === 'bool') return 'boolean';
  if (normalized === 'json' || normalized === 'jsonb') return 'jsonb';
  if (normalized === 'utf8' || normalized === 'text' || normalized === 'string') return 'text';
  if (normalized === 'file') return 'file';
  return 'text';
}

function toVariableName(namespaceId: string, tableName: string): string {
  return `${namespaceId}_${tableName}`;
}

function hasDefault(val: unknown): boolean {
  if (!val || val === 'None') return false;
  if (typeof val !== 'object' || val === null) return false;
  const obj = val as Record<string, unknown>;
  return 'FunctionCall' in obj || 'Literal' in obj;
}

function parseColumnsJson(columnsJson: string): ColumnInfo[] {
  try {
    const cols = JSON.parse(columnsJson) as { column_name: string; data_type: string; is_nullable: boolean; default_value?: unknown }[];
    return cols
      .map((col) => ({
        name: col.column_name,
        dataType: col.data_type,
        nullable: col.is_nullable,
        hasDefault: hasDefault(col.default_value),
      }))
      .filter((col) => !col.name.startsWith('_'));
  } catch {
    return [];
  }
}

async function fetchTables(client: KalamDBClient): Promise<TableInfo[]> {
  const response: QueryResponse = await client.query('SHOW TABLES');
  const rows = (response.results?.[0]?.named_rows ?? []) as Record<string, unknown>[];
  return rows.map((row) => ({
    tableId: String(row.table_id),
    tableName: String(row.table_name),
    namespaceId: String(row.namespace_id),
    columns: row.columns ? parseColumnsJson(String(row.columns)) : undefined,
  }));
}

async function fetchColumns(client: KalamDBClient, tableId: string): Promise<ColumnInfo[]> {
  const qualifiedName = tableId.replace(':', '.');
  const response: QueryResponse = await client.query(`DESCRIBE ${qualifiedName}`);
  const rows = (response.results?.[0]?.named_rows ?? []) as Record<string, unknown>[];
  return rows
    .map((row) => ({
      name: String(row.column_name),
      dataType: String(row.data_type),
      nullable: String(row.is_nullable) === 'YES',
    }))
    .filter((col) => !col.name.startsWith('_'));
}

function generateTableDefinition(table: TableInfo, columns: ColumnInfo[]): string {
  const varName = toVariableName(table.namespaceId, table.tableName);
  const qualifiedName = `${table.namespaceId}.${table.tableName}`;
  const lines: string[] = [];

  lines.push(`export const ${varName} = pgTable('${qualifiedName}', {`);

  for (const col of columns) {
    const drizzleType = mapKalamTypeToDrizzle(col.dataType);
    const fieldName = col.name;
    let def: string;
    if (drizzleType === 'bigint') {
      def = `  ${fieldName}: ${drizzleType}('${col.name}', { mode: 'number' })`;
    } else {
      def = `  ${fieldName}: ${drizzleType}('${col.name}')`;
    }
    if (col.hasDefault) {
      def += '.default(sql``)';
    }
    if (!col.nullable) {
      def += '.notNull()';
    }
    def += ',';
    lines.push(def);
  }

  lines.push('});');
  return lines.join('\n');
}

export interface GenerateOptions {
  includeSystem?: boolean;
}

const HIDDEN_TABLES = ['system.live', 'system.server_logs', 'system.cluster', 'system.settings', 'system.stats'];

export async function generateSchema(
  client: KalamDBClient,
  options?: GenerateOptions,
): Promise<string> {
  const tables = await fetchTables(client);

  for (const qualifiedName of HIDDEN_TABLES) {
    const [namespace, table] = qualifiedName.split('.');
    const tableId = `${namespace}:${table}`;
    if (tables.some((existing) => existing.tableId === tableId)) continue;
    try {
      if ((await fetchColumns(client, tableId)).length > 0)
        tables.push({ tableId, tableName: table, namespaceId: namespace });
    } catch {
      
    }
  }

  const filtered = tables.filter((t) => {
    if (!options?.includeSystem && (t.namespaceId === 'system' || t.namespaceId === 'dba')) {
      return false;
    }
    return true;
  });

  const drizzleImports = new Set<string>();
  const definitions: string[] = [];
  let hasDefaults = false;

  for (const table of filtered) {
    const columns = table.columns ?? await fetchColumns(client, table.tableId);
    for (const col of columns) {
      drizzleImports.add(mapKalamTypeToDrizzle(col.dataType));
      if (col.hasDefault) hasDefaults = true;
    }
    definitions.push(generateTableDefinition(table, columns));
  }

  drizzleImports.add('pgTable');

  const hasFile = drizzleImports.has('file');
  drizzleImports.delete('file');

  const pgImports = Array.from(drizzleImports).sort().join(', ');
  let header = `import { ${pgImports} } from 'drizzle-orm/pg-core';`;
  if (hasDefaults) {
    header += `\nimport { sql } from 'drizzle-orm';`;
  }
  if (hasFile) {
    header += `\nimport { file } from '@kalamdb/orm';`;
  }

  return `${header}\n\n${definitions.join('\n\n')}\n`;
}
