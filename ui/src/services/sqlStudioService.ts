import { executeQuery, executeSql } from "@/lib/kalam-client";
import type { SchemaField } from "@kalamdb/client";
import type {
  QueryLogEntry,
  QueryResultData,
  QueryResultSchemaField,
  StudioNamespace,
  StudioTable,
} from "@/components/sql-studio-v2/shared/types";

const MAX_SQL_STUDIO_RENDER_ROWS = 1000;

interface RawSqlStatementResult {
  schema?: SchemaField[];
  rows?: unknown[][];
  /** Pre-computed named rows from Rust WASM (schema → map transformation). */
  named_rows?: Record<string, unknown>[];
  row_count?: number;
  message?: string;
  as_user?: string;
}

export type RawQuerySchemaField = SchemaField;

function formatSchemaDataType(dataType: SchemaField["data_type"]): string {
  if (typeof dataType === "string") {
    return dataType;
  }

  if (dataType && typeof dataType === "object") {
    const entries = Object.entries(dataType as Record<string, unknown>);
    const [variant, value] = entries[0] ?? [];
    if (!variant) {
      return "Unknown";
    }

    if (typeof value === "number" || typeof value === "string") {
      return `${variant}(${value})`;
    }

    return variant;
  }

  return "Unknown";
}

type FieldFlags = Array<"pk" | "nn" | "uq">;

function isPrimaryKeyFlag(flags: FieldFlags | undefined): boolean {
  if (!flags || flags.length === 0) {
    return false;
  }

  return flags.includes("pk");
}

export function normalizeSchema(
  rawSchema: SchemaField[] | undefined,
): QueryResultSchemaField[] {
  if (!rawSchema) {
    return [];
  }

  return rawSchema.map((field) => ({
    name: field.name,
    dataType: formatSchemaDataType(field.data_type),
    index: field.index,
    flags: field.flags,
    isPrimaryKey: isPrimaryKeyFlag(field.flags),
  }));
}

function rowsToObjects(
  schema: QueryResultSchemaField[],
  rows: unknown[][] | undefined,
  namedRows?: Record<string, unknown>[],
): Record<string, unknown>[] {
  const normalizeValue = (value: unknown): unknown => {
    if (Array.isArray(value)) {
      return value.map((entry) => normalizeValue(entry));
    }

    if (value && typeof value === "object") {
      const maybeSerializable = value as { toJson?: () => unknown };
      if (typeof maybeSerializable.toJson === "function") {
        try {
          return normalizeValue(maybeSerializable.toJson());
        } catch {
          // Fall through to entry-wise normalization.
        }
      }

      return Object.fromEntries(
        Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
          key,
          normalizeValue(entry),
        ]),
      );
    }

    return value;
  };

  // Prefer named_rows: Rust WASM pre-computes the schema→map transformation.
  if (namedRows && namedRows.length > 0) {
    return namedRows.slice(0, MAX_SQL_STUDIO_RENDER_ROWS).map((row) => {
      const item: Record<string, unknown> = {};
      for (const key of Object.keys(row)) {
        item[key] = normalizeValue(row[key] ?? null);
      }
      return item;
    });
  }

  // Fallback: positional rows + schema (older server versions)
  if (!rows || schema.length === 0) {
    return [];
  }

  const rowsToRender = rows.slice(0, MAX_SQL_STUDIO_RENDER_ROWS);
  return rowsToRender.map((row) => {
    const item: Record<string, unknown> = {};
    schema.forEach((field) => {
      item[field.name] = normalizeValue(row[field.index] ?? null);
    });
    return item;
  });
}

function toQueryLogEntry(
  result: RawSqlStatementResult,
  statementIndex: number,
  createdAt: string,
): QueryLogEntry {
  const rowCount = typeof result.row_count === "number" ? result.row_count : 0;
  const explicitMessage = typeof result.message === "string" ? result.message.trim() : "";

  let message = explicitMessage;
  if (!message) {
    if (Array.isArray(result.schema) && result.schema.length > 0) {
      message = `Statement ${statementIndex + 1} returned ${rowCount} row${rowCount === 1 ? "" : "s"}.`;
    } else {
      message = `Statement ${statementIndex + 1} executed successfully.`;
    }
  }

  const asUser = typeof result.as_user === "string" ? result.as_user : undefined;

  return {
    id: `${createdAt}-stmt-${statementIndex}`,
    level: "info",
    message,
    response: result,
    asUser,
    rowCount,
    statementIndex,
    createdAt,
  };
}

function buildQueryLogs(statementResults: RawSqlStatementResult[] | undefined): QueryLogEntry[] {
  if (!statementResults || statementResults.length === 0) {
    return [];
  }

  const createdAt = new Date().toISOString();
  return statementResults.map((result, index) => toQueryLogEntry(result, index, createdAt));
}

function hasTabularPayload(result: RawSqlStatementResult): boolean {
  return (
    Array.isArray(result.schema) &&
    result.schema.length > 0 &&
    (Array.isArray(result.named_rows) || Array.isArray(result.rows))
  );
}

function ensureNamespace(
  namespaces: Map<string, StudioNamespace>,
  databaseName: string,
  namespaceName: string,
): StudioNamespace {
  let namespace = namespaces.get(namespaceName);
  if (!namespace) {
    namespace = {
      database: databaseName,
      name: namespaceName,
      tables: [],
    };
    namespaces.set(namespaceName, namespace);
  }
  return namespace;
}

function ensureTable(
  namespaces: Map<string, StudioNamespace>,
  tableMap: Map<string, StudioTable>,
  databaseName: string,
  namespaceName: string,
  tableName: string,
  tableType: string,
): StudioTable {
  const tableKey = `${namespaceName}.${tableName}`;
  let table = tableMap.get(tableKey);

  if (!table) {
    table = {
      database: databaseName,
      namespace: namespaceName,
      name: tableName,
      tableType,
      columns: [],
      storageId: null,
      version: null,
      options: null,
      comment: null,
      updatedAt: null,
      createdAt: null,
    };
    tableMap.set(tableKey, table);
    ensureNamespace(namespaces, databaseName, namespaceName).tables.push(table);
  }

  return table;
}

function normalizeNullable(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "yes" || normalized === "true" || normalized === "1") {
      return true;
    }
    if (normalized === "no" || normalized === "false" || normalized === "0") {
      return false;
    }
  }

  return Boolean(value);
}

function normalizeTextValue(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return null;
}

function normalizeNumericValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }

    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function normalizeStructuredValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => normalizeStructuredValue(entry));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
        key,
        normalizeStructuredValue(entry),
      ]),
    );
  }

  return value;
}

function normalizeTableOptions(value: unknown): Record<string, unknown> | null {
  if (value == null) {
    return null;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }

    try {
      return normalizeTableOptions(JSON.parse(trimmed) as unknown);
    } catch {
      return { value: trimmed };
    }
  }

  if (typeof value !== "object" || Array.isArray(value)) {
    return { value: normalizeStructuredValue(value) };
  }

  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, entry]) => [
      key,
      normalizeStructuredValue(entry),
    ]),
  );
}

function normalizeTimestampValue(value: unknown): string | number | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }

  return null;
}

function applyTableMetadata(table: StudioTable, row: Record<string, unknown>): void {
  table.storageId = normalizeTextValue(row.storage_id);
  table.version = normalizeNumericValue(row.version);
  table.options = normalizeTableOptions(row.options);
  table.comment = normalizeTextValue(row.comment);
  table.updatedAt = normalizeTimestampValue(row.updated_at);
  table.createdAt = normalizeTimestampValue(row.created_at);
}

function inferExplorerTableType(namespaceName: string, tableType: unknown): string {
  const normalizedNamespace = namespaceName.trim().toLowerCase();
  if (
    normalizedNamespace === "system" ||
    normalizedNamespace === "information_schema" ||
    normalizedNamespace === "pg_catalog" ||
    normalizedNamespace === "datafusion"
  ) {
    return "system";
  }

  const normalizedTableType = String(tableType ?? "").trim().toLowerCase();
  if (normalizedTableType === "stream" || normalizedTableType === "shared" || normalizedTableType === "user") {
    return normalizedTableType;
  }
  if (normalizedTableType.includes("view")) {
    return "view";
  }

  return "user";
}

export async function fetchSqlStudioSchemaTree(): Promise<StudioNamespace[]> {
  const databaseName = "database";

  const namespacesResult = await executeSql(`
    SELECT namespace_id
    FROM system.namespaces
    ORDER BY namespace_id
  `);

  const namespaces = new Map<string, StudioNamespace>();

  namespacesResult.forEach((row) => {
    const namespaceName = String(row.namespace_id ?? "");
    if (!namespaceName) {
      return;
    }

    ensureNamespace(namespaces, databaseName, namespaceName);
  });

  const tableAndColumnsResult = await executeSql(`
    SELECT
      t.namespace_id,
      t.table_name,
      t.table_type,
      t.storage_id,
      t.version,
      t.options,
      t.comment,
      t.updated_at,
      t.created_at,
      c.column_name,
      c.data_type,
      c.nullable,
      c.primary_key,
      c.ordinal
    FROM system.tables t
    LEFT JOIN system.columns c
      ON t.namespace_id = c.namespace_id
      AND t.table_name = c.table_name
    ORDER BY t.namespace_id, t.table_name, c.ordinal
  `);

  const tableMap = new Map<string, StudioTable>();

  tableAndColumnsResult.forEach((row) => {
    const namespaceName = String(row.namespace_id ?? "");
    const tableName = String(row.table_name ?? "");
    if (!namespaceName || !tableName) {
      return;
    }

    const table = ensureTable(
      namespaces,
      tableMap,
      databaseName,
      namespaceName,
      tableName,
      inferExplorerTableType(namespaceName, row.table_type),
    );

    applyTableMetadata(table, row);

    if (row.column_name) {
      table.columns.push({
        name: String(row.column_name),
        dataType: String(row.data_type ?? "unknown"),
        isNullable: normalizeNullable(row.nullable),
        isPrimaryKey: Boolean(row.primary_key),
        ordinal: Number(row.ordinal ?? table.columns.length),
      });
    }
  });

  const infoSchemaTablesResult = await executeSql(`
    SELECT
      table_schema AS namespace_id,
      table_name,
      table_type
    FROM information_schema.tables
    ORDER BY table_schema, table_name
  `);

  const infoSchemaColumnsResult = await executeSql(`
    SELECT
      table_schema AS namespace_id,
      table_name,
      column_name,
      data_type,
      is_nullable,
      CAST(ordinal_position AS BIGINT) AS ordinal_position
    FROM information_schema.columns
    ORDER BY table_schema, table_name, ordinal_position
  `);

  const infoColumnsByTable = new Map<string, Record<string, unknown>[]>();

  infoSchemaColumnsResult.forEach((row) => {
    const namespaceName = String(row.namespace_id ?? "");
    const tableName = String(row.table_name ?? "");
    if (!namespaceName || !tableName || !row.column_name) {
      return;
    }

    const tableKey = `${namespaceName}.${tableName}`;
    const tableColumns = infoColumnsByTable.get(tableKey);
    if (tableColumns) {
      tableColumns.push(row);
      return;
    }
    infoColumnsByTable.set(tableKey, [row]);
  });

  infoSchemaTablesResult.forEach((row) => {
    const namespaceName = String(row.namespace_id ?? "");
    const tableName = String(row.table_name ?? "");
    if (!namespaceName || !tableName) {
      return;
    }

    const table = ensureTable(
      namespaces,
      tableMap,
      databaseName,
      namespaceName,
      tableName,
      inferExplorerTableType(namespaceName, row.table_type),
    );

    const existingColumns = new Set(table.columns.map((column) => column.name));
    const infoColumns = infoColumnsByTable.get(`${namespaceName}.${tableName}`) ?? [];
    infoColumns.forEach((column) => {
      const columnName = String(column.column_name ?? "");
      if (!columnName || existingColumns.has(columnName)) {
        return;
      }

      table.columns.push({
        name: columnName,
        dataType: String(column.data_type ?? "unknown"),
        isNullable: normalizeNullable(column.is_nullable),
        isPrimaryKey: false,
        ordinal: Number(column.ordinal_position ?? table.columns.length + 1),
      });
      existingColumns.add(columnName);
    });
  });

  const sortedNamespaces = Array.from(namespaces.values())
    .map((namespace) => ({
      ...namespace,
      tables: namespace.tables
        .map((table) => ({
          ...table,
          columns: [...table.columns].sort((left, right) => left.ordinal - right.ordinal),
        }))
        .sort((left, right) => left.name.localeCompare(right.name)),
    }))
    .sort((left, right) => left.name.localeCompare(right.name));

  return sortedNamespaces;
}

export async function executeSqlStudioQuery(sql: string): Promise<QueryResultData> {
  const response = await executeQuery(sql);

  if (response.status === "error" && response.error) {
    const createdAt = new Date().toISOString();
    return {
      status: "error",
      rows: [],
      schema: [],
      tookMs: response.took ?? 0,
      rowCount: 0,
      logs: [{
        id: `${createdAt}-error`,
        level: "error",
        message: response.error.message,
        response: response.error,
        createdAt,
      }],
      errorMessage: response.error.message,
    };
  }

  const statementResults = (response.results ?? []) as RawSqlStatementResult[];
  const tabularResult = statementResults.find(hasTabularPayload);
  const firstResult = statementResults[0];

  const schema = normalizeSchema(tabularResult?.schema);
  const rows = rowsToObjects(schema, tabularResult?.rows, tabularResult?.named_rows);
  const logs = buildQueryLogs(statementResults);

  return {
    status: "success",
    rows,
    schema,
    tookMs: response.took ?? 0,
    rowCount:
      typeof tabularResult?.row_count === "number"
        ? tabularResult.row_count
        : rows.length,
    logs,
    message: firstResult?.message,
  };
}
