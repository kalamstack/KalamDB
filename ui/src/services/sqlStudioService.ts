import { executeQuery, executeSql } from "@/lib/kalam-client";
import { KalamCellValue } from "kalam-link";
import type { SchemaField } from "kalam-link";
import type {
  QueryLogEntry,
  QueryResultData,
  QueryResultSchemaField,
  StudioNamespace,
  StudioTable,
} from "@/components/sql-studio-v2/types";

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
  // Prefer named_rows: Rust WASM pre-computes the schema→map transformation.
  if (namedRows && namedRows.length > 0) {
    return namedRows.slice(0, MAX_SQL_STUDIO_RENDER_ROWS).map((row) => {
      const item: Record<string, unknown> = {};
      for (const key of Object.keys(row)) {
        item[key] = KalamCellValue.from(row[key] ?? null);
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
      item[field.name] = KalamCellValue.from(row[field.index] ?? null);
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

    namespaces.set(namespaceName, {
      database: databaseName,
      name: namespaceName,
      tables: [],
    });
  });

  const tableAndColumnsResult = await executeSql(`
    SELECT
      t.namespace_id,
      t.table_name,
      t.table_type,
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

    if (!namespaces.has(namespaceName)) {
      namespaces.set(namespaceName, {
        database: databaseName,
        name: namespaceName,
        tables: [],
      });
    }

    const tableKey = `${namespaceName}.${tableName}`;
    let table = tableMap.get(tableKey);

    if (!table) {
      table = {
        database: databaseName,
        namespace: namespaceName,
        name: tableName,
        tableType: String(row.table_type ?? "user"),
        columns: [],
      };
      tableMap.set(tableKey, table);
      namespaces.get(namespaceName)?.tables.push(table);
    }

    if (row.column_name) {
      table.columns.push({
        name: String(row.column_name),
        dataType: String(row.data_type ?? "unknown"),
        isNullable: Boolean(row.nullable),
        isPrimaryKey: Boolean(row.primary_key),
        ordinal: Number(row.ordinal ?? table.columns.length),
      });
    }
  });

  return Array.from(namespaces.values());
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
