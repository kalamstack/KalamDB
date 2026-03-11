import type { RowDeletion, RowEdit } from "@/hooks/useTableChanges";

function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

function toSqlLiteral(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return `'${escapeSqlString(value)}'`;
  if (typeof value === "object") return `'${escapeSqlString(JSON.stringify(value))}'`;
  return `'${escapeSqlString(String(value))}'`;
}

function buildWhereClause(primaryKeyValues: Record<string, unknown>): string {
  const conditions = Object.entries(primaryKeyValues).map(([column, value]) => {
    if (value === null || value === undefined) {
      return `${column} IS NULL`;
    }
    return `${column} = ${toSqlLiteral(value)}`;
  });
  return conditions.join(" AND ");
}

function generateUpdate(qualifiedTable: string, rowEdit: RowEdit): string {
  const setClauses = Object.values(rowEdit.cellEdits)
    .map((edit) => `${edit.columnName} = ${toSqlLiteral(edit.newValue)}`)
    .join(", ");

  const where = buildWhereClause(rowEdit.primaryKeyValues);
  return `UPDATE ${qualifiedTable} SET ${setClauses} WHERE ${where};`;
}

function generateDelete(qualifiedTable: string, deletion: RowDeletion): string {
  const where = buildWhereClause(deletion.primaryKeyValues);
  return `DELETE FROM ${qualifiedTable} WHERE ${where};`;
}

export interface GeneratedSql {
  statements: string[];
  fullSql: string;
  updateCount: number;
  deleteCount: number;
}

export function generateSqlStatements(
  namespace: string,
  tableName: string,
  edits: Map<number, RowEdit>,
  deletions: Map<number, RowDeletion>,
): GeneratedSql {
  const qualifiedTable = `${namespace}.${tableName}`;
  const statements: string[] = [];

  const sortedEdits = Array.from(edits.values()).sort((left, right) => left.rowIndex - right.rowIndex);
  for (const rowEdit of sortedEdits) {
    if (deletions.has(rowEdit.rowIndex)) continue;
    statements.push(generateUpdate(qualifiedTable, rowEdit));
  }

  const sortedDeletions = Array.from(deletions.values()).sort((left, right) => left.rowIndex - right.rowIndex);
  for (const deletion of sortedDeletions) {
    statements.push(generateDelete(qualifiedTable, deletion));
  }

  return {
    statements,
    fullSql: statements.join("\n"),
    updateCount: sortedEdits.filter((edit) => !deletions.has(edit.rowIndex)).length,
    deleteCount: sortedDeletions.length,
  };
}

export function generateAlterTableSql(
  namespace: string,
  tableName: string,
  operation: "add_column" | "drop_column" | "rename_column" | "modify_column",
  params: {
    columnName?: string;
    newColumnName?: string;
    dataType?: string;
    nullable?: boolean;
  },
): string {
  const qualifiedTable = `${namespace}.${tableName}`;

  switch (operation) {
    case "add_column":
      return `ALTER TABLE ${qualifiedTable} ADD COLUMN ${params.columnName ?? "new_column"} ${params.dataType ?? "STRING"}${params.nullable === false ? " NOT NULL" : ""};`;
    case "drop_column":
      return `ALTER TABLE ${qualifiedTable} DROP COLUMN ${params.columnName};`;
    case "rename_column":
      return `ALTER TABLE ${qualifiedTable} RENAME COLUMN ${params.columnName} TO ${params.newColumnName};`;
    case "modify_column":
      return `ALTER TABLE ${qualifiedTable} MODIFY COLUMN ${params.columnName} ${params.dataType ?? "STRING"};`;
    default:
      return `-- Unknown operation: ${operation}`;
  }
}

export function generateDropTableSql(namespace: string, tableName: string): string {
  return `DROP TABLE ${namespace}.${tableName};`;
}

export function generateCreateTableDdl(
  namespace: string,
  tableName: string,
  columns: { name: string; dataType: string; nullable?: boolean }[],
): string {
  const cols = columns
    .map((column) => `  ${column.name} ${column.dataType}${column.nullable === false ? " NOT NULL" : ""}`)
    .join(",\n");
  return `CREATE TABLE ${namespace}.${tableName} (\n${cols}\n);`;
}