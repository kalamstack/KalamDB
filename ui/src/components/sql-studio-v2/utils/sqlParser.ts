export interface TableContext {
  namespace: string;
  tableName: string;
}

export function extractTableContext(sql: string): TableContext | null {
  if (!sql) return null;

  const normalized = sql
    .replace(/--[^\n]*/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();

  const qualifiedMatch = normalized.match(
    /(?:FROM|INTO|UPDATE|TABLE)\s+([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)/i,
  );
  if (qualifiedMatch) {
    const [namespace, tableName] = qualifiedMatch[1].split(".");
    return { namespace: namespace.toLowerCase(), tableName: tableName.toLowerCase() };
  }

  const simpleMatch = normalized.match(/(?:FROM|INTO|UPDATE|TABLE)\s+([a-z_][a-z0-9_]*)/i);
  if (simpleMatch) {
    return { namespace: "default", tableName: simpleMatch[1].toLowerCase() };
  }

  return null;
}