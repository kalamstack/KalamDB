/**
 * Query helper utilities for KalamDB SDK
 *
 * Provides utilities for:
 * - Query response normalization and column reordering
 * - Schema manipulation
 *
 * Row parsing (schema → named maps) is now handled in the Rust WASM layer
 * via `QueryResult.named_rows`. SDKs read `named_rows` directly and wrap
 * values in `KalamCellValue` — no `parseRows`/`parseCellRows` needed.
 */

import type { QueryResponse, SchemaField } from '../types.js';

/* ================================================================== */
/*  Column Extraction & Sorting                                       */
/* ================================================================== */

/**
 * Get column names from schema or columns array (backwards compatible)
 *
 * New format: schema = [{name, data_type, index}, ...]
 * Old format: columns = ['name1', 'name2', ...]
 */
function getColumnNames(resp: QueryResponse): string[] {
  const firstResult = resp.results?.[0];
  if (!firstResult) return [];

  // New format: schema array with name, data_type, index
  if (Array.isArray(firstResult.schema) && firstResult.schema.length > 0) {
    // Sort by index to ensure correct order
    return firstResult.schema
      .slice()
      .sort((a, b) => (a.index ?? 0) - (b.index ?? 0))
      .map(field => field.name);
  }

  return [];
}

/**
 * Compute a stable sorted columns array given a preferred order.
 *
 * Columns in preferredOrder come first by that exact order;
 * others are appended keeping original relative order.
 */
export function sortColumns(columns: string[], preferredOrder: string[]): string[] {
  const orderIndex = new Map<string, number>();
  preferredOrder.forEach((name, i) => orderIndex.set(name, i));

  const listed: string[] = [];
  const unlisted: string[] = [];

  for (const c of columns) {
    (orderIndex.has(c) ? listed : unlisted).push(c);
  }

  listed.sort((a, b) => orderIndex.get(a)! - orderIndex.get(b)!);
  return [...listed, ...unlisted];
}

/* ================================================================== */
/*  Row Mapping                                                       */
/* ================================================================== */

/**
 * Remap an array row from currentColumns order to newColumns order
 */
function remapArrayRow(
  row: unknown[],
  currentColumns: string[],
  newColumns: string[],
): unknown[] {
  const idxMap = new Map<string, number>();
  currentColumns.forEach((c, i) => idxMap.set(c, i));
  return newColumns.map(c => (idxMap.has(c) ? row[idxMap.get(c)!] : null));
}

/**
 * Convert an object row to array format based on column order
 */
function objectRowToArray(row: Record<string, unknown>, newColumns: string[]): unknown[] {
  return newColumns.map(c => (c in row ? row[c] : null));
}

/* ================================================================== */
/*  Query Response Normalization                                      */
/* ================================================================== */

/**
 * Normalize query response to the preferred column order.
 *
 * Supports both new format (schema) and old format (columns).
 *
 * @param resp - Query response with results
 * @param preferredOrder - Desired column order
 * @returns Normalized query response
 */
export function normalizeQueryResponse(
  resp: QueryResponse,
  preferredOrder: string[],
): QueryResponse {
  const currentColumns = getColumnNames(resp);
  const newColumns = sortColumns(currentColumns, preferredOrder);

  const firstResult = resp.results?.[0];
  if (!firstResult) return resp;

  let newRows: unknown[][] = [];
  const rows = Array.isArray(firstResult.rows) ? firstResult.rows : [];

  if (rows.length > 0) {
    const first = rows[0];
    if (Array.isArray(first)) {
      newRows = rows.map(r => remapArrayRow(r as unknown[], currentColumns, newColumns));
    } else if (first && typeof first === 'object') {
      newRows = rows.map(r =>
        objectRowToArray(r as unknown as Record<string, unknown>, newColumns),
      );
    } else {
      newRows = rows as unknown[][];
    }
  }

  // Build new schema with updated indices
  const newSchema: SchemaField[] = newColumns.map((name, index) => {
    // Find original field to preserve data_type
    const original = firstResult.schema?.find(f => f.name === name);
    return {
      name,
      data_type: original?.data_type ?? 'Text',
      index,
    };
  });

  return {
    ...resp,
    results: [
      {
        ...firstResult,
        schema: newSchema,
        rows: newRows as any,
      },
    ],
  };
}

/* ================================================================== */
/*  Constants                                                         */
/* ================================================================== */

/**
 * Canonical column order for system.tables
 */
export const SYSTEM_TABLES_ORDER = [
  'table_id',
  'table_name',
  'namespace',
  'table_type',
  'created_at',
  'storage_location',
  'storage_id',
  'use_user_storage',
  'flush_policy',
  'schema_version',
  'deleted_retention_hours',
  'access_level',
];
