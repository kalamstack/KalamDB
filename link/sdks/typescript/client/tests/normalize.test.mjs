import assert from 'node:assert/strict';
import { normalizeQueryResponse, sortColumns, SYSTEM_TABLES_ORDER } from '../dist/src/helpers/query_helpers.js';

// Helper: build a minimal QueryResponse using the current schema format
function makeResp(columnNames, rows) {
  return {
    status: 'success',
    results: [
      {
        schema: columnNames.map((name, index) => ({ name, data_type: 'Text', index })),
        rows,
        row_count: rows.length,
      },
    ],
  };
}

// Helper: extract column names from a normalized response (sorted by index)
function getColumns(resp) {
  return (resp.results?.[0]?.schema ?? [])
    .slice()
    .sort((a, b) => (a.index ?? 0) - (b.index ?? 0))
    .map(f => f.name);
}

// Basic behavior: reorder columns and rows (array form)
{
  const resp = makeResp(
    ['schema_version', 'table_name', 'table_id'],
    [
      ['v3', 'users', 'tbl_1'],
      ['v4', 'jobs', 'tbl_2'],
    ],
  );

  const out = normalizeQueryResponse(resp, SYSTEM_TABLES_ORDER);
  const cols = getColumns(out);
  assert.deepEqual(cols.slice(0, 3), ['table_id', 'table_name', 'schema_version']);
  assert.deepEqual(out.results[0].rows[0].slice(0, 3), ['tbl_1', 'users', 'v3']);
}

// Object rows: convert to arrays in preferred order
{
  const resp = {
    status: 'success',
    results: [
      {
        schema: [
          { name: 'table_name',   data_type: 'Text', index: 0 },
          { name: 'access_level', data_type: 'Text', index: 1 },
          { name: 'table_id',     data_type: 'Text', index: 2 },
        ],
        rows: [{ table_id: 't1', table_name: 'tables', access_level: 'public' }],
        row_count: 1,
      },
    ],
  };

  const out = normalizeQueryResponse(resp, SYSTEM_TABLES_ORDER);
  const cols = getColumns(out);
  // table_id has the highest priority in SYSTEM_TABLES_ORDER
  assert.equal(cols[0], 'table_id');
  // first row, first column after reorder should be the table_id value
  assert.equal(out.results[0].rows[0][0], 't1');
}

// sortColumns leaves unknown columns after preferred ones
{
  const cols = ['x', 'table_name', 'y', 'table_id'];
  const sorted = sortColumns(cols, SYSTEM_TABLES_ORDER);
  const idxTableId = sorted.indexOf('table_id');
  const idxTableName = sorted.indexOf('table_name');
  assert.ok(idxTableId < idxTableName, 'table_id should come before table_name');
  // unknowns at the end (relative order preserved: x before y)
  const tail = sorted.slice(-2);
  assert.deepEqual(tail, ['x', 'y']);
}

console.log('normalize.test.mjs passed');
