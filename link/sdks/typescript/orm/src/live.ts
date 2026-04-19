import type { Table, InferSelectModel } from 'drizzle-orm';
import { getTableName, getTableColumns } from 'drizzle-orm';
import type { KalamDBClient, LiveRowsOptions, Unsubscribe, RowData } from '@kalamdb/client';

type LiveTableOptions<T> = Omit<LiveRowsOptions<T>, 'mapRow'>;

function unwrapCellValue(cell: unknown): unknown {
  if (cell == null) return null;
  if (typeof cell === 'object' && 'toJson' in cell) {
    return (cell as { toJson: () => unknown }).toJson();
  }
  return cell;
}

export function liveTable<TTable extends Table>(
  client: KalamDBClient,
  table: TTable,
  callback: (rows: InferSelectModel<TTable>[]) => void,
  options: LiveTableOptions<InferSelectModel<TTable>> = {},
): Promise<Unsubscribe> {
  const tableName = getTableName(table);
  const columns = getTableColumns(table);

  const mapRow = (row: RowData): InferSelectModel<TTable> => {
    const mapped: Record<string, unknown> = {};
    for (const [key, col] of Object.entries(columns)) {
      const raw = unwrapCellValue(row[col.name]);
      if (raw !== undefined && 'mapFromDriverValue' in col) {
        mapped[key] = (col as { mapFromDriverValue: (v: unknown) => unknown }).mapFromDriverValue(raw);
      } else {
        mapped[key] = raw ?? null;
      }
    }
    return mapped as InferSelectModel<TTable>;
  };

  return client.live<InferSelectModel<TTable>>(
    `SELECT * FROM ${tableName}`,
    callback,
    { ...options, mapRow },
  );
}
