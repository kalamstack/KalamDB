import { Clock3, History, Info } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatDate } from "@/lib/formatters";
import type {
  QueryRunSummary,
  StudioTable,
  StudioTableOptions,
  StudioTableTimestamp,
} from "../shared/types";

interface StudioInspectorPanelProps {
  selectedTable: StudioTable | null;
  history: QueryRunSummary[];
}

interface InspectorItem {
  label: string;
  value: string;
}

function formatTableType(tableType: string): string {
  if (!tableType) {
    return "Unknown";
  }

  return tableType.charAt(0).toUpperCase() + tableType.slice(1);
}

function normalizeTimestampValue(value: StudioTableTimestamp | null | undefined): string | null {
  if (value == null) {
    return null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    const milliseconds = value > 10_000_000_000_000 ? Math.trunc(value / 1000) : value;
    const date = new Date(milliseconds);
    return Number.isNaN(date.getTime()) ? String(value) : formatDate(date, "locale");
  }

  if (typeof value === "string") {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : formatDate(date, "locale");
  }

  return null;
}

function titleCaseSegment(segment: string): string {
  return segment
    .split("_")
    .map((word) => {
      const normalized = word.trim().toLowerCase();
      if (!normalized) {
        return "";
      }
      if (["id", "ttl", "api", "url", "sql"].includes(normalized)) {
        return normalized.toUpperCase();
      }
      return normalized.charAt(0).toUpperCase() + normalized.slice(1);
    })
    .join(" ");
}

function formatOptionKey(path: string): string {
  return path
    .split(".")
    .map((segment) => titleCaseSegment(segment))
    .join(" / ");
}

function formatOptionValue(value: unknown): string {
  if (value == null) {
    return "None";
  }

  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }

  if (typeof value === "number") {
    return Number.isInteger(value) ? value.toLocaleString() : String(value);
  }

  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.length === 0 ? "[]" : value.map((entry) => formatOptionValue(entry)).join(", ");
  }

  return JSON.stringify(value);
}

function flattenTableOptions(
  options: StudioTableOptions | null | undefined,
  prefix = "",
): InspectorItem[] {
  if (!options) {
    return [];
  }

  return Object.entries(options)
    .sort(([left], [right]) => left.localeCompare(right))
    .flatMap(([key, value]) => {
      if (!prefix && key === "table_type") {
        return [];
      }

      const nextPath = prefix ? `${prefix}.${key}` : key;

      if (value && typeof value === "object" && !Array.isArray(value)) {
        const nestedEntries = flattenTableOptions(value as StudioTableOptions, nextPath);
        if (nestedEntries.length > 0) {
          return nestedEntries;
        }
      }

      return [{
        label: formatOptionKey(nextPath),
        value: formatOptionValue(value),
      }];
    });
}

function formatDurationSeconds(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "Not set";
  }

  if (value % 86_400 === 0) {
    const days = value / 86_400;
    return `${days.toLocaleString()} day${days === 1 ? "" : "s"}`;
  }

  if (value % 3_600 === 0) {
    const hours = value / 3_600;
    return `${hours.toLocaleString()} hour${hours === 1 ? "" : "s"}`;
  }

  if (value % 60 === 0) {
    const minutes = value / 60;
    return `${minutes.toLocaleString()} minute${minutes === 1 ? "" : "s"}`;
  }

  return `${value.toLocaleString()} second${value === 1 ? "" : "s"}`;
}

function deriveRetentionValue(options: StudioTableOptions | null | undefined): string | null {
  if (!options) {
    return null;
  }

  const ttlSeconds = typeof options.ttl_seconds === "number"
    ? options.ttl_seconds
    : typeof options.retention_seconds === "number"
      ? options.retention_seconds
      : null;
  const maxBytes = typeof options.retention_max_bytes === "number"
    ? options.retention_max_bytes
    : null;

  const parts: string[] = [];
  if (ttlSeconds && ttlSeconds > 0) {
    parts.push(formatDurationSeconds(ttlSeconds));
  }
  if (maxBytes && maxBytes > 0) {
    parts.push(`${maxBytes.toLocaleString()} bytes max`);
  }

  return parts.length > 0 ? parts.join(" / ") : null;
}

function buildTableInfoItems(selectedTable: StudioTable): InspectorItem[] {
  const primaryKeys = selectedTable.columns
    .filter((column) => column.isPrimaryKey)
    .map((column) => column.name);
  const createdAt = normalizeTimestampValue(selectedTable.createdAt);
  const updatedAt = normalizeTimestampValue(selectedTable.updatedAt);
  const retention = deriveRetentionValue(selectedTable.options);

  const items: InspectorItem[] = [
    { label: "Namespace", value: selectedTable.namespace },
    { label: "Type", value: formatTableType(selectedTable.tableType) },
    { label: "Columns", value: selectedTable.columns.length.toLocaleString() },
    {
      label: "Primary Key",
      value: primaryKeys.length > 0 ? primaryKeys.join(", ") : "None",
    },
  ];

  if (selectedTable.version != null) {
    items.splice(2, 0, { label: "Revision", value: `v${selectedTable.version}` });
  }

  if (retention) {
    items.push({ label: "Retention", value: retention });
  }

  if (createdAt) {
    items.push({ label: "Created", value: createdAt });
  }

  if (updatedAt) {
    items.push({ label: "Updated", value: updatedAt });
  }

  return items;
}

function MetadataList({ items }: { items: InspectorItem[] }) {
  return (
    <ul className="overflow-hidden rounded-md border border-border bg-background">
      {items.map((item, index) => (
        <li
          key={item.label}
          className={`flex items-start justify-between gap-3 px-3 py-2 ${index === 0 ? "" : "border-t border-border"}`}
        >
          <span className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
            {item.label}
          </span>
          <span className="max-w-[65%] text-right text-sm text-foreground whitespace-pre-wrap break-words">
            {item.value}
          </span>
        </li>
      ))}
    </ul>
  );
}

export function StudioInspectorPanel({
  selectedTable,
  history,
}: StudioInspectorPanelProps) {
  const tableInfoItems = selectedTable ? buildTableInfoItems(selectedTable) : [];
  const tableOptionItems = selectedTable ? flattenTableOptions(selectedTable.options) : [];

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden border-l border-border bg-background">
      <Tabs defaultValue="details" className="flex h-full min-h-0 flex-col">
        <div className="shrink-0 border-b border-border px-2 py-2">
          <TabsList className="grid h-8 w-full grid-cols-2 bg-transparent">
            <TabsTrigger value="details" className="gap-1.5">
              <Info className="h-3.5 w-3.5" />
              Details
            </TabsTrigger>
            <TabsTrigger value="history" className="gap-1.5">
              <History className="h-3.5 w-3.5" />
              History
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="details" className="m-0 flex-1 overflow-hidden">
          <ScrollArea className="h-full p-3">
            {!selectedTable && (
              <p className="text-sm text-muted-foreground">
                Select a table from Explorer to inspect schema details.
              </p>
            )}

            {selectedTable && (
              <div className="space-y-4">
                <div className="space-y-2">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Table Information</p>
                      <p className="text-sm font-semibold text-foreground">
                        {selectedTable.namespace}.{selectedTable.name}
                      </p>
                      {selectedTable.comment && (
                        <p className="mt-1 text-xs text-muted-foreground whitespace-pre-wrap break-words">
                          {selectedTable.comment}
                        </p>
                      )}
                    </div>
                    <Badge variant="secondary" className="text-[10px] uppercase">
                      {formatTableType(selectedTable.tableType)}
                    </Badge>
                  </div>
                  <MetadataList items={tableInfoItems} />
                </div>

                <div className="space-y-2">
                  <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Table Schema</p>
                  {selectedTable.columns.map((column) => (
                    <div key={column.name} className="rounded-md border border-border bg-background p-2">
                      <div className="flex items-center justify-between gap-2">
                        <span className="truncate text-sm font-medium text-foreground">{column.name}</span>
                        <span className="text-[10px] uppercase text-muted-foreground">{column.dataType}</span>
                      </div>
                      <div className="mt-1 flex items-center gap-2 text-[10px] text-muted-foreground">
                        {column.isPrimaryKey && <span>Primary Key</span>}
                        <span>{column.isNullable ? "Nullable" : "Not Null"}</span>
                      </div>
                    </div>
                  ))}
                </div>

                <div className="space-y-2">
                  <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Table Options</p>
                  {tableOptionItems.length > 0 ? (
                    <MetadataList items={tableOptionItems} />
                  ) : (
                    <p className="rounded-md border border-border bg-background px-3 py-2 text-sm text-muted-foreground">
                      No table options exposed for this table.
                    </p>
                  )}
                </div>
              </div>
            )}
          </ScrollArea>
        </TabsContent>

        <TabsContent value="history" className="m-0 flex-1 overflow-hidden">
          <ScrollArea className="h-full p-3">
            <div className="space-y-2">
              {history.length === 0 && (
                <p className="text-sm text-muted-foreground">No query executions yet.</p>
              )}

              {history.map((entry) => (
                <div key={entry.id} className="rounded-md border border-border bg-background p-2">
                  <div className="flex items-center justify-between gap-2">
                    <p className="truncate text-xs font-semibold">{entry.tabTitle}</p>
                    <Badge variant={entry.status === "success" ? "secondary" : "outline"}>
                      {entry.status}
                    </Badge>
                  </div>
                  <p className="mt-1 line-clamp-2 text-xs text-muted-foreground">{entry.sql}</p>
                  <div className="mt-2 flex items-center gap-3 text-[10px] text-muted-foreground">
                    <span className="inline-flex items-center gap-1">
                      <Clock3 className="h-3 w-3" />
                      {new Date(entry.executedAt).toLocaleTimeString()}
                    </span>
                    <span>{entry.durationMs} ms</span>
                    <span>{entry.rowCount} rows</span>
                  </div>
                  {entry.errorMessage && (
                    <p className="mt-2 text-xs text-destructive">{entry.errorMessage}</p>
                  )}
                </div>
              ))}
            </div>
          </ScrollArea>
        </TabsContent>
      </Tabs>
    </div>
  );
}
