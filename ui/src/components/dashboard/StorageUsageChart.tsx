import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { Storage, StorageHealthResult } from "@/services/storageService";
import {
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";

interface StorageUsageChartProps {
  storages: Storage[];
  selectedStorageId: string;
  onStorageChange: (storageId: string) => void;
  health: StorageHealthResult | null;
  isLoading?: boolean;
  error?: string | null;
}

function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }

  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let size = value;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }

  const precision = size >= 100 ? 0 : size >= 10 ? 1 : 2;
  return `${size.toFixed(precision)} ${units[unitIndex]}`;
}

function getUsageSlices(health: StorageHealthResult | null) {
  const used = Math.max(0, health?.used_bytes ?? 0);
  const total = Math.max(0, health?.total_bytes ?? 0);
  const free = Math.max(total - used, 0);

  return {
    used,
    total,
    free,
    data: [
      { name: "Used", value: used, color: "#0f766e" },
      { name: "Free", value: free, color: "#cbd5e1" },
    ],
  };
}

export function StorageUsageChart({
  storages,
  selectedStorageId,
  onStorageChange,
  health,
  isLoading,
  error,
}: StorageUsageChartProps) {
  const selectedStorage = storages.find((storage) => storage.storage_id === selectedStorageId) ?? null;
  const { used, total, data } = getUsageSlices(health);
  const usagePercent = total > 0 ? Math.min(100, Math.round((used / total) * 100)) : null;

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-4 space-y-0">
        <div>
          <CardTitle className="text-base font-medium">Storage Usage</CardTitle>
          <p className="mt-1 text-sm text-muted-foreground">
            Capacity and usage for the selected storage backend.
          </p>
        </div>
        <Select value={selectedStorageId} onValueChange={onStorageChange} disabled={storages.length === 0}>
          <SelectTrigger className="w-[220px] h-9">
            <SelectValue placeholder="Select storage" />
          </SelectTrigger>
          <SelectContent>
            {storages.map((storage) => (
              <SelectItem key={storage.storage_id} value={storage.storage_id}>
                {storage.storage_name} ({storage.storage_id})
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">
            Checking storage health...
          </div>
        ) : error ? (
          <div className="flex h-72 items-center justify-center text-sm text-destructive">
            {error}
          </div>
        ) : !health || total <= 0 ? (
          <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">
            No storage capacity data is available for this backend yet.
          </div>
        ) : (
          <div className="grid gap-6 lg:grid-cols-[1.2fr_0.8fr]">
            <div className="h-72">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={data}
                    dataKey="value"
                    nameKey="name"
                    innerRadius={78}
                    outerRadius={112}
                    paddingAngle={2}
                    strokeWidth={0}
                  >
                    {data.map((entry) => (
                      <Cell key={entry.name} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value) => formatBytes(typeof value === "number" ? value : Number(value ?? 0))} />
                  <Legend verticalAlign="bottom" />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="space-y-4 rounded-lg border border-border/60 bg-muted/20 p-4">
              <div>
                <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Storage</p>
                <p className="mt-1 text-lg font-semibold">
                  {selectedStorage?.storage_name ?? selectedStorageId}
                </p>
                <p className="text-sm text-muted-foreground">{selectedStorage?.base_directory ?? "-"}</p>
              </div>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
                <div>
                  <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Used</p>
                  <p className="text-2xl font-semibold">{formatBytes(used)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Total Capacity</p>
                  <p className="text-2xl font-semibold">{formatBytes(total)}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Usage</p>
                  <p className="text-2xl font-semibold">{usagePercent === null ? "-" : `${usagePercent}%`}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Status</p>
                  <p className="text-2xl font-semibold capitalize">{health.status}</p>
                </div>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
