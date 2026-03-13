import { executeSql } from "@/lib/kalam-client";
import {
  SYSTEM_SETTINGS_QUERY,
  SYSTEM_STATS_QUERY,
  SYSTEM_USERS_QUERY,
  getDbaStatsQuery,
} from "@/services/sql/queries/systemQueries";

export async function fetchSystemSettings(): Promise<Record<string, unknown>[]> {
  return executeSql(SYSTEM_SETTINGS_QUERY);
}

export async function fetchSystemUsers(): Promise<Record<string, unknown>[]> {
  return executeSql(SYSTEM_USERS_QUERY);
}

export interface Setting {
  name: string;
  value: string;
  description: string;
  category: string;
}

export function mapSettingsRows(rows: Record<string, unknown>[]): Setting[] {
  if (rows.length === 0) {
    return [
      {
        name: "server.version",
        value: "0.1.0",
        description: "KalamDB server version",
        category: "Server",
      },
      {
        name: "storage.default_backend",
        value: "rocksdb",
        description: "Default storage backend for write operations",
        category: "Storage",
      },
      {
        name: "query.max_rows",
        value: "10000",
        description: "Maximum rows returned per query",
        category: "Query",
      },
      {
        name: "auth.jwt_expiry",
        value: "3600",
        description: "JWT token expiry in seconds",
        category: "Authentication",
      },
    ];
  }

  return rows.map((row) => ({
    name: String(row.name ?? ""),
    value: String(row.value ?? ""),
    description: String(row.description ?? ""),
    category: String(row.category ?? ""),
  }));
}

export type SystemStatsMap = Record<string, string>;

export async function fetchSystemStats(): Promise<SystemStatsMap> {
  const rows = await executeSql(SYSTEM_STATS_QUERY);
  const stats: SystemStatsMap = {};

  rows.forEach((row) => {
    const metricName = String(row.metric_name ?? "");
    if (!metricName) {
      return;
    }

    stats[metricName] = String(row.metric_value ?? "");
  });

  return stats;
}

export interface DbaStatRow {
  sampled_at: number;
  metric_name: string;
  metric_value: number;
}

const SUPPORTED_DBA_METRICS = new Set([
  "active_connections",
  "active_subscriptions",
  "memory_usage_mb",
  "cpu_usage_percent",
  "total_jobs",
  "jobs_running",
  "jobs_queued",
  "total_tables",
  "total_namespaces",
  "open_files_total",
  "open_files_regular",
]);

function normalizeEpochMillis(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    let epochMillis = value;
    while (Math.abs(epochMillis) >= 1e15) {
      epochMillis /= 1000;
    }
    return Math.trunc(epochMillis);
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return 0;
    }

    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      return normalizeEpochMillis(numeric);
    }

    const parsedDate = new Date(trimmed).getTime();
    return Number.isNaN(parsedDate) ? 0 : parsedDate;
  }

  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    for (const key of ["Timestamp", "$date", "value", "Utf8", "String"]) {
      if (key in record) {
        return normalizeEpochMillis(record[key]);
      }
    }
  }

  return 0;
}

function normalizeMetricValue(value: unknown): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : Number.NaN;
  }

  if (typeof value === "string") {
    const numeric = Number(value.trim());
    return Number.isFinite(numeric) ? numeric : Number.NaN;
  }

  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    for (const key of ["Float64", "Float32", "Int64", "Int32", "UInt64", "UInt32", "value"] as const) {
      if (key in record) {
        return normalizeMetricValue(record[key]);
      }
    }
  }

  return Number.NaN;
}

function getTimeRangeCutoff(timeRange: string): number {
  const match = timeRange.trim().match(/^(\d+)\s+(HOUR|HOURS|DAY|DAYS)$/i);
  if (!match) {
    return 0;
  }

  const amount = Number(match[1]);
  if (!Number.isFinite(amount) || amount <= 0) {
    return 0;
  }

  const unit = match[2].toUpperCase();
  const multiplier = unit.startsWith("DAY") ? 24 * 60 * 60 * 1000 : 60 * 60 * 1000;
  return Date.now() - amount * multiplier;
}

export async function fetchDbaStats(timeRange: string = "24 HOURS"): Promise<DbaStatRow[]> {
  const rows = await executeSql(getDbaStatsQuery());
  const cutoff = getTimeRangeCutoff(timeRange);

  return rows
    .map((row) => ({
      sampled_at: normalizeEpochMillis(row.sampled_at),
      metric_name: String(row.metric_name ?? ""),
      metric_value: normalizeMetricValue(row.metric_value),
    }))
    .filter((row) => {
      if (!row.metric_name || !SUPPORTED_DBA_METRICS.has(row.metric_name)) {
        return false;
      }

      if (!Number.isFinite(row.metric_value) || row.sampled_at <= 0) {
        return false;
      }

      return cutoff === 0 || row.sampled_at >= cutoff;
    })
    .sort((left, right) => left.sampled_at - right.sampled_at);
}
