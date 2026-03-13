import { useMemo } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  AreaChart,
  Area,
} from "recharts";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import type { DbaStatRow } from "@/services/systemTableService";

interface TimeSeriesData {
  time: string;
  timestamp: number;
  [key: string]: string | number;
}

interface MetricsChartProps {
  data: DbaStatRow[];
  isLoading?: boolean;
}

function formatTimeLabel(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export function MetricsChart({ data, isLoading }: MetricsChartProps) {
  const chartData = useMemo(() => {
    if (!data || data.length === 0) return [];

    // Stats for a single collection pass can land a few milliseconds apart.
    // Bucket to the second so related metrics render on the same point.
    const grouped = data.reduce((acc, row) => {
      const ts = Math.floor(row.sampled_at / 1000) * 1000;
      if (Number.isNaN(ts) || ts <= 0) return acc; // Skip invalid dates
      
      if (!acc[ts]) {
        acc[ts] = { time: formatTimeLabel(ts), timestamp: ts };
      }
      
      acc[ts][row.metric_name] = row.metric_value;
      return acc;
    }, {} as Record<number, TimeSeriesData>);

    // Sort by timestamp
    return Object.values(grouped).sort((a, b) => a.timestamp - b.timestamp);
  }, [data]);

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-6">
        <Card className="h-96 flex items-center justify-center text-muted-foreground">
          Loading connection metrics...
        </Card>
        <Card className="h-96 flex items-center justify-center text-muted-foreground">
          Loading resource metrics...
        </Card>
      </div>
    );
  }

  if (chartData.length === 0) {
    return (
      <div className="grid grid-cols-1 gap-4 mt-6">
        <Card className="h-64 flex items-center justify-center text-muted-foreground">
          No historical metric data available yet.
        </Card>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-base font-medium">Connections & Subscriptions</CardTitle>
        </CardHeader>
        <CardContent className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.3} />
              <XAxis dataKey="timestamp" type="number" domain={["dataMin", "dataMax"]} tickFormatter={formatTimeLabel} tick={{ fontSize: 12 }} tickMargin={10} minTickGap={30} />
              <YAxis tick={{ fontSize: 12 }} allowDecimals={false} />
              <Tooltip 
                labelFormatter={(value) => formatTimeLabel(Number(value))}
                contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
              />
              <Legend wrapperStyle={{ paddingTop: '20px' }} />
              <Line 
                type="monotone" 
                dataKey="active_connections" 
                name="Connections" 
                stroke="#3b82f6" 
                strokeWidth={2}
                dot={false}
                connectNulls
                activeDot={{ r: 4 }}
              />
              <Line 
                type="monotone" 
                dataKey="active_subscriptions" 
                name="Subscriptions" 
                stroke="#10b981" 
                strokeWidth={2}
                dot={false}
                connectNulls
                activeDot={{ r: 4 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base font-medium">Resource Usage</CardTitle>
        </CardHeader>
        <CardContent className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.3} />
              <XAxis dataKey="timestamp" type="number" domain={["dataMin", "dataMax"]} tickFormatter={formatTimeLabel} tick={{ fontSize: 12 }} tickMargin={10} minTickGap={30} />
              <YAxis yAxisId="left" tick={{ fontSize: 12 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12 }} domain={[0, 100]} />
              <YAxis yAxisId="openFiles" orientation="right" hide />
              <Tooltip 
                labelFormatter={(value) => formatTimeLabel(Number(value))}
                contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
              />
              <Legend wrapperStyle={{ paddingTop: '20px' }} />
              <Area 
                yAxisId="left"
                type="monotone" 
                dataKey="memory_usage_mb" 
                name="Memory (MB)" 
                stroke="#8b5cf6" 
                fill="#8b5cf6" 
                fillOpacity={0.2}
                connectNulls
              />
              <Area 
                yAxisId="right"
                type="monotone" 
                dataKey="cpu_usage_percent" 
                name="CPU (%)" 
                stroke="#f59e0b" 
                fill="#f59e0b" 
                fillOpacity={0.2}
                connectNulls
              />
              <Line
                yAxisId="openFiles"
                type="monotone"
                dataKey="open_files_total"
                name="Open Files"
                stroke="#14b8a6"
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            </AreaChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base font-medium">Background Jobs</CardTitle>
        </CardHeader>
        <CardContent className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.3} />
              <XAxis dataKey="timestamp" type="number" domain={["dataMin", "dataMax"]} tickFormatter={formatTimeLabel} tick={{ fontSize: 12 }} tickMargin={10} minTickGap={30} />
              <YAxis tick={{ fontSize: 12 }} allowDecimals={false} />
              <Tooltip 
                labelFormatter={(value) => formatTimeLabel(Number(value))}
                contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
              />
              <Legend wrapperStyle={{ paddingTop: '20px' }} />
              <Line 
                type="monotone" 
                dataKey="total_jobs" 
                name="Total Jobs" 
                stroke="#64748b" 
                strokeWidth={2}
                dot={false}
                connectNulls
              />
              <Line 
                type="monotone" 
                dataKey="jobs_running" 
                name="Running Jobs" 
                stroke="#ec4899" 
                strokeWidth={2}
                dot={false}
                connectNulls
              />
              <Line 
                type="monotone" 
                dataKey="jobs_queued" 
                name="Queued Jobs" 
                stroke="#f59e0b" 
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base font-medium">Database Objects</CardTitle>
        </CardHeader>
        <CardContent className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} opacity={0.3} />
              <XAxis dataKey="timestamp" type="number" domain={["dataMin", "dataMax"]} tickFormatter={formatTimeLabel} tick={{ fontSize: 12 }} tickMargin={10} minTickGap={30} />
              <YAxis tick={{ fontSize: 12 }} allowDecimals={false} />
              <Tooltip 
                labelFormatter={(value) => formatTimeLabel(Number(value))}
                contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
              />
              <Legend wrapperStyle={{ paddingTop: '20px' }} />
              <Line 
                type="step" 
                dataKey="total_tables" 
                name="Tables" 
                stroke="#06b6d4" 
                strokeWidth={2}
                dot={false}
                connectNulls
              />
              <Line 
                type="step" 
                dataKey="total_namespaces" 
                name="Namespaces" 
                stroke="#84cc16" 
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

    </div>
  );
}
