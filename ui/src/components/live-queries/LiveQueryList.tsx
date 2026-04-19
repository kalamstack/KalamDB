import { useMemo, useState } from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Card } from '@/components/ui/card';
import type { LiveQuery, LiveQueryFilters } from '@/services/liveQueryService';
import { useKillLiveQueryMutation, useGetLiveQueriesQuery } from '@/store/apiSlice';
import { Loader2, RefreshCw, XCircle, Activity, Clock, Database, CheckCircle } from 'lucide-react';

export function LiveQueryList() {
  const [filters, setFilters] = useState({
    user_id: '',
    namespace_id: '',
    table_name: '',
    status: 'all',
  });
  const [appliedFilters, setAppliedFilters] = useState<LiveQueryFilters>({});
  const [killLiveQueryMutation] = useKillLiveQueryMutation();
  const [killingIds, setKillingIds] = useState<Set<string>>(new Set());
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [killError, setKillError] = useState<string | null>(null);

  const filterParams = useMemo<LiveQueryFilters>(
    () => ({
      ...(filters.user_id && { user_id: filters.user_id }),
      ...(filters.namespace_id && { namespace_id: filters.namespace_id }),
      ...(filters.table_name && { table_name: filters.table_name }),
      ...(filters.status && filters.status !== 'all' && { status: filters.status }),
    }),
    [filters.namespace_id, filters.status, filters.table_name, filters.user_id],
  );

  const { data: fetchedLiveQueries, isLoading, error: queryError, refetch } = useGetLiveQueriesQuery(appliedFilters, {
    pollingInterval: 5000,
  });

  const liveQueries = fetchedLiveQueries || [];

  const error = queryError && "error" in queryError && typeof queryError.error === "string" 
    ? queryError.error 
    : queryError 
      ? "Failed to fetch live queries" 
      : null;

  const handleKillQuery = async (liveQuery: LiveQuery) => {
    if (!confirm(`Kill live query for ${liveQuery.user_id} on ${liveQuery.namespace_id}.${liveQuery.table_name}?`)) {
      return;
    }

    setKillingIds(prev => new Set(prev).add(liveQuery.live_id));
    setKillError(null);
    setSuccessMessage(null);
    
    try {
      await killLiveQueryMutation(liveQuery.live_id).unwrap();
      setSuccessMessage(`Successfully killed query ${liveQuery.subscription_id}`);
      setTimeout(() => setSuccessMessage(null), 3000);
    } catch (err) {
      setKillError(err instanceof Error ? err.message : 'Failed to kill query');
      setTimeout(() => setKillError(null), 5000);
    } finally {
      setKillingIds(prev => {
        const next = new Set(prev);
        next.delete(liveQuery.live_id);
        return next;
      });
    }
  };

  const handleRefresh = () => {
    refetch();
  };

  const handleApplyFilters = () => {
    setAppliedFilters(filterParams);
  };

  const formatDuration = (createdAt: string | null) => {
    if (!createdAt) return '-';
    const createdAtMs = new Date(createdAt).getTime();
    if (Number.isNaN(createdAtMs)) return '-';
    const now = Date.now();
    const diff = now - createdAtMs;
    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    if (days > 0) return `${days}d ${hours % 24}h`;
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
  };

  const getStatusBadge = (status: string) => {
    const variant = {
      'Active': 'default',
      'Paused': 'secondary',
      'Completed': 'outline',
      'Error': 'destructive',
    }[status] as 'default' | 'secondary' | 'outline' | 'destructive' | undefined;
    
    return <Badge variant={variant}>{status}</Badge>;
  };

  const activeCount = liveQueries.filter(q => q.status === 'Active').length;
  const totalCount = liveQueries.length;

  return (
    <div className="space-y-4">
      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="p-4">
          <div className="flex items-center gap-3">
            <Activity className="h-8 w-8 text-green-600" />
            <div>
              <p className="text-sm text-muted-foreground">Active Queries</p>
              <p className="text-2xl font-bold">{activeCount}</p>
            </div>
          </div>
        </Card>
        <Card className="p-4">
          <div className="flex items-center gap-3">
            <Database className="h-8 w-8 text-blue-600" />
            <div>
              <p className="text-sm text-muted-foreground">Total Queries</p>
              <p className="text-2xl font-bold">{totalCount}</p>
            </div>
          </div>
        </Card>
        <Card className="p-4">
          <div className="flex items-center gap-3">
            <Clock className="h-8 w-8 text-orange-600" />
            <div>
              <p className="text-sm text-muted-foreground">Auto-refresh</p>
              <p className="text-lg font-semibold">Live</p>
            </div>
          </div>
        </Card>
      </div>

      {/* Filters */}
      <Card className="p-4">
        <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
          <Input
            placeholder="Filter by User ID"
            value={filters.user_id}
            onChange={(e) => setFilters({ ...filters, user_id: e.target.value })}
          />
          <Input
            placeholder="Filter by Namespace"
            value={filters.namespace_id}
            onChange={(e) => setFilters({ ...filters, namespace_id: e.target.value })}
          />
          <Input
            placeholder="Filter by Table"
            value={filters.table_name}
            onChange={(e) => setFilters({ ...filters, table_name: e.target.value })}
          />
          <Select value={filters.status} onValueChange={(value) => setFilters({ ...filters, status: value })}>
            <SelectTrigger>
              <SelectValue placeholder="All Statuses" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Statuses</SelectItem>
              <SelectItem value="Active">Active</SelectItem>
              <SelectItem value="Paused">Paused</SelectItem>
              <SelectItem value="Completed">Completed</SelectItem>
              <SelectItem value="Error">Error</SelectItem>
            </SelectContent>
          </Select>
          <div className="flex gap-2">
            <Button onClick={handleApplyFilters} className="flex-1">Apply</Button>
            <Button onClick={handleRefresh} variant="outline" size="icon">
              <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            </Button>
          </div>
        </div>
      </Card>

      {/* Success Message */}
      {successMessage && (
        <div className="flex items-center gap-2 p-4 text-sm text-green-700 bg-green-50 border border-green-200 rounded">
          <CheckCircle className="h-4 w-4" />
          {successMessage}
        </div>
      )}

      {/* Error Display */}
      {error && (
        <div className="p-4 text-sm text-red-700 bg-red-50 border border-red-200 rounded">
          {error}
        </div>
      )}

      {/* Kill Error Display */}
      {killError && (
        <div className="p-4 text-sm text-red-700 bg-red-50 border border-red-200 rounded">
          {killError}
        </div>
      )}

      {/* Table */}
      <Card>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Subscription ID</TableHead>
                <TableHead>User</TableHead>
                <TableHead>Namespace</TableHead>
                <TableHead>Table</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Duration</TableHead>
                <TableHead>Changes</TableHead>
                <TableHead>Query</TableHead>
                <TableHead>Node</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading && liveQueries.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={10} className="text-center py-8">
                    <Loader2 className="h-6 w-6 animate-spin mx-auto text-muted-foreground" />
                    <p className="text-sm text-muted-foreground mt-2">Loading live queries...</p>
                  </TableCell>
                </TableRow>
              ) : liveQueries.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={10} className="text-center py-8 text-muted-foreground">
                    No live queries found
                  </TableCell>
                </TableRow>
              ) : (
                liveQueries.map((query) => (
                  <TableRow key={query.live_id}>
                    <TableCell className="font-mono text-xs">{query.subscription_id}</TableCell>
                    <TableCell className="font-mono text-xs">{query.user_id}</TableCell>
                    <TableCell>{query.namespace_id}</TableCell>
                    <TableCell className="font-semibold">{query.table_name}</TableCell>
                    <TableCell>{getStatusBadge(query.status)}</TableCell>
                    <TableCell className="text-sm">{formatDuration(query.created_at)}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{query.changes}</Badge>
                    </TableCell>
                    <TableCell className="max-w-xs">
                      <div className="text-xs font-mono truncate" title={query.query}>
                        {query.query}
                      </div>
                    </TableCell>
                    <TableCell className="text-xs">{query.node_id}</TableCell>
                    <TableCell className="text-right">
                      <Button
                        size="sm"
                        variant="destructive"
                        onClick={() => handleKillQuery(query)}
                        disabled={killingIds.has(query.live_id)}
                      >
                        {killingIds.has(query.live_id) ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <>
                            <XCircle className="h-4 w-4 mr-1" />
                            Kill
                          </>
                        )}
                      </Button>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}
