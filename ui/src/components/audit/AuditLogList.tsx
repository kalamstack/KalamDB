import { useState, useMemo } from 'react';
import type { AuditLog, AuditLogFilters, AuditLogSortKey } from '@/services/auditLogService';
import { useGetAuditLogsQuery } from '@/store/apiSlice';
import { formatUtcTimestamp } from '@/lib/formatters';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Loader2, RefreshCw, Filter, X, Eye, ChevronLeft, ChevronRight, ArrowUp, ArrowDown, ArrowUpDown } from 'lucide-react';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { DEFAULT_PAGE_SIZE, PAGE_SIZE_OPTIONS } from '@/lib/config';

const ACTION_COLORS: Record<string, string> = {
  'CREATE': 'bg-green-100 text-green-800',
  'INSERT': 'bg-green-100 text-green-800',
  'UPDATE': 'bg-blue-100 text-blue-800',
  'DELETE': 'bg-red-100 text-red-800',
  'DROP': 'bg-red-100 text-red-800',
  'ALTER': 'bg-yellow-100 text-yellow-800',
  'LOGIN': 'bg-purple-100 text-purple-800',
  'LOGOUT': 'bg-gray-100 text-gray-800',
  'QUERY': 'bg-cyan-100 text-cyan-800',
  'SELECT': 'bg-cyan-100 text-cyan-800',
};

function getActionColor(action: string): string {
  // Check if action starts with any known action type
  for (const [key, color] of Object.entries(ACTION_COLORS)) {
    if (action.toUpperCase().startsWith(key)) {
      return color;
    }
  }
  return 'bg-gray-100 text-gray-800';
}


  export function AuditLogList() {
    const [showFilters, setShowFilters] = useState(false);
    const [selectedLog, setSelectedLog] = useState<AuditLog | null>(null);
    const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
    const [page, setPage] = useState(0);
    const [sortBy, setSortBy] = useState<AuditLogSortKey>("timestamp");
    const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
    const [draftFilters, setDraftFilters] = useState<AuditLogFilters>({
      limit: DEFAULT_PAGE_SIZE,
    });
    const [appliedFilters, setAppliedFilters] = useState<AuditLogFilters>({
      limit: DEFAULT_PAGE_SIZE,
      offset: 0,
      sortBy: "timestamp",
      sortDirection: "desc",
    });

    const handleSort = (key: AuditLogSortKey) => {
      const newDirection = sortBy === key
        ? (sortDirection === "asc" ? "desc" : "asc")
        : "asc";
      setSortBy(key);
      setSortDirection(newDirection);
      setPage(0);
      setAppliedFilters((prev) => ({
        ...prev,
        sortBy: key,
        sortDirection: newDirection,
        offset: 0,
      }));
    };
    const { data: fetchedLogs, isLoading, error: queryError, refetch } = useGetAuditLogsQuery(appliedFilters, {
      pollingInterval: 5000,
    });
  
    const logs = fetchedLogs || [];
  
    const error = queryError && "error" in queryError && typeof queryError.error === "string" 
      ? queryError.error 
      : queryError 
        ? "Failed to fetch audit logs" 
        : null;
  
    const handleApplyFilters = () => {
      setPage(0);
      setAppliedFilters({ ...draftFilters, limit: pageSize, offset: 0 });
      setShowFilters(false);
    };

    const handleClearFilters = () => {
      const clearedFilters = { limit: pageSize, offset: 0 };
      setPage(0);
      setDraftFilters({ limit: pageSize });
      setAppliedFilters(clearedFilters);
      setShowFilters(false);
    };

    const handlePageChange = (newPage: number) => {
      setPage(newPage);
      setAppliedFilters((prev) => ({ ...prev, offset: newPage * pageSize }));
    };

    const handlePageSizeChange = (newSize: number) => {
      setPageSize(newSize);
      setPage(0);
      setAppliedFilters((prev) => ({ ...prev, limit: newSize, offset: 0 }));
    };
  
    const handleRefresh = () => {
      refetch();
    };

  const hasActiveFilters = useMemo(
    () =>
      Boolean(
        appliedFilters.username ||
          appliedFilters.action ||
          appliedFilters.target ||
          appliedFilters.startDate ||
          appliedFilters.endDate,
      ),
    [appliedFilters],
  );

  if (error) {
    return (
      <Card className="border-red-200">
        <CardContent className="py-6">
          <p className="text-red-700">{error}</p>
          <Button variant="outline" onClick={handleRefresh} className="mt-2">
            Retry
          </Button>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="flex flex-col h-full min-h-0 gap-4">
      {/* Toolbar */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Button
            variant={showFilters ? 'secondary' : 'outline'}
            onClick={() => setShowFilters(!showFilters)}
          >
            <Filter className="h-4 w-4 mr-2" />
            Filters
            {hasActiveFilters && (
              <span className="ml-2 px-1.5 py-0.5 bg-primary text-primary-foreground rounded-full text-xs">
                Active
              </span>
            )}
          </Button>
          {hasActiveFilters && (
            <Button variant="ghost" size="sm" onClick={handleClearFilters}>
              <X className="h-4 w-4 mr-1" />
              Clear
            </Button>
          )}
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Select
              value={String(pageSize)}
              onValueChange={(v) => handlePageSizeChange(Number(v))}
            >
              <SelectTrigger className="h-8 w-[70px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {PAGE_SIZE_OPTIONS.map((size) => (
                  <SelectItem key={size} value={String(size)}>
                    {size}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <span>per page</span>
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon"
              className="h-8 w-8"
              disabled={page === 0}
              onClick={() => handlePageChange(page - 1)}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="text-sm text-muted-foreground px-2">
              {page + 1}
            </span>
            <Button
              variant="outline"
              size="icon"
              className="h-8 w-8"
              disabled={logs.length < pageSize}
              onClick={() => handlePageChange(page + 1)}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
          <Button variant="outline" size="icon" className="h-8 w-8" onClick={handleRefresh} disabled={isLoading} aria-label="Refresh audit logs">
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          </Button>
        </div>
      </div>

      {/* Filter Panel */}
      {showFilters && (
        <Card>
          <CardContent className="pt-4">
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
              <div className="space-y-1">
                <label className="text-sm font-medium">Username</label>
                <Input
                  placeholder="Filter by username"
                  value={draftFilters.username || ''}
                  onChange={(e) => setDraftFilters({ ...draftFilters, username: e.target.value || undefined })}
                />
              </div>
              <div className="space-y-1">
                <label className="text-sm font-medium">Action</label>
                <Input
                  placeholder="e.g., CREATE, DELETE"
                  value={draftFilters.action || ''}
                  onChange={(e) => setDraftFilters({ ...draftFilters, action: e.target.value || undefined })}
                />
              </div>
              <div className="space-y-1">
                <label className="text-sm font-medium">Target</label>
                <Input
                  placeholder="Filter by target"
                  value={draftFilters.target || ''}
                  onChange={(e) => setDraftFilters({ ...draftFilters, target: e.target.value || undefined })}
                />
              </div>
              <div className="flex items-end">
                <Button onClick={handleApplyFilters} className="w-full">
                  <Filter className="mr-2 h-4 w-4" />
                  Apply Filters
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Table */}
      {isLoading && logs.length === 0 ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : logs.length === 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>No Audit Logs Found</CardTitle>
            <CardDescription>
              {hasActiveFilters 
                ? 'No logs match the current filters. Try adjusting your filters.'
                : 'No audit logs have been recorded yet.'}
            </CardDescription>
          </CardHeader>
        </Card>
      ) : (
        <div className="border rounded-lg flex-1 min-h-0 overflow-hidden [&>div]:h-full [&>div]:overflow-auto">
          <Table className="border-separate border-spacing-0 [&_td]:border-b [&_td]:border-border">
            <TableHeader>
              <TableRow>
                {([
                  ["timestamp", "Timestamp"],
                  ["actor_username", "User"],
                  ["action", "Action"],
                  ["target", "Target"],
                  ["ip_address", "IP Address"],
                ] as [AuditLogSortKey, string][]).map(([key, label]) => (
                  <TableHead
                    key={key}
                    className="sticky top-0 bg-background z-10 border-b border-border cursor-pointer select-none hover:text-foreground"
                    onClick={() => handleSort(key)}
                  >
                    <span className="inline-flex items-center gap-1">
                      {label}
                      {sortBy === key
                        ? (sortDirection === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />)
                        : <ArrowUpDown className="h-3 w-3 opacity-30" />}
                    </span>
                  </TableHead>
                ))}
                <TableHead className="sticky top-0 bg-background z-10 border-b border-border w-[80px]">Details</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {logs.map((log) => (
                <TableRow key={log.audit_id}>
                  <TableCell className="font-mono text-sm">
                    {formatUtcTimestamp(log.timestamp)}
                  </TableCell>
                  <TableCell>
                    <div>
                      <div className="font-medium">{log.actor_username}</div>
                      <div className="text-xs text-muted-foreground font-mono">
                        {log.actor_user_id.substring(0, 8)}...
                      </div>
                    </div>
                  </TableCell>
                  <TableCell>
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${getActionColor(log.action)}`}>
                      {log.action}
                    </span>
                  </TableCell>
                  <TableCell className="max-w-[300px] truncate" title={log.target}>
                    {log.target}
                  </TableCell>
                  <TableCell className="text-muted-foreground font-mono text-sm">
                    {log.ip_address || '-'}
                  </TableCell>
                  <TableCell>
                    {log.details && (
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => setSelectedLog(log)}
                        title="View details"
                        aria-label={`View details for audit log ${log.audit_id}`}
                      >
                        <Eye className="h-4 w-4" />
                      </Button>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {/* Details Dialog */}
      <Dialog open={!!selectedLog} onOpenChange={(open) => !open && setSelectedLog(null)}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Audit Log Details</DialogTitle>
            <DialogDescription>
              {selectedLog && formatUtcTimestamp(selectedLog.timestamp)}
            </DialogDescription>
          </DialogHeader>
          {selectedLog && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="text-muted-foreground">Audit ID:</span>
                  <p className="font-mono">{selectedLog.audit_id}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">Action:</span>
                  <p>
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${getActionColor(selectedLog.action)}`}>
                      {selectedLog.action}
                    </span>
                  </p>
                </div>
                <div>
                  <span className="text-muted-foreground">User:</span>
                  <p>{selectedLog.actor_username}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">User ID:</span>
                  <p className="font-mono text-xs">{selectedLog.actor_user_id}</p>
                </div>
                <div className="col-span-2">
                  <span className="text-muted-foreground">Target:</span>
                  <p className="font-mono">{selectedLog.target}</p>
                </div>
                <div>
                  <span className="text-muted-foreground">IP Address:</span>
                  <p className="font-mono">{selectedLog.ip_address || '-'}</p>
                </div>
              </div>
              {selectedLog.details && (
                <div>
                  <span className="text-muted-foreground text-sm">Details:</span>
                  <pre className="mt-1 p-3 bg-muted rounded-md text-sm font-mono whitespace-pre-wrap overflow-auto max-h-[300px]">
                    {selectedLog.details}
                  </pre>
                </div>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
