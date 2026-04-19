import { useState } from 'react';
import type { Job } from '@/services/jobService';
import type { JobFilters, JobSortKey } from '@/services/sql/queries/jobQueries';
import { useGetJobsFilteredQuery } from '@/store/apiSlice';
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
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { CodeBlock } from '@/components/ui/code-block';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Loader2, RefreshCw, Filter, X, Eye, Play, CheckCircle, XCircle, Clock, AlertCircle, ChevronLeft, ChevronRight, ArrowUp, ArrowDown, ArrowUpDown } from 'lucide-react';
import { formatTimestamp, toMilliseconds } from '@/lib/formatters';
import { DEFAULT_PAGE_SIZE, PAGE_SIZE_OPTIONS } from '@/lib/config';

const STATUS_COLORS: Record<string, string> = {
  'New': 'bg-gray-100 text-gray-800',
  'Queued': 'bg-blue-100 text-blue-800',
  'Running': 'bg-yellow-100 text-yellow-800',
  'Completed': 'bg-green-100 text-green-800',
  'Failed': 'bg-red-100 text-red-800',
  'Cancelled': 'bg-gray-100 text-gray-800',
  'Retrying': 'bg-orange-100 text-orange-800',
  'Skipped': 'bg-purple-100 text-purple-800',
};

const STATUS_ICONS: Record<string, React.ReactNode> = {
  'New': <Clock className="h-3 w-3" />,
  'Queued': <Clock className="h-3 w-3" />,
  'Running': <Play className="h-3 w-3 animate-pulse" />,
  'Completed': <CheckCircle className="h-3 w-3" />,
  'Failed': <XCircle className="h-3 w-3" />,
  'Cancelled': <XCircle className="h-3 w-3" />,
  'Retrying': <AlertCircle className="h-3 w-3" />,
  'Skipped': <AlertCircle className="h-3 w-3" />,
};

// Helper to extract namespace_id and table_name from parameters JSON
function parseJobParams(parameters: unknown): { namespace_id?: string; table_name?: string } {
  if (parameters === null || parameters === undefined) return {};

  if (typeof parameters === 'object') {
    return parameters as { namespace_id?: string; table_name?: string };
  }

  if (typeof parameters !== 'string') {
    return {};
  }

  try {
    return JSON.parse(parameters);
  } catch {
    return {};
  }
}

function stringifyJobParameters(parameters: unknown): string {
  if (typeof parameters === 'string') {
    return parameters;
  }

  try {
    return JSON.stringify(parameters, null, 2);
  } catch {
    return String(parameters);
  }
}

function getStatusColor(status: string): string {
  const capitalized = status.charAt(0).toUpperCase() + status.slice(1).toLowerCase();
  return STATUS_COLORS[capitalized] || STATUS_COLORS[status] || 'bg-gray-100 text-gray-800';
}

function formatDuration(startedAt: string | number | null, finishedAt: string | number | null): string {
  if (!startedAt) return '-';
  
  // Parse timestamps - they could be ISO strings or numeric timestamps
  // Try to parse as number first (microsecond timestamps from backend)
  let startMs: number;
  let endMs: number;
  
  const startNum = Number(startedAt);
  if (!isNaN(startNum) && startNum > 1000000000) {
    // It's a numeric timestamp (seconds, milliseconds, or microseconds)
    startMs = toMilliseconds(startNum);
  } else {
    // It's an ISO string
    startMs = new Date(startedAt).getTime();
  }
  
  if (finishedAt) {
    const endNum = Number(finishedAt);
    if (!isNaN(endNum) && endNum > 1000000000) {
      endMs = toMilliseconds(endNum);
    } else {
      endMs = new Date(finishedAt).getTime();
    }
  } else {
    endMs = Date.now();
  }
  
  const durationMs = Math.abs(endMs - startMs);
  
  if (durationMs < 1000) return `${Math.round(durationMs)}ms`;
  if (durationMs < 60000) return `${(durationMs / 1000).toFixed(1)}s`;
  if (durationMs < 3600000) return `${(durationMs / 60000).toFixed(1)}m`;
  return `${(durationMs / 3600000).toFixed(1)}h`;
}

interface JobListProps {
  initialFilters?: JobFilters;
  compact?: boolean;
  onJobClick?: (job: Job) => void;
}

export function JobList({ initialFilters, compact = false, onJobClick }: JobListProps) {
  const [showFilters, setShowFilters] = useState(false);
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
  const [page, setPage] = useState(0);
  const [sortBy, setSortBy] = useState<JobSortKey>("created_at");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [draftFilters, setDraftFilters] = useState<JobFilters>(initialFilters || {
    limit: DEFAULT_PAGE_SIZE,
  });
  const [appliedFilters, setAppliedFilters] = useState<JobFilters>(initialFilters || {
    limit: DEFAULT_PAGE_SIZE,
    offset: 0,
    sortBy: "created_at",
    sortDirection: "desc",
  });
  const { data: jobs = [], isLoading, error, refetch } = useGetJobsFilteredQuery(appliedFilters);
  const errorMessage =
    error && "error" in error && typeof error.error === "string"
      ? error.error
      : error
        ? "Failed to fetch jobs"
        : null;

  const handleApplyFilters = () => {
    setPage(0);
    setAppliedFilters({ ...draftFilters, limit: pageSize, offset: 0, sortBy, sortDirection });
    setShowFilters(false);
  };

  const handleClearFilters = () => {
    setPage(0);
    setDraftFilters({ limit: pageSize });
    setAppliedFilters({ limit: pageSize, offset: 0, sortBy, sortDirection });
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

  const handleSort = (key: JobSortKey) => {
    const newDirection = sortBy === key
      ? (sortDirection === "asc" ? "desc" : "asc")
      : "asc";
    setSortBy(key);
    setSortDirection(newDirection);
    setPage(0);
    setAppliedFilters((prev) => ({ ...prev, sortBy: key, sortDirection: newDirection, offset: 0 }));
  };

  const hasActiveFilters = appliedFilters.status || appliedFilters.job_type;

  const handleJobClick = (job: Job) => {
    if (onJobClick) {
      onJobClick(job);
    } else {
      setSelectedJob(job);
    }
  };

  if (errorMessage) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertTitle>Unable to load jobs</AlertTitle>
        <AlertDescription className="mt-2 space-y-3">
          <p>{errorMessage}</p>
          <Button variant="outline" onClick={() => refetch()}>
            Retry
          </Button>
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <div className="flex flex-col h-full min-h-0 gap-4">
      {/* Toolbar */}
      {!compact && (
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
              <Button variant="outline" size="icon" className="h-8 w-8" disabled={page === 0} onClick={() => handlePageChange(page - 1)}>
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="text-sm text-muted-foreground px-2">{page + 1}</span>
              <Button variant="outline" size="icon" className="h-8 w-8" disabled={jobs.length < pageSize} onClick={() => handlePageChange(page + 1)}>
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
            <Button variant="outline" size="icon" className="h-8 w-8" onClick={() => refetch()} disabled={isLoading} aria-label="Refresh jobs">
              <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            </Button>
          </div>
        </div>
      )}

      {/* Filter Panel */}
      {showFilters && !compact && (
        <Card>
          <CardContent className="pt-4">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="space-y-1">
                <label className="text-sm font-medium">Status</label>
                <Select
                  value={draftFilters.status || 'all'}
                  onValueChange={(value) => setDraftFilters({ ...draftFilters, status: value === 'all' ? undefined : value })}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="All statuses" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All</SelectItem>
                    <SelectItem value="New">New</SelectItem>
                    <SelectItem value="Queued">Queued</SelectItem>
                    <SelectItem value="Running">Running</SelectItem>
                    <SelectItem value="Completed">Completed</SelectItem>
                    <SelectItem value="Failed">Failed</SelectItem>
                    <SelectItem value="Cancelled">Cancelled</SelectItem>
                    <SelectItem value="Retrying">Retrying</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <label className="text-sm font-medium">Job Type</label>
                <Input
                  placeholder="e.g., Flush, Cleanup"
                  value={draftFilters.job_type || ''}
                  onChange={(e) => setDraftFilters({ ...draftFilters, job_type: e.target.value || undefined })}
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
      {isLoading && jobs.length === 0 ? (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : jobs.length === 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>No Jobs Found</CardTitle>
            <CardDescription>
              {hasActiveFilters 
                ? 'No jobs match the current filters. Try adjusting your filters.'
                : 'No jobs have been recorded yet.'}
            </CardDescription>
          </CardHeader>
        </Card>
      ) : (
        <div className="border rounded-lg flex-1 min-h-0 overflow-hidden [&>div]:h-full [&>div]:overflow-auto">
          <Table className="border-separate border-spacing-0 [&_td]:border-b [&_td]:border-border">
            <TableHeader>
              <TableRow>
                {([
                  ["status", "Status"],
                  ["job_type", "Job Type"],
                  [null, "Namespace / Table"],
                  ["created_at", "Created"],
                  [null, "Duration"],
                ] as [JobSortKey | null, string][]).map(([key, label], i) => (
                  <TableHead
                    key={key ?? `col-${i}`}
                    className={`sticky top-0 bg-background z-10 border-b border-border ${key ? "cursor-pointer select-none hover:text-foreground" : ""}`}
                    onClick={key ? () => handleSort(key) : undefined}
                  >
                    <span className="inline-flex items-center gap-1">
                      {label}
                      {key && (sortBy === key
                        ? (sortDirection === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />)
                        : <ArrowUpDown className="h-3 w-3 opacity-30" />)}
                    </span>
                  </TableHead>
                ))}
                {!compact && (
                  <TableHead
                    className="sticky top-0 bg-background z-10 border-b border-border cursor-pointer select-none hover:text-foreground"
                    onClick={() => handleSort("node_id")}
                  >
                    <span className="inline-flex items-center gap-1">
                      Node
                      {sortBy === "node_id"
                        ? (sortDirection === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />)
                        : <ArrowUpDown className="h-3 w-3 opacity-30" />}
                    </span>
                  </TableHead>
                )}
                <TableHead className="sticky top-0 bg-background z-10 border-b border-border w-[50px]"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {jobs.map((job) => (
                <TableRow 
                  key={job.job_id} 
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => handleJobClick(job)}
                >
                  <TableCell>
                    <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(job.status)}`}>
                      {STATUS_ICONS[job.status.charAt(0).toUpperCase() + job.status.slice(1).toLowerCase()] || STATUS_ICONS[job.status]}
                      {job.status}
                    </span>
                  </TableCell>
                  <TableCell className="font-medium">{job.job_type}</TableCell>
                  <TableCell>
                    {(() => {
                      const params = parseJobParams(job.parameters);
                      return (
                        <>
                          <span className="text-muted-foreground">{params.namespace_id || '-'}</span>
                          {params.table_name && (
                            <span>.{params.table_name}</span>
                          )}
                        </>
                      );
                    })()}
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {formatTimestamp(job.created_at)}
                  </TableCell>
                  <TableCell className="text-sm">
                    {job.status === 'Running' ? (
                      <span className="text-yellow-600 font-medium">
                        {formatDuration(job.started_at, null)}
                      </span>
                    ) : (
                      formatDuration(job.started_at, job.finished_at)
                    )}
                  </TableCell>
                  {!compact && (
                    <TableCell className="text-sm text-muted-foreground">
                      {job.node_id}
                    </TableCell>
                  )}
                  <TableCell>
                    <Button variant="ghost" size="sm" aria-label={`View details for job ${job.job_id}`} title="View job details">
                      <Eye className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}

      {/* Job Details Dialog */}
      <Dialog open={!!selectedJob} onOpenChange={() => setSelectedJob(null)}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              Job Details
              {selectedJob && (
                <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(selectedJob.status)}`}>
                  {STATUS_ICONS[selectedJob.status.charAt(0).toUpperCase() + selectedJob.status.slice(1).toLowerCase()] || STATUS_ICONS[selectedJob.status]}
                  {selectedJob.status}
                </span>
              )}
            </DialogTitle>
            <DialogDescription>
              {selectedJob?.job_id}
            </DialogDescription>
          </DialogHeader>
          {selectedJob && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Job Type</label>
                  <p className="font-medium">{selectedJob.job_type}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Target</label>
                  <p className="font-medium">
                    {(() => {
                      const params = parseJobParams(selectedJob.parameters);
                      return (
                        <>
                          {params.namespace_id || '-'}
                          {params.table_name && `.${params.table_name}`}
                        </>
                      );
                    })()}
                  </p>
                </div>
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Created At</label>
                  <p>{formatTimestamp(selectedJob.created_at)}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Started At</label>
                  <p>{formatTimestamp(selectedJob.started_at)}</p>
                </div>
                <div>
                    <label className="text-sm font-medium text-muted-foreground">Finished At</label>
                    <p>{formatTimestamp(selectedJob.finished_at)}</p>
                  </div>
                  <div>
                    <label className="text-sm font-medium text-muted-foreground">Duration</label>
                    <p>{formatDuration(selectedJob.started_at, selectedJob.finished_at)}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Node</label>
                  <p>{selectedJob.node_id}</p>
                </div>
              </div>
              
              {selectedJob.message && selectedJob.status?.toLowerCase() === 'failed' && (
                <div>
                  <Alert variant="destructive">
                    <AlertCircle className="h-4 w-4" />
                    <AlertTitle>Execution error</AlertTitle>
                    <AlertDescription className="mt-2">
                      <CodeBlock
                        value={selectedJob.message}
                        jsonPreferred
                        maxHeightClassName="max-h-[260px]"
                        className="border-destructive/40 bg-destructive/5"
                      />
                    </AlertDescription>
                  </Alert>
                </div>
              )}
              {selectedJob.message && selectedJob.status?.toLowerCase() !== 'failed' && (
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Message</label>
                  <CodeBlock value={selectedJob.message} jsonPreferred maxHeightClassName="max-h-[260px]" />
                </div>
              )}

              {Boolean(selectedJob.parameters) && (
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Parameters</label>
                  <CodeBlock value={stringifyJobParameters(selectedJob.parameters)} jsonPreferred maxHeightClassName="max-h-[260px]" />
                </div>
              )}

              {selectedJob.exception_trace && (
                <div>
                  <label className="text-sm font-medium text-muted-foreground">Stack Trace</label>
                  <CodeBlock value={selectedJob.exception_trace} maxHeightClassName="max-h-[260px]" />
                </div>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
