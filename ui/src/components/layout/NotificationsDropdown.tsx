import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { Job } from '@/services/jobService';
import type { AuditLog } from '@/services/auditLogService';
import { useGetNotificationsQuery } from '@/store/apiSlice';
import { Button } from '@/components/ui/button';
import { CodeBlock } from '@/components/ui/code-block';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Bell, Briefcase, Play, CheckCircle, Clock, ScrollText, RefreshCw, ExternalLink } from 'lucide-react';
import { cn } from '@/lib/utils';

const STATUS_COLORS: Record<string, string> = {
  'New': 'bg-muted text-muted-foreground',
  'Queued': 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-200',
  'Running': 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-200',
  'Completed': 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-200',
  'Failed': 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-200',
};

const ACTION_COLORS: Record<string, string> = {
  'CREATE': 'text-green-600 dark:text-green-300',
  'INSERT': 'text-green-600 dark:text-green-300',
  'UPDATE': 'text-blue-600 dark:text-blue-300',
  'DELETE': 'text-red-600 dark:text-red-300',
  'DROP': 'text-red-600 dark:text-red-300',
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

function getActionColor(action: string): string {
  for (const [key, color] of Object.entries(ACTION_COLORS)) {
    if (action.toUpperCase().startsWith(key)) {
      return color;
    }
  }
  return 'text-muted-foreground';
}

function formatTimestamp(timestamp: string | number | null): string {
  if (!timestamp) return '-';
  try {
    const date = typeof timestamp === 'number' 
      ? new Date(timestamp) 
      : new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    
    if (diffMs < 60000) return 'Just now';
    if (diffMs < 3600000) return `${Math.floor(diffMs / 60000)}m ago`;
    if (diffMs < 86400000) return `${Math.floor(diffMs / 3600000)}h ago`;
    return date.toLocaleDateString();
  } catch {
    return String(timestamp);
  }
}

export function NotificationsDropdown() {
  const navigate = useNavigate();
  const {
    data: notifications,
    isFetching: isLoading,
    refetch,
  } = useGetNotificationsQuery(undefined, {
    pollingInterval: 10000,
  });
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [selectedAuditLog, setSelectedAuditLog] = useState<AuditLog | null>(null);
  const [isOpen, setIsOpen] = useState(false);

  const runningJobs = notifications?.runningJobs ?? [];
  const recentAuditLogs = notifications?.recentAuditLogs ?? [];
  const runningJobsCount = notifications?.runningJobsCount ?? 0;
  const lastFetchTime = notifications?.fetchedAt ? new Date(notifications.fetchedAt) : null;

  return (
    <>
      <DropdownMenu open={isOpen} onOpenChange={setIsOpen}>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon" className="relative" aria-label="Open notifications">
            <Bell className="h-5 w-5" />
            {runningJobsCount > 0 && (
              <span className="absolute -top-1 -right-1 flex h-5 w-5 items-center justify-center">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-yellow-400 opacity-75"></span>
                <span className="relative inline-flex h-4 w-4 items-center justify-center rounded-full bg-yellow-500 text-[10px] font-bold text-white">
                  {runningJobsCount}
                </span>
              </span>
            )}
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-[380px]">
          <DropdownMenuLabel className="flex items-center justify-between">
            <span>Notifications</span>
            <div className="flex items-center gap-2">
              {lastFetchTime && (
                <span className="text-xs text-muted-foreground font-normal">
                  {formatTimestamp(lastFetchTime.toISOString())}
                </span>
              )}
                <Button 
                variant="ghost" 
                size="icon" 
                className="h-6 w-6" 
                aria-label="Refresh notifications"
                title="Refresh notifications"
                onClick={(e) => {
                  e.preventDefault();
                  void refetch();
                }}
              >
                <RefreshCw className={cn("h-3 w-3", isLoading && "animate-spin")} />
              </Button>
            </div>
          </DropdownMenuLabel>
          <DropdownMenuSeparator />
          
          <Tabs defaultValue="jobs" className="w-full">
            <TabsList className="w-full grid grid-cols-2 h-8">
              <TabsTrigger value="jobs" className="gap-1.5 text-xs">
                <Briefcase className="h-3.5 w-3.5" />
                Jobs {runningJobsCount > 0 && `(${runningJobsCount} running)`}
              </TabsTrigger>
              <TabsTrigger value="audit" className="gap-1.5 text-xs">
                <ScrollText className="h-3.5 w-3.5" />
                Audit Logs
              </TabsTrigger>
            </TabsList>
            
            <TabsContent value="jobs" className="max-h-[300px] overflow-auto">
              {runningJobs.length === 0 ? (
                <div className="p-4 text-center text-sm text-muted-foreground">
                  No active jobs
                </div>
              ) : (
                <div className="py-1">
                  {runningJobs.map((job) => (
                    <DropdownMenuItem
                      key={job.job_id}
                      className="flex items-start gap-3 p-3 cursor-pointer"
                      onClick={() => {
                        setSelectedJob(job);
                        setIsOpen(false);
                      }}
                    >
                      <div className="mt-0.5">
                        {job.status === 'Running' ? (
                          <Play className="h-4 w-4 text-yellow-500 animate-pulse" />
                        ) : job.status === 'Queued' ? (
                          <Clock className="h-4 w-4 text-blue-500" />
                        ) : (
                          <CheckCircle className="h-4 w-4 text-green-500" />
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-sm">{job.job_type}</span>
                          <span className={cn(
                            "px-1.5 py-0.5 rounded text-[10px] font-medium",
                            STATUS_COLORS[job.status] || 'bg-gray-100'
                          )}>
                            {job.status}
                          </span>
                        </div>
                        <p className="text-xs text-muted-foreground truncate">
                          {(() => {
                            const params = parseJobParams(job.parameters);
                            return `${params.namespace_id || '-'}${params.table_name ? `.${params.table_name}` : ''}`;
                          })()}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {formatTimestamp(job.created_at)}
                        </p>
                      </div>
                    </DropdownMenuItem>
                  ))}
                </div>
              )}
              <DropdownMenuSeparator />
              <DropdownMenuItem
                className="justify-center text-sm text-primary"
                onClick={() => {
                  navigate('/jobs');
                  setIsOpen(false);
                }}
              >
                View All Jobs <ExternalLink className="h-3 w-3 ml-1" />
              </DropdownMenuItem>
            </TabsContent>
            
            <TabsContent value="audit" className="max-h-[300px] overflow-auto">
              {recentAuditLogs.length === 0 ? (
                <div className="p-4 text-center text-sm text-muted-foreground">
                  No recent activity
                </div>
              ) : (
                <div className="py-1">
                  {recentAuditLogs.map((log) => (
                    <DropdownMenuItem
                      key={log.audit_id}
                      className="flex items-start gap-3 p-3 cursor-pointer"
                      onClick={() => {
                        setSelectedAuditLog(log);
                        setIsOpen(false);
                      }}
                    >
                      <div className="mt-0.5">
                        <ScrollText className={cn("h-4 w-4", getActionColor(log.action))} />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className={cn("font-medium text-sm", getActionColor(log.action))}>
                            {log.action}
                          </span>
                        </div>
                        <p className="text-xs text-muted-foreground truncate">
                          {log.target}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {log.actor_user_id} • {formatTimestamp(log.timestamp)}
                        </p>
                      </div>
                    </DropdownMenuItem>
                  ))}
                </div>
              )}
              <DropdownMenuSeparator />
              <DropdownMenuItem
                className="justify-center text-sm text-primary"
                onClick={() => {
                  navigate('/logging/audit');
                  setIsOpen(false);
                }}
              >
                View All Audit Logs <ExternalLink className="h-3 w-3 ml-1" />
              </DropdownMenuItem>
            </TabsContent>
          </Tabs>
        </DropdownMenuContent>
      </DropdownMenu>

      {/* Job Details Dialog */}
      <Dialog open={!!selectedJob} onOpenChange={() => setSelectedJob(null)}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              Job: {selectedJob?.job_type}
              {selectedJob && (
                <span className={cn(
                  "px-2 py-0.5 rounded text-xs font-medium",
                  STATUS_COLORS[selectedJob.status] || 'bg-gray-100'
                )}>
                  {selectedJob.status}
                </span>
              )}
            </DialogTitle>
            <DialogDescription>
              {selectedJob?.job_id}
            </DialogDescription>
          </DialogHeader>
          {selectedJob && (
            <div className="space-y-3 text-sm">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="text-muted-foreground">Target</label>
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
                  <label className="text-muted-foreground">Node</label>
                  <p>{selectedJob.node_id}</p>
                </div>
                <div>
                  <label className="text-muted-foreground">Created</label>
                  <p>{formatTimestamp(selectedJob.created_at)}</p>
                </div>
                <div>
                  <label className="text-muted-foreground">Started</label>
                  <p>{formatTimestamp(selectedJob.started_at)}</p>
                </div>
              </div>
              <div className="pt-2">
                <Button
                  variant="outline"
                  className="w-full"
                  onClick={() => {
                    navigate('/jobs');
                    setSelectedJob(null);
                  }}
                >
                  View in Jobs Page
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Audit Log Details Dialog */}
      <Dialog open={!!selectedAuditLog} onOpenChange={() => setSelectedAuditLog(null)}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle className={cn("flex items-center gap-2", getActionColor(selectedAuditLog?.action || ''))}>
              {selectedAuditLog?.action}
            </DialogTitle>
            <DialogDescription>
              Audit ID: {selectedAuditLog?.audit_id}
            </DialogDescription>
          </DialogHeader>
          {selectedAuditLog && (
            <div className="space-y-3 text-sm">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="text-muted-foreground">User</label>
                  <p className="font-medium font-mono">{selectedAuditLog.actor_user_id}</p>
                </div>
                <div>
                  <label className="text-muted-foreground">Timestamp</label>
                  <p>{new Date(selectedAuditLog.timestamp).toLocaleString()}</p>
                </div>
                <div className="col-span-2">
                  <label className="text-muted-foreground">Target</label>
                  <p className="font-medium">{selectedAuditLog.target}</p>
                </div>
                {selectedAuditLog.ip_address && (
                  <div>
                    <label className="text-muted-foreground">IP Address</label>
                    <p>{selectedAuditLog.ip_address}</p>
                  </div>
                )}
              </div>
              {selectedAuditLog.details && (
                <div>
                  <label className="text-muted-foreground">Details</label>
                  <div className="mt-1">
                    <CodeBlock value={selectedAuditLog.details} jsonPreferred maxHeightClassName="max-h-[240px]" />
                  </div>
                </div>
              )}
              <div className="pt-2">
                <Button
                  variant="outline"
                  className="w-full"
                  onClick={() => {
                    navigate('/logging/audit');
                    setSelectedAuditLog(null);
                  }}
                >
                  View in Audit Logs
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
