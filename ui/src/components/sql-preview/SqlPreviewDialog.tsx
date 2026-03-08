/**
 * SqlPreviewDialog - A globally reusable dialog that displays SQL commands
 * with syntax highlighting, a progress bar, status indicator, audit log, and commit/cancel controls.
 *
 * Features:
 *   - Monaco-based SQL editor with syntax coloring (same theme as SQL Studio)
 *   - Editable SQL before commit; becomes read-only once executing
 *   - Progress bar for long-running executions
 *   - Status messages next to action buttons
 *   - Per-statement execution audit with status and timing
 *   - Commit (execute all), Cancel, and Discard buttons
 *   - Can be invoked from anywhere via the SqlPreviewProvider context
 *
 * This component is designed to be rendered once at the app root and
 * controlled via the useSqlPreview() hook.
 */

import { useState, useCallback, useRef, useEffect } from 'react';
// Temporarily disabled Monaco for debugging
// import MonacoEditor from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import {
  Play,
  X,
  AlertCircle,
  CheckCircle2,
  Loader2,
  Undo2,
} from 'lucide-react';

// ─── Types ───────────────────────────────────────────────────────────────────

export type SqlPreviewStatus = 'idle' | 'executing' | 'success' | 'error';
export type StatementStatus = 'pending' | 'running' | 'success' | 'failed';

export interface StatementAudit {
  id: number;
  statement: string;
  status: StatementStatus;
  timeTook?: number; // milliseconds
  error?: string;
}

export interface SqlPreviewOptions {
  /** The SQL to display in the editor. */
  sql: string;
  /** Dialog title. */
  title?: string;
  /** Optional description below the title. */
  description?: string;
  /** Callback to execute a single SQL statement. Should throw on error. */
  onExecute: (sql: string) => Promise<void>;
  /** Callback when all statements complete successfully. */
  onComplete?: () => void | Promise<void>;
  /** Callback when user clicks Discard / Cancel. */
  onDiscard?: () => void;
  /** Whether the SQL is editable before commit (default: true). */
  editable?: boolean;
}

interface SqlPreviewDialogProps {
  open: boolean;
  options: SqlPreviewOptions | null;
  onClose: () => void;
}

function formatAuditErrorMessage(message: string): string {
  const trimmed = message.trim();
  if (!trimmed) {
    return "Unknown error";
  }

  try {
    const parsed = JSON.parse(trimmed) as unknown;
    return JSON.stringify(parsed, null, 2);
  } catch {
    return trimmed;
  }
}

// ─── Component ───────────────────────────────────────────────────────────────

export function SqlPreviewDialog({ open, options, onClose }: SqlPreviewDialogProps) {
  const [sql, setSql] = useState('');
  const [status, setStatus] = useState<SqlPreviewStatus>('idle');
  const [statusMessage, setStatusMessage] = useState<string>('');
  const [progress, setProgress] = useState(0);
  const [isReadOnly, setIsReadOnly] = useState(false);
  const [auditLog, setAuditLog] = useState<StatementAudit[]>([]);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const progressTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Sync SQL when options change
  useEffect(() => {
    if (options?.sql) {
      setSql(options.sql);
      setStatus('idle');
      setStatusMessage('');
      setProgress(0);
      setAuditLog([]);
      setIsReadOnly(options.editable === false);
      // Also update the editor directly if it's already mounted
      if (editorRef.current) {
        editorRef.current.setValue(options.sql);
      }
    }
  }, [options]);

  // When dialog opens, wait a tick for Monaco to mount then force update
  useEffect(() => {
    if (open && sql) {
      const timer = setTimeout(() => {
        // Force update the editor if it's mounted
        if (editorRef.current && sql) {
          editorRef.current.setValue(sql);
        }
      }, 100);
      return () => clearTimeout(timer);
    }
  }, [open, sql]);

  // Clean up progress timer
  useEffect(() => {
    return () => {
      if (progressTimerRef.current) clearInterval(progressTimerRef.current);
    };
  }, []);

  /** Start a fake progress bar that increases over time. */
  const startProgress = useCallback(() => {
    setProgress(0);
    if (progressTimerRef.current) clearInterval(progressTimerRef.current);
    progressTimerRef.current = setInterval(() => {
      setProgress((prev) => {
        // Slow down as we approach 90% – never reach 100% until actual completion
        if (prev >= 90) return prev + 0.1;
        if (prev >= 70) return prev + 0.5;
        if (prev >= 50) return prev + 1;
        return prev + 3;
      });
    }, 200);
  }, []);

  const stopProgress = useCallback((complete: boolean) => {
    if (progressTimerRef.current) {
      clearInterval(progressTimerRef.current);
      progressTimerRef.current = null;
    }
    setProgress(complete ? 100 : 0);
  }, []);

  /** Execute the SQL statements one-by-one and update audit log. */
  const handleCommit = useCallback(async () => {
    if (!options?.onExecute || !sql.trim()) {
      console.warn('[SqlPreviewDialog] Cannot commit - no onExecute or empty SQL');
      return;
    }

    // Parse statements (split by newline, filter empty/comments)
    const statements = sql
      .split('\n')
      .map((s) => s.trim())
      .filter((s) => s.length > 0 && !s.startsWith('--'));

    if (statements.length === 0) {
      setStatusMessage('No statements to execute');
      return;
    }

    // Initialize audit log with all statements as 'pending'
    const initialAudit: StatementAudit[] = statements.map((stmt, idx) => ({
      id: idx + 1,
      statement: stmt,
      status: 'pending',
    }));
    setAuditLog(initialAudit);

    setStatus('executing');
    setStatusMessage('Executing statements...');
    setIsReadOnly(true);
    startProgress();

    let successCount = 0;
    let failedCount = 0;

    // Execute each statement sequentially
    for (let i = 0; i < statements.length; i++) {
      const stmt = statements[i];
      const startTime = performance.now();

      // Update audit: mark as running
      setAuditLog((prev) =>
        prev.map((item) =>
          item.id === i + 1 ? { ...item, status: 'running' as StatementStatus } : item
        )
      );

      try {
        await options.onExecute(stmt);
        const endTime = performance.now();
        const timeTook = Math.round(endTime - startTime);

        // Update audit: mark as success
        setAuditLog((prev) =>
          prev.map((item) =>
            item.id === i + 1
              ? { ...item, status: 'success' as StatementStatus, timeTook }
              : item
          )
        );
        successCount++;
      } catch (err) {
        const endTime = performance.now();
        const timeTook = Math.round(endTime - startTime);
        const errorMsg = err instanceof Error ? err.message : 'Unknown error';
        const formattedError = formatAuditErrorMessage(errorMsg);

        // Update audit: mark as failed
        setAuditLog((prev) =>
          prev.map((item) =>
            item.id === i + 1
              ? { ...item, status: 'failed' as StatementStatus, timeTook, error: formattedError }
              : item
          )
        );
        failedCount++;
      }

      // Update progress
      const progressPercent = ((i + 1) / statements.length) * 100;
      setProgress(progressPercent);
    }

    stopProgress(true);

    // Update final status
    if (failedCount === 0) {
      setStatus('success');
      setStatusMessage(`All ${successCount} statement(s) executed successfully`);
      // Call onComplete callback if all succeeded
      if (options.onComplete) {
        try {
          await options.onComplete();
        } catch (err) {
          console.error('[SqlPreviewDialog] onComplete callback failed:', err);
        }
      }
    } else if (successCount === 0) {
      setStatus('error');
      setStatusMessage(`All ${failedCount} statement(s) failed`);
    } else {
      setStatus('error');
      setStatusMessage(`${successCount} succeeded, ${failedCount} failed`);
    }

    // Keep dialog open so user can see audit log - don't auto-close
  }, [options, sql, startProgress, stopProgress]);

  /** Discard all changes and close. */
  const handleDiscard = useCallback(() => {
    options?.onDiscard?.();
    onClose();
  }, [options, onClose]);

  /** Cancel / close without discarding (just closes the dialog). */
  const handleCancel = useCallback(() => {
    onClose();
  }, [onClose]);

  /** Truncate statement for display */
  const truncateStatement = (stmt: string, maxLen: number = 40): string => {
    if (stmt.length <= maxLen) return stmt;
    return stmt.substring(0, maxLen) + '...';
  };

  // Count statements for display
  const statementCount = sql
    .split('\n')
    .filter((line) => line.trim().length > 0 && !line.trim().startsWith('--'))
    .length;

  return (
    <Dialog open={open} onOpenChange={(isOpen) => { if (!isOpen) handleCancel(); }}>
      <DialogContent className="max-w-4xl max-h-[90vh] flex flex-col gap-0 p-0 overflow-hidden">
        {/* ── Header ─────────────────────────────────────────────────── */}
        <DialogHeader className="px-6 pt-6 pb-3">
          <DialogTitle className="text-lg">
            {options?.title ?? 'SQL Preview'}
          </DialogTitle>
          {options?.description && (
            <DialogDescription className="text-sm text-muted-foreground">
              {options.description}
            </DialogDescription>
          )}
        </DialogHeader>

        {/* ── Progress bar ───────────────────────────────────────────── */}
        {status === 'executing' && (
          <div className="mx-6 h-1.5 bg-muted rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500 rounded-full transition-all duration-300 ease-out"
              style={{ width: `${Math.min(progress, 100)}%` }}
            />
          </div>
        )}
        {status === 'success' && (
          <div className="mx-6 h-1.5 bg-muted rounded-full overflow-hidden">
            <div className="h-full bg-green-500 rounded-full w-full" />
          </div>
        )}
        {status === 'error' && (
          <div className="mx-6 h-1.5 bg-muted rounded-full overflow-hidden">
            <div className="h-full bg-red-500 rounded-full w-full" />
          </div>
        )}

        {/* ── SQL Editor ─────────────────────────────────────────────── */}
        <div className="flex-1 min-h-[200px] max-h-[30vh] mx-6 mt-3 border rounded-md overflow-hidden">
          {sql ? (
            <>
              {/* Temporary: Use textarea to verify data flow works */}
              <textarea
                value={sql}
                onChange={(e) => {
                  if (!isReadOnly) setSql(e.target.value);
                }}
                readOnly={isReadOnly}
                className="w-full h-full p-4 font-mono text-sm resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
                placeholder="SQL will appear here..."
              />
              {/* Monaco Editor - temporarily disabled for debugging
              <MonacoEditor
                key={sql.substring(0, 50)}
                height="100%"
                language="sql"
                theme="vs-light"
                value={sql}
                onChange={(value) => {
                  if (!isReadOnly) setSql(value ?? '');
                }}
                onMount={(editor) => {
                  editorRef.current = editor;
                  if (sql) {
                    editor.setValue(sql);
                  }
                }}
                options={{
                  readOnly: isReadOnly,
                  minimap: { enabled: false },
                  fontSize: 13,
                  lineNumbers: 'on',
                  scrollBeyondLastLine: false,
                  automaticLayout: true,
                  tabSize: 2,
                  wordWrap: 'on',
                  padding: { top: 8, bottom: 8 },
                  ...(isReadOnly ? { domReadOnly: true } : {}),
                }}
              />
              */}
            </>
          ) : (
            <div className="flex items-center justify-center h-full text-muted-foreground">
              <p>No SQL to display</p>
            </div>
          )}
        </div>

        {/* ── Audit Log ──────────────────────────────────────────────── */}
        {auditLog.length > 0 && (
          <div className="mx-6 mt-4 border rounded-md overflow-hidden">
            <div className="bg-muted px-3 py-2 text-xs font-semibold text-muted-foreground uppercase tracking-wide">
              Execution Audit
            </div>
            <div className="max-h-[200px] overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="bg-muted/50 sticky top-0">
                  <tr className="text-left">
                    <th className="px-3 py-2 w-12 font-medium text-muted-foreground">#</th>
                    <th className="px-3 py-2 font-medium text-muted-foreground">Statement</th>
                    <th className="px-3 py-2 w-24 font-medium text-muted-foreground">Status</th>
                    <th className="px-3 py-2 w-20 text-right font-medium text-muted-foreground">Time</th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {auditLog.map((item) => (
                    <tr
                      key={item.id}
                      className={cn(
                        'hover:bg-muted/30 transition-colors',
                        item.status === 'running' && 'bg-blue-50',
                        item.status === 'success' && 'bg-green-50',
                        item.status === 'failed' && 'bg-red-50',
                      )}
                    >
                      <td className="px-3 py-2 text-muted-foreground font-mono text-xs">
                        {item.id}
                      </td>
                      <td className="px-3 py-2 font-mono text-xs">
                        <span
                          title={item.statement}
                          className="cursor-help"
                        >
                          {truncateStatement(item.statement, 60)}
                        </span>
                        {item.error && (
                          <div className="mt-1 rounded-sm border border-red-200 bg-red-50 px-2 py-1">
                            <div className="text-[11px] font-medium uppercase tracking-wide text-red-700">
                              Error
                            </div>
                            <pre className="mt-1 whitespace-pre-wrap break-words font-mono text-[11px] leading-4 text-red-700">
                              {item.error}
                            </pre>
                          </div>
                        )}
                      </td>
                      <td className="px-3 py-2">
                        <span
                          className={cn(
                            'inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium',
                            item.status === 'pending' && 'bg-gray-100 text-gray-700',
                            item.status === 'running' && 'bg-blue-100 text-blue-700',
                            item.status === 'success' && 'bg-green-100 text-green-700',
                            item.status === 'failed' && 'bg-red-100 text-red-700',
                          )}
                        >
                          {item.status === 'running' && <Loader2 className="h-3 w-3 animate-spin" />}
                          {item.status === 'success' && <CheckCircle2 className="h-3 w-3" />}
                          {item.status === 'failed' && <AlertCircle className="h-3 w-3" />}
                          {item.status.charAt(0).toUpperCase() + item.status.slice(1)}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right font-mono text-xs text-muted-foreground">
                        {item.timeTook !== undefined ? `${item.timeTook}ms` : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* ── Footer: Status + Actions ───────────────────────────────── */}
        <div className="px-6 py-4 border-t flex items-center justify-between gap-4">
          {/* Left: status indicator */}
          <div className="flex items-center gap-2 text-sm min-w-0 flex-1">
            {status === 'idle' && (
              <span className="text-muted-foreground">
                {statementCount} statement{statementCount !== 1 ? 's' : ''} ready
              </span>
            )}
            {status === 'executing' && (
              <span className="flex items-center gap-1.5 text-blue-600">
                <Loader2 className="h-4 w-4 animate-spin" />
                {statusMessage}
              </span>
            )}
            {status === 'success' && (
              <span className="flex items-center gap-1.5 text-green-600">
                <CheckCircle2 className="h-4 w-4" />
                {statusMessage}
              </span>
            )}
            {status === 'error' && (
              <span className="flex items-center gap-1.5 text-red-600 truncate" title={statusMessage}>
                <AlertCircle className="h-4 w-4 shrink-0" />
                <span className="truncate">{statusMessage}</span>
              </span>
            )}
          </div>

          {/* Right: action buttons */}
          <div className="flex items-center gap-2 shrink-0">
            {options?.onDiscard && status !== 'executing' && (
              <Button
                size="sm"
                variant="ghost"
                onClick={handleDiscard}
                className="gap-1.5 text-muted-foreground"
              >
                <Undo2 className="h-3.5 w-3.5" />
                Discard
              </Button>
            )}
            <Button
              size="sm"
              variant="outline"
              onClick={handleCancel}
              disabled={status === 'executing'}
              className="gap-1.5"
            >
              <X className="h-3.5 w-3.5" />
              Cancel
            </Button>
            <Button
              size="sm"
              onClick={handleCommit}
              disabled={['executing', 'success'].includes(status) || !sql.trim()}
              className={cn(
                'gap-1.5',
                status === 'success' && 'bg-green-600 hover:bg-green-700',
              )}
            >
              {status === 'executing' ? (
                <>
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  Executing...
                </>
              ) : status === 'success' ? (
                <>
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  Done
                </>
              ) : (
                <>
                  <Play className="h-3.5 w-3.5" />
                  Commit
                </>
              )}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
