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

import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import Editor from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable';
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
  /** Optional pre-split statement list for display and audit purposes. */
  statements?: string[];
  /** Dialog title. */
  title?: string;
  /** Optional description below the title. */
  description?: string;
  /** Callback to execute the SQL currently displayed in the editor. Should throw on error. */
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

function splitSqlStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = "";
  let inSingleQuotedString = false;

  for (let index = 0; index < sql.length; index += 1) {
    const character = sql[index];
    current += character;

    if (character === "'") {
      if (inSingleQuotedString && sql[index + 1] === "'") {
        current += sql[index + 1];
        index += 1;
        continue;
      }

      inSingleQuotedString = !inSingleQuotedString;
      continue;
    }

    if (!inSingleQuotedString && character === ";") {
      const statement = current.trim();
      if (statement.length > 0) {
        statements.push(statement);
      }
      current = "";
    }
  }

  const trailingStatement = current.trim();
  if (trailingStatement.length > 0) {
    statements.push(trailingStatement);
  }

  return statements;
}

function buildPendingAudit(statements: string[]): StatementAudit[] {
  return statements.map((statement, index) => ({
    id: index + 1,
    statement,
    status: 'pending',
  }));
}

function isTransactionStatement(statement: string): boolean {
  return /^BEGIN\b/i.test(statement) || /^COMMIT\b/i.test(statement);
}

function isTransactionBatch(statements: string[]): boolean {
  if (statements.length < 3) {
    return false;
  }

  const first = statements[0]?.trim();
  const last = statements[statements.length - 1]?.trim();
  return /^BEGIN\b/i.test(first) && /^COMMIT\b/i.test(last);
}

function ExecutionSparkline({ active }: { active: boolean }) {
  return (
    <div className="flex items-end gap-1">
      {[12, 24, 18, 28].map((height, index) => (
        <span
          key={`${height}-${index}`}
          className={cn(
            'w-1.5 rounded-full bg-sky-500/75 transition-opacity duration-300',
            active ? 'animate-pulse opacity-100' : 'opacity-35',
          )}
          style={{
            height,
            animationDelay: `${index * 120}ms`,
          }}
        />
      ))}
    </div>
  );
}

// ─── Component ───────────────────────────────────────────────────────────────

export function SqlPreviewDialog({ open, options, onClose }: SqlPreviewDialogProps) {
  const [sql, setSql] = useState('');
  const [status, setStatus] = useState<SqlPreviewStatus>('idle');
  const [statusMessage, setStatusMessage] = useState<string>('');
  const [progress, setProgress] = useState(0);
  const [isReadOnly, setIsReadOnly] = useState(false);
  const [auditLog, setAuditLog] = useState<StatementAudit[]>([]);
  const [activeAuditIndex, setActiveAuditIndex] = useState<number | null>(null);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const progressTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const auditTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const previewStatements = useMemo(() => {
    if (options?.editable === false && options.statements && options.statements.length > 0) {
      return options.statements;
    }

    return splitSqlStatements(sql);
  }, [options?.editable, options?.statements, sql]);

  const visibleAudit = useMemo(() => {
    return auditLog.length > 0 ? auditLog : buildPendingAudit(previewStatements);
  }, [auditLog, previewStatements]);

  const statementCount = previewStatements.length;
  const transactionBatch = useMemo(() => isTransactionBatch(previewStatements), [previewStatements]);
  const mutationStatementCount = useMemo(
    () => previewStatements.filter((statement) => !isTransactionStatement(statement)).length,
    [previewStatements],
  );

  // Sync SQL when options change
  useEffect(() => {
    if (options?.sql) {
      setSql(options.sql);
      setStatus('idle');
      setStatusMessage('');
      setProgress(0);
      setAuditLog([]);
      setActiveAuditIndex(null);
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
      if (auditTimerRef.current) clearInterval(auditTimerRef.current);
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

  const startAuditAnimation = useCallback((count: number) => {
    if (count === 0) {
      setActiveAuditIndex(null);
      return;
    }

    setActiveAuditIndex(0);

    if (auditTimerRef.current) {
      clearInterval(auditTimerRef.current);
    }

    auditTimerRef.current = setInterval(() => {
      setActiveAuditIndex((current) => {
        if (current === null) {
          return 0;
        }
        return (current + 1) % count;
      });
    }, 650);
  }, []);

  const stopAuditAnimation = useCallback(() => {
    if (auditTimerRef.current) {
      clearInterval(auditTimerRef.current);
      auditTimerRef.current = null;
    }

    setActiveAuditIndex(null);
  }, []);

  /** Execute the SQL batch exactly as displayed and update the audit UI. */
  const handleCommit = useCallback(async () => {
    if (!options?.onExecute || !sql.trim()) {
      console.warn('[SqlPreviewDialog] Cannot commit - no onExecute or empty SQL');
      return;
    }

    if (previewStatements.length === 0) {
      setStatusMessage('No statements to execute');
      return;
    }

    const initialAudit = buildPendingAudit(previewStatements);
    setAuditLog(initialAudit);

    setStatus('executing');
    setStatusMessage(transactionBatch ? 'Executing transaction batch...' : 'Executing statements...');
    setIsReadOnly(true);
    startProgress();
    startAuditAnimation(previewStatements.length);

    const startTime = performance.now();

    try {
      await options.onExecute(sql);
      const timeTook = Math.round(performance.now() - startTime);

      stopAuditAnimation();
      stopProgress(true);
      setAuditLog(
        initialAudit.map((item) => ({
          ...item,
          status: 'success',
          timeTook,
        })),
      );
      setStatus('success');
      setStatusMessage(
        transactionBatch
          ? `Transaction committed across ${mutationStatementCount} change statement${mutationStatementCount === 1 ? '' : 's'}`
          : `Executed ${statementCount} statement${statementCount === 1 ? '' : 's'} successfully`,
      );

      if (options.onComplete) {
        try {
          await options.onComplete();
        } catch (err) {
          console.error('[SqlPreviewDialog] onComplete callback failed:', err);
        }
      }
    } catch (err) {
      const timeTook = Math.round(performance.now() - startTime);
      const errorMsg = err instanceof Error ? err.message : 'Unknown error';
      const formattedError = formatAuditErrorMessage(errorMsg);
      const errorIndex = previewStatements.findIndex((statement) => !isTransactionStatement(statement));

      stopAuditAnimation();
      stopProgress(false);
      setAuditLog(
        initialAudit.map((item, index) => ({
          ...item,
          status: 'failed',
          timeTook,
          error: index === (errorIndex >= 0 ? errorIndex : 0) ? formattedError : undefined,
        })),
      );
      setStatus('error');
      setStatusMessage(transactionBatch ? 'Transaction failed and was rolled back' : 'Execution failed');
    }

    // Keep dialog open so user can see the execution summary.
  }, [
    mutationStatementCount,
    options,
    previewStatements,
    sql,
    startAuditAnimation,
    startProgress,
    statementCount,
    stopAuditAnimation,
    stopProgress,
    transactionBatch,
  ]);

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

  return (
    <Dialog open={open} onOpenChange={(isOpen) => { if (!isOpen) handleCancel(); }}>
      <DialogContent className="flex h-[85vh] max-h-[90vh] max-w-5xl flex-col gap-0 overflow-hidden p-0">
        <DialogHeader className="border-b px-6 pb-4 pt-6">
          <DialogTitle className="text-lg">
            {options?.title ?? 'SQL Preview'}
          </DialogTitle>
          {options?.description && (
            <DialogDescription className="text-sm text-muted-foreground">
              {options.description}
            </DialogDescription>
          )}
          {!options?.description && (
            <DialogDescription className="sr-only">
              Review the SQL batch before committing it.
            </DialogDescription>
          )}

          <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
            <span className="rounded-full bg-muted px-2.5 py-1 text-muted-foreground">
              {statementCount} statement{statementCount === 1 ? '' : 's'}
            </span>
            <span className="rounded-full bg-muted px-2.5 py-1 text-muted-foreground">
              {transactionBatch ? 'BEGIN / COMMIT transaction' : 'Single statement execution'}
            </span>
            <span
              className={cn(
                'inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 font-medium',
                status === 'idle' && 'bg-slate-200/70 text-slate-700',
                status === 'executing' && 'bg-sky-100 text-sky-700',
                status === 'success' && 'bg-emerald-100 text-emerald-700',
                status === 'error' && 'bg-rose-100 text-rose-700',
              )}
            >
              {status === 'executing' && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {status === 'success' && <CheckCircle2 className="h-3.5 w-3.5" />}
              {status === 'error' && <AlertCircle className="h-3.5 w-3.5" />}
              {status === 'idle' ? 'Ready' : status === 'executing' ? 'Executing' : status === 'success' ? 'Committed' : 'Failed'}
            </span>
            {status === 'executing' && (
              <span className="inline-flex items-center gap-2 text-sky-700">
                <ExecutionSparkline active />
                <span>{statusMessage}</span>
              </span>
            )}
          </div>
        </DialogHeader>

        <div className="px-6 py-4">
          <div className="h-1.5 overflow-hidden rounded-full bg-muted">
            {status === 'executing' && (
              <div
                className="h-full rounded-full bg-[linear-gradient(90deg,_rgba(14,165,233,0.65),_rgba(56,189,248,1),_rgba(14,165,233,0.65))] transition-all duration-300 ease-out"
                style={{ width: `${Math.min(progress, 100)}%` }}
              />
            )}
            {status === 'success' && (
              <div className="h-full w-full rounded-full bg-emerald-500" />
            )}
            {status === 'error' && (
              <div className="h-full w-full rounded-full bg-rose-500" />
            )}
            {status === 'idle' && (
              <div className="h-full w-[18%] rounded-full bg-slate-300/70" />
            )}
          </div>
        </div>

        <div className="min-h-0 flex-1 px-6 pb-4">
          <ResizablePanelGroup orientation="vertical" className="min-h-0 flex-1">
            <ResizablePanel defaultSize={50} minSize={20} className="min-h-0 overflow-hidden">
              <section className="flex h-full min-h-0 flex-col">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <div>
                    <p className="text-sm font-medium text-foreground">SQL Preview</p>
                    <p className="text-xs text-muted-foreground">
                      {isReadOnly ? 'Read-only Monaco preview for this commit.' : 'Edit the SQL before sending it.'}
                    </p>
                  </div>
                  <span className="rounded-full border border-border bg-background px-2.5 py-1 text-xs text-muted-foreground">
                    {mutationStatementCount} change unit{mutationStatementCount === 1 ? '' : 's'}
                  </span>
                </div>

                <div className="min-h-0 flex-1 overflow-hidden rounded-md border border-border bg-background">
                  {sql ? (
                    <Editor
                      height="100%"
                      language="sql"
                      theme="vs-light"
                      value={sql}
                      onChange={(value) => {
                        if (!isReadOnly) {
                          setSql(value ?? '');
                        }
                      }}
                      onMount={(instance) => {
                        editorRef.current = instance;
                      }}
                      options={{
                        readOnly: isReadOnly,
                        domReadOnly: isReadOnly,
                        minimap: { enabled: false },
                        fontSize: 13,
                        lineNumbers: 'on',
                        scrollBeyondLastLine: false,
                        automaticLayout: true,
                        tabSize: 2,
                        wordWrap: 'on',
                        padding: { top: 16, bottom: 16 },
                        overviewRulerBorder: false,
                        renderLineHighlight: 'gutter',
                        scrollbar: {
                          verticalScrollbarSize: 10,
                          horizontalScrollbarSize: 10,
                        },
                      }}
                    />
                  ) : (
                    <div className="flex h-full items-center justify-center text-muted-foreground">
                      <p>No SQL to display</p>
                    </div>
                  )}
                </div>
              </section>
            </ResizablePanel>

            <ResizableHandle withHandle className="my-3" />

            <ResizablePanel defaultSize={50} minSize={20} className="min-h-0 overflow-hidden">
              <section className="flex h-full min-h-0 flex-col">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <div>
                    <p className="text-sm font-medium text-foreground">Execution Log</p>
                    <p className="text-xs text-muted-foreground">
                      {transactionBatch
                        ? 'The full batch is sent once; the rows below show the statements included in that transaction.'
                        : 'Statement audit for this execution.'}
                    </p>
                  </div>
                </div>

                <div className="min-h-0 flex-1 overflow-hidden rounded-md border border-border bg-background">
                  <div className="h-full overflow-auto">
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 z-10 bg-muted/80 text-left backdrop-blur">
                        <tr>
                          <th className="w-14 px-3 py-2 font-medium text-muted-foreground">#</th>
                          <th className="px-3 py-2 font-medium text-muted-foreground">Statement</th>
                          <th className="w-44 px-3 py-2 font-medium text-muted-foreground">Status</th>
                          <th className="w-24 px-3 py-2 text-right font-medium text-muted-foreground">Time</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y">
                        {visibleAudit.map((item, index) => {
                          const isAnimated = status === 'executing' && activeAuditIndex === index;
                          const badgeStatus: StatementStatus = item.status === 'pending' && isAnimated ? 'running' : item.status;

                          return (
                            <tr
                              key={item.id}
                              className={cn(
                                'transition-colors',
                                badgeStatus === 'running' && 'bg-sky-50/80',
                                item.status === 'success' && 'bg-emerald-50/70',
                                item.status === 'failed' && 'bg-rose-50/70',
                              )}
                            >
                              <td className="px-3 py-3 align-top font-mono text-xs text-muted-foreground">
                                #{item.id}
                              </td>
                              <td className="px-3 py-3 align-top">
                                <pre className="whitespace-pre-wrap break-words font-mono text-[11px] leading-5 text-foreground">
                                  {truncateStatement(item.statement, 180)}
                                </pre>
                                {item.error && (
                                  <div className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-2 py-2">
                                    <div className="text-[11px] font-medium uppercase tracking-wide text-rose-700">
                                      Error
                                    </div>
                                    <pre className="mt-1 whitespace-pre-wrap break-words font-mono text-[11px] leading-4 text-rose-700">
                                      {item.error}
                                    </pre>
                                  </div>
                                )}
                              </td>
                              <td className="px-3 py-3 align-top">
                                <span
                                  className={cn(
                                    'inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium',
                                    badgeStatus === 'pending' && 'bg-gray-100 text-gray-700',
                                    badgeStatus === 'running' && 'bg-sky-100 text-sky-700',
                                    item.status === 'success' && 'bg-emerald-100 text-emerald-700',
                                    item.status === 'failed' && 'bg-rose-100 text-rose-700',
                                  )}
                                >
                                  {badgeStatus === 'running' && <Loader2 className="h-3 w-3 animate-spin" />}
                                  {item.status === 'success' && <CheckCircle2 className="h-3 w-3" />}
                                  {item.status === 'failed' && <AlertCircle className="h-3 w-3" />}
                                  {badgeStatus === 'pending' && 'Queued'}
                                  {badgeStatus === 'running' && 'Running'}
                                  {item.status === 'success' && (transactionBatch ? 'Committed' : 'Done')}
                                  {item.status === 'failed' && (transactionBatch ? 'Rolled back' : 'Failed')}
                                </span>
                                {isAnimated && (
                                  <div className="mt-2 flex items-center gap-2 text-[11px] text-sky-700">
                                    <ExecutionSparkline active />
                                    <span>Running</span>
                                  </div>
                                )}
                              </td>
                              <td className="px-3 py-3 text-right align-top font-mono text-xs text-muted-foreground">
                                {item.timeTook !== undefined ? `${item.timeTook}ms` : '--'}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              </section>
            </ResizablePanel>
          </ResizablePanelGroup>
        </div>

        {/* ── Footer: Status + Actions ───────────────────────────────── */}
        <div className="flex shrink-0 items-center justify-between gap-4 border-t bg-background px-6 py-4">
          {/* Left: status indicator */}
          <div className="flex min-w-0 flex-1 items-center gap-2 text-sm">
            {status === 'idle' && (
              <span className="text-muted-foreground">
                {statementCount} statement{statementCount !== 1 ? 's' : ''} ready to review
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
          <div className="flex shrink-0 items-center gap-2">
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
