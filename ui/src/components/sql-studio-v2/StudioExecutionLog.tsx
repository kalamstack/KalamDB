import { useState } from "react";
import { AlertCircle, CheckCircle2, Eye } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { CodeBlock } from "@/components/ui/code-block";
import { cn } from "@/lib/utils";
import type { QueryLogEntry } from "./types";

interface StudioExecutionLogProps {
  logs: QueryLogEntry[];
  status: "success" | "error";
}

function formatLogTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleTimeString();
}

export function StudioExecutionLog({ logs, status }: StudioExecutionLogProps) {
  const [responseViewer, setResponseViewer] = useState<{
    open: boolean;
    title: string;
    response: unknown;
  }>({
    open: false,
    title: "",
    response: null,
  });

  if (logs.length === 0) {
    return (
      <div className="flex h-full items-center justify-center px-4 text-sm text-muted-foreground">
        No execution logs yet.
      </div>
    );
  }

  return (
    <>
    <ScrollArea className="min-h-0 flex-1">
      <div className="min-w-max p-2">
        <div className="mb-1.5 grid min-w-[820px] grid-cols-[24px_88px_1fr_96px] items-center gap-2 px-2 py-1 text-[11px] uppercase tracking-wide text-muted-foreground">
          <span />
          <span>Time</span>
          <span>Message</span>
          <span className="text-right">Response</span>
        </div>
        {logs.map((entry, index) => (
          <div
            key={entry.id}
            className={cn(
              "mb-1.5 grid min-w-[820px] grid-cols-[24px_88px_1fr_96px] items-start gap-2 rounded border border-border bg-background px-2 py-1.5 text-xs text-foreground",
              entry.level === "error" && "border-red-500/40 bg-red-950/20",
            )}
          >
            <span className="mt-0.5 flex h-4 w-4 items-center justify-center">
              {entry.level === "error" ? (
                <AlertCircle className="h-3.5 w-3.5 text-red-400" />
              ) : (
                <CheckCircle2 className={cn("h-3.5 w-3.5", status === "success" ? "text-emerald-400" : "text-muted-foreground")} />
              )}
            </span>
            <span className="font-mono text-[11px] text-muted-foreground">{formatLogTime(entry.createdAt)}</span>
            <div className="min-w-0 space-y-1">
              <div className="font-mono text-[12px] leading-5 text-foreground">{entry.message}</div>
              <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
                <span>#{index + 1}</span>
                {typeof entry.rowCount === "number" && <span>{entry.rowCount} rows</span>}
                {entry.asUser && <span>as {entry.asUser}</span>}
                {typeof entry.statementIndex === "number" && <span>statement {entry.statementIndex + 1}</span>}
              </div>
            </div>
            <div className="flex items-start justify-end">
              <Button
                variant="outline"
                size="sm"
                className="h-7 gap-1.5 border-border bg-transparent text-foreground hover:bg-muted"
                disabled={entry.response === undefined}
                onClick={() => {
                  if (entry.response === undefined) {
                    return;
                  }
                  setResponseViewer({
                    open: true,
                    title: `Response · Statement ${typeof entry.statementIndex === "number" ? entry.statementIndex + 1 : index + 1}`,
                    response: entry.response,
                  });
                }}
              >
                <Eye className="h-3.5 w-3.5" />
                View
              </Button>
            </div>
          </div>
        ))}
      </div>
      <ScrollBar orientation="horizontal" />
    </ScrollArea>
    <Dialog
      open={responseViewer.open}
      onOpenChange={(open) => setResponseViewer((previous) => ({ ...previous, open }))}
    >
      <DialogContent className="flex h-[85vh] max-h-[85vh] max-w-3xl min-h-0 flex-col overflow-hidden">
        <DialogHeader className="shrink-0">
          <DialogTitle>{responseViewer.title}</DialogTitle>
        </DialogHeader>
        <div className="min-h-0 flex-1 overflow-hidden">
          <CodeBlock value={responseViewer.response} jsonPreferred maxHeightClassName="max-h-full h-full" />
        </div>
      </DialogContent>
    </Dialog>
    </>
  );
}
