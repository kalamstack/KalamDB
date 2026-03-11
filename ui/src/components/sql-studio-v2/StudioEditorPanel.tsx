import { useEffect, useMemo, useRef, useState } from "react";
import Editor, { type Monaco } from "@monaco-editor/react";
import type { IDisposable, Position, editor, languages } from "monaco-editor";
import { MoreHorizontal, PenLine, Play, Save, Settings2, Square } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Input } from "@/components/ui/input";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { LiveSubscriptionOptions, StudioNamespace } from "@/components/sql-studio-v2/types";

interface StudioEditorPanelProps {
  schema: StudioNamespace[];
  tabTitle: string;
  lastSavedAt: string | null;
  isLive: boolean;
  liveStatus: "idle" | "connecting" | "connected" | "error";
  sql: string;
  isRunning: boolean;
  subscriptionOptions?: LiveSubscriptionOptions;
  onSqlChange: (value: string) => void;
  onRun: () => void;
  onToggleLive: (checked: boolean) => void;
  onSubscriptionOptionsChange: (options: LiveSubscriptionOptions | undefined) => void;
  onRename: (title: string) => void;
  onSave: () => void;
  onSaveCopy: () => void;
  onDelete: () => void;
}

export function StudioEditorPanel({
  schema,
  tabTitle,
  lastSavedAt,
  isLive,
  liveStatus,
  sql,
  isRunning,
  subscriptionOptions,
  onSqlChange,
  onRun,
  onToggleLive,
  onSubscriptionOptionsChange,
  onRename,
  onSave,
  onSaveCopy,
  onDelete,
}: StudioEditorPanelProps) {
  const [isEditingTitle, setIsEditingTitle] = useState(false);
  const [draftTitle, setDraftTitle] = useState(tabTitle);
  const [showSubscriptionOptions, setShowSubscriptionOptions] = useState(false);
  const completionProviderRef = useRef<IDisposable | null>(null);
  const completionDataRef = useRef<{
    namespaces: string[];
    tablesByNamespace: Record<string, string[]>;
    columnsByTable: Record<string, string[]>;
    keywords: string[];
  }>({
    namespaces: [],
    tablesByNamespace: {},
    columnsByTable: {},
    keywords: [],
  });

  const completionData = useMemo(() => {
    const namespaces: string[] = [];
    const tablesByNamespace: Record<string, string[]> = {};
    const columnsByTable: Record<string, string[]> = {};

    schema.forEach((namespace) => {
      const namespaceKey = namespace.name.toLowerCase();
      namespaces.push(namespace.name);
      tablesByNamespace[namespaceKey] = namespace.tables.map((table) => table.name);
      namespace.tables.forEach((table) => {
        columnsByTable[`${namespaceKey}.${table.name.toLowerCase()}`] = table.columns.map((column) => column.name);
      });
    });

    const keywords = [
      "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
      "GROUP BY", "ORDER BY", "LIMIT", "INSERT", "UPDATE", "DELETE",
      "CREATE", "ALTER", "DROP", "TABLE", "NAMESPACE", "VALUES", "SET",
      "AND", "OR", "NOT", "IN", "AS", "ON",
    ];

    return { namespaces, tablesByNamespace, columnsByTable, keywords };
  }, [schema]);

  useEffect(() => {
    completionDataRef.current = completionData;
  }, [completionData]);

  useEffect(() => {
    if (!isEditingTitle) {
      setDraftTitle(tabTitle);
    }
  }, [tabTitle, isEditingTitle]);

  useEffect(() => {
    return () => {
      completionProviderRef.current?.dispose();
    };
  }, []);

  const registerCompletionProvider = (monaco: Monaco) => {
    completionProviderRef.current?.dispose();
    completionProviderRef.current = monaco.languages.registerCompletionItemProvider("sql", {
      triggerCharacters: [".", " ", ","],
      provideCompletionItems: (model: editor.ITextModel, position: Position) => {
        const data = completionDataRef.current;
        const wordUntil = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: wordUntil.startColumn,
          endColumn: wordUntil.endColumn,
        };

        const textUntilPosition = model.getValueInRange({
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        });
        const prefix = wordUntil.word.toLowerCase();
        const suggestions: languages.CompletionItem[] = [];
        const seen = new Set<string>();
        const aliasToTable: Record<string, string> = {};

        const aliasRegex = /\b(?:from|join)\s+([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)(?:\s+(?:as\s+)?([a-zA-Z_][\w]*))?/gi;
        let aliasMatch: RegExpExecArray | null = aliasRegex.exec(textUntilPosition);
        while (aliasMatch) {
          const namespaceName = aliasMatch[1]?.toLowerCase();
          const tableName = aliasMatch[2]?.toLowerCase();
          const alias = aliasMatch[3]?.toLowerCase();
          if (namespaceName && tableName && alias) {
            aliasToTable[alias] = `${namespaceName}.${tableName}`;
          }
          aliasMatch = aliasRegex.exec(textUntilPosition);
        }

        const pushSuggestion = (label: string, kind: languages.CompletionItemKind, detail: string, insertText = label) => {
          const key = `${kind}-${label}-${insertText}`;
          if (seen.has(key)) {
            return;
          }
          if (prefix && !label.toLowerCase().includes(prefix)) {
            return;
          }
          seen.add(key);
          suggestions.push({ label, kind, detail, insertText, range });
        };

        const tableColumnMatch = /([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)?$/.exec(textUntilPosition);
        if (tableColumnMatch) {
          const namespaceKey = tableColumnMatch[1].toLowerCase();
          const tableKey = tableColumnMatch[2].toLowerCase();
          const columns = data.columnsByTable[`${namespaceKey}.${tableKey}`] ?? [];
          columns.forEach((column) =>
            pushSuggestion(column, monaco.languages.CompletionItemKind.Field, `${namespaceKey}.${tableKey} column`),
          );
          return { suggestions };
        }

        const aliasColumnMatch = /([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)?$/.exec(textUntilPosition);
        if (aliasColumnMatch) {
          const aliasKey = aliasColumnMatch[1].toLowerCase();
          const resolvedTable = aliasToTable[aliasKey];
          if (resolvedTable) {
            const columns = data.columnsByTable[resolvedTable] ?? [];
            columns.forEach((column) =>
              pushSuggestion(column, monaco.languages.CompletionItemKind.Field, `${aliasKey} alias column`),
            );
            return { suggestions };
          }
        }

        const namespaceTableMatch = /([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)?$/.exec(textUntilPosition);
        if (namespaceTableMatch) {
          const namespaceKey = namespaceTableMatch[1].toLowerCase();
          const namespaceTables = data.tablesByNamespace[namespaceKey];
          if (namespaceTables && namespaceTables.length > 0) {
            namespaceTables.forEach((table) =>
              pushSuggestion(table, monaco.languages.CompletionItemKind.Class, `${namespaceKey} table`),
            );
            return { suggestions };
          }
        }

        data.keywords.forEach((keyword) =>
          pushSuggestion(keyword, monaco.languages.CompletionItemKind.Keyword, "SQL keyword"),
        );
        data.namespaces.forEach((namespaceName) =>
          pushSuggestion(namespaceName, monaco.languages.CompletionItemKind.Module, "Namespace"),
        );
        Object.entries(data.tablesByNamespace).forEach(([namespaceName, tables]) => {
          tables.forEach((table) => {
            pushSuggestion(`${namespaceName}.${table}`, monaco.languages.CompletionItemKind.Class, "Qualified table name");
            pushSuggestion(table, monaco.languages.CompletionItemKind.Class, `Table in ${namespaceName}`);
          });
        });
        Object.entries(data.columnsByTable).forEach(([qualifiedTable, columns]) => {
          columns.forEach((column) => {
            pushSuggestion(column, monaco.languages.CompletionItemKind.Field, `Column (${qualifiedTable})`);
          });
        });

        return { suggestions };
      },
    });
  };

  const handleEditorMount = (instance: editor.IStandaloneCodeEditor, monaco: Monaco) => {
    instance.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      onRun();
    });
    registerCompletionProvider(monaco);
  };

  const lastSavedLabel = useMemo(() => {
    if (!lastSavedAt) {
      return "Never saved";
    }

    const savedAt = new Date(lastSavedAt);
    const now = Date.now();
    const diffMs = now - savedAt.getTime();
    if (Number.isNaN(savedAt.getTime()) || diffMs < 0) {
      return "Last saved just now";
    }

    const diffMinutes = Math.floor(diffMs / (1000 * 60));
    if (diffMinutes < 1) {
      return "Last saved just now";
    }
    if (diffMinutes < 60) {
      return `Last saved ${diffMinutes}m ago`;
    }

    const diffHours = Math.floor(diffMinutes / 60);
    if (diffHours < 24) {
      return `Last saved ${diffHours}h ago`;
    }

    return `Last saved ${savedAt.toLocaleString()}`;
  }, [lastSavedAt]);

  const commitRename = () => {
    const normalizedTitle = draftTitle.trim();
    if (normalizedTitle.length > 0 && normalizedTitle !== tabTitle) {
      onRename(normalizedTitle);
    }
    setIsEditingTitle(false);
  };

  return (
    <div className="flex h-full flex-col bg-background">
      <div className="flex items-center justify-between border-b border-border px-4 py-2">
        <div className="min-w-0">
          {isEditingTitle ? (
            <input
              value={draftTitle}
              onChange={(event) => setDraftTitle(event.target.value)}
              onBlur={commitRename}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  commitRename();
                }
                if (event.key === "Escape") {
                  setIsEditingTitle(false);
                }
              }}
              autoFocus
              className="h-7 w-full max-w-[320px] rounded border border-border bg-background px-2 text-base font-semibold text-foreground outline-none ring-2 ring-ring"
            />
          ) : (
            <div className="flex items-center gap-1.5">
              <p className="truncate text-base font-semibold text-foreground">{tabTitle}</p>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-6 w-6 shrink-0 text-muted-foreground hover:text-foreground"
                onClick={() => setIsEditingTitle(true)}
                title="Rename query"
              >
                <PenLine className="h-3.5 w-3.5" />
              </Button>
            </div>
          )}
          <p className="text-xs text-muted-foreground">Draft</p>
        </div>
        <div className="flex items-center gap-2">
          <div className="hidden items-center gap-2 lg:flex">
            <div className="flex items-center gap-2 border-r border-border pr-3">
              <Switch
                checked={isLive}
                onCheckedChange={onToggleLive}
                disabled={liveStatus === "connecting"}
              />
              <span className="text-xs text-muted-foreground">Live query</span>
              {isLive && (
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-6 w-6 text-muted-foreground hover:text-foreground"
                  onClick={() => setShowSubscriptionOptions((prev) => !prev)}
                  title="Subscription options"
                >
                  <Settings2 className="h-3.5 w-3.5" />
                </Button>
              )}
            </div>
            <span className="text-xs text-muted-foreground">{lastSavedLabel}</span>
          </div>
          <Button
            variant="secondary"
            size="sm"
            className="shrink-0"
            onClick={onSave}
          >
            <Save className="mr-1.5 h-3.5 w-3.5" />
            Save
          </Button>
          <Button
            size="sm"
            className="shrink-0 bg-primary text-foreground hover:bg-primary"
            onClick={onRun}
            disabled={isRunning || !sql.trim() || liveStatus === "connecting"}
          >
            {isLive && liveStatus === "connected" ? (
              <Square className="mr-1.5 h-3.5 w-3.5" />
            ) : (
              <Play className="mr-1.5 h-3.5 w-3.5" />
            )}
            {isLive && liveStatus === "connected"
              ? "Stop"
              : isRunning
                ? "Running..."
                : isLive
                  ? "Subscribe"
                  : "Run query"}
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant="secondary"
                size="icon"
                className="h-8 w-8"
              >
                <MoreHorizontal className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onSelect={() => setIsEditingTitle(true)}>Rename</DropdownMenuItem>
              <DropdownMenuItem onSelect={onSaveCopy}>Save a copy</DropdownMenuItem>
              <DropdownMenuItem onSelect={onDelete} className="text-destructive">Delete</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>

      {isLive && showSubscriptionOptions && (
        <div className="flex items-center gap-4 border-b border-border bg-muted/30 px-4 py-2">
          <span className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Options</span>
          <div className="flex items-center gap-1.5">
            <label className="text-xs text-muted-foreground" htmlFor="opt-last-rows">last_rows</label>
            <Input
              id="opt-last-rows"
              type="number"
              min={0}
              placeholder="–"
              value={subscriptionOptions?.last_rows ?? ""}
              onChange={(e) => {
                const val = e.target.value ? parseInt(e.target.value, 10) : undefined;
                onSubscriptionOptionsChange({
                  ...subscriptionOptions,
                  last_rows: Number.isFinite(val) ? val : undefined,
                });
              }}
              className="h-7 w-20 border-border bg-background text-xs"
            />
          </div>
          <div className="flex items-center gap-1.5">
            <label className="text-xs text-muted-foreground" htmlFor="opt-batch-size">batch_size</label>
            <Input
              id="opt-batch-size"
              type="number"
              min={0}
              placeholder="–"
              value={subscriptionOptions?.batch_size ?? ""}
              onChange={(e) => {
                const val = e.target.value ? parseInt(e.target.value, 10) : undefined;
                onSubscriptionOptionsChange({
                  ...subscriptionOptions,
                  batch_size: Number.isFinite(val) ? val : undefined,
                });
              }}
              className="h-7 w-20 border-border bg-background text-xs"
            />
          </div>
          <div className="flex items-center gap-1.5">
            <label className="text-xs text-muted-foreground" htmlFor="opt-from">from</label>
            <Input
              id="opt-from"
              type="text"
              placeholder="–"
              value={subscriptionOptions?.from ?? ""}
              onChange={(e) => {
                const raw = e.target.value.trim();
                let val: number | string | undefined;
                if (!raw) {
                  val = undefined;
                } else if (/^\d+$/.test(raw)) {
                  val = parseInt(raw, 10);
                } else {
                  val = raw;
                }
                onSubscriptionOptionsChange({
                  ...subscriptionOptions,
                  from: val,
                });
              }}
              className="h-7 w-28 border-border bg-background text-xs"
            />
          </div>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 text-xs text-muted-foreground hover:text-foreground"
            onClick={() => onSubscriptionOptionsChange(undefined)}
          >
            Clear
          </Button>
        </div>
      )}

      <div className="flex-1 overflow-hidden">
        <Editor
          height="100%"
          defaultLanguage="sql"
          theme="vs-dark"
          value={sql}
          onChange={(value) => onSqlChange(value ?? "")}
          onMount={handleEditorMount}
          options={{
            minimap: { enabled: false },
            fontSize: 13,
            lineNumbers: "on",
            lineNumbersMinChars: 3,
            automaticLayout: true,
            wordWrap: "on",
            scrollBeyondLastLine: false,
            padding: { top: 12 },
            fontFamily: "JetBrains Mono, monospace",
          }}
        />
      </div>
    </div>
  );
}
