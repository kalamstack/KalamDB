import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDown,
  ArrowUp,
  ChevronsLeft,
  ChevronsRight,
  ChevronLeft,
  ChevronRight,
  KeyRound,
  Trash2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { CodeBlock } from "@/components/ui/code-block";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useTableChanges } from "@/hooks/useTableChanges";
import { CellContextMenu, type CellContextMenuState } from "./CellContextMenu";
import { InlineCellEditor, type InlineEditContext } from "./InlineCellEditor";
import { generateSqlStatements } from "./utils/sqlGenerator";
import { extractTableContext } from "./utils/sqlParser";
import { useSqlPreview } from "@/components/sql-preview";
import { CellDisplay } from "@/components/datatype-display";
import { KalamCellValue } from "kalam-link";
import { executeSql } from "@/lib/kalam-client";
import { LIVE_META, LIVE_HIGHLIGHT_DURATION_MS } from "@/features/sql-studio/state/sqlStudioWorkspaceSlice";
import { cn } from "@/lib/utils";
import { StudioExecutionLog } from "./StudioExecutionLog";
import type { QueryResultData, SqlStudioResultView, StudioTable } from "./types";

interface StudioResultsGridProps {
  result: QueryResultData | null;
  isRunning: boolean;
  isLiveMode: boolean;
  activeSql: string;
  selectedTable: StudioTable | null;
  currentUsername: string;
  resultView: SqlStudioResultView;
  onResultViewChange: (view: SqlStudioResultView) => void;
  onRefreshAfterCommit: () => void;
}

type SortDirection = "asc" | "desc";
type SortState = { columnName: string; direction: SortDirection } | null;
type RowData = Record<string, unknown>;
type SelectedCell = { rowIndex: number; columnName: string } | null;

interface CellViewerState {
  open: boolean;
  title: string;
  content: unknown;
  editedValue: unknown;
  isNull: boolean;
  dataType?: string;
  rowIndex?: number;
  columnName?: string;
  canEdit: boolean;
}

const PAGE_SIZE = 50;
const MAX_RENDERED_ROWS = 1000;

function stringifyCellValue(value: unknown): string {
  // Unwrap KalamCellValue wrappers before stringifying
  const raw = value instanceof KalamCellValue ? value.toJson() : value;
  if (raw === null || raw === undefined) {
    return "null";
  }
  if (typeof raw === "string") {
    return raw;
  }
  if (typeof raw === "number" || typeof raw === "boolean" || typeof raw === "bigint") {
    return String(raw);
  }
  try {
    return JSON.stringify(raw, null, 2);
  } catch {
    return String(raw);
  }
}

function compareValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left === null || left === undefined) {
    return 1;
  }
  if (right === null || right === undefined) {
    return -1;
  }

  const leftType = typeof left;
  const rightType = typeof right;

  if (leftType === "number" && rightType === "number") {
    return (left as number) - (right as number);
  }

  if (leftType === "boolean" && rightType === "boolean") {
    return Number(left) - Number(right);
  }

  return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
}

export function StudioResultsGrid({
  result,
  isRunning,
  isLiveMode,
  activeSql,
  selectedTable,
  currentUsername,
  resultView,
  onResultViewChange,
  onRefreshAfterCommit,
}: StudioResultsGridProps) {
  const [sortState, setSortState] = useState<SortState>(null);
  const [pageIndex, setPageIndex] = useState(0);
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());
  const [selectedCell, setSelectedCell] = useState<SelectedCell>(null);
  const [cellContextMenu, setCellContextMenu] = useState<CellContextMenuState | null>(null);
  const [inlineEditContext, setInlineEditContext] = useState<InlineEditContext | null>(null);
  const [cellViewer, setCellViewer] = useState<CellViewerState>({
    open: false,
    title: "",
    content: "",
    editedValue: "",
    isNull: false,
    dataType: undefined,
    rowIndex: undefined,
    columnName: undefined,
    canEdit: false,
  });
  const [hasUnseenResults, setHasUnseenResults] = useState(false);
  const [hasUnseenLogs, setHasUnseenLogs] = useState(false);
  const previousResultRef = useRef<QueryResultData | null>(null);

  const {
    edits,
    deletions,
    changeCount,
    getRowStatus,
    isCellEdited,
    getCellEditedValue,
    editCell,
    deleteRow,
    undeleteRow,
    undoRowEdits,
    discardAll,
  } = useTableChanges();
  const { openSqlPreview } = useSqlPreview();

  useEffect(() => {
    discardAll();
    setSortState(null);
    setPageIndex(0);
    setSelectedRows(new Set());
    setSelectedCell(null);
    setCellContextMenu(null);
    setInlineEditContext(null);
    setCellViewer({
      open: false,
      title: "",
      content: "",
      editedValue: "",
      isNull: false,
      dataType: undefined,
      rowIndex: undefined,
      columnName: undefined,
      canEdit: false,
    });
  }, [result, discardAll]);

  useEffect(() => {
    if (resultView === "results") {
      setHasUnseenResults(false);
    }
    if (resultView === "log") {
      setHasUnseenLogs(false);
    }
  }, [resultView]);

  useEffect(() => {
    const previous = previousResultRef.current;
    if (result) {
      const rowsChanged =
        !previous ||
        previous.rows !== result.rows ||
        previous.schema !== result.schema ||
        previous.rowCount !== result.rowCount;
      const logsChanged = !previous || previous.logs.length !== result.logs.length;

      if (rowsChanged && resultView !== "results" && (result.rows.length > 0 || result.schema.length > 0)) {
        setHasUnseenResults(true);
      }
      if (logsChanged && resultView !== "log" && result.logs.length > 0) {
        setHasUnseenLogs(true);
      }
    }
    previousResultRef.current = result;
  }, [result, resultView]);

  const rawSchema = result?.schema ?? [];
  // In live mode, hide internal _live_* metadata columns (last_change is visible, not prefixed with _live_)
  const schema = isLiveMode
    ? rawSchema.filter((f) => !f.name.startsWith("_live_"))
    : rawSchema;
  const sourceRows =
    result?.status === "success"
      ? result.rows.slice(0, MAX_RENDERED_ROWS)
      : [];
  const parsedTableContext = useMemo(() => extractTableContext(activeSql), [activeSql]);
  const cellNamespace = parsedTableContext?.namespace ?? selectedTable?.namespace;
  const cellTableName = parsedTableContext?.tableName ?? selectedTable?.name;

  const sortedRowIndices = useMemo(() => {
    const indices = sourceRows.map((_, rowIndex) => rowIndex);
    
    if (sortState) {
      // Manual sort active - use it
      const { columnName, direction } = sortState;
      indices.sort((leftIndex, rightIndex) => {
        const leftValue = getCellEditedValue(leftIndex, columnName) ?? sourceRows[leftIndex]?.[columnName];
        const rightValue = getCellEditedValue(rightIndex, columnName) ?? sourceRows[rightIndex]?.[columnName];
        const comparison = compareValues(leftValue, rightValue);
        return direction === "asc" ? comparison : comparison * -1;
      });
      return indices;
    }

    if (isLiveMode) {
      // Live mode: sort by PK with new inserts at top
      const pkColumns = schema
        .filter((f) => f.isPrimaryKey)
        .sort((a, b) => a.index - b.index)
        .map((f) => f.name);
      
      indices.sort((leftIndex, rightIndex) => {
        const leftRow = sourceRows[leftIndex];
        const rightRow = sourceRows[rightIndex];
        const leftChangeType = leftRow?.[LIVE_META.CHANGE_TYPE] as string | undefined;
        const rightChangeType = rightRow?.[LIVE_META.CHANGE_TYPE] as string | undefined;
        
        // New inserts go to top
        const leftIsInsert = leftChangeType === "insert";
        const rightIsInsert = rightChangeType === "insert";
        if (leftIsInsert && !rightIsInsert) return -1;
        if (!leftIsInsert && rightIsInsert) return 1;
        
        // Otherwise sort by PK ascending
        for (const pkCol of pkColumns) {
          const leftValue = leftRow?.[pkCol];
          const rightValue = rightRow?.[pkCol];
          const comparison = compareValues(leftValue, rightValue);
          if (comparison !== 0) return comparison;
        }
        return 0;
      });
    }
    
    return indices;
  }, [sourceRows, sortState, edits, getCellEditedValue, isLiveMode, schema]);

  const pageCount = Math.max(1, Math.ceil(sortedRowIndices.length / PAGE_SIZE));
  const currentPageStart = pageIndex * PAGE_SIZE;
  const currentPageRows = sortedRowIndices.slice(currentPageStart, currentPageStart + PAGE_SIZE);
  const columnNames = useMemo(() => schema.map((field) => field.name), [schema]);
  const selectedCellKey = selectedCell ? `${selectedCell.rowIndex}:${selectedCell.columnName}` : null;

  useEffect(() => {
    if (pageIndex > pageCount - 1) {
      setPageIndex(Math.max(0, pageCount - 1));
    }
  }, [pageIndex, pageCount]);

  useEffect(() => {
    if (!selectedCell) {
      return;
    }

    if (!currentPageRows.includes(selectedCell.rowIndex)) {
      setSelectedCell(null);
    }
  }, [selectedCell, currentPageRows]);

  useEffect(() => {
    if (!isLiveMode) {
      return;
    }
    setSelectedRows(new Set());
  }, [isLiveMode]);

  const selectedVisibleRowCount = currentPageRows.filter((rowIndex) => selectedRows.has(rowIndex)).length;
  const allVisibleRowsSelected =
    currentPageRows.length > 0 && selectedVisibleRowCount === currentPageRows.length;

  const getPrimaryKeyValues = (rowIndex: number): Record<string, unknown> => {
    const row = sourceRows[rowIndex];
    if (!row) {
      return {};
    }

    const schemaPkColumns = schema
      .filter((field) => field.isPrimaryKey)
      .sort((left, right) => left.index - right.index)
      .map((field) => field.name);

    const selectedPkColumns = (selectedTable?.columns ?? [])
      .filter((column) => column.isPrimaryKey)
      .map((column) => column.name);

    const pkColumns =
      schemaPkColumns.length > 0
        ? schemaPkColumns
        : selectedPkColumns.length > 0
          ? selectedPkColumns
          : schema
              .map((field) => field.name)
              .filter((name) => name.toLowerCase() === "id" || name.endsWith("_id"));

    const fallbackColumn = schema[0]?.name;
    const keyColumns = pkColumns.length > 0 ? pkColumns : fallbackColumn ? [fallbackColumn] : [];

    return keyColumns.reduce<Record<string, unknown>>((acc, columnName) => {
      acc[columnName] = row[columnName];
      return acc;
    }, {});
  };

  const handleSortColumn = (columnName: string) => {
    setSortState((previous) => {
      if (!previous || previous.columnName !== columnName) {
        return { columnName, direction: "asc" };
      }
      if (previous.direction === "asc") {
        return { columnName, direction: "desc" };
      }
      return null;
    });
    setPageIndex(0);
  };

  const handleEditCell = (rowIndex: number, columnName: string, currentValue: unknown) => {
    openCellViewer(currentValue, columnName, rowIndex, true);
  };

  const handleSaveInlineEdit = (
    rowIndex: number,
    columnName: string,
    oldValue: unknown,
    newValue: unknown,
  ) => {
    const pkValues = getPrimaryKeyValues(rowIndex);
    editCell(rowIndex, columnName, oldValue, newValue, pkValues);
    setInlineEditContext(null);
  };

  const handleDeleteRow = (rowIndex: number) => {
    const row = sourceRows[rowIndex];
    if (!row) {
      return;
    }

    const pkValues = getPrimaryKeyValues(rowIndex);
    deleteRow(rowIndex, pkValues, row);
    setSelectedRows((previous) => {
      const next = new Set(previous);
      next.delete(rowIndex);
      return next;
    });
  };

  const handleDeleteSelectedRows = () => {
    const targetRows = Array.from(selectedRows);
    targetRows.forEach((rowIndex) => {
      const row = sourceRows[rowIndex];
      if (!row) {
        return;
      }
      const pkValues = getPrimaryKeyValues(rowIndex);
      deleteRow(rowIndex, pkValues, row);
    });
    setSelectedRows(new Set());
  };

  const openCellViewer = useCallback(
    (value: unknown, columnName: string, rowIndex: number, editable = false) => {
      const dataType = schema.find((field) => field.name === columnName)?.dataType;
      setCellViewer({
        open: true,
        title: `${columnName} · Row ${rowIndex + 1}${dataType ? ` (${dataType})` : ""}`,
        content: value,
        editedValue: value === null ? "" : stringifyCellValue(value),
        isNull: value === null,
        dataType,
        rowIndex,
        columnName,
        canEdit: editable && !isLiveMode,
      });
    },
    [schema, isLiveMode],
  );

  const moveSelectionByArrow = useCallback(
    (rowDelta: number, colDelta: number) => {
      if (!selectedCell || currentPageRows.length === 0 || columnNames.length === 0) {
        return;
      }

      const currentRowPosition = currentPageRows.indexOf(selectedCell.rowIndex);
      const currentColumnPosition = columnNames.findIndex((name) => name === selectedCell.columnName);

      if (currentRowPosition < 0 || currentColumnPosition < 0) {
        return;
      }

      const nextRowPosition = Math.max(0, Math.min(currentPageRows.length - 1, currentRowPosition + rowDelta));
      const nextColumnPosition = Math.max(0, Math.min(columnNames.length - 1, currentColumnPosition + colDelta));
      const nextRowIndex = currentPageRows[nextRowPosition];
      const nextColumnName = columnNames[nextColumnPosition];
      setSelectedCell({
        rowIndex: nextRowIndex,
        columnName: nextColumnName,
      });

      const nextCell = document.querySelector(
        `[data-row-index="${nextRowIndex}"][data-column-name="${nextColumnName}"]`,
      ) as HTMLElement | null;
      nextCell?.scrollIntoView({ block: "nearest", inline: "nearest" });
      nextCell?.focus();
    },
    [selectedCell, currentPageRows, columnNames],
  );

  useEffect(() => {
    if (!selectedCell) {
      return;
    }

    const handleArrowNavigation = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (
        target &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.tagName === "SELECT" ||
          target.isContentEditable)
      ) {
        return;
      }

      if (event.key === "ArrowUp") {
        event.preventDefault();
        moveSelectionByArrow(-1, 0);
      } else if (event.key === "ArrowDown") {
        event.preventDefault();
        moveSelectionByArrow(1, 0);
      } else if (event.key === "ArrowLeft") {
        event.preventDefault();
        moveSelectionByArrow(0, -1);
      } else if (event.key === "ArrowRight") {
        event.preventDefault();
        moveSelectionByArrow(0, 1);
      }
    };

    window.addEventListener("keydown", handleArrowNavigation);
    return () => window.removeEventListener("keydown", handleArrowNavigation);
  }, [selectedCell, moveSelectionByArrow]);

  const handleReviewChanges = () => {
    const parsed = extractTableContext(activeSql);
    const namespace = parsed?.namespace ?? selectedTable?.namespace;
    const tableName = parsed?.tableName ?? selectedTable?.name;

    if (!namespace || !tableName) {
      alert("Unable to determine target table for commit. Select a table and run a simple SELECT ... FROM namespace.table query.");
      return;
    }

    const generated = generateSqlStatements(namespace, tableName, edits, deletions);
    if (generated.statements.length === 0) {
      return;
    }

    openSqlPreview({
      title: "Review Changes",
      description: `${generated.updateCount} update(s), ${generated.deleteCount} delete(s)`,
      sql: generated.fullSql,
      onExecute: async (statement: string) => {
        await executeSql(statement);
      },
      onComplete: async () => {
        discardAll();
        onRefreshAfterCommit();
      },
      onDiscard: () => {
        discardAll();
      },
    });
  };

  const editCount = edits.size;
  const deleteCount = deletions.size;
  const isSuccess = !isRunning && result?.status === "success";
  const hasTabularResults = isSuccess && schema.length > 0;
  const logCount = result?.logs.length ?? 0;
  const showTableEditorBars = hasTabularResults && resultView === "results";

  return (
    <div className="flex h-full min-h-0 flex-col bg-background">
      <div className="flex h-11 items-end justify-between gap-3 border-b border-border px-3">
        <Tabs
          value={resultView}
          onValueChange={(value) => onResultViewChange(value as SqlStudioResultView)}
          className="h-full shrink-0"
        >
          <TabsList className="h-full gap-5 rounded-none bg-transparent p-0">
            <TabsTrigger
              value="results"
              className="relative h-full rounded-none border-b-2 border-transparent px-0 pt-0 text-xs font-medium text-muted-foreground shadow-none data-[state=active]:border-sky-500 data-[state=active]:bg-transparent data-[state=active]:text-foreground =active]:bg-transparent =active]:text-foreground"
            >
              <span>Results</span>
              {hasUnseenResults && (
                <span className="ml-1.5 inline-block h-1.5 w-1.5 rounded-full bg-sky-500" />
              )}
            </TabsTrigger>
            <TabsTrigger
              value="log"
              className="relative h-full rounded-none border-b-2 border-transparent px-0 pt-0 text-xs font-medium text-muted-foreground shadow-none data-[state=active]:border-sky-500 data-[state=active]:bg-transparent data-[state=active]:text-foreground =active]:bg-transparent =active]:text-foreground"
            >
              <span>Log ({logCount})</span>
              {hasUnseenLogs && (
                <span className="ml-1.5 inline-block h-1.5 w-1.5 rounded-full bg-sky-500" />
              )}
            </TabsTrigger>
          </TabsList>
        </Tabs>
        {isSuccess && (
          <div className="ml-auto flex h-full min-w-0 flex-nowrap items-center gap-2 overflow-x-auto whitespace-nowrap text-[11px] text-muted-foreground">
            <span>{result.rowCount.toLocaleString()} rows</span>
            <span>took {Math.round(result.tookMs)} ms</span>
            <span>as user: {currentUsername}</span>
            {result.rowCount > MAX_RENDERED_ROWS && (
              <span className="rounded bg-amber-500/20 px-1.5 py-0.5 text-[10px] text-amber-700">
                showing first {MAX_RENDERED_ROWS.toLocaleString()}
              </span>
            )}
            {resultView === "results" && selectedRows.size > 0 && (
              <span className="rounded bg-sky-500/20 px-1.5 py-0.5 text-[10px] text-sky-700">
                {selectedRows.size} selected
              </span>
            )}
            {resultView === "results" && sortState && (
              <span className="rounded bg-border px-1.5 py-0.5 text-[10px] text-foreground">
                {sortState.columnName} ({sortState.direction})
              </span>
            )}
            <Button
              size="sm"
              variant="destructive"
              className={cn("h-7 gap-1.5", (resultView !== "results" || selectedRows.size === 0 || isLiveMode) && "invisible pointer-events-none")}
              onClick={handleDeleteSelectedRows}
              disabled={resultView !== "results" || selectedRows.size === 0 || isLiveMode}
            >
              <Trash2 className="h-3.5 w-3.5" />
              Delete selected
            </Button>
          </div>
        )}
      </div>

      {isRunning && (
        <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
          Running query...
        </div>
      )}

      {!isRunning && !result && (
        <div className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
          Run your query to view results.
        </div>
      )}

      {!isRunning && result?.status === "error" && resultView === "results" && (
        <Alert variant="destructive" className="m-3">
          <AlertTitle>Execution failed</AlertTitle>
          <AlertDescription>
            {result.errorMessage ?? "The query could not be completed. Open the Log tab for statement-level details."}
          </AlertDescription>
        </Alert>
      )}

      {!isRunning && result?.status === "error" && resultView === "log" && (
        <StudioExecutionLog logs={result.logs} status={result.status} />
      )}

      {!isRunning && result?.status === "success" && resultView === "log" && (
        <StudioExecutionLog logs={result.logs} status={result.status} />
      )}

      {!isRunning && result?.status === "success" && resultView === "results" && !hasTabularResults && (
        <div className="flex flex-1 items-center justify-center px-4 text-sm text-muted-foreground">
          No tabular result set for this execution. Open the Log tab to inspect statement output.
        </div>
      )}

      {!isRunning && showTableEditorBars && (
        <>
          <div className="flex h-10 items-center justify-between border-b border-border bg-amber-50/70 px-3 /20">
            <div className="truncate text-xs text-amber-700">
              {changeCount === 0
                ? "No pending table changes"
                : `${changeCount} change${changeCount === 1 ? "" : "s"} • ${editCount} edit${editCount === 1 ? "" : "s"} • ${deleteCount} delete${deleteCount === 1 ? "" : "s"}`}
            </div>
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                variant="ghost"
                onClick={discardAll}
                disabled={changeCount === 0}
                className="h-7 gap-1.5 text-amber-700 hover:bg-amber-100 hover:text-amber-900 :bg-amber-900/40 :text-amber-200"
              >
                Discard
              </Button>
              <Button
                size="sm"
                onClick={handleReviewChanges}
                disabled={changeCount === 0}
                className="h-7 gap-1.5"
              >
                Review & Commit
              </Button>
            </div>
          </div>

          <ScrollArea className="min-h-0 flex-1">
            <div className="min-w-max">
              <table className="min-w-max border-collapse">
              <thead className="sticky top-0 z-10">
                <tr>
                  <th className="w-10 border-r border-border bg-background px-2 py-2 text-left">
                    {isLiveMode ? (
                      <div className="flex items-center justify-center">
                        <span className="text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">
                          Change
                        </span>
                      </div>
                    ) : (
                      <div className="flex items-center justify-center">
                        <input
                          type="checkbox"
                          checked={allVisibleRowsSelected}
                          onChange={(event) => {
                            setSelectedRows((previous) => {
                              const next = new Set(previous);
                              if (event.target.checked) {
                                currentPageRows.forEach((rowIndex) => next.add(rowIndex));
                              } else {
                                currentPageRows.forEach((rowIndex) => next.delete(rowIndex));
                              }
                              return next;
                            });
                          }}
                          className="h-3.5 w-3.5 rounded border-border bg-transparent disabled:opacity-40"
                        />
                      </div>
                    )}
                  </th>
                  {schema.map((field) => {
                    const isSorted = sortState?.columnName === field.name;
                    return (
                      <th
                        key={field.name}
                        className="border-r border-border bg-background px-2 py-2 text-left"
                      >
                        <button
                          type="button"
                          className="flex items-start gap-1 text-left"
                          onClick={() => handleSortColumn(field.name)}
                        >
                          <span>
                            <span className="flex items-center gap-1.5 text-[11px] uppercase tracking-wide">
                              <span>{field.name}</span>
                              {field.isPrimaryKey && (
                                <span
                                  className="inline-flex items-center gap-1 rounded bg-amber-400/20 px-1 py-0.5 text-[9px] font-semibold tracking-normal text-amber-300"
                                  title="Primary key column"
                                >
                                  <KeyRound className="h-2.5 w-2.5" />
                                  PK
                                </span>
                              )}
                            </span>
                            <span className="block text-[10px] font-normal uppercase text-muted-foreground">
                              {field.dataType}
                            </span>
                          </span>
                          {isSorted && (
                            sortState?.direction === "asc"
                              ? <ArrowUp className="mt-0.5 h-3 w-3 text-primary" />
                              : <ArrowDown className="mt-0.5 h-3 w-3 text-primary" />
                          )}
                        </button>
                      </th>
                    );
                  })}
                </tr>
              </thead>

              <tbody>
                {currentPageRows.map((rowIndex) => {
                  const row = sourceRows[rowIndex] as RowData | undefined;
                  if (!row) {
                    return null;
                  }

                  const rowStatus = getRowStatus(rowIndex);
                  const rowSelected = selectedRows.has(rowIndex);

                  // Live change metadata
                  const liveChangeType = isLiveMode
                    ? (row[LIVE_META.CHANGE_TYPE] as string | undefined)
                    : undefined;
                  const liveChangedAt = isLiveMode
                    ? (row[LIVE_META.CHANGED_AT] as string | undefined)
                    : undefined;
                  const liveChangedCols = isLiveMode && liveChangeType === "update"
                    ? new Set((row[LIVE_META.CHANGED_COLS] as string | undefined)?.split(",").filter(Boolean) ?? [])
                    : null;
                  // Determine if the change highlight is still fresh
                  const isRecentChange = (() => {
                    if (!liveChangedAt || !liveChangeType || liveChangeType === "initial") return false;
                    const elapsed = Date.now() - new Date(liveChangedAt).getTime();
                    return elapsed < LIVE_HIGHLIGHT_DURATION_MS;
                  })();
                  const isLiveDelete = isLiveMode && liveChangeType === "delete";
                  const isLiveInsert = isLiveMode && isRecentChange && liveChangeType === "insert";
                  const isLiveUpdate = isLiveMode && isRecentChange && liveChangeType === "update";

                  return (
                    <tr
                      key={rowIndex}
                      className={cn(
                        "border-b border-border transition-colors duration-500 ",
                        rowSelected && "bg-sky-500/10",
                        rowStatus === "edited" && "bg-amber-500/5",
                        rowStatus === "deleted" && "bg-red-500/10 opacity-60",
                        // Live mode row indicators
                        isLiveDelete && "bg-red-500/10 line-through opacity-60",
                        isLiveInsert && "bg-sky-500/10",
                        isLiveUpdate && "bg-amber-500/5",
                      )}
                    >
                      <td className="border-r border-border px-2 py-1">
                        {isLiveMode ? (
                          <div className="flex flex-col items-center justify-center gap-0.5 min-w-[60px]">
                            {liveChangeType && (
                              <>
                                <span
                                  className={cn(
                                    "rounded px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wide",
                                    liveChangeType === "initial" && "bg-background0/20 text-foreground ",
                                    liveChangeType === "insert" && "bg-sky-500/20 text-sky-700 ",
                                    liveChangeType === "update" && "bg-amber-500/20 text-amber-700 ",
                                    liveChangeType === "delete" && "bg-red-500/20 text-red-700 ",
                                  )}
                                >
                                  {liveChangeType}
                                </span>
                                {liveChangedAt && (
                                  <span className="text-[9px] text-muted-foreground">
                                    {new Date(liveChangedAt).toLocaleTimeString()}
                                  </span>
                                )}
                              </>
                            )}
                          </div>
                        ) : (
                          <div className="flex items-center justify-center">
                            <input
                              type="checkbox"
                              checked={rowSelected}
                              onChange={(event) => {
                                setSelectedRows((previous) => {
                                  const next = new Set(previous);
                                  if (event.target.checked) {
                                    next.add(rowIndex);
                                  } else {
                                    next.delete(rowIndex);
                                  }
                                  return next;
                                });
                              }}
                              className="h-3.5 w-3.5 rounded border-border bg-transparent disabled:opacity-40"
                            />
                          </div>
                        )}
                      </td>

                      {schema.map((field) => {
                        const value = getCellEditedValue(rowIndex, field.name) ?? row[field.name];
                        const cellEdited = isCellEdited(rowIndex, field.name);
                        const cellKey = `${rowIndex}:${field.name}`;

                        return (
                          <td key={`${rowIndex}-${field.name}`} className="border-r border-border px-1 py-1">
                            <div
                              data-row-index={rowIndex}
                              data-column-name={field.name}
                              onClick={() => {
                                setSelectedCell({
                                  rowIndex,
                                  columnName: field.name,
                                });
                              }}
                              onDoubleClick={() => {
                                openCellViewer(value, field.name, rowIndex, true);
                              }}
                              onContextMenu={(event) => {
                                event.preventDefault();
                                setSelectedCell({
                                  rowIndex,
                                  columnName: field.name,
                                });
                                setCellContextMenu({
                                  x: event.clientX,
                                  y: event.clientY,
                                  rowIndex,
                                  columnName: field.name,
                                  value,
                                  rowStatus,
                                  cellEdited,
                                });
                              }}
                              className={cn(
                                "min-h-6 px-1 py-0.5 font-mono text-xs outline-none transition-colors duration-500",
                                value === null && "italic text-muted-foreground",
                                cellEdited && "bg-amber-500/20",
                                selectedCellKey === cellKey && "ring-1 ring-ring",
                                // Highlight changed cells during live updates
                                isLiveUpdate && liveChangedCols?.has(field.name) && "bg-amber-400/25 ring-1 ring-amber-400/40",
                              )}
                              tabIndex={0}
                            >
                              <CellDisplay
                                value={value}
                                dataType={field.dataType}
                                namespace={cellNamespace}
                                tableName={cellTableName}
                              />
                            </div>
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
              </table>
            </div>
            <ScrollBar orientation="horizontal" />
          </ScrollArea>

          <div className="flex items-center justify-between border-t border-border bg-background px-3 py-2 text-xs text-muted-foreground">
            <div className="flex items-center gap-1">
              <Button
                size="icon"
                variant="ghost"
                className="h-7 w-7"
                onClick={() => setPageIndex(0)}
                disabled={pageIndex === 0}
              >
                <ChevronsLeft className="h-3.5 w-3.5" />
              </Button>
              <Button
                size="icon"
                variant="ghost"
                className="h-7 w-7"
                onClick={() => setPageIndex((previous) => Math.max(0, previous - 1))}
                disabled={pageIndex === 0}
              >
                <ChevronLeft className="h-3.5 w-3.5" />
              </Button>
              <Button
                size="icon"
                variant="ghost"
                className="h-7 w-7"
                onClick={() => setPageIndex((previous) => Math.min(pageCount - 1, previous + 1))}
                disabled={pageIndex >= pageCount - 1}
              >
                <ChevronRight className="h-3.5 w-3.5" />
              </Button>
              <Button
                size="icon"
                variant="ghost"
                className="h-7 w-7"
                onClick={() => setPageIndex(Math.max(0, pageCount - 1))}
                disabled={pageIndex >= pageCount - 1}
              >
                <ChevronsRight className="h-3.5 w-3.5" />
              </Button>
            </div>

            <span>
              Page {pageIndex + 1} of {pageCount}
            </span>
          </div>

          <CellContextMenu
            context={cellContextMenu}
            onEdit={handleEditCell}
            onDelete={handleDeleteRow}
            onUndoEdit={undoRowEdits}
            onUndoDelete={undeleteRow}
            onViewData={(value) => {
              const columnName = cellContextMenu?.columnName ?? "value";
              const rowIndex = cellContextMenu?.rowIndex ?? 0;
              openCellViewer(value, columnName, rowIndex, true);
            }}
            onCopyValue={(value) => {
              navigator.clipboard.writeText(stringifyCellValue(value)).catch(console.error);
            }}
            onClose={() => setCellContextMenu(null)}
          />

          <InlineCellEditor
            context={inlineEditContext}
            onSave={handleSaveInlineEdit}
            onCancel={() => setInlineEditContext(null)}
          />

          <Dialog
            open={cellViewer.open}
            onOpenChange={(open) => {
              if (!open) {
                setCellViewer((prev) => ({ ...prev, open: false }));
              }
            }}
          >
            <DialogContent className="flex max-h-[85vh] max-w-4xl flex-col overflow-hidden">
              <DialogHeader className="shrink-0">
                <DialogTitle>{cellViewer.title}</DialogTitle>
              </DialogHeader>

              {cellViewer.canEdit ? (
                <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-hidden">
                  {/* Null toggle */}
                  <label className="flex shrink-0 cursor-pointer items-center gap-2 text-sm text-muted-foreground">
                    <input
                      type="checkbox"
                      checked={cellViewer.isNull}
                      onChange={(e) => {
                        const checked = e.target.checked;
                        setCellViewer((prev) => ({
                          ...prev,
                          isNull: checked,
                          editedValue: checked ? "" : prev.editedValue,
                        }));
                      }}
                      className="h-4 w-4 rounded border-border bg-transparent"
                    />
                    Set to <span className="font-mono italic text-muted-foreground">NULL</span>
                  </label>

                  {/* Editor textarea or NULL placeholder */}
                  {cellViewer.isNull ? (
                    <div className="flex min-h-[120px] flex-1 items-center justify-center rounded-md border border-border bg-black font-mono text-sm italic text-slate-500">
                      NULL
                    </div>
                  ) : (
                    <textarea
                      className="min-h-[120px] flex-1 resize-none rounded-md border border-border bg-black p-3 font-mono text-xs leading-5 text-slate-200 outline-none focus:border-sky-500 focus:ring-1 focus:ring-ring"
                      value={typeof cellViewer.editedValue === "string" ? cellViewer.editedValue : stringifyCellValue(cellViewer.editedValue)}
                      onChange={(e) => {
                        setCellViewer((prev) => ({ ...prev, editedValue: e.target.value }));
                      }}
                      spellCheck={false}
                    />
                  )}
                </div>
              ) : (
                <div className="min-h-0 flex-1 overflow-hidden">
                  <CodeBlock
                    value={cellViewer.content}
                    jsonPreferred={(cellViewer.dataType?.toLowerCase() ?? "").includes("json")}
                    maxHeightClassName="max-h-full h-full"
                  />
                </div>
              )}

              <DialogFooter className="shrink-0 border-t border-border pt-3">
                <Button
                  variant="ghost"
                  onClick={() => setCellViewer((prev) => ({ ...prev, open: false }))}
                >
                  Cancel
                </Button>
                {cellViewer.canEdit && (
                  <Button
                    onClick={() => {
                      const { rowIndex, columnName, content, isNull, editedValue } = cellViewer;
                      if (rowIndex === undefined || !columnName) return;

                      let newValue: unknown;
                      if (isNull) {
                        newValue = null;
                      } else {
                        const raw = typeof editedValue === "string" ? editedValue : stringifyCellValue(editedValue);
                        if (
                          (cellViewer.dataType?.toLowerCase() ?? "").includes("json") ||
                          (raw.trim().startsWith("{") && raw.trim().endsWith("}")) ||
                          (raw.trim().startsWith("[") && raw.trim().endsWith("]"))
                        ) {
                          try {
                            newValue = JSON.parse(raw);
                          } catch {
                            newValue = raw;
                          }
                        } else {
                          newValue = raw;
                        }
                      }

                      const pkValues = getPrimaryKeyValues(rowIndex);
                      editCell(rowIndex, columnName, content, newValue, pkValues);
                      setCellViewer((prev) => ({ ...prev, open: false }));
                    }}
                  >
                    Save
                  </Button>
                )}
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </>
      )}
    </div>
  );
}
