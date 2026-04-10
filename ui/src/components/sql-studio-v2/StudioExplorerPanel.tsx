import { memo, type ReactNode, useMemo } from "react";
import {
  ChevronDown,
  ChevronRight,
  Database,
  FolderTree,
  KeyRound,
  RefreshCw,
  Radio,
  Search,
  Star,
  Type,
  User,
  Users,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import type { StudioNamespace, StudioTable } from "./types";
import type { SavedQuery } from "./types";

interface StudioExplorerPanelProps {
  schema: StudioNamespace[];
  filter: string;
  savedQueries: SavedQuery[];
  favoritesExpanded: boolean;
  namespaceSectionExpanded: boolean;
  expandedNamespaces: Record<string, boolean>;
  expandedTables: Record<string, boolean>;
  selectedTableKey: string | null;
  isRefreshing: boolean;
  onFilterChange: (value: string) => void;
  onRefresh: () => void;
  onToggleFavorites: () => void;
  onToggleNamespaceSection: () => void;
  onToggleNamespace: (namespaceName: string) => void;
  onToggleTable: (tableKey: string) => void;
  onOpenSavedQuery: (queryId: string) => void;
  onSelectTable: (table: StudioTable) => void;
  onTableContextMenu: (table: StudioTable, position: { x: number; y: number }) => void;
}

function columnIcon(isPrimaryKey: boolean) {
  if (isPrimaryKey) {
    return <KeyRound className="h-3 w-3 text-amber-500" />;
  }
  return <Type className="h-3 w-3 text-muted-foreground" />;
}

function tableTypeMeta(tableType: string): { icon: ReactNode; tooltip: string } {
  const normalized = tableType.toLowerCase();
  if (normalized === "stream") {
    return {
      icon: <Radio className="h-3.5 w-3.5 text-violet-400" />,
      tooltip: "Stream table",
    };
  }
  if (normalized === "shared") {
    return {
      icon: <Users className="h-3.5 w-3.5 text-cyan-400" />,
      tooltip: "Shared table",
    };
  }
  if (normalized === "system") {
    return {
      icon: <Database className="h-3.5 w-3.5 text-amber-400" />,
      tooltip: "System table",
    };
  }
  return {
    icon: <User className="h-3.5 w-3.5 text-emerald-400" />,
    tooltip: "User table",
  };
}

const StudioExplorerPanelComponent = ({
  schema,
  filter,
  savedQueries,
  favoritesExpanded,
  namespaceSectionExpanded,
  expandedNamespaces,
  expandedTables,
  selectedTableKey,
  isRefreshing,
  onFilterChange,
  onRefresh,
  onToggleFavorites,
  onToggleNamespaceSection,
  onToggleNamespace,
  onToggleTable,
  onOpenSavedQuery,
  onSelectTable,
  onTableContextMenu,
}: StudioExplorerPanelProps) => {
  const normalizedFilter = filter.trim().toLowerCase();
  const sectionButtonClassName = "mb-2 flex w-full items-center gap-2 rounded-xl border border-border/70 bg-background/80 px-2.5 py-2 text-left text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground shadow-sm transition-colors hover:border-border hover:bg-background";
  const sectionBadgeClassName = "ml-auto inline-flex min-w-5 items-center justify-center rounded-full bg-muted px-1.5 py-0.5 text-[10px] font-semibold tracking-normal text-foreground";

  const filteredSchema = useMemo(() => {
    return schema
      .map((namespace) => {
        const filteredTables = namespace.tables.filter((table) => {
          if (!normalizedFilter) {
            return true;
          }

          return (
            namespace.name.toLowerCase().includes(normalizedFilter) ||
            table.name.toLowerCase().includes(normalizedFilter) ||
            table.columns.some((column) => column.name.toLowerCase().includes(normalizedFilter))
          );
        });

        return {
          ...namespace,
          tables: filteredTables,
        };
      })
      .filter((namespace) => namespace.tables.length > 0 || !normalizedFilter);
  }, [schema, normalizedFilter]);

  const filteredTableCount = useMemo(
    () => filteredSchema.reduce((total, namespace) => total + namespace.tables.length, 0),
    [filteredSchema],
  );

  return (
    <TooltipProvider delayDuration={250}>
      <div className="flex h-full min-h-0 flex-col overflow-hidden border-r border-border bg-muted/30 text-muted-foreground">
        <div className="shrink-0 border-b border-border px-3 py-3">
          <div className="mb-2 flex items-center justify-between gap-2">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Explorer</p>
            <Button
              type="button"
              variant="outline"
              size="icon"
              className="h-7 w-7 shrink-0"
              onClick={onRefresh}
              disabled={isRefreshing}
              aria-label="Refresh explorer"
              title="Refresh explorer"
            >
              <RefreshCw className={cn("h-3.5 w-3.5", isRefreshing && "animate-spin")} />
            </Button>
          </div>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="relative">
                <Search className="pointer-events-none absolute left-2 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  value={filter}
                  onChange={(event) => onFilterChange(event.target.value)}
                  className="h-8 border-border bg-background pl-7 text-xs text-foreground placeholder:text-muted-foreground"
                  placeholder="Search tables..."
                />
              </div>
            </TooltipTrigger>
            <TooltipContent>Filter namespaces, tables, and columns</TooltipContent>
          </Tooltip>
        </div>

        <ScrollArea className="h-full min-h-0 flex-1 overflow-hidden">
          <div className="space-y-3 p-2">
            <div>
              <button
                type="button"
                onClick={onToggleFavorites}
                className={sectionButtonClassName}
              >
                {favoritesExpanded ? (
                  <ChevronDown className="h-3 w-3" />
                ) : (
                  <ChevronRight className="h-3 w-3" />
                )}
                <span className="inline-flex h-6 w-6 items-center justify-center rounded-lg bg-primary/10 text-primary ring-1 ring-primary/20">
                  <Star className="h-3.5 w-3.5" />
                </span>
                Favorites
                <span className={sectionBadgeClassName}>{savedQueries.length}</span>
              </button>
              {favoritesExpanded && (
                <div className="space-y-2">
                  <div className="ml-1 border-l border-border pl-3">
                    <div className="space-y-0.5">
                      {savedQueries.length === 0 && (
                        <p className="px-2 py-1 text-xs text-muted-foreground">No saved queries yet.</p>
                      )}
                      {savedQueries.map((savedQuery) => (
                        <Tooltip key={savedQuery.id}>
                          <TooltipTrigger asChild>
                            <button
                              type="button"
                              onClick={() => onOpenSavedQuery(savedQuery.id)}
                              className="flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-sky-300"
                            >
                              <Star className="h-3.5 w-3.5 text-primary" />
                              <span className="truncate">{savedQuery.title}</span>
                            </button>
                          </TooltipTrigger>
                          <TooltipContent>
                            {savedQuery.title}
                          </TooltipContent>
                        </Tooltip>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>

            <div>
              <button
                type="button"
                onClick={onToggleNamespaceSection}
                className={sectionButtonClassName}
              >
                {namespaceSectionExpanded ? (
                  <ChevronDown className="h-3 w-3" />
                ) : (
                  <ChevronRight className="h-3 w-3" />
                )}
                <span className="inline-flex h-6 w-6 items-center justify-center rounded-lg bg-sky-500/10 text-sky-400 ring-1 ring-sky-500/20">
                  <FolderTree className="h-3.5 w-3.5" />
                </span>
                Namespaces
                <span className={sectionBadgeClassName}>{filteredSchema.length}</span>
              </button>
              {namespaceSectionExpanded && (
                <div className="ml-2 space-y-3 border-l-2 border-border/60 pl-3">
                  {filteredSchema.length === 0 && (
                    <div className="rounded-xl border border-dashed border-border/70 bg-background/70 px-3 py-4 text-xs text-muted-foreground shadow-sm">
                      No matching namespaces or tables.
                    </div>
                  )}
                  {filteredSchema.map((namespace) => {
                    const namespaceOpen = expandedNamespaces[namespace.name] ?? false;
                    return (
                      <div key={namespace.name} className="relative">
                        <div className="absolute -left-[17px] top-5 h-px w-3 bg-border/80" />
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button
                              type="button"
                              onClick={() => onToggleNamespace(namespace.name)}
                              className={cn(
                                "flex w-full items-center gap-2 rounded-xl border px-3 py-2 text-left text-[11px] shadow-sm transition-colors",
                                namespaceOpen
                                  ? "border-sky-500/25 bg-background text-foreground"
                                  : "border-border/70 bg-background/80 text-muted-foreground hover:border-border hover:bg-background",
                              )}
                            >
                              <span className="inline-flex h-5 w-5 items-center justify-center rounded-md bg-muted text-muted-foreground">
                                {namespaceOpen ? (
                                  <ChevronDown className="h-3 w-3" />
                                ) : (
                                  <ChevronRight className="h-3 w-3" />
                                )}
                              </span>
                              <span className="inline-flex h-7 w-7 items-center justify-center rounded-lg bg-sky-500/10 text-sky-400 ring-1 ring-sky-500/15">
                                <Database className="h-3.5 w-3.5" />
                              </span>
                              <span className="min-w-0 flex-1">
                                <span className="block truncate text-sm font-semibold text-foreground">{namespace.name}</span>
                                <span className="block truncate text-[10px] uppercase tracking-[0.14em] text-muted-foreground">
                                  {namespace.tables.length} {namespace.tables.length === 1 ? "table" : "tables"}
                                </span>
                              </span>
                              <span className="inline-flex min-w-6 items-center justify-center rounded-full bg-muted px-1.5 py-0.5 text-[10px] font-semibold text-foreground">
                                {namespace.tables.length}
                              </span>
                            </button>
                          </TooltipTrigger>
                          <TooltipContent>Namespace: {namespace.name}</TooltipContent>
                        </Tooltip>

                        {namespaceOpen && (
                          <div className="ml-3 mt-2 space-y-2 border-l border-dashed border-border/80 pl-4">
                            {namespace.tables.map((table) => {
                              const tableKey = `${table.namespace}.${table.name}`;
                              const tableOpen = expandedTables[tableKey] ?? tableKey === selectedTableKey;
                              const isSelected = selectedTableKey === tableKey;
                              const tableMeta = tableTypeMeta(table.tableType);

                              return (
                                <div key={tableKey} className="relative">
                                  <div className="absolute -left-4 top-4 h-px w-4 bg-border/70" />
                                  <div
                                    className={cn(
                                      "group rounded-xl border px-1.5 py-1.5 shadow-sm transition-colors",
                                      isSelected
                                        ? "border-sky-500/30 bg-accent/80 text-sky-300"
                                        : "border-border/70 bg-background/70 text-muted-foreground hover:border-border hover:bg-background hover:text-foreground",
                                    )}
                                    onContextMenu={(event) => {
                                      event.preventDefault();
                                      event.stopPropagation();
                                      onSelectTable(table);
                                      onTableContextMenu(table, { x: event.clientX, y: event.clientY });
                                    }}
                                  >
                                    <div className="flex items-center gap-1">
                                      <button
                                        type="button"
                                        onClick={() => onToggleTable(tableKey)}
                                        aria-label={tableOpen ? `Collapse ${table.name}` : `Expand ${table.name}`}
                                        className="inline-flex h-7 w-7 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
                                      >
                                        {tableOpen ? (
                                          <ChevronDown className="h-3 w-3" />
                                        ) : (
                                          <ChevronRight className="h-3 w-3" />
                                        )}
                                      </button>

                                      <button
                                        type="button"
                                        onClick={() => onSelectTable(table)}
                                        className="flex min-w-0 flex-1 items-center gap-2 text-left"
                                      >
                                        <Tooltip>
                                          <TooltipTrigger asChild>
                                            <span className="inline-flex h-7 w-7 items-center justify-center rounded-lg bg-muted/80 ring-1 ring-border/60">
                                              {tableMeta.icon}
                                            </span>
                                          </TooltipTrigger>
                                          <TooltipContent>{tableMeta.tooltip}</TooltipContent>
                                        </Tooltip>
                                        <span className="min-w-0 flex-1">
                                          <span className="block truncate text-sm font-medium text-foreground">{table.name}</span>
                                          <span className="block truncate text-[10px] uppercase tracking-[0.14em] text-muted-foreground">
                                            {table.columns.length} {table.columns.length === 1 ? "column" : "columns"}
                                          </span>
                                        </span>
                                      </button>
                                      <span className="inline-flex min-w-6 items-center justify-center rounded-full bg-muted px-1.5 py-0.5 text-[10px] font-semibold text-foreground">
                                        {table.columns.length}
                                      </span>
                                    </div>

                                    {tableOpen && (
                                      <div className="ml-6 mt-2 space-y-1.5 rounded-lg border border-border/70 bg-background/85 px-2.5 py-2 shadow-sm">
                                        <div className="flex items-center gap-2 px-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                                          <span className="h-1.5 w-1.5 rounded-full bg-sky-400/70" />
                                          Columns
                                          <span className="ml-auto inline-flex min-w-5 items-center justify-center rounded-full bg-muted px-1.5 py-0.5 text-[9px] font-semibold tracking-normal text-foreground">
                                            {table.columns.length}
                                          </span>
                                        </div>
                                        {table.columns.map((column) => (
                                          <Tooltip key={`${tableKey}.${column.name}`}>
                                            <TooltipTrigger asChild>
                                              <div className="flex items-center gap-2 rounded-md px-2 py-1 text-xs text-muted-foreground transition-colors hover:bg-muted/80 hover:text-foreground">
                                                {columnIcon(column.isPrimaryKey)}
                                                <span className="truncate">{column.name}</span>
                                                <span className="ml-auto truncate font-mono text-[10px] lowercase">{column.dataType}</span>
                                              </div>
                                            </TooltipTrigger>
                                            <TooltipContent>
                                              {column.name} ({column.dataType})
                                            </TooltipContent>
                                          </Tooltip>
                                        ))}
                                      </div>
                                    )}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        )}
                      </div>
                    );
                  })}
                  {filteredSchema.length > 0 && filteredTableCount > 0 && (
                    <div className="pl-1 text-[10px] uppercase tracking-[0.14em] text-muted-foreground">
                      {filteredTableCount} visible {filteredTableCount === 1 ? "table" : "tables"}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </ScrollArea>

      </div>
    </TooltipProvider>
  );
};

export const StudioExplorerPanel = memo(StudioExplorerPanelComponent);
