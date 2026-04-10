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
  const sectionButtonClassName = "flex w-full items-center gap-1.5 px-1 py-1.5 text-left text-xs font-semibold uppercase tracking-wider text-muted-foreground transition-colors hover:text-foreground hover:bg-accent rounded-sm";
  const sectionBadgeClassName = "ml-auto inline-flex items-center justify-center rounded bg-muted px-1.5 py-0.5 text-[9px] font-medium tracking-normal text-foreground";

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

  // const filteredTableCount = useMemo(
  //   () => filteredSchema.reduce((total, namespace) => total + namespace.tables.length, 0),
  //   [filteredSchema],
  // );

  return (
    <TooltipProvider delayDuration={250}>
      <div className="flex h-full min-h-0 flex-col overflow-hidden border-r border-border bg-background text-foreground">
        <div className="shrink-0 border-b border-border pl-3 pr-2 py-2">
          <div className="flex items-center justify-between gap-1 mb-2">
            <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">Explorer</p>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-6 w-6 shrink-0 text-muted-foreground hover:text-foreground"
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
                <Search className="pointer-events-none absolute left-2 top-1.5 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  value={filter}
                  onChange={(event) => onFilterChange(event.target.value)}
                  className="h-7 border-border bg-muted/40 pl-7 text-xs text-foreground placeholder:text-muted-foreground focus-visible:bg-background rounded-sm"
                  placeholder="Search tables..."
                />
              </div>
            </TooltipTrigger>
            <TooltipContent>Filter namespaces, tables, and columns</TooltipContent>
          </Tooltip>
        </div>

        <ScrollArea className="h-full min-h-0 flex-1 overflow-hidden">
          <div className="space-y-1 p-2">
            <div>
              <button
                type="button"
                onClick={onToggleFavorites}
                className={sectionButtonClassName}
              >
                <div className="flex w-5 items-center justify-center shrink-0">
                  {favoritesExpanded ? (
                    <ChevronDown className="h-4 w-4" />
                  ) : (
                    <ChevronRight className="h-4 w-4" />
                  )}
                </div>
                <Star className="h-3.5 w-3.5 text-primary" />
                <span className="font-semibold ml-1">Favorites</span>
                <span className={sectionBadgeClassName}>{savedQueries.length}</span>
              </button>
              {favoritesExpanded && (
                <div className="space-y-0.5 mt-0.5 border-l border-border/40 ml-2.5 pl-2 mb-2">
                  {savedQueries.length === 0 && (
                    <p className="px-5 py-1.5 text-xs text-muted-foreground">No saved queries yet.</p>
                  )}
                  {savedQueries.map((savedQuery) => (
                    <Tooltip key={savedQuery.id}>
                      <TooltipTrigger asChild>
                        <button
                          type="button"
                          onClick={() => onOpenSavedQuery(savedQuery.id)}
                          className="flex w-full items-center gap-2 rounded-sm px-2 py-1 text-left text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                        >
                          <div className="w-5 shrink-0" />
                          <Star className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                          <span className="truncate">{savedQuery.title}</span>
                        </button>
                      </TooltipTrigger>
                      <TooltipContent>
                        {savedQuery.title}
                      </TooltipContent>
                    </Tooltip>
                  ))}
                </div>
              )}
            </div>

            <div>
              <button
                type="button"
                onClick={onToggleNamespaceSection}
                className={sectionButtonClassName}
              >
                <div className="flex w-5 items-center justify-center shrink-0">
                  {namespaceSectionExpanded ? (
                    <ChevronDown className="h-4 w-4" />
                  ) : (
                    <ChevronRight className="h-4 w-4" />
                  )}
                </div>
                <FolderTree className="h-3.5 w-3.5" />
                <span className="font-semibold ml-1">Namespaces</span>
                <span className={sectionBadgeClassName}>{filteredSchema.length}</span>
              </button>
              {namespaceSectionExpanded && (
                <div className="space-y-0.5 mt-0.5 pb-2">
                  {filteredSchema.length === 0 && (
                    <div className="px-8 py-1.5 text-xs text-muted-foreground">
                      No matching namespaces or tables.
                    </div>
                  )}
                  {filteredSchema.map((namespace) => {
                    const namespaceOpen = expandedNamespaces[namespace.name] ?? false;
                    return (
                      <div key={namespace.name}>
                        <button
                          type="button"
                          onClick={() => onToggleNamespace(namespace.name)}
                          className="flex w-full items-center gap-2 rounded-sm px-2 py-1 text-left text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                        >
                          <div className="flex w-5 items-center justify-center shrink-0">
                            {namespaceOpen ? (
                              <ChevronDown className="h-4 w-4" />
                            ) : (
                              <ChevronRight className="h-4 w-4" />
                            )}
                          </div>
                          <Database className="h-3.5 w-3.5 shrink-0" />
                          <span className="truncate font-medium flex-1">{namespace.name}</span>
                          <span className="text-[10px] tabular-nums text-muted-foreground/60">{namespace.tables.length}</span>
                        </button>

                        {namespaceOpen && (
                          <div className="space-y-0.5">
                            {namespace.tables.map((table) => {
                              const tableKey = `${table.namespace}.${table.name}`;
                              const tableOpen = expandedTables[tableKey] ?? tableKey === selectedTableKey;
                              const isSelected = selectedTableKey === tableKey;
                              const tableMeta = tableTypeMeta(table.tableType);

                              return (
                                <div key={tableKey}>
                                  <div
                                    className={cn(
                                      "flex w-full items-center gap-2 rounded-sm pr-2 py-1 text-left text-xs transition-colors cursor-pointer",
                                      isSelected
                                        ? "bg-sky-500/15 text-sky-400 font-medium"
                                        : "text-muted-foreground hover:bg-accent hover:text-foreground",
                                    )}
                                    onClick={() => onSelectTable(table)}
                                    onContextMenu={(event) => {
                                      event.preventDefault();
                                      event.stopPropagation();
                                      onSelectTable(table);
                                      onTableContextMenu(table, { x: event.clientX, y: event.clientY });
                                    }}
                                  >
                                    <div className="w-5 shrink-0" />
                                    <div 
                                      className="flex w-5 items-center justify-center shrink-0 hover:bg-muted rounded"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        onToggleTable(tableKey);
                                      }}
                                    >
                                      {tableOpen ? (
                                        <ChevronDown className="h-4 w-4" />
                                      ) : (
                                        <ChevronRight className="h-4 w-4" />
                                      )}
                                    </div>
                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        <div className="shrink-0">{tableMeta.icon}</div>
                                      </TooltipTrigger>
                                      <TooltipContent>{tableMeta.tooltip}</TooltipContent>
                                    </Tooltip>
                                    <span className="truncate flex-1">{table.name}</span>
                                    <span className="text-[10px] tabular-nums text-muted-foreground/60 opacity-0 group-hover:opacity-100 transition-opacity">{table.columns.length}</span>
                                  </div>

                                  {tableOpen && (
                                    <div className="space-y-0.5">
                                      {table.columns.map((column) => (
                                        <Tooltip key={`${tableKey}.${column.name}`}>
                                          <TooltipTrigger asChild>
                                            <div className="flex items-center gap-2 rounded-sm pr-2 py-0.5 text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground cursor-default">
                                              <div className="w-5 shrink-0" />
                                              <div className="w-5 shrink-0" />
                                              <div className="flex w-5 shrink-0 items-center justify-center">
                                                {columnIcon(column.isPrimaryKey)}
                                              </div>
                                              <span className="truncate">{column.name}</span>
                                              <span className="ml-auto truncate font-mono text-[9px] lowercase opacity-70">
                                                {column.dataType}
                                              </span>
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
                              );
                            })}
                          </div>
                        )}
                      </div>
                    );
                  })}
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
