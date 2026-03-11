import type { RootState } from "@/store";

export const selectSqlStudioUi = (state: RootState) => state.sqlStudioUi;
export const selectSchemaFilter = (state: RootState) => state.sqlStudioUi.schemaFilter;
export const selectFavoritesExpanded = (state: RootState) => state.sqlStudioUi.favoritesExpanded;
export const selectNamespaceSectionExpanded = (state: RootState) => state.sqlStudioUi.namespaceSectionExpanded;
export const selectExpandedNamespaces = (state: RootState) => state.sqlStudioUi.expandedNamespaces;
export const selectExpandedTables = (state: RootState) => state.sqlStudioUi.expandedTables;
export const selectSelectedTableKey = (state: RootState) => state.sqlStudioUi.selectedTableKey;
export const selectIsInspectorCollapsed = (state: RootState) => state.sqlStudioUi.isInspectorCollapsed;
export const selectHorizontalLayout = (state: RootState) => state.sqlStudioUi.horizontalLayout;
export const selectVerticalLayout = (state: RootState) => state.sqlStudioUi.verticalLayout;

export const selectSqlStudioWorkspace = (state: RootState) => state.sqlStudioWorkspace;
export const selectWorkspaceTabs = (state: RootState) => state.sqlStudioWorkspace.tabs;
export const selectWorkspaceSavedQueries = (state: RootState) => state.sqlStudioWorkspace.savedQueries;
export const selectWorkspaceActiveTabId = (state: RootState) => state.sqlStudioWorkspace.activeTabId;
export const selectWorkspaceIsRunning = (state: RootState) => state.sqlStudioWorkspace.isRunning;
export const selectWorkspaceTabResults = (state: RootState) => state.sqlStudioWorkspace.tabResults;
export const selectWorkspaceHistory = (state: RootState) => state.sqlStudioWorkspace.history;
