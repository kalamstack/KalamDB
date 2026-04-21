import { createSlice, type PayloadAction } from "@reduxjs/toolkit";
import type { SqlStudioPanelLayout } from "@/components/sql-studio-v2/shared/types";

interface SqlStudioUiState {
  schemaFilter: string;
  favoritesExpanded: boolean;
  namespaceSectionExpanded: boolean;
  expandedNamespaces: Record<string, boolean>;
  expandedTables: Record<string, boolean>;
  selectedTableKey: string | null;
  isInspectorCollapsed: boolean;
  horizontalLayout: SqlStudioPanelLayout;
  verticalLayout: SqlStudioPanelLayout;
}

const initialState: SqlStudioUiState = {
  schemaFilter: "",
  favoritesExpanded: true,
  namespaceSectionExpanded: true,
  expandedNamespaces: {},
  expandedTables: {},
  selectedTableKey: null,
  isInspectorCollapsed: true,
  horizontalLayout: [21, 79],
  verticalLayout: [42, 58],
};

const sqlStudioUiSlice = createSlice({
  name: "sqlStudioUi",
  initialState,
  reducers: {
    hydrateSqlStudioUi(state, action: PayloadAction<Partial<SqlStudioUiState>>) {
      const payload = action.payload;
      if (typeof payload.schemaFilter === "string") {
        state.schemaFilter = payload.schemaFilter;
      }
      if (typeof payload.favoritesExpanded === "boolean") {
        state.favoritesExpanded = payload.favoritesExpanded;
      }
      if (typeof payload.namespaceSectionExpanded === "boolean") {
        state.namespaceSectionExpanded = payload.namespaceSectionExpanded;
      }
      if (payload.expandedNamespaces) {
        state.expandedNamespaces = payload.expandedNamespaces;
      }
      if (payload.expandedTables) {
        state.expandedTables = payload.expandedTables;
      }
      if (typeof payload.selectedTableKey === "string" || payload.selectedTableKey === null) {
        state.selectedTableKey = payload.selectedTableKey;
      }
      if (typeof payload.isInspectorCollapsed === "boolean") {
        state.isInspectorCollapsed = payload.isInspectorCollapsed;
      }
      if (Array.isArray(payload.horizontalLayout) && payload.horizontalLayout.length === 2) {
        state.horizontalLayout = payload.horizontalLayout;
      }
      if (Array.isArray(payload.verticalLayout) && payload.verticalLayout.length === 2) {
        state.verticalLayout = payload.verticalLayout;
      }
    },
    setSchemaFilter(state, action: PayloadAction<string>) {
      state.schemaFilter = action.payload;
    },
    toggleFavoritesExpanded(state) {
      state.favoritesExpanded = !state.favoritesExpanded;
    },
    toggleNamespaceSectionExpanded(state) {
      state.namespaceSectionExpanded = !state.namespaceSectionExpanded;
    },
    toggleNamespaceExpanded(state, action: PayloadAction<string>) {
      const namespaceName = action.payload;
      state.expandedNamespaces[namespaceName] = !state.expandedNamespaces[namespaceName];
    },
    setNamespaceExpanded(
      state,
      action: PayloadAction<{ namespaceName: string; expanded: boolean }>,
    ) {
      state.expandedNamespaces[action.payload.namespaceName] = action.payload.expanded;
    },
    toggleTableExpanded(state, action: PayloadAction<string>) {
      const tableKey = action.payload;
      state.expandedTables[tableKey] = !state.expandedTables[tableKey];
    },
    setTableExpanded(
      state,
      action: PayloadAction<{ tableKey: string; expanded: boolean }>,
    ) {
      state.expandedTables[action.payload.tableKey] = action.payload.expanded;
    },
    setSelectedTableKey(state, action: PayloadAction<string | null>) {
      state.selectedTableKey = action.payload;
    },
    setInspectorCollapsed(state, action: PayloadAction<boolean>) {
      state.isInspectorCollapsed = action.payload;
    },
    setHorizontalLayout(state, action: PayloadAction<SqlStudioPanelLayout>) {
      state.horizontalLayout = action.payload;
    },
    setVerticalLayout(state, action: PayloadAction<SqlStudioPanelLayout>) {
      state.verticalLayout = action.payload;
    },
  },
});

export const {
  hydrateSqlStudioUi,
  setSchemaFilter,
  toggleFavoritesExpanded,
  toggleNamespaceSectionExpanded,
  toggleNamespaceExpanded,
  setNamespaceExpanded,
  toggleTableExpanded,
  setTableExpanded,
  setSelectedTableKey,
  setInspectorCollapsed,
  setHorizontalLayout,
  setVerticalLayout,
} = sqlStudioUiSlice.actions;

export default sqlStudioUiSlice.reducer;
