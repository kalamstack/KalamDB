import {
  createContext,
  useContext,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import { type UserInfo, type LoginRequest } from "./api";
import { useAppDispatch, useAppSelector } from "../store/hooks";
import {
  login as loginThunk,
  logout as logoutThunk,
  refresh as refreshThunk,
  checkAuth,
} from "../store/authSlice";
import { ensureSyncedSqlStudioWorkspaceInitialized } from "@/services/sqlStudioWorkspaceSyncService";

interface AuthContextValue {
  user: UserInfo | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  accessToken: string | null;
  login: (credentials: LoginRequest) => Promise<void>;
  logout: () => Promise<void>;
  refresh: () => Promise<void>;
  error: string | null;
}

const AuthContext = createContext<AuthContextValue | null>(null);

// Refresh token 5 minutes before expiry
const REFRESH_BUFFER_MS = 5 * 60 * 1000;

export function AuthProvider({ children }: { children: ReactNode }) {
  const dispatch = useAppDispatch();
  const { user, isLoading, isAuthenticated, accessToken, expiresAt, error } =
    useAppSelector((state) => state.auth);

  // Silent refresh before token expires
  const refresh = useCallback(async () => {
    await dispatch(refreshThunk());
  }, [dispatch]);

  // Login
  const login = useCallback(
    async (credentials: LoginRequest) => {
      await dispatch(loginThunk(credentials)).unwrap();
    },
    [dispatch]
  );

  // Logout
  const logout = useCallback(async () => {
    await dispatch(logoutThunk());
  }, [dispatch]);

  // Initial auth check on mount
  useEffect(() => {
    dispatch(checkAuth());
  }, [dispatch]);

  // Setup refresh timer
  useEffect(() => {
    if (!expiresAt || !user) return;

    const expiresAtDate = new Date(expiresAt);
    const now = Date.now();
    const expiryTime = expiresAtDate.getTime();
    const refreshTime = expiryTime - REFRESH_BUFFER_MS;
    const delay = Math.max(0, refreshTime - now);

    if (delay > 0) {
      const timer = setTimeout(() => {
        refresh();
      }, delay);
      return () => clearTimeout(timer);
    } else if (expiryTime > now) {
      // Token is close to expiry, refresh now
      refresh();
    }
  }, [expiresAt, user, refresh]);

  // Ensure SQL Studio favorites storage exists for admin users on first UI entry.
  useEffect(() => {
    if (!isAuthenticated || !user || (user.role !== "dba" && user.role !== "system")) {
      return;
    }

    void ensureSyncedSqlStudioWorkspaceInitialized().catch((error) => {
      console.warn("Failed to initialize SQL Studio workspace storage", error);
    });
  }, [isAuthenticated, user]);

  const value: AuthContextValue = {
    user,
    isLoading,
    isAuthenticated,
    accessToken,
    login,
    logout,
    refresh,
    error,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
