import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { getApiBaseUrl, getBackendOrigin } from "./backend-url";
import { probeBackendReachability } from "./api";
import { useAuth } from "./auth";
import { connectWebSocket, isSubscriptionClientConnected, subscribeToClientConnectivity } from "./kalam-client";

type BackendStatusState = "checking" | "online" | "offline";

interface BackendStatusContextValue {
  state: BackendStatusState;
  targetOrigin: string;
  apiBaseUrl: string;
  isServerReachable: boolean;
  isWebSocketConnected: boolean;
  usingWebSocket: boolean;
  message: string;
  lastCheckedAt: number | null;
  refreshStatus: () => Promise<void>;
}

const BackendStatusContext = createContext<BackendStatusContextValue | null>(null);

function extractErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }
  return fallback;
}

export function BackendStatusProvider({ children }: { children: ReactNode }) {
  const { isAuthenticated, accessToken, isLoading } = useAuth();
  const [isServerReachable, setIsServerReachable] = useState(false);
  const [isCheckingServer, setIsCheckingServer] = useState(true);
  const [isWebSocketConnected, setIsWebSocketConnected] = useState(false);
  const [isSocketConnecting, setIsSocketConnecting] = useState(false);
  const [lastCheckedAt, setLastCheckedAt] = useState<number | null>(null);
  const [serverMessage, setServerMessage] = useState("Checking backend availability...");
  const [socketMessage, setSocketMessage] = useState("WebSocket disconnected.");
  const socketConnectInFlightRef = useRef(false);

  const refreshStatus = useCallback(async () => {
    try {
      await probeBackendReachability();
      setIsServerReachable(true);
      setServerMessage("Backend reachable.");
    } catch (error) {
      setIsServerReachable(false);
      setServerMessage(extractErrorMessage(error, "Backend is unreachable."));
    } finally {
      setIsCheckingServer(false);
      setLastCheckedAt(Date.now());
    }
  }, []);

  const ensureSocketConnected = useCallback(async () => {
    if (!isAuthenticated || !accessToken || socketConnectInFlightRef.current) {
      return;
    }

    socketConnectInFlightRef.current = true;
    setIsSocketConnecting(true);
    try {
      await connectWebSocket();
      const connected = isSubscriptionClientConnected();
      setIsWebSocketConnected(connected);
      setSocketMessage(connected ? "WebSocket connected." : "WebSocket disconnected.");
    } catch (error) {
      setIsWebSocketConnected(false);
      setSocketMessage(extractErrorMessage(error, "WebSocket disconnected."));
    } finally {
      setIsSocketConnecting(false);
      socketConnectInFlightRef.current = false;
    }
  }, [accessToken, isAuthenticated]);

  useEffect(() => {
    void refreshStatus();
  }, [refreshStatus]);

  useEffect(() => {
    if (!isAuthenticated || !accessToken) {
      setIsSocketConnecting(false);
      setIsWebSocketConnected(false);
      setSocketMessage("WebSocket disconnected.");
      return;
    }

    setIsWebSocketConnected(isSubscriptionClientConnected());
    const unsubscribeConnectivity = subscribeToClientConnectivity((event) => {
      if (event.type === "connect") {
        setIsWebSocketConnected(true);
        setIsSocketConnecting(false);
        setSocketMessage("WebSocket connected.");
        return;
      }

      if (event.type === "disconnect") {
        setIsWebSocketConnected(false);
        setIsSocketConnecting(false);
        setSocketMessage(event.reason.message || "WebSocket disconnected.");
        return;
      }

      setIsWebSocketConnected(false);
      setIsSocketConnecting(false);
      setSocketMessage(event.error.message || "WebSocket disconnected.");
    });

    const reconnectVisibleTab = () => {
      if (document.visibilityState === "hidden") {
        return;
      }

      void ensureSocketConnected();
    };

    document.addEventListener("visibilitychange", reconnectVisibleTab);
    window.addEventListener("focus", reconnectVisibleTab);
    window.addEventListener("pageshow", reconnectVisibleTab);

    void ensureSocketConnected();

    return () => {
      unsubscribeConnectivity();
      document.removeEventListener("visibilitychange", reconnectVisibleTab);
      window.removeEventListener("focus", reconnectVisibleTab);
      window.removeEventListener("pageshow", reconnectVisibleTab);
    };
  }, [accessToken, ensureSocketConnected, isAuthenticated]);

  const usingWebSocket = isAuthenticated && Boolean(accessToken);
  const state: BackendStatusState = useMemo(() => {
    if (usingWebSocket) {
      if (isWebSocketConnected) {
        return "online";
      }
      if (isSocketConnecting || isLoading) {
        return "checking";
      }
      return "offline";
    }

    if (isCheckingServer && lastCheckedAt === null) {
      return "checking";
    }
    if (!isServerReachable) {
      return "offline";
    }
    return "online";
  }, [isCheckingServer, isLoading, isServerReachable, isSocketConnecting, isWebSocketConnected, lastCheckedAt, usingWebSocket]);

  const message = useMemo(() => {
    if (usingWebSocket) {
      if (isWebSocketConnected) {
        return "WebSocket connected.";
      }
      if (isSocketConnecting || isLoading) {
        return "Connecting WebSocket...";
      }
      return socketMessage;
    }

    if (!isServerReachable) {
      return serverMessage;
    }
    return "Backend reachable.";
  }, [isLoading, isServerReachable, isSocketConnecting, isWebSocketConnected, serverMessage, socketMessage, usingWebSocket]);

  const value = useMemo<BackendStatusContextValue>(() => ({
    state,
    targetOrigin: getBackendOrigin(),
    apiBaseUrl: getApiBaseUrl(),
    isServerReachable,
    isWebSocketConnected,
    usingWebSocket,
    message,
    lastCheckedAt,
    refreshStatus,
  }), [isServerReachable, isWebSocketConnected, lastCheckedAt, message, refreshStatus, state, usingWebSocket]);

  return (
    <BackendStatusContext.Provider value={value}>
      {children}
    </BackendStatusContext.Provider>
  );
}

export function useBackendStatus() {
  const context = useContext(BackendStatusContext);
  if (!context) {
    throw new Error("useBackendStatus must be used within a BackendStatusProvider");
  }
  return context;
}