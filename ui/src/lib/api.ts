import { KalamCellValue, type SchemaField } from "@kalamdb/client";
import { executeQuery, getCurrentToken } from "./kalam-client";
import { getApiBaseUrl } from "./backend-url";

// HTTP API client for auth/setup endpoints.
// SQL calls should go through @kalamdb/client in `kalam-client.ts`.

const API_BASE = getApiBaseUrl();
const NO_AUTH_ENDPOINTS = new Set([
  "/auth/login",
  "/auth/refresh",
  "/auth/setup",
  "/auth/status",
]);

export interface ApiError {
  error: string;
  message: string;
  details?: Record<string, unknown>;
}

export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE) {
    this.baseUrl = baseUrl;
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    const headers = new Headers(options.headers);
    if (!headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }

    const token = getCurrentToken();
    if (token && !NO_AUTH_ENDPOINTS.has(endpoint) && !headers.has("Authorization")) {
      headers.set("Authorization", `Bearer ${token}`);
    }

    const response = await fetch(url, {
      ...options,
      credentials: "include",
      headers,
    });

    if (!response.ok) {
      const error: ApiError = await response.json().catch(() => ({
        error: "unknown_error",
        message: `Request failed with status ${response.status}`,
      }));
      throw new ApiRequestError(error, response.status);
    }

    // Handle empty responses
    const text = await response.text();
    if (!text) {
      return {} as T;
    }
    return JSON.parse(text);
  }

  async get<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: "GET" });
  }

  async post<T>(endpoint: string, body?: unknown): Promise<T> {
    return this.request<T>(endpoint, {
      method: "POST",
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async put<T>(endpoint: string, body?: unknown): Promise<T> {
    return this.request<T>(endpoint, {
      method: "PUT",
      body: body ? JSON.stringify(body) : undefined,
    });
  }

  async delete<T>(endpoint: string): Promise<T> {
    return this.request<T>(endpoint, { method: "DELETE" });
  }
}

export class ApiRequestError extends Error {
  constructor(
    public apiError: ApiError,
    public status: number
  ) {
    super(apiError.message);
    this.name = "ApiRequestError";
  }

  get isUnauthorized(): boolean {
    return this.status === 401;
  }

  get isForbidden(): boolean {
    return this.status === 403;
  }
}

// Singleton instance
export const api = new ApiClient();

// SQL execution types
export interface SqlRequest {
  sql: string;
  namespace?: string;
}

export type SqlRow = KalamCellValue[];

// Query result alias for hooks
export interface QueryResult {
  schema: SchemaField[];
  rows: SqlRow[];
  row_count: number;
  truncated: boolean;
  execution_time_ms: number;
  as_user?: string;
}

export interface SqlResponse {
  schema: SchemaField[];
  rows: SqlRow[];
  row_count: number;
  truncated: boolean;
  execution_time_ms: number;
  as_user?: string;
}

function wrapSqlRows(rows: unknown[][] | undefined): SqlRow[] {
  if (!rows || rows.length === 0) {
    return [];
  }

  return rows.map((row) => row.map((value) => KalamCellValue.from(value)));
}

export async function executeSql(sql: string, namespace?: string): Promise<SqlResponse> {
  if (namespace && namespace.trim().length > 0) {
    console.warn("[api] executeSql namespace parameter is ignored; SQL execution now routes through @kalamdb/client");
  }

  const response = await executeQuery(sql);
  const result = response.results?.[0];

  return {
    schema: (result?.schema ?? []) as SchemaField[],
    rows: wrapSqlRows(result?.rows as unknown[][] | undefined),
    row_count: result?.row_count ?? 0,
    truncated: false,
    execution_time_ms: response.took ?? 0,
    as_user: (result as { as_user?: string } | undefined)?.as_user,
  };
}

// Auth API helpers
export interface LoginRequest {
  user: string;
  password: string;
}

export interface UserInfo {
  id: string;
  username?: string;
  role: string;
  email: string | null;
  created_at: string;
  updated_at: string;
}

export interface LoginResponse {
  user: UserInfo;
  admin_ui_access: boolean;
  expires_at: string;
  access_token: string;
  refresh_token: string;
  refresh_expires_at: string;
}

export interface AuthStatusResponse {
  needs_setup: boolean;
  message?: string;
}

export const authApi = {
  status: () => api.get<AuthStatusResponse>("/auth/status"),

  login: (credentials: LoginRequest) =>
    api.post<LoginResponse>("/auth/login", credentials),
  
  logout: () => api.post("/auth/logout"),
  
  refresh: () => api.post<LoginResponse>("/auth/refresh"),
  
  me: () => api.get<UserInfo>("/auth/me"),
};

export async function probeBackendReachability(): Promise<AuthStatusResponse> {
  return authApi.status();
}
