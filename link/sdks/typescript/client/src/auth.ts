/**
 * Authentication types and utilities for KalamDB client
 * 
 * Provides a type-safe authentication API with support for:
 * - Basic Auth (username/password)
 * - JWT Token Auth
 * - Anonymous (no auth - for localhost bypass)
 * - Dynamic provider (async callback for refresh-token flows)
 */

/**
 * Basic authentication credentials (username/password)
 */
export interface BasicAuthCredentials {
  type: 'basic';
  username: string;
  password: string;
}

/**
 * JWT token authentication credentials
 */
export interface JwtAuthCredentials {
  type: 'jwt';
  token: string;
}

/**
 * No authentication (anonymous access)
 */
export interface NoAuthCredentials {
  type: 'none';
}

/**
 * Union type for all authentication credential types
 */
export type AuthCredentials = BasicAuthCredentials | JwtAuthCredentials | NoAuthCredentials;

type BufferCtor = {
  from(input: string, encoding?: string): {
    toString(encoding: string): string;
  };
};

/**
 * Type guard to check if credentials are Basic Auth
 */
export function isBasicAuth(auth: AuthCredentials): auth is BasicAuthCredentials {
  return auth.type === 'basic';
}

/**
 * Type guard to check if credentials are JWT Auth
 */
export function isJwtAuth(auth: AuthCredentials): auth is JwtAuthCredentials {
  return auth.type === 'jwt';
}

/**
 * Type guard to check if credentials are No Auth
 */
export function isNoAuth(auth: AuthCredentials): auth is NoAuthCredentials {
  return auth.type === 'none';
}

/**
 * Type guard to check if any authentication is configured
 */
export function isAuthenticated(auth: AuthCredentials): auth is BasicAuthCredentials | JwtAuthCredentials {
  return auth.type !== 'none';
}

/**
 * Base64 encode a string (works in both Node.js and browser)
 */
function base64Encode(str: string): string {
  const runtime = globalThis as typeof globalThis & {
    btoa?: (input: string) => string;
    Buffer?: BufferCtor;
  };

  if (typeof runtime.btoa === 'function') {
    // Browser environment
    return runtime.btoa(str);
  } else if (runtime.Buffer) {
    // Node.js environment
    return runtime.Buffer.from(str, 'utf8').toString('base64');
  }
  throw new Error('No base64 encoding available');
}

/**
 * Encode username and password for Basic Auth header
 * 
 * @param username - Username
 * @param password - Password
 * @returns Base64 encoded credentials string
 */
export function encodeBasicAuth(username: string, password: string): string {
  return base64Encode(`${username}:${password}`);
}

/**
 * Build the Authorization header value for the given credentials
 * 
 * @param auth - Authentication credentials
 * @returns Authorization header value or undefined for no auth
 */
export function buildAuthHeader(auth: AuthCredentials): string | undefined {
  switch (auth.type) {
    case 'basic':
      return `Basic ${encodeBasicAuth(auth.username, auth.password)}`;
    case 'jwt':
      return `Bearer ${auth.token}`;
    case 'none':
      return undefined;
    default:
      // Exhaustiveness check
      const _exhaustive: never = auth;
      throw new Error(`Unknown auth type: ${(_exhaustive as AuthCredentials).type}`);
  }
}

/**
 * Auth factory for creating type-safe authentication credentials
 * 
 * @example
 * ```typescript
 * import { createClient, Auth } from '@kalamdb/client';
 * 
 * // Basic Auth
 * const client = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.basic('admin', 'admin')
 * });
 * 
 * // JWT Token
 * const jwtClient = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.jwt('eyJhbGciOiJIUzI1NiIs...')
 * });
 * 
 * // Anonymous (no auth)
 * const anonClient = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider: async () => Auth.none()
 * });
 * ```
 */
export const Auth = {
  /**
   * Create Basic Auth credentials
   * 
   * @param username - Username for authentication
   * @param password - Password for authentication
   * @returns BasicAuthCredentials object
   */
  basic(username: string, password: string): BasicAuthCredentials {
    return { type: 'basic', username, password };
  },

  /**
   * Create JWT Auth credentials
   * 
   * @param token - JWT token string
   * @returns JwtAuthCredentials object
   */
  jwt(token: string): JwtAuthCredentials {
    return { type: 'jwt', token };
  },

  /**
   * Create No Auth credentials (anonymous access)
   * 
   * @returns NoAuthCredentials object
   */
  none(): NoAuthCredentials {
    return { type: 'none' };
  }
} as const;

/**
 * Async authentication provider callback.
 *
 * Called before each (re-)connection attempt to obtain fresh credentials.
 * This is the recommended approach for refresh-token flows.
 *
 * The returned `AuthCredentials` can be any auth type. Returning
 * `{ type: 'basic' }` is supported — the SDK will automatically exchange
 * the credentials for a JWT before establishing the WebSocket connection.
 *
 * @example
 * ```typescript
 * const authProvider: AuthProvider = async () => {
 *   const token = await myApp.getOrRefreshJwt();
 *   return Auth.jwt(token);
 * };
 *
 * const client = createClient({
 *   url: 'http://localhost:8080',
 *   authProvider,
 * });
 * ```
 */
export type AuthProvider = () => Promise<AuthCredentials>;

