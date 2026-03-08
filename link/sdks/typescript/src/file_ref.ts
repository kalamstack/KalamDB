/**
 * FileRef — Type-safe representation of the FILE datatype in KalamDB.
 *
 * ## Architecture
 *
 * The canonical `FileRef` lives in Rust (`kalam-link/src/models/file_ref.rs`).
 * When the WASM bindings are rebuilt, tsify generates a matching `FileRef`
 * TypeScript interface in `wasm/kalam_link.d.ts`.
 *
 * This module provides:
 *
 * - **`FileRefData`** — mirrors the Rust struct (once WASM is rebuilt,
 *   re-export the auto-generated interface from `wasm/` instead).
 * - **`FileRef`** — thin TS class wrapping `FileRefData`, adds convenience
 *   methods (same logic as Rust `impl FileRef`).
 * - **`BoundFileRef`** — TS-only: binds server/table context for no-arg URLs.
 * - **`KalamRow<T>`** — TS-only: generic typed row wrapper with `.file()`.
 * - **`KalamChange<T>`** — TS-only: generic typed change event wrapper.
 *
 * The `FileRef` methods (`downloadUrl`, `isImage`, `formatSize`, etc.) mirror
 * the Rust impl.  They're kept in TS (not WASM calls) to avoid crossing the
 * WASM boundary for trivial string operations — each SDK implements them from
 * the same Rust reference.
 *
 * `KalamRow<T>` and `KalamChange<T>` are inherently SDK-level because they
 * use TypeScript generics (Rust FFI can't express `<T>` row types).
 *
 * @example
 * ```typescript
 * const fileRef = FileRef.from(row.avatar);
 * const url = fileRef.getDownloadUrl('http://localhost:8080', 'default', 'users');
 * ```
 */

// Import KalamCellValue for unwrapping typed cell values in KalamRow.file()
// Note: lazy import pattern avoids circular dependency (cell_value.ts → file_ref.ts → cell_value.ts)

import { KalamCellValue, wrapRowMap } from './cell_value.js';
import type { RowData } from './cell_value.js';

/**
 * Data shape for a file reference stored in FILE columns.
 *
 * **Source of truth**: `kalam-link/src/models/file_ref.rs` → `struct FileRef`.
 * Once WASM bindings are rebuilt, tsify generates an identical interface in
 * `wasm/kalam_link.d.ts` which should be re-exported here.
 *
 * Each SDK (TypeScript, Dart, etc.) must match this shape exactly.
 */
export interface FileRefData {
  /** Unique file identifier (Snowflake ID) */
  id: string;

  /** Subfolder name (e.g., "f0001", "f0002") */
  sub: string;

  /** Original filename (preserved for display/download) */
  name: string;

  /** File size in bytes */
  size: number;

  /** MIME type (e.g., "image/png", "application/pdf") */
  mime: string;

  /** SHA-256 hash of file content (hex-encoded) */
  sha256: string;

  /** Optional shard ID for shared tables */
  shard?: number;
}

/**
 * Type-safe FileRef class for working with FILE columns.
 *
 * Methods mirror the Rust `impl FileRef` in `kalam-link/src/models/file_ref.rs`.
 * Implemented in TypeScript to avoid per-call WASM boundary overhead for
 * trivial string operations — each SDK follows the same Rust reference.
 */
export class FileRef implements FileRefData {
  id: string;
  sub: string;
  name: string;
  size: number;
  mime: string;
  sha256: string;
  shard?: number;

  constructor(data: FileRefData) {
    this.id = data.id;
    this.sub = data.sub;
    this.name = data.name;
    this.size = data.size;
    this.mime = data.mime;
    this.sha256 = data.sha256;
    this.shard = data.shard;
  }

  /**
   * Parse FileRef from JSON string (as stored in FILE columns)
   *
   * @param json - JSON string from FILE column
   * @returns FileRef instance
   * @throws Error if JSON is invalid
   *
   * @example
   * ```typescript
   * const fileRef = FileRef.fromJson(row.document);
   * console.log(fileRef.name, fileRef.size);
   * ```
   */
  static fromJson(json: string | null | undefined): FileRef | null {
    if (!json) return null;

    try {
      const data = typeof json === 'string' ? JSON.parse(json) : json;
      return new FileRef(data);
    } catch (err) {
      console.error('[FileRef] Failed to parse JSON:', err);
      return null;
    }
  }

  /**
   * Parse FileRef from unknown value (handles JSON string or object)
   *
   * @param value - Value from FILE column (string or object)
   * @returns FileRef instance or null
   *
   * @example
   * ```typescript
   * const fileRef = FileRef.from(row.avatar);
   * if (fileRef) {
   *   console.log('Avatar:', fileRef.name);
   * }
   * ```
   */
  static from(value: unknown): FileRef | null {
    if (!value) return null;

    if (typeof value === 'string') {
      return FileRef.fromJson(value);
    }

    if (typeof value === 'object' && value !== null) {
      return new FileRef(value as FileRefData);
    }

    return null;
  }

  /**
   * Generate download URL for this file
   *
   * @param baseUrl - KalamDB server URL (e.g., 'http://localhost:8080')
   * @param namespace - Table namespace
   * @param table - Table name
   * @returns Full download URL
   *
   * @example
   * ```typescript
   * const url = fileRef.getDownloadUrl('http://localhost:8080', 'default', 'users');
   * // Returns: http://localhost:8080/api/v1/files/default/users/f0001/12345
   * ```
   */
  getDownloadUrl(baseUrl: string, namespace: string, table: string): string {
    const cleanUrl = baseUrl.replace(/\/$/, '');
    return `${cleanUrl}/api/v1/files/${namespace}/${table}/${this.sub}/${this.id}`;
  }

  /**
   * Get the stored filename (with ID prefix)
   *
   * Format: `{id}-{sanitized_name}.{ext}` or `{id}.{ext}` if name is non-ASCII
   *
   * @returns Stored filename
   */
  storedName(): string {
    const sanitized = this.sanitizeFilename(this.name);
    const ext = this.extractExtension(this.name);

    if (sanitized.length === 0) {
      return `${this.id}.${ext}`;
    }

    return `${this.id}-${sanitized}.${ext}`;
  }

  /**
   * Get the relative path within the table folder
   *
   * For user tables: `{subfolder}/{stored_name}`
   * For shared tables with shard: `shard-{n}/{subfolder}/{stored_name}`
   *
   * @returns Relative path
   */
  relativePath(): string {
    const storedName = this.storedName();
    if (this.shard !== undefined) {
      return `shard-${this.shard}/${this.sub}/${storedName}`;
    }
    return `${this.sub}/${storedName}`;
  }

  /**
   * Format file size in human-readable format
   *
   * @returns Formatted size (e.g., "1.5 MB", "256 KB")
   *
   * @example
   * ```typescript
   * const fileRef = FileRef.fromJson(row.document);
   * console.log(fileRef.formatSize()); // "2.3 MB"
   * ```
   */
  formatSize(): string {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = this.size;
    let unitIndex = 0;

    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }

    return `${size.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
  }

  /**
   * Check if this is an image file (based on MIME type)
   *
   * @returns true if image MIME type
   */
  isImage(): boolean {
    return this.mime.startsWith('image/');
  }

  /**
   * Check if this is a video file (based on MIME type)
   *
   * @returns true if video MIME type
   */
  isVideo(): boolean {
    return this.mime.startsWith('video/');
  }

  /**
   * Check if this is an audio file (based on MIME type)
   *
   * @returns true if audio MIME type
   */
  isAudio(): boolean {
    return this.mime.startsWith('audio/');
  }

  /**
   * Check if this is a PDF file
   *
   * @returns true if PDF MIME type
   */
  isPdf(): boolean {
    return this.mime === 'application/pdf';
  }

  /**
   * Get a user-friendly file type description
   *
   * @returns File type description (e.g., "Image", "PDF Document", "Video")
   */
  getTypeDescription(): string {
    if (this.isImage()) return 'Image';
    if (this.isVideo()) return 'Video';
    if (this.isAudio()) return 'Audio';
    if (this.isPdf()) return 'PDF Document';

    // Extract from MIME type
    const parts = this.mime.split('/');
    if (parts.length === 2) {
      return parts[1].toUpperCase() + ' File';
    }

    return 'File';
  }

  /**
   * Serialize to JSON string
   *
   * @returns JSON string representation
   */
  toJson(): string {
    return JSON.stringify({
      id: this.id,
      sub: this.sub,
      name: this.name,
      size: this.size,
      mime: this.mime,
      sha256: this.sha256,
      ...(this.shard !== undefined && { shard: this.shard }),
    });
  }

  /**
   * Convert to plain object
   *
   * @returns Plain object representation
   */
  toObject(): FileRefData {
    return {
      id: this.id,
      sub: this.sub,
      name: this.name,
      size: this.size,
      mime: this.mime,
      sha256: this.sha256,
      ...(this.shard !== undefined && { shard: this.shard }),
    };
  }

  /**
   * Sanitize filename for storage (matches Rust implementation)
   *
   * @private
   */
  private sanitizeFilename(name: string): string {
    const nameWithoutExt = name.includes('.')
      ? name.substring(0, name.lastIndexOf('.'))
      : name;

    const sanitized = nameWithoutExt
      .split('')
      .map((c) => {
        if (/[a-zA-Z0-9]/.test(c)) {
          return c.toLowerCase();
        }
        if (c === ' ' || c === '_' || c === '-') {
          return '-';
        }
        return '';
      })
      .join('')
      .substring(0, 50);

    // Remove leading/trailing dashes and collapse multiple dashes
    return sanitized
      .replace(/^-+/, '')
      .replace(/-+$/, '')
      .replace(/-+/g, '-');
  }

  /**
   * Extract file extension (matches Rust implementation)
   *
   * @private
   */
  private extractExtension(name: string): string {
    if (!name.includes('.')) {
      return 'bin';
    }

    const ext = name.substring(name.lastIndexOf('.') + 1).toLowerCase();

    // Only keep extension if it's ASCII alphanumeric and reasonable length
    if (ext.length <= 10 && /^[a-z0-9]+$/.test(ext)) {
      return ext;
    }

    return 'bin';
  }
}

/**
 * Helper function to parse FileRef from query results
 *
 * @param value - Value from FILE column
 * @returns FileRef instance or null
 *
 * @example
 * ```typescript
 * const results = await client.query('SELECT * FROM users');
 * results.results[0].rows.forEach(row => {
 *   const avatar = parseFileRef(row.avatar);
 *   if (avatar) {
 *     console.log('Avatar URL:', avatar.getDownloadUrl(baseUrl, 'default', 'users'));
 *   }
 * });
 * ```
 */
export function parseFileRef(value: unknown): FileRef | null {
  return FileRef.from(value);
}

/**
 * Helper function to parse array of FileRefs from query results
 *
 * @param values - Array of values from FILE columns
 * @returns Array of FileRef instances (nulls filtered out)
 */
export function parseFileRefs(values: unknown[]): FileRef[] {
  return values
    .map((v) => FileRef.from(v))
    .filter((ref): ref is FileRef => ref !== null);
}

/* ================================================================== */
/*  Context-bound FileRef, Row, and Change wrappers                  */
/* ================================================================== */

/**
 * Context needed to generate file URLs without repeating server/table info.
 */
export interface FileRefContext {
  /** KalamDB server base URL (e.g., 'http://localhost:8080') */
  baseUrl: string;
  /** Namespace of the table (e.g., 'default') */
  namespace: string;
  /** Table name (e.g., 'users') */
  table: string;
}

/**
 * A `FileRef` with its server/table context already bound.
 *
 * Returned by `KalamRow.file()` — you never need to pass `baseUrl`,
 * `namespace`, or `table` again when generating URLs.
 *
 * @example
 * ```typescript
 * const unsub = await client.subscribeRows<User>('default.users', (change) => {
 *   change.rows.forEach(row => {
 *     const avatar = row.file('avatar');
 *     if (avatar) {
 *       img.src = avatar.downloadUrl();        // full URL
 *       console.log(avatar.relativeUrl());     // /api/v1/files/default/users/f0001/12345
 *       console.log(avatar.name, avatar.formatSize()); // all FileRef attrs still work
 *     }
 *   });
 * });
 * ```
 */
export class BoundFileRef extends FileRef {
  private readonly _ctx: FileRefContext;

  constructor(data: FileRefData, ctx: FileRefContext) {
    super(data);
    this._ctx = ctx;
  }

  /**
   * Full download URL — no args needed, context is already bound.
   *
   * @example
   * ```typescript
   * const url = row.file('avatar')?.downloadUrl();
   * // → 'http://localhost:8080/api/v1/files/default/users/f0001/12345'
   * ```
   */
  downloadUrl(): string {
    return this.getDownloadUrl(this._ctx.baseUrl, this._ctx.namespace, this._ctx.table);
  }

  /**
   * Relative HTTP path — no args needed.
   *
   * Useful when the base URL is already known from your app config (e.g.,
   * to construct `<img src={row.file('avatar')?.relativeUrl()} />`).
   *
   * @example
   * ```typescript
   * const path = row.file('avatar')?.relativeUrl();
   * // → '/api/v1/files/default/users/f0001/12345'
   * ```
   */
  relativeUrl(): string {
    return `/api/v1/files/${this._ctx.namespace}/${this._ctx.table}/${this.sub}/${this.id}`;
  }
}

/**
 * A row with ergonomic FILE column access (SDK-level generic wrapper).
 *
 * This is inherently SDK-specific because `<T>` is a TypeScript generic that
 * cannot cross the Rust/WASM FFI boundary. Each SDK implements its own thin
 * wrapper; the heavy lifting (FileRef parsing, URL generation) delegates to
 * the shared `FileRef` class backed by `kalam-link` Rust.
 *
 * - Access raw column values via `.data.columnName`
 * - Access any FILE column as a `BoundFileRef` via `.file('columnName')`
 *
 * @example
 * ```typescript
 * const rows = await client.queryRows<User>('SELECT * FROM default.users', 'default.users');
 * rows.forEach(row => {
 *   console.log(row.data.name);
 *   const avatar = row.file('avatar');
 *   if (avatar) img.src = avatar.downloadUrl();
 * });
 * ```
 */
export class KalamRow<T extends Record<string, unknown> = Record<string, unknown>> {
  /** Raw row data — all column values as returned from the server. */
  readonly data: T;
  private readonly _ctx: FileRefContext;
  private _typedData?: RowData;

  constructor(data: T, ctx: FileRefContext) {
    this.data = data;
    this._ctx = ctx;
  }

  /**
   * Access a column value as a `KalamCellValue` for type-safe extraction.
   *
   * Works identically whether the row came from a query or a subscription,
   * providing the unified `row.cell('colname').asString()` access pattern.
   *
   * @param column - Column name (type-checked against row type `T`)
   *
   * @example
   * ```typescript
   * // Query path:
   * const rows = await client.queryRows<User>('SELECT * FROM users', 'users');
   * rows.forEach(row => {
   *   console.log(row.cell('name').asString());
   *   console.log(row.cell('age').asInt());
   * });
   *
   * // Subscribe path — same API:
   * await client.subscribeRows<User>('users', (change) => {
   *   change.rows.forEach(row => {
   *     console.log(row.cell('name').asString());
   *   });
   * });
   * ```
   */
  cell(column: keyof T): KalamCellValue {
    const value = this.data[column];
    return value instanceof KalamCellValue ? value : KalamCellValue.from(value);
  }

  /**
   * All column values wrapped as `KalamCellValue` — cached on first access.
   *
   * Returns `RowData` (`Record<string, KalamCellValue>`) so you can use the
   * familiar `typedData['colname'].asString()` pattern.
   *
   * @example
   * ```typescript
   * const row = rows[0];
   * const td = row.typedData;
   * console.log(td['name'].asString(), td['score'].asFloat());
   * ```
   */
  get typedData(): RowData {
    if (!this._typedData) {
      const raw = this.data as Record<string, unknown>;
      const entries = Object.entries(raw);
      const alreadyWrapped = entries.every(([, value]) => value instanceof KalamCellValue);
      this._typedData = alreadyWrapped
        ? raw as unknown as RowData
        : wrapRowMap(raw);
    }
    return this._typedData!;
  }

  /**
   * Parse a FILE column and return a context-bound `BoundFileRef`.
   *
   * Returns `null` if the column is missing, null, or not a valid file reference.
   *
   * @param column - Column name (type-checked against row type `T`)
   *
   * @example
   * ```typescript
   * const avatar = row.file('avatar');
   * if (avatar) {
   *   img.src = avatar.downloadUrl();
   *   console.log(avatar.name, avatar.mime, avatar.formatSize());
   * }
   * ```
   */
  file(column: keyof T): BoundFileRef | null {
    const value = this.data[column];
    // Unwrap KalamCellValue instances to get the raw JSON for FileRef parsing
    const rawValue =
      value instanceof KalamCellValue ? value.toJson() : value;
    const ref = FileRef.from(rawValue);
    if (!ref) return null;
    return new BoundFileRef(ref.toObject(), this._ctx);
  }
}

/**
 * A live subscription change event with rows wrapped as `KalamRow<T>`.
 *
 * SDK-level generic wrapper — maps the raw `ServerMessage` (from the Rust WASM
 * `kalam-link` crate) into typed rows with `.file()` support.
 *
 * The raw `ServerMessage.rows` are `HashMap<String, JsonValue>[]` in Rust,
 * which arrive as plain JS objects. This wrapper casts them to `T` and wraps
 * each in a `KalamRow<T>` for file access.
 *
 * @example
 * ```typescript
 * const unsub = await client.subscribeRows<User>('default.users', (change) => {
 *   if (change.type === 'insert' || change.type === 'update') {
 *     change.rows.forEach(row => {
 *       const avatar = row.file('avatar');
 *       if (avatar) console.log('Avatar:', avatar.downloadUrl());
 *     });
 *   }
 * });
 * ```
 */
export class KalamChange<T extends Record<string, unknown> = Record<string, unknown>> {
  /** Change type: `'insert'`, `'update'`, `'delete'`, `'initial_data_batch'`, etc. */
  readonly type: string;

  /** New/current rows (present on insert, update, initial_data_batch). */
  readonly rows: KalamRow<T>[];

  /**
   * Previous row values before the change (present on update and delete).
   * Empty array for inserts.
   */
  readonly oldValues: KalamRow<T>[];

  /** Raw underlying `ServerMessage` from the WASM layer (Rust `kalam-link`). */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly raw: any;

  /**
   * Construct from a raw `ServerMessage` event and table context.
   *
   * Rows come from Rust as `HashMap<String, JsonValue>[]` — plain JS objects.
   * This constructor casts them to `T` and wraps with `KalamRow` for file access.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(event: any, ctx: FileRefContext) {
    this.raw = event;
    this.type = (event as { type?: string }).type ?? 'unknown';
    const rowsArr: T[] = Array.isArray(event.rows) ? (event.rows as T[]) : [];
    const oldArr: T[] = Array.isArray(event.old_values) ? (event.old_values as T[]) : [];
    this.rows = rowsArr.map(r => new KalamRow<T>(r, ctx));
    this.oldValues = oldArr.map(r => new KalamRow<T>(r, ctx));
  }
}

/**
 * Wrap an array of raw row objects as `KalamRow<T>` with file access.
 *
 * @param rows - Raw row objects (from `queryAll()` or subscribe events)
 * @param ctx - Server/table context for file URL generation
 *
 * @example
 * ```typescript
 * const ctx = { baseUrl: 'http://localhost:8080', namespace: 'default', table: 'users' };
 * const rows = wrapRows<User>(queryResults, ctx);
 * const avatar = rows[0].file('avatar');
 * ```
 */
export function wrapRows<T extends Record<string, unknown> = Record<string, unknown>>(
  rows: T[],
  ctx: FileRefContext,
): KalamRow<T>[] {
  return rows.map(r => new KalamRow<T>(r, ctx));
}
