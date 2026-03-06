/**
 * KalamCellValue — Type-safe wrapper for individual cell values in KalamDB rows.
 *
 * ## Overview
 *
 * `KalamCellValue` wraps the raw JSON value returned by the server for a single
 * cell and provides typed accessor methods that mirror the Rust `KalamCellValue`
 * newtype (`kalam-link/src/models/cell_value.rs`).
 *
 * Instead of receiving `Record<string, unknown>` rows, consumers now receive
 * `RowData = Record<string, KalamCellValue>` rows and call safe accessors:
 *
 * ```typescript
 * const name   = row.name.asString();    // string | null
 * const age    = row.age.asInt();        // number | null
 * const active = row.active.asBool();   // boolean | null
 * const avatar = row.avatar.asFile();   // FileRef | null
 * const url    = row.avatar.asFileUrl('http://localhost:8080', 'default', 'users');
 * ```
 *
 * ## FILE columns
 *
 * `asFile()` parses the FILE column JSON and returns a `FileRef` instance, or
 * `null` if the cell is null or the value is not a valid file reference.
 * `asFileUrl()` is a convenience shorthand that builds the download URL directly.
 *
 * ## Utilities
 *
 * - `KalamCellValue.from(raw)` — wraps any raw JS value
 * - `wrapRowMap(raw)` — converts `Record<string, unknown>` → `RowData`
 *
 * @example
 * ```typescript
 * import { KalamCellValue, RowData } from 'kalam-link';
 *
 * // rows are already RowData[] from queryAll()
 * for (const row of rows) {
 *   console.log(row.id.asString(), row.score.asFloat());
 * }
 * ```
 *
 * @module
 */

import { FileRef } from './file_ref.js';
import { SeqId } from './seq_id.js';
import type { JsonValue } from './types.js';

/* ================================================================== */
/*  KalamCellValue class                                              */
/* ================================================================== */

/**
 * A type-safe wrapper for a single cell value in a KalamDB query result row.
 *
 * Mirrors `KalamCellValue` in Rust (`kalam-link/src/models/cell_value.rs`).
 */
export class KalamCellValue {
  /** @internal Raw JSON value as returned by the server */
  readonly #raw: JsonValue;

  /** @internal */
  private constructor(raw: JsonValue) {
    this.#raw = raw;
  }

  /* ------------------------------------------------------------------ */
  /*  Factory                                                            */
  /* ------------------------------------------------------------------ */

  /**
   * Wrap a raw JS value (from JSON deserialization) as a `KalamCellValue`.
   *
   * Pass `null` / `undefined` for SQL NULL cells.
   */
  static from(raw: unknown): KalamCellValue {
    // Normalise undefined → null (treat as SQL NULL)
    return new KalamCellValue(raw === undefined ? null : (raw as JsonValue));
  }

  /* ------------------------------------------------------------------ */
  /*  Raw access (mirrors Rust `.inner()`)                              */
  /* ------------------------------------------------------------------ */

  /**
   * Return the underlying raw JSON value.
   *
   * Use this when you need to pass the value to code that expects plain JSON
   * (e.g., existing WASM helpers, `FileRef.from(cell.toJson())`).
   */
  toJson(): JsonValue {
    return this.#raw;
  }

  /**
   * Return the underlying raw JSON value.
   *
   * Alias of `toJson()` for codebases that prefer `asX()` naming.
   */
  asJson(): JsonValue {
    return this.#raw;
  }

  /* ------------------------------------------------------------------ */
  /*  Type guards                                                       */
  /* ------------------------------------------------------------------ */

  /** Returns `true` if this cell is SQL NULL. */
  isNull(): boolean {
    return this.#raw === null || this.#raw === undefined;
  }

  /** Returns `true` if the underlying value is a JSON string. */
  isString(): boolean {
    return typeof this.#raw === 'string';
  }

  /** Returns `true` if the underlying value is a JSON number. */
  isNumber(): boolean {
    return typeof this.#raw === 'number';
  }

  /** Returns `true` if the underlying value is a JSON boolean. */
  isBool(): boolean {
    return typeof this.#raw === 'boolean';
  }

  /** Returns `true` if the underlying value is a JSON object (not array, not null). */
  isObject(): boolean {
    return (
      this.#raw !== null &&
      typeof this.#raw === 'object' &&
      !Array.isArray(this.#raw)
    );
  }

  /** Returns `true` if the underlying value is a JSON array. */
  isArray(): boolean {
    return Array.isArray(this.#raw);
  }

  /* ------------------------------------------------------------------ */
  /*  Typed accessors                                                   */
  /* ------------------------------------------------------------------ */

  /**
   * Return the value as a string, or `null` for SQL NULL.
   *
   * Handles the `{ "Utf8": "..." }` envelope that some Rust drivers emit and
   * converts numbers / booleans to their string representations.
   */
  asString(): string | null {
    if (this.isNull()) return null;

    // Envelope: { "Utf8": "..." }
    if (this.isObject()) {
      const obj = this.#raw as Record<string, JsonValue>;
      if ('Utf8' in obj && typeof obj['Utf8'] === 'string') return obj['Utf8'];
      if ('String' in obj && typeof obj['String'] === 'string') return obj['String'];
    }

    if (typeof this.#raw === 'string') return this.#raw;
    if (typeof this.#raw === 'number') return String(this.#raw);
    if (typeof this.#raw === 'boolean') return this.#raw ? 'true' : 'false';

    return JSON.stringify(this.#raw);
  }

  /**
   * Return the value as an integer (`number`), or `null` for SQL NULL / non-numeric.
   *
   * Truncates floating-point values (same as `as i64` in Rust).
   * String-encoded integers (e.g. `"42"`) are parsed.
   */
  asInt(): number | null {
    if (this.isNull()) return null;
    if (typeof this.#raw === 'number') return Math.trunc(this.#raw);
    if (typeof this.#raw === 'string') {
      const n = Number(this.#raw);
      return Number.isFinite(n) ? Math.trunc(n) : null;
    }
    if (typeof this.#raw === 'boolean') return this.#raw ? 1 : 0;
    return null;
  }

  /**
   * Return the value as a native `bigint`, or `null` for SQL NULL / non-numeric.
   *
   * Useful for `Int64` / `UInt64` columns where the value may exceed
   * `Number.MAX_SAFE_INTEGER`.  String-encoded integers are parsed.
   */
  asBigInt(): bigint | null {
    if (this.isNull()) return null;
    try {
      if (typeof this.#raw === 'bigint') return this.#raw;
      if (typeof this.#raw === 'number') return BigInt(Math.trunc(this.#raw));
      if (typeof this.#raw === 'string') return BigInt(this.#raw.trim());
    } catch {
      // non-integer string
    }
    return null;
  }

  /**
   * Return the value as a `SeqId`, or `null` for SQL NULL / non-numeric.
   *
   * Use this for `_seq` columns or any Snowflake-based sequence ID.
   *
   * @example
   * ```typescript
   * const seq = row._seq.asSeqId();
   * if (seq) {
   *   console.log(seq.timestampMillis()); // when the row was written
   *   console.log(seq.workerId());        // which worker generated it
   * }
   * ```
   */
  asSeqId(): SeqId | null {
    if (this.isNull()) return null;
    try {
      if (typeof this.#raw === 'number') return SeqId.from(this.#raw);
      if (typeof this.#raw === 'bigint') return SeqId.from(this.#raw);
      if (typeof this.#raw === 'string') return SeqId.from(this.#raw.trim());
    } catch {
      // not parseable as SeqId
    }
    return null;
  }

  /**
   * Return the value as a floating-point number, or `null` for SQL NULL / non-numeric.
   *
   * String-encoded floats (e.g. `"3.14"`) are parsed.
   */
  asFloat(): number | null {
    if (this.isNull()) return null;
    if (typeof this.#raw === 'number') return this.#raw;
    if (typeof this.#raw === 'string') {
      const n = parseFloat(this.#raw);
      return Number.isFinite(n) ? n : null;
    }
    if (typeof this.#raw === 'boolean') return this.#raw ? 1.0 : 0.0;
    return null;
  }

  /**
   * Return the value as a boolean, or `null` for SQL NULL / non-boolean.
   *
   * Handles string-encoded booleans (`"true"` / `"false"` / `"1"` / `"0"`).
   */
  asBool(): boolean | null {
    if (this.isNull()) return null;
    if (typeof this.#raw === 'boolean') return this.#raw;
    if (typeof this.#raw === 'number') return this.#raw !== 0;
    if (typeof this.#raw === 'string') {
      const lc = this.#raw.toLowerCase().trim();
      if (lc === 'true' || lc === '1') return true;
      if (lc === 'false' || lc === '0') return false;
    }
    return null;
  }

  /**
   * Return the value as a `Date`, or `null` for SQL NULL / unparseable values.
   *
   * Handles:
   * - Unix milliseconds (number)
   * - ISO 8601 strings (`"2024-01-01T00:00:00Z"`)
   * - Numeric timestamp strings (`"1704067200000"`)
   */
  asDate(): Date | null {
    if (this.isNull()) return null;
    if (typeof this.#raw === 'number') {
      const d = new Date(this.#raw);
      return isNaN(d.getTime()) ? null : d;
    }
    if (typeof this.#raw === 'string') {
      // Try numeric timestamp first
      const n = Number(this.#raw);
      if (Number.isFinite(n)) {
        const d = new Date(n);
        return isNaN(d.getTime()) ? null : d;
      }
      const d = new Date(this.#raw);
      return isNaN(d.getTime()) ? null : d;
    }
    return null;
  }

  /**
   * Return the raw value as a plain JSON object, or `null`.
   */
  asObject(): Record<string, JsonValue> | null {
    if (this.isObject()) return this.#raw as Record<string, JsonValue>;
    return null;
  }

  /**
   * Return the raw value as a JSON array, or `null`.
   */
  asArray(): JsonValue[] | null {
    if (this.isArray()) return this.#raw as JsonValue[];
    return null;
  }

  /* ------------------------------------------------------------------ */
  /*  FILE column support                                               */
  /* ------------------------------------------------------------------ */

  /**
   * Parse a FILE column value and return a `FileRef` instance, or `null`.
   *
   * The FILE column stores a serialised JSON object matching `FileRefData`.
   * This accessor deserialises it into a class instance with helper methods.
   *
   * @example
   * ```typescript
   * const fileRef = row.avatar.asFile();
   * if (fileRef) {
   *   const url = fileRef.getDownloadUrl('http://localhost:8080', 'default', 'users');
   *   console.log(fileRef.name, fileRef.formatSize(), fileRef.isImage());
   * }
   * ```
   */
  asFile(): FileRef | null {
    if (this.isNull()) return null;
    return FileRef.from(this.#raw);
  }

  /**
   * Convenience: parse a FILE column and return the download URL directly.
   *
   * Returns `null` if the cell is null, not a valid file reference, or the
   * file ref does not produce a URL.
   *
   * @param baseUrl   - Server base URL (e.g. `"http://localhost:8080"`)
   * @param namespace - Namespace of the table (e.g. `"default"`)
   * @param table     - Table name (e.g. `"users"`)
   *
   * @example
   * ```typescript
   * const url = row.avatar.asFileUrl('http://localhost:8080', 'default', 'users');
   * img.src = url ?? '/placeholder.png';
   * ```
   */
  asFileUrl(baseUrl: string, namespace: string, table: string): string | null {
    const ref = this.asFile();
    if (!ref) return null;
    return ref.getDownloadUrl(baseUrl, namespace, table);
  }

  /* ------------------------------------------------------------------ */
  /*  Serialisation / display                                           */
  /* ------------------------------------------------------------------ */

  /**
   * Human-readable string for display purposes.
   *
   * - SQL NULL → `"NULL"`
   * - strings   → value as-is
   * - objects / arrays → compact JSON
   * - everything else → `String(value)`
   */
  toString(): string {
    if (this.isNull()) return 'NULL';
    if (typeof this.#raw === 'string') return this.#raw;
    if (typeof this.#raw === 'object') return JSON.stringify(this.#raw);
    return String(this.#raw);
  }
}

/* ================================================================== */
/*  RowData type                                                      */
/* ================================================================== */

/**
 * A single query result row with all values wrapped as `KalamCellValue`.
 *
 * Replaces `Record<string, unknown>` for strongly-typed row access.
 */
export type RowData = Record<string, KalamCellValue>;

/* ================================================================== */
/*  Utility helpers                                                   */
/* ================================================================== */

/**
 * Wrap a single `Record<string, unknown>` object row as `RowData`,
 * converting each value to a `KalamCellValue`.
 *
 * @example
 * ```typescript
 * const typedRow = wrapRowMap(rawRow);
 * console.log(typedRow.name.asString());
 * ```
 */
export function wrapRowMap(raw: Record<string, unknown>): RowData {
  const result: RowData = {};
  for (const key of Object.keys(raw)) {
    result[key] = KalamCellValue.from(raw[key]);
  }
  return result;
}

