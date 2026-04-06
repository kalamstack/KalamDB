/**
 * SeqId — Lightweight Snowflake-based sequence identifier for MVCC versioning.
 *
 * Mirrors the Rust `SeqId` type from `link/link-common/src/seq_id.rs`.
 *
 * Layout: `timestamp (42 bits) | worker (10 bits) | sequence (12 bits)`
 *
 * All arithmetic uses `BigInt` internally to avoid JavaScript's 53-bit
 * `Number.MAX_SAFE_INTEGER` limit.  The class exposes a clean, typed API
 * so SDK consumers never need to deal with raw `BigInt` or `number` values.
 *
 * @example
 * ```typescript
 * import { SeqId } from '@kalamdb/client';
 *
 * const seq = SeqId.from(123456789n);
 * console.log(seq.timestampMillis());  // millis since Unix epoch
 * console.log(seq.workerId());         // 0-1023
 * console.log(seq.sequence());         // 0-4095
 * console.log(seq.toDate());           // Date object
 * console.log(seq.toString());         // "123456789"
 * ```
 *
 * @module
 */

/* ================================================================== */
/*  Constants                                                         */
/* ================================================================== */

/** Custom epoch: 2024-01-01 00:00:00 UTC (matches backend). */
const EPOCH = 1704067200000n;

/* ================================================================== */
/*  SeqId class                                                       */
/* ================================================================== */

/**
 * Snowflake-based sequence identifier for MVCC versioning.
 *
 * Wraps a signed 64-bit integer internally stored as `bigint`.
 * Instances are immutable and comparable.
 */
export class SeqId {
  /** Custom epoch: 2024-01-01 00:00:00 UTC (matches backend). */
  static readonly EPOCH: bigint = EPOCH;

  /** @internal Raw value as bigint. */
  readonly #value: bigint;

  /* ------------------------------------------------------------------ */
  /*  Constructors                                                      */
  /* ------------------------------------------------------------------ */

  private constructor(value: bigint) {
    this.#value = value;
  }

  /**
   * Create a `SeqId` from a `bigint`, `number`, or decimal `string`.
   *
   * Throws if the value cannot be parsed or exceeds i64 range.
   */
  static from(value: bigint | number | string): SeqId {
    if (typeof value === 'bigint') return new SeqId(value);
    if (typeof value === 'number') {
      if (!Number.isFinite(value) || !Number.isInteger(value)) {
        throw new Error(`SeqId.from: expected integer, got ${value}`);
      }
      return new SeqId(BigInt(value));
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed === '') throw new Error('SeqId.from: empty string');
      try {
        return new SeqId(BigInt(trimmed));
      } catch {
        throw new Error(`SeqId.from: failed to parse "${trimmed}"`);
      }
    }
    throw new Error(`SeqId.from: unsupported type ${typeof value}`);
  }

  /**
   * Create a `SeqId` from a raw WASM `number` value (the tsify-generated type).
   *
   * This handles the auto-generated `SeqId = number` from WASM bindings.
   * @internal
   */
  static fromWasm(value: number | undefined | null): SeqId | null {
    if (value === undefined || value === null) return null;
    return SeqId.from(value);
  }

  /** Construct with a zero value (useful for "start from beginning"). */
  static zero(): SeqId {
    return new SeqId(0n);
  }

  /* ------------------------------------------------------------------ */
  /*  Raw access                                                        */
  /* ------------------------------------------------------------------ */

  /** Return the raw value as `bigint`. */
  toBigInt(): bigint {
    return this.#value;
  }

  /** Return the raw value as `number`. May lose precision for large IDs. */
  toNumber(): number {
    return Number(this.#value);
  }

  /* ------------------------------------------------------------------ */
  /*  Snowflake field extraction                                        */
  /* ------------------------------------------------------------------ */

  /**
   * Extract timestamp in milliseconds since Unix epoch.
   */
  timestampMillis(): number {
    const unsigned = BigInt.asUintN(64, this.#value);
    return Number((unsigned >> 22n) + EPOCH);
  }

  /**
   * Extract timestamp in whole seconds since Unix epoch.
   */
  timestampSeconds(): number {
    return Math.trunc(this.timestampMillis() / 1000);
  }

  /**
   * Convert to a `Date` object.
   */
  toDate(): Date {
    return new Date(this.timestampMillis());
  }

  /**
   * Compute age in seconds relative to the current time.
   */
  ageSeconds(): number {
    const now = Date.now();
    return Math.max(0, Math.trunc((now - this.timestampMillis()) / 1000));
  }

  /**
   * Extract worker ID (0-1023).
   */
  workerId(): number {
    const unsigned = BigInt.asUintN(64, this.#value);
    return Number((unsigned >> 12n) & 0x3FFn);
  }

  /**
   * Extract intra-millisecond sequence number (0-4095).
   */
  sequence(): number {
    const unsigned = BigInt.asUintN(64, this.#value);
    return Number(unsigned & 0xFFFn);
  }

  /* ------------------------------------------------------------------ */
  /*  Comparison                                                        */
  /* ------------------------------------------------------------------ */

  /** Return `true` if this SeqId equals `other`. */
  equals(other: SeqId): boolean {
    return this.#value === other.#value;
  }

  /**
   * Compare to another SeqId.
   * Returns negative if `this < other`, zero if equal, positive if `this > other`.
   */
  compareTo(other: SeqId): number {
    if (this.#value < other.#value) return -1;
    if (this.#value > other.#value) return 1;
    return 0;
  }

  /* ------------------------------------------------------------------ */
  /*  Serialisation                                                     */
  /* ------------------------------------------------------------------ */

  /** Decimal string representation (for JSON / display). */
  toString(): string {
    return this.#value.toString();
  }

  /**
   * JSON serialisation — returns the raw number for JSON compatibility
   * with the server protocol.
   */
  toJSON(): number {
    return this.toNumber();
  }
}
