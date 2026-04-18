/**
 * Formatting Utilities for KalamDB Admin UI
 * 
 * Provides functions to format values for display, including:
 * - Timestamps (microseconds, milliseconds, etc.)
 * - Numbers
 * - Data types
 */

import {
  DEFAULT_TIMESTAMP_FORMAT,
  DEFAULT_TIMEZONE,
  SHOW_MILLISECONDS,
  MAX_DECIMAL_PLACES,
  extractTimestampUnit,
  type TimestampFormat,
  type TimezoneOption,
} from './config';

// =============================================================================
// TIMESTAMP FORMATTING
// =============================================================================

/**
 * Convert a raw timestamp value to milliseconds.
 * Handles microseconds, nanoseconds, and other units.
 */
export function toMilliseconds(
  value: number | string,
  unit: 'microsecond' | 'millisecond' | 'nanosecond' | 'second' | null = null
): number {
  const normalizedValue = typeof value === 'string' ? value.trim() : value;
  const isNumericString =
    typeof normalizedValue === 'string' && /^-?\d+(\.\d+)?$/.test(normalizedValue);
  const num =
    typeof normalizedValue === 'number'
      ? normalizedValue
      : isNumericString
        ? Number(normalizedValue)
        : NaN;

  if (isNaN(num)) {
    if (typeof normalizedValue === 'string') {
      const parsedDate = Date.parse(normalizedValue);
      if (!Number.isNaN(parsedDate)) {
        return parsedDate;
      }
    }
    return NaN;
  }
  
  switch (unit) {
    case 'microsecond':
      return Math.floor(num / 1000);
    case 'nanosecond':
      return Math.floor(num / 1000000);
    case 'second':
      return num * 1000;
    case 'millisecond':
    default:
      // If no unit specified, try to auto-detect based on magnitude
      // Microseconds since epoch are typically > 1e15 (year 2001+)
      // Milliseconds are typically > 1e12 (year 2001+)
      // Seconds are typically > 1e9 (year 2001+)
      if (num > 1e15) {
        // Likely microseconds
        return Math.floor(num / 1000);
      } else if (num > 1e12) {
        // Likely milliseconds
        return num;
      } else if (num > 1e9) {
        // Likely seconds
        return num * 1000;
      }
      return num;
  }
}

/**
 * Format a timestamp value for display.
 * 
 * @param value - The raw timestamp value (number or string)
 * @param dataType - The Arrow data type string (e.g., "Timestamp(Microsecond, None)")
 * @param format - The desired output format (defaults to config value)
 * @param timezone - The timezone to use (defaults to config value)
 */
export function formatTimestamp(
  value: number | string | null | undefined,
  dataType?: string,
  format: TimestampFormat = DEFAULT_TIMESTAMP_FORMAT,
  timezone: TimezoneOption = DEFAULT_TIMEZONE
): string {
  if (value === null || value === undefined) {
    return '-';
  }
  
  try {
    // Extract unit from data type
    const unit = dataType ? extractTimestampUnit(dataType) : null;
    
    // Convert to milliseconds
    const ms = toMilliseconds(value, unit);
    
    if (isNaN(ms)) {
      return String(value);
    }
    
    const date = new Date(ms);
    
    if (isNaN(date.getTime())) {
      return String(value);
    }
    
    return formatDate(date, format, timezone);
  } catch {
    return String(value);
  }
}

/**
 * Format a Date object according to the specified format.
 */
export function formatDate(
  date: Date,
  format: TimestampFormat = DEFAULT_TIMESTAMP_FORMAT,
  timezone: TimezoneOption = DEFAULT_TIMEZONE
): string {
  switch (format) {
    case 'iso8601':
      return timezone === 'utc'
        ? date.toISOString()
        : formatLocalISO8601(date, SHOW_MILLISECONDS);
    
    case 'iso8601-date':
      return timezone === 'utc'
        ? date.toISOString().split('T')[0]
        : formatLocalDate(date);
    
    case 'iso8601-datetime':
      return timezone === 'utc'
        ? date.toISOString().replace(/\.\d{3}Z$/, 'Z')
        : formatLocalISO8601(date, false);
    
    case 'locale':
      return date.toLocaleString(undefined, {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        ...(timezone === 'utc' ? { timeZone: 'UTC' } : {}),
      });
    
    case 'locale-short':
      return date.toLocaleString(undefined, {
        year: '2-digit',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        ...(timezone === 'utc' ? { timeZone: 'UTC' } : {}),
      });
    
    case 'relative':
      return formatRelativeTime(date);
    
    case 'unix-ms':
      return String(date.getTime());
    
    case 'unix-sec':
      return String(Math.floor(date.getTime() / 1000));
    
    default:
      return date.toLocaleString();
  }
}

/**
 * Format a date in local ISO 8601 format.
 */
function formatLocalISO8601(date: Date, includeMs: boolean = true): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  
  let result = `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
  
  if (includeMs) {
    const ms = String(date.getMilliseconds()).padStart(3, '0');
    result += `.${ms}`;
  }
  
  // Add timezone offset
  const offset = -date.getTimezoneOffset();
  if (offset === 0) {
    result += 'Z';
  } else {
    const sign = offset > 0 ? '+' : '-';
    const absOffset = Math.abs(offset);
    const offsetHours = String(Math.floor(absOffset / 60)).padStart(2, '0');
    const offsetMinutes = String(absOffset % 60).padStart(2, '0');
    result += `${sign}${offsetHours}:${offsetMinutes}`;
  }
  
  return result;
}

/**
 * Format a date as local YYYY-MM-DD.
 */
function formatLocalDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Format a date as relative time (e.g., "2 hours ago").
 */
export function formatRelativeTime(date: Date): string {
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSeconds = Math.floor(Math.abs(diffMs) / 1000);
  
  const isFuture = diffMs < 0;
  const suffix = isFuture ? 'from now' : 'ago';
  
  if (diffSeconds < 60) {
    return diffSeconds === 1 ? `1 second ${suffix}` : `${diffSeconds} seconds ${suffix}`;
  }
  
  const diffMinutes = Math.floor(diffSeconds / 60);
  if (diffMinutes < 60) {
    return diffMinutes === 1 ? `1 minute ${suffix}` : `${diffMinutes} minutes ${suffix}`;
  }
  
  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return diffHours === 1 ? `1 hour ${suffix}` : `${diffHours} hours ${suffix}`;
  }
  
  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 30) {
    return diffDays === 1 ? `1 day ${suffix}` : `${diffDays} days ${suffix}`;
  }
  
  const diffMonths = Math.floor(diffDays / 30);
  if (diffMonths < 12) {
    return diffMonths === 1 ? `1 month ${suffix}` : `${diffMonths} months ${suffix}`;
  }
  
  const diffYears = Math.floor(diffMonths / 12);
  return diffYears === 1 ? `1 year ${suffix}` : `${diffYears} years ${suffix}`;
}

// =============================================================================
// NUMBER FORMATTING
// =============================================================================

/**
 * Format a number for display, limiting decimal places.
 */
export function formatNumber(value: number, maxDecimals: number = MAX_DECIMAL_PLACES): string {
  if (Number.isInteger(value)) {
    return value.toLocaleString();
  }
  
  // Round to max decimal places
  const factor = Math.pow(10, maxDecimals);
  const rounded = Math.round(value * factor) / factor;
  
  // Remove trailing zeros
  return rounded.toLocaleString(undefined, {
    maximumFractionDigits: maxDecimals,
  });
}

// =============================================================================
// GENERIC VALUE FORMATTING
// =============================================================================

/**
 * Format a cell value based on its data type.
 * This is the main entry point for formatting table cell values.
 */
export function formatCellValue(
  value: unknown,
  dataType?: string
): { formatted: string; isTimestamp: boolean; isNull: boolean } {
  // Handle null/undefined
  if (value === null || value === undefined) {
    return { formatted: '-', isTimestamp: false, isNull: true };
  }
  
  // Check if this is a timestamp type
  if (dataType && /^Timestamp\(|^Date32$|^Date64$/i.test(dataType)) {
    const formatted = formatTimestamp(value as number | string, dataType);
    return { formatted, isTimestamp: true, isNull: false };
  }
  
  // Boolean
  if (typeof value === 'boolean') {
    return { formatted: String(value), isTimestamp: false, isNull: false };
  }
  
  // Number
  if (typeof value === 'number') {
    return { formatted: formatNumber(value), isTimestamp: false, isNull: false };
  }
  
  // Object (JSON)
  if (typeof value === 'object') {
    return { formatted: JSON.stringify(value), isTimestamp: false, isNull: false };
  }
  
  // Default: string
  return { formatted: String(value), isTimestamp: false, isNull: false };
}

export function formatUtcTimestamp(value: number | string | null | undefined): string {
  return formatTimestamp(value, undefined, "iso8601-datetime", "utc");
}
