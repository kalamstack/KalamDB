//! Timestamp formatting utilities for KalamDB.
//!
//! Provides configurable formatting of millisecond timestamps (i64) from KalamDB
//! to various human-readable formats. This module is exposed via WASM bindings
//! for use in all language SDKs (TypeScript, Python, etc.).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;
const MILLIS_PER_DAY: i64 = 24 * MILLIS_PER_HOUR;

const WEEKDAY_NAMES: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTH_NAMES: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UtcDateTimeParts {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    millisecond: u16,
    weekday: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    message: &'static str,
}

impl ParseError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for ParseError {}

fn split_timestamp(ms: i64) -> Option<UtcDateTimeParts> {
    let days = ms.div_euclid(MILLIS_PER_DAY);
    let millis_of_day = ms.rem_euclid(MILLIS_PER_DAY);
    let (year, month, day) = civil_from_days(days)?;

    let hour = (millis_of_day / MILLIS_PER_HOUR) as u8;
    let minute = ((millis_of_day % MILLIS_PER_HOUR) / MILLIS_PER_MINUTE) as u8;
    let second = ((millis_of_day % MILLIS_PER_MINUTE) / MILLIS_PER_SECOND) as u8;
    let millisecond = (millis_of_day % MILLIS_PER_SECOND) as u16;
    let weekday = (days + 4).rem_euclid(7) as u8;

    Some(UtcDateTimeParts {
        year,
        month,
        day,
        hour,
        minute,
        second,
        millisecond,
        weekday,
    })
}

fn civil_from_days(days: i64) -> Option<(i32, u8, u8)> {
    let z = i128::from(days) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };

    Some((i32::try_from(year).ok()?, u8::try_from(m).ok()?, u8::try_from(d).ok()?))
}

fn days_from_civil(year: i32, month: u8, day: u8) -> i64 {
    let year = i64::from(year) - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u8) -> Option<u8> {
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return None,
    })
}

fn parse_digits(bytes: &[u8], start: usize, len: usize) -> Result<u32, ParseError> {
    let end = start.checked_add(len).ok_or_else(|| ParseError::new("Invalid timestamp"))?;
    let slice = bytes.get(start..end).ok_or_else(|| ParseError::new("Invalid timestamp"))?;
    if !slice.iter().all(|byte| byte.is_ascii_digit()) {
        return Err(ParseError::new("Invalid timestamp digits"));
    }

    let mut value = 0u32;
    for byte in slice {
        value = value * 10 + u32::from(byte - b'0');
    }
    Ok(value)
}

fn parse_fractional_millis(bytes: &[u8], start: usize, end: usize) -> Result<u16, ParseError> {
    let digits = bytes.get(start..end).ok_or_else(|| ParseError::new("Invalid timestamp"))?;
    if digits.is_empty() || !digits.iter().all(|byte| byte.is_ascii_digit()) {
        return Err(ParseError::new("Invalid fractional seconds"));
    }

    let mut millis = 0u16;
    for index in 0..3 {
        millis *= 10;
        if let Some(byte) = digits.get(index) {
            millis += u16::from(byte - b'0');
        }
    }
    Ok(millis)
}

fn parse_offset_millis(bytes: &[u8], start: usize) -> Result<i64, ParseError> {
    match bytes.get(start) {
        Some(b'Z') => {
            if start + 1 != bytes.len() {
                return Err(ParseError::new("Invalid trailing timestamp data"));
            }
            Ok(0)
        },
        Some(sign @ (b'+' | b'-')) => {
            if start + 6 != bytes.len() || bytes.get(start + 3) != Some(&b':') {
                return Err(ParseError::new("Invalid timestamp offset"));
            }

            let offset_hours = parse_digits(bytes, start + 1, 2)?;
            let offset_minutes = parse_digits(bytes, start + 4, 2)?;
            if offset_hours > 23 || offset_minutes > 59 {
                return Err(ParseError::new("Invalid timestamp offset"));
            }

            let offset_ms = i64::from(offset_hours) * MILLIS_PER_HOUR
                + i64::from(offset_minutes) * MILLIS_PER_MINUTE;
            if *sign == b'-' {
                Ok(-offset_ms)
            } else {
                Ok(offset_ms)
            }
        },
        _ => Err(ParseError::new("Timestamp must end with Z or an offset")),
    }
}

/// Format timestamp parts to ISO 8601 string.
///
/// Uses `write!` into a pre-sized `String` to avoid the formatting overhead
/// of `format!()` which goes through `fmt::Arguments` heap allocation.
#[inline]
fn format_parts_iso8601(parts: UtcDateTimeParts, include_millis: bool, zulu: bool) -> String {
    use std::fmt::Write;
    // Max length: "YYYY-MM-DDTHH:MM:SS.mmm+00:00" = 29 chars
    let mut buf = String::with_capacity(32);
    if include_millis {
        if zulu {
            let _ = write!(
                buf,
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
                parts.year,
                parts.month,
                parts.day,
                parts.hour,
                parts.minute,
                parts.second,
                parts.millisecond
            );
        } else {
            let _ = write!(
                buf,
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}+00:00",
                parts.year,
                parts.month,
                parts.day,
                parts.hour,
                parts.minute,
                parts.second,
                parts.millisecond
            );
        }
    } else {
        let _ = write!(
            buf,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            parts.year, parts.month, parts.day, parts.hour, parts.minute, parts.second
        );
    }
    buf
}

/// Timestamp format options.
///
/// Controls how timestamps are displayed in query results and subscriptions.
/// Default format is ISO 8601 with milliseconds (`2024-12-14T15:30:45.123Z`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify_next::Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum TimestampFormat {
    /// ISO 8601 with milliseconds: `2024-12-14T15:30:45.123Z`
    /// This is the default and most widely compatible format.
    #[serde(rename = "iso8601")]
    #[default]
    Iso8601,

    /// ISO 8601 date only: `2024-12-14`
    #[serde(rename = "iso8601-date")]
    Iso8601Date,

    /// ISO 8601 without milliseconds: `2024-12-14T15:30:45Z`
    #[serde(rename = "iso8601-datetime")]
    Iso8601DateTime,

    /// Unix timestamp in milliseconds: `1734211234567`
    #[serde(rename = "unix-ms")]
    UnixMs,

    /// Unix timestamp in seconds: `1734211234`
    #[serde(rename = "unix-sec")]
    UnixSec,

    /// Relative time: `2 hours ago`, `in 5 minutes`
    #[serde(rename = "relative")]
    Relative,

    /// RFC 2822 format: `Fri, 14 Dec 2024 15:30:45 +0000`
    #[serde(rename = "rfc2822")]
    Rfc2822,

    /// RFC 3339 format (same as ISO 8601): `2024-12-14T15:30:45.123+00:00`
    #[serde(rename = "rfc3339")]
    Rfc3339,
}

impl fmt::Display for TimestampFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Iso8601 => write!(f, "iso8601"),
            Self::Iso8601Date => write!(f, "iso8601-date"),
            Self::Iso8601DateTime => write!(f, "iso8601-datetime"),
            Self::UnixMs => write!(f, "unix-ms"),
            Self::UnixSec => write!(f, "unix-sec"),
            Self::Relative => write!(f, "relative"),
            Self::Rfc2822 => write!(f, "rfc2822"),
            Self::Rfc3339 => write!(f, "rfc3339"),
        }
    }
}

/// Timestamp formatter for converting millisecond timestamps to strings.
///
/// # Examples
///
/// ```rust
/// use kalam_link::timestamp::{TimestampFormatter, TimestampFormat};
///
/// let formatter = TimestampFormatter::new(TimestampFormat::Iso8601);
/// let formatted = formatter.format(Some(1734211234567));
/// assert_eq!(formatted, "2024-12-14T21:20:34.567Z");
/// ```
#[derive(Debug, Clone)]
pub struct TimestampFormatter {
    format: TimestampFormat,
}

impl TimestampFormatter {
    /// Create a new timestamp formatter with the specified format.
    pub fn new(format: TimestampFormat) -> Self {
        Self { format }
    }

    /// Format a millisecond timestamp according to the configured format.
    ///
    /// Returns "NULL" for None values.
    pub fn format(&self, ms: Option<i64>) -> String {
        match ms {
            None => "NULL".to_string(),
            Some(ms) => self.format_value(ms),
        }
    }

    /// Format a non-null millisecond timestamp.
    fn format_value(&self, ms: i64) -> String {
        match self.format {
            TimestampFormat::Iso8601 => self.format_iso8601(ms),
            TimestampFormat::Iso8601Date => self.format_iso8601_date(ms),
            TimestampFormat::Iso8601DateTime => self.format_iso8601_datetime(ms),
            TimestampFormat::UnixMs => ms.to_string(),
            TimestampFormat::UnixSec => (ms / 1000).to_string(),
            TimestampFormat::Relative => self.format_relative(ms),
            TimestampFormat::Rfc2822 => self.format_rfc2822(ms),
            TimestampFormat::Rfc3339 => self.format_rfc3339(ms),
        }
    }

    /// Format as ISO 8601 with milliseconds: `2024-12-14T15:30:45.123Z`
    fn format_iso8601(&self, ms: i64) -> String {
        match split_timestamp(ms) {
            Some(parts) => format_parts_iso8601(parts, true, true),
            None => format!("Invalid timestamp: {}", ms),
        }
    }

    /// Format as ISO 8601 date only: `2024-12-14`
    fn format_iso8601_date(&self, ms: i64) -> String {
        match split_timestamp(ms) {
            Some(parts) => {
                use std::fmt::Write;
                let mut buf = String::with_capacity(10);
                let _ = write!(buf, "{:04}-{:02}-{:02}", parts.year, parts.month, parts.day);
                buf
            },
            None => format!("Invalid timestamp: {}", ms),
        }
    }

    /// Format as ISO 8601 without milliseconds: `2024-12-14T15:30:45Z`
    fn format_iso8601_datetime(&self, ms: i64) -> String {
        match split_timestamp(ms) {
            Some(parts) => format_parts_iso8601(parts, false, true),
            None => format!("Invalid timestamp: {}", ms),
        }
    }

    /// Format as RFC 2822: `Fri, 14 Dec 2024 15:30:45 +0000`
    fn format_rfc2822(&self, ms: i64) -> String {
        match split_timestamp(ms) {
            Some(parts) => {
                use std::fmt::Write;
                let mut buf = String::with_capacity(32);
                let _ = write!(
                    buf,
                    "{}, {:02} {} {:04} {:02}:{:02}:{:02} +0000",
                    WEEKDAY_NAMES[usize::from(parts.weekday)],
                    parts.day,
                    MONTH_NAMES[usize::from(parts.month - 1)],
                    parts.year,
                    parts.hour,
                    parts.minute,
                    parts.second
                );
                buf
            },
            None => format!("Invalid timestamp: {}", ms),
        }
    }

    /// Format as RFC 3339: `2024-12-14T15:30:45.123+00:00`
    fn format_rfc3339(&self, ms: i64) -> String {
        match split_timestamp(ms) {
            Some(parts) => format_parts_iso8601(parts, true, false),
            None => format!("Invalid timestamp: {}", ms),
        }
    }

    /// Format as relative time: `2 hours ago`, `in 5 minutes`
    pub fn format_relative(&self, ms: i64) -> String {
        let now = now();
        let diff_ms = now - ms;
        let abs_diff = diff_ms.abs();
        let is_future = diff_ms < 0;

        let seconds = abs_diff / 1000;
        let minutes = seconds / 60;
        let hours = minutes / 60;
        let days = hours / 24;
        let weeks = days / 7;
        let months = days / 30;
        let years = days / 365;

        let (value, unit) = if years > 0 {
            (years, if years == 1 { "year" } else { "years" })
        } else if months > 0 {
            (months, if months == 1 { "month" } else { "months" })
        } else if weeks > 0 {
            (weeks, if weeks == 1 { "week" } else { "weeks" })
        } else if days > 0 {
            (days, if days == 1 { "day" } else { "days" })
        } else if hours > 0 {
            (hours, if hours == 1 { "hour" } else { "hours" })
        } else if minutes > 0 {
            (minutes, if minutes == 1 { "minute" } else { "minutes" })
        } else {
            (seconds, if seconds == 1 { "second" } else { "seconds" })
        };

        if is_future {
            format!("in {} {}", value, unit)
        } else {
            format!("{} {} ago", value, unit)
        }
    }
}

impl Default for TimestampFormatter {
    fn default() -> Self {
        Self::new(TimestampFormat::Iso8601)
    }
}

/// Parse ISO 8601 string back to milliseconds.
///
/// # Examples
///
/// ```rust
/// use kalam_link::timestamp::parse_iso8601;
///
/// let ms = parse_iso8601("2024-12-14T15:30:45.123Z").unwrap();
/// assert_eq!(ms, 1734190245123);
/// ```
pub fn parse_iso8601(iso: &str) -> Result<i64, ParseError> {
    let bytes = iso.as_bytes();
    if bytes.len() < 20 {
        return Err(ParseError::new("Timestamp too short"));
    }

    if bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return Err(ParseError::new("Timestamp must be RFC3339/ISO8601"));
    }

    let year =
        i32::try_from(parse_digits(bytes, 0, 4)?).map_err(|_| ParseError::new("Invalid year"))?;
    let month =
        u8::try_from(parse_digits(bytes, 5, 2)?).map_err(|_| ParseError::new("Invalid month"))?;
    let day =
        u8::try_from(parse_digits(bytes, 8, 2)?).map_err(|_| ParseError::new("Invalid day"))?;
    let hour =
        u8::try_from(parse_digits(bytes, 11, 2)?).map_err(|_| ParseError::new("Invalid hour"))?;
    let minute =
        u8::try_from(parse_digits(bytes, 14, 2)?).map_err(|_| ParseError::new("Invalid minute"))?;
    let second =
        u8::try_from(parse_digits(bytes, 17, 2)?).map_err(|_| ParseError::new("Invalid second"))?;

    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month).ok_or_else(|| ParseError::new("Invalid month"))?
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(ParseError::new("Invalid timestamp component"));
    }

    let mut index = 19;
    let mut millisecond = 0u16;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
            index += 1;
        }
        millisecond = parse_fractional_millis(bytes, fraction_start, index)?;
    }

    let offset_ms = parse_offset_millis(bytes, index)?;
    let days = days_from_civil(year, month, day);
    let local_ms = days
        .checked_mul(MILLIS_PER_DAY)
        .and_then(|value| value.checked_add(i64::from(hour) * MILLIS_PER_HOUR))
        .and_then(|value| value.checked_add(i64::from(minute) * MILLIS_PER_MINUTE))
        .and_then(|value| value.checked_add(i64::from(second) * MILLIS_PER_SECOND))
        .and_then(|value| value.checked_add(i64::from(millisecond)))
        .ok_or_else(|| ParseError::new("Timestamp out of range"))?;

    local_ms
        .checked_sub(offset_ms)
        .ok_or_else(|| ParseError::new("Timestamp out of range"))
}

/// Get current timestamp in milliseconds (compatible with KalamDB).
///
/// # Examples
///
/// ```rust
/// use kalam_link::timestamp::now;
///
/// let current_ms = now();
/// assert!(current_ms > 1700000000000); // After Nov 2023
/// ```
pub fn now() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_iso8601() {
        let formatter = TimestampFormatter::new(TimestampFormat::Iso8601);
        let result = formatter.format(Some(1734191445123));
        assert!(result.contains("2024-12-14"));
        assert!(result.ends_with("Z"));
    }

    #[test]
    fn test_format_iso8601_date() {
        let formatter = TimestampFormatter::new(TimestampFormat::Iso8601Date);
        let result = formatter.format(Some(1734191445123));
        assert_eq!(result, "2024-12-14");
    }

    #[test]
    fn test_format_unix_ms() {
        let formatter = TimestampFormatter::new(TimestampFormat::UnixMs);
        let result = formatter.format(Some(1734191445123));
        assert_eq!(result, "1734191445123");
    }

    #[test]
    fn test_format_unix_sec() {
        let formatter = TimestampFormatter::new(TimestampFormat::UnixSec);
        let result = formatter.format(Some(1734191445123));
        assert_eq!(result, "1734191445");
    }

    #[test]
    fn test_format_null() {
        let formatter = TimestampFormatter::new(TimestampFormat::Iso8601);
        let result = formatter.format(None);
        assert_eq!(result, "NULL");
    }

    #[test]
    fn test_format_relative() {
        let formatter = TimestampFormatter::new(TimestampFormat::Relative);
        let two_hours_ago = now() - (2 * 60 * 60 * 1000);
        let result = formatter.format(Some(two_hours_ago));
        assert!(result.contains("2 hours ago") || result.contains("1 hour ago"));
    }

    #[test]
    fn test_parse_iso8601() {
        let ms = parse_iso8601("2024-12-14T15:30:45.123Z").unwrap();
        assert!(ms > 1700000000000);
    }

    #[test]
    fn test_parse_iso8601_with_offset() {
        let utc = parse_iso8601("2024-12-14T15:30:45.123Z").unwrap();
        let offset = parse_iso8601("2024-12-14T17:30:45.123+02:00").unwrap();
        assert_eq!(utc, offset);
    }

    #[test]
    fn test_format_rfc2822() {
        let formatter = TimestampFormatter::new(TimestampFormat::Rfc2822);
        assert_eq!(formatter.format(Some(1734190245123)), "Sat, 14 Dec 2024 15:30:45 +0000");
    }

    #[test]
    fn test_now() {
        let ms = now();
        assert!(ms > 1700000000000);
    }

    // ── Regression tests for write!-based formatting (zero-alloc rewrite) ──

    #[test]
    fn test_format_iso8601_exact_output() {
        let formatter = TimestampFormatter::new(TimestampFormat::Iso8601);
        assert_eq!(
            formatter.format(Some(1734211234567)),
            "2024-12-14T21:20:34.567Z"
        );
    }

    #[test]
    fn test_format_iso8601_date_exact() {
        let formatter = TimestampFormatter::new(TimestampFormat::Iso8601Date);
        // Known epoch: 2024-01-01T00:00:00.000Z
        assert_eq!(formatter.format(Some(1704067200000)), "2024-01-01");
        // End-of-year rollover
        assert_eq!(formatter.format(Some(1735689599999)), "2024-12-31");
    }

    #[test]
    fn test_format_rfc2822_exact() {
        let formatter = TimestampFormatter::new(TimestampFormat::Rfc2822);
        // Mon, 01 Jan 2024 00:00:00 +0000
        assert_eq!(
            formatter.format(Some(1704067200000)),
            "Mon, 01 Jan 2024 00:00:00 +0000"
        );
    }

    #[test]
    fn test_format_rfc3339_exact() {
        let formatter = TimestampFormatter::new(TimestampFormat::Rfc3339);
        assert_eq!(
            formatter.format(Some(1734211234567)),
            "2024-12-14T21:20:34.567+00:00"
        );
    }

    #[test]
    fn test_format_iso8601_datetime_no_millis() {
        let formatter = TimestampFormatter::new(TimestampFormat::Iso8601DateTime);
        assert_eq!(
            formatter.format(Some(1734211234567)),
            "2024-12-14T21:20:34Z"
        );
    }

    #[test]
    fn test_format_epoch_zero() {
        let iso = TimestampFormatter::new(TimestampFormat::Iso8601);
        assert_eq!(iso.format(Some(0)), "1970-01-01T00:00:00.000Z");

        let date = TimestampFormatter::new(TimestampFormat::Iso8601Date);
        assert_eq!(date.format(Some(0)), "1970-01-01");

        let rfc = TimestampFormatter::new(TimestampFormat::Rfc2822);
        assert_eq!(rfc.format(Some(0)), "Thu, 01 Jan 1970 00:00:00 +0000");
    }

    #[test]
    fn test_format_consistency_across_formats() {
        // All formats should agree on the date portion for the same timestamp
        let ms = 1734211234567_i64;
        let iso = TimestampFormatter::new(TimestampFormat::Iso8601).format(Some(ms));
        let date = TimestampFormatter::new(TimestampFormat::Iso8601Date).format(Some(ms));
        let rfc = TimestampFormatter::new(TimestampFormat::Rfc3339).format(Some(ms));

        assert!(iso.starts_with("2024-12-14"));
        assert_eq!(date, "2024-12-14");
        assert!(rfc.starts_with("2024-12-14"));
    }
}
