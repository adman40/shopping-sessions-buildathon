use shopping_sessions_domain::time::ATTRIBUTION_WINDOW_MS;

/// Exact failure from unsigned ASCII integer parsing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegerError {
    Empty,
    NonDigit,
    Overflow,
}

/// A syntactically valid timestamp, including an F3 integer too large for `i64`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParsedTimestamp {
    Milliseconds(i64),
    Overflow,
}

/// Calendar and shape failures are distinct from an out-of-range F3 integer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampError {
    InvalidFormat,
    InvalidCalendar,
}

/// Window result that preserves invalid-versus-outside classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowClassification {
    InWindow(i64),
    OutsideWindow,
    Invalid(TimestampError),
}

/// Event and click windows derived from the manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampWindow {
    start_ms: i64,
    end_ms: i64,
    click_start_ms: i64,
}

impl TimestampWindow {
    /// Creates a nonempty half-open event window whose click lookback is representable.
    #[must_use]
    pub fn new(start_ms: i64, end_ms: i64) -> Option<Self> {
        if start_ms >= end_ms {
            return None;
        }
        Some(Self {
            start_ms,
            end_ms,
            click_start_ms: start_ms.checked_sub(ATTRIBUTION_WINDOW_MS)?,
        })
    }

    #[must_use]
    pub const fn start_ms(self) -> i64 {
        self.start_ms
    }

    #[must_use]
    pub const fn end_ms(self) -> i64 {
        self.end_ms
    }

    #[must_use]
    pub const fn click_start_ms(self) -> i64 {
        self.click_start_ms
    }

    #[must_use]
    pub fn classify_event(self, value: &[u8]) -> WindowClassification {
        classify(value, self.start_ms, self.end_ms)
    }

    #[must_use]
    pub fn classify_click(self, value: &[u8]) -> WindowClassification {
        classify(value, self.click_start_ms, self.end_ms)
    }
}

/// Parses a nonempty unsigned base-10 integer using ASCII digits only.
///
/// # Errors
///
/// Distinguishes an empty value, any non-ASCII-digit byte, and `u64` overflow.
pub fn parse_ascii_u64(value: &[u8]) -> Result<u64, IntegerError> {
    if value.is_empty() {
        return Err(IntegerError::Empty);
    }
    let mut parsed = 0_u64;
    for byte in value {
        if !byte.is_ascii_digit() {
            return Err(IntegerError::NonDigit);
        }
        parsed = parsed
            .checked_mul(10)
            .and_then(|number| number.checked_add(u64::from(*byte - b'0')))
            .ok_or(IntegerError::Overflow)?;
    }
    Ok(parsed)
}

/// Parses F1, F2, or F3 exactly.
///
/// F3 overflow is retained as a syntactically valid but unrepresentable value so
/// window classification can report it as outside rather than malformed.
///
/// # Errors
///
/// Returns the exact format or calendar failure for malformed F1/F2 input and
/// non-digit input that is not one of those forms.
pub fn parse_timestamp(value: &[u8]) -> Result<ParsedTimestamp, TimestampError> {
    if !value.is_empty() && value.iter().all(u8::is_ascii_digit) {
        let mut parsed = 0_i64;
        for byte in value {
            let Some(next) = parsed
                .checked_mul(10)
                .and_then(|number| number.checked_add(i64::from(*byte - b'0')))
            else {
                return Ok(ParsedTimestamp::Overflow);
            };
            parsed = next;
        }
        return Ok(ParsedTimestamp::Milliseconds(parsed));
    }

    parse_calendar_timestamp(value, false).map(ParsedTimestamp::Milliseconds)
}

/// Parses exact UTC whole-second F1 syntax.
///
/// # Errors
///
/// Returns the exact format or calendar failure when the value is not valid F1.
pub fn parse_f1_timestamp(value: &[u8]) -> Result<i64, TimestampError> {
    parse_calendar_timestamp(value, true)
}

fn classify(value: &[u8], start: i64, end: i64) -> WindowClassification {
    match parse_timestamp(value) {
        Ok(ParsedTimestamp::Milliseconds(milliseconds))
            if milliseconds >= start && milliseconds < end =>
        {
            WindowClassification::InWindow(milliseconds)
        }
        Ok(ParsedTimestamp::Milliseconds(_) | ParsedTimestamp::Overflow) => {
            WindowClassification::OutsideWindow
        }
        Err(error) => WindowClassification::Invalid(error),
    }
}

fn parse_calendar_timestamp(value: &[u8], f1_only: bool) -> Result<i64, TimestampError> {
    let is_f1 = value.len() == 20 && value.get(19) == Some(&b'Z');
    let is_f2 = !f1_only
        && value.len() == 25
        && matches!(value.get(19), Some(b'+' | b'-'))
        && value.get(22) == Some(&b':');
    if !is_f1 && !is_f2 {
        return Err(TimestampError::InvalidFormat);
    }
    if value.get(4) != Some(&b'-')
        || value.get(7) != Some(&b'-')
        || value.get(10) != Some(&b'T')
        || value.get(13) != Some(&b':')
        || value.get(16) != Some(&b':')
    {
        return Err(TimestampError::InvalidFormat);
    }

    let year = fixed_digits(value, 0, 4)?;
    let month = fixed_digits(value, 5, 2)?;
    let day = fixed_digits(value, 8, 2)?;
    let hour = fixed_digits(value, 11, 2)?;
    let minute = fixed_digits(value, 14, 2)?;
    let second = fixed_digits(value, 17, 2)?;
    if year == 0
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(TimestampError::InvalidCalendar);
    }

    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day));
    let local_seconds =
        days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    let mut utc_seconds = local_seconds;
    if is_f2 {
        let offset_hour = fixed_digits(value, 20, 2)?;
        let offset_minute = fixed_digits(value, 23, 2)?;
        if offset_hour > 23 || offset_minute > 59 {
            return Err(TimestampError::InvalidCalendar);
        }
        let offset_seconds = i64::from(offset_hour * 60 + offset_minute) * 60;
        if value[19] == b'+' {
            utc_seconds -= offset_seconds;
        } else {
            utc_seconds += offset_seconds;
        }
    }
    utc_seconds
        .checked_mul(1_000)
        .ok_or(TimestampError::InvalidCalendar)
}

fn fixed_digits(value: &[u8], start: usize, length: usize) -> Result<u32, TimestampError> {
    let bytes = value
        .get(start..start + length)
        .ok_or(TimestampError::InvalidFormat)?;
    let mut parsed = 0_u32;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return Err(TimestampError::InvalidFormat);
        }
        parsed = parsed * 10 + u32::from(*byte - b'0');
    }
    Ok(parsed)
}

const fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => 0,
    }
}

const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = year - if month <= 2 { 1 } else { 0 };
    let era = adjusted_year / 400;
    let year_of_era = adjusted_year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_forms_and_offsets() {
        let expected = ParsedTimestamp::Milliseconds(1_759_535_890_000);
        assert_eq!(parse_timestamp(b"2025-10-03T23:58:10Z"), Ok(expected));
        assert_eq!(parse_timestamp(b"2025-10-03T19:58:10-04:00"), Ok(expected));
        assert_eq!(parse_timestamp(b"001759535890000"), Ok(expected));
        assert_eq!(parse_timestamp(b"2025-10-03T23:58:10-00:00"), Ok(expected));
    }

    #[test]
    fn validates_calendar_and_exact_shape() {
        assert!(parse_timestamp(b"2028-02-29T00:00:00Z").is_ok());
        assert_eq!(
            parse_timestamp(b"2027-02-29T00:00:00Z"),
            Err(TimestampError::InvalidCalendar)
        );
        for bad in [
            &b"2028-02-29t00:00:00Z"[..],
            &b"2028-02-29T00:00:00z"[..],
            &b"2028-02-29T00:00:00.000Z"[..],
            &b"2028-02-29T00:00:00+2400"[..],
            &b"-5"[..],
        ] {
            assert_eq!(parse_timestamp(bad), Err(TimestampError::InvalidFormat));
        }
        for bad in [
            &b"2028-13-01T00:00:00Z"[..],
            &b"2028-01-01T24:00:00Z"[..],
            &b"2028-01-01T00:00:60Z"[..],
            &b"2028-01-01T00:00:00+24:00"[..],
            &b"2028-01-01T00:00:00+05:60"[..],
        ] {
            assert_eq!(parse_timestamp(bad), Err(TimestampError::InvalidCalendar));
        }
    }

    #[test]
    fn overflow_is_valid_but_outside_every_window() {
        let huge = b"999999999999999999999";
        assert_eq!(parse_timestamp(huge), Ok(ParsedTimestamp::Overflow));
        let window = TimestampWindow::new(0, 1_000).unwrap();
        assert_eq!(
            window.classify_event(huge),
            WindowClassification::OutsideWindow
        );
    }

    #[test]
    fn ascii_integer_parser_rejects_signs_unicode_and_overflow() {
        assert_eq!(parse_ascii_u64(b"0250"), Ok(250));
        assert_eq!(parse_ascii_u64(b""), Err(IntegerError::Empty));
        assert_eq!(parse_ascii_u64(b"+1"), Err(IntegerError::NonDigit));
        assert_eq!(
            parse_ascii_u64("٢٥٠".as_bytes()),
            Err(IntegerError::NonDigit)
        );
        assert_eq!(
            parse_ascii_u64(b"18446744073709551616"),
            Err(IntegerError::Overflow)
        );
    }
}
