use shopping_sessions_parser::{
    ParsedTimestamp, TimestampError, TimestampWindow, WindowClassification, parse_timestamp,
};

fn leap(year: i32) -> bool {
    year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)
}

fn month_days(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap(year) => 29,
        2 => 28,
        _ => panic!("invalid test month"),
    }
}

fn reference_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
    let mut days = 0_i64;
    if year >= 1970 {
        for candidate in 1970..year {
            days += if leap(candidate) { 366 } else { 365 };
        }
    } else {
        for candidate in year..1970 {
            days -= if leap(candidate) { 366 } else { 365 };
        }
    }
    for candidate in 1..month {
        days += i64::from(month_days(year, candidate));
    }
    days += i64::from(day - 1);
    (days * 86_400 + i64::from(hour * 3_600 + minute * 60 + second)) * 1_000
}

#[test]
fn generated_calendar_and_offset_cases_match_an_independent_reference() {
    let mut state = 0x1234_5678_9abc_def0_u64;
    for _ in 0..20_000 {
        state = state
            .wrapping_mul(2_862_933_555_777_941_757)
            .wrapping_add(3_037_000_493);
        let year = 1900 + i32::try_from(state % 201).unwrap();
        let month = 1 + u32::try_from((state >> 8) % 12).unwrap();
        let day = 1 + u32::try_from((state >> 16) % u64::from(month_days(year, month))).unwrap();
        let hour = u32::try_from((state >> 24) % 24).unwrap();
        let minute = u32::try_from((state >> 32) % 60).unwrap();
        let second = u32::try_from((state >> 40) % 60).unwrap();
        let text = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z");
        assert_eq!(
            parse_timestamp(text.as_bytes()),
            Ok(ParsedTimestamp::Milliseconds(reference_ms(
                year, month, day, hour, minute, second
            )))
        );

        let offset_hour = u32::try_from((state >> 12) % 24).unwrap();
        let offset_minute = u32::try_from((state >> 20) % 60).unwrap();
        let sign = if state & 1 == 0 { '+' } else { '-' };
        let offset_text = format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{sign}{offset_hour:02}:{offset_minute:02}"
        );
        let offset_ms = i64::from(offset_hour * 60 + offset_minute) * 60_000;
        let expected = if sign == '+' {
            reference_ms(year, month, day, hour, minute, second) - offset_ms
        } else {
            reference_ms(year, month, day, hour, minute, second) + offset_ms
        };
        assert_eq!(
            parse_timestamp(offset_text.as_bytes()),
            Ok(ParsedTimestamp::Milliseconds(expected))
        );
    }
}

#[test]
fn leap_century_boundaries_are_exact() {
    for (year, valid) in [(1900, false), (2000, true), (2100, false), (2400, true)] {
        let text = format!("{year:04}-02-29T00:00:00Z");
        if valid {
            assert!(parse_timestamp(text.as_bytes()).is_ok());
        } else {
            assert_eq!(
                parse_timestamp(text.as_bytes()),
                Err(TimestampError::InvalidCalendar)
            );
        }
    }
}

#[test]
fn event_and_click_windows_are_half_open_with_inclusive_lookback() {
    let start = reference_ms(2026, 10, 1, 0, 0, 0);
    let end = reference_ms(2026, 10, 15, 0, 0, 0);
    let window = TimestampWindow::new(start, end).unwrap();
    for (value, event, click) in [
        (
            start - 604_800_001,
            WindowClassification::OutsideWindow,
            WindowClassification::OutsideWindow,
        ),
        (
            start - 604_800_000,
            WindowClassification::OutsideWindow,
            WindowClassification::InWindow(start - 604_800_000),
        ),
        (
            start,
            WindowClassification::InWindow(start),
            WindowClassification::InWindow(start),
        ),
        (
            end - 1,
            WindowClassification::InWindow(end - 1),
            WindowClassification::InWindow(end - 1),
        ),
        (
            end,
            WindowClassification::OutsideWindow,
            WindowClassification::OutsideWindow,
        ),
    ] {
        let text = value.to_string();
        assert_eq!(window.classify_event(text.as_bytes()), event);
        assert_eq!(window.classify_click(text.as_bytes()), click);
    }
}

#[test]
fn constructor_rejects_empty_reversed_and_lookback_overflow_windows() {
    assert!(TimestampWindow::new(1, 1).is_none());
    assert!(TimestampWindow::new(2, 1).is_none());
    assert!(TimestampWindow::new(i64::MIN, 0).is_none());
}
